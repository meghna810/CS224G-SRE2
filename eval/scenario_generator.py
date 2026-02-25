"""
scenario_generator.py - Generates synthetic OTLP protobuf data for each benchmark scenario.

Given a scenario definition, produces:
  - ExportTraceServiceRequest  (traces showing error propagation)
  - ExportMetricsServiceRequest (minimal, signals exist)
  - ExportLogsServiceRequest    (error logs at root-cause service)

The generator encodes the fault cleanly in the telemetry so that
RootScout's ingestion + RCA pipeline has everything it needs.
"""

import time
from datetime import timezone
from typing import Dict, Any, Tuple

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span, Status
from opentelemetry.proto.metrics.v1.metrics_pb2 import ResourceMetrics, ScopeMetrics
from opentelemetry.proto.logs.v1.logs_pb2 import ResourceLogs, ScopeLogs, LogRecord
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.common.v1.common_pb2 import KeyValue, AnyValue, InstrumentationScope


# ---------------------------------------------------------------------------
# Protobuf helpers
# ---------------------------------------------------------------------------

def _kv(key: str, value: str) -> KeyValue:
    return KeyValue(key=key, value=AnyValue(string_value=value))

def _kv_int(key: str, value: int) -> KeyValue:
    return KeyValue(key=key, value=AnyValue(int_value=value))


def _resource(service_name: str, version: str = "1.0.0") -> Resource:
    return Resource(attributes=[
        _kv("service.name", service_name),
        _kv("service.version", version),
        _kv("deployment.environment.name", "production"),
    ])


def _scope(service_name: str) -> InstrumentationScope:
    return InstrumentationScope(name=f"opentelemetry.instrumentation.{service_name}")


# ---------------------------------------------------------------------------
# Trace generation
# ---------------------------------------------------------------------------

def _make_span(
    service_name: str,
    span_name: str,
    trace_id: bytes,
    span_id: bytes,
    parent_span_id: bytes | None,
    start_ns: int,
    duration_ns: int,
    is_error: bool,
    error_message: str = "",
    http_status: str = "200",
) -> Span:
    status_code = Status.STATUS_CODE_ERROR if is_error else Status.STATUS_CODE_OK
    attrs = [
        _kv("http.method", "GET"),
        _kv("http.route", f"/{service_name}"),
        _kv("http.status_code", http_status),
    ]
    if is_error:
        attrs += [
            _kv("error", "true"),
            _kv("error.message", error_message[:200]),
        ]
    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name=span_name,
        kind=Span.SPAN_KIND_SERVER,
        start_time_unix_nano=start_ns,
        end_time_unix_nano=start_ns + duration_ns,
        status=Status(code=status_code, message=error_message[:200] if is_error else ""),
        attributes=attrs,
    )
    if parent_span_id:
        span.parent_span_id = parent_span_id
    return span


def generate_traces(scenario: Dict[str, Any]) -> ExportTraceServiceRequest:
    """
    Build a chain of spans that reflects the scenario's topology and fault.

    Strategy:
    - Root span is at observed_service (where the alert fires)
    - We walk the topology edges to build a call chain to the root cause
    - The root-cause service span is marked ERROR; its callers are also ERROR
      (timeout / propagation); healthy services are OK
    """
    topology = scenario["topology"]
    fault = scenario["fault_injection"]
    root_cause_svc = fault["root_cause_service"]
    propagates_to = set(fault.get("propagates_to", []))
    error_services = propagates_to | {root_cause_svc}
    error_message = fault["error_message"]
    http_status = fault.get("status_code_http", "500")
    observed_service = scenario["observed_service"]

    fault_start_ts = scenario["fault_start_ts"]
    base_ns = int(fault_start_ts.replace(tzinfo=timezone.utc).timestamp() * 1e9)

    # Build a simple trace_id shared across all spans
    trace_id = bytes.fromhex("aabbccdd11223344aabbccdd11223344")

    services = topology["services"]
    edges = topology["edges"]

    # Build a parent map: service -> parent service (for span linking)
    # We'll build a BFS order from observed_service through the edges
    from collections import deque
    adj: Dict[str, list] = {s: [] for s in services}
    for src, dst in edges:
        adj[src].append(dst)

    # BFS to order services
    visited = {}
    queue = deque([(observed_service, None)])
    order = []
    while queue:
        svc, parent = queue.popleft()
        if svc in visited:
            continue
        visited[svc] = parent
        order.append(svc)
        for child in adj.get(svc, []):
            if child not in visited:
                queue.append((child, svc))

    # Assign span IDs
    span_ids: Dict[str, bytes] = {}
    for i, svc in enumerate(order):
        hex_id = f"{(i+1):016x}"
        span_ids[svc] = bytes.fromhex(hex_id)

    resource_spans_list = []
    for idx, svc in enumerate(order):
        parent_svc = visited[svc]
        parent_span_id = span_ids.get(parent_svc) if parent_svc else None

        # Stagger start times so the root cause is earlier
        start_offset = max(0, (len(order) - idx - 1)) * int(0.5e9)
        # Root-cause span: longer duration to signal the problem
        is_error = svc in error_services
        duration = int(5e9) if is_error else int(0.1e9)

        span = _make_span(
            service_name=svc,
            span_name=f"GET /{svc}/handle",
            trace_id=trace_id,
            span_id=span_ids[svc],
            parent_span_id=parent_span_id,
            start_ns=base_ns + start_offset,
            duration_ns=duration,
            is_error=is_error,
            error_message=error_message if svc == root_cause_svc else f"upstream error from {root_cause_svc}",
            http_status=http_status if is_error else "200",
        )

        rs = ResourceSpans(
            resource=_resource(svc),
            scope_spans=[ScopeSpans(scope=_scope(svc), spans=[span])],
        )
        resource_spans_list.append(rs)

    return ExportTraceServiceRequest(resource_spans=resource_spans_list)


# ---------------------------------------------------------------------------
# Log generation
# ---------------------------------------------------------------------------

def generate_logs(scenario: Dict[str, Any]) -> ExportLogsServiceRequest:
    """
    Emit ERROR log records for the root-cause service.
    Also emit WARN logs for services that received propagated errors.
    """
    fault = scenario["fault_injection"]
    root_cause_svc = fault["root_cause_service"]
    propagates_to = fault.get("propagates_to", [])
    error_message = fault["error_message"]

    fault_start_ts = scenario["fault_start_ts"]
    base_ns = int(fault_start_ts.replace(tzinfo=timezone.utc).timestamp() * 1e9)

    trace_id = bytes.fromhex("aabbccdd11223344aabbccdd11223344")
    resource_logs_list = []

    # Error log at root cause service
    error_log = LogRecord(
        time_unix_nano=base_ns + int(1e9),
        severity_number=17,  # ERROR
        severity_text="ERROR",
        body=AnyValue(string_value=error_message),
        attributes=[
            _kv("log.logger", f"{root_cause_svc}.handler"),
            _kv("error.type", fault["fault_type"]),
        ],
        trace_id=trace_id,
    )
    rl = ResourceLogs(
        resource=_resource(root_cause_svc),
        scope_logs=[ScopeLogs(scope=_scope(root_cause_svc), log_records=[error_log])],
    )
    resource_logs_list.append(rl)

    # Warn logs at propagated services
    for downstream_svc in propagates_to:
        warn_log = LogRecord(
            time_unix_nano=base_ns + int(2e9),
            severity_number=13,  # WARN
            severity_text="WARN",
            body=AnyValue(string_value=f"Received error response from upstream service {root_cause_svc}"),
            attributes=[_kv("log.logger", f"{downstream_svc}.client")],
            trace_id=trace_id,
        )
        rl = ResourceLogs(
            resource=_resource(downstream_svc),
            scope_logs=[ScopeLogs(scope=_scope(downstream_svc), log_records=[warn_log])],
        )
        resource_logs_list.append(rl)

    return ExportLogsServiceRequest(resource_logs=resource_logs_list)


# ---------------------------------------------------------------------------
# Metrics generation (minimal stubs — graph health derived from traces/logs)
# ---------------------------------------------------------------------------

def generate_metrics(scenario: Dict[str, Any]) -> ExportMetricsServiceRequest:
    """Minimal metric stubs — service list exists in metric scope."""
    services = scenario["topology"]["services"]
    resource_metrics_list = []
    for svc in services:
        rm = ResourceMetrics(
            resource=_resource(svc),
            scope_metrics=[ScopeMetrics(scope=_scope(svc), metrics=[])],
        )
        resource_metrics_list.append(rm)
    return ExportMetricsServiceRequest(resource_metrics=resource_metrics_list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_otlp(
    scenario: Dict[str, Any],
) -> Tuple[ExportTraceServiceRequest, ExportMetricsServiceRequest, ExportLogsServiceRequest]:
    """Generate all three OTLP signal types for a scenario."""
    traces = generate_traces(scenario)
    metrics = generate_metrics(scenario)
    logs = generate_logs(scenario)
    return traces, metrics, logs
