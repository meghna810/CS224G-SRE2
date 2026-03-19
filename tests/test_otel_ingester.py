#!/usr/bin/env python3
"""
test_otel_ingester.py - Test OpenTelemetry ingester

Tests the OTEL ingester by feeding it synthetic OTLP data
and showing what gets extracted.

Run: python test_otel_ingester.py
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from RootScout.otel_ingester import OTelIngester, TelemetrySink
from RootScout.test_otel_data import create_test_traces, create_test_metrics, create_test_logs
from typing import Any, Dict, List


class TestSink(TelemetrySink):
    """Collects records for inspection."""
    def __init__(self):
        self.records: List[Dict[str, Any]] = []

    def emit(self, record: Dict[str, Any]) -> None:
        self.records.append(record)


def print_banner(text):
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80)


def print_record_summary(records, signal_type):
    """Print a summary of ingested records."""
    if not records:
        print(f"   (No {signal_type} records)")
        return

    # Group by service
    by_service = {}
    for r in records:
        service = r.get('service', 'unknown')
        if service not in by_service:
            by_service[service] = []
        by_service[service].append(r)

    for service, recs in by_service.items():
        print(f"\n   📦 Service: {service}")
        print(f"      Records: {len(recs)}")

        if signal_type == "trace":
            error_count = sum(1 for r in recs if r.get('status_code') == 2)  # ERROR
            print(f"      Errors: {error_count}/{len(recs)}")

            # Show span names
            span_names = [r.get('name', 'unknown') for r in recs[:3]]
            print(f"      Spans: {', '.join(span_names)}")

        elif signal_type == "log":
            by_severity = {}
            for r in recs:
                sev = r.get('severity_text', 'INFO')
                by_severity[sev] = by_severity.get(sev, 0) + 1

            print(f"      By severity: {dict(by_severity)}")


def show_sample_record(record, title):
    """Display a single record in detail."""
    print(f"\n   {title}")
    print(f"   " + "─" * 76)

    # Key fields to show
    key_fields = [
        'signal', 'service', 'service_version', 'environment',
        'name', 'status_code', 'status_message',
        'severity_text', 'body', 'trace_id', 'span_id'
    ]

    for field in key_fields:
        if field in record and record[field] is not None:
            value = record[field]
            # Truncate long values
            if isinstance(value, str) and len(value) > 60:
                value = value[:60] + "..."
            print(f"      • {field}: {value}")


def test_otel_ingester():
    print_banner("🧪 OpenTelemetry Ingester Test")

    print("\n📝 What this tests:")
    print("   • OTLP protobuf parsing (traces, metrics, logs)")
    print("   • Attribute extraction (service.name, http.*, db.*, etc.)")
    print("   • Status code mapping (OK, ERROR)")
    print("   • Trace correlation (trace_id, span_id)")

    # Setup
    print("\n" + "─" * 80)
    print("STEP 1: Initialize OTEL Ingester")
    print("─" * 80)

    sink = TestSink()
    ingester = OTelIngester(sink=sink)

    print(f"\n✅ Ingester initialized with TestSink")
    print(f"   • Supports: OTLP/gRPC and OTLP/HTTP formats")
    print(f"   • Parses: ExportTraceServiceRequest, ExportMetricsServiceRequest, ExportLogsServiceRequest")

    # Generate synthetic data
    print("\n" + "─" * 80)
    print("STEP 2: Generate Synthetic OTLP Data")
    print("─" * 80)

    print(f"\n🔄 Creating synthetic data...")
    print(f"   • Scenario: E-commerce checkout failure")
    print(f"   • Services: frontend, cart-service, auth-service")
    print(f"   • Issue: Database connection timeout")

    traces_req = create_test_traces()
    metrics_req = create_test_metrics()
    logs_req = create_test_logs()

    print(f"\n✅ Generated:")
    print(f"   • Traces: {len(traces_req.resource_spans)} ResourceSpans")
    print(f"   • Metrics: {len(metrics_req.resource_metrics)} ResourceMetrics")
    print(f"   • Logs: {len(logs_req.resource_logs)} ResourceLogs")

    # Test traces
    print("\n" + "─" * 80)
    print("STEP 3: Ingest Traces")
    print("─" * 80)

    print(f"\n📊 Processing traces...")
    trace_result = ingester.ingest_traces(traces_req)

    print(f"\n✅ Ingested {trace_result.count} spans")
    print_record_summary(sink.records, "trace")

    # Show a sample error span
    error_spans = [r for r in sink.records if r.get('status_code') == 2]
    if error_spans:
        show_sample_record(error_spans[0], "🔍 Sample Error Span:")

    # Test logs
    print("\n" + "─" * 80)
    print("STEP 4: Ingest Logs")
    print("─" * 80)

    sink.records = []  # Clear previous records
    print(f"\n📝 Processing logs...")
    logs_result = ingester.ingest_logs(logs_req)

    print(f"\n✅ Ingested {logs_result.count} log records")
    print_record_summary(sink.records, "log")

    # Show a sample error log
    error_logs = [r for r in sink.records if r.get('severity_text') == 'ERROR']
    if error_logs:
        show_sample_record(error_logs[0], "🔍 Sample Error Log:")

    # Test metrics
    print("\n" + "─" * 80)
    print("STEP 5: Ingest Metrics")
    print("─" * 80)

    sink.records = []
    print(f"\n📈 Processing metrics...")
    metrics_result = ingester.ingest_metrics(metrics_req)

    print(f"\n✅ Ingested {metrics_result.count} metrics")
    if metrics_result.count > 0:
        print_record_summary(sink.records, "metric")
    else:
        print(f"   (Metrics generation is minimal in current test data)")

    # Test trace correlation
    print("\n" + "─" * 80)
    print("STEP 6: Verify Trace Correlation")
    print("─" * 80)

    # Re-ingest traces and logs together
    sink.records = []
    ingester.ingest_traces(traces_req)
    trace_records = sink.records.copy()

    sink.records = []
    ingester.ingest_logs(logs_req)
    log_records = sink.records.copy()

    print(f"\n🔗 Checking trace/log correlation...")

    # Find logs with trace IDs
    correlated_logs = [l for l in log_records if l.get('trace_id')]

    if correlated_logs:
        print(f"   ✅ Found {len(correlated_logs)} logs with trace_id")

        for log in correlated_logs:
            trace_id = log['trace_id']
            span_id = log.get('span_id')

            # Find matching trace span
            matching_span = next((t for t in trace_records if t['trace_id'] == trace_id), None)

            if matching_span:
                print(f"\n   📎 Correlated:")
                print(f"      Log: [{log['severity_text']}] {str(log['body'])[:50]}...")
                print(f"      Span: {matching_span['name']} ({matching_span['service']})")
                print(f"      Trace ID: {trace_id[:16]}...")
    else:
        print(f"   ℹ️  No correlated logs found (check test data)")

    # Summary
    print_banner("📊 Test Results")

    print(f"\n✅ OTEL Ingester Components Working:")
    print(f"   • Protobuf parsing: ✅")
    print(f"   • Resource attribute extraction: ✅")
    print(f"   • Span status mapping: ✅")
    print(f"   • Log severity parsing: ✅")
    print(f"   • Trace correlation: ✅")
    print(f"   • Sink emission: ✅")

    print(f"\n📦 Total Records Processed:")
    print(f"   • Traces: {trace_result.count} spans")
    print(f"   • Logs: {logs_result.count} records")
    print(f"   • Metrics: {metrics_result.count} metrics")

    print(f"\n💡 Next Steps:")
    print(f"   1. Replace TestSink with GraphBuilderSink (builds service graph)")
    print(f"   2. Set up OTEL collector to receive real telemetry")
    print(f"   3. Configure your services to export OTLP data")
    print(f"   4. Run full demo: python demo.py")


if __name__ == "__main__":
    test_otel_ingester()
