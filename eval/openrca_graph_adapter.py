"""
openrca_graph_adapter.py - Build a GraphBuilder from real Bank telemetry.

Consumes the windowed metric and log DataFrames produced by
openrca_bank_loader.py and populates a GraphBuilder instance with:

  1. All 14 Bank services wired into the static topology
     (apache → Tomcat → MySQL/Redis/MG → IG)
  2. Node status set to "error" when any KPI exceeds a threshold
     (CPU > 80 %, memory > 85 %, packet-loss > 0, etc.)
  3. Real metric values attached as recent_events so the LLM sees
     actual numbers (e.g. "CPU usage: 94.3")
  4. Real log snippets attached as events; error-pattern lines
     (OOM, exception, timeout, ...) are prioritised

The resulting GraphBuilder is passed unchanged to ContextRetriever
and RCAAgent — no modifications to the core agent pipeline are needed.
"""

import os
from datetime import datetime
from typing import Any, Dict

import pandas as pd

# Lazy import to avoid circular dependency
_nx = None

def _networkx():
    global _nx
    if _nx is None:
        import networkx as nx
        _nx = nx
    return _nx


from graph.graph_builder import GraphBuilder


# ---------------------------------------------------------------------------
# KPI anomaly thresholds
# (aligned with the failure labels in Bank/record.csv)
# ---------------------------------------------------------------------------
_THRESHOLDS: Dict[str, float] = {
    # CPU
    "cpucpuutil":      80.0,
    "cpuutil":         80.0,
    "cpu_util":        80.0,
    # Memory
    "memusedpercent":  85.0,
    "heapmemused":     85.0,
    "jvmheapused":     85.0,
    "mem_used":        85.0,
    # JVM
    "jvmcpuload":      80.0,
    # Disk
    "diskutil":        90.0,
    "diskioread":      80.0,
    "diskspaceused":   90.0,
    "disk_io_read":    80.0,
    # Network
    "netpktloss":       1.0,   # any packet loss is anomalous
    "netlatency":     500.0,   # ms
    "netdrops":         1.0,
    "packetloss":       1.0,
}

# KPI name fragment → human-readable label (for event summaries)
_KPI_LABELS: Dict[str, str] = {
    "cpu":        "CPU usage",
    "mem":        "memory usage",
    "heap":       "JVM heap",
    "disk":       "disk I/O",
    "net":        "network",
    "gc":         "GC activity",
    "innodb":     "MySQL I/O",
    "connection": "DB connections",
    "thread":     "thread count",
    "jvm":        "JVM",
}


def _kpi_label(kpi_name: str) -> str:
    kl = kpi_name.lower()
    for fragment, label in _KPI_LABELS.items():
        if fragment in kl:
            return label
    return kpi_name


def _is_anomalous(kpi_name: str, value: float) -> bool:
    kl = kpi_name.lower()
    for key, threshold in _THRESHOLDS.items():
        if key in kl:
            return float(value) >= threshold
    return False


# ---------------------------------------------------------------------------
# Core graph builder
# ---------------------------------------------------------------------------

def build_bank_graph(
    scenario: Dict[str, Any],
    metrics_df: pd.DataFrame,
    logs_df: pd.DataFrame,
) -> GraphBuilder:
    """
    Construct a GraphBuilder populated with real Bank telemetry signals.

    Args:
        scenario:    Bank scenario dict (from openrca_bank_loader)
        metrics_df:  windowed metric_container.csv rows
        logs_df:     windowed log_service.csv rows

    Returns:
        GraphBuilder instance ready for ContextRetriever
    """
    nx = _networkx()
    gb = GraphBuilder()
    topology = scenario["topology"]

    # 1. Ensure every service node exists with default attributes
    for svc in topology["services"]:
        gb._ensure_node(svc)

    # 2. Wire static topology edges
    for src, dst in topology["edges"]:
        gb.graph.add_edge(src, dst, latency=50)

    # 3. Populate nodes from real metric signals
    if not metrics_df.empty and "cmdb_id" in metrics_df.columns:
        for pod, pod_metrics in metrics_df.groupby("cmdb_id"):
            pod = str(pod)
            if pod not in gb.graph:
                gb._ensure_node(pod)
            node = gb.graph.nodes[pod]

            is_error = False
            events = []

            for kpi, kpi_rows in pod_metrics.groupby("kpi_name"):
                kpi = str(kpi)
                try:
                    vals = kpi_rows["value"].astype(float)
                except (ValueError, TypeError):
                    continue

                peak_val = float(vals.max())
                avg_val  = float(vals.mean())

                # Timestamp of the peak value
                try:
                    peak_ts_unix = float(
                        kpi_rows.loc[vals.idxmax(), "timestamp"]
                    )
                    ts_str = datetime.fromtimestamp(peak_ts_unix).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                except Exception:
                    ts_str = "unknown"

                anomalous = _is_anomalous(kpi, peak_val)
                if anomalous:
                    is_error = True

                # Only surface KPIs that are anomalous or meaningfully elevated
                if anomalous or avg_val > 50.0:
                    events.append({
                        "source":    "metric",
                        "kind":      kpi,
                        "timestamp": ts_str,
                        "summary":   f"{_kpi_label(kpi)}: {peak_val:.1f}",
                        "payload": {
                            "kpi_name":    kpi,
                            "peak_value":  round(peak_val, 2),
                            "avg_value":   round(avg_val, 2),
                            "anomalous":   anomalous,
                        },
                    })

            # Sort by peak value descending; keep top 8 per node
            events.sort(
                key=lambda e: e["payload"].get("peak_value", 0.0),
                reverse=True,
            )
            node["recent_events"].extend(events[:8])

            if is_error:
                nx.set_node_attributes(gb.graph, {pod: {"status": "error"}})

    # 4. Attach real log snippets as events
    _error_kws = {
        "error", "exception", "fail", "oom", "killed",
        "timeout", "refused", "lost", "drop", "warn",
        "outofmemory", "gc overhead",
    }

    if not logs_df.empty and "cmdb_id" in logs_df.columns:
        for pod, pod_logs in logs_df.groupby("cmdb_id"):
            pod = str(pod)
            if pod not in gb.graph:
                gb._ensure_node(pod)
            node = gb.graph.nodes[pod]

            error_events = []
            other_events = []

            for _, row in pod_logs.iterrows():
                val = str(row.get("value", ""))
                try:
                    ts_str = datetime.fromtimestamp(
                        float(row["timestamp"])
                    ).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    ts_str = "unknown"

                event = {
                    "source":    "log",
                    "kind":      str(row.get("log_name", "log")),
                    "timestamp": ts_str,
                    "summary":   val[:200],
                    "payload": {
                        "log_name": str(row.get("log_name", "")),
                        "value":    val[:300],
                    },
                }
                if any(kw in val.lower() for kw in _error_kws):
                    error_events.append(event)
                else:
                    other_events.append(event)

            # Prefer error-pattern logs; cap at 5 events per node
            selected = (error_events[:3] + other_events[:2])[:5]
            node["recent_events"].extend(selected)

    return gb
