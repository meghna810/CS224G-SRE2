"""
rcaeval_graph_adapter.py - Build a GraphBuilder from RE3-OB (RCAEval) telemetry.

Consumes the wide-format data.csv (metrics) and logs.csv (with stack traces)
for one RE3-OB case and populates a GraphBuilder instance with:

  1. All 12 Online Boutique services wired into the static topology
  2. Anomaly detection using dual approach:
       - Z-score: inject window vs. baseline window (inject − 30 min to − 15 min)
       - Static thresholds: CPU > 80 %, memory > 85 %, latency > 500 ms
  3. Real metric anomaly events attached per node
  4. Stack traces from logs detected and attached as high-priority "code_fault"
     events — this is the key advantage over the Bank adapter (code-level signals)
  5. Regular error-keyword log events attached as secondary signals

Input data format:
    data.csv  — wide-format: columns = [time, {svc}_{kpi}, {svc}_{kpi}, ...]
                timestamps are Unix seconds (UTC)
    logs.csv  — columns: [time, service_name, log_message]
                timestamps are Unix seconds (UTC)

All reads are chunked (50 k rows at a time) so large files never fully load
into memory.
"""

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from graph.graph_builder import GraphBuilder
from eval.rcaeval_code_fetcher import enrich_with_code_snippets

# Lazy import to avoid circular dependency
_nx = None


def _networkx():
    global _nx
    if _nx is None:
        import networkx as nx
        _nx = nx
    return _nx


# ---------------------------------------------------------------------------
# Known Online Boutique service names
# Sorted longest-first to avoid short-prefix false matches during column melt
# ---------------------------------------------------------------------------

_OB_SERVICES: List[str] = sorted(
    [
        "frontend",
        "adservice",
        "cartservice",
        "checkoutservice",
        "currencyservice",
        "emailservice",
        "paymentservice",
        "productcatalogservice",
        "recommendationservice",
        "shippingservice",
        "redis-cart",
        "loadgenerator",
    ],
    key=len,
    reverse=True,
)

# ---------------------------------------------------------------------------
# KPI anomaly thresholds (static fallback when baseline is unavailable)
# ---------------------------------------------------------------------------

_THRESHOLDS: Dict[str, float] = {
    "cpu":           80.0,   # %
    "mem":           85.0,   # %
    "memory":        85.0,
    "heap":          85.0,
    "latency":      500.0,   # ms
    "duration":     500.0,
    "error_rate":     0.01,  # fraction
    "error":          0.01,
    "disk":          90.0,
    "net_drop":       1.0,
    "packet_loss":    1.0,
}

# KPI name fragment → human-readable label
_KPI_LABELS: Dict[str, str] = {
    "cpu":      "CPU usage",
    "mem":      "memory usage",
    "memory":   "memory usage",
    "heap":     "JVM heap",
    "latency":  "latency",
    "duration": "request duration",
    "error":    "error rate",
    "disk":     "disk I/O",
    "net":      "network",
    "gc":       "GC activity",
    "thread":   "thread count",
}

# Error keywords for general log scanning
_ERROR_KEYWORDS: Set[str] = {
    "error", "exception", "fail", "fatal", "panic",
    "oom", "killed", "timeout", "refused", "lost",
    "drop", "warn", "outofmemory", "gc overhead",
    "connection refused", "null pointer", "nullpointer",
    "segfault", "abort", "crash",
}

# Stack-trace indicator patterns
_STACKTRACE_PATTERNS: List[str] = [
    r"Traceback \(most recent call last\)",   # Python
    r"\tat ",                                  # Java stack frame
    r"Exception in thread",                   # Java uncaught exception
    r'File ".*", line \d+',                   # Python file/line
    r"goroutine \d+ \[",                      # Go panic
    r"panic:",                                # Go panic
    r"at .*\.(java|go|py|rb):\d+",           # generic file:line
    r"caused by:",                            # Java chained exception
    r"\.\.\.(\d+ more)",                      # Java truncated trace
]
_STACKTRACE_RE = re.compile(
    "|".join(_STACKTRACE_PATTERNS), re.IGNORECASE
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _kpi_label(kpi_name: str) -> str:
    kl = kpi_name.lower()
    for fragment, label in _KPI_LABELS.items():
        if fragment in kl:
            return label
    return kpi_name


def _exceeds_threshold(kpi_name: str, value: float) -> bool:
    kl = kpi_name.lower()
    for key, threshold in _THRESHOLDS.items():
        if key in kl:
            return float(value) >= threshold
    return False


def _is_counter_kpi(kpi_name: str) -> bool:
    """Return True for monotonically-increasing counter KPIs (skip z-score for these)."""
    kl = kpi_name.lower()
    return any(w in kl for w in ("count", "total", "sum", "received", "sent"))


def _is_stacktrace_line(msg: str) -> bool:
    return bool(_STACKTRACE_RE.search(msg))


def _ts_to_str(ts_unix: float) -> str:
    try:
        return datetime.fromtimestamp(ts_unix, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Windowed CSV readers (chunked for memory efficiency)
# ---------------------------------------------------------------------------

def _load_windowed_wide_metrics(
    case_dir: str,
    start_ts: float,
    end_ts: float,
    chunk_size: int = 50_000,
) -> pd.DataFrame:
    """
    Read data.csv in chunks, keep rows in [start_ts, end_ts].
    Returns the raw wide-format DataFrame (columns: time, svc_kpi, ...).
    The `time` column holds Unix timestamps (UTC).
    """
    path = os.path.join(case_dir, "data.csv")
    if not os.path.exists(path):
        # Fallback: some dataset versions use metrics.csv
        alt = os.path.join(case_dir, "metrics.csv")
        if os.path.exists(alt):
            path = alt
        else:
            return pd.DataFrame()

    chunks = []
    for chunk in pd.read_csv(
        path, chunksize=chunk_size, low_memory=False,
        encoding="utf-8", encoding_errors="replace",
    ):
        if "time" not in chunk.columns:
            break
        filtered = chunk[(chunk["time"] >= start_ts) & (chunk["time"] <= end_ts)]
        if not filtered.empty:
            chunks.append(filtered)

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def _load_windowed_logs(
    case_dir: str,
    start_ts: float,
    end_ts: float,
    chunk_size: int = 50_000,
    per_service_cap: int = 200,
) -> pd.DataFrame:
    """
    Read logs.csv in chunks, keep rows in [start_ts, end_ts].
    Caps per-service rows to keep memory bounded.
    Expected columns: [time, service_name, log_message]
    """
    path = os.path.join(case_dir, "logs.csv")
    if not os.path.exists(path):
        return pd.DataFrame()

    chunks = []
    for chunk in pd.read_csv(
        path, chunksize=chunk_size, low_memory=False,
        encoding="utf-8", encoding_errors="replace",
    ):
        # Detect the numeric Unix-seconds time column.
        # Some datasets have:
        #   "time"      — numeric Unix seconds
        #   "timestamp" — numeric Unix nanoseconds (RCAEval RE3-OB format)
        # When both exist, prefer "timestamp" if "time" is non-numeric (HH:MM strings).
        time_col = None
        ns_scale = 1.0  # multiplier to convert stored value → Unix seconds
        if "timestamp" in chunk.columns:
            sample = pd.to_numeric(chunk["timestamp"].iloc[:5], errors="coerce")
            if sample.notna().any():
                time_col = "timestamp"
                # Nanoseconds if value > 1e12 (Unix ns since epoch >> Unix s since epoch)
                if float(sample.dropna().iloc[0]) > 1e12:
                    ns_scale = 1e-9
        if time_col is None and "time" in chunk.columns:
            sample = pd.to_numeric(chunk["time"].iloc[:5], errors="coerce")
            if sample.notna().any():
                time_col = "time"
        if time_col is None:
            break

        ts_numeric = pd.to_numeric(chunk[time_col], errors="coerce") * ns_scale
        mask = (ts_numeric >= start_ts) & (ts_numeric <= end_ts)
        filtered = chunk[mask].copy()
        if not filtered.empty:
            filtered["time"] = ts_numeric[mask].values
            if time_col != "time":
                filtered = filtered.drop(columns=[time_col], errors="ignore")
            chunks.append(filtered)

    if not chunks:
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)

    # Cap per-service rows
    for _sc in ("service_name", "container_name", "cmdb_id"):
        if _sc in df.columns:
            svc_col = _sc
            break
    else:
        svc_col = df.columns[1] if len(df.columns) >= 2 else None
    if svc_col:
        df = (
            df.groupby(svc_col, group_keys=False)
              .head(per_service_cap)
              .reset_index(drop=True)
        )

    return df


# ---------------------------------------------------------------------------
# Wide-format metric melt
# ---------------------------------------------------------------------------

def _melt_wide_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert wide-format data.csv to long format.

    Input columns:  [time, svc_kpi, svc_kpi, ...]
    Output columns: [time, service, kpi, value]

    Service extraction uses longest-prefix-first matching against known OB
    service names to prevent short names incorrectly matching longer ones.
    Columns not matching any known service are silently dropped.
    """
    if df.empty:
        return pd.DataFrame(columns=["time", "service", "kpi", "value"])

    # Build column → (service, kpi) mapping
    col_map: Dict[str, Tuple[str, str]] = {}
    unmatched: List[str] = []
    for col in df.columns:
        if col == "time":
            continue
        matched = False
        for svc in _OB_SERVICES:   # sorted longest-first
            prefix = svc + "_"
            if col.startswith(prefix):
                kpi = col[len(prefix):]
                col_map[col] = (svc, kpi)
                matched = True
                break
        if not matched:
            unmatched.append(col)

    if unmatched:
        # Warn once per batch (not per row)
        print(
            f"[re3_adapter] {len(unmatched)} metric columns not matched to any OB "
            f"service (dropped): {unmatched[:5]}{'...' if len(unmatched) > 5 else ''}"
        )

    if not col_map:
        return pd.DataFrame(columns=["time", "service", "kpi", "value"])

    # Vectorised melt via pd.melt then split
    metric_cols = list(col_map.keys())
    long = df[["time"] + metric_cols].melt(
        id_vars=["time"], value_vars=metric_cols,
        var_name="svc_kpi", value_name="value",
    )
    long["service"] = long["svc_kpi"].map(lambda c: col_map[c][0])
    long["kpi"]     = long["svc_kpi"].map(lambda c: col_map[c][1])
    long = long[["time", "service", "kpi", "value"]].dropna(subset=["value"])
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    return long.dropna(subset=["value"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

def _compute_anomaly_scores(
    inject_long: pd.DataFrame,
    baseline_long: pd.DataFrame,
) -> Dict[str, Dict[str, float]]:
    """
    For each (service, kpi) pair compute a z-score:
        z = (inject_mean − baseline_mean) / (baseline_std + 1e-6)

    Returns: {service: {kpi: z_score}}

    Counter KPIs (count/total/sum) are excluded from z-score computation.
    If the baseline has fewer than 10 rows, z-scores are not computed for
    that (service, kpi) pair (returns 0.0, so threshold-only detection applies).
    """
    scores: Dict[str, Dict[str, float]] = {}

    if inject_long.empty or baseline_long.empty:
        return scores

    for (svc, kpi), grp_inject in inject_long.groupby(["service", "kpi"]):
        if _is_counter_kpi(str(kpi)):
            continue

        grp_baseline = baseline_long[
            (baseline_long["service"] == svc) & (baseline_long["kpi"] == kpi)
        ]
        if len(grp_baseline) < 10:
            continue

        b_mean = float(grp_baseline["value"].mean())
        b_std  = float(grp_baseline["value"].std())
        i_mean = float(grp_inject["value"].mean())

        z = (i_mean - b_mean) / (b_std + 1e-6)
        scores.setdefault(str(svc), {})[str(kpi)] = round(z, 3)

    return scores


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_re3_graph(
    scenario: Dict[str, Any],
    fetch_code: bool = True,
) -> GraphBuilder:
    """
    Construct a GraphBuilder populated with RE3-OB telemetry signals.

    Reads data.csv and logs.csv from scenario["re3_case_dir"] using the
    ±15-min window defined by re3_load_start / re3_load_end.

    Steps:
      1. Wire all 12 Online Boutique services into the static topology.
      2. Load inject-window and baseline-window metrics; melt to long format.
      3. Compute z-score anomaly scores per (service, kpi).
      4. For each service: attach anomalous metric events; set status="error"
         if any KPI is anomalous (z > 2.0 or static threshold exceeded).
      5. Load inject-window logs.
      6. For each service: scan for stack traces → high-priority code_fault
         events; scan for error keywords → error events.
      7. (If fetch_code=True) For each service with stack traces, fetch the
         actual source code lines from GitHub and attach as source_code events
         so the LLM sees the exact faulty code, not just the error message.
      8. Return the populated GraphBuilder.

    Args:
        scenario:   RE3-OB scenario dict (from rcaeval_loader).
        fetch_code: Whether to fetch source code snippets from GitHub.
                    Disable with --no-code flag or for offline testing.

    Returns:
        GraphBuilder ready for ContextRetriever.
    """
    nx  = _networkx()
    gb  = GraphBuilder()
    top = scenario["topology"]

    case_dir    = scenario["re3_case_dir"]
    load_start  = scenario["re3_load_start"]
    load_end    = scenario["re3_load_end"]
    inject_time = scenario["re3_inject_time"]

    start_ts    = load_start.timestamp()
    end_ts      = load_end.timestamp()
    # Baseline: 15–30 min before inject (normal behaviour window)
    baseline_start_ts = (inject_time.timestamp()) - 30 * 60
    baseline_end_ts   = (inject_time.timestamp()) - 15 * 60

    # 1. Wire topology
    for svc in top["services"]:
        gb._ensure_node(svc)
    for src, dst in top["edges"]:
        gb.graph.add_edge(src, dst, latency=50)

    # 2. Load metrics
    inject_wide   = _load_windowed_wide_metrics(case_dir, start_ts, end_ts)
    baseline_wide = _load_windowed_wide_metrics(
        case_dir, baseline_start_ts, baseline_end_ts
    )
    inject_long   = _melt_wide_metrics(inject_wide)
    baseline_long = _melt_wide_metrics(baseline_wide)

    # 3. Z-score anomaly scores
    z_scores = _compute_anomaly_scores(inject_long, baseline_long)

    # 4. Attach metric events per service
    if not inject_long.empty:
        for svc, svc_df in inject_long.groupby("service"):
            svc = str(svc)
            if svc not in gb.graph:
                gb._ensure_node(svc)
            node = gb.graph.nodes[svc]

            is_error  = False
            events: List[Dict[str, Any]] = []

            for kpi, kpi_df in svc_df.groupby("kpi"):
                kpi = str(kpi)
                try:
                    vals = kpi_df["value"].astype(float)
                except (ValueError, TypeError):
                    continue

                peak_val = float(vals.max())
                avg_val  = float(vals.mean())

                try:
                    peak_ts_unix = float(
                        kpi_df.loc[vals.idxmax(), "time"]
                    )
                    ts_str = _ts_to_str(peak_ts_unix)
                except Exception:
                    ts_str = "unknown"

                z = z_scores.get(svc, {}).get(kpi, 0.0)
                threshold_hit = _exceeds_threshold(kpi, peak_val)
                z_hit = z > 2.0

                anomalous = threshold_hit or z_hit
                if anomalous:
                    is_error = True

                if anomalous or avg_val > 50.0:
                    if z_hit:
                        summary = (
                            f"{_kpi_label(kpi)}: {peak_val:.1f} "
                            f"(z={z:.1f} above baseline avg {float(baseline_long[baseline_long['kpi'] == kpi]['value'].mean()) if not baseline_long.empty else 0.0:.1f})"
                        )
                    else:
                        summary = f"{_kpi_label(kpi)}: {peak_val:.1f}"

                    events.append({
                        "source":    "metric",
                        "kind":      kpi,
                        "timestamp": ts_str,
                        "summary":   summary,
                        "payload": {
                            "kpi_name":      kpi,
                            "peak_value":    round(peak_val, 2),
                            "avg_value":     round(avg_val, 2),
                            "z_score":       round(z, 3),
                            "anomalous":     anomalous,
                        },
                    })

            # Keep top 6 anomalous/elevated KPIs per node (by |z_score| then peak_value)
            events.sort(
                key=lambda e: (
                    abs(e["payload"].get("z_score", 0.0)),
                    e["payload"].get("peak_value", 0.0),
                ),
                reverse=True,
            )
            node["recent_events"].extend(events[:6])

            if is_error:
                nx.set_node_attributes(gb.graph, {svc: {"status": "error"}})

    # 5. Load logs
    logs_df = _load_windowed_logs(case_dir, start_ts, end_ts)

    # 6. Attach log events per service
    if not logs_df.empty:
        # Detect service column name (handle multiple naming conventions)
        for _sc in ("service_name", "container_name", "cmdb_id"):
            if _sc in logs_df.columns:
                svc_col = _sc
                break
        else:
            svc_col = logs_df.columns[1] if len(logs_df.columns) >= 3 else None
        msg_col = "log_message" if "log_message" in logs_df.columns else (
            "message" if "message" in logs_df.columns else (
                logs_df.columns[2] if len(logs_df.columns) >= 3 else None
            )
        )
        time_col = "time" if "time" in logs_df.columns else "timestamp"

        if svc_col and msg_col:
            for svc, svc_logs in logs_df.groupby(svc_col):
                svc = str(svc)
                if svc not in gb.graph:
                    gb._ensure_node(svc)
                node = gb.graph.nodes[svc]

                trace_events: List[Dict[str, Any]] = []
                error_events: List[Dict[str, Any]] = []

                for _, row in svc_logs.iterrows():
                    msg = str(row.get(msg_col, ""))
                    try:
                        ts_str = _ts_to_str(float(row[time_col]))
                    except Exception:
                        ts_str = "unknown"

                    if _is_stacktrace_line(msg):
                        # Extract first meaningful line as summary
                        first_line = msg.split("\n")[0][:200]
                        trace_events.append({
                            "source":    "log",
                            "kind":      "code_fault",
                            "timestamp": ts_str,
                            "summary":   first_line,
                            "payload": {
                                "log_message":   msg[:500],
                                "is_stacktrace": True,
                            },
                        })
                    elif any(kw in msg.lower() for kw in _ERROR_KEYWORDS):
                        error_events.append({
                            "source":    "log",
                            "kind":      "error_log",
                            "timestamp": ts_str,
                            "summary":   msg[:200],
                            "payload": {
                                "log_message":   msg[:500],
                                "is_stacktrace": False,
                            },
                        })

                # Stack traces first (code-level signal), then error logs
                selected = (trace_events[:5] + error_events[:5])[:10]
                node["recent_events"].extend(selected)

                # Stack traces on this node → mark as error
                if trace_events:
                    nx.set_node_attributes(gb.graph, {svc: {"status": "error"}})

                # Fetch actual source code from GitHub for stack-traced services
                # so the LLM sees the faulty code, not just the error message
                if fetch_code and trace_events:
                    inject_ts = _ts_to_str(inject_time.timestamp())
                    code_events = enrich_with_code_snippets(
                        trace_events=trace_events,
                        service=svc,
                        inject_ts=inject_ts,
                    )
                    # Source code events go right after trace events (before metric events)
                    node["recent_events"].extend(code_events)

    return gb
