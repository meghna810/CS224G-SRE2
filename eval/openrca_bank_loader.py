"""
openrca_bank_loader.py - Load and sample real Bank telemetry from the OpenRCA dataset.

Replaces the old openrca_loader.py which generated synthetic OTLP data from
instruction text. This version reads the actual metric and log CSV files from
the Bank/ folder in memory-efficient time-windowed chunks, then constructs
scenario dicts compatible with run_openrca_eval.py.

Data layout expected:
    <data_dir>/
        query.csv                                  (~136 incident questions)
        record.csv                                 (~137 ground-truth fault records)
        telemetry/
            2021_03_04/
                metric/metric_container.csv        [timestamp, cmdb_id, kpi_name, value]
                log/log_service.csv                [log_id, timestamp, cmdb_id, log_name, value]
            2021_03_06/ ...

Usage:
    from eval.openrca_bank_loader import load_bank_scenarios
    scenarios = load_bank_scenarios(data_dir="Bank", n=27)
"""

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd

# All timestamps in the Bank telemetry CSVs are Unix seconds (UTC).
# All datetimes in query.csv / record.csv are expressed in UTC+8 (CST).
# We attach this timezone when converting parsed datetimes to Unix timestamps.
_UTC8 = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# Bank service topology (static, inferred from domain knowledge)
#
# The Bank system is a Java-based banking platform on Kubernetes:
#   apache (web tier) → Tomcat (app tier) → MySQL/Redis/MG/IG (data tier)
# ---------------------------------------------------------------------------

BANK_TOPOLOGY: Dict[str, Any] = {
    "services": [
        "apache01", "apache02",
        "Tomcat01", "Tomcat02", "Tomcat03", "Tomcat04",
        "Mysql01",  "Mysql02",
        "Redis01",  "Redis02",
        "MG01",     "MG02",
        "IG01",     "IG02",
    ],
    "edges": [
        ("apache01", "Tomcat01"), ("apache01", "Tomcat02"),
        ("apache01", "Tomcat03"), ("apache01", "Tomcat04"),
        ("apache02", "Tomcat01"), ("apache02", "Tomcat02"),
        ("apache02", "Tomcat03"), ("apache02", "Tomcat04"),
        ("Tomcat01", "Mysql01"),  ("Tomcat01", "Mysql02"),
        ("Tomcat01", "Redis01"),  ("Tomcat01", "Redis02"),
        ("Tomcat01", "MG01"),     ("Tomcat01", "MG02"),
        ("Tomcat02", "Mysql01"),  ("Tomcat02", "Mysql02"),
        ("Tomcat02", "Redis01"),  ("Tomcat02", "Redis02"),
        ("Tomcat02", "MG01"),     ("Tomcat02", "MG02"),
        ("Tomcat03", "Mysql01"),  ("Tomcat03", "Mysql02"),
        ("Tomcat03", "Redis01"),  ("Tomcat03", "Redis02"),
        ("Tomcat03", "MG01"),     ("Tomcat03", "MG02"),
        ("Tomcat04", "Mysql01"),  ("Tomcat04", "Mysql02"),
        ("Tomcat04", "Redis01"),  ("Tomcat04", "Redis02"),
        ("Tomcat04", "MG01"),     ("Tomcat04", "MG02"),
        ("MG01", "IG01"),         ("MG01", "IG02"),
        ("MG02", "IG01"),         ("MG02", "IG02"),
    ],
}

# ---------------------------------------------------------------------------
# Failure type → difficulty tier mapping
# ---------------------------------------------------------------------------
_DIFFICULTY_MAP: Dict[str, str] = {
    "high CPU usage":               "medium",
    "network packet loss":          "medium",
    "network latency":              "medium",
    "high disk I/O read usage":     "medium",
    "high memory usage":            "hard",
    "JVM Out of Memory (OOM) Heap": "hard",
    "high disk space usage":        "easy",
    "high JVM CPU load":            "hard",
}

# ---------------------------------------------------------------------------
# How many incidents to select per failure type (total ≈ 27)
# ---------------------------------------------------------------------------
_SELECTION_QUOTA: Dict[str, int] = {
    "high CPU usage":               5,
    "network packet loss":          5,
    "network latency":              4,
    "high disk I/O read usage":     4,
    "high memory usage":            3,
    "JVM Out of Memory (OOM) Heap": 2,
    "high disk space usage":        2,
    "high JVM CPU load":            2,
}


# ---------------------------------------------------------------------------
# Instruction parsing helpers
# ---------------------------------------------------------------------------

def _parse_time_range_from_instruction(
    instruction: str,
) -> Optional[Tuple[datetime, datetime]]:
    """
    Extract the incident time window from an OpenRCA Bank instruction.

    Handles patterns like:
      "On March 4, 2021, within the time range of 14:30 to 15:00"
      "On March 4, 2021, between 18:00 and 18:30"
      "from March 6, 2021, from 23:30 to March 7, 2021, at 00:00"  (midnight cross)

    Returns (start_dt, end_dt) as naive datetimes (Bank data is UTC+8).
    Returns None if parsing fails.
    """
    # --- Handle midnight-crossing format first ---
    # "from March 6, 2021, from 23:30 to March 7, 2021, at 00:00"
    midnight_m = re.search(
        r'(\w+ \d+,\s*\d{4}),?\s+(?:from|at)\s+(\d{1,2}:\d{2})\s+to\s+'
        r'(\w+ \d+,\s*\d{4}),?\s+(?:at|from)\s+(\d{1,2}:\d{2})',
        instruction,
    )
    if midnight_m:
        try:
            d1 = datetime.strptime(midnight_m.group(1).strip(), "%B %d, %Y")
            d2 = datetime.strptime(midnight_m.group(3).strip(), "%B %d, %Y")
            t1 = datetime.strptime(midnight_m.group(2), "%H:%M").time()
            t2 = datetime.strptime(midnight_m.group(4), "%H:%M").time()
            return datetime.combine(d1.date(), t1), datetime.combine(d2.date(), t2)
        except ValueError:
            pass

    # --- Standard single-date format ---
    date_m = re.search(r"(\w+ \d+,\s*\d{4})", instruction)
    time_m = re.search(
        r"(?:from|between|time range of)\s+(\d{1,2}:\d{2})\s+(?:to|and)\s+(\d{1,2}:\d{2})",
        instruction,
    )

    if not date_m or not time_m:
        return None

    try:
        date = datetime.strptime(date_m.group(1).strip(), "%B %d, %Y")
        start_t = datetime.strptime(time_m.group(1), "%H:%M").time()
        end_t   = datetime.strptime(time_m.group(2), "%H:%M").time()
        start_dt = datetime.combine(date.date(), start_t)
        end_dt   = datetime.combine(date.date(), end_t)
        if end_dt <= start_dt:          # safety: shouldn't happen after midnight fix
            end_dt += timedelta(days=1)
        return start_dt, end_dt
    except ValueError:
        return None


def _match_record(
    instruction: str,
    record_df: pd.DataFrame,
) -> Optional[pd.Series]:
    """
    Find the single record.csv row whose datetime falls within the
    instruction's time window (OpenRCA guarantees one failure per window).
    Returns None if zero or multiple matches are found.
    """
    time_range = _parse_time_range_from_instruction(instruction)
    if time_range is None:
        return None
    start_dt, end_dt = time_range

    matches = []
    for _, row in record_df.iterrows():
        try:
            row_dt = datetime.strptime(str(row["datetime"]).strip(), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if start_dt <= row_dt <= end_dt:
            matches.append(row)

    return matches[0] if len(matches) == 1 else None


# ---------------------------------------------------------------------------
# Windowed telemetry readers (memory-efficient via chunked CSV reads)
# ---------------------------------------------------------------------------

def _load_windowed_metrics(
    date_dir: str,
    window_start: datetime,
    window_end: datetime,
    chunk_size: int = 50_000,
) -> pd.DataFrame:
    """
    Load metric_container.csv rows within [window_start, window_end].

    Reads in 50k-row chunks (≈4 MB each) so the full 86 MB file is never
    fully resident in memory; a 60-min window typically yields ~3-5 MB.
    """
    path = os.path.join(date_dir, "metric", "metric_container.csv")
    if not os.path.exists(path):
        return pd.DataFrame()

    # Bank datetimes are UTC+8 — attach timezone before converting to Unix ts
    start_ts = window_start.replace(tzinfo=_UTC8).timestamp()
    end_ts   = window_end.replace(tzinfo=_UTC8).timestamp()
    chunks = []

    for chunk in pd.read_csv(path, chunksize=chunk_size, low_memory=False):
        filtered = chunk[
            (chunk["timestamp"] >= start_ts) & (chunk["timestamp"] <= end_ts)
        ]
        if not filtered.empty:
            chunks.append(filtered)

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def _load_windowed_logs(
    date_dir: str,
    window_start: datetime,
    window_end: datetime,
    chunk_size: int = 50_000,
) -> pd.DataFrame:
    """
    Load log_service.csv rows within [window_start, window_end].

    The full log file is ~214 MB; a 60-min window is ~9 MB.
    Per-pod output is capped at 100 rows to avoid bloating LLM context.
    """
    path = os.path.join(date_dir, "log", "log_service.csv")
    if not os.path.exists(path):
        return pd.DataFrame()

    start_ts = window_start.replace(tzinfo=_UTC8).timestamp()
    end_ts   = window_end.replace(tzinfo=_UTC8).timestamp()
    chunks = []

    for chunk in pd.read_csv(path, chunksize=chunk_size, low_memory=False):
        filtered = chunk[
            (chunk["timestamp"] >= start_ts) & (chunk["timestamp"] <= end_ts)
        ]
        if not filtered.empty:
            chunks.append(filtered)

    if not chunks:
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    # Cap per-pod log volume so the LLM context stays manageable
    return (
        df.groupby("cmdb_id", group_keys=False)
          .head(100)
          .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_bank_scenarios(
    data_dir: str = "Bank",
    n: int = 27,
) -> List[Dict[str, Any]]:
    """
    Select up to `n` diverse Bank incidents and return them as scenario dicts
    compatible with run_openrca_eval.py.

    Selection is stratified by failure type (see _SELECTION_QUOTA) and
    spread across the 7 OpenRCA task types for diversity.

    Each returned scenario includes:
      - Standard fields shared with synthetic benchmark (id, task_index,
        difficulty, title, topology, scoring_points, ground_truth, ...)
      - Bank-specific loading fields:
          bank_date_dir   : path to the telemetry date folder
          bank_load_start : window start for telemetry reads (fault_ts − 30 min)
          bank_load_end   : window end  for telemetry reads (fault_ts + 30 min)

    Telemetry is NOT loaded here — it is loaded lazily per-incident in
    run_openrca_eval.py so memory stays bounded (~15 MB peak at any time).
    """
    # Resolve path relative to project root if not absolute
    if not os.path.isabs(data_dir):
        data_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", data_dir)
        )

    query_path  = os.path.join(data_dir, "query.csv")
    record_path = os.path.join(data_dir, "record.csv")

    if not os.path.exists(query_path) or not os.path.exists(record_path):
        print(
            f"[bank_loader] Bank CSVs not found at {data_dir}/\n"
            f"  Expected: query.csv and record.csv"
        )
        return []

    query_df  = pd.read_csv(query_path)
    record_df = pd.read_csv(record_path)

    # --- Build a matched pool: (query_row, record_row, window_start, window_end) ---
    pool: List[Tuple] = []
    for _, q_row in query_df.iterrows():
        rec = _match_record(str(q_row["instruction"]), record_df)
        if rec is None:
            continue
        time_range = _parse_time_range_from_instruction(str(q_row["instruction"]))
        if time_range is None:
            continue
        pool.append((q_row, rec, time_range[0], time_range[1]))

    if not pool:
        print("[bank_loader] No query rows could be matched to record rows.")
        return []

    # --- Stratified selection by failure type, spread across task types ---
    type_pool: Dict[str, list] = {}
    for entry in pool:
        reason = str(entry[1]["reason"])
        type_pool.setdefault(reason, []).append(entry)

    selected: List[Tuple] = []
    for failure_type, quota in _SELECTION_QUOTA.items():
        bucket = type_pool.get(failure_type, [])
        if not bucket:
            continue
        # Sort by task_index to spread across task types (task_1 … task_7)
        bucket_sorted = sorted(bucket, key=lambda x: str(x[0]["task_index"]))
        step  = max(1, len(bucket_sorted) // quota)
        picks = bucket_sorted[::step][:quota]
        selected.extend(picks)

    selected = selected[:n]

    # --- Build scenario dicts ---
    telemetry_dir = os.path.join(data_dir, "telemetry")
    scenarios: List[Dict[str, Any]] = []

    for i, (q_row, rec, window_start, window_end) in enumerate(selected):
        task_index     = str(q_row["task_index"])
        instruction    = str(q_row["instruction"])
        scoring_points = str(q_row["scoring_points"])
        component      = str(rec["component"])
        reason         = str(rec["reason"])

        try:
            fault_dt = datetime.strptime(
                str(rec["datetime"]).strip(), "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            fault_dt = window_start

        # Telemetry folder name (e.g. 2021-03-04 → 2021_03_04)
        date_folder  = fault_dt.strftime("%Y_%m_%d")
        date_dir_path = os.path.join(telemetry_dir, date_folder)

        # Load window: ±30 min around the fault for richer signal context
        load_start = fault_dt - timedelta(minutes=30)
        load_end   = fault_dt + timedelta(minutes=30)

        scenario: Dict[str, Any] = {
            "id":           f"bank_{i + 1:03d}",
            "task_index":   task_index,
            "difficulty":   _DIFFICULTY_MAP.get(reason, "medium"),
            "title":        f"[Bank] {component} — {reason}",
            "description":  instruction,
            "topology":     BANK_TOPOLOGY,
            "observed_service": "apache01",      # alert entry point (web tier)
            "fault_start_ts": fault_dt.replace(tzinfo=timezone.utc),
            "ground_truth": {
                "root_cause_component": component,
                "root_cause_reason":    reason,
            },
            "scoring_points": scoring_points,
            # Bank-specific fields consumed by run_openrca_eval.py
            "bank_date_dir":    date_dir_path,
            "bank_load_start":  load_start,
            "bank_load_end":    load_end,
        }
        scenarios.append(scenario)

    failure_types = len({s["ground_truth"]["root_cause_reason"] for s in scenarios})
    print(
        f"[bank_loader] Selected {len(scenarios)} Bank scenarios "
        f"({failure_types} failure types, "
        f"{len({s['task_index'] for s in scenarios})} task types)"
    )
    return scenarios
