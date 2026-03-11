"""
rcaeval_loader.py - Load RE3-OB (Online Boutique) cases from the RCAEval dataset.

RE3 injects code-level faults into microservices, producing stack traces in logs
alongside metric anomalies. This gives richer signals than the Bank dataset (which
has only infrastructure-level faults).

The five fault types (F1–F5) map to code-level issues:
  F1 — incorrect parameter passed to function
  F2 — missing function call causing incorrect behaviour
  F3 — missing exception handler causing unhandled error
  F4 — incorrect control flow / complex interactions
  F5 — wrong return value causing downstream logic failure

Data layout expected:
    <data_dir>/                    (default: data/RE3-OB)
        cartservice_F1_1/
            data.csv              wide-format metrics: [time, svc_kpi, ...]
            logs.csv              [time, service_name, log_message]
            inject_time.txt       single Unix timestamp (UTC)
        cartservice_F1_2/
        ...
        checkoutservice_F3_12/
        ...

Usage:
    from eval.rcaeval_loader import load_re3_scenarios
    scenarios = load_re3_scenarios(data_dir="data/RE3-OB", n=30)

Download:
    git clone https://github.com/phamquiluan/RCAEval /tmp/RCAEval
    cd /tmp/RCAEval && pip install -e .
    python main.py --download --dataset RE3-OB
    cp -r data/RE3-OB <project_root>/data/RE3-OB
"""

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Online Boutique topology (12 services + edges)
# Source: https://github.com/GoogleCloudPlatform/microservices-demo
# ---------------------------------------------------------------------------

RE3_OB_TOPOLOGY: Dict[str, Any] = {
    "services": [
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
    "edges": [
        ("frontend",             "adservice"),
        ("frontend",             "cartservice"),
        ("frontend",             "checkoutservice"),
        ("frontend",             "currencyservice"),
        ("frontend",             "productcatalogservice"),
        ("frontend",             "recommendationservice"),
        ("frontend",             "shippingservice"),
        ("checkoutservice",      "cartservice"),
        ("checkoutservice",      "currencyservice"),
        ("checkoutservice",      "emailservice"),
        ("checkoutservice",      "paymentservice"),
        ("checkoutservice",      "productcatalogservice"),
        ("checkoutservice",      "shippingservice"),
        ("recommendationservice","productcatalogservice"),
        ("cartservice",          "redis-cart"),
    ],
}

# ---------------------------------------------------------------------------
# Fault type metadata
# ---------------------------------------------------------------------------

_FAULT_TYPE_DIFFICULTY: Dict[str, str] = {
    "F1": "easy",     # incorrect params — immediate and obvious error
    "F2": "medium",   # missing function call — silent misbehaviour
    "F3": "medium",   # missing exception handler — propagation depends on caller
    "F4": "hard",     # wrong control flow / complex interactions
    "F5": "easy",     # wrong return value — usually caught fast by callers
}

# Maps fault type → task_index (controls scoring criteria in evaluate.py).
# task_3 = component + reason (no datetime); task_6 = same but medium complexity.
_FAULT_TYPE_TASK_INDEX: Dict[str, str] = {
    "F1": "task_3",
    "F2": "task_6",
    "F3": "task_6",
    "F4": "task_6",
    "F5": "task_3",
}

# Short ground-truth reason phrases used for cosine-similarity scoring.
_FAULT_TYPE_REASON: Dict[str, str] = {
    "F1": "incorrect parameter passed to function",
    "F2": "missing function call causing incorrect behaviour",
    "F3": "missing exception handler causing unhandled error propagation",
    "F4": "wrong return value causing downstream logic failure",
    "F5": "wrong return value causing downstream logic failure",
}

# Human-readable descriptions of each fault type (for scenario title/description)
_FAULT_TYPE_DESCRIPTION: Dict[str, str] = {
    "F1": "incorrect parameter",
    "F2": "missing function call",
    "F3": "missing exception handler",
    "F4": "incorrect control flow",
    "F5": "wrong return value",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_case_dir_name(
    dir_name: str,
) -> Optional[Tuple[str, str, int]]:
    """
    Parse "{service}_{fault_type}_{case_num}" → (service, fault_type, case_num).
    Returns None if the pattern does not match.

    Examples:
        "cartservice_F1_1"        → ("cartservice", "F1", 1)
        "checkoutservice_F3_12"   → ("checkoutservice", "F3", 12)
        "redis-cart_F2_3"         → ("redis-cart", "F2", 3)
    """
    # Allow service names with hyphens; fault type is F1–F5
    m = re.fullmatch(r"(.+?)_(F[1-5])_(\d+)", dir_name)
    if m is None:
        return None
    service   = m.group(1)
    fault_type = m.group(2)
    case_num   = int(m.group(3))
    return service, fault_type, case_num


def _read_inject_time(case_dir: str) -> Optional[datetime]:
    """
    Read inject_time.txt (single Unix integer, UTC) → UTC-aware datetime.
    Returns None on any read / parse failure.
    """
    path = os.path.join(case_dir, "inject_time.txt")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            ts = int(f.read().strip())
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except (ValueError, OSError):
        return None


def _build_scoring_points(component: str, reason: str) -> str:
    """
    Build OpenRCA-format scoring_points string for RE3 cases.
    Deliberately omits the datetime criterion — the agent's datetime prediction
    is unreliable and excluding it avoids score distortion.
    """
    return (
        f"The only predicted root cause component is {component}\n"
        f"The only predicted root cause reason is {reason}"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_re3_scenarios(
    data_dir: str = "data/RE3-OB",
    n: Optional[int] = None,
    fault_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Discover all RE3-OB case directories under data_dir and return scenario
    dicts compatible with run_rcaeval_eval.py.

    Args:
        data_dir:    Path to the RE3-OB directory (absolute or relative to
                     project root). Default: "data/RE3-OB".
        n:           Max scenarios to return (None = all).
        fault_types: Filter to specific fault types, e.g. ["F1", "F3"].
                     None = all five types.

    Returns:
        List of scenario dicts sorted by (fault_type, service, case_num).

    Standard scenario fields (shared with Bank / synthetic benchmarks):
        id, task_index, difficulty, title, description, topology,
        observed_service, fault_start_ts, ground_truth, scoring_points

    RE3-specific extra fields (consumed by rcaeval_graph_adapter):
        re3_case_dir    — absolute path to the case directory
        re3_inject_time — UTC-aware datetime of fault injection
        re3_load_start  — inject_time − 15 min
        re3_load_end    — inject_time + 15 min
        re3_fault_type  — e.g. "F1"
    """
    # Resolve path relative to project root if not absolute
    if not os.path.isabs(data_dir):
        data_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", data_dir)
        )

    if not os.path.isdir(data_dir):
        print(
            f"[re3_loader] Directory not found: {data_dir}\n"
            f"  Download RE3-OB first — see eval/rcaeval_loader.py docstring."
        )
        return []

    # Discover and parse case directories
    parsed: List[Tuple[str, str, int, str]] = []   # (service, fault_type, case_num, abs_path)
    for entry in os.listdir(data_dir):
        abs_path = os.path.join(data_dir, entry)
        if not os.path.isdir(abs_path):
            continue
        result = _parse_case_dir_name(entry)
        if result is None:
            continue
        service, fault_type, case_num = result

        # Apply fault_types filter
        if fault_types is not None and fault_type not in fault_types:
            continue

        # Must have inject_time.txt
        if _read_inject_time(abs_path) is None:
            print(f"[re3_loader] Skipping {entry}: inject_time.txt missing or unreadable")
            continue

        parsed.append((service, fault_type, case_num, abs_path))

    if not parsed:
        print(f"[re3_loader] No valid RE3-OB cases found in {data_dir}")
        return []

    # Sort for deterministic ordering
    parsed.sort(key=lambda x: (x[1], x[0], x[2]))   # (fault_type, service, case_num)

    if n is not None:
        parsed = parsed[:n]

    # Build scenario dicts
    scenarios: List[Dict[str, Any]] = []
    for i, (service, fault_type, case_num, abs_path) in enumerate(parsed):
        inject_time = _read_inject_time(abs_path)   # already validated above
        load_start  = inject_time - timedelta(minutes=15)
        load_end    = inject_time + timedelta(minutes=15)

        reason      = _FAULT_TYPE_REASON[fault_type]
        description = (
            _FAULT_TYPE_DESCRIPTION[fault_type]
        )

        scenario: Dict[str, Any] = {
            "id":              f"re3_{service}_{fault_type}_{case_num:03d}",
            "task_index":      _FAULT_TYPE_TASK_INDEX[fault_type],
            "difficulty":      _FAULT_TYPE_DIFFICULTY[fault_type],
            "title":           f"[RE3-OB] {service} — {fault_type} ({description})",
            "description": (
                f"Code fault injected into {service} (fault type {fault_type}: "
                f"{description}) at {inject_time.strftime('%Y-%m-%d %H:%M:%S')} UTC. "
                f"Alert expected at frontend (Online Boutique entry point)."
            ),
            "topology":        RE3_OB_TOPOLOGY,
            "observed_service": "frontend",
            "fault_start_ts":  inject_time,
            "ground_truth": {
                "root_cause_component": service,
                "root_cause_reason":    reason,
            },
            "scoring_points":  _build_scoring_points(service, reason),
            # RE3-specific fields consumed by rcaeval_graph_adapter
            "re3_case_dir":    abs_path,
            "re3_inject_time": inject_time,
            "re3_load_start":  load_start,
            "re3_load_end":    load_end,
            "re3_fault_type":  fault_type,
        }
        scenarios.append(scenario)

    fault_type_counts = {}
    for s in scenarios:
        ft = s["re3_fault_type"]
        fault_type_counts[ft] = fault_type_counts.get(ft, 0) + 1
    services_seen = len({s["ground_truth"]["root_cause_component"] for s in scenarios})

    print(
        f"[re3_loader] Loaded {len(scenarios)} RE3-OB scenarios "
        f"({services_seen} services, fault types: "
        f"{', '.join(f'{k}×{v}' for k, v in sorted(fault_type_counts.items()))})"
    )
    return scenarios
