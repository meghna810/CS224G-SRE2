"""
openrca_loader.py - DEPRECATED.

This file previously generated synthetic OTLP data from OpenRCA instruction
text, which meant the agent never saw real telemetry signals.

It has been replaced by:
  eval/openrca_bank_loader.py   - loads real Bank metric/log CSVs
  eval/openrca_graph_adapter.py - converts real telemetry → GraphBuilder
  eval/run_openrca_eval.py      - CLI runner for real-data evaluation

The load_openrca_scenarios() function below is retained for backwards
compatibility with any existing callers, but it now delegates to the
new real-data loader.
"""

import warnings

from eval.openrca_bank_loader import load_bank_scenarios


def load_openrca_scenarios(
    system: str = "Bank",
    max_cases: int = 5,
    task_index_offset: int = 10,
):
    warnings.warn(
        "load_openrca_scenarios() is deprecated. "
        "Use eval.openrca_bank_loader.load_bank_scenarios() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return load_bank_scenarios(data_dir=system, n=max_cases)
