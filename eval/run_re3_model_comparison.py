"""
run_re3_model_comparison.py - Compare LLM models on the RE3-OB benchmark.

Runs run_rcaeval_eval's benchmark loop once per model and saves results under
eval/results/ with model-named filenames (e.g. re3_gemini_predictions.csv).
Prints a side-by-side comparison table at the end.

Models evaluated:
  gemini        — Gemini 2.5 Flash  (requires GEMINI_API_KEY)
  claude-sonnet — Claude Sonnet 4.6 (requires ANTHROPIC_API_KEY)
  claude-opus   — Claude Opus 4.6   (requires ANTHROPIC_API_KEY)

Usage
-----
# Full comparison (all models, all cases):
    python eval/run_re3_model_comparison.py

# Quick sanity check — 5 cases per model:
    python eval/run_re3_model_comparison.py --n 5

# Specific fault types only:
    python eval/run_re3_model_comparison.py --fault-types F1 F3

# Skip models you don't have API keys for:
    python eval/run_re3_model_comparison.py --models gemini claude-sonnet

# Custom data directory:
    python eval/run_re3_model_comparison.py --data-dir /path/to/RE3-OB

Data prerequisite:
    git clone https://github.com/phamquiluan/RCAEval /tmp/RCAEval
    cd /tmp/RCAEval && pip install -e .
    python main.py --download --dataset RE3-OB
    cp -r data/RE3-OB <project_root>/data/RE3-OB
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eval.rcaeval_loader import load_re3_scenarios
from eval.run_rcaeval_eval import run_re3_benchmark
from eval.benchmark import results_to_prediction_csv, scenarios_to_query_csv
from eval.evaluate import file_evaluate, report as print_report
from llm_integration.client import get_client, MockClient

_DEFAULT_MODELS = ["gemini", "claude-sonnet", "claude-opus"]

# Map CLI model name → short label used in output filenames
_MODEL_LABEL = {
    "gemini":        "gemini",
    "claude-sonnet": "claude_sonnet",
    "claude-opus":   "claude_opus",
}


def _run_one_model(
    model: str,
    scenarios: list,
    results_dir: str,
    fetch_code: bool,
    verbose: bool,
) -> dict:
    """Run the full RE3-OB benchmark for one model; return summary dict."""
    label = _MODEL_LABEL.get(model, model.replace("-", "_"))
    pred_csv   = os.path.join(results_dir, f"re3_{label}_predictions.csv")
    query_csv  = os.path.join(results_dir, f"re3_{label}_query.csv")
    report_csv = os.path.join(results_dir, f"re3_{label}_report.csv")

    print(f"\n{'═' * 60}")
    print(f"  Model: {model}")
    print(f"{'═' * 60}")

    try:
        llm_client = get_client(model)
        print(f"  LLM client initialised: {model}")
    except Exception as exc:
        print(f"  WARNING: Could not initialise '{model}': {exc}")
        print(f"  Skipping this model.")
        return {"model": model, "skipped": True, "reason": str(exc)}

    results = run_re3_benchmark(
        scenarios=scenarios,
        llm_client=llm_client,
        output_csv=None,
        verbose=verbose,
        fetch_code=fetch_code,
    )

    os.makedirs(results_dir, exist_ok=True)
    results_to_prediction_csv(results, pred_csv)
    scenarios_to_query_csv(scenarios, query_csv)

    try:
        file_evaluate(pred_csv, query_csv, report_csv)
    except Exception as exc:
        print(f"  [warn] Report generation failed: {exc}")
        report_csv = None

    # Compute summary stats
    scores  = [r["score"] for r in results]
    n_total = len(scores)
    n_pass  = sum(1 for s in scores if s == 1.0)
    avg     = sum(scores) / n_total if n_total else 0.0

    print(f"\n  Artifacts saved:")
    print(f"    Predictions : {pred_csv}")
    print(f"    Query/GT    : {query_csv}")
    if report_csv:
        print(f"    Report      : {report_csv}")

    return {
        "model":   model,
        "skipped": False,
        "n_total": n_total,
        "n_pass":  n_pass,
        "avg":     avg,
        "pred_csv":   pred_csv,
        "query_csv":  query_csv,
        "report_csv": report_csv,
    }


def _print_comparison_table(summaries: list) -> None:
    w = 18
    print(f"\n{'═' * 60}")
    print("RE3-OB MODEL COMPARISON")
    print(f"{'═' * 60}")
    print(f"{'Model':<{w}}{'Total':<10}{'Full pass':<12}{'Avg score':<12}")
    print(f"{'─' * 60}")
    for s in summaries:
        if s.get("skipped"):
            print(f"{s['model']:<{w}}{'(skipped)'}")
        else:
            print(
                f"{s['model']:<{w}}"
                f"{s['n_total']:<10}"
                f"{s['n_pass']:<12}"
                f"{s['avg']:.2f}"
            )
    print(f"{'═' * 60}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare LLM models on the RE3-OB benchmark"
    )
    parser.add_argument(
        "--models", nargs="+", default=_DEFAULT_MODELS,
        choices=list(_MODEL_LABEL.keys()),
        help=f"Models to compare (default: {' '.join(_DEFAULT_MODELS)})",
    )
    parser.add_argument(
        "--data-dir", default="data/RE3-OB",
        help="Path to the RE3-OB directory (default: data/RE3-OB)",
    )
    parser.add_argument(
        "--n", type=int, default=None,
        help="Number of scenarios per model (default: all)",
    )
    parser.add_argument(
        "--fault-types", nargs="+", choices=["F1", "F2", "F3", "F4", "F5"],
        default=None,
        help="Filter to specific fault types (default: all)",
    )
    parser.add_argument(
        "--results-dir", default="eval/results",
        help="Directory for output CSVs (default: eval/results)",
    )
    parser.add_argument(
        "--no-code", action="store_true",
        help="Disable GitHub source code fetching (faster, offline-safe)",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress per-scenario output (only show summaries)",
    )
    args = parser.parse_args()

    scenarios = load_re3_scenarios(
        data_dir=args.data_dir,
        n=args.n,
        fault_types=args.fault_types,
    )
    if not scenarios:
        print(
            "No scenarios loaded. Ensure data/RE3-OB contains case directories "
            "like cartservice_F1_1/ with data.csv, logs.csv, inject_time.txt."
        )
        sys.exit(1)

    print(f"\nRootScout RE3-OB Model Comparison")
    print(f"  Models     : {', '.join(args.models)}")
    print(f"  Scenarios  : {len(scenarios)}")
    print(f"  Fault types: {args.fault_types or 'all'}")
    print(f"  Code fetch : {'disabled' if args.no_code else 'enabled (GitHub)'}")

    summaries = []
    for model in args.models:
        summary = _run_one_model(
            model=model,
            scenarios=scenarios,
            results_dir=args.results_dir,
            fetch_code=not args.no_code,
            verbose=not args.quiet,
        )
        summaries.append(summary)

    _print_comparison_table(summaries)


if __name__ == "__main__":
    main()
