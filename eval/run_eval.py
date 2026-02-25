"""
run_eval.py - CLI entry point for the RootScout benchmark.

Usage
-----
# Run all 10 synthetic scenarios with Gemini (real LLM):
    python eval/run_eval.py

# Run with mock LLM (for fast smoke-testing):
    python eval/run_eval.py --mock

# Run only easy scenarios:
    python eval/run_eval.py --difficulty easy

# Also load up to 5 real OpenRCA cases (requires CSVs — see openrca_loader.py):
    python eval/run_eval.py --with-openrca --openrca-system Telecom --openrca-n 5

# Specify custom output path:
    python eval/run_eval.py --output eval/results/my_run.csv

# Re-score an existing predictions CSV without re-running the LLM:
    python eval/run_eval.py --rescore eval/results/run_predictions.csv \
                            --query   eval/results/run_query.csv
"""

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eval.scenarios import SYNTHETIC_SCENARIOS
from eval.benchmark import (
    run_benchmark,
    results_to_prediction_csv,
    scenarios_to_query_csv,
)
from eval.evaluate import file_evaluate, report as print_report
from llm_integration.client import get_client


def main():
    parser = argparse.ArgumentParser(
        description="RootScout benchmark runner (OpenRCA evaluation methodology)"
    )

    # Scenario selection
    parser.add_argument(
        "--difficulty",
        choices=["easy", "medium", "hard", "all"],
        default="all",
        help="Filter scenarios by difficulty tier (default: all)",
    )
    parser.add_argument(
        "--scenario-ids",
        nargs="+",
        metavar="ID",
        help="Run only specific scenario IDs, e.g. scenario_001 scenario_005",
    )

    # OpenRCA real data
    parser.add_argument(
        "--with-openrca",
        action="store_true",
        help="Append real OpenRCA cases (requires CSVs in eval/openrca_data/)",
    )
    parser.add_argument(
        "--openrca-system",
        default="Telecom",
        choices=["Telecom", "Bank", "Market"],
        help="Which OpenRCA system to load (default: Telecom)",
    )
    parser.add_argument(
        "--openrca-n",
        type=int,
        default=5,
        help="How many OpenRCA cases to load (default: 5)",
    )

    # LLM
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use MockClient instead of any real LLM (fast smoke test)",
    )
    parser.add_argument(
        "--model",
        default="gemini",
        help=(
            "LLM provider/model to use. Examples: gemini, claude, openai, "
            "gpt-4o, claude-sonnet, gemini-2.5-pro, openai/gpt-4o-mini "
            "(default: gemini)"
        ),
    )

    # Output
    parser.add_argument(
        "--output",
        default=None,
        help="Path for results CSV (default: eval/results/run_<timestamp>.csv)",
    )

    # Re-score mode
    parser.add_argument(
        "--rescore",
        metavar="PRED_CSV",
        help="Re-score an existing predictions CSV without re-running the LLM",
    )
    parser.add_argument(
        "--query",
        metavar="QUERY_CSV",
        help="Ground-truth query CSV (required with --rescore)",
    )

    args = parser.parse_args()

    # -----------------------------------------------------------------------
    # Re-score mode
    # -----------------------------------------------------------------------
    if args.rescore:
        if not args.query:
            parser.error("--rescore requires --query")
        report_path = args.output or args.rescore.replace(".csv", "_report.csv")
        print(f"Re-scoring {args.rescore} against {args.query} ...")
        file_evaluate(args.rescore, args.query, report_path)
        print_report(report_path)
        return

    # -----------------------------------------------------------------------
    # Build scenario list
    # -----------------------------------------------------------------------
    scenarios = list(SYNTHETIC_SCENARIOS)

    if args.with_openrca:
        from eval.openrca_loader import load_openrca_scenarios
        openrca_cases = load_openrca_scenarios(
            system=args.openrca_system,
            max_cases=args.openrca_n,
            task_index_offset=len(scenarios),
        )
        scenarios.extend(openrca_cases)

    # Filter by difficulty
    if args.difficulty != "all":
        scenarios = [s for s in scenarios if s["difficulty"] == args.difficulty]

    # Filter by explicit IDs
    if args.scenario_ids:
        id_set = set(args.scenario_ids)
        scenarios = [s for s in scenarios if s["id"] in id_set]

    if not scenarios:
        print("No scenarios matched the given filters. Exiting.")
        sys.exit(1)

    model_label = "MockClient" if args.mock else args.model
    print(f"\nRootScout Benchmark")
    print(f"  Scenarios  : {len(scenarios)}")
    print(f"  Difficulty : {args.difficulty}")
    print(f"  LLM        : {model_label}")
    if args.with_openrca:
        print(f"  OpenRCA    : {args.openrca_system} (up to {args.openrca_n} cases)")

    # -----------------------------------------------------------------------
    # LLM client
    # -----------------------------------------------------------------------
    llm_client = None
    if args.mock:
        from llm_integration.client import MockClient
        llm_client = MockClient()
    else:
        try:
            llm_client = get_client(args.model)
        except Exception as e:
            print(f"[run_eval] Could not initialise {args.model}: {e}")
            print("[run_eval] Falling back to MockClient")
            from llm_integration.client import MockClient
            llm_client = MockClient()

    # -----------------------------------------------------------------------
    # Determine output path
    # -----------------------------------------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.output:
        results_csv = args.output
    else:
        os.makedirs("eval/results", exist_ok=True)
        results_csv = f"eval/results/run_{timestamp}.csv"

    pred_csv = results_csv.replace(".csv", "_predictions.csv")
    query_csv = results_csv.replace(".csv", "_query.csv")
    report_csv = results_csv.replace(".csv", "_report.csv")

    # -----------------------------------------------------------------------
    # Run benchmark
    # -----------------------------------------------------------------------
    results = run_benchmark(
        scenarios=scenarios,
        llm_client=llm_client,
        output_csv=results_csv,
        verbose=True,
    )

    # -----------------------------------------------------------------------
    # Save CSVs for OpenRCA-compatible file_evaluate()
    # -----------------------------------------------------------------------
    results_to_prediction_csv(results, pred_csv)
    scenarios_to_query_csv(scenarios, query_csv)

    # -----------------------------------------------------------------------
    # Final report using OpenRCA's evaluator
    # -----------------------------------------------------------------------
    print("\n--- OpenRCA-style evaluation report ---")
    file_evaluate(pred_csv, query_csv, report_csv)
    print_report(report_csv)

    print(f"Artifacts written:")
    print(f"  Full results : {results_csv}")
    print(f"  Predictions  : {pred_csv}")
    print(f"  Query/GT     : {query_csv}")
    print(f"  Report       : {report_csv}")


if __name__ == "__main__":
    main()
