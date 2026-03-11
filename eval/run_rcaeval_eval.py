"""
run_rcaeval_eval.py - Evaluate RootScout on RE3-OB (RCAEval) code-level faults.

Unlike the Bank benchmark (infrastructure faults), RE3 injects code-level faults
(wrong parameters, missing function calls, missing exception handlers, wrong return
values) into Online Boutique microservices. The logs contain stack traces that
point directly to faulty code lines — a richer signal than infrastructure metrics.

Scoring follows the same OpenRCA methodology:
  component accuracy  (exact string match)
  reason accuracy     (cosine similarity ≥ 0.50)
  datetime accuracy   NOT scored — excluded from scoring_points to avoid
                      artificial inflation (the agent does not predict datetime
                      reliably for code-level faults)

Usage
-----
# Full evaluation — all RE3-OB cases with default LLM (Gemini):
    python eval/run_rcaeval_eval.py

# Dry-run with mock LLM (no API key needed, tests plumbing):
    python eval/run_rcaeval_eval.py --mock

# Only 5 cases for a quick sanity check:
    python eval/run_rcaeval_eval.py --n 5

# Specific fault types only:
    python eval/run_rcaeval_eval.py --fault-types F1 F3

# Run with Claude Opus:
    python eval/run_rcaeval_eval.py --model claude-opus

# Custom data directory:
    python eval/run_rcaeval_eval.py --data-dir /path/to/RE3-OB

Data download (prerequisite):
    git clone https://github.com/phamquiluan/RCAEval /tmp/RCAEval
    cd /tmp/RCAEval && pip install -e .
    python main.py --download --dataset RE3-OB
    cp -r data/RE3-OB <project_root>/data/RE3-OB
"""

import argparse
import csv
import os
import sys
import time
import traceback
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from eval.rcaeval_loader import load_re3_scenarios
from eval.rcaeval_graph_adapter import build_re3_graph
from eval.evaluate import evaluate as openrca_evaluate, file_evaluate, report as print_report
from eval.benchmark import format_prediction, results_to_prediction_csv, scenarios_to_query_csv
from graph.context_retriever import ContextRetriever
from graph.agent import RCAAgent
from llm_integration.client import get_client, MockClient


# ---------------------------------------------------------------------------
# Single scenario runner
# ---------------------------------------------------------------------------

def run_re3_scenario(
    scenario: dict,
    llm_client,
    verbose: bool = True,
    fetch_code: bool = True,
) -> dict:
    """
    End-to-end evaluation of one RE3-OB case.

    Steps:
      1. Build GraphBuilder from RE3 telemetry (metrics + logs, ±15 min window)
      2. Run ContextRetriever BFS from "frontend"
      3. Run RCAAgent
      4. Score against component + reason criteria (no datetime)
    """
    result = {
        "scenario_id":  scenario["id"],
        "task_index":   scenario["task_index"],
        "difficulty":   scenario["difficulty"],
        "title":        scenario["title"],
        "fault_type":   scenario.get("re3_fault_type", "?"),
        "prediction":   "",
        "score":        0.0,
        "passing":      [],
        "failing":      [],
        "agent_output": {},
        "error":        None,
    }

    if verbose:
        print(f"\n{'─' * 60}")
        print(f"Running [{scenario['task_index']}] {scenario['title']}")
        print(f"  Difficulty : {scenario['difficulty']}")
        print(
            f"  Expected   : {scenario['ground_truth']['root_cause_component']}"
            f" — {scenario['ground_truth']['root_cause_reason']}"
        )
        print(f"  Case dir   : {os.path.basename(scenario['re3_case_dir'])}")

    try:
        # 1. Build graph from RE3 telemetry (does its own I/O)
        case_dir = scenario["re3_case_dir"]
        if not os.path.isdir(case_dir):
            raise FileNotFoundError(
                f"Case directory not found: {case_dir}\n"
                f"  Make sure data/RE3-OB is present at the project root.\n"
                f"  Download: python main.py --download --dataset RE3-OB  "
                f"(in the RCAEval repo)"
            )

        graph_builder = build_re3_graph(scenario, fetch_code=fetch_code)

        # 2. Retrieve context for the alerting service (frontend = web tier)
        observed_svc = scenario["observed_service"]
        if observed_svc not in graph_builder.graph:
            graph_builder._ensure_node(observed_svc)

        retriever = ContextRetriever(graph_builder)
        context   = retriever.get_context(observed_svc)

        if verbose:
            error_nodes = [
                n["service"]
                for n in context.get("related_nodes", [])
                if n.get("status") == "error"
            ]
            print(f"  Error nodes: {error_nodes if error_nodes else 'none detected'}")

        # 3. Run RCA agent (no GitHub enrichment for RE3)
        agent = RCAAgent(client=llm_client, github_output_path=None)
        agent_output = agent.analyze(context)
        result["agent_output"] = agent_output

        predicted_component = agent_output.get("root_cause_service", "")
        predicted_dt        = agent_output.get("root_cause_datetime", "")
        predicted_reason    = agent_output.get("reasoning", "")

        # 4. Format prediction in OpenRCA JSON format
        prediction_str = format_prediction(agent_output, scenario)
        result["prediction"] = prediction_str

        # 5. Score against ground truth (component + reason only — no datetime)
        passing, failing, score, details = openrca_evaluate(
            prediction_str, scenario["scoring_points"]
        )
        result["score"]   = score
        result["passing"] = passing
        result["failing"] = failing

        if verbose:
            from eval.evaluate import _SIM_THRESHOLD
            from eval.benchmark import _log_criterion_details
            _log_criterion_details(
                details, predicted_component, predicted_dt, _SIM_THRESHOLD, predicted_reason
            )
            verdict = (
                "PASS"
                if score == 1.0
                else f"PARTIAL ({score:.2f})"
                if score > 0
                else "FAIL"
            )
            print(f"  Score      : {score:.2f} [{verdict}]")

    except Exception:
        result["error"] = traceback.format_exc()
        if verbose:
            print(f"  ERROR      : {result['error'].splitlines()[-1]}")

    return result


# ---------------------------------------------------------------------------
# Full benchmark runner
# ---------------------------------------------------------------------------

def run_re3_benchmark(
    scenarios: list,
    llm_client=None,
    output_csv: str = None,
    verbose: bool = True,
    fetch_code: bool = True,
) -> list:
    """Run all scenarios sequentially and return result dicts."""
    if llm_client is None:
        try:
            llm_client = get_client("gemini")
            if verbose:
                print("[re3_eval] Using GeminiClient")
        except Exception as e:
            if verbose:
                print(f"[re3_eval] GeminiClient unavailable ({e}), using MockClient")
            llm_client = MockClient()

    results = []
    for scenario in scenarios:
        result = run_re3_scenario(
            scenario, llm_client=llm_client, verbose=verbose, fetch_code=fetch_code
        )
        results.append(result)
        time.sleep(0.3)   # light buffer between LLM calls

    _print_summary(results, verbose=verbose)

    if output_csv:
        _save_results(results, output_csv)
        if verbose:
            print(f"\n[re3_eval] Full results → {output_csv}")

    return results


# ---------------------------------------------------------------------------
# Summary + CSV helpers
# ---------------------------------------------------------------------------

def _print_summary(results: list, verbose: bool = True) -> None:
    if not verbose:
        return

    # Per-difficulty summary
    tiers: dict = {}
    for r in results:
        t = r.get("difficulty", "unknown")
        tiers.setdefault(t, []).append(r["score"])

    w = 14
    print(f"\n{'═' * 56}")
    print("RE3-OB BENCHMARK SUMMARY  (RCAEval code-level faults)")
    print(f"{'═' * 56}")
    print(f"{'Class':<{w}}{'Total(#)':<{w}}{'Full pass(#)':<{w}}{'Avg score':<{w}}")
    print(f"{'─' * 56}")

    grand_total = grand_correct = 0
    grand_sum = 0.0
    for tier in ["easy", "medium", "hard"]:
        scores = tiers.get(tier, [])
        if not scores:
            continue
        total   = len(scores)
        correct = sum(1 for s in scores if s == 1.0)
        avg     = sum(scores) / total
        grand_total   += total
        grand_correct += correct
        grand_sum     += sum(scores)
        print(f"{tier:<{w}}{total:<{w}}{correct:<{w}}{avg:.2f}")

    print(f"{'─' * 56}")
    grand_avg = grand_sum / grand_total if grand_total else 0.0
    print(f"{'Total':<{w}}{grand_total:<{w}}{grand_correct:<{w}}{grand_avg:.2f}")
    print(f"{'═' * 56}\n")

    # Per-fault-type breakdown
    ft_scores: dict = {}
    for r in results:
        ft = r.get("fault_type", "?")
        ft_scores.setdefault(ft, []).append(r["score"])
    print("Per-fault-type averages:")
    for ft in sorted(ft_scores):
        ss = ft_scores[ft]
        print(f"  {ft:<10} n={len(ss):>3}  avg={sum(ss)/len(ss):.2f}")


def _save_results(results: list, output_csv: str) -> None:
    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    fields = [
        "scenario_id", "task_index", "difficulty", "fault_type", "title",
        "prediction", "score", "passing", "failing", "error",
    ]
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in results:
            writer.writerow({k: r.get(k, "") for k in fields})


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="RootScout evaluation on RE3-OB (RCAEval code-level faults)"
    )
    parser.add_argument(
        "--data-dir", default="data/RE3-OB",
        help="Path to the RE3-OB directory (default: data/RE3-OB)",
    )
    parser.add_argument(
        "--n", type=int, default=None,
        help="Number of scenarios to evaluate (default: all)",
    )
    parser.add_argument(
        "--fault-types", nargs="+", choices=["F1", "F2", "F3", "F4", "F5"],
        default=None,
        help="Filter to specific fault types (default: all)",
    )
    parser.add_argument(
        "--mock", action="store_true",
        help="Use MockClient instead of any real LLM (fast plumbing test)",
    )
    parser.add_argument(
        "--model", default="gemini",
        help=(
            "LLM provider/model to use. Examples: gemini, claude-opus, claude-sonnet, "
            "openai, gpt-4o (default: gemini)"
        ),
    )
    parser.add_argument(
        "--output", default=None,
        help="Output CSV path (default: eval/results/re3_run_<timestamp>.csv)",
    )
    parser.add_argument(
        "--difficulty", choices=["easy", "medium", "hard", "all"],
        default="all",
        help="Filter scenarios by difficulty tier (default: all)",
    )
    parser.add_argument(
        "--no-code", action="store_true",
        help="Disable GitHub source code fetching (faster, offline-safe)",
    )
    args = parser.parse_args()

    # Load scenarios
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

    if args.difficulty != "all":
        scenarios = [s for s in scenarios if s["difficulty"] == args.difficulty]

    if not scenarios:
        print(f"No scenarios match difficulty='{args.difficulty}'. Exiting.")
        sys.exit(1)

    fetch_code  = not args.no_code
    model_label = "MockClient" if args.mock else args.model
    print(f"\nRootScout RE3-OB Evaluation  (RCAEval code-level faults)")
    print(f"  Scenarios  : {len(scenarios)}")
    print(f"  Difficulty : {args.difficulty}")
    print(f"  Fault types: {args.fault_types or 'all'}")
    print(f"  LLM        : {model_label}")
    print(f"  Code fetch : {'enabled (GitHub)' if fetch_code else 'disabled'}")

    # LLM client
    if args.mock:
        llm_client = MockClient()
        print("  WARNING: Using MockClient — results are FAKE (hardcoded, not real LLM)")
    else:
        try:
            llm_client = get_client(args.model)
            print(f"  LLM client initialised: {args.model}")
        except Exception as e:
            print(f"\n{'!'*60}")
            print(f"  WARNING: Could not initialise '{args.model}': {e}")
            print(f"  FALLING BACK TO MockClient — results are FAKE")
            print(f"  Check your API key in .env")
            print(f"{'!'*60}\n")
            llm_client = MockClient()

    # Output paths
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_csv = args.output or f"eval/results/re3_run_{timestamp}.csv"
    pred_csv    = results_csv.replace(".csv", "_predictions.csv")
    query_csv   = results_csv.replace(".csv", "_query.csv")
    report_csv  = results_csv.replace(".csv", "_report.csv")
    os.makedirs("eval/results", exist_ok=True)

    # Run benchmark
    results = run_re3_benchmark(
        scenarios=scenarios,
        llm_client=llm_client,
        output_csv=results_csv,
        verbose=True,
        fetch_code=fetch_code,
    )

    # Persist CSVs for OpenRCA-compatible file_evaluate()
    results_to_prediction_csv(results, pred_csv)
    scenarios_to_query_csv(scenarios, query_csv)

    # Final report
    print("\n--- OpenRCA-style evaluation report ---")
    try:
        file_evaluate(pred_csv, query_csv, report_csv)
        print_report(report_csv)
    except Exception as e:
        print(f"[re3_eval] Report generation failed: {e}")

    print("Artifacts written:")
    print(f"  Full results : {results_csv}")
    print(f"  Predictions  : {pred_csv}")
    print(f"  Query/GT     : {query_csv}")
    print(f"  Report       : {report_csv}")


if __name__ == "__main__":
    main()
