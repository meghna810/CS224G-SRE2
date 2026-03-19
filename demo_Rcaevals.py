#!/usr/bin/env python3
"""
demo_Rcaevals.py - End-to-End RootScout Demo: RE3-OB Online Boutique
=====================================================================

Scenario: A code-level fault injected into the Online Boutique (RE3-OB).

Flow:
  1. Slack alert fires  — frontend errors detected via monitoring
  2. RootScout activates — loads RE3-OB telemetry, builds causal graph
  3. RCA agent runs      — identifies root cause service + fault type
  4. Slack RCA report    — posts structured report + recommended action

Run with real Slack:
    SLACK_BOT_TOKEN=xoxb-... SLACK_ALERT_CHANNEL=#incidents python demo_Rcaevals.py

Dry-run (no token needed):
    python demo_Rcaevals.py

Data prerequisite (RE3-OB):
    git clone https://github.com/phamquiluan/RCAEval /tmp/RCAEval
    cd /tmp/RCAEval && pip install -e . && python main.py --download --dataset RE3-OB
    cp -r data/RE3-OB <project_root>/data/RE3-OB
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# Terminal colours
# ---------------------------------------------------------------------------

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
RED    = "\033[31m"
GREY   = "\033[90m"
DIM    = "\033[2m"
BLUE   = "\033[34m"


def ok(text):    print(f"  {GREEN}[ok]{RESET}  {text}")
def warn(text):  print(f"  {YELLOW}[warn]{RESET} {text}")
def info(text):  print(f"  {GREY}-->{RESET}  {text}")
def err(text):   print(f"  {RED}[err]{RESET} {text}")
def step(n, t):  print(f"\n{BOLD}Step {n}: {t}{RESET}")
def rule():      print(f"  {DIM}{'─' * 60}{RESET}")


# ---------------------------------------------------------------------------
# Dry-run notifier (mirrors demo_slack.py's DryRunNotifier)
# ---------------------------------------------------------------------------

class DryRunNotifier:
    """Prints what would be posted to Slack without making network calls."""

    class _FakeCfg:
        alert_cooldown_seconds = 0

    _config = _FakeCfg()

    def post_incident_alert(self, service, status, signal, detail=""):
        print(f"\n  {YELLOW}[DRY-RUN] Slack — Incident Alert{RESET}")
        _print_preview({
            "service": service,
            "status":  status.upper(),
            "signal":  signal,
            "detail":  detail[:200] if detail else "",
        })

    def post_rca_report(self, focus_service, report):
        print(f"\n  {YELLOW}[DRY-RUN] Slack — RCA Report{RESET}")
        reasoning = str(report.get("reasoning", ""))
        _print_preview({
            "focus_service": focus_service,
            "root_cause":    report.get("root_cause_service"),
            "confidence":    f"{float(report.get('confidence', 0)) * 100:.0f}%",
            "reasoning":     reasoning[:200] + ("..." if len(reasoning) > 200 else ""),
            "action":        report.get("recommended_action"),
        })


def _print_preview(d):
    rule()
    for k, v in d.items():
        if v is not None and v != "":
            print(f"  {GREY}{k:<18}{RESET}{v}")
    rule()


# ---------------------------------------------------------------------------
# Main demo
# ---------------------------------------------------------------------------

# Target RE3-OB case — easy/well-known fault for demo purposes
_DEMO_SERVICE   = "cartservice"
_DEMO_FAULT     = "F1"
_DEMO_CASE_NUM  = "1"
_DEMO_CASE_DIR  = f"{_DEMO_SERVICE}_{_DEMO_FAULT}_{_DEMO_CASE_NUM}"
_DATA_DIR       = "data/RE3/RE3-OB"


def main():
    print(f"\n{BOLD}  RootScout RE3-OB Online Boutique Demo{RESET}")
    print(f"\n  Scenario  : Code-level fault injected into Online Boutique")
    print(f"  Case      : {_DEMO_CASE_DIR}  (F1 = incorrect parameter)")
    print(f"  Topology  : frontend → cartservice → redis-cart")
    print(f"  Flow      : Slack alert → RootScout RCA → Slack report")

    # ------------------------------------------------------------------
    # Verify RE3-OB data is present (support both flat and nested layouts)
    # ------------------------------------------------------------------
    # Flat layout:   data_dir/cartservice_F1_1/
    # Nested layout: data_dir/cartservice_f1/1/
    flat_path   = os.path.join(_DATA_DIR, _DEMO_CASE_DIR)
    nested_path = os.path.join(
        _DATA_DIR,
        f"{_DEMO_SERVICE}_{_DEMO_FAULT.lower()}",
        _DEMO_CASE_NUM,
    )
    if not os.path.isdir(flat_path) and not os.path.isdir(nested_path):
        print(f"\n  {RED}[err]{RESET} RE3-OB data not found at: {flat_path}")
        print(f"\n  Download the dataset first:")
        print(f"    git clone https://github.com/phamquiluan/RCAEval /tmp/RCAEval")
        print(f"    cd /tmp/RCAEval && pip install -e .")
        print(f"    python main.py --download --dataset RE3-OB")
        print(f"    cp -r data/RE3-OB <project_root>/data/RE3-OB\n")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Slack setup
    # ------------------------------------------------------------------
    slack_token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    dry_run     = not slack_token

    print()
    if dry_run:
        warn("SLACK_BOT_TOKEN not set — running in dry-run mode (no real messages sent)")
        warn("Set SLACK_BOT_TOKEN + SLACK_ALERT_CHANNEL to post to real Slack")
    else:
        alert_ch = os.getenv("SLACK_ALERT_CHANNEL", "#incidents")
        rca_ch   = os.getenv("SLACK_RCA_CHANNEL", "") or alert_ch
        ok(f"Slack connected  alert→{alert_ch}  rca→{rca_ch}")

    time.sleep(0.4)

    # ------------------------------------------------------------------
    # Step 1 — Initialise
    # ------------------------------------------------------------------
    step(1, "Initialise components")

    if dry_run:
        notifier = DryRunNotifier()
    else:
        from RootScout.slack_connector import SlackConfig, SlackNotifier
        cfg = SlackConfig(
            bot_token=slack_token,
            signing_secret=os.getenv("SLACK_SIGNING_SECRET", ""),
            alert_channel=os.getenv("SLACK_ALERT_CHANNEL", "#incidents"),
            rca_channel=os.getenv("SLACK_RCA_CHANNEL", ""),
            alert_cooldown_seconds=0,
        )
        notifier = SlackNotifier(cfg)
    ok("SlackNotifier ready")

    from eval.rcaeval_loader import load_re3_scenarios
    scenarios = load_re3_scenarios(
        data_dir=_DATA_DIR,
        n=None,
        fault_types=[_DEMO_FAULT],
    )
    # Pick our specific case by matching service + fault type + case number
    def _matches_demo(s: dict) -> bool:
        gt_svc = s["ground_truth"]["root_cause_component"].lower()
        ft     = s.get("re3_fault_type", "").upper()
        # case number encoded in the scenario id: re3_{service}_{ft}_{num:03d}
        sid    = s.get("id", "")
        try:
            case_n = sid.rsplit("_", 1)[-1].lstrip("0") or "0"
        except Exception:
            case_n = ""
        return (
            gt_svc == _DEMO_SERVICE.lower()
            and ft == _DEMO_FAULT.upper()
            and case_n == _DEMO_CASE_NUM
        )

    scenario = next((s for s in scenarios if _matches_demo(s)), None)
    if scenario is None:
        err(f"Case '{_DEMO_CASE_DIR}' not found after loading — check data directory.")
        sys.exit(1)
    ok(f"Scenario loaded: {scenario['title']}")
    ok(f"Inject time    : {scenario['re3_inject_time'].strftime('%Y-%m-%d %H:%M:%S')} UTC")
    ok(f"Ground truth   : {scenario['ground_truth']['root_cause_component']}"
       f" — {scenario['ground_truth']['root_cause_reason']}")

    time.sleep(0.4)

    # ------------------------------------------------------------------
    # Step 2 — Slack alert fires
    # ------------------------------------------------------------------
    step(2, "Slack alert received — frontend error detected")
    info("Monitoring detected elevated error rate on 'frontend' (Online Boutique entry point)")
    print()

    inject_ts = scenario["re3_inject_time"].strftime("%Y-%m-%dT%H:%M:%SZ")
    alert_detail = (
        f"frontend reporting HTTP 5xx errors since {inject_ts}. "
        "Downstream service(s) returning errors — possible code fault."
    )
    notifier.post_incident_alert(
        service="frontend",
        status="error",
        signal="trace",
        detail=alert_detail,
    )
    ok("Incident alert posted to Slack")

    time.sleep(0.5)

    # ------------------------------------------------------------------
    # Step 3 — Build causal graph from RE3-OB telemetry
    # ------------------------------------------------------------------
    step(3, "RootScout activates — loading telemetry + building graph")
    info(f"Reading metrics and logs from: {_DEMO_CASE_DIR}/")
    info(f"Window: inject_time ± 15 min")
    print()

    from eval.rcaeval_graph_adapter import build_re3_graph
    graph_builder = build_re3_graph(scenario, fetch_code=False)
    ok("Causal dependency graph built")

    g = graph_builder.graph
    ok(f"Graph: {g.number_of_nodes()} nodes, {g.number_of_edges()} edges")

    # Show error nodes
    error_nodes = [n for n in g.nodes if g.nodes[n].get("status") == "error"]
    healthy     = [n for n in g.nodes if g.nodes[n].get("status") == "ok"]

    print()
    if error_nodes:
        for svc in error_nodes:
            events = g.nodes[svc].get("recent_events", [])
            n_ev   = len(events)
            sample = (events[0].get("message", "")[:80] + "...") if events else ""
            print(f"    {RED}[ERROR]{RESET}  {svc}  ({n_ev} event(s))")
            if sample:
                print(f"             └─ {GREY}{sample}{RESET}")
    else:
        print(f"    {GREY}No error-status nodes detected{RESET}")

    for svc in healthy[:6]:          # show a few healthy ones
        print(f"    {GREEN}[ok]{RESET}     {svc}")
    if len(healthy) > 6:
        print(f"    {GREY}... and {len(healthy) - 6} more healthy services{RESET}")

    time.sleep(0.5)

    # ------------------------------------------------------------------
    # Step 4 — RCA agent analysis
    # ------------------------------------------------------------------
    step(4, "RCA agent analysis")
    info("Running BFS context retrieval from 'frontend'...")

    from graph.context_retriever import ContextRetriever
    from graph.agent import RCAAgent
    from llm_integration.client import MockClient

    observed_svc = scenario["observed_service"]   # "frontend"
    if observed_svc not in g:
        graph_builder._ensure_node(observed_svc)

    context = ContextRetriever(graph_builder).get_context(observed_svc)
    related = context.get("related_nodes", [])
    ok(f"Context packet: {len(related)} related services retrieved")

    # Choose LLM
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    gemini_key    = os.getenv("GEMINI_API_KEY", "").strip()

    if anthropic_key:
        try:
            from llm_integration.client import ClaudeClient
            llm = ClaudeClient()
            ok("LLM: Claude (claude-sonnet-4-6)")
        except Exception as exc:
            warn(f"Claude unavailable ({exc}) — falling back to MockClient")
            llm = MockClient()
    elif gemini_key:
        try:
            from llm_integration.client import GeminiClient
            llm = GeminiClient()
            ok("LLM: Gemini 2.5 Flash")
        except Exception as exc:
            warn(f"Gemini unavailable ({exc}) — falling back to MockClient")
            llm = MockClient()
    else:
        llm = MockClient()
        warn("No API key found (ANTHROPIC_API_KEY / GEMINI_API_KEY) — using MockClient")

    info("Sending context to LLM for root cause analysis...")
    agent  = RCAAgent(client=llm, github_output_path=None)
    report = agent.analyze(context)
    print()

    root  = report.get("root_cause_service", "unknown")
    conf  = float(report.get("confidence", 0))
    why   = report.get("reasoning", "")
    fix   = report.get("recommended_action", "")

    ok(f"Root cause   : {BOLD}{root}{RESET}")
    ok(f"Confidence   : {BOLD}{conf * 100:.0f}%{RESET}")
    if why:
        ok(f"Reasoning    : {why[:140]}{'...' if len(why) > 140 else ''}")
    if fix:
        ok(f"Action       : {fix[:140]}{'...' if len(fix) > 140 else ''}")

    # Quick correctness check
    expected = scenario["ground_truth"]["root_cause_component"]
    print()
    if expected.lower() in root.lower():
        ok(f"{GREEN}Correct{RESET} — predicted '{root}' matches ground truth '{expected}'")
    else:
        warn(f"Predicted '{root}' — ground truth is '{expected}'")

    time.sleep(0.5)

    # ------------------------------------------------------------------
    # Step 5 — Post RCA report back to Slack
    # ------------------------------------------------------------------
    step(5, "Post RCA report to Slack")
    print()

    notifier.post_rca_report(focus_service=observed_svc, report=report)
    ok("RCA report posted to Slack")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n  {'─' * 60}")
    print(f"  {BOLD}Demo complete.{RESET}")
    if dry_run:
        print(f"\n  To post real Slack messages set SLACK_BOT_TOKEN in .env")
    else:
        print(f"\n  Incident alert + RCA report posted to Slack.")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted.")
