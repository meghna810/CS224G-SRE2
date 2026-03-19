#!/usr/bin/env python3
import sys
import os
import json
import time
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


def check(label, condition, detail=""):
    tag = PASS if condition else FAIL
    msg = f"  [{tag}] {label}"
    if detail:
        msg += f" ({detail})"
    print(msg)
    if not condition:
        sys.exit(1)


def test_otel_ingestion():
    print("\nOTel Ingestion")
    from RootScout.otel_ingester import OTelIngester, TelemetrySink
    from RootScout.test_otel_data import create_test_traces, create_test_logs

    class Sink(TelemetrySink):
        def __init__(self): self.records = []
        def emit(self, r): self.records.append(r)

    sink = Sink()
    ingester = OTelIngester(sink=sink)

    result = ingester.ingest_traces(create_test_traces())
    check("traces ingested", result.count > 0)

    traces = [r for r in sink.records if r.get("signal") == "trace"]
    services = {r["service"] for r in traces if r.get("service")}
    check("frontend and cart-service in traces", {"frontend", "cart-service"}.issubset(services))
    check("error spans present", any(r.get("status_code") == 2 for r in traces))

    result = ingester.ingest_logs(create_test_logs())
    check("logs ingested", result.count > 0)

    logs = [r for r in sink.records if r.get("signal") == "log"]
    check("ERROR log emitted", any(r.get("severity_text") == "ERROR" for r in logs))


def test_graph_construction():
    print("\nGraph Construction")
    from graph.graph_builder import GraphBuilder

    gb = GraphBuilder()
    gb.ingest_trace_span({"service_name": "frontend", "parent_service": None, "status": "OK", "latency_ms": 50})
    gb.ingest_trace_span({"service_name": "cart-service", "parent_service": "frontend", "status": "ERROR", "latency_ms": 5200})
    gb.ingest_trace_span({"service_name": "database", "parent_service": "cart-service", "status": "OK", "latency_ms": 10})

    check("3 nodes in graph", gb.graph.number_of_nodes() == 3)
    check("frontend -> cart-service edge", gb.graph.has_edge("frontend", "cart-service"))
    check("cart-service -> database edge", gb.graph.has_edge("cart-service", "database"))
    check("cart-service marked error", gb.graph.nodes["cart-service"]["status"] == "error")
    check("frontend marked ok", gb.graph.nodes["frontend"]["status"] == "ok")


def test_error_detection_from_logs():
    print("\nError Detection from Logs")
    from RootScout.otel_ingester import OTelIngester
    from RootScout.graph_sink import GraphBuilderSink
    from RootScout.test_otel_data import create_test_logs
    from graph.graph_builder import GraphBuilder

    gb = GraphBuilder()
    sink = GraphBuilderSink(gb)
    ingester = OTelIngester(sink=sink)
    ingester.ingest_logs(create_test_logs())

    gb._ensure_node("cart-service")
    events = gb.graph.nodes["cart-service"]["recent_events"]
    check("cart-service has error events", any(e["type"] == "error_log" for e in events))

    health = sink.get_health_summary()
    check("cart-service error count > 0", health.get("cart-service", {}).get("error_count", 0) > 0)


def test_context_retrieval():
    print("\nContext Retrieval")
    from graph.graph_builder import GraphBuilder
    from graph.context_retriever import ContextRetriever

    gb = GraphBuilder()
    for svc in ["frontend", "cart-service", "database"]:
        gb._ensure_node(svc)
    gb.graph.add_edge("frontend", "cart-service")
    gb.graph.add_edge("cart-service", "database")
    gb.graph.nodes["cart-service"]["status"] = "error"
    gb.graph.nodes["cart-service"]["recent_events"].append({
        "type": "error_log", "severity": "ERROR",
        "message": "DB timeout", "timestamp": time.time()
    })

    retriever = ContextRetriever(gb)
    check("unknown service returns error", "error" in retriever.get_context("bad-service"))

    ctx = retriever.get_context("frontend")
    check("focus_service is frontend", ctx["focus_service"] == "frontend")

    names = {n["service"] for n in ctx["related_nodes"]}
    check("cart-service in context", "cart-service" in names)

    cart = next(n for n in ctx["related_nodes"] if n["service"] == "cart-service")
    check("cart-service status error in context", cart["status"] == "error")
    check("cart-service has events in context", len(cart.get("events", [])) >= 1)


def test_rca_agent():
    print("\nRCA Agent")
    from graph.graph_builder import GraphBuilder
    from graph.context_retriever import ContextRetriever
    from graph.agent import RCAAgent
    from llm_integration.client import MockClient

    gb = GraphBuilder()
    for svc in ["frontend", "cart-service"]:
        gb._ensure_node(svc)
    gb.graph.add_edge("frontend", "cart-service")
    gb.graph.nodes["cart-service"]["status"] = "error"
    gb.graph.nodes["cart-service"]["recent_events"].append({
        "type": "error_log", "severity": "ERROR",
        "message": "DB timeout after 5000ms", "timestamp": time.time()
    })

    context = ContextRetriever(gb).get_context("frontend")
    result = RCAAgent(client=MockClient()).analyze(context)

    check("result is a dict", isinstance(result, dict))
    for key in ["root_cause_service", "confidence", "reasoning", "recommended_action"]:
        check(f"{key} present", key in result)
    check("confidence in [0, 1]", 0.0 <= result["confidence"] <= 1.0)


def test_github_enrichment():
    print("\nGitHub Enrichment")
    from graph.data_parser import enrich_context_from_github_output_path

    event = {
        "ingested_at": "2026-03-13T10:00:00+00:00",
        "event_type": "pull_request",
        "repo_owner": "test-org",
        "repo_name": "my-app",
        "service_id": "cart-service",
        "watch_path_prefix": "services/cart",
        "pr_number": 42,
        "title": "Increase DB pool size",
        "url": "https://github.com/test-org/my-app/pull/42",
        "files": [{"filename": "services/cart/db.py", "status": "modified",
                   "additions": 5, "deletions": 1, "patch": "@@ pool_size = 50"}],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        f.write(json.dumps(event) + "\n")
        tmp = f.name

    try:
        ctx = {
            "focus_service": "frontend",
            "related_nodes": [{"service": "cart-service", "status": "error", "events": []}],
        }
        enriched = enrich_context_from_github_output_path(
            ctx, github_output_path=tmp, max_events_per_service=10, lookback_hours=168, verbose=False
        )
        cart = next(n for n in enriched["related_nodes"] if n["service"] == "cart-service")
        gh_events = [e for e in cart.get("events", []) if e.get("source") == "github"]
        check("github events added to cart-service", len(gh_events) >= 1)
        check("event has summary", bool(gh_events[0].get("summary")))
    finally:
        os.unlink(tmp)


def test_full_pipeline():
    print("\nFull End-to-End Pipeline")
    from RootScout.otel_ingester import OTelIngester
    from RootScout.graph_sink import GraphBuilderSink
    from RootScout.test_otel_data import create_test_traces, create_test_metrics, create_test_logs
    from graph.graph_builder import GraphBuilder
    from graph.context_retriever import ContextRetriever
    from graph.agent import RCAAgent
    from llm_integration.client import MockClient
    import networkx as nx

    gb = GraphBuilder()
    sink = GraphBuilderSink(gb)
    ingester = OTelIngester(sink=sink)

    result = ingester.ingest_traces(create_test_traces())
    ingester.ingest_metrics(create_test_metrics())
    ingester.ingest_logs(create_test_logs())
    check("traces ingested", result.count > 0)

    for svc in ["frontend", "cart-service", "auth-service", "database"]:
        gb._ensure_node(svc)
    gb.graph.add_edge("frontend", "cart-service", latency=70)
    gb.graph.add_edge("frontend", "auth-service", latency=50)
    gb.graph.add_edge("cart-service", "database", latency=30)
    nx.set_node_attributes(gb.graph, {
        "cart-service": {"status": "error"},
        "frontend": {"status": "ok"},
        "auth-service": {"status": "ok"},
        "database": {"status": "ok"},
    })

    cart_node = gb.graph.nodes["cart-service"]
    if not cart_node["recent_events"]:
        cart_node["recent_events"].append({
            "type": "error_log", "severity": "ERROR",
            "message": "DB timeout after 5000ms", "timestamp": time.time()
        })

    check("cart-service is error", gb.graph.nodes["cart-service"]["status"] == "error")

    context = ContextRetriever(gb).get_context("cart-service")
    check("context focuses on cart-service", context["focus_service"] == "cart-service")

    related = {n["service"] for n in context["related_nodes"]}
    check("database in context", "database" in related)

    report = RCAAgent(client=MockClient()).analyze(context)
    check("rca report returned", isinstance(report, dict))
    check("has root_cause_service", "root_cause_service" in report)
    check("has reasoning", bool(report.get("reasoning")))


if __name__ == "__main__":
    print("RootScout End-to-End Tests")

    test_otel_ingestion()
    test_graph_construction()
    test_error_detection_from_logs()
    test_context_retrieval()
    test_rca_agent()
    test_github_enrichment()
    test_full_pipeline()

    print("All tests passed.")
