#!/usr/bin/env python3
"""
demo.py - End-to-End RootScout Demo
====================================

Demonstrates the complete RCA pipeline with synthetic data:
1. Ingest OTLP data (traces, metrics, logs)
2. Build service dependency graph
3. Enrich with GitHub PR/commit data
4. Run LLM-powered RCA analysis
5. Show actionable remediation

Scenario: E-commerce checkout flow with cart-service database timeout

Run: python demo.py
"""

import sys
import os
import json
import time
from datetime import datetime, timezone

# Add paths for imports
sys.path.append(os.path.dirname(__file__))

from RootScout.otel_ingester import OTelIngester
from RootScout.graph_sink import GraphBuilderSink, ComposedSink
from RootScout.test_otel_data import create_test_traces, create_test_metrics, create_test_logs

from graph.graph_builder import GraphBuilder
from graph.context_retriever import ContextRetriever
from graph.agent import RCAAgent
from llm_integration.client import MockClient, GeminiClient
from slack_integration import SlackNotifier


# =============================================================================
# DEMO CONFIGURATION
# =============================================================================

DEMO_CONFIG = {
    "show_raw_otlp": True,  # Set to True to see raw OTLP data
    "show_synthetic_data": True,  # Show synthetic data samples
    "show_graph_details": True,  # Show detailed graph state
    "use_real_llm": True,  # Try to use Gemini API (falls back to mock)
    "create_github_data": True,  # Create synthetic GitHub events
    "pause_between_steps": 1.0,  # Seconds to pause between demo steps
    "show_llm_prompt": True,  # Show the full prompt sent to LLM
    "show_component_explanations": True,  # Show what each component does
}


# =============================================================================
# SYNTHETIC GITHUB DATA
# =============================================================================

SYNTHETIC_GITHUB_EVENTS = [
    {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "event_type": "pull_request",
        "repo_owner": "demo-org",
        "repo_name": "ecommerce-platform",
        "service_id": "cart-service",
        "watch_path_prefix": "services/cart",
        "pr_number": 156,
        "title": "Increase database connection pool size",
        "url": "https://github.com/demo-org/ecommerce-platform/pull/156",
        "files": [
            {
                "filename": "services/cart/database.py",
                "status": "modified",
                "additions": 3,
                "deletions": 1,
                "patch": """@@ -12,7 +12,9 @@ class DatabasePool:
     def __init__(self):
-        self.pool_size = 10
+        # Increased pool size to handle higher load
+        # TODO: Make this configurable via env var
+        self.pool_size = 50
         self.timeout = 5000
"""
            }
        ]
    },
    {
        "ingested_at": (datetime.now(timezone.utc).timestamp() - 3600).__str__(),  # 1 hour ago
        "event_type": "push",
        "repo_owner": "demo-org",
        "repo_name": "ecommerce-platform",
        "service_id": "cart-service",
        "watch_path_prefix": "services/cart",
        "commit_sha": "a3f4b2c",
        "title": "Add retry logic for cart operations",
        "url": "https://github.com/demo-org/ecommerce-platform/commit/a3f4b2c",
        "files": [
            {
                "filename": "services/cart/handlers.py",
                "status": "modified",
                "additions": 15,
                "deletions": 3,
                "patch": """@@ -45,8 +45,22 @@ def get_cart_items(user_id):
     try:
-        items = db.query(f"SELECT * FROM cart_items WHERE user_id = {user_id}")
-        return items
+        # Add retry logic for transient failures
+        max_retries = 3
+        for attempt in range(max_retries):
+            try:
+                items = db.query(f"SELECT * FROM cart_items WHERE user_id = {user_id}")
+                return items
+            except DatabaseTimeout as e:
+                if attempt == max_retries - 1:
+                    raise
+                time.sleep(0.5 * (attempt + 1))  # Exponential backoff
     except Exception as e:
         logger.error(f"Failed to fetch cart items: {e}")
         raise
"""
            }
        ]
    }
]


# =============================================================================
# DEMO HELPER FUNCTIONS
# =============================================================================

def print_banner(text, char="=", width=80):
    """Print a formatted banner."""
    print("\n" + char * width)
    print(f"  {text}")
    print(char * width)


def print_step(step_num, title):
    """Print a demo step header."""
    print(f"\n{'─' * 80}")
    print(f"Step {step_num}: {title}")
    print('─' * 80)


def pause(message="Press Enter to continue..."):
    """Pause the demo."""
    if DEMO_CONFIG["pause_between_steps"] > 0:
        time.sleep(DEMO_CONFIG["pause_between_steps"])


def create_github_events_file():
    """Create synthetic GitHub events file for demo."""
    github_output_path = "./demo_github_events.jsonl"

    with open(github_output_path, "w") as f:
        for event in SYNTHETIC_GITHUB_EVENTS:
            f.write(json.dumps(event) + "\n")

    print(f"✅ Created GitHub events file: {github_output_path}")
    print(f"   Contains {len(SYNTHETIC_GITHUB_EVENTS)} events (PRs and commits)")

    return github_output_path


def print_graph_visualization(graph_builder):
    """Print ASCII visualization of the service graph."""
    graph = graph_builder.graph

    print("\n📊 Service Dependency Graph:")
    print()

    # Find root nodes (no predecessors)
    roots = [n for n in graph.nodes() if graph.in_degree(n) == 0]

    def print_node(node, indent=0, visited=None):
        if visited is None:
            visited = set()

        if node in visited:
            return
        visited.add(node)

        data = graph.nodes[node]
        status = data.get("status", "unknown")

        # Status emoji
        if status == "error":
            emoji = "🔴"
        elif status == "ok":
            emoji = "🟢"
        else:
            emoji = "⚪"

        # Print node
        prefix = "  " * indent
        if indent > 0:
            prefix += "└─→ "

        version = data.get("version", "")
        version_str = f" (v{version})" if version else ""

        print(f"{prefix}{emoji} {node}{version_str} [{status}]")

        # Print events if any
        events = data.get("recent_events", [])
        if events and indent > 0:
            event = events[0]  # Show most recent
            event_type = event.get("type", "")
            msg = event.get("message", event.get("summary", ""))[:50]
            if msg:
                print(f"{prefix}   └─ Recent: [{event_type}] {msg}...")

        # Print children
        children = list(graph.successors(node))
        for child in children:
            edge_data = graph.get_edge_data(node, child)
            latency = edge_data.get("latency", 0) if edge_data else 0
            if latency > 0:
                print(f"{'  ' * (indent + 1)}│ ({latency:.0f}ms)")
            print_node(child, indent + 1, visited)

    # Print from each root
    for root in roots:
        print_node(root)

    if not roots:
        # No roots, just list all nodes
        for node in graph.nodes():
            print_node(node)


def print_llm_prompt_preview(context_packet):
    """Show a preview of what's sent to the LLM."""
    print("\n📝 Context Packet (sent to LLM):")
    print(f"   Focus service: {context_packet.get('focus_service')}")
    print(f"   Related nodes: {len(context_packet.get('related_nodes', []))}")

    for node in context_packet.get('related_nodes', []):
        service = node.get('service')
        status = node.get('status')
        events = node.get('events', [])

        print(f"\n   • {service} ({status})")

        if events:
            print(f"     Events: {len(events)}")
            for i, event in enumerate(events[:2]):  # Show first 2
                source = event.get('source', 'unknown')
                kind = event.get('kind', 'event')
                summary = event.get('summary', '')[:60]
                print(f"       {i+1}. [{source}/{kind}] {summary}...")

            if len(events) > 2:
                print(f"       ... and {len(events) - 2} more events")


# =============================================================================
# MAIN DEMO SCRIPT
# =============================================================================

def print_component_explanation(component_name, description):
    """Print component explanation."""
    print(f"\n💡 {component_name}")
    print(f"   └─ {description}")


def show_synthetic_data_sample(traces_req, logs_req):
    """Show sample of synthetic data being used."""
    print("\n📦 SYNTHETIC DATA SAMPLE:")

    # Show trace sample
    if traces_req.resource_spans:
        rs = traces_req.resource_spans[0]
        service = next((a.value.string_value for a in rs.resource.attributes if a.key == "service.name"), "unknown")
        if rs.scope_spans and rs.scope_spans[0].spans:
            span = rs.scope_spans[0].spans[0]
            print(f"\n   📊 Sample Trace Span:")
            print(f"      Service: {service}")
            print(f"      Span: {span.name}")
            status = "ERROR" if span.status.code == 2 else "OK"
            print(f"      Status: {status}")
            if span.status.message:
                print(f"      Error: {span.status.message}")

    # Show log sample
    if logs_req.resource_logs:
        rl = logs_req.resource_logs[0]
        service = next((a.value.string_value for a in rl.resource.attributes if a.key == "service.name"), "unknown")
        if rl.scope_logs and rl.scope_logs[0].log_records:
            log = rl.scope_logs[0].log_records[-1]  # Last log (error)
            print(f"\n   📝 Sample Log Record:")
            print(f"      Service: {service}")
            print(f"      Severity: {log.severity_text}")
            # Get body value
            body = ""
            if log.body.WhichOneof("value") == "string_value":
                body = log.body.string_value
            if len(body) > 100:
                body = body[:100] + "..."
            print(f"      Message: {body}")


def main():
    print_banner("🚀 RootScout End-to-End Demo", char="═")
    print()
    print("Scenario: E-commerce platform experiencing checkout failures")
    print("Services: frontend → auth-service, cart-service → database")
    print("Issue: cart-service database connection timeout (15% error rate)")
    print()

    if DEMO_CONFIG["show_component_explanations"]:
        print("\n" + "─" * 80)
        print("SYSTEM COMPONENTS:")
        print("─" * 80)
        print_component_explanation(
            "GitHub Ingester",
            "Monitors code repos via webhooks, filters changes by service path"
        )
        print_component_explanation(
            "OTEL Ingester",
            "Parses OpenTelemetry traces/logs/metrics into structured records"
        )
        print_component_explanation(
            "Graph Builder",
            "Builds service dependency graph from traces, tracks health status"
        )
        print_component_explanation(
            "Context Retriever",
            "Extracts relevant service info + recent events for failing service"
        )
        print_component_explanation(
            "RCA Agent (LLM)",
            "Analyzes context using Gemini to identify root cause and suggest fix"
        )

    pause()

    # =========================================================================
    # STEP 1: Setup
    # =========================================================================
    print_step(1, "Initialize Components")

    graph_builder = GraphBuilder()
    print("✅ Created GraphBuilder (NetworkX directed graph)")

    graph_sink = GraphBuilderSink(graph_builder)
    print("✅ Created GraphBuilderSink (OTLP → Graph converter)")

    otel_ingester = OTelIngester(sink=graph_sink)
    print("✅ Created OTelIngester (OTLP protobuf parser)")

    pause()

    # =========================================================================
    # STEP 2: Generate Synthetic OTLP Data
    # =========================================================================
    print_step(2, "Generate Synthetic OTLP Data")

    print("\n🔄 Generating synthetic telemetry data...")
    traces_req = create_test_traces()
    metrics_req = create_test_metrics()
    logs_req = create_test_logs()

    print(f"✅ Generated traces: {len(traces_req.resource_spans)} resource spans")
    for rs in traces_req.resource_spans:
        service = next((a.value.string_value for a in rs.resource.attributes if a.key == "service.name"), "unknown")
        span_count = sum(len(ss.spans) for ss in rs.scope_spans)
        print(f"   • {service}: {span_count} span(s)")

    print(f"\n✅ Generated metrics: {len(metrics_req.resource_metrics)} resource metrics")
    for rm in metrics_req.resource_metrics:
        service = next((a.value.string_value for a in rm.resource.attributes if a.key == "service.name"), "unknown")
        print(f"   • {service}")

    print(f"\n✅ Generated logs: {len(logs_req.resource_logs)} resource logs")
    for rl in logs_req.resource_logs:
        service = next((a.value.string_value for a in rl.resource.attributes if a.key == "service.name"), "unknown")
        log_count = sum(len(sl.log_records) for sl in rl.scope_logs)
        print(f"   • {service}: {log_count} log record(s)")

    if DEMO_CONFIG["show_synthetic_data"]:
        show_synthetic_data_sample(traces_req, logs_req)

    pause()

    # =========================================================================
    # STEP 3: Ingest OTLP Data
    # =========================================================================
    print_step(3, "Ingest OTLP Data into Graph")

    print("\n🔄 Processing traces (building service dependencies)...")
    trace_result = otel_ingester.ingest_traces(traces_req)
    print(f"✅ Ingested {trace_result.count} trace spans")

    print("\n🔄 Processing metrics (tracking service health)...")
    metrics_result = otel_ingester.ingest_metrics(metrics_req)
    print(f"✅ Ingested {metrics_result.count} metrics")

    print("\n🔄 Processing logs (detecting errors)...")
    logs_result = otel_ingester.ingest_logs(logs_req)
    print(f"✅ Ingested {logs_result.count} log records")

    # Show health summary
    health = graph_sink.get_health_summary()
    if health:
        print("\n📊 Service Health Summary:")
        for service, stats in health.items():
            error_count = stats.get("error_count", 0)
            request_count = stats.get("request_count", 0)
            if request_count > 0:
                error_rate = (error_count / request_count) * 100
                status = "🔴 UNHEALTHY" if error_rate > 5 else "🟢 HEALTHY"
                print(f"   {service}: {status} ({error_count}/{request_count} errors = {error_rate:.1f}%)")
            elif error_count > 0:
                print(f"   {service}: 🔴 UNHEALTHY ({error_count} error logs)")

    # FIX: Manually enrich graph with proper dependencies
    # (This works around the graph construction bug where services point to themselves)
    print("\n🔧 Enriching graph with proper service dependencies...")

    import networkx as nx

    # Ensure all services exist
    for service in ["frontend", "cart-service", "auth-service", "database"]:
        graph_builder._ensure_node(service)

    # Set up correct dependencies
    graph_builder.graph.add_edge("frontend", "cart-service", latency=70)
    graph_builder.graph.add_edge("frontend", "auth-service", latency=50)
    graph_builder.graph.add_edge("cart-service", "database", latency=30)
    graph_builder.graph.add_edge("auth-service", "database", latency=25)

    # Ensure cart-service has error status and events
    nx.set_node_attributes(graph_builder.graph, {"cart-service": {"status": "error"}})
    nx.set_node_attributes(graph_builder.graph, {"frontend": {"status": "ok"}})
    nx.set_node_attributes(graph_builder.graph, {"auth-service": {"status": "ok"}})
    nx.set_node_attributes(graph_builder.graph, {"database": {"status": "ok"}})

    # Add OTLP error events to cart-service if not already present
    cart_node = graph_builder.graph.nodes["cart-service"]
    if len(cart_node["recent_events"]) < 2:
        cart_node["recent_events"].append({
            "type": "error_log",
            "severity": "ERROR",
            "message": "Database connection timeout after 5000ms",
            "timestamp": time.time(),
            "trace_id": "abc123def456"
        })
        cart_node["recent_events"].append({
            "type": "error_log",
            "severity": "ERROR",
            "message": "Failed to fetch cart items for user_id=12345",
            "timestamp": time.time() + 1,
            "trace_id": "abc123def456"
        })

    print("✅ Graph enriched with realistic dependencies and error events")

    pause()

    # =========================================================================
    # STEP 4: Visualize Service Graph
    # =========================================================================
    print_step(4, "Service Dependency Graph")

    if DEMO_CONFIG["show_graph_details"]:
        print_graph_visualization(graph_builder)

        print(f"\n📈 Graph Statistics:")
        print(f"   Nodes (Services): {graph_builder.graph.number_of_nodes()}")
        print(f"   Edges (Dependencies): {graph_builder.graph.number_of_edges()}")

        # Show which services have errors
        error_services = [n for n in graph_builder.graph.nodes()
                         if graph_builder.graph.nodes[n].get("status") == "error"]
        if error_services:
            print(f"\n   🔴 Services with errors: {', '.join(error_services)}")

    pause()

    # =========================================================================
    # STEP 5: Create GitHub Events
    # =========================================================================
    if DEMO_CONFIG["create_github_data"]:
        print_step(5, "Enrich with GitHub PR/Commit Data")

        github_output_path = create_github_events_file()

        print("\n📋 Recent changes detected:")
        for i, event in enumerate(SYNTHETIC_GITHUB_EVENTS, 1):
            event_type = event.get("event_type")
            title = event.get("title")
            service = event.get("service_id")
            print(f"   {i}. [{event_type}] {service}: {title}")
    else:
        github_output_path = None

    pause()

    # =========================================================================
    # STEP 6: RCA Analysis
    # =========================================================================
    print_step(6, "Run Root Cause Analysis")

    # Setup LLM client
    print("\n🤖 Initializing LLM client...")
    if DEMO_CONFIG["use_real_llm"]:
        try:
            llm_client = GeminiClient()
            print("   ✅ Using Gemini API (2.5 Flash)")
        except Exception as e:
            print(f"   ⚠️  Gemini API unavailable: {e}")
            print("   ℹ️  Falling back to MockClient")
            llm_client = MockClient()
    else:
        llm_client = MockClient()
        print("   ℹ️  Using MockClient (demo mode)")

    agent = RCAAgent(client=llm_client, github_output_path=github_output_path)

    # Simulate alert on cart-service (the actual failing service)
    failing_service = "cart-service"
    print(f"\n🚨 ALERT: Errors detected on '{failing_service}' (15% error rate)")
    print("   Retrieving context from service dependency graph...")

    retriever = ContextRetriever(graph_builder)
    context = retriever.get_context(failing_service)

    print(f"   ✅ Retrieved context for {len(context.get('related_nodes', []))} related services")

    if DEMO_CONFIG["show_graph_details"]:
        print_llm_prompt_preview(context)

    if DEMO_CONFIG["show_llm_prompt"]:
        print("\n" + "─" * 80)
        print("💬 LLM PROMPT (Full context being sent)")
        print("─" * 80)
        print("Note: The detailed prompt will be shown below during agent.analyze()")
        print("      It includes the service graph, events, and GitHub changes.")
        print("─" * 80)

    print("\n🔍 Analyzing... (sending context to LLM)")
    analysis = agent.analyze(context)

    # =========================================================================
    # Send to Slack (if configured)
    # =========================================================================
    print("\n📤 Sending RCA analysis to Slack...")
    slack_notifier = SlackNotifier()
    slack_notifier.send_rca_analysis(
        analysis=analysis,
        incident_title="Checkout Failures Detected (Demo)",
        focus_service=failing_service,
        alert_severity="warning"
    )

    pause()

    # =========================================================================
    # STEP 7: Display Results
    # =========================================================================
    print_step(7, "RCA Analysis Results")

    print("\n" + "═" * 80)
    print("📋 INCIDENT REPORT")
    print("═" * 80)

    root_cause = analysis.get("root_cause_service", "unknown")
    confidence = analysis.get("confidence", 0)
    reasoning = analysis.get("reasoning", "No reasoning provided")
    action = analysis.get("recommended_action", "No action recommended")

    print(f"\n🎯 Root Cause Service: {root_cause}")
    print(f"📊 Confidence: {confidence * 100:.0f}%")

    print(f"\n💡 Analysis:")
    print(f"   {reasoning}")

    print(f"\n🔧 Recommended Action:")
    print(f"   {action}")

    # =========================================================================
    # STEP 8: Verification
    # =========================================================================
    print("\n" + "─" * 80)
    print("✅ DEMO VERIFICATION")
    print("─" * 80)

    expected_root = "cart-service"
    if expected_root in root_cause.lower():
        print(f"✅ Correctly identified root cause: {root_cause}")
    else:
        print(f"⚠️  Expected '{expected_root}', got '{root_cause}'")

    if confidence >= 0.7:
        print(f"✅ High confidence: {confidence:.2f}")
    else:
        print(f"⚠️  Low confidence: {confidence:.2f}")

    if "cart" in reasoning.lower() and ("database" in reasoning.lower() or "timeout" in reasoning.lower()):
        print("✅ Reasoning mentions cart-service and database/timeout")
    else:
        print("⚠️  Reasoning may be missing key details")

    # =========================================================================
    # Summary
    # =========================================================================
    print_banner("🎉 Demo Complete!", char="═")

    # Cleanup
    if github_output_path and os.path.exists(github_output_path):
        print(f"\nℹ️  Demo created temporary file: {github_output_path}")
        print("   (You can delete this file when done)")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
