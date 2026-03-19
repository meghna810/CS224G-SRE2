#!/usr/bin/env python3
"""
show_synthetic_data.py - Display synthetic OTEL data in human-readable format

Run: python show_synthetic_data.py
"""

import json
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from RootScout.test_otel_data import create_test_traces, create_test_metrics, create_test_logs
from RootScout.otel_ingester import OTelIngester, TelemetrySink


class JSONSink(TelemetrySink):
    """Collects records into a list for display."""
    def __init__(self):
        self.records = []

    def emit(self, record):
        self.records.append(record)


def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def format_timestamp(nano):
    """Convert nanoseconds to readable datetime."""
    if nano:
        seconds = nano / 1e9
        return datetime.fromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return "N/A"


def print_traces(records):
    print_section("📊 TRACES (Distributed Request Flow)")

    # Group by service
    by_service = {}
    for r in records:
        service = r.get('service', 'unknown')
        if service not in by_service:
            by_service[service] = []
        by_service[service].append(r)

    for service, spans in by_service.items():
        print(f"\n🔹 Service: {service}")
        print(f"   Version: {spans[0].get('service_version', 'N/A')}")
        print(f"   Environment: {spans[0].get('environment', 'N/A')}")
        print(f"   Spans: {len(spans)}")

        for i, span in enumerate(spans, 1):
            start_time = format_timestamp(span['start_time_unix_nano'])
            end_time = format_timestamp(span['end_time_unix_nano'])
            duration_ms = (span['end_time_unix_nano'] - span['start_time_unix_nano']) / 1e6

            status = "✅ OK" if span.get('status_code') == 1 else "❌ ERROR"

            print(f"\n      Span #{i}: {span['name']}")
            print(f"      ├─ Status: {status}")
            if span.get('status_message'):
                print(f"      ├─ Error: {span['status_message']}")
            print(f"      ├─ Duration: {duration_ms:.0f}ms")
            print(f"      ├─ Start: {start_time}")
            print(f"      ├─ Trace ID: {span['trace_id'][:16]}...")
            print(f"      └─ Span ID: {span['span_id'][:16]}...")

            # Show interesting attributes
            attrs = span.get('span_attributes', {})
            if attrs:
                print(f"      Attributes:")
                for key in ['http.method', 'http.route', 'http.status_code', 'error.type', 'error.message', 'db.system', 'db.statement']:
                    if key in attrs:
                        print(f"         • {key}: {attrs[key]}")


def print_metrics(records):
    print_section("📈 METRICS (Performance & Health)")

    if not records:
        print("\n   (No metric data generated in current implementation)")
        return

    for r in records:
        service = r.get('service', 'unknown')
        metric_name = r.get('name', 'unknown')
        metric_type = r.get('type', 'unknown')

        print(f"\n🔹 Service: {service}")
        print(f"   Metric: {metric_name}")
        print(f"   Type: {metric_type}")
        print(f"   Description: {r.get('description', 'N/A')}")
        print(f"   Points: {len(r.get('points', []))}")


def print_logs(records):
    print_section("📝 LOGS (Application Events)")

    # Group by service
    by_service = {}
    for r in records:
        service = r.get('service', 'unknown')
        if service not in by_service:
            by_service[service] = []
        by_service[service].append(r)

    for service, logs in by_service.items():
        print(f"\n🔹 Service: {service}")
        print(f"   Version: {logs[0].get('service_version', 'N/A')}")
        print(f"   Log Records: {len(logs)}")

        for i, log in enumerate(logs, 1):
            timestamp = format_timestamp(log['time_unix_nano'])
            severity = log.get('severity_text', 'INFO')
            body = log.get('body', 'N/A')

            # Emoji based on severity
            if severity == "ERROR":
                emoji = "❌"
            elif severity == "WARN":
                emoji = "⚠️"
            else:
                emoji = "ℹ️"

            print(f"\n      Log #{i} {emoji} [{severity}]")
            print(f"      ├─ Time: {timestamp}")
            print(f"      ├─ Message: {body}")

            # Show trace correlation
            if log.get('trace_id'):
                print(f"      ├─ Trace ID: {log['trace_id'][:16]}...")
            if log.get('span_id'):
                print(f"      ├─ Span ID: {log['span_id'][:16]}...")

            # Show interesting attributes
            attrs = log.get('attributes', {})
            if attrs:
                print(f"      └─ Attributes:")
                for key, value in attrs.items():
                    print(f"         • {key}: {value}")


def print_data_characteristics():
    print_section("🔍 DATA CHARACTERISTICS")

    print("""
The synthetic data is:

1. ⚡ SEMI-DYNAMIC
   • Timestamps: Generated fresh each time (uses current time)
   • Trace/Span IDs: Static (hardcoded) for reproducibility
   • Values: Static (same errors, same services, same messages)

2. 📊 REPRODUCIBLE
   • Always generates the same scenario: cart-service database timeout
   • Predictable for testing and demos
   • Good for validating RCA logic

3. 🎯 REALISTIC STRUCTURE
   • Uses real OpenTelemetry Protocol (OTLP) protobuf format
   • Follows OTEL semantic conventions (service.name, http.*, db.*, etc.)
   • Mimics real distributed traces with parent-child relationships

4. 🔄 SCENARIO-BASED
   Current scenario: "E-commerce checkout failure"
   • frontend service → cart-service (timeout)
   • frontend service → auth-service (success)
   • cart-service → database (timeout)
   • Connection pool exhaustion (10/10 connections used)

5. 📦 WHAT'S INCLUDED
   • 5 spans across 3 services (frontend, cart-service, auth-service)
   • 2 log records (WARN + ERROR from cart-service)
   • Minimal metrics (placeholder)
   • Correlated via trace_id to show the full request flow

To make it FULLY DYNAMIC (different data each run), you would:
   • Randomize user IDs, request paths, error rates
   • Vary latencies and error types
   • Generate multiple traces (not just one)
   • Use random.choice() for different failure scenarios
""")


def main():
    print("\n" + "=" * 80)
    print("  🧪 SYNTHETIC OTEL DATA VIEWER")
    print("=" * 80)
    print("\nGenerating synthetic OpenTelemetry data...")

    # Generate the data
    traces_req = create_test_traces()
    metrics_req = create_test_metrics()
    logs_req = create_test_logs()

    # Parse into human-readable format
    sink = JSONSink()
    ingester = OTelIngester(sink=sink)

    trace_result = ingester.ingest_traces(traces_req)
    trace_records = sink.records.copy()

    sink.records = []
    metrics_result = ingester.ingest_metrics(metrics_req)
    metrics_records = sink.records.copy()

    sink.records = []
    logs_result = ingester.ingest_logs(logs_req)
    logs_records = sink.records.copy()

    # Display
    print(f"\n✅ Generated:")
    print(f"   • {trace_result.count} trace spans")
    print(f"   • {metrics_result.count} metrics")
    print(f"   • {logs_result.count} log records")

    print_traces(trace_records)
    print_metrics(metrics_records)
    print_logs(logs_records)
    print_data_characteristics()

    # Option to save to file
    print("\n" + "=" * 80)
    print("💾 SAVE TO FILE?")
    print("=" * 80)
    print("\nTo save raw JSON data, uncomment the code below in show_synthetic_data.py")
    print("Or run with: python show_synthetic_data.py > output.txt")

    # Uncomment to save:
    # with open('synthetic_traces.json', 'w') as f:
    #     for record in trace_records:
    #         f.write(json.dumps(record) + '\n')
    # print("\n✅ Saved traces to synthetic_traces.json")


if __name__ == "__main__":
    main()
