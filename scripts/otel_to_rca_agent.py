#!/usr/bin/env python3
"""
End-to-end RootScout pipeline for automated root-cause analysis.

Generates synthetic OTLP telemetry, ingests it via the OTelIngester,
summarizes distributed traces into a compact analysis packet,
and invokes a Claude-based SRE agent to produce a structured RCA report.
"""

from __future__ import annotations

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from RootScout.otel_ingester import OTelIngester, TelemetrySink
from RootScout.test_otel_data import create_test_traces, create_test_metrics, create_test_logs



# Config 

CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = os.getenv("ANTHROPIC_VERSION", "2023-06-01")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5")

OUT_TRACE_PACKET = os.getenv("OUT_TRACE_PACKET", "./trace_packet.json")
OUT_RCA_REPORT = os.getenv("OUT_RCA_REPORT", "./rca_report.json")

MAX_TRACES_TO_INCLUDE = int(os.getenv("MAX_TRACES_TO_INCLUDE", "50"))
MAX_SPANS_PER_TRACE_EXAMPLE = int(os.getenv("MAX_SPANS_PER_TRACE_EXAMPLE", "40"))
MAX_BAD_TRACES_TO_INCLUDE = int(os.getenv("MAX_BAD_TRACES_TO_INCLUDE", "5"))

SLOW_SPAN_MS = float(os.getenv("SLOW_SPAN_MS", "1500"))

CLAUDE_MAX_TOKENS = int(os.getenv("CLAUDE_MAX_TOKENS", "1200"))
CLAUDE_TIMEOUT_S = int(os.getenv("CLAUDE_TIMEOUT_S", "60"))



# Helpers (records produced by OTelIngester)

def _ns_to_ms(ns: int) -> float:
    return ns / 1_000_000.0


def _span_latency_ms(span_record: Dict[str, Any]) -> float:
    s = int(span_record.get("start_time_unix_nano") or 0)
    e = int(span_record.get("end_time_unix_nano") or 0)
    if e >= s and s > 0:
        return _ns_to_ms(e - s)
    return 0.0


def _status_from_status_code(status_code: Optional[int]) -> str:
    if status_code == 2:
        return "ERROR"
    if status_code == 1:
        return "OK"
    return "UNSET"


def _pctl(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    xs2 = sorted(xs)
    idx = int(q * (len(xs2) - 1))
    return xs2[idx]


# ClaudeSink: buffers ingester output, builds packet, calls Claude

class ClaudeSink(TelemetrySink):
    """
    Receives records emitted by OTelIngester.emit(record).
    Buffers trace/log/metric records and later summarizes traces and calls Claude.
    """

    def __init__(self) -> None:
        self.trace_spans: List[Dict[str, Any]] = []
        self.logs: List[Dict[str, Any]] = []
        self.metrics: List[Dict[str, Any]] = []

    def emit(self, record: Dict[str, Any]) -> None:
        sig = record.get("signal")
        if sig == "trace":
            self.trace_spans.append(record)
        elif sig == "log":
            self.logs.append(record)
        elif sig == "metric":
            self.metrics.append(record)
        else:
            pass

    # packet building 

    def _group_spans_by_trace(self) -> Dict[str, List[Dict[str, Any]]]:
        by_trace: Dict[str, List[Dict[str, Any]]] = {}
        for sp in self.trace_spans:
            tid = sp.get("trace_id")
            if not tid:
                continue
            by_trace.setdefault(tid, []).append(sp)

        # sort spans inside each trace by start time
        for tid in list(by_trace.keys()):
            by_trace[tid].sort(key=lambda r: int(r.get("start_time_unix_nano") or 0))
        return by_trace

    def _infer_edges(self, trace_spans: List[Dict[str, Any]]) -> List[Tuple[str, str, float, str]]:
        """
        Infer caller->callee edges via parent_span_id:
          parent.service -> child.service if different

        Returns: (caller_service, callee_service, callee_latency_ms, callee_status)
        """
        idmap = {sp.get("span_id"): sp for sp in trace_spans if sp.get("span_id")}
        edges: List[Tuple[str, str, float, str]] = []
        for child in trace_spans:
            parent_id = child.get("parent_span_id")
            if not parent_id:
                continue
            parent = idmap.get(parent_id)
            if not parent:
                continue
            a = parent.get("service") or "unknown"
            b = child.get("service") or "unknown"
            if a != b:
                edges.append((a, b, _span_latency_ms(child), _status_from_status_code(child.get("status_code"))))
        return edges

    def build_trace_packet(self) -> Dict[str, Any]:
        by_trace = self._group_spans_by_trace()
        trace_ids = list(by_trace.keys())[:MAX_TRACES_TO_INCLUDE]

        # Per-service
        svc_lat: Dict[str, List[float]] = {}
        svc_err: Dict[str, int] = {}

        # Per-edge
        edge_lat: Dict[Tuple[str, str], List[float]] = {}
        edge_err: Dict[Tuple[str, str], int] = {}

        bad_traces: List[Dict[str, Any]] = []

        for tid in trace_ids:
            spans = by_trace[tid]

            for sp in spans:
                svc = sp.get("service") or "unknown"
                lat = _span_latency_ms(sp)
                svc_lat.setdefault(svc, []).append(lat)

                if _status_from_status_code(sp.get("status_code")) == "ERROR":
                    svc_err[svc] = svc_err.get(svc, 0) + 1

            edges = self._infer_edges(spans)
            for (a, b, lat, st) in edges:
                edge_lat.setdefault((a, b), []).append(lat)
                if st == "ERROR":
                    edge_err[(a, b)] = edge_err.get((a, b), 0) + 1

            has_error = any(_status_from_status_code(sp.get("status_code")) == "ERROR" for sp in spans)
            has_slow = any(_span_latency_ms(sp) >= SLOW_SPAN_MS for sp in spans)

            if has_error or has_slow:
                chain = []
                for sp in spans[:MAX_SPANS_PER_TRACE_EXAMPLE]:
                    attrs = sp.get("span_attributes") or {}
                    chain.append({
                        "service": sp.get("service"),
                        "span": sp.get("name"),
                        "latency_ms": round(_span_latency_ms(sp), 2),
                        "status": _status_from_status_code(sp.get("status_code")),
                        "http.method": attrs.get("http.method"),
                        "http.route": attrs.get("http.route") or attrs.get("http.target"),
                        "http.status_code": attrs.get("http.status_code"),
                        "rpc.system": attrs.get("rpc.system"),
                        "exception.message": attrs.get("exception.message"),
                    })
                bad_traces.append({"trace_id": tid, "span_chain": chain})

        # rank services
        top_services = []
        for svc, lats in svc_lat.items():
            top_services.append({
                "service": svc,
                "count_spans": len(lats),
                "p50_ms": round(_pctl(lats, 0.50), 2),
                "p95_ms": round(_pctl(lats, 0.95), 2),
                "errors": int(svc_err.get(svc, 0)),
            })
        top_services.sort(key=lambda r: (r["errors"], r["p95_ms"]), reverse=True)

        # rank edges
        top_edges = []
        for (a, b), lats in edge_lat.items():
            top_edges.append({
                "caller": a,
                "callee": b,
                "count_calls": len(lats),
                "p50_ms": round(_pctl(lats, 0.50), 2),
                "p95_ms": round(_pctl(lats, 0.95), 2),
                "errors": int(edge_err.get((a, b), 0)),
            })
        top_edges.sort(key=lambda r: (r["errors"], r["p95_ms"]), reverse=True)

        log_samples = []
        for lr in self.logs[:50]:
            log_samples.append({
                "service": lr.get("service"),
                "severity_text": lr.get("severity_text"),
                "body": lr.get("body"),
                "trace_id": lr.get("trace_id"),
                "span_id": lr.get("span_id"),
                "attributes": lr.get("attributes", {}),
            })

        packet = {
            "schema_version": "rootscout.no_graph.synthetic.v1",
            "generated_at_unix": time.time(),
            "counts": {
                "trace_span_records": len(self.trace_spans),
                "log_records": len(self.logs),
                "metric_records": len(self.metrics),
                "sampled_traces": len(trace_ids),
            },
            "top_services": top_services[:10],
            "top_edges": top_edges[:15],
            "bad_traces": bad_traces[:MAX_BAD_TRACES_TO_INCLUDE],
            "log_samples": log_samples[:10],
        }
        return packet


    # Claude call
    
    def call_claude(self, trace_packet: Dict[str, Any]) -> Dict[str, Any]:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("Missing ANTHROPIC_API_KEY env var")

        system_prompt = (
            "You are an expert SRE root-cause analyst.\n"
            "You will be given a structured summary derived from distributed traces/logs for an e-commerce app.\n"
            "Return JSON ONLY with exactly these keys:\n"
            "root_cause_service (string), confidence (0-1 number), reasoning (string), recommended_action (string),\n"
            "evidence (array of short strings).\n"
            "Ground your answer in the trace_packet (errors, slow edges, exemplar traces, log samples)."
        )

        user_text = "Analyze this trace_packet and output the RCA JSON.\n\n" + json.dumps(trace_packet, indent=2)

        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": CLAUDE_MAX_TOKENS,
            "system": system_prompt,
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": user_text}]}
            ],
        }

        headers = {
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        }

        resp = requests.post(CLAUDE_API_URL, headers=headers, json=payload, timeout=CLAUDE_TIMEOUT_S)
        resp.raise_for_status()
        data = resp.json()

        # Claude returns JSON
        blocks = data.get("content", []) or []
        text = ""
        for block in blocks:
            if block.get("type") == "text":
                text += block.get("text", "")

        text = (text or "").strip()

        fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if fenced:
            text = fenced.group(1).strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # fallback
            m = re.search(r"\{.*\}", text, flags=re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(0))
                except json.JSONDecodeError:
                    pass

            return {"_error": "Claude did not return valid JSON", "_raw": text, "_model": CLAUDE_MODEL}


# Main: synthetic -> ingester -> sink -> claude

def main() -> None:
    print("Synthetic OTLP -> Claude agent")

    # create sink and ingester
    sink = ClaudeSink()
    ingester = OTelIngester(sink=sink)

    # generate synthetic OTLP protobuf requests
    traces_req = create_test_traces()
    metrics_req = create_test_metrics()
    logs_req = create_test_logs()

    # ingest 
    print("Ingesting synthetic traces...")
    tr = ingester.ingest_traces(traces_req)
    print(f"{tr.kind}: {tr.count} records")

    print("Ingesting synthetic metrics...")
    mr = ingester.ingest_metrics(metrics_req)
    print(f"{mr.kind}: {mr.count} records")

    print("Ingesting synthetic logs...")
    lr = ingester.ingest_logs(logs_req)
    print(f"{lr.kind}: {lr.count} records")

    # build packet
    print("Building trace packet...")
    packet = sink.build_trace_packet()

    with open(OUT_TRACE_PACKET, "w") as f:
        json.dump(packet, f, indent=2)
    print(f"Wrote {OUT_TRACE_PACKET}")

    # call Claude
    print(f"Calling Claude: {CLAUDE_MODEL}")
    report = sink.call_claude(packet)

    with open(OUT_RCA_REPORT, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Wrote {OUT_RCA_REPORT}")

    # print summary
    print("\n--- RCA ---")
    print("Root cause:", report.get("root_cause_service"))
    print("Confidence:", report.get("confidence"))
    print("Action:", report.get("recommended_action"))


if __name__ == "__main__":
    main()
