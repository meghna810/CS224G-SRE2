# RootScout

RootScout is an AI on-call agent that diagnosis production incidents automatically. 

Check us out at : [rootscout](https://cs-224-g-sre-2-mr8b.vercel.app)

---

## Prerequisites

- Python 3.9+
- Gemini API key from [Google AI Studio](https://aistudio.google.com/) and/or Anthropic API key
- Set `SLACK_BOT_TOKEN=xoxb-...` in your `.env` file to post real Slack messages (optional — all demos work in dry-run mode without it)

## Install

```bash
git clone https://github.com/asthamohta/CS224G-SRE.git
cd CS224G-SRE
pip install -r requirements.txt
pip install -r requirements_eval.txt
```

## Configure

```bash
cp .env.example .env
# Set GEMINI_API_KEY and/or ANTHROPIC_API_KEY in .env
```

---

## Evaluation

Three evaluation tracks test whether the agent correctly identifies the root cause component and reason. Scoring follows the [OpenRCA](https://github.com/microsoft/OpenRCA) protocol: exact string match on component, cosine similarity ≥ 0.50 (all-MiniLM-L6-v2) on reason.

### Eval 1 — Synthetic benchmark

Ten hand-crafted scenarios with known topology and injected faults.

```bash
python eval/run_eval.py              # all 10 scenarios
python eval/run_eval.py --mock       # no API key needed
python eval/run_eval.py --difficulty easy
```

---

### Eval 2 — OpenRCA (real Bank telemetry)

27 incidents from the [OpenRCA Bank dataset](https://github.com/microsoft/OpenRCA) — a Java-based banking microservices system with 14 pods.

**Data setup:** Download the Bank dataset and place it at `Bank/` in the project root:

```
Bank/
  query.csv
  record.csv
  telemetry/
    2021_03_04/
      metric/metric_container.csv
      log/log_service.csv
    2021_03_06/ ...
```

```bash
python eval/run_openrca_eval.py              # 27 Bank incidents
python eval/run_openrca_eval.py --mock       # no API key needed
python eval/run_openrca_eval.py --n 5        # quick test with 5 incidents
python eval/run_openrca_eval.py --bank-dir /path/to/Bank
```

---

### Eval 3 — RCAEvals (RE3-OB code-level faults)

Code-level faults injected into the Online Boutique microservices system from the [RCAEval benchmark](https://github.com/phamquiluan/RCAEval). Each case includes metric time series, logs with stack traces, and a known injection time.

**Data setup:**

```bash
git clone https://github.com/phamquiluan/RCAEval /tmp/RCAEval
cd /tmp/RCAEval && pip install -e .
python main.py --download --dataset RE3-OB
cp -r data/RE3-OB <project_root>/data/RE3-OB
```

```bash
python eval/run_rcaeval_eval.py              # all RE3-OB cases
python eval/run_rcaeval_eval.py --mock       # no API key needed
python eval/run_rcaeval_eval.py --n 5        # quick sanity check
python eval/run_rcaeval_eval.py --fault-types F1 F3
python eval/run_rcaeval_eval.py --model claude-opus
```

---

## Demo — End-to-End with Slack

Runs a full end-to-end scenario using RE3-OB telemetry: Slack alert fires → RootScout builds the causal graph → LLM identifies root cause → Slack RCA report is posted.

**Prerequisite:** RE3-OB data downloaded (see Eval 3 above).

```bash
# Dry-run (no Slack token needed):
python demo/demo_Rcaevals.py

# With real Slack:
SLACK_BOT_TOKEN=xoxb-... SLACK_ALERT_CHANNEL=#incidents python demo/demo_Rcaevals.py
```

---

## Results

| Dataset | Strengths | Limitations | Best Model | Component match | RCA cosine similarity |
|---|---|---|---|---|---|
| OpenRCA (Microsoft Bank) | Emulates real-life production incidents | Missing codebase | Claude Opus 4.6 | 45% | 18% |
| RCAEvals (RE3-OB) | Telemetry + codebase present; deeper code-level signals | Doesn't emulate real-life incidents well | Claude Opus 4.6 | 56% | 28% |
| Synthetic data | Easy to generate; controllable fault scenarios | Doesn't emulate real-life incidents | Claude Opus 4.6 | 100% | 91% |

---

## Known limitations

- **Datetime scoring on OpenRCA is not genuine.** The fault timestamp is taken directly from `record.csv` rather than predicted by the agent, so datetime criteria always pass.
- **No trace topology on real data.** `trace_span.csv` uses internal container IDs that don't map to pod names, so a static hand-written topology is used instead.
- **Noisy anomaly detection.** KPI thresholds are heuristic; during real incidents many pods spike simultaneously, making causal isolation harder.
- **Single system.** Only the Bank system is evaluated for OpenRCA. The dataset also includes Telecom and Market.
