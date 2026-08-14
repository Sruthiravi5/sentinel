# Sentinel — Self-Healing Credit Data Quality Pipeline

A financial data pipeline that ingests real credit delinquency data, transforms it autonomously, checks it against deterministic quality rules, quarantines anything that fails, and has an LLM agent explain why — all on a schedule, with zero manual intervention.

**Architecture claim:** Deterministic SQL checks decide whether data is trustworthy. The LLM agent only explains failures a rule already caught — so a hallucination can produce a bad sentence, never a bad decision.

---

## What it does

1. **Ingests** credit card delinquency, consumer credit, and unemployment data from FRED (Federal Reserve Economic Data)
2. **Lands** it in versioned AWS S3 as an immutable raw layer
3. **Transforms** it autonomously through Snowflake Dynamic Tables — RAW → CLEAN → MART, self-refreshing hourly, no manual triggers
4. **Checks** every refresh against deterministic rules: null checks, range checks, and 2-sigma statistical anomaly detection
5. **Quarantines** anything that fails, so bad data never reaches the dashboard-facing view
6. **Explains** each failure in plain English via an LLM agent (Python + Groq) — triggered only after a rule has already failed, never deciding pass/fail itself
7. **Orchestrates** the whole chain via Apache Airflow, running hourly with zero manual steps
8. **Visualizes** the result in a live Tableau dashboard connected directly to Snowflake

## Architecture

~~~
FRED API → S3 (raw, versioned)
         → Snowflake RAW (exact copy)
         → Snowflake CLEAN (Dynamic Table, typed + validated)
         → Snowflake MART (Dynamic Table, business metrics)
              ├─→ credit_health_clean (view, dashboard-safe)
              └─→ quarantine (failed rows + reason)
                        ↓
              quality_log (audit trail, every check run)
                        ↓
              incident_log (LLM root-cause narrative)
                        ↓
              Tableau (live dashboard)

Orchestrated hourly by Apache Airflow.
~~~

## Why this design

- **RAW/CLEAN/MART separation** gives a provable lineage trail — every number on the dashboard can be traced back to its raw source file and load timestamp.
- **Dynamic Tables are read-only** by design, so the pipeline can't simply delete bad rows from the transformed layer. Instead, a `quarantine` table holds failed rows, and a `credit_health_clean` *view* excludes anything in quarantine — the autonomous data layer stays untouched, and a separate "safe to show" layer enforces trust.
- **The LLM never decides pass/fail.** All checks are deterministic SQL. The agent is invoked only after a check has already failed, and its only job is to explain the failure in plain English using recent data as context.
- **Airflow orchestrates cross-system steps** (Snowflake SQL → external Python/LLM API) that Snowflake's own Task scheduler can't chain natively.

## Stack

Python · AWS S3 · Snowflake (Dynamic Tables, Tasks) · Groq (LLM) · Apache Airflow · Tableau

## Known limitations

- The range check currently re-flags the same underlying bad row on every scheduled run it's still present, rather than only once — a production version would deduplicate by row rather than by check execution.
- Cortex (Snowflake's native LLM function) was the original plan but is restricted on trial accounts without a payment method; the agent runs via an external LLM API (Groq) instead, called from Python and orchestrated by Airflow.
- Tableau's dashboard reflects a snapshot at last refresh, not a live stream — the *pipeline* is autonomous and self-healing; the dashboard is a window into it that updates when refreshed, same as most BI tools.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Configure AWS CLI (`aws configure`) and set Snowflake/Groq credentials in a `.env` file before running `ingest.py`, `upload.py`, or `investigate.py`.
