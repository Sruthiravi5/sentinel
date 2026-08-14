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

