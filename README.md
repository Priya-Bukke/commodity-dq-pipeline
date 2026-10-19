# Commodity Price Data-Quality Pipeline

A small, production-style data pipeline for European **power and gas price
assessments**, built around one question: *can the business trust this data?*

Python + SQL, config-driven validation, quarantine instead of silent drops,
auditable results, tests and CI — plus an AI-assisted rule suggester with
guardrails on LLM output.

```
 raw CSVs ──► raw_prices ──► stg_prices ──► DQ rules (YAML) ──┬─► clean_prices ──► marts
 (monthly)   (idempotent    (typed,         compiled to SQL   │   (+ warning tags)   daily / weekly /
              ingest,        standardised)                     │                      spark spread
              SHA-256 log)                                     └─► quarantine_prices
                                                                   (+ reasons)
                              dq_runs / dq_results / dq_failures  ──► dq_report.md / .json
```

## Quick start

```bash
pip install -r requirements.txt
python -m pipeline.sample_data --out data/raw --days 365   # synthetic data with known defects
python -m pipeline.run                                      # full pipeline -> warehouse.db, reports/
python -m pipeline.suggest_rules --db warehouse.db          # auto-proposed rules -> rules/rules_suggested.yaml
python -m pytest -q                                         # 15 unit + end-to-end tests
```

Only dependency: PyYAML (SQLite is in the Python standard library).

## What each layer does

| Layer | File | Key idea |
|---|---|---|
| Ingest | `pipeline/ingest.py` | Each file is fingerprinted (SHA-256) in `load_log`; re-runs never double-load. One transaction per file. Lineage columns `_batch_id`, `_source_file`, `_loaded_at`. |
| Staging | `sql/staging.sql` | Strict ISO date parsing, numeric check before `CAST` (SQLite turns `'n/a'` into `0.0` otherwise), EUR/kWh → EUR/MWh conversion, trimming / upper-casing of codes. Nothing is dropped. |
| Validation | `pipeline/validate.py`, `rules/prices.yaml` | Rules declared in YAML, schema-checked, compiled to parameterised SQL. 7 rule types: `not_null`, `unique`, `range`, `accepted_values`, `outlier_zscore`, `row_count`, `freshness`. |
| Routing | `split_clean_quarantine()` | `error` rules → row quarantined with reasons. `warn` rules → row kept but tagged in `dq_warnings`. |
| Marts | `sql/marts.sql` | Daily prices (unique index = data contract), weekly aggregates, spark spread (power − gas / 50% efficiency, excl. carbon) via a hub reference table. |
| Reporting | `pipeline/report.py` | Markdown + JSON report per run; full history in `dq_runs`, `dq_results`, `dq_failures`. |
| Orchestration | `pipeline/run.py` | Non-zero exit if data is stale or quarantine rate > threshold, so a scheduler / CI job can alert. |

## Design decisions worth discussing

- **Quarantine, don't delete.** Bad rows stay queryable with the exact rule that rejected them, so analysts can fix the source instead of losing data.
- **Statistical outliers are warnings, not errors.** Negative German power prices are real (renewable surplus) but look like outliers. Dropping them would corrupt the data product, so the z-score rule only tags rows for review.
- **Duplicates vs. revisions.** The uniqueness rule keeps the first-loaded row and quarantines later copies, including conflicting revisions with a different price — a policy the business should confirm.
- **Dynamic SQL is guarded.** Column names are checked against the real table schema and a strict identifier regex; values are passed as SQL parameters.
- **Table-level failures stop the run; row-level failures don't.** One bad row should not block 1,400 good ones, but stale data should block everything.

## AI-assisted rule suggestions

`pipeline/suggest_rules.py` profiles a table (null rates, distinct values, quantiles, top values) and proposes rules:

- **Heuristics** — e.g. it discovers the business key `(assessment_date, hub)` from distinct-count ratios, and leaves rare values such as `USD` out of `accepted_values` with a note to review them.
- **LLM (optional, `--llm`)** — sends the *profile only* (no raw rows) to the Claude API. The reply is treated as untrusted input: parsed with `yaml.safe_load`, every rule validated against the same schema the pipeline uses, `where` SQL filters rejected, invalid rules logged and dropped.
- Output is a file for **human review**; nothing is applied automatically.

## Test coverage

`tests/test_pipeline.py` generates data with a known defect manifest and checks that:
every injected defect is caught by the intended rule with the exact count · no row is lost or duplicated between clean and quarantine · price spikes are flagged but kept · kWh prices are converted correctly · hub codes are standardised · mart keys are unique and spark-spread maths is correct · re-ingesting is a no-op · stale data fails the run · invalid / injected rule configs and bad LLM output are rejected.

## Possible next steps

- Swap SQLite for DuckDB or a cloud warehouse; the SQL is close to ANSI.
- Express the transforms as dbt models and the checks as dbt / Great Expectations tests.
- Schedule with Airflow and send the JSON report to a monitoring channel.
- Load real data from public sources (e.g. ENTSO-E day-ahead prices).
