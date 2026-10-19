"""Run the full pipeline: ingest -> stage -> validate -> quarantine -> marts -> report.

    python -m pipeline.run --raw-dir data/raw --db warehouse.db

Exit code 1 if a table-level error rule fails (e.g. stale data) or the
quarantine rate exceeds max_quarantine_pct — so a scheduler/CI job can alert.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from pathlib import Path

from . import ingest, report, validate
from .db import connect, run_sql_file, table_columns

ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("pipeline")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Commodity price DQ pipeline")
    ap.add_argument("--raw-dir", default=str(ROOT / "data" / "raw"))
    ap.add_argument("--db", default=str(ROOT / "warehouse.db"))
    ap.add_argument("--rules", default=str(ROOT / "rules" / "prices.yaml"))
    ap.add_argument("--report-dir", default=str(ROOT / "reports"))
    ap.add_argument("--as-of", type=dt.date.fromisoformat, default=dt.date.today())
    ap.add_argument("-q", "--quiet", action="store_true")
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.WARNING if a.quiet else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    conn = connect(a.db)
    try:
        n_new = ingest.ingest_dir(conn, Path(a.raw_dir))
        log.info("ingested %d new rows", n_new)

        run_sql_file(conn, ROOT / "sql" / "staging.sql")

        cfg = validate.load_config(a.rules)
        table = cfg.get("table", "stg_prices")
        rules = validate.validate_config(cfg, table_columns(conn, table))
        run_id, results = validate.run_rules(conn, table, rules, a.as_of)

        split = validate.split_clean_quarantine(conn, table, run_id)
        log.info("clean=%d quarantined=%d (%.2f%%)",
                 split["clean"], split["quarantined"], split["quarantine_pct"])

        run_sql_file(conn, ROOT / "sql" / "reference.sql")
        run_sql_file(conn, ROOT / "sql" / "marts.sql")

        md, _ = report.write_report(conn, run_id, results, split,
                                    Path(a.report_dir), a.as_of.isoformat())
        log.info("report written to %s", md)

        blocking = [r.name for r in results
                    if r.severity == "error" and r.status == "fail"
                    and validate.RULE_SPECS[r.type][2] == "table"]
        limit = float(cfg.get("max_quarantine_pct", 100))
        if split["quarantine_pct"] > limit:
            blocking.append(f"quarantine rate {split['quarantine_pct']}% > {limit}%")
        if blocking:
            log.error("pipeline FAILED: %s", "; ".join(blocking))
            return 1
        log.info("pipeline OK (run %s)", run_id)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
