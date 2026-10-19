"""Write a human-readable (Markdown) and machine-readable (JSON) DQ report."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path

from .validate import RuleResult


def write_report(conn: sqlite3.Connection, run_id: str, results: list[RuleResult],
                 split: dict, out_dir: Path, as_of: str) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "run_id": run_id,
        "as_of": as_of,
        "summary": split,
        "rules": [{k: v for k, v in asdict(r).items() if k != "failing_ids"} for r in results],
    }
    json_path = out_dir / "dq_report.json"
    json_path.write_text(json.dumps(payload, indent=2))

    lines = [
        f"# Data-quality report — run `{run_id}`",
        "",
        f"As of **{as_of}** · {split['total']} rows checked · "
        f"**{split['clean']} clean** · **{split['quarantined']} quarantined** "
        f"({split['quarantine_pct']}%)",
        "",
        "| Rule | Type | Severity | Status | Observed |",
        "|---|---|---|---|---|",
    ]
    for r in results:
        icon = "PASS" if r.status == "pass" else ("FAIL" if r.severity == "error" else "WARN")
        lines.append(f"| {r.name} | {r.type} | {r.severity} | {icon} | {r.observed} |")

    sample = conn.execute(
        "SELECT row_id, assessment_date, commodity, hub, price_raw, currency, dq_errors "
        "FROM quarantine_prices ORDER BY row_id LIMIT 10"
    ).fetchall()
    if sample:
        lines += ["", "## Quarantine sample (first 10)", "",
                  "| row_id | date | commodity | hub | price_raw | currency | reasons |",
                  "|---|---|---|---|---|---|---|"]
        for s in sample:
            lines.append("| " + " | ".join("" if v is None else str(v) for v in tuple(s)) + " |")

    md_path = out_dir / "dq_report.md"
    md_path.write_text("\n".join(lines) + "\n")
    return md_path, json_path
