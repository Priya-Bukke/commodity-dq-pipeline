"""Config-driven data-quality engine.

Rules are declared in YAML (see rules/prices.yaml). Each rule is checked
against a schema, compiled to SQL and run against a table. Results are stored
in dq_results / dq_failures so every run is auditable.

Row-level rules (not_null, unique, range, accepted_values, outlier_zscore)
return the row_ids that fail. Table-level rules (row_count, freshness) pass or
fail as a whole.

Severity:
  error -> failing rows are quarantined and never reach the data marts
  warn  -> rows are kept but tagged (dq_warnings column) for analyst review
"""
from __future__ import annotations

import datetime as dt
import logging
import sqlite3
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .db import check_identifier, quote

log = logging.getLogger(__name__)

SEVERITIES = {"error", "warn"}
COMMON_KEYS = {"name", "type", "severity", "where", "description"}

# type -> (required keys, optional keys, level)
RULE_SPECS: dict[str, tuple[set[str], set[str], str]] = {
    "not_null":        ({"column"}, set(), "row"),
    "unique":          ({"columns"}, set(), "row"),
    "range":           ({"column"}, {"min", "max"}, "row"),
    "accepted_values": ({"column", "values"}, set(), "row"),
    "outlier_zscore":  ({"column"}, {"group_by", "threshold", "min_group_size"}, "row"),
    "row_count":       (set(), {"min", "max"}, "table"),
    "freshness":       ({"column", "max_age_days"}, set(), "table"),
}


class RuleConfigError(ValueError):
    pass


@dataclass
class RuleResult:
    name: str
    type: str
    severity: str
    status: str                 # "pass" | "fail"
    failed_rows: int = 0
    observed: str = ""
    failing_ids: list[int] = field(default_factory=list, repr=False)


# --------------------------------------------------------------------------- #
# Config validation
# --------------------------------------------------------------------------- #
def _is_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def validate_rule(rule, columns: list[str], allow_where: bool = True) -> dict:
    """Check one rule against the schema. Returns a normalised copy or raises."""
    if not isinstance(rule, dict):
        raise RuleConfigError(f"rule must be a mapping, got {type(rule).__name__}")
    rtype = rule.get("type")
    if rtype not in RULE_SPECS:
        raise RuleConfigError(f"unknown rule type {rtype!r}")
    required, optional, _ = RULE_SPECS[rtype]

    missing = required - rule.keys()
    if missing:
        raise RuleConfigError(f"{rtype}: missing keys {sorted(missing)}")
    unknown = rule.keys() - required - optional - COMMON_KEYS
    if unknown:
        raise RuleConfigError(f"{rtype}: unknown keys {sorted(unknown)}")

    out = dict(rule)
    out.setdefault("severity", "error")
    if out["severity"] not in SEVERITIES:
        raise RuleConfigError(f"{rtype}: severity must be one of {sorted(SEVERITIES)}")

    if "where" in out:
        if not allow_where:
            raise RuleConfigError(f"{rtype}: 'where' filters are not allowed from this source")
        if not isinstance(out["where"], str) or ";" in out["where"]:
            raise RuleConfigError(f"{rtype}: 'where' must be a single SQL expression")

    def need_cols(cols):
        for c in cols:
            check_identifier(c)
            if c not in columns:
                raise RuleConfigError(f"{rtype}: column {c!r} not in table")

    if "column" in out:
        need_cols([out["column"]])
    if rtype == "unique":
        if not isinstance(out["columns"], list) or not out["columns"]:
            raise RuleConfigError("unique: 'columns' must be a non-empty list")
        need_cols(out["columns"])
    if rtype == "outlier_zscore":
        out.setdefault("group_by", [])
        need_cols(out["group_by"])
        out.setdefault("threshold", 4.0)
        out.setdefault("min_group_size", 30)
        if not _is_number(out["threshold"]) or out["threshold"] <= 0:
            raise RuleConfigError("outlier_zscore: threshold must be > 0")
    if rtype in ("range", "row_count"):
        if "min" not in out and "max" not in out:
            raise RuleConfigError(f"{rtype}: give at least one of min / max")
        for k in ("min", "max"):
            if k in out and not _is_number(out[k]):
                raise RuleConfigError(f"{rtype}: {k} must be a number")
    if rtype == "accepted_values":
        if not isinstance(out["values"], list) or not out["values"]:
            raise RuleConfigError("accepted_values: 'values' must be a non-empty list")
    if rtype == "freshness":
        if not isinstance(out["max_age_days"], int) or out["max_age_days"] < 0:
            raise RuleConfigError("freshness: max_age_days must be a non-negative integer")

    default_target = out.get("column") or "_".join(out.get("columns", [])) or "table"
    out.setdefault("name", f"{rtype}__{default_target}")
    return out


def validate_config(cfg: dict, columns: list[str], allow_where: bool = True) -> list[dict]:
    """Validate every rule; report all problems at once instead of the first."""
    if not isinstance(cfg, dict) or not isinstance(cfg.get("rules"), list):
        raise RuleConfigError("config must contain a 'rules' list")
    rules, errors = [], []
    for i, rule in enumerate(cfg["rules"]):
        try:
            rules.append(validate_rule(rule, columns, allow_where))
        except (RuleConfigError, ValueError) as e:
            errors.append(f"rule #{i + 1}: {e}")
    names = [r["name"] for r in rules]
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        errors.append(f"duplicate rule names: {sorted(dupes)}")
    if errors:
        raise RuleConfigError("invalid rules config:\n  " + "\n  ".join(errors))
    return rules


def load_config(path: str | Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


# --------------------------------------------------------------------------- #
# Rule -> SQL
# --------------------------------------------------------------------------- #
def _row_rule_sql(table: str, rule: dict) -> tuple[str, list]:
    t = quote(table)
    w = f"({rule['where']})" if rule.get("where") else "1=1"
    rtype = rule["type"]

    if rtype == "not_null":
        c = quote(rule["column"])
        return f"SELECT row_id FROM {t} WHERE {w} AND {c} IS NULL", []

    if rtype == "unique":
        cols = ", ".join(quote(c) for c in rule["columns"])
        # keep the first-loaded row, flag every later copy
        return (
            f"SELECT row_id FROM ("
            f"  SELECT row_id, ROW_NUMBER() OVER (PARTITION BY {cols} ORDER BY row_id) AS rn"
            f"  FROM {t} WHERE {w}"
            f") WHERE rn > 1",
            [],
        )

    if rtype == "range":
        c = quote(rule["column"])
        conds, params = [], []
        if "min" in rule:
            conds.append(f"{c} < ?")
            params.append(rule["min"])
        if "max" in rule:
            conds.append(f"{c} > ?")
            params.append(rule["max"])
        return (
            f"SELECT row_id FROM {t} WHERE {w} AND {c} IS NOT NULL AND ({' OR '.join(conds)})",
            params,
        )

    if rtype == "accepted_values":
        c = quote(rule["column"])
        ph = ",".join("?" * len(rule["values"]))
        return (
            f"SELECT row_id FROM {t} WHERE {w} AND {c} IS NOT NULL AND {c} NOT IN ({ph})",
            list(rule["values"]),
        )

    if rtype == "outlier_zscore":
        c = quote(rule["column"])
        groups = [quote(g) for g in rule["group_by"]]
        base = f"SELECT * FROM {t} WHERE {w} AND {c} IS NOT NULL"
        agg = f"AVG({c}) AS m, AVG({c} * {c}) - AVG({c}) * AVG({c}) AS v, COUNT(*) AS n"
        if groups:
            g = ", ".join(groups)
            stats = f"SELECT {g}, {agg} FROM base GROUP BY {g}"
            join = f"JOIN stats s USING ({g})"
        else:
            stats = f"SELECT {agg} FROM base"
            join = "CROSS JOIN stats s"
        # compare squared distances: (x - mean)^2 > z^2 * variance  (no sqrt needed)
        return (
            f"WITH base AS ({base}), stats AS ({stats}) "
            f"SELECT b.row_id FROM base b {join} "
            f"WHERE s.n >= ? AND s.v > 0 AND (b.{c} - s.m) * (b.{c} - s.m) > ? * s.v",
            [int(rule["min_group_size"]), float(rule["threshold"]) ** 2],
        )

    raise RuleConfigError(f"not a row-level rule: {rtype}")


def _run_table_rule(conn: sqlite3.Connection, table: str, rule: dict, as_of: dt.date) -> RuleResult:
    t = quote(table)
    w = f"({rule['where']})" if rule.get("where") else "1=1"
    res = RuleResult(rule["name"], rule["type"], rule["severity"], "pass")

    if rule["type"] == "row_count":
        n = conn.execute(f"SELECT COUNT(*) FROM {t} WHERE {w}").fetchone()[0]
        ok = rule.get("min", float("-inf")) <= n <= rule.get("max", float("inf"))
        res.observed = f"rows={n}"
    elif rule["type"] == "freshness":
        c = quote(rule["column"])
        latest = conn.execute(f"SELECT MAX({c}) FROM {t} WHERE {w}").fetchone()[0]
        cutoff = as_of - dt.timedelta(days=rule["max_age_days"])
        ok = latest is not None and dt.date.fromisoformat(latest) >= cutoff
        res.observed = f"latest={latest}, cutoff={cutoff.isoformat()}"
    else:
        raise RuleConfigError(f"not a table-level rule: {rule['type']}")

    res.status = "pass" if ok else "fail"
    return res


# --------------------------------------------------------------------------- #
# Run + persist
# --------------------------------------------------------------------------- #
DQ_DDL = """
CREATE TABLE IF NOT EXISTS dq_runs (
    run_id TEXT PRIMARY KEY, table_name TEXT, as_of TEXT, started_at TEXT, rows_checked INTEGER
);
CREATE TABLE IF NOT EXISTS dq_results (
    run_id TEXT, rule_name TEXT, rule_type TEXT, severity TEXT,
    status TEXT, failed_rows INTEGER, observed TEXT
);
CREATE TABLE IF NOT EXISTS dq_failures (
    run_id TEXT, rule_name TEXT, severity TEXT, row_id INTEGER
);
CREATE INDEX IF NOT EXISTS ix_dq_failures_run ON dq_failures (run_id, severity, row_id);
"""


def run_rules(conn: sqlite3.Connection, table: str, rules: list[dict],
              as_of: dt.date) -> tuple[str, list[RuleResult]]:
    conn.executescript(DQ_DDL)
    run_id = uuid.uuid4().hex[:12]
    n_rows = conn.execute(f"SELECT COUNT(*) FROM {quote(table)}").fetchone()[0]
    started = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    results: list[RuleResult] = []
    for rule in rules:
        if RULE_SPECS[rule["type"]][2] == "row":
            sql, params = _row_rule_sql(table, rule)
            ids = [r[0] for r in conn.execute(sql, params)]
            res = RuleResult(rule["name"], rule["type"], rule["severity"],
                             "fail" if ids else "pass", len(ids),
                             f"{len(ids)}/{n_rows} rows", ids)
        else:
            res = _run_table_rule(conn, table, rule, as_of)
        log.info("%-36s %-5s %-4s %s", res.name, res.severity, res.status, res.observed)
        results.append(res)

    with conn:
        conn.execute("INSERT INTO dq_runs VALUES (?, ?, ?, ?, ?)",
                     (run_id, table, as_of.isoformat(), started, n_rows))
        conn.executemany(
            "INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(run_id, r.name, r.type, r.severity, r.status, r.failed_rows, r.observed)
             for r in results],
        )
        conn.executemany(
            "INSERT INTO dq_failures VALUES (?, ?, ?, ?)",
            [(run_id, r.name, r.severity, i) for r in results for i in r.failing_ids],
        )
    return run_id, results


def split_clean_quarantine(conn: sqlite3.Connection, table: str, run_id: str,
                           clean_table: str = "clean_prices",
                           quarantine_table: str = "quarantine_prices") -> dict:
    """Route rows: error failures -> quarantine, everything else -> clean (with warning tags)."""
    t, ct, qt = quote(table), quote(clean_table), quote(quarantine_table)
    with conn:
        conn.execute(f"DROP TABLE IF EXISTS {qt}")
        conn.execute(
            f"CREATE TABLE {qt} AS "
            f"SELECT s.*, f.dq_errors FROM {t} s JOIN ("
            f"  SELECT row_id, GROUP_CONCAT(rule_name, '; ') AS dq_errors"
            f"  FROM dq_failures WHERE run_id = ? AND severity = 'error' GROUP BY row_id"
            f") f USING (row_id)",
            (run_id,),
        )
        conn.execute(f"DROP TABLE IF EXISTS {ct}")
        conn.execute(
            f"CREATE TABLE {ct} AS "
            f"SELECT s.*, w.dq_warnings FROM {t} s LEFT JOIN ("
            f"  SELECT row_id, GROUP_CONCAT(rule_name, '; ') AS dq_warnings"
            f"  FROM dq_failures WHERE run_id = ? AND severity = 'warn' GROUP BY row_id"
            f") w USING (row_id) "
            f"WHERE s.row_id NOT IN ("
            f"  SELECT row_id FROM dq_failures WHERE run_id = ? AND severity = 'error')",
            (run_id, run_id),
        )
    total = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    quarantined = conn.execute(f"SELECT COUNT(*) FROM {qt}").fetchone()[0]
    clean = conn.execute(f"SELECT COUNT(*) FROM {ct}").fetchone()[0]
    return {
        "total": total,
        "clean": clean,
        "quarantined": quarantined,
        "quarantine_pct": round(100.0 * quarantined / total, 2) if total else 0.0,
    }
