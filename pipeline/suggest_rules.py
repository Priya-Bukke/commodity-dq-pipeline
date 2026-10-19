"""Automatically propose data-quality rules for a table.

1. Profile every column with SQL (null rate, distinct values, ranges, top values).
2. Heuristics turn the profile into candidate rules.
3. Optionally (--llm) send the profile to an LLM (Claude API) for extra
   suggestions. LLM output is treated as untrusted: it is parsed with
   yaml.safe_load, every rule is checked against the same schema the
   pipeline uses, and free-form SQL 'where' filters are rejected.
4. Write rules_suggested.yaml for a human to review. Nothing is auto-applied.

    python -m pipeline.suggest_rules --db warehouse.db --table stg_prices
    ANTHROPIC_API_KEY=... python -m pipeline.suggest_rules --db warehouse.db --llm
"""
from __future__ import annotations

import argparse
import itertools
import json
import logging
import math
import os
import re
import sqlite3
import urllib.request
from pathlib import Path

import yaml

from .db import connect, quote, table_columns
from .validate import RULE_SPECS, RuleConfigError, validate_rule

log = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = os.environ.get("DQ_LLM_MODEL", "claude-sonnet-5-5")


# --------------------------------------------------------------------------- #
# Profiling
# --------------------------------------------------------------------------- #
def _skip(col: str) -> bool:
    return col == "row_id" or col.startswith("_") or col.endswith("_raw")


def _quantile(sorted_vals: list[float], q: float) -> float:
    i = (len(sorted_vals) - 1) * q
    lo, hi = math.floor(i), math.ceil(i)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (i - lo)


def profile_table(conn: sqlite3.Connection, table: str) -> dict:
    t = quote(table)
    n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    cols = {}
    for col in table_columns(conn, table):
        if _skip(col):
            continue
        c = quote(col)
        nulls, distinct = conn.execute(
            f"SELECT SUM({c} IS NULL), COUNT(DISTINCT {c}) FROM {t}").fetchone()
        numeric = conn.execute(
            f"SELECT COUNT(*) FROM {t} WHERE typeof({c}) IN ('integer','real')").fetchone()[0]
        nonnull = n - (nulls or 0)
        info = {
            "null_rate": round((nulls or 0) / n, 4) if n else 0.0,
            "distinct": distinct,
            "is_numeric": nonnull > 0 and numeric == nonnull,
        }
        if info["is_numeric"]:
            vals = sorted(r[0] for r in conn.execute(f"SELECT {c} FROM {t} WHERE {c} IS NOT NULL"))
            info.update(min=vals[0], max=vals[-1],
                        p01=round(_quantile(vals, 0.01), 4), p99=round(_quantile(vals, 0.99), 4))
        else:
            top = conn.execute(
                f"SELECT {c}, COUNT(*) FROM {t} WHERE {c} IS NOT NULL "
                f"GROUP BY {c} ORDER BY COUNT(*) DESC LIMIT 15").fetchall()
            info["top_values"] = {str(v): k for v, k in top}
            sample = [str(v) for v, _ in top[:5]]
            info["looks_like_date"] = bool(sample) and all(
                re.fullmatch(r"\d{4}-\d{2}-\d{2}", s) for s in sample)
        cols[col] = info
    return {"table": table, "rows": n, "columns": cols}


# --------------------------------------------------------------------------- #
# Heuristic suggestions
# --------------------------------------------------------------------------- #
def heuristic_rules(conn: sqlite3.Connection, profile: dict) -> list[dict]:
    n = profile["rows"]
    rules: list[dict] = []
    categorical = []

    for col, p in profile["columns"].items():
        if p["null_rate"] <= 0.01:
            rules.append({"type": "not_null", "column": col, "severity": "error",
                          "description": f"profile: {p['null_rate']:.2%} nulls"})
        if p["is_numeric"]:
            span = (p["p99"] - p["p01"]) or abs(p["p99"]) or 1.0
            rules.append({"type": "range", "column": col, "severity": "warn",
                          "min": round(p["p01"] - 0.5 * span, 2),
                          "max": round(p["p99"] + 0.5 * span, 2),
                          "description": "profile: p1/p99 widened by 50% of the spread"})
        elif p.get("looks_like_date"):
            categorical.append(col)
            rules.append({"type": "freshness", "column": col, "max_age_days": 3,
                          "severity": "warn", "description": "profile: date-like column"})
        elif p["distinct"] <= 15:
            categorical.append(col)
            # values seen in < 0.5% of rows are suspicious -> left out on purpose
            common = [v for v, k in p["top_values"].items() if k / n >= 0.005]
            rare = [v for v in p["top_values"] if v not in common]
            rules.append({"type": "accepted_values", "column": col, "values": common,
                          "severity": "error",
                          "description": ("profile: rare values excluded for review: "
                                          + ", ".join(rare)) if rare else "profile"})

    key = candidate_key(conn, profile["table"], categorical)
    if key:
        cols, ratio = key
        rules.append({"type": "unique", "columns": list(cols), "severity": "error",
                      "description": f"profile: candidate business key ({ratio:.2%} unique)"})
    return rules


def candidate_key(conn, table: str, columns: list[str], min_ratio: float = 0.98):
    """Smallest column combination that is (nearly) unique -> likely business key."""
    t = quote(table)
    n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    for size in (1, 2, 3):
        best = None
        for combo in itertools.combinations(columns, size):
            cols = ", ".join(quote(c) for c in combo)
            distinct = conn.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT {cols} FROM {t})").fetchone()[0]
            ratio = distinct / n if n else 0
            if ratio >= min_ratio and (best is None or ratio > best[1]):
                best = (combo, ratio)
        if best:
            return best
    return None


# --------------------------------------------------------------------------- #
# LLM suggestions (optional)
# --------------------------------------------------------------------------- #
def build_prompt(profile: dict) -> str:
    schema = {t: {"required": sorted(r), "optional": sorted(o)} for t, (r, o, _) in RULE_SPECS.items()}
    return (
        "You are a data-quality engineer for commodity price data. "
        "Given this column profile, propose additional data-quality rules.\n\n"
        f"Allowed rule types and keys (plus name, severity [error|warn], description):\n"
        f"{json.dumps(schema, indent=1)}\n\n"
        "Do NOT use 'where'. Only use columns from the profile.\n"
        "Reply with YAML only: a list of rules.\n\n"
        f"Profile:\n{json.dumps(profile, indent=1, default=str)}"
    )


def ask_llm(prompt: str, api_key: str, model: str = DEFAULT_MODEL, timeout: int = 60) -> str:
    body = json.dumps({"model": model, "max_tokens": 2000,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    req = urllib.request.Request(ANTHROPIC_URL, data=body, method="POST", headers={
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.load(resp)
    return "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")


def parse_llm_rules(text: str, columns: list[str]) -> tuple[list[dict], list[str]]:
    """Parse untrusted LLM YAML. Returns (accepted_rules, rejection_reasons)."""
    m = re.search(r"```(?:ya?ml)?\s*\n(.*?)```", text, re.S)
    raw = m.group(1) if m else text
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        return [], [f"unparseable YAML: {e}"]
    if isinstance(data, dict):
        data = data.get("rules", [])
    if not isinstance(data, list):
        return [], ["expected a list of rules"]
    accepted, rejected = [], []
    for item in data:
        try:
            rule = validate_rule(item, columns, allow_where=False)
            rule["description"] = ("llm: " + str(rule.get("description", ""))).strip()
            accepted.append(rule)
        except (RuleConfigError, ValueError) as e:
            rejected.append(f"{item!r:.80} -> {e}")
    return accepted, rejected


# --------------------------------------------------------------------------- #
def _signature(rule: dict):
    return (rule["type"], rule.get("column"), tuple(rule.get("columns", [])))


def merge(*rule_lists: list[dict]) -> list[dict]:
    seen, out = set(), []
    for rules in rule_lists:
        for r in rules:
            if _signature(r) not in seen:
                seen.add(_signature(r))
                out.append(r)
    return out


def suggest(conn, table: str, use_llm: bool = False) -> tuple[list[dict], dict, list[str]]:
    columns = table_columns(conn, table)
    profile = profile_table(conn, table)
    rules = [validate_rule(r, columns) for r in heuristic_rules(conn, profile)]
    rejected: list[str] = []
    if use_llm:
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise SystemExit("--llm needs ANTHROPIC_API_KEY in the environment")
        llm_rules, rejected = parse_llm_rules(ask_llm(build_prompt(profile), key), columns)
        rules = merge(rules, llm_rules)
    return rules, profile, rejected


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Suggest DQ rules from a table profile")
    ap.add_argument("--db", default="warehouse.db")
    ap.add_argument("--table", default="stg_prices")
    ap.add_argument("--out", default="rules/rules_suggested.yaml")
    ap.add_argument("--llm", action="store_true", help="also ask the Claude API")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = connect(a.db)
    rules, profile, rejected = suggest(conn, a.table, a.llm)
    for r in rejected:
        log.warning("rejected LLM rule: %s", r)

    header = (f"# AUTO-SUGGESTED rules for {a.table} ({profile['rows']} rows profiled).\n"
              "# Review before copying into a real rules file - nothing here is applied automatically.\n")
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(header + yaml.safe_dump({"table": a.table, "rules": rules},
                                                   sort_keys=False, allow_unicode=True))
    print(f"wrote {len(rules)} suggested rules to {a.out}")


if __name__ == "__main__":
    main()
