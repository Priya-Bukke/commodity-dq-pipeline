"""End-to-end and unit tests. Run with:  python -m pytest -q   (or python -m unittest)"""
from __future__ import annotations

import datetime as dt
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline import ingest, run, sample_data, suggest_rules, validate
from pipeline.db import connect

END = dt.date(2026, 1, 31)


class PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        base = Path(cls.tmp.name)
        cls.raw, cls.db, cls.reports = base / "raw", base / "wh.db", base / "reports"
        cls.manifest = sample_data.generate(cls.raw, days=200, end=END, seed=7)
        cls.exit_code = run.main(["--raw-dir", str(cls.raw), "--db", str(cls.db),
                                  "--report-dir", str(cls.reports),
                                  "--as-of", "2026-02-01", "-q"])
        cls.conn = sqlite3.connect(cls.db)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        cls.tmp.cleanup()

    def q(self, sql, *params):
        return self.conn.execute(sql, params).fetchall()

    def latest_result(self, rule):
        return self.q("SELECT failed_rows, status FROM dq_results r JOIN dq_runs USING (run_id) "
                      "WHERE rule_name = ? ORDER BY r.rowid DESC LIMIT 1", rule)[0]

    def defects(self, name):
        return len(self.manifest["defects"][name])

    # --- end to end -----------------------------------------------------------
    def test_pipeline_succeeds(self):
        self.assertEqual(self.exit_code, 0)
        self.assertTrue((self.reports / "dq_report.md").exists())

    def test_each_defect_caught_by_the_right_rule(self):
        expected = {
            "price_is_numeric": self.defects("null_price") + self.defects("non_numeric_price"),
            "date_is_valid": self.defects("bad_date"),
            "gas_price_in_range": self.defects("negative_gas"),
            "currency_is_eur": self.defects("bad_currency"),
            "one_assessment_per_day_hub":
                self.defects("exact_duplicate") + self.defects("conflicting_revision"),
            "unit_is_mwh": 0,
            "hub_known": 0,
        }
        for rule, n in expected.items():
            with self.subTest(rule=rule):
                self.assertEqual(self.latest_result(rule)[0], n)

    def test_row_accounting(self):
        total = self.q("SELECT COUNT(*) FROM stg_prices")[0][0]
        clean = self.q("SELECT COUNT(*) FROM clean_prices")[0][0]
        quarantined = self.q("SELECT COUNT(*) FROM quarantine_prices")[0][0]
        self.assertEqual(total, self.manifest["rows"])
        self.assertEqual(clean + quarantined, total)  # no row lost, none duplicated
        error_defects = sum(self.defects(d) for d in (
            "null_price", "non_numeric_price", "bad_date", "negative_gas",
            "bad_currency", "exact_duplicate", "conflicting_revision"))
        self.assertEqual(quarantined, error_defects)

    def test_spikes_flagged_not_dropped(self):
        for date, commodity, hub in self.manifest["defects"]["price_spike_x10"]:
            with self.subTest(key=(date, commodity, hub)):
                row = self.q("SELECT dq_warnings FROM clean_prices "
                             "WHERE assessment_date=? AND commodity=? AND hub=?", date, commodity, hub)
                self.assertEqual(len(row), 1)
                self.assertIn("price_statistical_outlier", row[0][0] or "")

    def test_kwh_prices_converted_to_mwh(self):
        for date, commodity, hub, mwh_price in self.manifest["defects"]["kwh_unit"]:
            rows = self.q("SELECT price, unit FROM clean_prices "
                          "WHERE assessment_date=? AND commodity=? AND hub=?", date, commodity, hub)
            if not rows:   # row may have been hit by another (error) defect - not in this design
                self.fail(f"missing kWh row {date} {hub}")
            self.assertAlmostEqual(rows[0][0], mwh_price, places=2)
            self.assertEqual(rows[0][1], "MWh")

    def test_hub_names_standardised(self):
        hubs = {r[0] for r in self.q("SELECT DISTINCT hub FROM clean_prices")}
        self.assertEqual(hubs, {"DE", "NL", "THE", "TTF"})

    def test_mart_unique_and_spark_spread_math(self):
        dupes = self.q("SELECT COUNT(*) FROM (SELECT 1 FROM mart_daily_prices "
                       "GROUP BY assessment_date, commodity, hub HAVING COUNT(*) > 1)")[0][0]
        self.assertEqual(dupes, 0)
        for power, gas, spread in self.q(
                "SELECT power_eur_mwh, gas_eur_mwh, spark_spread_eur_mwh FROM mart_spark_spread"):
            self.assertAlmostEqual(spread, power - 2 * gas, delta=0.02)

    def test_reingest_is_idempotent(self):
        conn = connect(self.db)
        try:
            self.assertEqual(ingest.ingest_dir(conn, self.raw), 0)
        finally:
            conn.close()

    def test_stale_data_fails_the_run(self):
        code = run.main(["--raw-dir", str(self.raw), "--db", str(self.db),
                         "--report-dir", str(self.reports), "--as-of", "2026-06-01", "-q"])
        self.assertEqual(code, 1)


class ConfigValidationTest(unittest.TestCase):
    COLS = ["row_id", "price", "hub"]

    def test_rejects_unknown_type_and_column(self):
        with self.assertRaises(validate.RuleConfigError) as ctx:
            validate.validate_config({"rules": [
                {"type": "magic", "column": "price"},
                {"type": "not_null", "column": "nope"},
            ]}, self.COLS)
        msg = str(ctx.exception)
        self.assertIn("rule #1", msg)
        self.assertIn("rule #2", msg)   # all problems reported, not just the first

    def test_rejects_sql_injection_in_identifiers(self):
        with self.assertRaises(validate.RuleConfigError):
            validate.validate_config({"rules": [
                {"type": "not_null", "column": 'price"; DROP TABLE x; --'}]}, self.COLS)

    def test_where_blocked_for_untrusted_source(self):
        with self.assertRaises(validate.RuleConfigError):
            validate.validate_rule({"type": "not_null", "column": "price", "where": "1=1"},
                                   self.COLS, allow_where=False)

    def test_defaults_applied(self):
        r = validate.validate_rule({"type": "outlier_zscore", "column": "price"}, self.COLS)
        self.assertEqual((r["severity"], r["threshold"], r["name"]),
                         ("error", 4.0, "outlier_zscore__price"))


class SuggestRulesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        sample_data.generate(base / "raw", days=200, end=END, seed=11)
        run.main(["--raw-dir", str(base / "raw"), "--db", str(base / "wh.db"),
                  "--report-dir", str(base / "r"), "--as-of", "2026-02-01", "-q"])
        self.conn = connect(base / "wh.db")

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_heuristics_find_key_and_flag_rare_currency(self):
        rules, _, _ = suggest_rules.suggest(self.conn, "stg_prices")
        currency = next(r for r in rules
                        if r["type"] == "accepted_values" and r["column"] == "currency")
        self.assertEqual(currency["values"], ["EUR"])          # USD left out on purpose
        self.assertIn("USD", currency["description"])
        key = next(r for r in rules if r["type"] == "unique")
        self.assertIn("assessment_date", key["columns"])

    def test_llm_output_is_validated(self):
        cols = [c for c in ["row_id", "assessment_date", "price", "hub"]]
        text = ("Here you go:\n```yaml\n"
                "- {type: range, column: price, min: 0, max: 300, severity: warn}\n"
                "- {type: not_null, column: does_not_exist}\n"
                "- {type: not_null, column: price, where: \"1=1; DROP TABLE x\"}\n"
                "- {type: teleport}\n```")
        accepted, rejected = suggest_rules.parse_llm_rules(text, cols)
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0]["type"], "range")
        self.assertEqual(len(rejected), 3)


if __name__ == "__main__":
    unittest.main()
