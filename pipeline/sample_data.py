"""Generate realistic synthetic European power & gas price assessments,
with known data-quality defects injected so the pipeline can be tested.

    python -m pipeline.sample_data --out data/raw --days 365

Prices follow a mean-reverting random walk (Ornstein-Uhlenbeck style), and
power occasionally prints legitimately negative prices (renewable surplus).
Files are written one per month to mimic incremental vendor deliveries.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import random
from collections import defaultdict
from pathlib import Path

from .ingest import RAW_COLUMNS

# commodity, hub, long-run mean (EUR/MWh), daily volatility, mean-reversion speed
SERIES = [
    ("POWER", "DE", 85.0, 9.0, 0.25),
    ("POWER", "NL", 88.0, 8.0, 0.25),
    ("GAS", "THE", 36.0, 1.6, 0.15),
    ("GAS", "TTF", 35.0, 1.5, 0.15),
]

# defect -> number of rows affected (defect rows are disjoint)
DEFECTS = {
    "null_price": 6,          # -> price_is_numeric
    "non_numeric_price": 2,   # -> price_is_numeric
    "bad_date": 3,            # -> date_is_valid
    "negative_gas": 4,        # -> gas_price_in_range
    "bad_currency": 3,        # -> currency_is_eur
    "exact_duplicate": 5,     # -> one_assessment_per_day_hub
    "conflicting_revision": 3,  # -> one_assessment_per_day_hub
    "price_spike_x10": 4,     # -> price_statistical_outlier (warn)
    "kwh_unit": 15,           # valid: converted to MWh in staging
    "messy_hub": 20,          # valid: trimmed / upper-cased in staging
}


def simulate(days: int, end: dt.date, rng: random.Random) -> list[dict]:
    start = end - dt.timedelta(days=days - 1)
    rows = []
    for commodity, hub, mean, vol, k in SERIES:
        x = mean
        for i in range(days):
            x = x + k * (mean - x) + rng.gauss(0, vol)
            price = x
            if commodity == "POWER" and rng.random() < 0.01:
                price = -rng.uniform(1, 40)
            rows.append({
                "assessment_date": (start + dt.timedelta(days=i)).isoformat(),
                "commodity": commodity,
                "hub": hub,
                "price": f"{price:.2f}",
                "currency": "EUR",
                "unit": "MWh",
                "source": "SIM",
            })
    return rows


def inject_defects(rows: list[dict], rng: random.Random) -> tuple[list[dict], dict]:
    """Return (rows_with_defects, manifest). Manifest lists the keys of affected rows."""
    rows = [dict(r) for r in rows]
    gas = [i for i, r in enumerate(rows) if r["commodity"] == "GAS"]
    positive = [i for i, r in enumerate(rows) if float(r["price"]) > 0]
    used: set[int] = set()

    def take(pool, k):
        free = [i for i in pool if i not in used]
        chosen = rng.sample(free, k)
        used.update(chosen)
        return chosen

    manifest: dict[str, list] = defaultdict(list)

    def key(r):
        return [r["assessment_date"], r["commodity"], r["hub"].strip().upper()]

    for i in take(range(len(rows)), DEFECTS["null_price"]):
        rows[i]["price"] = ""
        manifest["null_price"].append(key(rows[i]))
    for i in take(range(len(rows)), DEFECTS["non_numeric_price"]):
        rows[i]["price"] = "n/a"
        manifest["non_numeric_price"].append(key(rows[i]))
    for n, i in enumerate(take(range(len(rows)), DEFECTS["bad_date"])):
        manifest["bad_date"].append(key(rows[i]))
        rows[i]["assessment_date"] = ["2025-13-01", "31/12/2025", "not-a-date"][n % 3]
    for i in take(gas, DEFECTS["negative_gas"]):
        rows[i]["price"] = f"{-rng.uniform(5, 20):.2f}"
        manifest["negative_gas"].append(key(rows[i]))
    for i in take(range(len(rows)), DEFECTS["bad_currency"]):
        rows[i]["currency"] = "USD"
        manifest["bad_currency"].append(key(rows[i]))
    for i in take(positive, DEFECTS["price_spike_x10"]):
        rows[i]["price"] = f"{float(rows[i]['price']) * 10:.2f}"
        manifest["price_spike_x10"].append(key(rows[i]))
    for i in take(range(len(rows)), DEFECTS["kwh_unit"]):
        manifest["kwh_unit"].append(key(rows[i]) + [float(rows[i]["price"])])
        rows[i]["price"] = f"{float(rows[i]['price']) / 1000:.6f}"
        rows[i]["unit"] = "kWh"
    for i in take(range(len(rows)), DEFECTS["messy_hub"]):
        rows[i]["hub"] = f"  {rows[i]['hub'].lower()} "
        manifest["messy_hub"].append(key(rows[i]))

    extra = []
    for i in take(range(len(rows)), DEFECTS["exact_duplicate"]):
        extra.append(dict(rows[i]))
        manifest["exact_duplicate"].append(key(rows[i]))
    for i in take(range(len(rows)), DEFECTS["conflicting_revision"]):
        rev = dict(rows[i])
        rev["price"] = f"{float(rev['price']) + 0.5:.2f}"
        extra.append(rev)
        manifest["conflicting_revision"].append(key(rows[i]))

    return rows + extra, dict(manifest)


def write_monthly(rows: list[dict], out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        try:
            month = dt.date.fromisoformat(r["assessment_date"]).strftime("%Y-%m")
        except ValueError:
            month = "unparsed"
        buckets[month].append(r)
    paths = []
    for month, chunk in sorted(buckets.items()):
        p = out_dir / f"prices_{month}.csv"
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
            w.writeheader()
            w.writerows(chunk)
        paths.append(p)
    return paths


def generate(out_dir: Path, days: int = 365, end: dt.date | None = None, seed: int = 42) -> dict:
    end = end or (dt.date.today() - dt.timedelta(days=1))
    rng = random.Random(seed)
    rows, manifest = inject_defects(simulate(days, end, rng), rng)
    files = write_monthly(rows, Path(out_dir))
    summary = {
        "rows": len(rows),
        "files": [p.name for p in files],
        "end": end.isoformat(),
        "seed": seed,
        "defects": manifest,
    }
    (Path(out_dir) / "_manifest.json").write_text(json.dumps(summary, indent=2))
    return summary


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--end", type=dt.date.fromisoformat, default=None,
                    help="last assessment date (default: yesterday)")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args(argv)
    s = generate(Path(a.out), a.days, a.end, a.seed)
    print(f"wrote {s['rows']} rows in {len(s['files'])} files to {a.out}")


if __name__ == "__main__":
    main()
