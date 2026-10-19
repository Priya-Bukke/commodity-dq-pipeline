"""Idempotent CSV ingestion into a raw (untyped) landing table.

Every file is fingerprinted with SHA-256. A file that was already loaded is
skipped, so re-running the pipeline never double-counts data.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import logging
import sqlite3
import uuid
from pathlib import Path

log = logging.getLogger(__name__)

RAW_COLUMNS = ["assessment_date", "commodity", "hub", "price", "currency", "unit", "source"]

DDL = """
CREATE TABLE IF NOT EXISTS raw_prices (
    assessment_date TEXT,
    commodity       TEXT,
    hub             TEXT,
    price           TEXT,
    currency        TEXT,
    unit            TEXT,
    source          TEXT,
    _batch_id       TEXT NOT NULL,
    _source_file    TEXT NOT NULL,
    _loaded_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS load_log (
    file_sha256 TEXT PRIMARY KEY,
    file_name   TEXT NOT NULL,
    batch_id    TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    loaded_at   TEXT NOT NULL
);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_file(conn: sqlite3.Connection, path: Path) -> int:
    """Load one CSV. Returns the number of rows inserted (0 if already loaded)."""
    digest = _sha256(path)
    if conn.execute("SELECT 1 FROM load_log WHERE file_sha256 = ?", (digest,)).fetchone():
        log.info("skip %s (already loaded)", path.name)
        return 0

    batch_id = uuid.uuid4().hex[:12]
    loaded_at = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = set(RAW_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
        rows = [
            tuple(r.get(c) for c in RAW_COLUMNS) + (batch_id, path.name, loaded_at)
            for r in reader
        ]

    placeholders = ",".join("?" * (len(RAW_COLUMNS) + 3))
    with conn:  # one transaction per file: all rows or none
        conn.executemany(f"INSERT INTO raw_prices VALUES ({placeholders})", rows)
        conn.execute(
            "INSERT INTO load_log VALUES (?, ?, ?, ?, ?)",
            (digest, path.name, batch_id, len(rows), loaded_at),
        )
    log.info("loaded %s: %d rows (batch %s)", path.name, len(rows), batch_id)
    return len(rows)


def ingest_dir(conn: sqlite3.Connection, directory: Path) -> int:
    ensure_schema(conn)
    files = sorted(Path(directory).glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"no CSV files in {directory}")
    return sum(ingest_file(conn, p) for p in files)
