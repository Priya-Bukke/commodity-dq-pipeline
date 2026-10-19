"""Small database helpers (SQLite, standard library only)."""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def run_sql_file(conn: sqlite3.Connection, path: str | Path) -> None:
    """Execute a .sql file containing one or more statements."""
    conn.executescript(Path(path).read_text(encoding="utf-8"))
    conn.commit()


def check_identifier(name: str) -> str:
    """Reject anything that is not a plain SQL identifier (guards dynamic SQL)."""
    if not isinstance(name, str) or not _IDENT.match(name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return name


def quote(name: str) -> str:
    return f'"{check_identifier(name)}"'


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({quote(table)})").fetchall()
    return [r[1] for r in rows]
