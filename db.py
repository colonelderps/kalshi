"""Sqlite helpers for the whale tracker."""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
DB_PATH = DATA_DIR / "kalshi.db"
SCHEMA_PATH = HERE / "schema.sql"


def connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout = 120000")
    con.executescript(SCHEMA_PATH.read_text())
    return con


def commit_with_retry(con: sqlite3.Connection, *, attempts: int = 6, label: str = "commit") -> None:
    """Commit with backoff on "database is locked" -- the collector runs 24/7
    against this same file, so any script that writes here can collide with
    it. busy_timeout already covers most contention, but a commit can still
    lose the race against the collector's periodic WAL checkpoint; this is
    the belt to that pragma's suspenders. Matches collect_social.py's
    already-proven insert_batch() retry pattern, factored out so every writer
    uses the same behavior instead of re-inventing it (or, worse, not having
    it -- this is exactly the gap that let a locked commit surface as an
    unhandled OperationalError and kill enrich_markets.py mid-run)."""
    for attempt in range(attempts):
        try:
            con.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() or attempt == attempts - 1:
                raise
            backoff = 5 * (attempt + 1)
            print(f"DB locked on {label} (attempt {attempt+1}/{attempts}). Waiting {backoff}s...")
            time.sleep(backoff)
