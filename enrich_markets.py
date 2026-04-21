"""Populate the `markets` table with metadata for every ticker we've seen in trades.

Two-pass approach:
  1. For each unique ticker in trades_public that we don't already have fresh data for,
     call /trade-api/v2/markets/{ticker}  -> status, result, close_time, event_ticker, etc.
  2. For each unique event_ticker, call /trade-api/v2/events/{event_ticker} once to pick
     up category (not available on the market endpoint).

Safe to rerun. Refreshes any row older than --stale-hours (default 6h) so resolved markets
eventually get their `result` filled in.

Usage:
    python enrich_markets.py                    # enrich everything new/stale
    python enrich_markets.py --stale-hours 24   # only re-fetch rows older than 24h
    python enrich_markets.py --limit 500        # cap tickers processed this run
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from typing import Any

import client
import db

SLEEP = 0.1


def parse_iso(s: str | None) -> int | None:
    if not s or s.startswith("0001-"):
        return None
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def settlement_value(m: dict) -> int | None:
    r = (m.get("result") or "").lower()
    if r == "yes":
        return 100
    if r == "no":
        return 0
    return None


def upsert_market(con, m: dict, category: str | None, subcategory: str | None) -> None:
    con.execute(
        "INSERT OR REPLACE INTO markets "
        "(ticker, event_ticker, series_ticker, category, subcategory, title, status, "
        " close_ts, settle_ts, result, settlement_value, last_refreshed_ts, raw_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            m["ticker"],
            m.get("event_ticker"),
            None,  # filled in by event pass
            category,
            subcategory,
            m.get("title"),
            m.get("status"),
            parse_iso(m.get("close_time")),
            parse_iso(m.get("expected_expiration_time") or m.get("expiration_time")),
            m.get("result") or None,
            settlement_value(m),
            int(time.time()),
            json.dumps(m, separators=(",", ":")),
        ),
    )


def fetch_market(ticker: str) -> dict | None:
    try:
        body = client.get(f"/trade-api/v2/markets/{ticker}")
        return body.get("market")
    except client.KalshiError as e:
        if e.status == 404:
            return None
        raise


def fetch_event(event_ticker: str) -> dict | None:
    try:
        body = client.get(f"/trade-api/v2/events/{event_ticker}")
        return body
    except client.KalshiError as e:
        if e.status == 404:
            return None
        raise


def tickers_to_refresh(con, stale_cutoff: int, limit: int | None) -> list[str]:
    sql = """
        SELECT DISTINCT t.ticker
        FROM trades_public t
        LEFT JOIN markets m ON m.ticker = t.ticker
        WHERE m.ticker IS NULL
           OR m.last_refreshed_ts < ?
           OR m.result IS NULL
        ORDER BY t.ticker
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = con.execute(sql, (stale_cutoff,)).fetchall()
    return [r["ticker"] for r in rows]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stale-hours", type=int, default=6)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    con = db.connect()
    stale_cutoff = int(time.time()) - args.stale_hours * 3600
    tickers = tickers_to_refresh(con, stale_cutoff, args.limit or None)
    print(f"To enrich: {len(tickers)} tickers")
    if not tickers:
        return 0

    pass1_ok = 0
    pass1_missing = 0
    event_tickers: dict[str, list[str]] = {}

    for i, ticker in enumerate(tickers):
        m = fetch_market(ticker)
        if not m:
            pass1_missing += 1
            continue
        upsert_market(con, m, category=None, subcategory=None)
        ev = m.get("event_ticker")
        if ev:
            event_tickers.setdefault(ev, []).append(ticker)
        pass1_ok += 1
        if (i + 1) % 50 == 0:
            con.commit()
            print(f"  pass1 {i+1}/{len(tickers)}  ok={pass1_ok}  404={pass1_missing}  events={len(event_tickers)}")
        time.sleep(SLEEP)
    con.commit()
    print(f"Pass 1 done. markets_ok={pass1_ok} missing={pass1_missing} unique_events={len(event_tickers)}")

    # Pass 2: categories via event endpoint
    for i, (ev, related_tickers) in enumerate(event_tickers.items()):
        body = fetch_event(ev)
        if not body:
            continue
        event = body.get("event", {})
        category = event.get("category")
        series_ticker = event.get("series_ticker")
        subcategory = (event.get("product_metadata") or {}).get("competition")
        for t in related_tickers:
            con.execute(
                "UPDATE markets SET category = ?, subcategory = ?, series_ticker = ? WHERE ticker = ?",
                (category, subcategory, series_ticker, t),
            )
        if (i + 1) % 20 == 0:
            con.commit()
            print(f"  pass2 {i+1}/{len(event_tickers)}")
        time.sleep(SLEEP)
    con.commit()
    print(f"Pass 2 done. Events enriched: {len(event_tickers)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
