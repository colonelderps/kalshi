"""
Kalshi internal /v1/social/* API probe.

Discover what named-bettor data is exposed to an authenticated user via the
private v1 API that the Kalshi webapp uses (not documented in docs.kalshi.com).

Goal: test Dave's one-shot-whale thesis on Kalshi. Needs per-user trade history
with sizes, markets, and timestamps, plus a way to enumerate users.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import time
from pathlib import Path
from typing import Any

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

REPO_ROOT = Path(__file__).resolve().parent.parent
CREDS = json.loads((REPO_ROOT / "credentials.json").read_text())["kalshi"]
BASE = CREDS["base_url"]
KEY_ID = CREDS["key_id"]
PRIVATE_KEY: RSAPrivateKey = serialization.load_pem_private_key(
    (REPO_ROOT / CREDS["private_key_path"]).read_bytes(), password=None
)


def sign(method: str, path: str) -> dict[str, str]:
    ts = str(int(time.time() * 1000))
    msg = (ts + method.upper() + path).encode()
    sig = PRIVATE_KEY.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Accept": "application/json",
    }


def call(method: str, path: str, params: dict | None = None) -> tuple[int, Any]:
    headers = sign(method, path)
    r = requests.request(method, BASE + path, headers=headers, params=params, timeout=20)
    try:
        body = r.json()
    except Exception:
        body = r.text[:500]
    return r.status_code, body


def pp(label: str, status: int, body: Any) -> None:
    print(f"\n=== {label}  [{status}] ===")
    if isinstance(body, (dict, list)):
        s = json.dumps(body, indent=2, default=str)
        print(s[:3500] + ("\n...[truncated]" if len(s) > 3500 else ""))
    else:
        print(str(body)[:1500])


def probe_pagination():
    # v1/social ignores time / page / offset filters -> realtime firehose only.
    # See if it takes cursor / start_ts / before_trade_id
    pp("social/trades cursor=''", *call("GET", "/v1/social/trades", {"limit": 3, "cursor": ""}))
    pp("social/trades start_ts epoch", *call("GET", "/v1/social/trades", {"limit": 3, "start_ts": 1704067200}))
    pp("social/trades end_ts 2024", *call("GET", "/v1/social/trades", {"limit": 3, "end_ts": 1704067200}))
    pp("social/trades before_ts", *call("GET", "/v1/social/trades", {"limit": 3, "before_ts": 1704067200}))
    pp("social/trades last_trade_id", *call("GET", "/v1/social/trades", {"limit": 3, "last_trade_id": "00000000"}))
    # v2 public, confirm cursor works deeply and check fields
    s, b = call("GET", "/trade-api/v2/markets/trades", {"limit": 5, "min_ts": 1704067200})
    pp("v2 trades min_ts 2024-01", s, b)
    if isinstance(b, dict) and "cursor" in b:
        cur = b["cursor"]
        pp("v2 trades next page", *call("GET", "/trade-api/v2/markets/trades", {"limit": 5, "cursor": cur, "min_ts": 1704067200}))


def probe_market_metadata():
    """For a real ticker we saw, what metadata (category, settlement, close time)?"""
    # Use a ticker from earlier probe
    ticker = "KXATPMATCH-26APR20POTKOS-POT"
    pp("v2 market detail", *call("GET", f"/trade-api/v2/markets/{ticker}", None))
    pp("v2 series detail", *call("GET", "/trade-api/v2/series/KXATPMATCH", None))
    pp("v2 events", *call("GET", "/trade-api/v2/events/KXATPMATCH-26APR20POTKOS", None))


def probe_leaderboard():
    """Figure out valid metric_name values."""
    for m in ["volume", "profit", "pnl", "realized_profit", "predictions", "accuracy", "win_rate", "roi"]:
        s, b = call("GET", "/v1/social/leaderboard", {"metric_name": m, "timeframe": "all_time", "limit": 3})
        pp(f"leaderboard metric={m}", s, b)


if __name__ == "__main__":
    import sys
    which = sys.argv[1] if len(sys.argv) > 1 else "pagination"
    {"pagination": probe_pagination, "market": probe_market_metadata, "leaderboard": probe_leaderboard}[which]()
