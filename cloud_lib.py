"""Shared helpers for cloud-side (GitHub Actions) collectors.

Reads Kalshi creds from env vars in CI, or falls back to local creds.json.
Writes collected rows as gzipped JSONL files under data/<stream>/.

Why JSONL-in-repo instead of SQLite-in-repo:
  - SQLite binary diffs bloat git history fast
  - JSONL is append-only, gzip-compresses ~10:1 on this data
  - Trivially mergeable locally (one INSERT OR IGNORE per row)

State (resume cursors, last-seen timestamps) is committed alongside
the data under data/<stream>/state.json so the next run can pick up
where the last one left off.
"""
from __future__ import annotations

import base64
import gzip
import json
import os
import random
import time
from pathlib import Path
from typing import Any

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"


# ---------------------------------------------------------------------------
# Credentials: env vars first (CI), then local creds.json (dev box)
# ---------------------------------------------------------------------------


def _load_creds() -> tuple[str, str, RSAPrivateKey]:
    key_id = os.environ.get("KALSHI_KEY_ID")
    pem = os.environ.get("KALSHI_PRIVATE_KEY_PEM")
    base_url = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com")
    if key_id and pem:
        pk = serialization.load_pem_private_key(pem.encode(), password=None)
        return base_url, key_id, pk

    # Fallback: local creds.json next to this file (dev box).
    creds_path = HERE / "creds.json"
    if not creds_path.exists():
        raise RuntimeError(
            "No Kalshi credentials available. Set KALSHI_KEY_ID + "
            "KALSHI_PRIVATE_KEY_PEM env vars, or place creds.json in the repo root."
        )
    creds = json.loads(creds_path.read_text())["kalshi"]
    pk = serialization.load_pem_private_key(
        (HERE / creds["private_key_path"]).read_bytes(), password=None
    )
    return creds["base_url"], creds["key_id"], pk


BASE_URL, KEY_ID, _PK = _load_creds()


# ---------------------------------------------------------------------------
# Signed HTTP client (same signing scheme as local client.py)
# ---------------------------------------------------------------------------


class KalshiError(RuntimeError):
    def __init__(self, status: int, body: Any, path: str):
        super().__init__(f"{status} {path}: {str(body)[:400]}")
        self.status, self.body, self.path = status, body, path


def _sign(method: str, path: str) -> dict[str, str]:
    ts = str(int(time.time() * 1000))
    sig = _PK.sign(
        (ts + method.upper() + path).encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Accept": "application/json",
    }


def request(method: str, path: str, params: dict | None = None, *, retries: int = 4) -> Any:
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            r = requests.request(
                method, BASE_URL + path, headers=_sign(method, path), params=params, timeout=30
            )
            if r.status_code == 429 or 500 <= r.status_code < 600:
                raise KalshiError(r.status_code, r.text[:400], path)
            if r.status_code >= 400:
                try:
                    raise KalshiError(r.status_code, r.json(), path)
                except ValueError:
                    raise KalshiError(r.status_code, r.text[:400], path)
            return r.json()
        except (requests.RequestException, KalshiError) as e:
            last_err = e
            if attempt == retries:
                break
            time.sleep(2**attempt + random.random())
    raise last_err  # type: ignore[misc]


def get(path: str, params: dict | None = None) -> Any:
    return request("GET", path, params)


# ---------------------------------------------------------------------------
# JSONL-gz output + state
# ---------------------------------------------------------------------------


def stream_dir(stream: str) -> Path:
    p = DATA_DIR / stream
    p.mkdir(parents=True, exist_ok=True)
    return p


def load_state(stream: str) -> dict:
    p = stream_dir(stream) / "state.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text())


def save_state(stream: str, state: dict) -> None:
    p = stream_dir(stream) / "state.json"
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(p)


def write_jsonl_gz(stream: str, run_ts: int, rows: list[dict]) -> Path | None:
    """Write rows to data/<stream>/YYYY-MM-DDTHH-MM-SSZ.jsonl.gz. Returns path or None if rows empty."""
    if not rows:
        return None
    from datetime import datetime, timezone
    stamp = datetime.fromtimestamp(run_ts, timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out = stream_dir(stream) / f"{stamp}.jsonl.gz"
    with gzip.open(out, "wt", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":"), sort_keys=True))
            f.write("\n")
    return out
