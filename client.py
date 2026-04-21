"""Kalshi API client: RSA-PSS-SHA256 signed requests, retries, light rate limiting."""
from __future__ import annotations

import base64
import json
import random
import time
from pathlib import Path
from typing import Any

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

HERE = Path(__file__).resolve().parent


def _load_key() -> tuple[str, str, RSAPrivateKey]:
    creds = json.loads((HERE / "creds.json").read_text())["kalshi"]
    pk = serialization.load_pem_private_key(
        (HERE / creds["private_key_path"]).read_bytes(), password=None
    )
    return creds["base_url"], creds["key_id"], pk


BASE_URL, KEY_ID, _PK = _load_key()


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


class KalshiError(RuntimeError):
    def __init__(self, status: int, body: Any, path: str):
        super().__init__(f"{status} {path}: {str(body)[:400]}")
        self.status, self.body, self.path = status, body, path


def request(method: str, path: str, params: dict | None = None, *, retries: int = 6) -> Any:
    """Signed request. Retries on 429/5xx and network/DNS errors with exp
    backoff. Default 6 retries = backoffs of ~1,2,4,8,16,32s = ~1 min total
    retry budget, enough to ride through most DNS blips without the caller
    crashing. Callers in tight polling loops can pass retries=2 to fast-fail.
    """
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            r = requests.request(
                method, BASE_URL + path, headers=_sign(method, path), params=params, timeout=30
            )
            if r.status_code == 429 or 500 <= r.status_code < 600:
                raise KalshiError(r.status_code, r.text[:400], path)
            if r.status_code >= 400:
                # 4xx other than 429 -> probably schema problem, don't retry
                try:
                    raise KalshiError(r.status_code, r.json(), path)
                except ValueError:
                    raise KalshiError(r.status_code, r.text[:400], path)
            return r.json()
        except (requests.RequestException, KalshiError) as e:
            last_err = e
            if attempt == retries:
                break
            sleep = min(2**attempt + random.random(), 60)
            time.sleep(sleep)
    raise last_err  # type: ignore[misc]


def get(path: str, params: dict | None = None) -> Any:
    return request("GET", path, params)
