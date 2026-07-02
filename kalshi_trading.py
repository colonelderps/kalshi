"""Live (real-money) Kalshi trading client -- gas-nearclose fade only.

Builds order tickets for the ONE validated strategy (series_gas_nearclose_roi:
fade any gas-family taker within 1h of market close -- see experiments_tracker.md)
and can submit them to Kalshi's real trading API. Manual-trigger only: there is
no scheduler wired to this, by design -- you decide when to scan and when to act.

HARD RULE, not just a docstring: this tool never submits a live order on its
own. `execute --live` refuses to run outside an interactive terminal and
requires an exact typed confirmation that echoes the order's ticker/side/price
back. The assistant that built this will not run `execute --live` on the
user's behalf under any circumstance -- placing a real order is always a
human, in their own terminal, doing it themselves.

Usage:
    python kalshi_trading.py balance                  # read-only sanity check
    python kalshi_trading.py positions
    python kalshi_trading.py orders [--status resting|canceled|executed]
    python kalshi_trading.py fills
    python kalshi_trading.py scan                      # live gas-nearclose candidates right now
    python kalshi_trading.py ticket --ticker T --side yes|no --price CENTS --qty N [--note "..."]
    python kalshi_trading.py list-tickets [--status ticket|submitted|resting|filled|rejected|ambiguous|canceled]
    python kalshi_trading.py execute --ticket-id N --live     # THE DANGEROUS ONE. See HARD RULE above.
    python kalshi_trading.py check-ticket --ticket-id N       # reconcile an 'ambiguous' ticket via client_order_id
    python kalshi_trading.py cancel --order-id ID

API notes (verified 2026-07-01 against docs.kalshi.com -- NOT the legacy v1
shape, which is on a deprecation track this year):
    POST   /trade-api/v2/portfolio/events/orders        create order (v2)
    DELETE /trade-api/v2/portfolio/events/orders/{id}   cancel order (v2)
    GET    /trade-api/v2/portfolio/balance
    GET    /trade-api/v2/portfolio/positions
    GET    /trade-api/v2/portfolio/orders
    GET    /trade-api/v2/portfolio/fills

Order body fields: ticker, side ("bid"|"ask"), count, price, time_in_force,
self_trade_prevention_type, client_order_id (idempotency key -- always sent;
Kalshi 409s a resubmit with the same id rather than double-ordering).

`side` is bid/ask on the YES LEG ONLY (per the official create-order-v2
schema): bid = buy YES, ask = sell YES. `price` is ALWAYS the yes-denominated
price in dollars, regardless of side -- selling YES at price p is the same
trade as buying NO at (1-p). This module's internal convention (mirroring
fade_backtest.py / paper_trades.py) is "our_side: yes|no" + "our_price_cents:
the cost of the side we hold" -- see to_kalshi_order_fields() for the
conversion, which self-checks at import time so a future edit can't silently
invert it and flip every order to the wrong side.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid

import client
import db

# Must match experiments.py:series_gas_nearclose_roi's segment_expr exactly --
# this is the one validated candidate (see experiments_tracker.md).
GAS_SERIES = ("KXAAAGASW", "KXAAAGASD", "KXAAAGASM")
NEARCLOSE_WINDOW_SEC = 3600

# The backtested edge nets ~$400/day of TOTAL opportunity across all gas
# markets combined -- individual positions should be tiny. This is a soft
# warning, not a hard cap, since the user may deliberately want to size up.
DEFAULT_MAX_NOTIONAL_CENTS = 5000  # $50

SCHEMA = """
CREATE TABLE IF NOT EXISTS live_tickets (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy          TEXT NOT NULL,
    ticker            TEXT NOT NULL,
    our_side          TEXT NOT NULL,          -- yes | no (the side WE want)
    price_cents       INTEGER NOT NULL,       -- cost of OUR side, in cents
    qty               REAL NOT NULL,
    api_side          TEXT NOT NULL,          -- bid | ask, as sent to Kalshi
    api_price_dollars TEXT NOT NULL,          -- as sent to Kalshi
    client_order_id   TEXT NOT NULL,
    note              TEXT,
    status            TEXT DEFAULT 'ticket',  -- ticket | submitted | resting | filled | rejected | ambiguous | canceled
    kalshi_order_id   TEXT,
    created_ts        INTEGER NOT NULL,
    submitted_ts      INTEGER
);
"""


def connect():
    con = db.connect()
    con.executescript(SCHEMA)
    return con


# ---------------------------------------------------------------------------
# The one conversion that must never be silently wrong.
# ---------------------------------------------------------------------------


def to_kalshi_order_fields(our_side: str, our_price_cents: int) -> tuple[str, str]:
    """our_side='yes'|'no', our_price_cents=cost of THAT side in cents (1-99).
    Returns (api_side, api_price_dollars) for the create-order-v2 request body.

    Verified 2026-07-01 against docs.kalshi.com/api-reference/orders/create-order-v2:
      want YES @ P  ->  side=bid, price=P/100
      want NO  @ P  ->  side=ask, price=(100-P)/100   (sell YES at 100-P == buy NO at P)
    """
    if our_side not in ("yes", "no"):
        raise ValueError(f"our_side must be 'yes' or 'no', got {our_side!r}")
    if not (0 < our_price_cents < 100):
        raise ValueError(f"our_price_cents must be 1-99, got {our_price_cents!r}")
    if our_side == "yes":
        api_side, yes_price_cents = "bid", our_price_cents
    else:
        api_side, yes_price_cents = "ask", 100 - our_price_cents
    return api_side, f"{yes_price_cents/100:.2f}"


def _self_check() -> None:
    assert to_kalshi_order_fields("yes", 56) == ("bid", "0.56")
    assert to_kalshi_order_fields("no", 38) == ("ask", "0.62")
    assert to_kalshi_order_fields("no", 1) == ("ask", "0.99")
    assert to_kalshi_order_fields("yes", 99) == ("bid", "0.99")


_self_check()  # fail loudly at import time if this mapping is ever edited incorrectly


def describe_ticket_plain(ticker: str, our_side: str, price_cents: int, qty: float) -> str:
    notional = price_cents * qty / 100
    return f"BUY {our_side.upper()} on {ticker} @ {price_cents}c x {qty:g} contracts (${notional:,.2f} notional)"


# ---------------------------------------------------------------------------
# Read-only endpoints (safe -- no money moves, no orders placed)
# ---------------------------------------------------------------------------


def get_balance() -> dict:
    return client.get("/trade-api/v2/portfolio/balance")


def get_positions() -> dict:
    return client.get("/trade-api/v2/portfolio/positions")


def get_orders(status: str | None = None) -> dict:
    return client.get("/trade-api/v2/portfolio/orders", {"status": status} if status else None)


def get_fills() -> dict:
    return client.get("/trade-api/v2/portfolio/fills")


def get_market_quote(ticker: str) -> dict:
    """Live yes_bid/yes_ask/no_bid/no_ask in cents, from the current market
    snapshot. Same endpoint + response shape enrich_markets.py already uses
    (body["market"]), reused here rather than guessed."""
    body = client.get(f"/trade-api/v2/markets/{ticker}")
    m = body.get("market") or {}

    def c(key):
        v = m.get(key)
        return round(float(v) * 100) if v not in (None, "") else None

    return {
        "yes_bid": c("yes_bid_dollars"), "yes_ask": c("yes_ask_dollars"),
        "no_bid": c("no_bid_dollars"), "no_ask": c("no_ask_dollars"),
        "status": m.get("status"),
    }


# ---------------------------------------------------------------------------
# Scan: find a live fade candidate right now (read-only: local DB + one quote
# fetch per candidate). Mirrors experiments.py:series_gas_nearclose_roi's
# validated segment -- don't loosen this without re-backtesting first.
# ---------------------------------------------------------------------------


def scan_gas_nearclose(lookback_sec: int = 600) -> list[dict]:
    con = db.connect()
    now = int(time.time())
    placeholders = ",".join("?" * len(GAS_SERIES))
    rows = con.execute(f"""
        SELECT DISTINCT m.ticker, m.close_ts
        FROM markets m
        WHERE m.series_ticker IN ({placeholders})
          AND m.result IS NULL
          AND m.close_ts IS NOT NULL
          AND m.close_ts > ?
          AND m.close_ts - ? < ?
    """, (*GAS_SERIES, now, now, NEARCLOSE_WINDOW_SEC)).fetchall()

    candidates = []
    for r in rows:
        taker = con.execute("""
            SELECT taker_side, price_cents, created_ts
            FROM trades_social
            WHERE ticker = ? AND created_ts >= ?
            ORDER BY created_ts DESC LIMIT 1
        """, (r["ticker"], now - lookback_sec)).fetchone()
        if taker is None:
            continue  # closing soon, but no recent taker to fade
        fade_side = "no" if taker["taker_side"] == "yes" else "yes"
        try:
            quote = get_market_quote(r["ticker"])
        except client.KalshiError as e:
            candidates.append({"ticker": r["ticker"], "error": str(e)})
            continue
        candidates.append({
            "ticker": r["ticker"],
            "close_in_sec": r["close_ts"] - now,
            "taker_side": taker["taker_side"],
            "taker_price_cents": taker["price_cents"],
            "taker_age_sec": now - taker["created_ts"],
            "fade_side": fade_side,
            "quote": quote,
        })
    return candidates


# ---------------------------------------------------------------------------
# Ticket: pure local computation + logging. No Kalshi order call happens here.
# ---------------------------------------------------------------------------


def build_ticket(con, strategy: str, ticker: str, our_side: str, price_cents: int, qty: float,
                  note: str = "") -> int:
    api_side, api_price = to_kalshi_order_fields(our_side, price_cents)
    notional = price_cents * qty
    if notional > DEFAULT_MAX_NOTIONAL_CENTS:
        print(f"WARNING: notional ${notional/100:.2f} exceeds the default "
              f"${DEFAULT_MAX_NOTIONAL_CENTS/100:.2f} soft cap for this small-edge strategy. "
              f"Ticket will still be built -- double check the size before executing.")
    client_order_id = str(uuid.uuid4())
    con.execute("""
        INSERT INTO live_tickets (strategy, ticker, our_side, price_cents, qty, api_side,
                                   api_price_dollars, client_order_id, note, created_ts)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (strategy, ticker, our_side, price_cents, qty, api_side, api_price,
          client_order_id, note, int(time.time())))
    con.commit()
    tid = con.execute("SELECT last_insert_rowid()").fetchone()[0]
    print(f"Ticket #{tid} built (NOT submitted): {describe_ticket_plain(ticker, our_side, price_cents, qty)}")
    print(f"  Kalshi fields: side={api_side}  price=${api_price}  count={qty:.2f}  "
          f"client_order_id={client_order_id}")
    return tid


# ---------------------------------------------------------------------------
# Execute: the dangerous one. Interactive-only, exact-match confirmation,
# stale-price guard, no auto-retry. The assistant will never call this.
# ---------------------------------------------------------------------------


def execute_live(con, ticket_id: int) -> int:
    if not sys.stdin.isatty():
        print("Refusing to run: this must be executed interactively, by a human, in their own terminal.")
        return 1

    row = con.execute("SELECT * FROM live_tickets WHERE id=?", (ticket_id,)).fetchone()
    if row is None:
        print(f"No ticket #{ticket_id}")
        return 1
    if row["status"] != "ticket":
        print(f"Ticket #{ticket_id} already has status={row['status']!r} -- refusing to resubmit.")
        return 1

    quote = get_market_quote(row["ticker"])
    if row["our_side"] == "yes":
        current_price = quote.get("yes_ask")
    else:
        current_price = (100 - quote["yes_bid"]) if quote.get("yes_bid") is not None else None
    if current_price is None:
        print("Could not get a current live quote for this ticker -- refusing to execute a stale ticket.")
        return 1
    drift = abs(current_price - row["price_cents"])
    if drift > 5:
        print(f"Price has moved {drift}c since this ticket was built "
              f"({row['price_cents']}c -> {current_price}c now). Build a fresh ticket instead.")
        return 1

    print("=" * 70)
    print("YOU ARE ABOUT TO SUBMIT A REAL, LIVE, REAL-MONEY ORDER.")
    print("=" * 70)
    print(describe_ticket_plain(row["ticker"], row["our_side"], row["price_cents"], row["qty"]))
    print(f"  Kalshi fields: side={row['api_side']}  price=${row['api_price_dollars']}  "
          f"count={row['qty']:.2f}  client_order_id={row['client_order_id']}")
    print()
    # Every field that determines dollar exposure must be re-typed here, not
    # just ticker/side/price -- qty was originally missing, which meant a
    # fat-fingered --qty at ticket-build time could slip through execution
    # completely unconfirmed. Caught in adversarial review, fixed here.
    expected = f"CONFIRM {row['ticker']} {row['our_side'].upper()} {row['price_cents']} QTY {row['qty']:g}"
    typed = input(f'Type exactly "{expected}" to submit this live order, or anything else to abort: ')
    if typed.strip() != expected:
        print("Aborted -- confirmation did not match. No order submitted.")
        return 1

    body = {
        "ticker": row["ticker"],
        "client_order_id": row["client_order_id"],
        "side": row["api_side"],
        "count": f"{row['qty']:.2f}",
        "price": row["api_price_dollars"],
        # Fade the print now or don't -- a resting order could fill hours
        "time_in_force": "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
    }
    con.execute("UPDATE live_tickets SET status='submitted', submitted_ts=? WHERE id=?",
                (int(time.time()), ticket_id))
    con.commit()
    try:
        resp = client.post("/trade-api/v2/portfolio/events/orders", body, retries=0)
    except client.KalshiError as e:
        if e.status == 409:
            # Kalshi rejects a resubmit of the same client_order_id -- meaning
            # SOME attempt with this id already reached the exchange. That's
            # not "failed", it's "we don't know from here" -- resolve with
            # check-ticket, don't touch it further.
            print("409 Conflict: an order with this client_order_id already exists on Kalshi's side. "
                  f"Run `python kalshi_trading.py check-ticket --ticket-id {ticket_id}` to find out what "
                  "actually happened before doing anything else. Do NOT build a new ticket for this "
                  "market/side until you've resolved this one.")
            con.execute("UPDATE live_tickets SET status='ambiguous' WHERE id=?", (ticket_id,))
        else:
            # A definitive rejection (bad request, insufficient balance, etc.)
            # -- Kalshi did see this and said no. Safe to call genuinely failed.
            print(f"Order submission FAILED: {e}")
            con.execute("UPDATE live_tickets SET status='rejected' WHERE id=?", (ticket_id,))
        con.commit()
        return 1
    except Exception as e:
        # Network-level failure (timeout, connection reset, DNS blip -- not a
        # response FROM Kalshi at all). We genuinely do not know whether the
        # order reached the exchange. Do NOT mark 'rejected' -- that would
        # falsely imply nothing happened. Leave it 'ambiguous' and force a
        # manual check via client_order_id before anything else touches this
        # ticket or this market/side.
        print(f"Network-level failure, no response from Kalshi: {e!r}")
        print(f"We do NOT know if this order reached the exchange. Run "
              f"`python kalshi_trading.py check-ticket --ticket-id {ticket_id}` before doing anything "
              "else -- do not blindly retry or assume it failed.")
        con.execute("UPDATE live_tickets SET status='ambiguous' WHERE id=?", (ticket_id,))
        con.commit()
        return 1

    order_id = resp.get("order_id")
    fill_count = resp.get("fill_count")
    remaining = resp.get("remaining_count")
    if fill_count and float(fill_count) > 0:
        status = "filled"
    elif remaining and float(remaining) > 0:
        status = "resting"
    else:
        # immediate_or_cancel with zero fill and zero remaining -> Kalshi
        # accepted then fully canceled it (nothing crossed). Not "submitted"
        # (that implies still in flight) -- it's already resolved.
        status = "canceled"
    con.execute("UPDATE live_tickets SET kalshi_order_id=?, status=? WHERE id=?", (order_id, status, ticket_id))
    con.commit()
    print(f"Order submitted. order_id={order_id}  fill_count={fill_count}  remaining_count={remaining}")
    return 0


def check_ticket(con, ticket_id: int) -> int:
    """Reconcile an 'ambiguous' (or any) ticket against Kalshi's own order
    list by client_order_id -- the only safe way to resolve "did that actually
    go through" after a 409 or a network-level failure during execute."""
    row = con.execute("SELECT * FROM live_tickets WHERE id=?", (ticket_id,)).fetchone()
    if row is None:
        print(f"No ticket #{ticket_id}")
        return 1
    coid = row["client_order_id"]
    print(f"Checking Kalshi for client_order_id={coid}  (ticket #{ticket_id}, local status={row['status']!r})...")
    orders = get_orders().get("orders", [])
    match = next((o for o in orders if o.get("client_order_id") == coid), None)
    if match is None:
        print("NOT found among current orders. Either it never reached Kalshi (safe to build a fresh "
              "ticket instead), or it resolved long enough ago to fall off this endpoint -- check "
              "`python kalshi_trading.py fills` for this ticker too before concluding it's safe to retry.")
        return 0
    kalshi_status = match.get("status")
    print(f"FOUND: order_id={match.get('order_id')}  status={kalshi_status}  "
          f"fill_count={match.get('fill_count_fp')}  remaining={match.get('remaining_count_fp')}")
    new_status = {"resting": "resting", "executed": "filled", "canceled": "canceled"}.get(kalshi_status, "ambiguous")
    con.execute("UPDATE live_tickets SET status=?, kalshi_order_id=? WHERE id=?",
                (new_status, match.get("order_id"), ticket_id))
    con.commit()
    print(f"Local ticket status updated to {new_status!r}.")
    return 0


def cancel_order(order_id: str) -> int:
    if not sys.stdin.isatty():
        print("Refusing to run: must be interactive.")
        return 1
    print(f"About to CANCEL live order {order_id}.")
    expected = f"CANCEL {order_id}"
    typed = input(f'Type exactly "{expected}" to proceed, or anything else to abort: ')
    if typed.strip() != expected:
        print("Aborted.")
        return 1
    try:
        resp = client.delete(f"/trade-api/v2/portfolio/events/orders/{order_id}", retries=0)
    except client.KalshiError as e:
        print(f"Cancel failed: {e}")
        return 1
    print(f"Canceled. reduced_by={resp.get('reduced_by')}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("balance")
    sub.add_parser("positions")
    p = sub.add_parser("orders")
    p.add_argument("--status", choices=["resting", "canceled", "executed"], default=None)
    sub.add_parser("fills")
    sub.add_parser("scan")

    p = sub.add_parser("ticket")
    p.add_argument("--strategy", default="gas_nearclose")
    p.add_argument("--ticker", required=True)
    p.add_argument("--side", required=True, choices=["yes", "no"])
    p.add_argument("--price", type=int, required=True, help="cents, 1-99")
    p.add_argument("--qty", type=float, required=True)
    p.add_argument("--note", default="")

    p = sub.add_parser("list-tickets")
    p.add_argument("--status", default=None)

    p = sub.add_parser("execute")
    p.add_argument("--ticket-id", type=int, required=True)
    p.add_argument("--live", action="store_true", required=True,
                    help="required -- explicit friction, not just a default-off flag")

    p = sub.add_parser("check-ticket")
    p.add_argument("--ticket-id", type=int, required=True)

    p = sub.add_parser("cancel")
    p.add_argument("--order-id", required=True)

    args = ap.parse_args()

    if args.cmd == "balance":
        print(json.dumps(get_balance(), indent=2))
    elif args.cmd == "positions":
        print(json.dumps(get_positions(), indent=2))
    elif args.cmd == "orders":
        print(json.dumps(get_orders(args.status), indent=2))
    elif args.cmd == "fills":
        print(json.dumps(get_fills(), indent=2))
    elif args.cmd == "scan":
        cands = scan_gas_nearclose()
        if not cands:
            print("No live gas-nearclose candidates right now.")
        for c in cands:
            print(json.dumps(c, indent=2))
    elif args.cmd == "ticket":
        build_ticket(connect(), args.strategy, args.ticker, args.side, args.price, args.qty, args.note)
    elif args.cmd == "list-tickets":
        con = connect()
        q = "SELECT * FROM live_tickets"
        params: tuple = ()
        if args.status:
            q += " WHERE status=?"
            params = (args.status,)
        q += " ORDER BY created_ts DESC"
        for r in con.execute(q, params).fetchall():
            print(dict(r))
    elif args.cmd == "execute":
        return execute_live(connect(), args.ticket_id)
    elif args.cmd == "check-ticket":
        return check_ticket(connect(), args.ticket_id)
    elif args.cmd == "cancel":
        return cancel_order(args.order_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
