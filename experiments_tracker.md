# Experiments Tracker

Lightweight registry of hypotheses we've tested, their results, and whether we've taken them through a **realistic backtest** (not just a segment-vs-complement stat test).

The `experiments` DB table is the source of truth for raw results. This doc is the curated view — **rewritten 2026-07-01.** The three prior "live candidates" (politics/elections coat-tail, gas-nearclose fade, midrange fade) are gone from this doc per instruction: politics/elections **died** on fresh data (see graveyard), gas-nearclose **survived** and is now the one deployed candidate (`kalshi_trading.py` is built around it — it doesn't need to keep living here as a "candidate," it's promoted to production), and midrange was always third-by-elimination. Their history is in git.

## Status legend
- 🔬 **Tested** — ran through the `daily_experiment.py` framework (two-sample test on resolved trades)
- ⚗️ **Backtest pending** — flagged as interesting; realistic P&L sim not yet run
- ✅ **Backtested** — survived a realistic P&L sim + concentration audit
- ⚠️ **Mixed / inconclusive** — some checks pass, some fail, or sample too thin to say either way
- 📉 **Dead** — backtested and failed, or shown to be noise

## 🚨 Project policy: Sports excluded

As of 2026-05-12, **Sports is excluded project-wide** (Dave has no domain edge there; the category drowned out everything else and produced only 2-NBA-game mirages). Enforced in `experiments.py:_TRADE_ROW_CTE` and both backtesters (`--include-sports` to override).

## The one deployed candidate

**`series_gas_nearclose_roi`** — fade any gas-family (KXAAAGASW/D/M) taker within 1h of market close. Validated at scale (33 close-dates, ex-top-2 +11.29% post-fee). `kalshi_trading.py` is built around this exact segment (see `GAS_SERIES` / `NEARCLOSE_WINDOW_SEC` constants). This doc's job now is finding out whether anything **extends or sharpens** it — see below.

## The two backtesters

```bash
# Fade (bet AGAINST the taker) — for segments where takers lose:
python fade_backtest.py --segment "<sql>" --min-notional 0 --exec-source public

# Coat-tail (COPY the taker) — for segments where takers win:
python coat_tail_backtest.py --segment "<sql>" --exec-source social
```

Both simulate entry at the next trade's price and include the concentration audit (top-2-market P&L share, ex-top-2 ROI, per-close-date breakdown). **An edge is only real if it survives ex-top-2 AND has majority-positive close-dates across multiple weeks.**

---

## Five new hypotheses (2026-07-01), derived from what survived/died

Designed from three named learnings, not guesses:
- **Learning A** (gas-nearclose survived at scale): final-hour overconfidence on a recurring-threshold market is a real mechanism, not a mirage.
- **Learning B** (politics/elections coat-tail collapsed at scale): the taker-side edge there is real (+223% ROI) but decays to worthless within one trade of slippage (0.5¢→35¢ as sample grew 13×).
- **Learning C** (`taker_vs_named_maker_roi` rejection): named/social-visible participants don't behave like the "predatory sharp" thesis assumed — worth testing on the taker side too.

None of the five reach the same confidence bar the original gas-nearclose finding did. That's a real result, not a failure to find one — reported honestly below.

### ⚠️ `cat_economics_nearclose_roi` — generalize near-close fade beyond gas (DEGENERATE TEST)
- **Hypothesis:** Any Economics-category taker within 1h of close is overconfident, not just gas takers.
- **Backtest:** `python fade_backtest.py --segment "m.category = 'Economics' AND m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) < 3600"` → +21.52% post-fee, ex-top-2 +14.61%, 4/5 close-dates positive.
- **Why this doesn't count as a generalization:** checked the series breakdown — **1,967 of 1,983 trades (99.2%) are `KXAAAGASD` (gas daily)**, plus 14 gas-monthly, plus exactly **2** trades from anything else (`KXADP`). This is gas wearing a category label, not an independent test.
- **Verdict:** inconclusive by construction — we don't currently have enough non-gas Economics volume to test whether the mechanism generalizes. Open question, not resolved either way. Revisit once non-gas Economics categories accumulate more near-close volume.

### 📉 `cat_crypto_nearclose_roi` — generalize near-close fade to Crypto (DEAD — insufficient data)
- **Hypothesis:** Crypto price-threshold markets (KXBTCD etc.) show the same near-close overconfidence.
- **Backtest:** `python fade_backtest.py --segment "m.category = 'Crypto' AND m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) < 3600"` → 0.4% execution coverage (11 of 3,045 taker trades), $52 total fadeable notional, **2 markets = 100% of P&L**, 1 close-date, ROI –3.08%.
- **Verdict:** dead on arrival, but from data scarcity, not disproof. Crypto near-close liquidity in `trades_public` is too thin to backtest at all right now. Flagged this risk before running it — confirmed.

### ⚠️ `trade_gas_nearclose_bignotional_roi` — refine by size (PROMISING, not yet independently confirmed)
- **Hypothesis:** Large (≥$15 notional — ~85th percentile for this population, not the $1000 "whale" threshold used elsewhere) gas-nearclose trades are even more overconfident than the pooled population.
- **Backtest:** `python fade_backtest.py --segment "m.series_ticker IN ('KXAAAGASW','KXAAAGASD','KXAAAGASM') AND m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) < 3600 AND t.price_cents * t.count_fp >= 1500"` → **+24.21% post-fee**, ex-top-2 **+21.89%** (stronger than the parent's +11.29%), 3/3 close-dates positive.
- **Caveat:** it's a strict subset of the already-validated population, with only **3 close-dates** (vs. 33 for the full gas-nearclose signal) — directionally supportive and encouraging, but far too thin to independently confirm at the same confidence level as the parent finding.
- **Verdict:** promising refinement, worth re-checking as more gas closes accrue. Not yet a reason to change `kalshi_trading.py`'s sizing logic.

### ⚠️ `trade_gas_nearclose_named_roi` — named vs anonymous near-close takers (TOO THIN TO SAY ANYTHING)
- **Hypothesis:** Named (social-opted-in) gas-nearclose takers behave differently than anonymous ones.
- **Backtest:** `python fade_backtest.py --segment "m.series_ticker IN ('KXAAAGASW','KXAAAGASD','KXAAAGASM') AND m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) < 3600 AND t.taker_nickname IS NOT NULL AND t.taker_nickname != ''"` → +79.29% post-fee headline, but **$154 total fadeable notional**, 23 trades, 3 distinct markets, ex-top-2 leaves just $17.
- **Verdict:** meaningless at this scale. Only ~10% of gas-nearclose trades (199 of 1,981) are named at all — nowhere near enough to test Learning C on this population. Revisit once named-taker volume grows.

### ⚠️ `series_politics_fastcoattail` — does faster reaction rescue the dead politics coat-tail? (MIXED, and not currently actionable even if real)
- **Hypothesis:** The politics/elections taker edge (+223% ROI) decays via slippage as execution gets slower. Does restricting to only very fast fills recover a positive coat-tail edge?
- **Not a new `GENERATORS` entry** — this tests execution speed, not a different taker population, so it reuses `cat_politics_elections_roi`'s existing segment with a tighter `--window-sec`.
- **60s window:** `python coat_tail_backtest.py --segment "m.category IN ('Politics','Elections')" --exec-source social --window-sec 60` → +2.10% post-fee, but **ex-top-2 flips negative (–0.90%)**. Slippage was actually *worse* at 60s (+45.9¢) than at the unrestricted window (+35.4¢) — plausible explanation: markets with very fast next-fills are disproportionately the ones where OTHER traders are also racing to react, so price discovery is faster there too, not slower.
- **15s window:** `--window-sec 15` → +24.36% post-fee, **ex-top-2 survives at +9.99%** — but only 2.5% coverage (246 of 9,884 trades), and the close-date majority test **FAILS** (5 positive, 6 negative).
- **Operational catch, regardless of the numbers:** our own social collector polls every 8 seconds. A 15-second reaction window requires detecting the print, building a ticket, and executing inside one polling cycle — not achievable under the current **manual-trigger** design (`kalshi_trading.py` has no scheduler, by explicit choice). Even a confirmed 15s edge wouldn't be actionable as the tooling exists today.
- **Verdict:** genuinely mixed evidence, and moot for now — this would need full automation to even test live, which is out of scope for what's been built.

---

## Graveyard (one-liners; details in git history)

- **All Sports edges** (favorite-YES, underdog-NO, charm, huge-notional, near-close, overnight, integer-count): 2 NBA upsets on 2026-04-20 in costume. Policy exclusion since 2026-05-12.
- **Politics/Elections coat-tail (`cat_politics_elections_roi`):** looked like the best candidate in the project at n=745/4 close-dates (+56.76% post-fee, ex-top-2 +25.31%). Collapsed at n=9,884/24 close-dates to **–0.26% post-fee, ex-top-2 –8.49%**. Entry slippage exploded from +0.5¢ to +35¢ as sample grew — the informed-flow alpha is real but gets priced in before a next-trade follower can capture it. See `series_politics_fastcoattail` above for the (mixed, moot) follow-up.
- **Weekly gas fade (`series_kxaaagasw_roi`):** +5.1% at n=779 → **–1.05%** at n=2,479. Died at scale.
- **Gas favorites >70¢:** +13% headline, **–2.91% ex-top-2**. Mirage.
- **Overnight-ET fade:** +18.9% headline but it's just the gas-nearclose trades wearing a time-of-day costume.
- **Midrange fade (`trade_midrange_roi`):** never cleared +5% at any fee level, concentration never audited favorably. Third-by-elimination, retired without ceremony.
- **Tier-favorite "+30% EV":** look-ahead bias (used full-period avg price for entry). Realistic timing → ~0%.
- **Extreme-tier longshot buyers:** –88% EV (favorite-longshot bias, textbook), shorting them needs a giant bankroll — not interesting.
- **Named-maker predation:** rejected — takers facing named makers do *better* (+22pp, n=42).
- **CABOUT standalone:** +468% taker ROI but 1 close-date, 9 markets. Hormuz standalone: **ex-top-2 = –32%**. Both were subsumed into (and then died with) the category-level politics/elections coat-tail.
- **Tiny-notional, large-round-count, climate-favorite, politics-fade:** dead in week-1 backtests.

---

_Add new experiments above the graveyard as they surface._
