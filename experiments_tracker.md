# Experiments Tracker

Lightweight registry of hypotheses we've tested, their results, and whether we've taken them through a **realistic backtest** (not just a segment-vs-complement stat test).

The `experiments` DB table is the source of truth for raw results. This doc is the curated view — **trimmed 2026-06-08 to the three strongest live candidates.** Everything previously tested-and-killed (Sports mirages, weekly gas fade at scale, gas favorites, midrange, overnight-ET overlap, named-maker, CABOUT/Hormuz standalone, tier-favorite look-ahead) lives in git history.

## Status legend
- 🔬 **Tested** — ran through the `daily_experiment.py` framework (two-sample test on resolved trades)
- ⚗️ **Backtest pending** — flagged as interesting; realistic P&L sim not yet run
- ✅ **Backtested** — survived a realistic P&L sim + concentration audit
- 📉 **Dead** — backtested and failed, or shown to be noise

## 🚨 Project policy: Sports excluded

As of 2026-05-12, **Sports is excluded project-wide** (Dave has no domain edge there; the category drowned out everything else and produced only 2-NBA-game mirages). Enforced in `experiments.py:_TRADE_ROW_CTE` and both backtesters (`--include-sports` to override).

## The two backtesters

```bash
# Fade (bet AGAINST the taker) — for segments where takers lose:
python fade_backtest.py --segment "<sql>" --min-notional 0 --exec-source public

# Coat-tail (COPY the taker) — for segments where takers win:
python coat_tail_backtest.py --segment "<sql>" --exec-source social
```

Both simulate entry at the next trade's price and include the concentration audit (top-2-market P&L share, ex-top-2 ROI, per-close-date breakdown). **An edge is only real if it survives ex-top-2 AND has majority-positive close-dates across multiple weeks.**

---

## The three live candidates (2026-06-08)

### 🥇 `cat_politics_elections_coattail` — COPY Politics/Elections takers
- **Hypothesis:** Takers in Politics + Elections markets are informed; copying their trades at the next print is +EV.
- **Mechanism:** Consistent with the 2026 insider-trading wave (DOJ/CFTC Polymarket-Maduro indictment, Kalshi's 400+ flagged trades). Entry slippage confirms it: price runs toward the taker's side right after they print (Hormuz +18.9¢, Elections +10¢) — informed-flow signature.
- **Status:** ✅ Backtested 2026-06-08 — **first candidate to clear every bar at once**
- **Backtest command:** `python coat_tail_backtest.py --segment "m.category IN ('Politics','Elections')" --exec-source social`
- **Headline:** **+56.76% post-2%-fee** on $8,983 notional (392 coat-tail entries, 52.6% coverage). Takers themselves: +76.27%.
- **Concentration audit (all PASS):**
  - 22 distinct markets; top-2 = 62.7% of P&L (elevated but not fatal)
  - **Ex-top-2: +25.31% post-fee on $7.2K** — survives with margin
  - **4 of 4 close-dates positive — spanning April AND May** (04-20 +186%, 04-21 +37%, 05-20 +17%, 05-27 +24%). NOT the April-mirage pattern.
- **Sub-slices:** works in favorites (+43% post-fee at >70¢) AND longshots (+50%); best window 1-24h pre-close (+35%) and <1h (+143%, smaller n).
- **Fee sensitivity:** +50.76% even at 8% fees. Extremely forgiving.
- **Caveats:** modest notional ($9K over ~6 weeks), 52% coverage, and the April cluster still contributes 2 of 4 dates. Want 8+ close-dates before sizing.
- **Next steps:** (1) keep accumulating — every politics/elections close adds a data point; (2) slice by sub-series as n grows; (3) legal review before any live deployment (coat-tailing suspected-informed flow is a regulatory grey zone — see 2026 enforcement wave).

### 🥈 `series_gas_nearclose_roi` — FADE gas-family takers in the final hour
- **Hypothesis:** Gas-price-market takers (KXAAAGASW/D/M) trading within 1h of close are overconfident; fading them is +EV.
- **Mechanism:** "I'm sure gas clears $4.50" near-certainty buying at the deadline; the classic retail overconfidence pathology, concentrated where it peaks.
- **Status:** ⚗️ Backtested 2026-05-31, re-confirmed 2026-06-05 — survives ex-top-2, sample still thin
- **Backtest command:** `python fade_backtest.py --segment "m.series_ticker IN ('KXAAAGASW','KXAAAGASD','KXAAAGASM') AND m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) < 3600" --min-notional 0 --exec-source public`
- **Headline:** **+22.05% post-2%-fee** on $11,164 notional (229 fades, 30.1% coverage). Takers: –42.72%.
- **Concentration audit:** 11 markets; top-2 = 83.5% (high). **Ex-top-2: +14.71% post-fee** — survives. 3 of 3 close-dates positive (05-15, 05-16, 05-23).
- **Caveats:** only 3 close-dates; 05-23 carries $10K of $11K notional; numbers unchanged since 05-31 because few gas markets have resolved since. **Verdict expected ~mid-June** as more closes land.
- **Note:** the broader gas-family fade (all hours) is only +2.80% post-fee and the weekly-series-only version DIED at scale (–1.05% on 2,479 takers). The final-hour slice is the only gas framing still alive.

### 🥉 `trade_midrange_roi` — FADE midrange-price (40-60¢) takers (weak, watch-only)
- **Hypothesis:** Non-Sports takers at 40-60¢ (maximum-uncertainty pricing) lose more than baseline; fading them is mildly +EV.
- **Status:** 🔬 Tested 2026-05-15 (effect –45.7pp, p=2e-5), backtested 2026-06-05: **+2.35% post-2%-fee** on $7.2K (14.1% coverage). Takers: –39.73%.
- **Why it's still here:** the segment-test effect is large and highly significant, and the fade stays positive post-fee — but the margin is thin (dies at 5% fees) and the per-market concentration hasn't been audited at the current sample. Third place by default: everything else is dead.
- **Next:** concentration audit + re-run when sample grows. Promote or kill by end of June.

---

## Graveyard (one-liners; details in git history)

- **All Sports edges** (favorite-YES, underdog-NO, charm, huge-notional, near-close, overnight, integer-count): 2 NBA upsets on 2026-04-20 in costume. Policy exclusion since 2026-05-12.
- **Tier-favorite "+30% EV":** look-ahead bias (used full-period avg price for entry). Realistic timing → ~0%.
- **Extreme-tier longshot buyers:** –88% EV (favorite-longshot bias, textbook), shorting them needs a giant bankroll — not interesting.
- **Weekly gas fade (`series_kxaaagasw_roi`):** +5.1% at n=779 → **–1.05%** at n=2,479. Died at scale.
- **Gas favorites >70¢:** +13% headline, **–2.91% ex-top-2**. Mirage.
- **Overnight-ET fade:** +18.9% headline but it's just the gas-nearclose trades wearing a time-of-day costume.
- **Named-maker predation:** rejected — takers facing named makers do *better* (+22pp, n=42).
- **CABOUT standalone:** +468% taker ROI but 1 close-date, 9 markets. Hormuz standalone: **ex-top-2 = –32%**. Both subsumed into the category-level coat-tail (#1).
- **Tiny-notional, large-round-count, climate-favorite, politics-fade:** dead in week-1 backtests.

---

_Add new experiments above the graveyard as they surface._
