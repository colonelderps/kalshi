# Experiments Tracker

Lightweight registry of hypotheses we've tested, their results, and whether we've taken them through a **realistic backtest** (not just a segment-vs-complement stat test).

The `experiments` DB table is the source of truth for raw results. This doc is the curated view: what was promising, what got confirmed by a P&L sim, and what's dead.

## Status legend
- 🔬 **Tested** — ran through the `daily_experiment.py` framework (two-sample test on resolved trades)
- ⚗️ **Backtest pending** — flagged as interesting; realistic P&L sim not yet run
- ✅ **Backtested** — run through `fade_backtest.py --segment ...` or equivalent; edge confirmed
- 📉 **Dead** — backtested and failed, or segmented and shown to be noise

## 🚨 Project policy: Sports excluded

As of 2026-05-12, **Sports is excluded project-wide**. Dave has no domain edge there, and the category was drowning out non-Sports signals. Every prior "edge" we found in Sports turned out to be the same two NBA upsets on 2026-04-20 (DEN-MIN, NYK-ATL) — every Sports-flavored hypothesis collapsed once we either excluded those two games or re-ran at a larger sample.

**Policy enforcement:**
- `experiments.py` `_TRADE_ROW_CTE` filters out Sports — all `GENERATORS` hypotheses implicitly exclude it
- `fade_backtest.py` excludes Sports by default (`--include-sports` flag if ever needed)
- This doc only tracks non-Sports findings going forward

Historical Sports entries have been scrubbed from this tracker — git history preserves them if ever needed.

## Running a backtest

```bash
# General pattern: run fade_backtest with the segment SQL from the hypothesis
python fade_backtest.py --segment "<segment_expr>" --min-notional 0 --exec-source social
```

`--min-notional 0` removes the whale-size floor — most hypotheses aren't about size.
`--exec-source social` avoids the trades_public sparsity issue (see CLAUDE.md).

---

## Experiments

### 🔥 `series_gas_nearclose_roi` — Gas fade, final hour to close (CURRENT BEST CANDIDATE)
- **Hypothesis:** Gas-family trades (daily+weekly+monthly) placed within 1h of close have different ROI than baseline.
- **Rationale:** Sub-slice of the broader gas-family fade. The final-hour window is where "I'm sure gas will clear $4.50" overconfidence peaks — takers pay up for near-certainty that often misses.
- **Status:** ⚗️ Backtested 2026-05-31 — **survives ex-top-2 audit (first to do so with room), but sample thin**
- **Backtest command:** `python fade_backtest.py --segment "m.series_ticker IN ('KXAAAGASW','KXAAAGASD','KXAAAGASM') AND m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) < 3600" --min-notional 0 --exec-source public`
- **Backtest result (headline):** Fade → **+22.05% post-2% fee** on $11,164 notional (229 fades, 11 distinct markets).
- **Concentration audit:** Top-2 markets = 83.5% of P&L (high). **BUT ex-top-2: +14.71% post-fee on $2.65K** — first sub-slice to stay clearly positive after removing the luckiest 2. All **3 of 3 close-dates positive** (May-15 +4.4%, May-16 +93.2%, May-23 +25.8%).
- **Why not deployable yet:** Only **3 close-dates**, and May-23 alone carries $10K of the $11K notional. Promising shape, far too thin to bet. Needs 5-8 more close-dates.
- **Next step:** `series_gas_nearclose_roi` is in `GENERATORS`; re-audit in ~2 weeks (mid-June) when more close-dates accrue.

---

### ⛽ `series_kxaaagasw_roi` — Weekly gas-price fade (FIRST SURVIVING EDGE)
- **Hypothesis:** Trades in the AAA weekly gas-price series (KXAAAGASW) have different ROI than non-Sports baseline.
- **Rationale:** Retail bettors are systematically miscalibrated on average weekly gas prices. They think they know where gas is going; they don't. The market makes them pay for that miscalibration.
- **Tested:** landscape survey 2026-05-12. 779 resolved takers, **taker ROI –41.78%** on $20K notional. Spread across 22 days (the only non-Sports series with real temporal diversity in current sample).
- **Status:** ✅ Backtested 2026-05-12 — **fade edge confirmed, survives concentration audit**
- **Backtest command:** `python fade_backtest.py --segment "m.series_ticker = 'KXAAAGASW'" --min-notional 0 --exec-source public`
- **Backtest result (headline):** Fade takers → **+5.10% post-2% fee** on $7,523 fade notional (221 fade trades, 28.4% exec coverage). Survives all fee scenarios up to 5%.
- **Per-close-date breakdown (the key concentration check — all 3 positive):**
  | Close week | trades | fade ROI |
  |---|---|---|
  | 2026-04-27 | 49 | **+18.89%** |
  | 2026-05-04 | 81 | **+6.56%** |
  | 2026-05-11 | 91 | **+5.92%** |
  Unlike every prior "edge" we found, this isn't dominated by a single weekend.
- **Concentration audit:** Top 2 markets = 61.4% of P&L (high but not fatal). **Ex-top-2: +4.06% raw / +2.06% post-2% fee on $5K notional.** The edge stays positive after stripping the top markets — first time we've seen this.
- **Fee sensitivity:** +7.10% raw → +5.10% at 2% → +2.10% at 5%. Tight; needs Kalshi's actual fee curve verified before deployment.
- **Actionable strategy:** **Fade KXAAAGASW takers** — when a taker hits, immediately buy the opposite side at the next public-trade price. Hold to weekly settlement.
- **Caveats:**
  - Only 3 weekly closes of data — need 8-12 for statistical confidence
  - $7.5K total fade notional → small deployable size per week
  - 28.4% execution coverage means many trades have no fadeable next print within 1h
- **Next steps:** (1) Paper-trade the next 4-6 weekly closes (May 18, 25, Jun 1, 8). (2) Watch daily-experiment runs — `series_kxaaagasw_roi` is in `GENERATORS` and will retest each cycle. (3) If 8 consecutive weeks stay positive at >+3% post-fee, consider small live deployment.

---

### 📊 `trade_midrange_roi` — Midrange-price (40-60¢) fade (NEW, post-Sports-exclusion)
- **Hypothesis:** Trades placed at midrange prices (40-60¢) have different ROI than non-midrange.
- **Status:** 🔬 Tested 2026-05-15 (re-run after Sports exclusion) — **needs concentration audit before promoting**
- **Tested result:** n_seg=473, segment ROI **–49.66%**; n_base=1,721, baseline ROI –4.00%. Effect **–45.67pp**, p=2.14e-5.
- **Why this is interesting:** This was previously *positive* effect (+20.5%) in the Sports-included sample — Sports midrange-favorites were earning. With Sports excluded, midrange takers in everything-else are getting crushed. **Direction flipped + became highly significant.**
- **Caveat:** Need per-day concentration check next. If 1-2 events drive the P&L (the recurring pattern), demote to "needs more sample." If diversified, this is candidate #2.

---

### 🌙 `trade_overnight_et_roi` — Overnight ET fade (NEW, post-Sports-exclusion)
- **Hypothesis:** Trades placed during 00:00-06:00 UTC (~early-morning ET) have different ROI than rest-of-day.
- **Status:** 🔬 Tested 2026-05-15 — **modest effect, statistically clean, concentration check pending**
- **Tested result:** n_seg=366, segment ROI –25.70%; n_base=1,828, baseline ROI –15.49%. Effect **–10.22pp**, p=0.003.
- **Note:** Smaller effect than midrange but still in fade direction. Becomes significant with the bigger sample.

---

### 🕊️ `series_kxhormuztrafficw_roi` — Hormuz Strait traffic (coat-tail anomaly)
- **Hypothesis:** Trades in KXHORMUZTRAFFICW (Strait of Hormuz weekly traffic) have different ROI than baseline.
- **Status:** 📉 Flagged then demoted 2026-05-15 — **2-market mirage**
- **Original finding:** Landscape survey showed takers **WIN +37.92%** on this series. Re-test under `cat_politics_elections_roi` showed +66% segment ROI at p<0.001 (n=66).
- **Concentration check:** $724 of $594 net P&L (122%, with everything else net-negative) came from **two markets in the Hormuz series resolving on 2026-04-19** (T80 and T60). Without those 2 markets the segment is net-negative.
- **Verdict:** Same April-20/21 concentration pattern as the Sports findings. Re-test when we have more event-days of politics data (~2-3 more weeks with the local collector firehose).

---

### 🗳️ `series_kxcabout_roi` — KXCABOUT politics (coat-tail anomaly)
- **Hypothesis:** Trades in KXCABOUT politics series have different ROI than baseline.
- **Status:** 🔬 Flagged 2026-05-12 — needs coat-tail backtest at larger sample
- **Notes:** Landscape showed takers +32.35%. Same caveat as Hormuz (thin sample, likely 1-2 events driving). Defer until firehose accumulates more politics trades.

---

### 🛢️ `series_aaa_gas_family_roi` — Gas family generalization (extrapolation of kxaaagasw)
- **Hypothesis:** Trades in AAA gas-price family (daily KXAAAGASD + weekly KXAAAGASW + monthly KXAAAGASM) have different ROI than baseline.
- **Status:** ✅ Backtested 2026-05-31 — **mechanism real, but the edge lives in sub-slices, not family-wide**
- **Tested result (2026-05-15):** n_seg=131, segment ROI –74.76%; baseline –14.26%. Effect –60.50pp, p=0.493 (named-only cohort too thin to be significant).
- **Backtest (2026-05-31):** Full family fade → +4.30% raw / **+2.30% post-2% fee** on $52.5K notional (4,587 takers). Survives 2% but flips negative at 5% fees. Family-wide is the WEAKEST framing.
- **Sub-slice breakdown (where the edge actually concentrates):**
  - **Favorites >70¢:** +13% post-fee headline BUT **ex-top-2 = –2.91%** → 📉 dead (mirage)
  - **Final hour (<1h):** +22% post-fee, **ex-top-2 = +14.71%** → 🔥 promoted to its own entry (`series_gas_nearclose_roi`, top of doc)
- **Verdict:** Don't trade the family flat. The final-hour sub-slice is the live candidate; favorites-fade is dead.

---

### 🏛️ `cat_politics_elections_roi` — Politics/Elections coat-tail (extrapolation)
- **Hypothesis:** Politics + Elections category trades have different (positive) ROI vs baseline.
- **Status:** 📉 Significant at face value, **2-market mirage** on inspection (2026-05-15)
- **Tested result:** n_seg=66, segment ROI **+66.10%**; baseline –20.07%. Effect **+86.17pp**, p=1.03e-6.
- **Concentration:** Same Hormuz T80/T60 markets drive 122% of P&L (rest net-negative). Not yet trustable.

---

### 🛒 `taker_vs_named_maker_roi` — Named makers prey on takers (NOVEL maker-side test)
- **Hypothesis:** Takers facing a named (opt-in social) maker lose more than takers facing anonymous makers.
- **Status:** 📉 Tested 2026-05-15 — **hypothesis rejected at face value**
- **Tested result:** n_seg=42, segment ROI **+3.17%**; baseline –19.14%. Effect **+22.31pp** (wrong direction), p=0.315.
- **Interpretation:** Takers facing named makers actually do *slightly better* than facing anonymous makers. Either (a) named makers are recreational not predatory, or (b) n=42 is too small. Re-run at larger sample before final verdict.

---

_Add new experiments above this line as they surface._
