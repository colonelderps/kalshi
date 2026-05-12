# Experiments Tracker

Lightweight registry of hypotheses we've tested, their results, and whether we've taken them through a **realistic backtest** (not just a segment-vs-complement stat test).

The `experiments` DB table is the source of truth for raw results. This doc is the curated view: what was promising, what got confirmed by a P&L sim, and what's dead.

## Status legend
- 🔬 **Tested** — ran through the `daily_experiment.py` framework (two-sample test on resolved trades)
- ⚗️ **Backtest pending** — flagged as interesting; realistic P&L sim not yet run
- ✅ **Backtested** — run through `fade_backtest.py --segment ...` or equivalent; edge confirmed
- 📉 **Dead** — backtested and failed, or segmented and shown to be noise

## 🚨 Corrections (2026-05-12)

**Most prior "✅ Backtested" Sports entries are mirages.** Almost all P&L came from two NBA upsets on 2026-04-20 (DEN-MIN, NYK-ATL). After excluding those two games OR after rerunning at the 22-day sample, every Sports-favorite / underdog-NO / charm-pricing / huge-notional fade either flips negative or collapses to <30% of its headline ROI. Tier-favorite "+30% EV" was look-ahead bias (full-period avg used for entry; realistic timing → ~0% ROI).

**Net: 0 confirmed Sports edges as of 2026-05-12.**

**Policy: Sports excluded project-wide.** Enforced in `experiments.py:_TRADE_ROW_CTE` and `fade_backtest.py` (override: `--include-sports`). See CLAUDE.md gotchas.

**The one surviving edge** is the **gas-price weekly fade** (`series_kxaaagasw_roi`) — first non-Sports candidate to survive concentration audit. Entry below.

## Running a backtest

```bash
# General pattern: run fade_backtest with the segment SQL from the hypothesis
python fade_backtest.py --segment "<segment_expr>" --min-notional 0 --exec-source social
```

`--min-notional 0` removes the whale-size floor — most hypotheses aren't about size.
`--exec-source social` avoids the trades_public sparsity issue (see CLAUDE.md).

---

## Experiments

### 🎯 `trade_price_ends_in_9_roi` — Charm pricing
- **Hypothesis:** Trades at prices ending in 9¢ (9, 19, 29, …, 99) earn different ROI than trades at other prices.
- **Rationale:** Charm pricing / anchoring — do buyers perceive $.29 as meaningfully cheaper than $.30?
- **Tested:** 2026-04-22. segment n=2,684, ROI=**–36.86%**; baseline n=22,315, ROI=**–13.39%**. Effect **–23.47pp**, p=0.024. Auto-flagged as candidate.
- **Status:** ✅ Backtested 2026-04-22 — **confirmed, but only on a sub-slice**
- **Backtest command:** `python fade_backtest.py --segment "t.price_cents IN (9,19,29,39,49,59,69,79,89,99)" --min-notional 0 --exec-source social`
- **Backtest result (headline):** Fading all charm takers → **+10.09% net** ROI on $1.3M notional after 2% fees (14,434 trades, 97.5% exec coverage).
- **Backtest result (the real story — price-bucket breakdown):**
  | Price bucket | n | Taker ROI | Fade ROI (post-fee) |
  |---|---|---|---|
  | **>70¢ favorite** | 4,342 | –52.73% | **+70.51%** |
  | 50-70¢ | 2,879 | –36.31% | +30.90% |
  | 30-50¢ | 3,625 | +17.54% | –10.43% |
  | **<30¢ longshot** | 3,588 | **+203.70%** | –32.22% |
  Two opposite effects were averaging out. Charm-priced *favorites* get crushed (pay 89¢ to win 11¢ — asymmetric downside). Charm-priced *longshots* actually win big.
- **Time-to-close split:** <1h to close → fade ROI **+37.9%**. 1-24h → –10.9%. Edge concentrates in the final hour.
- **Category split:** Sports dominates the sample ($1.28M of $1.3M notional) at +9.3% fade post-fee; other categories have samples too tiny to trust yet.
- **Fee sensitivity:** raw +12.09%; still +7.09% at 5% fees; +4.09% at 8%.
- **Actionable strategy:** **Fade charm-priced favorites (≥70¢) within 1h of close**, specifically in Sports. Do *not* fade charm longshots — those takers are winning.
- **Follow-ups worth doing:** (a) re-run after more social data accumulates; (b) test "charm favorites × specific ≥70¢ thresholds" (maybe the edge is really at 89¢/99¢, not 79¢); (c) `combine_experiments` may naturally surface this as a pair once we log `trade_favorite_yes_roi` results.

---

### 💎 `trade_favorite_yes_roi` — Favorites get crushed
- **Hypothesis:** Takers buying YES at >70¢ earn different ROI than takers at other prices.
- **Rationale:** Pay a lot to win a little — any miss ruins many wins. "Overconfidence on favorites" is a classic retail pathology.
- **Tested:** candidate in the daily experiment log. segment n=3,566, ROI=–37.30%; baseline n=21,429, ROI=–4.22%; effect **–33.07pp**, p≈0.
- **Status:** ✅ Backtested 2026-04-22 — **strong edge confirmed at scale**
- **Backtest command:** `python fade_backtest.py --segment "t.taker_side='yes' AND t.price_cents > 70" --min-notional 0 --exec-source social`
- **Backtest result (headline):** Fading favorite-YES takers → **+16.71% net** ROI on $1.84M fade notional after 2% fees (19,456 trades, 98.8% exec coverage). Takers themselves lost **$1.43M on $4.27M notional**.
- **Backtest result (time-to-close breakdown — this is the critical slice):**
  | Time to close | n | Taker ROI | Fade ROI (post-fee) |
  |---|---|---|---|
  | **1-24h before close** | 9,847 | –43.95% | **+89.21%** |
  | <1h before close | 9,609 | –22.09% | **–35.00%** |
  **Edge is in 1-24h window, NOT the final hour.** Contradicts the charm-price conclusion — charm-price's "<1h best" was a tiny-sample artifact. In the final hour, markets have already converged and the fade becomes a coin flip losing to fees.
- **Category split:** Sports dominates — 18,011 trades, $1.80M notional, **+18.92% post-fee**. Other categories have samples too tiny to trust (many show "–100%" fade ROI from 1-2 losses in a 30-trade cohort).
- **Fee sensitivity:** +18.71% raw → still **+10.71% at 8% fees**. Extremely forgiving.
- **Actionable strategy:** **Fade favorite-YES takers (price >70¢) in Sports markets, placed 1-24 hours before market close.** Buy NO at next trade, hold to expiry.
- **Expected scale (from sample):** $764K of fade notional in the "hot slice" (1-24h Sports favorites) over the social-firehose window (~2 wks of collection). Annualized, that's meaningful but not infinite — liquidity per individual market is still the constraint for a live executor.
- **Subsumes:** the charm-price edge is a proper subset of this one. Act on this, not charm separately.

---

### 🚨 `trade_sports_underdog_no_roi` — Sports "pay-up-for-certainty" NO takers
- **Hypothesis:** Takers buying NO at ≥70¢ in Sports markets earn different ROI than other takers.
- **Rationale:** Mirror of favorite-YES — paying 89¢ for NO means you're paying near-certainty money to bet something *won't* happen. Any upset is catastrophic. Same pathology as 89¢-YES favorites.
- **Tested:** 2026-04-23. segment n=1,848, ROI=**–88.79%**; baseline n=23,178, ROI=–3.73%; effect **–85.06pp**, p≈0. Biggest effect size in the library at time of test.
- **Status:** ✅ Backtested 2026-04-23 — **edge confirmed, complementary to favorite-YES in timing**
- **Backtest command:** `python fade_backtest.py --segment "m.category='Sports' AND t.taker_side='no' AND t.price_cents >= 70" --min-notional 0 --exec-source social`
- **Backtest result (headline):** Fading Sports underdog-NO takers → **+22.83% net** ROI on $1.51M fade notional after 2% fees (9,132 trades, 98.1% exec coverage). Takers lost **$1.62M on $2.39M notional** (–67.71% ROI).
- **Backtest result (time-to-close breakdown — inverts favorite-YES timing!):**
  | Time to close | n | Taker ROI | Fade ROI (post-fee) |
  |---|---|---|---|
  | **<1h before close** | 5,939 | **–80.61%** | **+49.47%** |
  | 1-24h before close | 3,191 | –37.30% | –25.99% |
  **Edge is in the final hour, NOT earlier.** Exactly the OPPOSITE of favorite-YES (which peaks 1-24h out). Likely mechanism: last-minute "it's over" NO-buyers on near-certain outcomes eat losses when the underdog actually wins — a specifically late-market pathology. Earlier in the market, these same bets settle efficiently.
- **Price-bucket split:** ≥70¢ is where the edge lives. 50-70¢ sub-slice (269 trades, tiny) fades *negative* — don't generalize below 70¢.
- **Fee sensitivity:** +24.83% raw → **+16.83% at 8% fees**. Also forgiving.
- **Actionable strategy:** **Fade Sports NO takers at ≥70¢, placed within 1 hour of market close.** Buy YES at next trade, hold to expiry.
- **Complementary to favorite-YES:** together the two strategies cover opposite ends of the timing spectrum (underdog-NO <1h; favorite-YES 1-24h). A combined live executor can act on both signals from the same firehose.
- **Scale (sample):** $979K fade notional in the "hot slice" (<1h-to-close Sports underdog-NO) over the ~2-week social-firehose window.

---

### 🐋 `trade_huge_notional_roi` — Big-ticket whale trades get crushed
- **Hypothesis:** Single taker trades with ≥$1,000 notional earn different ROI than smaller trades.
- **Rationale:** Whales on expensive bets across *any* side/category — classic "big is dumb" signal. Tests whether the fade edge generalizes beyond favorite-YES / underdog-NO.
- **Tested:** multiple runs, latest segment n=216, ROI=–45.2%; baseline ROI≈–1.4%; effect **–43.8pp**, p≈0.
- **Status:** ✅ Backtested 2026-04-23 — **robust cross-cutting fade edge confirmed**
- **Backtest command:** `python fade_backtest.py --segment "t.price_cents * t.count_fp >= 100000" --min-notional 0 --exec-source social`
- **Backtest result (headline):** Fading ≥$1K-notional takers → **+6.96% net** ROI on $4.86M fade notional after 2% fees (2,130 trades, 98.9% exec coverage). Takers lost **$1.9M on $6.53M notional** (–29.2% ROI).
- **Price-bucket breakdown:**
  | Price bucket | n | Taker ROI | Fade ROI (post-fee) |
  |---|---|---|---|
  | **>70¢ favorite** | 1,167 | –49.09% | **+17.24%** |
  | **50-70¢** | 588 | –24.77% | **+20.12%** |
  | 30-50¢ | 281 | +20.62% | –6.20% |
  | <30¢ longshot | 94 | +170.67% | –36.61% |
  Same "charm/favorite" asymmetry as favorite-YES: big whales on expensive bets lose; big whales on longshots win.
- **Time-to-close breakdown:** Unlike favorite-YES (1-24h only) and underdog-NO (<1h only), this edge is **time-independent** — 1-24h fade +6.82%, <1h +7.17%. The fade works regardless of when the whale prints.
- **Category split:** Sports dominates (2,077 of 2,130 trades, $4.78M notional, +6.79% post-fee). Exotics has 34 trades at +23.96% post-fee (tiny sample).
- **Fee sensitivity:** +8.96% raw → still **+0.96% at 8% fees**. Tight but survives.
- **Actionable strategy:** **Fade any ≥$1K whale trade in Sports priced at 50¢+.** Buy opposite side at next trade. No time-of-day filter needed. Complements the other two fade edges.
- **Relation to other edges:** Overlaps favorite-YES (many big whales are favorite-YES takers) but adds coverage in the 50-70¢ band and is time-agnostic — useful as a **simpler, always-on whale-watch rule** layered over the more surgical edges.

---

### 🌙 `trade_overnight_longshot_roi` — Overnight <30¢ longshots PRINT (do NOT fade)
- **Hypothesis:** Trades at <30¢ placed during 00:00-06:00 UTC (≈ 8pm-2am ET) earn different ROI than other trades.
- **Rationale:** Late-night longshot gambling hypothesis — expected takers to lose (lottery-ticket pathology). Instead, backtest inverts the expected direction.
- **Tested:** segment n=4,122, ROI=**+182.2%**; baseline ROI=–27.9%; effect **+210.1pp**, p≈0.
- **Status:** ⚗️ Partially backtested 2026-04-23 — **fade direction confirmed wrong; coat-tail quantification pending**
- **Backtest command:** `python fade_backtest.py --segment "(t.created_ts % 86400) < 21600 AND t.price_cents < 30" --min-notional 0 --exec-source social`
- **Backtest result (headline):** Takers earn **+266.5% ROI** on $774K notional ($2.06M profit). Fading loses **–34.3% post-fee** on $3.16M fade notional. Doesn't break down — both time buckets lose as a fade.
- **Why "partially":** `fade_backtest.py` only simulates the fade direction. A proper coat-tail backtest needs to buy *the same side* at next-trade price and account for entry-slippage (markets re-rate fast when a longshot starts winning). Edge could be much smaller than the raw +266% taker ROI suggests.
- **Actionable-next-step:** Build a coat-tail backtest variant, then re-evaluate. Raw signal is enormous; the question is what survives realistic entry slippage.
- **Flags:** Sample skew — Sports is 22,062 of 22,595 trades. Not really "overnight" as a time-of-day effect; it's "longshots that eventually win" with time-of-day along for the ride. The real hypothesis is `trade_longshot_yes_roi` (below).

---

### 🎰 `trade_longshot_yes_roi` — <30¢ YES longshots PRINT (do NOT fade)
- **Hypothesis:** YES taker trades at <30¢ earn different ROI than other trades.
- **Rationale:** Same question as overnight-longshot but without the time filter — does the longshot edge exist at all hours?
- **Tested:** n=5,520, ROI=**+156.8%**; baseline=–27.9%; effect **+184.7pp**, p≈0.
- **Status:** ⚗️ Partially backtested 2026-04-23 — **fade wrong, coat-tail math TBD**
- **Backtest command:** `python fade_backtest.py --segment "t.taker_side='yes' AND t.price_cents < 30" --min-notional 0 --exec-source social`
- **Backtest result (headline):** Takers earn **+160.4% ROI** on $809K notional ($1.30M profit). Fade loses **–23.2% post-fee**. Subsumes the "overnight" version: full-day longshots print just as hard as overnight longshots.
- **Time-to-close:** <1h fade –2.43% post-fee (near-breakeven → tiny potential fade window, but thin). 1-24h fade –41.5% (catastrophic — strongest coat-tail signal here).
- **Category split:** Sports 24,458 trades +167% taker ROI. Exotics 1,742 +58%. Climate 703 takers lose (–27.5%) — so climate longshots are the exception.
- **Next step:** Same as overnight — need a coat-tail backtest. The mechanical problem is steep: after a <30¢ YES prints a winning trade, the next trade's YES price often spikes 10-20¢, which wipes out most of the edge.
- **Subsumes:** `trade_overnight_longshot_roi`.

---

### 🕒 `trade_near_close_roi` — Final-hour trades lose (weak edge, subsumed)
- **Hypothesis:** Trades placed in the last hour before market close earn different ROI.
- **Rationale:** End-of-market "last-second" pathologies — whoever's left standing might be the suckers.
- **Tested:** n=11,289, ROI=–29.0%; baseline=–4.1%; effect **–25.0pp**, p≈0.
- **Status:** ✅ Backtested 2026-04-23 — **edge exists but weak, fully subsumed by favorite-YES / underdog-NO**
- **Backtest command:** `python fade_backtest.py --segment "m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) BETWEEN 0 AND 3600" --min-notional 0 --exec-source social`
- **Backtest result (headline):** Fade raw +2.73%, **+0.73% post-fee** on $6.0M notional (56,771 trades, 98.8% coverage). Flips negative at 3% fees.
- **Price-bucket breakdown (the real story, again):**
  | Price bucket | n | Taker ROI | Fade ROI (post-fee) |
  |---|---|---|---|
  | >70¢ favorite | 16,008 | –49.57% | +6.84% |
  | 50-70¢ | 9,434 | –23.88% | +9.91% |
  | 30-50¢ | 10,849 | +5.75% | +6.75% |
  | <30¢ longshot | 20,480 | +243.97% | –10.57% |
  Same split we've seen three times now. Final-hour by itself isn't the edge; **final-hour ≥50¢** is.
- **Actionable strategy:** Nothing new — the favorite-YES (<1h subset) and underdog-NO (<1h) rules already cover the profitable sub-slices. No reason to trade "anything in the last hour" as a standalone rule.
- **Verdict:** Weak standalone, but confirms the pattern: **fade ≥50¢ takers in the final hour of Sports markets**. Use the more targeted rules.

---

### 💨 `trade_tiny_notional_roi` — Sub-$10 bettors quietly win (not actionable)
- **Hypothesis:** Taker trades under $10 notional earn different ROI than larger trades.
- **Rationale:** Retail dust vs "real money" — tiny trades as a proxy for uninformed retail.
- **Tested:** n=12,617, ROI=+32.8%; baseline=–18.2%; effect **+51.0pp**, p≈0.
- **Status:** ✅ Backtested 2026-04-23 — **direction wrong; effect real but un-tradeable**
- **Backtest command:** `python fade_backtest.py --segment "t.price_cents * t.count_fp < 1000" --min-notional 0 --exec-source social`
- **Backtest result:** Takers earn **+128.2% ROI** on $246K total notional (64,269 trades). Fading loses **–9.8% post-fee**. Tiny traders win, big traders lose — an inversion of naive whale-following.
- **Why un-tradeable:** The $246K notional is spread over 64,269 trades = avg $3.83/trade. There's no capacity to deploy meaningful size behind tiny-notional takers. The signal is informative (confirms "big notional is dumb notional") but not a trade.
- **Subsumes into:** the inverse of `trade_huge_notional_roi`. Act on the whale-fade side.

---

### 💯 `trade_large_round_count_roi` — Round-100 quantities (DEAD)
- **Hypothesis:** Taker trades at round quantities ≥100 (100, 200, 300, …) earn different ROI.
- **Rationale:** Round-number bias → maybe these are retail "I'll bet 100 shares" trades → might be dumb money.
- **Tested:** n=834, ROI=–31.3%; baseline=–13.6%; effect **–17.7pp**, p≈0.
- **Status:** 📉 Backtested 2026-04-23 — **DEAD**
- **Backtest command:** `python fade_backtest.py --segment "t.count_fp >= 100 AND CAST(t.count_fp AS INTEGER) % 100 = 0" --min-notional 0 --exec-source social`
- **Backtest result:** Fade **loses –11.16% post-fee** across 8,731 trades. Takers are close to breakeven (–2.4% ROI). Fade loses at every price bucket and time-to-close slice. Fee sensitivity just makes it worse.
- **Why it failed:** The experiment's effect size (-17.7pp) looked big, but takers in the backtest cohort are close to the overall market's breakeven — the "segment underperforms baseline" signal in the daily test doesn't translate because the baseline isn't a tradable fade (most of its loss is fees and spread you can't capture). Lesson: segment-vs-complement effect ≠ absolute taker unprofitability.
- **Verdict:** Not a fade. Don't re-run.

---

### ⛅ `trade_climate_favorite_roi` — Climate ≥70¢ YES (not actionable)
- **Hypothesis:** Climate-category YES trades ≥70¢ earn different ROI than other trades.
- **Rationale:** Mirror of the sports-favorite-YES edge in a different category.
- **Tested:** n=111, ROI=+9.8%; baseline=–16.7%; effect **+26.5pp**, p=0.0002.
- **Status:** 📉 Backtested 2026-04-23 — **sample too thin; no tradable edge either direction**
- **Backtest command:** `python fade_backtest.py --segment "m.category='Climate and Weather' AND t.taker_side='yes' AND t.price_cents >= 70" --min-notional 0 --exec-source social`
- **Backtest result:** n=518 (tiny). Taker ROI **+5.3%** (thin positive). Fade catastrophically loses **–99.4% post-fee** (near-total loss — fades almost never hit because favorites resolve as favorites in climate markets).
- **Why not actionable:** Taker side is *slightly* positive but needs ~85¢ entry to win ~15¢ — per-trade capacity is tight, and the coat-tail slippage would eat most of the 5% edge. Sample also tiny ($18K total notional).
- **Verdict:** Skip. Sports favorites are a real edge at scale; climate's isn't.

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

### 🕊️ `series_kxhormuztrafficw_roi` — Hormuz Strait traffic (coat-tail anomaly)
- **Hypothesis:** Trades in KXHORMUZTRAFFICW (Strait of Hormuz weekly traffic) have different ROI than baseline.
- **Rationale:** Landscape survey showed takers **WIN +37.92%** on this series — unusual, since retail typically loses. Either information asymmetry, structural mispricing, or noise from small sample (257 trades).
- **Status:** 🔬 Flagged — needs realistic backtest of *coat-tail* direction (buy same side as taker at next-trade price)
- **Notes:** Coat-tail simulator doesn't exist yet. Build needed before this is testable. Geopolitics/oil-traffic markets may attract knowledgeable traders.

---

### 🗳️ `series_kxcabout_roi` — KXCABOUT politics (coat-tail anomaly)
- **Hypothesis:** Trades in KXCABOUT politics series have different ROI than baseline.
- **Rationale:** Same coat-tail signature as Hormuz — takers **WIN +32.35%**. Politics is a category where opinionated retail often has strong (sometimes correct) priors.
- **Status:** 🔬 Flagged — needs coat-tail backtest

---

_Add new experiments above this line as they surface._
