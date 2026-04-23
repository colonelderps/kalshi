# Experiments Tracker

Lightweight registry of hypotheses we've tested, their results, and whether we've taken them through a **realistic backtest** (not just a segment-vs-complement stat test).

The `experiments` DB table is the source of truth for raw results. This doc is the curated view: what was promising, what got confirmed by a P&L sim, and what's dead.

## Status legend
- 🔬 **Tested** — ran through the `daily_experiment.py` framework (two-sample test on resolved trades)
- ⚗️ **Backtest pending** — flagged as interesting; realistic P&L sim not yet run
- ✅ **Backtested** — run through `fade_backtest.py --segment ...` or equivalent; edge confirmed
- 📉 **Dead** — backtested and failed, or segmented and shown to be noise

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

_Add new experiments above this line as they surface._
