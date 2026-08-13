# 📜 Shadow-Box Institutional Alpha: The Complete Backtest Lineage

This document serves as the permanent historical record of the system's entire mathematical evolution. It spans dozens of iterations and tens of thousands of lines of code, tracking the engine from its primitive beginnings all the way to the final optimized AI institutional scanner.

---

## 🏛️ ERA 1: The Early Momentum Scanners (Scripts 08 - 24)
*The foundation of the system, focusing strictly on basic price and delivery momentum.*

* **`08_simple_strategy_backtest.py`**: The very first attempt at a quantitative backtest. Used basic moving averages and price crossovers. Highly susceptible to market noise.
* **`10_mentor_exact_8conditions.py`**: Integration of the strict "8-Condition Mentor Rules" targeting volume and delivery spikes.
* **`21_complete_strategy_with_atw.py`**: **Breakthrough.** The first script to introduce `Average Trade Worth (ATW)` as a metric to track ticket sizes, moving the system from retail momentum to institutional tracking.

---

## 🌊 ERA 2: The Cascade Architecture (Scripts 25 - 69)
*The introduction of progressive, multi-timeframe dominance.*

* **`25_complete_11_conditions_backtest.py`**: The birth of the cascade. A stock had to prove momentum across 1D > 1W > 1M > 3M for both Delivery % and Volume.
* **`57_full_backtest_mentor_strategy.py`**: First deep integration of the 11-condition cascade against multi-year historical data. Proved that the cascade worked, but win rates hovered around 45% due to late entries.
* **`61_final_realistic_backtest.py`**: Added real-world friction (slippage, taxes) and strict fixed-fractional position sizing.
* **`69_full_performance_backtest.py`**: The peak of the pure 11-condition logic without Machine Learning. Solid profitability, but suffered during major market corrections due to lack of dynamic risk management.

---

## 🔬 ERA 3: The Hybrid & Strict Realism Phase (Scripts 70 - 91)
*Optimizing execution and eradicating "fake" signals.*

* **`80_hybrid_daily_weekly_backtest.py`**: Blended daily momentum triggers with macro weekly structure to filter out low-timeframe noise.
* **`88_FINAL_complete_backtest_analytics.py`**: Introduced deep analytics on holding periods and drawdown timelines. Proved that cutting losers early was mathematically necessary for swing trading.
* **`91_STRICT_realistic_backtest.py`**: **The Reality Check.** Implemented brutal T+1 open execution rules, pessimistic slippage, and gap-down modeling. Forced the system to survive true market conditions.

---

## 🤖 ERA 4: The Machine Learning & AI Integration (Scripts 92 - 94)
*Replacing rigid condition floors with statistical probabilities and Cross-Sectional Ranking.*

* **`92_ML_COMPOUND_backtest.py`**: The first system to use `shadow_box_model.pkl`. Replaced binary checks with percentile scoring (`MOMENTUM_SCORE`, `FOOTPRINT_SCORE`, `STABILITY_SCORE`). 
* **`93_ULTIMATE_patched_backtest.py`**: The **12-Condition ML Bouncer**. 
    * **Rules:** Kept the cascading floors but gated entries behind an `AI_WIN_PROBABILITY >= 60.0%`.
    * **Risk:** $2.0 \times ATR$ Stop / $4.0 \times ATR$ TP. 10% Hard Cap position sizing.
    * **Result:** 418 Trades | 50.5% Win Rate | 1.57 Profit Factor | +31.46% ROI

---

## 👑 ERA 5: The Flex-Gate & Institutional Fingerprints (Scripts 95 - 96)
*The finalized, mathematically optimized production engine.*

* **`96_PATTERNS_institutional_fingerprint.py`**: **The Rosetta Stone.** Ran pattern recognition on the 93_Ultimate trades. Discovered that the true drivers of profitability were:
    1. **Goldilocks Zone:** SIS Score strictly between 50.0 and 70.0.
    2. **ICT Temporal Clustering:** Exactly 2 accumulation alerts within 10 days.
    3. **Whale Density Spikes:** Price-normalized ATW surges.

* **`95_FLEXGATE_accumulation_backtest.py`**: 
    * **Rules:** Introduced "Flex-Gates" (Trigger A, B, C) and required a 200-SMA baseline.
    * **Exit:** Replaced fixed R:R with the **3x ATR Chandelier Trailing Stop**.
    * **Result:** 87 Trades | 55.2% Win Rate | 4.84 Profit Factor | +30.81% ROI

* **`95_ULTIMATE_FLEXGATE_backtest.py`**: **The Final Form.**
    * **Rules:** Dropped the 200-SMA for max coverage. Enforced the absolute institutional fingerprints (SIS 50-70 + Exactly 2 Triggers in 10D).
    * **Result:** 355 Trades | 53.0% Win Rate | 3.49 Profit Factor | +62.71% ROI | 4.13% Max Drawdown.

---
**Status:** Legacy analysis is permanently locked. `95_ULTIMATE_FLEXGATE_backtest.py` logic is designated as the master engine.
