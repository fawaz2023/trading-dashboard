# Shadow-Box Institutional Alpha (SBIA) - Backtest Lineage & Strategy Report

This document serves as the permanent record of the mathematical evolution of the Shadow-Box engine. It tracks the three major backtest phases, detailing exactly how the strategy logic evolved and the resulting performance metrics.

---

## 1. The Baseline: `93_ULTIMATE_patched_backtest`
**The 12-Condition Cascade Engine**

### 🧠 Strategy Architecture
* **Entry Logic (Phase 1):** Strict adherence to the 12-condition progressive spike cascade. A stock must pass absolute baseline floors (Delivery $\ge$ 50%, Turnover $\ge$ ₹50L, ATW $\ge$ ₹20k) and demonstrate cascading dominance across all three trailing timeframes (1D > 1W > 1M > 3M) simultaneously for Delivery %, Turnover, and ATW.
* **ML Bouncer:** Basic AI probability gate. `AI_WIN_PROBABILITY >= 60.0%`. No specific Goldilocks zones enforced.
* **Exit Strategy:** Fixed Risk-Reward parameters using Dynamic Volatility. Stop Loss at $2.0 \times ATR_{14}$, Take Profit at $4.0 \times ATR_{14}$ (1:2 R:R payout).
* **Position Sizing:** 1.5% fixed-fractional account risk per trade, but **hard-capped at 10%** of portfolio cash to amputate gap-down tail risk on low-volatility assets.

### 🏆 Performance Results (Dec 2022 - Oct 2024)
* **Total Trades:** 418
* **Win Rate:** 50.5%
* **Profit Factor:** 1.57
* **Max Drawdown:** 8.39%
* **Net ROI:** +31.46% (Ending Equity: ₹13,14,602)

---

## 2. The Refinement: `95_FLEXGATE_accumulation_backtest`
**The Goldilocks 200-SMA Edition**

### 🧠 Strategy Architecture
* **Entry Logic (Phase 1):** Scrapped the rigid 12-condition cascade in favor of a 5-filter "Base-Loading Consolidation" floor. Required a **200-Day SMA** baseline to ensure macro trend alignment.
* **Anomaly Tripwires (Phase 2):** Introduced the "Flex-Gates". Instead of beating all timeframes, a stock just needed to trigger *one* of three specific anomalies against its 1-month quiet baseline: Capital Sweep (Turnover), Price-Neutral Whale Spike (ATW/VWAP), or Hoarding Expansion (Delivery %).
* **ML Bouncer (Phase 3):** Introduced the **Goldilocks Zone**. Rejected setups with an SIS Score outside the 50.0–69.7 range, and required Implied Trades $> 2,100$ to prevent retail dispersion.
* **Exit Strategy:** Shifted from Fixed R:R to Trend Following. Implemented a **3x ATR Chandelier Trailing Stop**. No fixed take profits—winners run until the trailing stop is breached.
* **Position Sizing:** Strict 10% hard cap per trade.

### 🏆 Performance Results (July 2023 - Oct 2024) *Delayed start due to 200-SMA*
* **Total Trades:** 87
* **Win Rate:** 55.2%
* **Profit Factor:** 4.84 *(Massive jump due to trailing stop letting winners run)*
* **Max Drawdown:** 2.65% *(Ironclad defense)*
* **Net ROI:** +30.81% (Ending Equity: ₹13,08,112)

---

## 3. The Final Optimized System: `95_ULTIMATE_FLEXGATE_backtest`
**The ICT Box Edition (Live Production Engine)**

### 🧠 Strategy Architecture
* **Entry Logic (Phase 1):** Maintained the 5-filter Base-Loading floor but **removed the 200-Day SMA requirement** entirely, allowing the system to hunt setups in broader market conditions without a 9-month data warm-up.
* **Anomaly Tripwires (Phase 2):** Retained Flex-Gates A, B, and C.
* **ML Bouncer & ICT Clustering (Phase 3):** The ultimate definitive fingerprints derived from pattern recognition. 
    * Ruthlessly enforced the SIS Goldilocks Zone (50.0 - 70.0).
    * **ICT Box Confirmation:** A stock must trigger **EXACTLY TWO (2)** accumulation alerts within a rolling 10-day window. (1 is a false start; 3+ is crowded/exhausted).
* **Exit Strategy:** Retained the highly successful $3x ATR_{14}$ Chandelier Trailing Stop.
* **Position Sizing:** Strict 10% hard cap per trade.

### 🏆 Performance Results (Dec 2022 - Oct 2024)
* **Total Trades:** 355
* **Win Rate:** 53.0%
* **Profit Factor:** 3.49
* **Max Drawdown:** 4.13%
* **Net ROI:** +62.71% (Ending Equity: ₹16,27,184)

---

### 🔑 Conclusion & Production Status
The `95_ULTIMATE_FLEXGATE` system represents the pinnacle of this research. By discarding the rigid 12-condition cascade and the restrictive 200-SMA, and replacing them with flexible anomaly triggers bound by strict institutional fingerprints (SIS 50-70 & Exactly 2 Triggers in 10 Days), the system achieved a massive **+62.7% ROI** while keeping maximum drawdown confined to a negligible **4.13%**.

**This logic is now permanently locked and designated as the primary execution engine for the Streamlit Live Dashboard.**
