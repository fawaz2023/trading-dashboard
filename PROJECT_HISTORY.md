# Trading Dashboard - Project Chat History & Development Log

This document serves as a high-level summary of the development work, architectural decisions, and bug fixes completed during our AI pair-programming sessions.

## 📅 Session Summary: August 2026

### 1. BSE Data Pipeline Fixes
* **Issue:** The BSE Bhavcopy and Delivery data was inconsistently downloading, causing the `auto_update_smart.py` script to report `BSE=✗` and `Deliv=✗`.
* **Fix:** Upgraded `bse_downloader_working.py` with robust headers, session retries, and zip-file extraction logic for the new BSE India API.
* **Logging Fix:** Patched `auto_update_smart.py` to correctly check for the existence of `nse_delivery_` files rather than `bse_delivery_` files for the final summary status check, resolving the false-negative `Deliv=✗` logs.

### 2. Dash Architecture Migration ("Parallel Prototype")
* **Goal:** Migrate from Streamlit to a professional, highly customizable Plotly Dash application to bypass Streamlit's layout constraints.
* **Architecture:** Adopted Dash's `pages/` multi-page routing system.
* **Styling:** Migrated the "Bloomberg / SaaS" premium dark aesthetic using `dash-bootstrap-components` (DARKLY theme) and injected custom CSS via the `assets/style.css` directory.
* **Components:** Implemented `dash-ag-grid` with the `alpine-dark` theme for ultra-fast, professional financial data tables.
* **Bug Fix:** Fixed an encoding bug where PowerShell `echo` corrupted the `requirements.txt` file (UTF-16LE instead of UTF-8) causing Streamlit Cloud to crash.

### 3. Machine Learning & AI Score Deep Dive
* **AI Score Logic:** Clarified that the `AI_SCORE` is not a raw model output, but rather a weighted percentile-ranking engine comparing the current 30-day "survivor" pool.
    * 60% Weight: Stability Percentile (based on ATW).
    * 10% Weight: Momentum Percentile (based on Delivery %).
    * 10% Weight: Footprint Percentile (based on Delivery Turnover).
    * 20% Weight: Fresh Trigger Bonus (penalizes repeat triggers).
* **The "Green Row" Rule:** Clarified that in the Dash app, a row only highlights in neon-green if it is a fresh trigger (1st time in 30 days) AND its `STABILITY_RAW` exceeds the legendary **3.16** threshold.
* **Streamlit UI Clarification:** Explained that Streamlit's light green tint on rows simply indicates that a stock was triggered *today*, distinguishing it from the strict >3.16 "ML Edge" styling.
* **Notebook LM:** Generated highly detailed Masterclass essays (`ai_score_explanation_detailed.md`) to feed into Google Notebook LM for conceptual Q&A on the trading model.

---
*Note: This is an automatically generated summary of our AI chat history for this workspace.*
