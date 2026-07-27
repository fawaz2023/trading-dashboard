# Institutional Metrics Rebuild - Walkthrough

> **CRITICAL ARCHITECTURE NOTE:** This rebuild supersedes the older validator/UI patch documents from v3 and Nov 30. The live flow now reads the main pipeline output from `data/combineddashboardlive.csv` and uses the natively generated pipeline column names (`DELIVPER1M`, `DELIVERYTURNOVER1M`, `ATW1M`) as the scoring baseline.

The Institutional Metrics layer has been completely rebuilt from the ground up to solve the previous fragility issues and seamlessly integrate into your dashboard.

## What Was Done

### 1. Golden Row Decoupled Architecture
We abandoned the old design (which attempted to recalculate historical delivery averages from raw Bhavcopies per symbol). Instead, the new script `calculate_active_signals.py` relies on `data/combineddashboardlive.csv` as the source of truth, reading the cleanly calculated `DELIVPER1M`, `DELIVERYTURNOVER1M`, and `ATW1M` from the main pipeline. 
This "Golden Row" approach guarantees zero mismatch between what the screener sees and what the institutional scorer sees.

### 2. Binary Option C Scoring
We implemented the strict Binary confluence logic:
- **Momentum Score:** Passed if `DELIVPER > DELIVPER1M`
- **Footprint Score:** Passed if `DELIVERYTURNOVER > DELIVERYTURNOVER1M`
- **Stability Score:** Passed if `ATW > ATW1M`
Signals matching all 3 conditions get their boolean flags (`HASMOMENTUMDATA`, etc.) set to True.

### 3. Active Expiry Window
A critical bug in the old implementation was that signals were appended to history forever. The new implementation enforces a **10 Trading Day (14 Calendar Day)** active expiry window. Old signals automatically age out, keeping `active_signals_ranked.csv` clean and relevant.

### 4. Non-Blocking Integration
The metrics engine is now bolted onto the end of `auto_update_smart.py` via an isolated `subprocess.run()`.

> [!TIP]
> If the metrics engine ever encounters a runtime error, it will now simply log the exception to standard output and allow the pipeline to complete successfully, ensuring your core dashboard is never broken by an institutional scoring failure.

### 5. Surgical UI Patch (Enhanced)
The Streamlit dashboard (`dashboard_full.py`) received a localized patch. An `elif page == "Institutional Signals":` block was added. It directly loads the pre-computed `data/active_signals_ranked.csv` and offers:
- An interactive toggle for **"All Active (10 Days)" vs "Today Only"** (implemented dynamically without reading a second file).
- A **Min Score filter** mapped precisely to the 0-3 Binary Option C scale.
- A **Search Box** to filter by exact symbol.
- An **Exchange Filter** to isolate NSE or BSE setups.

> **UI REDESIGN NOTE:** The current Institutional Signals page no longer reads a separate `TODAY_FILE`; the "Today Only" view is derived dynamically from the latest date in the ranked active dataset, and Min Score operates on the 0–3 binary confluence scale rather than the older 60/70/80 score bands.

### 6. Automated UI Testing
Because relying solely on static `py_compile` checks is dangerous for Streamlit logic, a dedicated UI test script (`test_ui.py`) was introduced. It uses Streamlit's native `AppTest` framework to launch the app in-memory, load all 7 pages sequentially, inject inputs into the new Institutional filters (score, search, timeframe), and guarantee that no Pandas data-type errors or tracebacks occur at runtime.

## File Outputs
The rebuilt pipeline retains the Stage 1 validator from v3 and generates five operational files, plus one test artifact:
1. `institutional_config.json`: The schema mapping built by the validator (Stage 1).
2. `signal_scores_today.csv`: Today's scored outputs.
3. `signal_scores_history.csv`: The append-only historical store.
4. `data/active_signals_ranked.csv`: The deduplicated, 10-day active window ranked by score, powering the UI.
5. `signal_feature_store.csv`: The append-only ML dataset containing all raw features and scores.
6. *`test_ui.py`: (Test artifact) The programmatic UI test suite.*

## Validation Results
Testing the newly built active signals calculator across the latest output yielded **2 valid institutional signals** matching the active timeframe, which successfully rendered in the Streamlit UI.
