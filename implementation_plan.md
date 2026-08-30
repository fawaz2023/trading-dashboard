# Implementation Plan: SBIA Terminal — Full Remediation

**Date:** 2026-08-31  
**Based on:** GLM Audit Report v2 + Live Verification (Aug 30 session)  
**Priority order:** Fix the broken data engine first. Everything else is secondary.

---

## Live Verification Findings (Before Coding)

From our audit session right now:

| Check | Finding |
|---|---|
| venv joblib/sklearn/dash | ✅ **All present** — GLM's P0 venv claim is PARTIALLY wrong. Packages exist. |
| FlexGate 2.0 signals | ✅ Scanner is running. Signals detected but rejected by 60% AI threshold (market conditions). |
| BSE delivery corruption | ✅ **CONFIRMED** — exactly **167 files** from `20251117` → `20260724` have DDMMYYYY internal dates. |
| Junk files in git root | ✅ **CONFIRMED** — 25+ artifacts (`100k'`, `ATW_1M`, `bse['ATW_1W']`, `df`, `python`, etc.) |
| requirements.txt | ✅ Already correct — all packages listed including dash-iconify, joblib, sklearn. |

> [!IMPORTANT]
> The venv packages ARE installed (we just verified). The P0 issue may be that `auto_push_github.bat` is calling the wrong Python path. The BSE corruption (P1) is the most critical fix — 167 corrupt files affect signal quality.

---

## Phase 1 — Data Pipeline Correctness (CRITICAL)

### Task 1 — Verify nightly automation uses correct Python path

**Problem:** Even though venv has all packages, the `.bat` file scheduler may be calling `python` (system) instead of `venv\Scripts\python.exe`. This would silently bypass the venv.

**Files to check/fix:**
#### [MODIFY] [auto_update_daily.bat](file:///C:/Users/fawaz/Desktop/trading_dashboard/auto_update_daily.bat)
- Verify the first line activates the venv: `call venv\Scripts\activate`
- Verify subsequent Python calls use the activated venv, not a full system path
- Add explicit echo of which Python is being used: `python --version && where python`

#### [MODIFY] [auto_push_github.bat](file:///C:/Users/fawaz/Desktop/trading_dashboard/auto_push_github.bat)
- Same verification — confirm venv activation before any `python` calls
- Fix line 146: update messaging that still references "Streamlit cloud refresh"

**Verification:** Run `venv\Scripts\python -c "import joblib, sklearn, dash, dash_iconify"` — must succeed with no errors.

---

### Task 2 — Repair 167 Corrupted BSE Delivery Files (P1)

**Problem:** Files `bse_delivery_20251117.csv` → `bse_delivery_20260724.csv` (167 files) have dates stored in DDMMYYYY format internally while filenames are YYYYMMDD. This causes:
- `data_status.json` to show "Missing" for BSE delivery
- Days 01–09 of each month silently losing all BSE delivery data
- ML feature contamination for all historical training on this window

**Files to create:**
#### [NEW] `repair_bse_delivery_format.py`
- Backup each corrupt file to `backups/bse_delivery_badformat/`
- For each of the 167 confirmed corrupt files:
  - Read with `dtype=str`
  - Replace internal DATE column value with the correct YYYYMMDD from the filename
  - Strip zero-padding from `DELIV_QTY` (convert `0000000000001136` → `1136`)
  - Strip zero-padding from `DELIV_PER` (convert `043.71` → `43.71`)
  - Validate row count is preserved exactly (abort on mismatch)
  - Write back to same file with clean format
- Log every file processed and any errors to `logs/bse_repair.log`
- Print summary: files processed, rows verified, any failures

**Verification after running:**
- Re-run the spot-check: `python -c "..."` — all 167 should now show `date_val == filename_date`
- Run `auto_update_smart.py` — `data_status.json` should show real dates for BSE delivery

---

### Task 2.5 — Fix Ledger Ingest Bug (AI_APPROVED Bypass)

**Problem:** The user discovered that signals rejected by the ML model (e.g., `BANKBETA` with 55% probability) were showing up in the UI with a red `X` but were *still* entering the active trade ledger. This is because `flexgate_2_scanner.py` passes the entire unfiltered watchlist to `ledger_manager.py`, and `ledger_manager.py` never checks the `AI_APPROVED` flag before appending new rows to the ledger.

#### [MODIFY] [flexgate_2_scanner.py](file:///C:/Users/fawaz/Desktop/trading_dashboard/flexgate_2_scanner.py)
- At line 256, before calling `update_flexgate_ledger()`, filter the dataframe to only pass approved signals:
  `approved_only = flexgate_final[flexgate_final["AI_APPROVED"] == True]`
  `flexgate_active, flexgate_ledger_full = update_flexgate_ledger(approved_only, df, ...)`

#### [MODIFY] [calculate_active_signals.py](file:///C:/Users/fawaz/Desktop/trading_dashboard/calculate_active_signals.py)
- Do the same at line 224 for the legacy FlexGate ledger update.

---

### Task 3 — Harden Failure Visibility (P0 prevention)

**Problem:** When `calculate_active_signals.py` or `flexgate_2_scanner.py` crash inside the nightly script, the error is silently swallowed. You only notice weeks later when signals stop updating.

#### [MODIFY] [auto_update_smart.py](file:///C:/Users/fawaz/Desktop/trading_dashboard/auto_update_smart.py)
- **At lines 638–655** (where subprocesses are called): wrap each `subprocess.run()` in a check — if `returncode != 0`, print full stderr and append to `logs/metrics_errors.log`
- **In `get_max_date()`**: fix the date parsing to handle both YYYYMMDD and DDMMYYYY, and log any values that parse as neither — never silently return "Missing"
- **Add a freshness check**: after the signal generation step, verify `signal_scores_today.csv` max DATE matches `combined_dashboard_live.csv` max DATE. If not, log a loud warning.

---

### Task 4 — Add 2026 NSE Holidays (P4)

#### [MODIFY] [auto_update_smart.py](file:///C:/Users/fawaz/Desktop/trading_dashboard/auto_update_smart.py)
- At lines 22–26: replace the 2025-only holiday list with the official NSE 2025 + 2026 holiday list
- Source at implementation time from NSE official calendar — do not guess dates
- This prevents the backfill from treating 2026 holidays as missing trading days

---

## Phase 2 — Repo Hygiene (HIGH PRIORITY)

### Task 5 — Delete Junk Files from Git (P3)

**Problem:** 25+ PowerShell redirect accidents are tracked in git root. Confirmed list includes:

`0`, `100000`, `100k'`, `20`, `20'`, `50`, `50'`, `50)`, `5000000`, `ATW_1M`, `ATW_1W`, `ATW_3M`, `DELIVERY_TURNOVER_1M`, `DELIVERY_TURNOVER_1W`, `DELIVERY_TURNOVER_3M`, `DELIV_PER_1M`, `DELIV_PER_1W`, `DELIV_PER_3M`, `SC_CODE`, `bse['ATW_1M']`, `bse['ATW_1W']`, `bse['ATW_3M']`, `bse['DELIVERY_TURNOVER_1M']`, `bse['DELIVERY_TURNOVER_1W']`, `bse['DELIVERY_TURNOVER_3M']`, `bse['DELIV_PER_1M']`, `bse['DELIV_PER_1W']`, `bse['DELIV_PER_3M']`, `bse_pass['DELIV_PER_1M']`, `bse_pass['DELIV_PER_1W'])`, `df`, `python`, `schtasks`, `set`

**Also:** `venv/` directory is being tracked in git — the entire venv/Lib/site-packages tree is committed. This is bloating the repo massively.

#### [MODIFY] [.gitignore](file:///C:/Users/fawaz/Desktop/trading_dashboard/.gitignore)
Add:
```
venv/
*.log
flexgate_test_output.log
backups/
```

**Steps:**
1. `git rm --cached` all 25+ junk files
2. `git rm -r --cached venv/` (removes venv from tracking, keeps local files)
3. Add entries to `.gitignore`
4. Single commit: `"Remove junk shell artifacts + untrack venv from git"`

> [!WARNING]
> Removing venv from git tracking will make a large commit with many deleted files. This is correct behavior — venv should never be in git. The local venv stays intact on your machine.

---

## Phase 3 — Dash Cutover (MEDIUM PRIORITY)

### Task 6 — Fix Launcher and Retire Dead Shells

#### [MODIFY] [run.bat](file:///C:/Users/fawaz/Desktop/trading_dashboard/run.bat)
- Change from: `streamlit run dashboard_full.py`
- Change to: `call venv\Scripts\activate && python dash_app_v2.py`

#### [MODIFY] [dash_app_v2.py](file:///C:/Users/fawaz/Desktop/trading_dashboard/dash_app_v2.py)
- Line 207: Change `debug=True` to `debug=os.environ.get("DASH_DEBUG") == "1"`
- Debug is off by default; developers set `DASH_DEBUG=1` to enable it

#### [DELETE] `app.py` — unstyled shell, has `host='0.0.0.0'` + `debug=True` (network-exposed vulnerability)
#### [DELETE] `dash_app.py` — obsolete single-page prototype

---

### Task 7 — Fix Hardcoded Universe Count (P5)

#### [MODIFY] [dash_pages/dashboard.py](file:///C:/Users/fawaz/Desktop/trading_dashboard/dash_pages/dashboard.py)
- Line 10: Remove `total_scanned = 5518`
- Replace with: `total_scanned = len(pd.read_csv("data/combined_dashboard_live.csv"))` with a fallback to `5518` if file unreadable

---

### Task 8 — Port Remaining 4 Dash Pages

Four of the seven planned routes currently return 404. Port in this order:

| Priority | Page | Source in Streamlit | Route |
|---|---|---|---|
| 1 | Signals (12-Condition ProgressiveSpiker) | `dashboard_full.py:800–896` | `/signals` |
| 2 | Data Health | `data_status.json` + debug CSVs | `/data-health` |
| 3 | Watchlist | `dashboard_full.py:1250+` | `/watchlist` |
| 4 | Win Rate / Ledger | `dashboard_full.py:1544+` | `/win-rate` |

#### [NEW] `dash_pages/signals.py`
#### [NEW] `dash_pages/data_health.py`
#### [NEW] `dash_pages/watchlist.py`
#### [NEW] `dash_pages/win_rate.py`

---

### Task 9 — CSS Polish (LOW PRIORITY, do last)

#### [MODIFY] [assets/style.css](file:///C:/Users/fawaz/Desktop/trading_dashboard/assets/style.css)
- Consolidate 4 conflicting `.glass-panel` definitions (lines 143, 251, 259, 283) into one
- Drop the Inter `@import` — standardize on Geist + JetBrains Mono (already loaded via Tailwind config)

---

## Verification Checklist (End State)

- [ ] `venv\Scripts\python -c "import joblib, sklearn, dash, dash_iconify"` — no errors
- [ ] Nightly run regenerates `active_signals_ranked.csv` with only in-window signals
- [ ] All 167 BSE delivery files internally consistent with filenames
- [ ] `data_status.json` shows real dates for all 4 feeds — no "Missing"
- [ ] `git ls-files` root shows no junk files, no venv directory
- [ ] `python dash_app_v2.py` serves all 7 routes styled
- [ ] `run.bat` launches Dash, not Streamlit
- [ ] Next nightly auto-update completes cleanly and pushes to GitHub

---

## Execution Order

```
Phase 1 (Data) → Phase 2 (Cleanup) → Phase 3 (Dash)
Task 1 → Task 2 → Task 3 → Task 4 → Task 5 → Task 6 → Task 7 → Task 8 → Task 9
```

Phase 1 must be completed and verified before anything else. Until the BSE data is repaired and the nightly automation is confirmed healthy, all signal quality is suspect.
