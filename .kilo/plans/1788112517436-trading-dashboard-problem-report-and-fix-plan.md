# Trading Dashboard — Problem Report & Remediation Plan (v3, review-approved)

**Date:** 2026-08-30 · **Repo:** `C:\Users\fawaz\Desktop\trading_dashboard` · **Branch:** main (clean, `8ebec349`)
**v3 changes (user review feedback incorporated):** Added Task 6 — post-repair review of ACTIVE ledger positions (incl. manually backfilled NOVUS, verified at `flexgate2_ledger.csv:9`, BSE entry dated 2026-08-14 inside the corrupted window). Softened P1 Effect 3 (ML contamination severity depends on NSE/BSE training mix; model not invalidated — retrain stays deferred) and the CSS-debt risk note (cosmetic; current setup works in practice).
**v2 changes:** Added P0 (dead nightly metrics engines), consolidated Dash migration facts and cutover tasks. Resolved decisions: canonical Dash shell = `dash_app_v2.py` (user-confirmed); ML retrain deferred; keep tracking daily CSVs in git.

---

## Part A — Verified Problem Report

### P0 — CRITICAL: Nightly metrics engines silently dead since ~Aug 15
- **Environment split-brain (verified via site-packages):** the venv (activated by `auto_push_github.bat` → nightly Task Scheduler job) has streamlit/pandas/yfinance but **lacks** `joblib`, `scikit-learn`, and the entire Dash stack. Those live only in system Python 3.12.
- `calculate_active_signals.py:7` imports `joblib`; `flexgate_2_scanner.py` needs `joblib`+`sklearn`. Nightly, `auto_update_smart.py:638-655` invokes them via the venv `python` → `ModuleNotFoundError` → swallowed as "non-critical" (stderr truncated to 200 chars).
- **Proof in data:** `data/active_signals_ranked.csv` frozen at 2026-08-14 (contains Jul-15/Aug-6 rows that should have aged out of the 10-day window weeks ago); `signal_scores_today.csv` has no Aug-25–28 rows. This file feeds the Dash **Dashboard** and **Institutional Signals** pages → live product shows stale signals.
- `sbia_ledger.csv`/`flexgate2_ledger.csv` have Aug-25–28 entries only because of manual system-python runs (matches untracked `flexgate_test_output.log`). Nightly automation does not update them.

### P1 — CRITICAL: ~170 corrupted BSE delivery files (2025-11-20 → 2026-07-24)
- Files contain raw-format rows: `25112025,500002,0000000000001136,043.71` — DATE in **DDMMYYYY**, DELIV_QTY zero-padded to 16 chars, DELIV_PER zero-padded. Window verified by scan: `bse_delivery_20251120.csv` → ~`bse_delivery_20260724.csv`. Files before (through 2025-11-19) and after (2026-07-27+) are clean YYYYMMDD. Historical downloader bug, not live.
- **Effect 1:** `data_status.json` reports `bse_deliv_date: "Missing"` — `get_max_date` (`auto_update_smart.py:350-361`) takes `max()` over mixed ints; DDMMYYYY values like `31122025` fail `%Y%m%d` → NaT → `Missing`. Displayed on Data Health page.
- **Effect 2:** Days 01–09 of each month in the window silently lose all BSE delivery data — `01012026` becomes int `1012026`, fits neither `%d%m%Y` nor `%Y%m%d` in `merge_bse_bhav_delivery` (`bse_downloader_working.py:61-69`) → ~45+ trading days of BSE `DELIV_PER=0`. Corroborated by `debug_bse_row_counts.csv`: zero-delivery rows dropped 402,893→78,542 after the Jul-26 partial fix.
- **Effect 3 (nuanced per review):** ML feature contamination — `build_historical_features.py:150` parses the corrupted DATE column with `pd.to_datetime` and no format → garbage dates for BSE delivery in that window. **Severity depends on how much RF training leaned on BSE stocks from Nov 2025–Jul 2026**; if training was primarily NSE, impact is limited. Worth fixing at the data layer (Task 2); does not by itself invalidate the model. Retrain remains deferred (Phase 4). (`calculate_real_progressives.py` is immune — derives DATE from filename.)

### P2 — HIGH: requirements.txt / venv mismatch
- Lists Dash packages never installed into the venv; omits `joblib`, `scikit-learn`, `yfinance`, `dash-iconify` (imported by `dash_pages/verify_conditions.py:8`), `plotly`, `numpy`. Fresh `pip install -r requirements.txt` + activate-venv workflow cannot run the pipeline or the Dash app.

### P3 — MEDIUM: ~25 junk files tracked in git root
PowerShell quoting accidents committed & re-pushed daily: `0`, `100000`, `100k'`, `20'`, `50'`, `SC_CODE`, `df`, `df.columns`, `python`, `schtasks`, `set`, `ATW_1M`, `DELIV_PER_*`, `DELIVERY_TURNOVER_*`, `bse_pass['...']`, `bse['...']` variants (confirmed via `git ls-files`). Plus untracked `flexgate_test_output.log`.

### P4 — MEDIUM: Holiday calendar is 2025-only
`auto_update_smart.py:22-26`. In 2026, backfill treats holidays as missing trading days → noisy retry failures (no corruption).

### P5 — LOW: Hardcoded universe size
`dash_pages/dashboard.py:10` — `total_scanned = 5518` magic number, silently stale.

### P6 — Dash migration state (facts; canonical shell = `dash_app_v2.py`)
- **Three shells exist.** Only `dash_app_v2.py` loads the Tailwind CDN runtime (`external_scripts`, line 11-17) that all `dash_pages/` utility classes depend on. `app.py` renders pages **unstyled** (no Tailwind); `dash_app.py` is an obsolete single-page prototype with inline CSS. User launches via `python dash_app_v2.py` (system Python).
- **Ported:** 3 of 7 pages — Dashboard (`/`), Institutional Signals (`/institutional-signals`), Verify Conditions (`/verify-conditions`). **Not ported:** Signals (12-condition ProgressiveSpiker, `dashboard_full.py:800`), Watchlist (`:1250`), Win Rate (`:1544`), Data Health (`:701`). The `dash_app_v2.py` nav (lines 157-165) already links all 7 routes → 4 routes currently 404.
- **CSS debt (cosmetic per review, low risk):** `assets/style.css` has 4 conflicting `.glass-panel` definitions (lines 143, 251, 259, 283); two font systems (Inter via style.css `@import` vs Geist via tailwind config + external stylesheets). `assets/tailwind_config.js` is auto-included by Dash's assets loader — load order vs the CDN script is theoretically unguaranteed but **works in practice today**; treat as polish, not defect.
- **Launcher:** `run.bat` still launches legacy Streamlit (`dashboard_full.py`). `dash_app_v2.py:207` runs `debug=True` port 8050 (localhost-bound; `app.py`'s `debug=True` + `host='0.0.0.0'` is the exposed one — retired when app.py is deleted). `auto_push_github.bat:146` messaging still references the Streamlit cloud refresh.

### P7 — INFO
- `Institutional-Metrics-Status.md` (open editor tab) no longer exists. `pages/` dir is empty (stale tab).
- `data/bse_delivery_temp/` (73 raw files through 2025-11-19) is redundant after repair but serves as raw backup.
- `auto_push_full.log` is stale (July 28) — scheduled task evidently logs elsewhere or not at all; not investigated further (out of scope).

---

## Part B — Remediation Plan

### Phase 1 — Data pipeline correctness (production)

**Task 1 — Fix venv + requirements.txt (P0, P2)**
1. `venv\Scripts\pip install joblib scikit-learn dash dash-bootstrap-components dash-ag-grid dash-iconify plotly numpy` (yfinance/pandas already present).
2. Rewrite `requirements.txt` to match reality: keep existing pins, add `dash-iconify`, `plotly`, `numpy`, `yfinance`, `scikit-learn`, `joblib`.
3. Validate: `venv\Scripts\python -c "import joblib, sklearn, dash, dash_iconify, dash_ag_grid, dash_bootstrap_components, plotly"`.

**Task 2 — Repair corrupted BSE delivery files (P1 root)**
Create `repair_bse_delivery_format.py`, run with venv python:
1. Backup each file to `backups/bse_delivery_badformat/`.
2. For each `data/bse_delivery_YYYYMMDD.csv` read `dtype=str`. Bad-file test: DATE values that parse as `%d%m%Y` equal to the filename date, OR DELIV_QTY matches `^\d{16}$`.
3. Rewrite canonical: DATE = filename `YYYYMMDD` (cross-check parsed DDMMYYYY == filename date; abort + log on mismatch), `DELIV_QTY=int`, `DELIV_PER=float`, header `DATE,SYMBOL,DELIV_QTY,DELIV_PER`. Row count must be preserved exactly.

**Task 3 — Harden status + metrics-failure visibility (P0, P1 effect 1)**
1. `auto_update_smart.py` `get_max_date`: per-value parse `%Y%m%d` then `%d%m%Y` fallback; print file+value loudly on values matching neither (never silently `Missing`).
2. When `calculate_active_signals.py` / `flexgate_2_scanner.py` subprocess returns non-zero: print full stderr and append to `logs/metrics_errors.log`.

**Task 4 — diagnostic.py gates (P0/P1 prevention)**
Add checks that hard-fail (blocking the nightly push + triggering the email alert):
- **Format validator:** every `data/bse_delivery_*.csv` internal DATE == filename date; no 16-digit padded quantities.
- **Metrics freshness:** `signal_scores_today.csv` max DATE == `combined_dashboard_live.csv` max DATE (metrics engines crashed ⇒ gate fails; legitimate zero-signal days still write today's file ⇒ pass).

**Task 5 — Rebuild + verify**
1. Run `python auto_update_smart.py` (venv, after Tasks 1-2) → recomputes combined data and 1W/1M/3M baselines over corrected BSE history; regenerates `active_signals_ranked.csv` with a true 10-day window; resumes nightly ledger updates.
2. Verify: `data_status.json` shows real dates for all 4 feeds (e.g. `28 Aug 2026`); `diagnostic.py` PASS incl. new gates; `debug_bse_row_counts.csv` new row shows `zero_delivery` below the ~78.5k baseline; grep confirms no DDMMYYYY/padded rows remain in `data/bse_delivery_*.csv`.
3. Expected side effect: BSE baselines shift for the repaired window → some signals/ledger positions may legitimately change.

**Task 6 — Review ACTIVE ledger positions against repaired baselines (user-flagged gap)**
Post-rebuild (after Task 5), active positions may rest on corrupted BSE baselines:
1. Enumerate ACTIVE rows in `data/flexgate2_ledger.csv` and `data/sbia_ledger.csv`. Priority: BSE entries and any entry dated 2025-11-20→2026-07-24. Known cases: NOVUS (`flexgate2_ledger.csv:9`, BSE, entered 2026-08-14, manually backfilled), CARBORUNIV, REMAGNET, KALIND, and the Aug-25–28 manual-run entries in `sbia_ledger.csv`.
2. For each, re-derive the entry-day baselines (DELIV_PER/DELIVERY_TURNOVER/ATW vs 1W/1M/3M) from the repaired data and compare to the ledger's recorded trigger metrics.
3. Where a position's signal no longer holds under corrected baselines (would not have triggered, or SL/TP math changed materially), surface it in a review report (`data/ledger_review_post_repair.csv`): symbol, entry date, old vs new baseline, old vs new SL/TP, verdict KEEP / REVIEW / CLOSE.
4. **Do not auto-close positions** — present the report to the user for manual trading decisions. Ledger edits only on explicit user instruction.

**Task 7 — 2026 holidays (P4)**
Replace the 2025-only list in `auto_update_smart.py:22-26` with the official NSE 2026 holiday list (source at implementation time — do not guess) + keep 2025 for lookback.

### Phase 2 — Repo hygiene (P3)

**Task 8 — Junk cleanup**
1. `git rm` the ~25 artifacts enumerated in P3 (exact list via `git ls-files` filter at implementation time); delete untracked `flexgate_test_output.log`.
2. Append to `.gitignore`: `*.log`, `scratch/`.
3. Single commit "Remove accidental shell-redirect artifacts" — **ask user confirmation before committing/pushing**.

### Phase 3 — Dash cutover (P6; canonical = `dash_app_v2.py`)

**Task 9 — Retire dead shells**
Delete `app.py` (unstyled shell, network-exposed debugger) and `dash_app.py` (obsolete prototype). Git history preserves them. Keep `dashboard_full.py`/`lollipop_dashboard_full.py` (legacy production) until page parity is reached.

**Task 10 — Launcher cutover**
1. `run.bat` → activate venv + `python dash_app_v2.py` (works after Task 1 installs the Dash stack into venv).
2. `dash_app_v2.py`: `app.run(debug=os.environ.get("DASH_DEBUG") == "1", port=8050)` — debug off by default; dev sessions set `DASH_DEBUG=1`.

**Task 11 — Dynamic universe count (P5)**
`dash_pages/dashboard.py:10`: derive `total_scanned` from `data/combined_dashboard_live.csv` row count (cached read); fallback to last known constant on read error.

**Task 12 — Port remaining 4 pages** (order: core first)
1. **Signals** (`/signals`): 12-condition ProgressiveSpiker + momentum score, port logic from `dashboard_full.py:800-896` (`progressive_screener.ProgressiveSpiker` already imported by `verify_conditions.py`).
2. **Data Health** (`/data-health`): feed dates from `data_status.json`, row counts from `combined_dashboard_live.csv`, delivery merge stats from `debug_bse_row_counts.csv`.
3. **Watchlist** (`/watchlist`): from `dashboard_full.py:1250+` + `watchlist/` CSVs.
4. **Win Rate** (`/win-rate`): from `dashboard_full.py:1544+` + ledger CSVs.

**Task 13 — CSS polish (low risk, do last)**
Consolidate the 4 `.glass-panel` rules into one; unify fonts on Geist + JetBrains Mono (drop the Inter `@import` in `style.css`); optionally inline `tailwind.config` into the shell before the CDN script to guarantee ordering. Cosmetic — current rendering works.

**Task 14 — Post-parity cleanup**
After all 7 pages validated in Dash: archive Streamlit entry points (move `dashboard_full.py`, `lollipop_dashboard_full.py`, `run.bat` note) and update `auto_push_github.bat:146` messaging.

### Phase 4 — Deferred / flagged (explicitly out of scope for this plan)
- **ML retrain:** after P1 repair, FlexGate/ML training & backtests over Nov 2025–Jul 2026 should be re-run on corrected BSE history — separate follow-up once Phase 1 is validated.
- **Repo slimming:** keep tracking daily delivery CSVs in git (current nightly backup behavior unchanged).

---

## Validation checklist (end state)
- [ ] `venv\Scripts\python -c "import joblib, sklearn, dash, dash_iconify"` succeeds.
- [ ] Nightly run regenerates `active_signals_ranked.csv` with only in-window signals; `signal_scores_today.csv` dated to latest trading day.
- [ ] All `data/bse_delivery_*.csv` internally consistent with filenames; row counts unchanged from backups.
- [ ] `data_status.json` shows real dates for all 4 feeds; no `Missing`.
- [ ] `diagnostic.py` passes incl. format-validator + metrics-freshness gates (and would have caught both the Nov-2025 corruption and the Aug-2026 engine death).
- [ ] `debug_bse_row_counts.csv` shows reduced zero-delivery count.
- [ ] `data/ledger_review_post_repair.csv` produced; user has reviewed ACTIVE positions (esp. NOVUS, CARBORUNIV, REMAGNET, KALIND) against repaired baselines.
- [ ] `python dash_app_v2.py` from venv serves all 7 routes styled; `run.bat` launches it.
- [ ] `git ls-files` root contains only intended source/config files.
- [ ] Next nightly auto-update completes cleanly and pushes.
