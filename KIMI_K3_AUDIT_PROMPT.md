# Kimi K3 — Cold Audit Prompt: SBIA Trading Dashboard (Dash)

**Role:** You are a senior fintech frontend auditor with expertise in 2026 design standards, terminal-grade trading UX, and Python/Dash production code. You will audit the SBIA Institutional Trading Dashboard — a Plotly Dash multi-page app running in parallel with a legacy Streamlit version.

**Output format:** Severity-ranked findings only. Use P0 (breaking), P1 (serious), P2 (minor). Each finding must include: severity, file:line reference, one-line description, concrete fix. No praise. No padding.

---

## 1. What You Are Auditing

A 7-page Plotly Dash app (`dash_app_v2.py`) for institutional Indian equity trading signals. It is a parallel reimplementation of a Streamlit dashboard (`dashboard_full.py`) — the Streamlit version is the **ground truth for data logic and business rules**. The Dash version must match it exactly.

### App Shell (`dash_app_v2.py`)
- Collapsible sidebar: 240px expanded → 80px collapsed
- Sticky top nav bar with ⌘K bar, "Ask AI ✨" button, notification/settings icons, user avatar
- Design system: `assets/style.css` (legacy glass-panel CSS) + `assets/tailwind_config.js` (Tailwind CDN + Material-3 tokens loaded at runtime)
- Fonts: Geist + JetBrains Mono (Google Fonts)
- Page routing via `dash.page_container`

### 7 Pages

| Path | File | Status |
|------|------|--------|
| `/` | `dash_pages/dashboard.py` | Live |
| `/signals` | `dash_pages/signals.py` | Live |
| `/institutional-signals` | `dash_pages/institutional_signals.py` | **OLD 249-line stub — audit this specifically** |
| `/watchlist` | `dash_pages/watchlist.py` | Live |
| `/win-rate` | `dash_pages/win_rate.py` | Live |
| `/data-health` | `dash_pages/data_health.py` | Live |
| `/settings` | `dash_pages/settings.py` | Live |

---

## 2. The Ground Truth: Velocity Simulation Arithmetic

The SBIA Alpha ledger simulation runs with `capital = ₹10,00,000`, `risk_pct = 0.003`. FlexGate uses `risk_pct = 0.002, ai_threshold = 65.0`. FlexGate 2.0 uses `risk_pct = 0.002, ai_threshold = 60.0`.

**Exact arithmetic (from `dashboard_full.py:14-86`):**
```python
capital = 1000000.0
risk_per_trade = capital * risk_pct

# Position sizing per trade:
if pd.isna(entry) or pd.isna(sl) or entry <= sl:
    invested = capital * 0.10          # NaN-SL fallback: flat 10%
    shares = invested / entry if entry > 0 else 0
else:
    sl_dist = entry - sl
    shares = risk_per_trade / sl_dist
    invested = shares * entry
    if invested > capital * 0.10:      # 10% capital cap
        invested = capital * 0.10
        shares = invested / entry

# PnL per trade:
if status != 'ACTIVE':
    exit_px = row.get('EXIT_PRICE', entry)
    if pd.isna(exit_px): exit_px = entry
    r_pnl = shares * (exit_px - entry)
    if status in ['HIT_TP', 'MOMENTUM_LOST'] and r_pnl > 0: wins += 1
    elif status == 'HIT_SL' or r_pnl < 0: losses += 1
else:
    # MTM via dashboard_cloud.csv; fallback to entry if file missing
    current_px = latest_prices.get(sym, entry)
    u_pnl = shares * (current_px - entry)

current_equity = capital + total_realized + total_unrealized
win_rate = wins / (wins + losses) * 100
```

Any Dash port of this function that deviates from the above is a **P0 parity bug**.

---

## 3. Data Sources (all CSVs in `/data/`)

| File | Used By |
|------|---------|
| `legacy_watchlist.csv` | Tab 1 Legacy Screener |
| `sbia_alpha_watchlist.csv` | Tab 2 SBIA Alpha Engine |
| `sbia_flexgate_watchlist.csv` | Tab 3 FlexGate Base-Loading |
| `sbia_flexgate2_watchlist.csv` | Tab 4 FlexGate 2.0 ML |
| `sbia_ledger.csv` | SBIA Alpha velocity sim + completed trades |
| `flexgate_ledger.csv` | FlexGate velocity sim |
| `flexgate2_ledger.csv` | FlexGate 2.0 velocity sim |
| `dashboard_cloud.csv` | Live MTM prices (SYMBOL, CLOSE columns) |

Ledger column schema: `ENTRY_DATE, SYMBOL, ENTRY_PRICE, ATR14, STOP_LOSS, TAKE_PROFIT, ENTRY_AI_PROB, ENTRY_WHALE_DENSITY, REC_POS_SIZE_INR, STATUS, EXIT_DATE, EXIT_PRICE`

---

## 4. Known Issues — Verify Each

These were identified in a prior design audit. Confirm they still exist and assign severity.

### Design System
- **Dual design system conflict:** `assets/style.css` uses a legacy CSS variable palette (`--primary`, `--surface`, etc.) while `assets/tailwind_config.js` injects Tailwind with Material-3 tokens. Classes from both systems appear on the same elements.
- **Invalid CSS variable:** `style.css:8` references `---border-active` (three dashes — invalid CSS, silently ignored by browsers).
- **Dead legacy classes:** `.sidebar`, `.header`, `.summary-card`, `.banner`, `.ag-theme-alpine` defined in style.css but referenced nowhere in current page files. Pure dead weight.

### Fake / Non-Functional UI
- **⌘K command bar:** Rendered in the top nav but has no callback. Pressing it does nothing.
- **"Ask AI ✨" button:** Top nav button — no callback, no route, purely decorative.
- **"Trade Now" button:** Appears on signal rows — no callback.
- **Notification + Settings icons:** In top nav — no callbacks.
- **Fake sparkline (`dashboard.py:90-99`):** A hardcoded Plotly line chart with static data `[1,3,2,5,4,6,5]`, presented as if it's live price data.

### `/institutional-signals` Page (Critical)
- **Still the OLD 249-line stub:** Reads only `data/active_signals_ranked.csv`. Has a fake filter bar (Sector/Market Cap/Exchange dropdowns — no callbacks), hardcoded "Showing 1-5 of 142 signals" pagination, decorative AI summary block. The full 4-engine port (4 `dcc.Tab` tabs with velocity sims) was planned but **never committed**.

### Accessibility
- **WCAG contrast failure:** `#64748b` text on `#1e293b` background = ~3.9:1 contrast ratio (WCAG AA requires 4.5:1 for normal text). Appears in secondary labels, timestamps, subtitle text throughout.

### Performance
- **Tailwind CDN in production:** `assets/tailwind_config.js` loads Tailwind CSS via CDN at runtime. This is explicitly flagged in Tailwind's own docs as not suitable for production (no tree-shaking, ~300KB overhead on every page load).

---

## 5. Seven Audit Sections — What to Deliver

### ① Design System & 2026 Aesthetic
- Does the dual CSS system (style.css + Tailwind M3) produce a coherent visual result or visual conflicts?
- Is the glassmorphism (`glass-panel`) consistent across all 7 pages?
- Rate the overall aesthetic vs 2026 fintech standards (Bloomberg Terminal, Robinhood Pro, Zerodha Kite).
- List any component that looks visually broken or generic.

### ② Layout Logic & Information Architecture
- Is the sidebar/nav structure logical for a trading terminal?
- Is the 7-page IA clear? What is confusing or redundant?
- Does the `/institutional-signals` stub break user trust (fake dropdowns, hardcoded counts)?

### ③ Dash Code Quality
- Callback hygiene: are there any `app.callback` vs `@dash.callback` conflicts? (All callbacks must use `@dash.callback`.)
- Are `lru_cache` loaders correctly keyed on file mtime? (Check `signals.py`, `win_rate.py` for the pattern.)
- Any layout functions that could raise uncaught exceptions (missing file, bad column)?
- Any N+1 data loads (reading same CSV multiple times per page load)?

### ④ Streamlit Parity Matrix
Produce a table: for each of the 4 engines, compare the Dash implementation against the Streamlit ground truth on:
- Column set (exact match?)
- Row highlighting logic (exact match?)
- Velocity simulation arithmetic (exact match to the code in Section 2 above?)
- Empty-state handling (friendly panel vs crash?)

### ⑤ Performance
- Count total CSV reads on a cold `/institutional-signals` page load.
- Identify any synchronous blocking operations in `layout()` functions.
- Flag the Tailwind CDN issue with estimated payload size.

### ⑥ Accessibility (WCAG AA)
- Verify the `#64748b` contrast issue and list all elements affected.
- Check all interactive elements (buttons, tabs, dropdowns) for keyboard navigation support.
- Any missing `aria-label` on icon-only buttons?

### ⑦ Data Correctness Spot-Checks
- Open `sbia_ledger.csv`. Pick 3 closed trades (STATUS != ACTIVE). Run the arithmetic from Section 2 manually. Does the Dash simulation produce the same `TOTAL_PNL` and `PNL_%`?
- Verify `dashboard_cloud.csv` has a `CLOSE` column and is deduplicated by `SYMBOL`. If not, the MTM prices will be wrong.
- Check `flexgate2_ledger.csv` — does it have a `TAKE_PROFIT` column? (It shouldn't — FlexGate 2.0 uses `CHANDELIER_EXIT` instead. Any Dash code referencing `TAKE_PROFIT` for this engine is a parity bug.)

---

## 6. What is Out of Scope
- The Streamlit files (`dashboard_full.py`, `lollipop_dashboard_full.py`, `run.bat`) — do not audit or suggest changes to these.
- The nightly pipeline (`auto_update_smart.py`, `auto_update_daily.bat`) — backend only.
- ML retraining or new engine logic.
- Implementing fixes — audit and rank findings only. A fix plan is a separate deliverable.

Whatever is done in Dash must not affect the main Streamlit app.
