---
name: "Dashboard UI Skill"
description: "Loaded when working on the Dash app or UI components."
---

# DASHBOARD UI DIRECTIVES

## 1. Boundary Enforcement (CRITICAL)
- **Streamlit Files are READ-ONLY:** The files `dashboard_full.py`, `lollipop_dashboard_full.py`, and `run.bat` are production files. Any agent proposing changes to them must refuse and redirect the user to Dash.
- **Dash Workspace:** Development happens in `dash_app_v2.py`, `dash_pages/`, and `assets/`.

## 2. Design System & CSS
- The project has a dual CSS system: legacy variables in `assets/style.css` and Tailwind M3 tokens injected via CDN.
- Rely on the established `glass-panel` and `details.glass-panel` classes for layout cards.
- **Aesthetic:** Match the dark-mode, terminal-grade 2026 fintech style (e.g. text-primary accents, font-label-caps).

## 3. Performance Patterns
- Use `@dash.callback`, NOT `app.callback`.
- Data loading must be wrapped in `@lru_cache` keyed on the file's `mtime` to prevent N+1 reads.
- Always include try/except blocks around `pd.read_csv` and provide a friendly empty-state UI component if a file is missing.
