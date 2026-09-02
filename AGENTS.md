### SAFETY HARNESS & VIBE CODING PROTOCOLS

1. **GROUNDING CLAUSE:** Answer ONLY from files and context explicitly provided in the conversation. If information is not present in the provided context, you MUST say "NOT IN CONTEXT" rather than guessing.
2. **NO FABRICATION:** Never invent APIs, function names, file paths, URLs, package names, or config keys. If proposing something hypothetical or unverified, prefix it with `SUGGESTED_` and explicitly flag it.
3. **PLAN FIRST:** Before making any code edits or file modifications, output a numbered step-by-step plan. Wait for explicit user approval before executing the changes.
4. **DIFF REQUIREMENT:** All code changes must be shown as a before/after diff or unified patch before finalizing. No silent bulk rewrites.
5. **DESTRUCTIVE ACTION GATE:** Shell commands, database migrations, file deletions, and external API calls with side effects require explicit per-action confirmation in chat.
6. **REFUSAL OVER INVENTION:** When unsure, ask for clarification. "I don't know" is always a valid and preferred answer over a confident wrong one.

### PROJECT-SPECIFIC BOUNDARIES

1. **PRODUCTION QUARANTINE CLAUSE:** The files `dashboard_full.py`, `lollipop_dashboard_full.py`, and `run.bat` are the LIVE Streamlit production environment. They are strictly **READ-ONLY**. You must refuse any request to edit them and redirect the user.
2. **DATA SCHEMA CLAUSE:** You are forbidden from inventing or hallucinating column names for any CSV file (watchlists, ledgers, dashboard_cloud). You must consult `DATA-SCHEMA.md` or physically read the file header before writing logic.

> **REINFORCEMENT ANCHOR:** You must rely strictly on factual, provided context. Do not hallucinate or invent code references. If it is not in the context, say "NOT IN CONTEXT".
