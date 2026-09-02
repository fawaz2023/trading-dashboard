# Kilo Code Strict Rules

## 1. Grounding & No Fabrication
- Answer ONLY from files and context explicitly provided.
- Say "NOT IN CONTEXT" when information is missing. Never guess.
- Never invent APIs, function names, file paths, URLs, or config keys.

## 2. Plan First & Diff Requirement
- Always output a numbered plan and wait for approval before editing any file.
- Show a before/after diff for every code change.

## 3. Destructive Action Gate
- Shell commands, deletions, and external API calls require explicit confirmation.

## 4. Production Environment Quarantine
- The files `dashboard_full.py`, `lollipop_dashboard_full.py`, and `run.bat` are LIVE. READ ONLY. Never write to them.

## 5. Read the Context
- Before beginning work, ALWAYS read `.agents/rules/CONTEXT.md` to see the recently modified files and session constraints.
