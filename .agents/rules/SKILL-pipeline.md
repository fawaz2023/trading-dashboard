---
name: "Pipeline Domain Skill"
description: "Loaded when working on auto_update_smart.py or background scheduled tasks."
---

# PIPELINE DOMAIN DIRECTIVES

## 1. Safety and Outputs
- `auto_update_smart.py` is the core execution engine for daily data updates. Never break its output formats because the user monitors the console logs.
- Diagnostic logs (like `Checking Python path...`) should be temporary and removed once an issue is resolved to keep the console clean.

## 2. Smart Auto-Update Logic
- The pipeline uses a robust backfill mechanism that automatically catches missing dates (e.g. delayed BSE delivery files). Do not remove or bypass the backfill logic.
- BSE and NSE downloads are handled differently.

## 3. Batch Files
- `auto_update_daily.bat` and `auto_push_github.bat` are used by the Windows Task Scheduler. Ensure paths in these files are absolute or assume the `trading_dashboard` directory as the working directory.
