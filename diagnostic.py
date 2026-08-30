import pandas as pd
import sys
import os
import re
import glob
from datetime import datetime


def fail(msg):
    print(f"[FAIL] ERROR: {msg}")
    sys.exit(1)


def check_bse_delivery_format():
    """Gate: every data/bse_delivery_*.csv must have internal DATE == filename
    date (YYYYMMDD) and no zero-padded 16-digit DELIV_QTY values.
    Would have caught the 2025-11 -> 2026-07 raw-format corruption."""
    files = sorted(glob.glob("data/bse_delivery_*.csv"))
    if not files:
        fail("No data/bse_delivery_*.csv files found!")

    bad_files = 0
    for fp in files:
        fname_date = os.path.basename(fp).replace("bse_delivery_", "").replace(".csv", "")
        if not re.match(r"^\d{8}$", fname_date):
            print(f"[FAIL] BSE delivery format: {fp} filename is not YYYYMMDD")
            bad_files += 1
            continue
        try:
            fname_dt = datetime.strptime(fname_date, "%Y%m%d")
        except ValueError:
            print(f"[FAIL] BSE delivery format: {fp} filename date is not a real date")
            bad_files += 1
            continue

        bad_dates = set()
        padded_qty = 0
        with open(fp, "r", encoding="utf-8") as f:
            header = f.readline().strip()
            if header != "DATE,SYMBOL,DELIV_QTY,DELIV_PER":
                print(f"[FAIL] BSE delivery format: {fp} unexpected header '{header}'")
                bad_files += 1
                continue
            for line in f:
                parts = line.strip().split(",")
                if len(parts) != 4:
                    bad_dates.add("<malformed row>")
                    continue
                if parts[0] != fname_date:
                    bad_dates.add(parts[0])
                if len(parts[2]) == 16 and parts[2].isdigit():
                    padded_qty += 1

        if bad_dates or padded_qty:
            bad_files += 1
            print(f"[FAIL] BSE delivery format: {fp}")
            if bad_dates:
                print(f"        DATE values != filename date {fname_date}: {sorted(bad_dates)[:5]}")
            if padded_qty:
                print(f"        {padded_qty} zero-padded 16-digit DELIV_QTY rows")

    if bad_files:
        fail(f"{bad_files} BSE delivery files have a corrupt format. "
             f"Run repair_bse_delivery_format.py (originals are backed up).")
    print(f"[PASS] BSE delivery format: {len(files)} files internally consistent with filenames.")


def check_metrics_freshness():
    """Gate: metrics engines must have run against the latest combined data.
    - data/signal_scores_today.csv (written every run by calculate_active_signals.py,
      even on zero-signal days) must be dated to the latest combined trading day.
    - data/sbia_flexgate2_watchlist.csv (rewritten every run by flexgate_2_scanner.py)
      must be at least as new as the combined file.
    Would have caught the Aug-2026 silent ModuleNotFoundError engine death."""
    combined_path = "data/combined_dashboard_live.csv"
    probe_path = "data/signal_scores_today.csv"
    flexgate_path = "data/sbia_flexgate2_watchlist.csv"

    df = pd.read_csv(combined_path)
    combined_max = pd.to_datetime(df["DATE"], errors="coerce").max()
    if pd.isna(combined_max):
        fail("Metrics freshness: combined_dashboard_live.csv has no parseable DATE.")
    combined_mtime = os.path.getmtime(combined_path)

    # --- Path A probe: signal_scores_today.csv
    if not os.path.exists(probe_path):
        fail(f"Metrics freshness: {probe_path} missing — calculate_active_signals.py has never run.")
    try:
        probe = pd.read_csv(probe_path)
        probe_max = pd.to_datetime(probe["DATE"], errors="coerce").max() if ("DATE" in probe.columns and not probe.empty) else pd.NaT
    except Exception:
        probe_max = pd.NaT
    probe_mtime = os.path.getmtime(probe_path)

    if pd.notna(probe_max) and probe_max == combined_max:
        print(f"[PASS] Metrics freshness: signal_scores_today.csv dated {probe_max.strftime('%Y-%m-%d')} == combined max {combined_max.strftime('%Y-%m-%d')}.")
    elif probe_mtime >= combined_mtime:
        # Engine ran after the combined file was written but produced no signals
        # for the latest trading day (legitimate zero-signal day).
        print(f"[PASS] Metrics freshness: signal_scores_today.csv empty/stale-dated but freshly written "
              f"(zero-signal day); combined max {combined_max.strftime('%Y-%m-%d')}.")
    else:
        fail(f"Metrics freshness: signal_scores_today.csv is stale (max DATE "
             f"{probe_max.strftime('%Y-%m-%d') if pd.notna(probe_max) else 'n/a'}, "
             f"file older than combined_dashboard_live.csv; combined max "
             f"{combined_max.strftime('%Y-%m-%d')}) — calculate_active_signals.py likely crashed. "
             f"Check logs/metrics_errors.log.")

    # --- Path B probe: sbia_flexgate2_watchlist.csv (rewritten on every engine run)
    if not os.path.exists(flexgate_path):
        fail(f"Metrics freshness: {flexgate_path} missing — flexgate_2_scanner.py has never run.")
    if os.path.getmtime(flexgate_path) < combined_mtime:
        fail(f"Metrics freshness: {flexgate_path} is older than combined_dashboard_live.csv — "
             f"flexgate_2_scanner.py likely crashed. Check logs/metrics_errors.log.")
    print("[PASS] Metrics freshness: flexgate_2_scanner output is current.")


def run_diagnostics():
    print("======================================================================")
    print("PIPELINE DIAGNOSTICS & SANITY CHECK")
    print("======================================================================")

    file_path = "data/combined_dashboard_live.csv"
    if not os.path.exists(file_path):
        fail(f"ERROR: {file_path} not found!")

    df = pd.read_csv(file_path)
    print(f"Total rows in output: {len(df)}")

    # Check 1: Minimum total rows
    if len(df) < 3000:
        fail(f"Total rows ({len(df)}) abnormally low. Expected > 3000.")

    # Check 2: Minimum NSE and BSE rows
    exch_counts = df["EXCHANGE"].value_counts()
    nse_count = exch_counts.get("NSE", 0)
    bse_count = exch_counts.get("BSE", 0)

    print(f"NSE Stocks: {nse_count}")
    print(f"BSE Stocks: {bse_count}")

    if nse_count < 1500:
        fail(f"NSE row count ({nse_count}) abnormally low. Expected > 1500.")

    if bse_count < 1500:
        fail(f"BSE row count ({bse_count}) abnormally low. Expected > 1500.")

    # Check 3: Essential columns exist
    essential_cols = ["SYMBOL", "EXCHANGE", "CLOSE", "DELIV_PER", "ATW", "EVER_100_DELIV",
                      "DELIV_PER_1W", "DELIV_PER_1M", "ATW_1W", "ATW_1M"]
    missing = [c for c in essential_cols if c not in df.columns]

    if missing:
        fail(f"Missing essential columns: {missing}")

    # Check 4: Date check (should be recent)
    if "DATE" in df.columns:
        # Check if max date is not completely empty
        if df["DATE"].isna().all():
            fail("All DATE values are missing!")

    # Check 5: BSE delivery file format validator
    check_bse_delivery_format()

    # Check 6: Metrics engine freshness
    check_metrics_freshness()

    print("[PASS] Diagnostics passed! Data looks sane.")
    print("======================================================================\n")


if __name__ == "__main__":
    run_diagnostics()
