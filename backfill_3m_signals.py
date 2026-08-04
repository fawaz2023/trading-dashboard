"""
backfill_3m_signals.py
======================
Replays the live auto_update_smart.py pipeline over raw historical data
to extract every stock that passed the 12-condition ProgressiveSpiker filter
in the last 90 days, scored with the binary 0-3 institutional metric.

Usage:
    python backfill_3m_signals.py                 # Full 90-day backfill
    python backfill_3m_signals.py --validate-only  # Phase 1: validate one day only

Output:
    data/historical_signals_3months.csv
    data/historical_t2t_rejected_3months.csv

This script is READ-ONLY: it does not modify any existing pipeline files or CSVs.
"""

import os
import sys
import glob
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# === Copied exact helpers from auto_update_smart.py to avoid executing it ===
def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def safe_read_csv(path, **kw):
    try:
        return pd.read_csv(path, **kw)
    except Exception as e:
        print(f"Read error: {path} -> {e}")
        return pd.DataFrame()

def ensure_cols(df, cols_with_default):
    for c, v in cols_with_default.items():
        if c not in df.columns:
            df[c] = v
    return df

def normalize_bse_bhav(df):
    ren = {
        "BizDt": "DATE",
        "TckrSymb": "SYMBOL",
        "ClsPric": "CLOSE",
        "TtlTradgVol": "TOTTRDQTY",
        "TtlTrfVal": "TOTTRDVAL",
        "TtlNbOfTxsExctd": "NO_OF_TRADES",
    }
    df = df.rename(columns=ren)
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    for c in ["CLOSE","TOTTRDQTY","TOTTRDVAL","NO_OF_TRADES"]:
        if c in df.columns:
            df[c] = to_num(df[c]).fillna(0)
    df["EXCHANGE"] = "BSE"
    df = ensure_cols(df, {"ISIN": None, "FinInstrmId": None, "SYMBOL": None})
    return df

def normalize_nse_bhav(df, date):
    df = df.copy()
    df["DATE"] = date
    df["EXCHANGE"] = "NSE"
    if "TtlNbOfTxsExctd" in df.columns:
        df["NO_OF_TRADES"] = df["TtlNbOfTxsExctd"]
    for c in ["CLOSE","TOTTRDQTY","TOTTRDVAL","NO_OF_TRADES"]:
        if c in df.columns:
            df[c] = to_num(df[c]).fillna(0)
    df = ensure_cols(df, {"ISIN": None, "SYMBOL": None})
    return df

# bse_downloader_working.py helpers (L19, L43)
from bse_downloader_working import normalize_bse_delivery, merge_bse_bhav_delivery
# progressive_screener.py (the 12-condition filter)
from progressive_screener import ProgressiveSpiker
# calculate_active_signals.py (the binary 0-3 scorer)
from calculate_active_signals import score_signals
# config.py
from config import Config

pd.options.mode.chained_assignment = None


def log(msg):
    print(f"  🔢 {msg}")


def load_all_raw_data():
    """
    Steps 1-7 of auto_update_smart.py:
    Load, normalize, merge delivery, combine, dedup, compute metrics, filter.
    """
    nse_raw_dir = Config.NSE_RAW_DIR

    # ---------------------------------------------------------------
    # Step 1: Load NSE bhav (replicates auto_update_smart.py L254-267)
    # ---------------------------------------------------------------
    print("\n[Step 1] Loading NSE bhav files...")
    nse_bhav_files = sorted([f for f in os.listdir(nse_raw_dir) if f.startswith("nse_bhav_")])
    nse_frames = []
    for fn in nse_bhav_files:
        date_str = fn.replace("nse_bhav_", "").replace(".csv", "")
        d = datetime.strptime(date_str, "%Y%m%d")
        df = safe_read_csv(os.path.join(nse_raw_dir, fn))
        if df.empty:
            continue
        df = normalize_nse_bhav(df, d)
        nse_frames.append(df)

    df_nse = pd.concat(nse_frames, ignore_index=True) if nse_frames else pd.DataFrame()
    log(f"NSE bhav rows: {len(df_nse)}")

    # ---------------------------------------------------------------
    # Step 2: Load BSE bhav (replicates auto_update_smart.py L273-287)
    # ---------------------------------------------------------------
    print("\n[Step 2] Loading BSE bhav files...")
    bse_raw_dir = os.path.join(os.path.dirname(nse_raw_dir), "bse_raw")
    bse_bhav_files = sorted(glob.glob(os.path.join(bse_raw_dir, "bse_bhav_*.csv")))
    bse_frames = []
    for fp in bse_bhav_files:
        df = safe_read_csv(fp)
        if df.empty:
            continue
        df = normalize_bse_bhav(df)
        bse_frames.append(df)

    df_bse = pd.concat(bse_frames, ignore_index=True) if bse_frames else pd.DataFrame()
    log(f"BSE bhav rows: {len(df_bse)}")

    # ---------------------------------------------------------------
    # Step 3: Load NSE delivery (replicates auto_update_smart.py L292-309)
    # ---------------------------------------------------------------
    print("\n[Step 3] Loading NSE delivery files...")
    nse_delivery_files = sorted([f for f in os.listdir(nse_raw_dir) if f.startswith("nse_delivery_")])
    nse_del_frames = []
    for fn in nse_delivery_files:
        date_str = fn.replace("nse_delivery_", "").replace(".csv", "")
        d = datetime.strptime(date_str, "%Y%m%d")
        df = safe_read_csv(os.path.join(nse_raw_dir, fn))
        if df.empty:
            continue
        # Exact replication of L302-305
        if " SYMBOL" in df.columns:
            df = df.rename(columns={" SYMBOL": "SYMBOL"})
        df = normalize_bse_delivery(df)  # reuse standardizer (same as live L304)
        df["DATE"] = d
        nse_del_frames.append(df)

    df_nse_deliv = pd.concat(nse_del_frames, ignore_index=True) if nse_del_frames else pd.DataFrame()
    log(f"NSE delivery rows: {len(df_nse_deliv)}")

    # ---------------------------------------------------------------
    # Step 4: Load BSE delivery (replicates auto_update_smart.py L314-331)
    # ---------------------------------------------------------------
    print("\n[Step 4] Loading BSE delivery files...")
    bse_delivery_files = sorted(glob.glob("data/bse_delivery_*.csv"))
    bse_del_frames = []
    for fp in bse_delivery_files:
        date_str = os.path.basename(fp).replace("bse_delivery_", "").replace(".csv", "")
        d = datetime.strptime(date_str, "%Y%m%d")
        df = safe_read_csv(fp)
        if df.empty:
            continue
        df = normalize_bse_delivery(df)
        # v4 FIX: Do NOT overwrite DATE (same as live L326-327)
        if "DATE" not in df.columns or df["DATE"].isna().all():
            df["DATE"] = d
        bse_del_frames.append(df)

    df_bse_deliv = pd.concat(bse_del_frames, ignore_index=True) if bse_del_frames else pd.DataFrame()
    log(f"BSE delivery rows: {len(df_bse_deliv)}")

    # ---------------------------------------------------------------
    # Step 5: Merge delivery into bhav (replicates L336-349)
    # ---------------------------------------------------------------
    print("\n[Step 5] Merging delivery data...")
    if not df_nse.empty and not df_nse_deliv.empty:
        cols_keep = [c for c in ["SYMBOL", "DATE", "DELIV_PER", "DELIV_QTY"] if c in df_nse_deliv.columns]
        df_nse = df_nse.merge(df_nse_deliv[cols_keep], on=["SYMBOL", "DATE"], how="left")
    else:
        df_nse = ensure_cols(df_nse, {"DELIV_PER": 0, "DELIV_QTY": 0})

    df_bse = merge_bse_bhav_delivery(df_bse, df_bse_deliv)

    nse_with_deliv = len(df_nse[df_nse["DELIV_PER"] > 0]) if not df_nse.empty else 0
    bse_with_deliv = len(df_bse[df_bse["DELIV_PER"] > 0]) if not df_bse.empty else 0
    log(f"NSE with delivery: {nse_with_deliv}/{len(df_nse)}")
    log(f"BSE with delivery: {bse_with_deliv}/{len(df_bse)}")

    # ---------------------------------------------------------------
    # Step 6: Combine + Dedup (replicates L386-419)
    # ---------------------------------------------------------------
    print("\n[Step 6] Combining NSE + BSE and deduplicating...")
    df_all = pd.concat([df_nse, df_bse], ignore_index=True, sort=False)
    log(f"Combined before dedup: {len(df_all)} (NSE={len(df_nse)}, BSE={len(df_bse)})")

    df_all["EXCH_PRIORITY"] = df_all["EXCHANGE"].apply(lambda x: 0 if x == "NSE" else 1)

    has_isin = "ISIN" in df_all.columns and df_all["ISIN"].notna().sum() > 0
    if has_isin:
        df_all = df_all.sort_values(["ISIN", "DATE", "EXCH_PRIORITY"])
        before = len(df_all)
        df_all = df_all.drop_duplicates(subset=["ISIN", "DATE"], keep="first")
        log(f"Combined after ISIN dedup: {len(df_all)} (removed {before - len(df_all)})")
    else:
        df_all = df_all.sort_values(["SYMBOL", "DATE", "EXCH_PRIORITY"])
        before = len(df_all)
        df_all = df_all.drop_duplicates(subset=["SYMBOL", "DATE"], keep="first")
        log(f"Combined after SYMBOL dedup: {len(df_all)} (removed {before - len(df_all)})")

    df_all.drop(columns=["EXCH_PRIORITY"], inplace=True, errors="ignore")

    # ---------------------------------------------------------------
    # Step 7: Compute metrics + filter universe (replicates L424-489)
    # ---------------------------------------------------------------
    print("\n[Step 7] Computing metrics and filtering...")
    df_all = ensure_cols(df_all, {
        "CLOSE": 0, "TOTTRDQTY": 0, "TOTTRDVAL": 0,
        "NO_OF_TRADES": 0, "DELIV_QTY": 0, "DELIV_PER": 0
    })
    for c in ["CLOSE", "TOTTRDQTY", "TOTTRDVAL", "DELIV_QTY", "DELIV_PER"]:
        df_all[c] = to_num(df_all[c]).fillna(0)

    # Core metrics (L430-431)
    df_all["DELIVERY_TURNOVER"] = df_all["DELIV_QTY"] * df_all["CLOSE"]
    df_all["ATW"] = (df_all["TOTTRDVAL"] / df_all["NO_OF_TRADES"].replace(0, pd.NA)).fillna(0)

    # Filter SERIES: NSE EQ + all BSE (L434-441)
    if "SERIES" in df_all.columns:
        before = len(df_all)
        df_all = df_all[
            ((df_all["EXCHANGE"] == "NSE") & (df_all["SERIES"] == "EQ")) |
            (df_all["EXCHANGE"] == "BSE")
        ].copy()
        log(f"After SERIES filter: {len(df_all)} (removed {before - len(df_all)})")

    # Symbol exclusions (L444-454)
    if "SYMBOL" in df_all.columns:
        before = len(df_all)
        df_all = df_all[
            ~df_all["SYMBOL"].str.contains(
                "ETF|LIQUID|FUND|INDEX|NIFTY|SENSEX|GLOBE",
                case=False, na=False
            )
        ].copy()
        log(f"After ETF/FUND exclusion: {len(df_all)} (removed {before - len(df_all)})")

    # Bond exclusions (L468-489) — exact same regex patterns
    if "SYMBOL" in df_all.columns:
        before = len(df_all)
        bond_patterns = [
            r'^GS\d',
            r'^\d{3,4}GS\d',
            r'^\d{3,4}[A-Z]{2,4}\d{2,4}[A-Z]?$',
            r'^SGB',
            r'\d+TB$',
            r'SDL',
            r'MHSDL',
            r'^\d{2,}[A-Z]+\d{2,}[A-Z]*$',
            r'^[A-Z]+\d{4,}[A-Z]*$',
            r'ZC\d{2,}',
            r'PP$',
            r'^CS\d',
            r'^EELZ',
        ]
        pattern = '|'.join(bond_patterns)
        df_all = df_all[~df_all["SYMBOL"].str.contains(pattern, regex=True, na=False, case=False)].copy()
        log(f"After bond exclusion: {len(df_all)} (removed {before - len(df_all)})")

    # Bad ISIN exclusions (L457-466)
    bad_isins = [
        "INE148I07PY7", "INE1O3X15025", "INE296G07200", "INE296G07226",
        "INE306N08342", "INE443L08172", "INE501X07554", "INE501X08081",
        "INE549K08293", "INE612U07118", "INE733E07JR2", "INE787H07362",
        "INE836K07312", "INE939X07093",
    ]
    if "ISIN" in df_all.columns:
        before = len(df_all)
        df_all = df_all[~df_all["ISIN"].isin(bad_isins)].copy()
        log(f"After bad ISIN exclusion: {len(df_all)} (removed {before - len(df_all)})")

    log(f"Final filtered universe: {len(df_all)} rows")
    return df_all


def compute_progressive_averages(df_all):
    """
    Step 8: Vectorized progressive averages.
    Equivalent to auto_update_smart.py L504-543 but for ALL dates, not just the latest.
    """
    print("\n[Step 8] Computing vectorized progressive averages...")
    df_all["DATE"] = pd.to_datetime(df_all["DATE"], errors="coerce")
    df_all = df_all.sort_values(["SYMBOL", "DATE"]).copy()

    grouped = df_all.groupby("SYMBOL")

    # shift(1) = strictly PAST data, no forward leakage
    for base_col, suffixes in [
        ("DELIV_PER", ["_1W", "_1M", "_3M"]),
        ("DELIVERY_TURNOVER", ["_1W", "_1M", "_3M"]),
        ("ATW", ["_1W", "_1M", "_3M"]),
    ]:
        windows = [5, 22, 66]  # 1W=5 trading days, 1M=22, 3M=66
        for suffix, window in zip(suffixes, windows):
            col_name = f"{base_col}{suffix}"
            df_all[col_name] = grouped[base_col].transform(
                lambda x: x.shift(1).rolling(window, min_periods=1).mean()
            )

    # EVER_100_DELIV: expanding check — has this symbol EVER had >= 99.9% delivery?
    df_all["EVER_100_DELIV"] = grouped["DELIV_PER"].transform(
        lambda x: x.expanding().max()
    ) >= 99.9

    valid_rows = df_all["DELIV_PER_1M"].notna().sum()
    log(f"Rows with valid progressive data: {valid_rows}/{len(df_all)}")

    return df_all


def extract_signals(df_all, target_dates, validate_date=None):
    """
    Step 9: For each target date, apply ProgressiveSpiker + score_signals.
    """
    print(f"\n[Step 9] Extracting signals for {len(target_dates)} trading days...")
    all_clean = []
    all_t2t = []

    keep_cols = [
        "DATE", "SYMBOL", "EXCHANGE", "CLOSE", "DELIV_PER", "ATW",
        "DELIVERY_TURNOVER",
        "MOMENTUM_SCORE", "FOOTPRINT_SCORE", "STABILITY_SCORE", "COMBINED_SCORE",
        "HASMOMENTUMDATA", "HASFOOTPRINTDATA", "HASSTABILITYHISTORY20D",
    ]

    for dt in target_dates:
        day_df = df_all[df_all["DATE"] == dt].copy()
        if day_df.empty:
            continue

        # The real 12-condition filter
        signals = ProgressiveSpiker(day_df).get_signals()
        if signals.empty:
            continue

        # The real binary 0-3 institutional rank
        scored = score_signals(signals)
        if scored.empty:
            continue

        scored["DATE"] = pd.to_datetime(scored["DATE"], errors="coerce")

        # Separate clean vs T2T
        if "EVER_100_DELIV" in scored.columns:
            t2t_mask = scored["EVER_100_DELIV"] == True
            clean = scored[~t2t_mask].copy()
            t2t = scored[t2t_mask].copy()
        else:
            clean = scored.copy()
            t2t = pd.DataFrame()

        dt_str = pd.Timestamp(dt).strftime("%Y-%m-%d")
        clean_count = len(clean)
        t2t_count = len(t2t)

        if clean_count > 0 or t2t_count > 0:
            print(f"  {dt_str}: {clean_count} clean, {t2t_count} T2T rejected", end="")
            if clean_count > 0:
                syms = ", ".join(clean["SYMBOL"].tolist())
                print(f"  [{syms}]", end="")
            print()

        if not clean.empty:
            # Filter to keep_cols that exist
            available = [c for c in keep_cols if c in clean.columns]
            all_clean.append(clean[available])
        if not t2t.empty:
            available = [c for c in keep_cols if c in t2t.columns]
            all_t2t.append(t2t[available])

    return all_clean, all_t2t


def validate_against_live(all_clean, validate_date):
    """
    Phase 1 validation: compare backfill output for one day against known live output.
    """
    print("\n" + "=" * 70)
    print("PHASE 1 VALIDATION")
    print("=" * 70)

    # Load known live output
    live_path = "data/signal_scores_today.csv"
    if not os.path.exists(live_path):
        print("ERROR: Cannot validate — data/signal_scores_today.csv not found.")
        return False

    live = pd.read_csv(live_path)
    live["DATE"] = pd.to_datetime(live["DATE"], errors="coerce")
    live_date = live["DATE"].max()
    live_symbols = set(live["SYMBOL"].tolist())
    live_scores = dict(zip(live["SYMBOL"], live["COMBINED_SCORE"]))

    print(f"Live pipeline date: {live_date.strftime('%Y-%m-%d')}")
    print(f"Live survivors: {live_symbols}")
    print(f"Live scores: {live_scores}")

    # Get backfill output for the validation date
    if not all_clean:
        print("\nBACKFILL RESULT: 0 clean signals.")
        print("VALIDATION: FAILED — live has survivors but backfill found none.")
        return False

    backfill_df = pd.concat(all_clean, ignore_index=True)
    backfill_df["DATE"] = pd.to_datetime(backfill_df["DATE"], errors="coerce")
    bf_day = backfill_df[backfill_df["DATE"] == validate_date]

    bf_symbols = set(bf_day["SYMBOL"].tolist())
    bf_scores = dict(zip(bf_day["SYMBOL"], bf_day["COMBINED_SCORE"]))

    print(f"\nBackfill date: {validate_date.strftime('%Y-%m-%d')}")
    print(f"Backfill survivors: {bf_symbols}")
    print(f"Backfill scores: {bf_scores}")

    # Compare
    if bf_symbols == live_symbols and bf_scores == live_scores:
        print("\n✅ VALIDATION PASSED — backfill matches live output exactly.")
        return True
    else:
        missing = live_symbols - bf_symbols
        extra = bf_symbols - live_symbols
        score_mismatches = {s: (live_scores.get(s), bf_scores.get(s))
                           for s in live_symbols & bf_symbols
                           if live_scores.get(s) != bf_scores.get(s)}
        print(f"\n❌ VALIDATION FAILED")
        if missing:
            print(f"  Missing from backfill: {missing}")
        if extra:
            print(f"  Extra in backfill: {extra}")
        if score_mismatches:
            print(f"  Score mismatches (live vs backfill): {score_mismatches}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Backfill 3-month historical signals")
    parser.add_argument("--validate-only", action="store_true",
                        help="Phase 1: validate one day only, do not save files")
    parser.add_argument("--days", type=int, default=90,
                        help="Number of days to backfill (default: 90)")
    args = parser.parse_args()

    print("=" * 70)
    print("HISTORICAL SIGNAL BACKFILL")
    print("=" * 70)

    # Load and process all raw data (Steps 1-7)
    df_all = load_all_raw_data()

    # Compute progressive averages (Step 8)
    df_all = compute_progressive_averages(df_all)

    # Determine target dates
    max_date = df_all["DATE"].max()
    cutoff = max_date - pd.Timedelta(days=args.days)
    all_dates = sorted(df_all["DATE"].dropna().unique())
    target_dates = [d for d in all_dates if d >= cutoff]

    log(f"Max date in data: {max_date.strftime('%Y-%m-%d')}")
    log(f"Cutoff ({args.days} days): {cutoff.strftime('%Y-%m-%d')}")
    log(f"Target dates: {len(target_dates)}")

    if args.validate_only:
        # Phase 1: validate against live output for the latest date only
        target_dates = [max_date]
        print(f"\n[PHASE 1] Validating single day: {max_date.strftime('%Y-%m-%d')}")

    # Extract signals (Step 9)
    all_clean, all_t2t = extract_signals(df_all, target_dates, validate_date=max_date)

    if args.validate_only:
        # Run validation
        passed = validate_against_live(all_clean, max_date)
        if passed:
            print("\nPhase 1 complete. Run without --validate-only for the full 90-day backfill.")
        else:
            print("\nPhase 1 FAILED. Fix the replay logic before running the full backfill.")
        sys.exit(0 if passed else 1)

    # Step 10: Save output
    print("\n[Step 10] Saving output...")
    clean_path = "data/historical_signals_3months.csv"
    t2t_path = "data/historical_t2t_rejected_3months.csv"

    if all_clean:
        clean_df = pd.concat(all_clean, ignore_index=True)
        clean_df = clean_df.sort_values(["DATE", "COMBINED_SCORE", "SYMBOL"],
                                        ascending=[False, False, True])
        clean_df.to_csv(clean_path, index=False)
        log(f"Clean signals saved: {len(clean_df)} rows across {clean_df['DATE'].nunique()} days")
        log(f"  → {clean_path}")
    else:
        log("No clean signals found in the backfill window.")

    if all_t2t:
        t2t_df = pd.concat(all_t2t, ignore_index=True)
        t2t_df = t2t_df.sort_values(["DATE", "COMBINED_SCORE", "SYMBOL"],
                                    ascending=[False, False, True])
        t2t_df.to_csv(t2t_path, index=False)
        log(f"T2T rejects saved: {len(t2t_df)} rows across {t2t_df['DATE'].nunique()} days")
        log(f"  → {t2t_path}")
    else:
        log("No T2T rejects found in the backfill window.")

    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
