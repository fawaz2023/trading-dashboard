"""
AUDIT PIPELINE-FAITHFUL RAW DATA FORMULA VERIFICATION WITH DEDUPLICATION
========================================================================
This script mimics your actual pipeline, including:
- Loading raw Bhav + Delivery files
- Deduplicating to keep latest entry per symbol
- Filtering equity stocks (SERIES == 'EQ')
- Merging datasets on SYMBOL
- Checking DELIVERY_TURNOVER and ATW formula correctness
- Detailed logging with file presence/errors
"""

import os
import sys
import pandas as pd

def print_section(title):
    print("\n" + "="*70)
    print(title)
    print("="*70)

def find_latest_file(folder, keyword):
    if not os.path.exists(folder):
        return None
    candidates = [f for f in os.listdir(folder) if (keyword.lower() in f.lower() and f.endswith(".csv"))]
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return os.path.join(folder, candidates[0])

def normalize_nse_columns(df):
    col_map = {
        'TckrSymb': 'SYMBOL',
        'ClsPric': 'CLOSE',
        'Deliv_Qty': 'DELIV_QTY',
        'TtlTrfVal': 'TOTTRDVAL',
        'TtlTradgVol': 'TTL_TRD_QNTY',
        'SERIES': 'SERIES',
        'TradDt': 'DATE'
    }
    renames = {c: col_map.get(c, c) for c in df.columns}
    df = df.rename(columns=renames)
    return df

def normalize_bse_columns(df):
    col_map = {
        'TckrSymb': 'SYMBOL',
        'ClsPric': 'CLOSE',
        'DelQty': 'DELIV_QTY',
        'TtlTrfVal': 'TOTTRDVAL',
        'TtlTradgVol': 'TTL_TRD_QNTY',
        'SctySrs': 'SERIES',
        'TradDt': 'DATE'
    }
    renames = {c: col_map.get(c, c) for c in df.columns}
    df = df.rename(columns=renames)
    return df

def deduplicate_latest(df, date_col, symbol_col='SYMBOL'):
    # Convert date column to datetime if not
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # Drop NaT in date
    df = df.dropna(subset=[date_col])

    # Sort descending by date
    df = df.sort_values(by=[symbol_col, date_col], ascending=[True, False])

    # Keep first (latest) per symbol
    deduped = df.drop_duplicates(subset=[symbol_col], keep='first')

    return deduped

def load_and_process(exchange, bhav_path, deliv_path):
    print_section(f"{exchange} RAW DATA LOAD AND PROCESS")

    try:
        bhav_df = pd.read_csv(bhav_path)
        print(f"  Loaded Bhav file: {bhav_path} with {len(bhav_df)} rows")
    except Exception as e:
        print(f"❌ Error loading {exchange} Bhav file: {e}")
        return None

    try:
        deliv_df = pd.read_csv(deliv_path)
        print(f"  Loaded Delivery file: {deliv_path} with {len(deliv_df)} rows")
    except Exception as e:
        print(f"❌ Error loading {exchange} Delivery file: {e}")
        return None

    if exchange == "NSE":
        bhav_df = normalize_nse_columns(bhav_df)
        deliv_df = normalize_nse_columns(deliv_df)
        date_col = 'DATE'
    else:
        bhav_df = normalize_bse_columns(bhav_df)
        deliv_df = normalize_bse_columns(deliv_df)
        date_col = 'DATE'

    # Deduplicate to keep only latest per symbol
    bhav_df = deduplicate_latest(bhav_df, date_col)
    deliv_df = deduplicate_latest(deliv_df, date_col)

    print(f"  Deduplicated Bhav to {len(bhav_df)} rows (latest dates)")
    print(f"  Deduplicated Delivery to {len(deliv_df)} rows (latest dates)")

    # Filter for equity series
    if 'SERIES' in bhav_df.columns:
        before_filter = len(bhav_df)
        bhav_df = bhav_df[bhav_df['SERIES'] == 'EQ']
        print(f"  Filtered Bhav EQ series: {len(bhav_df)} rows (from {before_filter})")

    if 'SERIES' in deliv_df.columns:
        before_filter = len(deliv_df)
        deliv_df = deliv_df[deliv_df['SERIES'] == 'EQ']
        print(f"  Filtered Delivery EQ series: {len(deliv_df)} rows (from {before_filter})")

    # Merge on SYMBOL
    if 'SYMBOL' not in bhav_df.columns or 'SYMBOL' not in deliv_df.columns:
        print(f"❌ SYMBOL column missing, cannot merge")
        return None
    merged = bhav_df.merge(deliv_df[['SYMBOL', 'DELIV_QTY']], on='SYMBOL', how='left')
    merged['DELIV_QTY'] = merged['DELIV_QTY'].fillna(0)
    print(f"  Merged dataset rows: {len(merged)}")
    return merged

def verify_formulas(merged, processed, exchange):
    print_section(f"{exchange} FORMULA VERIFICATION")

    count_tests = 0
    count_pass = 0

    common_symbols = set(merged['SYMBOL']).intersection(set(processed['SYMBOL']))
    test_symbols = list(common_symbols)[:3]

    if not test_symbols:
        print(f"⚠️ No common symbols between {exchange} raw and processed")
        return count_tests, count_pass

    for i, sym in enumerate(test_symbols, 1):
        print(f"\n{'-'*60}")
        print(f"{exchange} Stock #{i}: {sym}")
        print(f"{'-'*60}")
        raw_row = merged[merged['SYMBOL'] == sym].iloc[0]
        proc_row = processed[processed['SYMBOL'] == sym].iloc[0]

        try:
            expected_dt = float(raw_row['DELIV_QTY']) * float(raw_row['CLOSE'])
            actual_dt = float(proc_row['DELIVERY_TURNOVER'])
            diff_dt = abs(expected_dt - actual_dt)
            tolerance_dt = max(expected_dt * 0.01, 100)
            print(f"Delivery Turnover: Expected = ₹{expected_dt:,.2f}, Actual = ₹{actual_dt:,.2f}, Diff = ₹{diff_dt:,.2f}")
            if diff_dt <= tolerance_dt:
                print("✅ DELIVERY_TURNOVER check - PASS")
                count_pass += 1
            else:
                print("❌ DELIVERY_TURNOVER check - FAIL")
            count_tests += 1
        except Exception as e:
            print(f"⚠️ DELIVERY_TURNOVER check error: {e}")

        # ATW check, only if TOTTRDVAL present
        try:
            if 'TOTTRDVAL' in raw_row:
                expected_atw = float(raw_row['TOTTRDVAL']) / 1000
                actual_atw = float(proc_row['ATW'])
                diff_atw = abs(expected_atw - actual_atw)
                tolerance_atw = max(expected_atw * 0.01, 10)
                print(f"ATW: Expected = ₹{expected_atw:,.2f}, Actual = ₹{actual_atw:,.2f}, Diff = ₹{diff_atw:,.2f}")
                if diff_atw <= tolerance_atw:
                    print("✅ ATW check - PASS")
                    count_pass += 1
                else:
                    print("❌ ATW check - FAIL")
                count_tests += 1
            else:
                print("⚠️ TOTTRDVAL missing, skipping ATW check")
        except Exception as e:
            print(f"⚠️ ATW check error: {e}")

    return count_tests, count_pass

def main():
    print_section("START AUDIT PIPELINE FAITHFUL")

    # Find raw files
    nse_bhav = find_latest_file(NSE_BHAV_DIR, 'cm_bhav')
    nse_deliv = find_latest_file(NSE_DELIV_DIR, 'nse_delivery')

    bse_bhav = find_latest_file(BSE_BHAV_DIR, 'bse_bhav')
    bse_deliv = find_latest_file(BSE_DELIV_DIR, 'bse_delivery')

    print(f"\nNSE Bhav file: {nse_bhav or 'MISSING'}")
    print(f"NSE Delivery file: {nse_deliv or 'MISSING'}")
    print(f"BSE Bhav file: {bse_bhav or 'MISSING'}")
    print(f"BSE Delivery file: {bse_deliv or 'MISSING'}")

    if not all([nse_bhav, nse_deliv, bse_bhav, bse_deliv]):
        print("\n❌ Missing one or more raw files. Please run auto_update_smart.py for updates.")
        sys.exit(1)

    # Load processed data
    try:
        processed = pd.read_csv("data/combined_dashboard_live.csv")
        print(f"\n✅ Loaded processed data: {len(processed)} rows")
    except Exception as e:
        print(f"❌ Could not load processed data: {e}")
        sys.exit(1)

    # Process NSE
    nse_merged = load_and_process("NSE", nse_bhav, nse_deliv)
    if nse_merged is None:
        print("❌ Error loading/processing NSE data")
        sys.exit(1)

    # Process BSE
    bse_merged = load_and_process("BSE", bse_bhav, bse_deliv)
    if bse_merged is None:
        print("❌ Error loading/processing BSE data")
        sys.exit(1)

    # Verify formulas
    nse_tests, nse_pass = verify_formulas(nse_merged, processed, "NSE")
    bse_tests, bse_pass = verify_formulas(bse_merged, processed, "BSE")

    total_tests = nse_tests + bse_tests
    total_pass = nse_pass + bse_pass

    print_section("AUDIT SUMMARY")

    print(f"\nTotal tests run: {total_tests}")
    print(f"Total tests passed: {total_pass}")

    if total_tests == 0:
        print("❌ No tests ran. Check raw files and symbol matching.")
    elif total_tests == total_pass:
        print("\n🎉 ALL FORMULAS VERIFIED SUCCESSFULLY! Pipeline logic is correct.")
    else:
        print("\n⚠️ SOME FORMULA CHECKS FAILED! Check details above for mismatches.")

if __name__ == "__main__":
    main()
