"""
AUDIT PIPELINE-FAITHFUL RAW DATA FORMULA VERIFICATION WITH DEDUPLICATION
========================================================================
This script mimics your actual pipeline, including:
- Loading raw NSE and BSE Bhav + Delivery files
- Deduplicating to keep latest entry per symbol
- Filtering equity stocks (SERIES == 'EQ')
- Merging datasets on SYMBOL
- Verifying DELIVERY_TURNOVER and ATW formulas
- Detailed logging of files and errors
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
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=[date_col])
    df = df.sort_values(by=[symbol_col, date_col], ascending=[True, False])
    deduped = df.drop_duplicates(subset=[symbol_col], keep='first')
    return deduped

def load_and_normalize_bhav_delivery(bhav_path, deliv_path, exchange):
    print_section(f"{exchange} RAW DATA LOAD AND PROCESS")

    try:
        bhav_df = pd.read_csv(bhav_path)
        deliv_df = pd.read_csv(deliv_path)
    except Exception as e:
        print(f"❌ ERROR loading files: {e}")
        return None

    if exchange == "NSE":
        bhav_df = normalize_nse_columns(bhav_df)
        deliv_df = normalize_nse_columns(deliv_df)
    else:
        bhav_df = normalize_bse_columns(bhav_df)
        deliv_df = normalize_bse_columns(deliv_df)

    # Remove duplicated columns if any
    bhav_df = bhav_df.loc[:, ~bhav_df.columns.duplicated()]
    deliv_df = deliv_df.loc[:, ~deliv_df.columns.duplicated()]

    # Remove duplicate 'SYMBOL' columns if any
    for df in [bhav_df, deliv_df]:
        if sum(df.columns == 'SYMBOL') > 1:
            cols = df.columns.tolist()
            new_cols = []
            seen = set()
            for col in cols:
                if col == 'SYMBOL':
                    if col in seen:
                        new_cols.append(f"{col}_dup")
                    else:
                        new_cols.append(col)
                        seen.add(col)
                else:
                    new_cols.append(col)
            df.columns = new_cols

    # Deduplicate based on date to get latest per symbol
    date_col = 'DATE' if 'DATE' in bhav_df.columns else None
    if date_col:
        bhav_df = deduplicate_latest(bhav_df, date_col)
    if date_col and date_col in deliv_df.columns:
        deliv_df = deduplicate_latest(deliv_df, date_col)

    # Filter equities (SERIES == 'EQ')
    if 'SERIES' in bhav_df.columns:
        before = len(bhav_df)
        bhav_df = bhav_df[bhav_df['SERIES'] == 'EQ']
        print(f"Filtered Bhav EQ: {len(bhav_df)} rows (from {before})")
    if 'SERIES' in deliv_df.columns:
        before = len(deliv_df)
        deliv_df = deliv_df[deliv_df['SERIES'] == 'EQ']
        print(f"Filtered Delivery EQ: {len(deliv_df)} rows (from {before})")

    if 'SYMBOL' not in bhav_df.columns or 'SYMBOL' not in deliv_df.columns:
        print("❌ SYMBOL column missing; cannot merge")
        return None

    merged = bhav_df.merge(deliv_df[['SYMBOL', 'DELIV_QTY']], on='SYMBOL', how='left')
    merged['DELIV_QTY'] = merged['DELIV_QTY'].fillna(0)
    print(f"Merged data count: {len(merged)}")
    return merged

def verify_formulas(merged, processed, exchange):
    print_section(f"{exchange} FORMULA VERIFICATION")

    count_tests, count_pass = 0, 0
    common_symbols = set(merged['SYMBOL']).intersection(set(processed['SYMBOL']))
    test_symbols = list(common_symbols)[:3]

    if not test_symbols:
        print(f"⚠️ No common {exchange} symbols to test")
        return count_tests, count_pass

    for i, sym in enumerate(test_symbols, 1):
        print(f"\n{'-'*60}\n{exchange} Stock #{i}: {sym}\n{'-'*60}")
        raw_row = merged[merged['SYMBOL'] == sym].iloc[0]
        proc_row = processed[processed['SYMBOL'] == sym].iloc[0]

        try:
            expected_dt = float(raw_row['DELIV_QTY']) * float(raw_row['CLOSE'])
            actual_dt = float(proc_row['DELIVERY_TURNOVER'])
            diff = abs(expected_dt - actual_dt)
            tolerance = max(expected_dt * 0.01, 100)
            print(f"Delivery Turnover - Expected: ₹{expected_dt:,.2f}, Actual: ₹{actual_dt:,.2f}, Diff: ₹{diff:,.2f}")
            if diff <= tolerance:
                print("✅ DELIVERY_TURNOVER PASS")
                count_pass += 1
            else:
                print("❌ DELIVERY_TURNOVER FAIL")
            count_tests +=1
        except Exception as e:
            print(f"Error in DELIVERY_TURNOVER check: {e}")

        try:
            if 'TOTTRDVAL' in raw_row:
                expected_atw = float(raw_row['TOTTRDVAL']) / 1000
                actual_atw = float(proc_row['ATW'])
                diff = abs(expected_atw - actual_atw)
                tolerance = max(expected_atw * 0.01,10)
                print(f"ATW - Expected: ₹{expected_atw:,.2f}, Actual: ₹{actual_atw:,.2f}, Diff: ₹{diff:,.2f}")
                if diff <= tolerance:
                    print("✅ ATW PASS")
                    count_pass +=1
                else:
                    print("❌ ATW FAIL")
                count_tests +=1
            else:
                print("⚠️ TOTTRDVAL missing; skipping ATW check")
        except Exception as e:
            print(f"Error in ATW check: {e}")
    return count_tests, count_pass

def main():
    NSE_BHAV_DIR = "data/nse_raw"
    NSE_DELIV_DIR = "data/nse_raw"
    BSE_BHAV_DIR = "data/bse_raw"
    BSE_DELIV_DIR = "data"

    print_section("START AUDIT PIPELINE FAITHFUL")

    nse_bhav = find_latest_file(NSE_BHAV_DIR, 'cm_bhav')
    nse_deliv = find_latest_file(NSE_DELIV_DIR, 'nse_delivery')
    bse_bhav = find_latest_file(BSE_BHAV_DIR, 'bse_bhav')
    bse_deliv = find_latest_file(BSE_DELIV_DIR, 'bse_delivery')

    print(f"NSE Bhav: {nse_bhav or 'MISSING'}")
    print(f"NSE Delivery: {nse_deliv or 'MISSING'}")
    print(f"BSE Bhav: {bse_bhav or 'MISSING'}")
    print(f"BSE Delivery: {bse_deliv or 'MISSING'}")

    if not all([nse_bhav, nse_deliv, bse_bhav, bse_deliv]):
        print("\n❌ Missing one or more raw files. Run auto_update_smart.py.")
        sys.exit(1)

    try:
        processed = pd.read_csv("data/combined_dashboard_live.csv")
        print(f"\nLoaded processed data: {len(processed)} rows")
    except Exception as e:
        print(f"\n❌ Could not load processed data: {e}")
        sys.exit(1)

    nse_merged = load_and_normalize_bhav_delivery(nse_bhav, nse_deliv, "NSE")
    if nse_merged is None:
        print("❌ NSE data loading failed")
        sys.exit(1)

    bse_merged = load_and_normalize_bhav_delivery(bse_bhav, bse_deliv, "BSE")
    if bse_merged is None:
        print("❌ BSE data loading failed")
        sys.exit(1)

    nse_tests, nse_pass = verify_formulas(nse_merged, processed, "NSE")
    bse_tests, bse_pass = verify_formulas(bse_merged, processed, "BSE")

    total_tests = nse_tests + bse_tests
    total_pass = nse_pass + bse_pass

    print_section("AUDIT SUMMARY")
    print(f"Total tests run: {total_tests}")
    print(f"Total tests passed: {total_pass}")
    if total_tests == 0:
        print("\n❌ No tests run, check symbol matching")
    elif total_tests == total_pass:
        print("\n🎉 ALL FORMULAS VERIFIED SUCCESSFULLY!")
    else:
        print("\n⚠️ SOME FORMULAS FAILED! Check details above.")

if __name__ == "__main__":
    main()
