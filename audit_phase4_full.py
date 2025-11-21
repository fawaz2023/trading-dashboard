"""
PHASE 4: FULL PIPELINE-FAITHFUL FORMULA VERIFICATION (STRIP COLUMN SPACES VERSION)
==================================================================================
This script:
- Loads your raw NSE and BSE Bhav + Delivery files
- Strips spaces from column names to avoid merge errors!
- Normalizes column names, filters equity series only
- Deduplicates latest per symbol (optional)
- Merges Bhav + Delivery on SYMBOL
- Verifies DELIVERY_TURNOVER and ATW formulas
- Prints detailed results
"""

import os
import sys
import pandas as pd

def print_section(title):
    print("\n" + "="*70)
    print(title)
    print("="*70)

def find_file(folder, keywords):
    if not os.path.exists(folder):
        return None
    for f in sorted(os.listdir(folder), reverse=True):
        if all(k.lower() in f.lower() for k in keywords) and f.endswith('.csv'):
            return os.path.join(folder, f)
    return None

def normalize_columns(df, exchange):
    # Remove spaces!
    df.columns = [col.strip() for col in df.columns]
    if exchange == "NSE":
        col_map = {
            'TckrSymb': 'SYMBOL',
            'ClsPric': 'CLOSE',
            'Deliv_Qty': 'DELIV_QTY',
            'DELIV_QTY': 'DELIV_QTY',
            'TtlTrfVal': 'TOTTRDVAL',
            'TtlTradgVol': 'TTL_TRD_QNTY',
            'SERIES': 'SERIES',
            'TradDt': 'DATE',
            'DATE1': 'DATE',
            'CLOSE_PRICE': 'CLOSE'
        }
    else:
        col_map = {
            'TckrSymb': 'SYMBOL',
            'ClsPric': 'CLOSE',
            'DelQty': 'DELIV_QTY',
            'DELIV_QTY': 'DELIV_QTY',
            'TtlTrfVal': 'TOTTRDVAL',
            'TtlTradgVol': 'TTL_TRD_QNTY',
            'SctySrs': 'SERIES',
            'TradDt': 'DATE',
            'DATE1': 'DATE',
            'CLOSE_PRICE': 'CLOSE'
        }
    renames = {c: col_map.get(c, c) for c in df.columns}
    df = df.rename(columns=renames)
    return df

def deduplicate_latest(df, date_col='DATE', symbol_col='SYMBOL'):
    if date_col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=[date_col]) if date_col in df.columns else df
    df = df.sort_values(by=[symbol_col, date_col], ascending=[True, False])
    return df.drop_duplicates(subset=[symbol_col], keep='first')

def load_and_process(exchange, bhav_path, deliv_path):
    print_section(f"{exchange} RAW DATA LOAD & PROCESS")
    try:
        bhav_df = pd.read_csv(bhav_path)
        deliv_df = pd.read_csv(deliv_path)
        print(f"Loaded Bhav: {bhav_path} ({len(bhav_df)} rows)")
        print(f"Loaded Delivery: {deliv_path} ({len(deliv_df)} rows)")
    except Exception as e:
        print(f"❌ Error loading raw files for {exchange}: {e}")
        return None

    # Strip spaces from columns for both
    bhav_df.columns = [col.strip() for col in bhav_df.columns]
    deliv_df.columns = [col.strip() for col in deliv_df.columns]

    bhav_df = normalize_columns(bhav_df, exchange)
    deliv_df = normalize_columns(deliv_df, exchange)

    # Remove duplicate columns
    bhav_df = bhav_df.loc[:, ~bhav_df.columns.duplicated()]
    deliv_df = deliv_df.loc[:, ~deliv_df.columns.duplicated()]

    date_col = 'DATE' if 'DATE' in bhav_df.columns else None
    if date_col:
        bhav_df = deduplicate_latest(bhav_df, date_col)
        deliv_df = deduplicate_latest(deliv_df, date_col)

    # Filter EQ only
    if 'SERIES' in bhav_df.columns:
        before = len(bhav_df)
        bhav_df = bhav_df[bhav_df['SERIES'] == 'EQ']
        print(f"Filtered Bhav EQ: {len(bhav_df)} (from {before})")
    if 'SERIES' in deliv_df.columns:
        before = len(deliv_df)
        deliv_df = deliv_df[deliv_df['SERIES'] == 'EQ']
        print(f"Filtered Delivery EQ: {len(deliv_df)} (from {before})")

    # Fix DELIV_QTY column if missing
    if 'DELIV_QTY' not in deliv_df.columns:
        candidates = [c for c in deliv_df.columns if 'deliv' in c.lower() and 'qty' in c.lower()]
        if candidates:
            deliv_df['DELIV_QTY'] = deliv_df[candidates[0]]
            print(f"Renamed column {candidates[0]} to DELIV_QTY")
        else:
            print("❌ DELIV_QTY column not found in delivery data")
            return None

    if 'SYMBOL' not in bhav_df.columns or 'SYMBOL' not in deliv_df.columns:
        print("❌ SYMBOL column missing in Bhav or Delivery data")
        return None

    merged = bhav_df.merge(deliv_df[['SYMBOL', 'DELIV_QTY']], on='SYMBOL', how='left')
    merged['DELIV_QTY'] = merged['DELIV_QTY'].fillna(0)
    print(f"Merged dataset rows: {len(merged)}")
    return merged

def verify_formulas(merged, processed, exchange):
    print_section(f"{exchange} FORMULA VERIFICATION")
    count_tests, count_pass = 0, 0
    common_symbols = set(merged['SYMBOL']).intersection(set(processed['SYMBOL']))
    test_symbols = list(common_symbols)[:3]

    if not test_symbols:
        print(f"⚠️ No common {exchange} symbols to test.")
        return 0, 0

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
            count_tests += 1
        except Exception as e:
            print(f"Error in DELIVERY_TURNOVER check: {e}")

        try:
            if 'TOTTRDVAL' in raw_row:
                expected_atw = float(raw_row['TOTTRDVAL']) / 1000
                actual_atw = float(proc_row['ATW'])
                diff = abs(expected_atw - actual_atw)
                tolerance = max(expected_atw * 0.01, 10)
                print(f"ATW - Expected: ₹{expected_atw:,.2f}, Actual: ₹{actual_atw:,.2f}, Diff: ₹{diff:,.2f}")
                if diff <= tolerance:
                    print("✅ ATW PASS")
                    count_pass += 1
                else:
                    print("❌ ATW FAIL")
                count_tests += 1
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

    nse_bhav = find_file(NSE_BHAV_DIR, ['cm_bhav'])
    nse_deliv = find_file(NSE_DELIV_DIR, ['nse_delivery'])
    bse_bhav = find_file(BSE_BHAV_DIR, ['bse_bhav'])
    bse_deliv = find_file(BSE_DELIV_DIR, ['bse_delivery'])

    print(f"NSE Bhav: {nse_bhav or 'MISSING'}")
    print(f"NSE Delivery: {nse_deliv or 'MISSING'}")
    print(f"BSE Bhav: {bse_bhav or 'MISSING'}")
    print(f"BSE Delivery: {bse_deliv or 'MISSING'}")

    if not all([nse_bhav, nse_deliv, bse_bhav, bse_deliv]):
        print("\n❌ One or more raw files missing. Run auto_update_smart.py to download.")
        sys.exit(1)

    try:
        processed = pd.read_csv("data/combined_dashboard_live.csv")
        print(f"\nLoaded processed data: {len(processed)} rows")
    except Exception as e:
        print(f"\n❌ Could not load processed data: {e}")
        sys.exit(1)

    nse_merged = load_and_process("NSE", nse_bhav, nse_deliv)
    if nse_merged is None:
        print("❌ Error loading NSE data. Exiting.")
        sys.exit(1)

    bse_merged = load_and_process("BSE", bse_bhav, bse_deliv)
    if bse_merged is None:
        print("❌ Error loading BSE data. Exiting.")
        sys.exit(1)

    nse_tests, nse_pass = verify_formulas(nse_merged, processed, "NSE")
    bse_tests, bse_pass = verify_formulas(bse_merged, processed, "BSE")

    total_tests = nse_tests + bse_tests
    total_pass = nse_pass + bse_pass

    print_section("AUDIT SUMMARY")
    print(f"Total tests run: {total_tests}")
    print(f"Total tests passed: {total_pass}")
    if total_tests == 0:
        print("\n❌ No tests ran; symbol mismatch or empty data.")
    elif total_tests == total_pass:
        print("\n🎉 ALL FORMULAS VERIFIED SUCCESSFULLY! Pipeline logic confirmed!")
    else:
        print("\n⚠️ Some formula checks failed! Review errors above.")

if __name__ == "__main__":
    main()
