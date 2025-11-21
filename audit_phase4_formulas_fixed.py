"""
AUDIT PHASE 4 - RAW FORMULA VERIFICATION (FIXED)
================================================
This version auto-detects column names and handles NSE/BSE format differences.

Formulas to verify:
- DELIVERY_TURNOVER = DELIV_QTY × CLOSE
- ATW = TOTTRDVAL / 1000

How to run:
    python audit_phase4_formulas_fixed.py
"""

import pandas as pd
import os
import sys
from pathlib import Path

def find_column(df, possible_names):
    """Find first matching column name from a list of possibilities."""
    for name in possible_names:
        if name in df.columns:
            return name
        # Try case-insensitive match
        for col in df.columns:
            if col.upper() == name.upper():
                return col
    return None

def get_files_in_folder(directory):
    """Get all CSV files in a directory."""
    if not os.path.exists(directory):
        return []
    return [f for f in os.listdir(directory) if f.endswith('.csv')]

def main():
    print("\n" + "="*70)
    print("PHASE 4: RAW FORMULA VERIFICATION (FIXED)")
    print("="*70)

    # -----------------------------
    # FIND RAW FILES
    # -----------------------------
    print("\n🔍 Searching for raw data files...")
    
    # Check multiple possible locations
    locations_to_check = [
        ("data/nse_raw", "NSE bhav"),
        ("data/bse_raw", "BSE bhav"),
        ("data", "fallback data folder")
    ]
    
    nse_bhav_file = None
    bse_bhav_file = None
    
    for folder, label in locations_to_check:
        if os.path.exists(folder):
            files = get_files_in_folder(folder)
            print(f"\n📁 {folder}/: {len(files)} CSV files found")
            for f in files[:5]:  # Show first 5
                print(f"   - {f}")
            
            # Look for NSE bhav (not delivery!)
            for f in files:
                if not nse_bhav_file and 'cm' in f.lower() and 'bhav' in f.lower() and 'nse' not in f.lower():
                    nse_bhav_file = os.path.join(folder, f)
            
            # Look for BSE bhav
            for f in files:
                if not bse_bhav_file and 'bse' in f.lower() and 'bhav' in f.lower():
                    bse_bhav_file = os.path.join(folder, f)

    if not nse_bhav_file and not bse_bhav_file:
        print("\n❌ ERROR: No raw bhav files found!")
        print("\n💡 Expected file patterns:")
        print("   NSE bhav: cm19NOV2025bhav.csv")
        print("   BSE bhav: bse_bhav_20251119.csv")
        print("\n📥 Run this first: python auto_update_smart.py")
        sys.exit(1)

    print("\n✅ Files to audit:")
    if nse_bhav_file:
        print(f"   NSE: {nse_bhav_file}")
    if bse_bhav_file:
        print(f"   BSE: {bse_bhav_file}")

    # -----------------------------
    # LOAD PROCESSED DATA
    # -----------------------------
    try:
        processed = pd.read_csv("data/combined_dashboard_live.csv")
        print(f"\n✅ Loaded processed data: {len(processed)} rows")
    except FileNotFoundError:
        print("\n❌ ERROR: data/combined_dashboard_live.csv not found!")
        sys.exit(1)

    all_match = True
    tests_run = 0

    # -----------------------------
    # AUDIT NSE DATA
    # -----------------------------
    if nse_bhav_file:
        print("\n" + "="*70)
        print("NSE RAW FORMULA AUDIT")
        print("="*70)

        try:
            raw_nse = pd.read_csv(nse_bhav_file)
            print(f"\nRaw NSE file: {len(raw_nse)} rows")
            print(f"Columns (first 15): {', '.join(raw_nse.columns[:15])}")

            # Auto-detect column names
            col_symbol = find_column(raw_nse, ['SYMBOL'])
            col_close = find_column(raw_nse, ['CLOSE', 'CLOSE_PRICE', 'ClsgPric'])
            col_deliv_qty = find_column(raw_nse, ['DELIV_QTY', 'DELIV_QUANTITY', 'DELIVERY_QTY'])
            col_tottrdval = find_column(raw_nse, ['TOTTRDVAL', 'TOTAL_TRADED_VALUE', 'TotTradgVal'])
            col_series = find_column(raw_nse, ['SERIES', 'INSTRUMENT', 'SctySrs'])

            print(f"\n🔍 Detected columns:")
            print(f"   SYMBOL → {col_symbol}")
            print(f"   CLOSE → {col_close}")
            print(f"   DELIV_QTY → {col_deliv_qty}")
            print(f"   TOTTRDVAL → {col_tottrdval}")
            print(f"   SERIES → {col_series}")

            if not all([col_symbol, col_close, col_deliv_qty, col_tottrdval]):
                print("\n⚠️  Missing required columns in NSE file, skipping...")
            else:
                # Filter for EQ series
                if col_series:
                    raw_nse = raw_nse[raw_nse[col_series] == 'EQ']
                    print(f"\nAfter EQ filter: {len(raw_nse)} rows")

                # Pick 3 test stocks
                test_stocks = []
                for symbol in raw_nse[col_symbol].sample(min(10, len(raw_nse))):
                    if symbol in processed['SYMBOL'].values:
                        test_stocks.append(symbol)
                        if len(test_stocks) >= 3:
                            break

                if not test_stocks:
                    print("⚠️  No common symbols found")
                else:
                    print(f"\nTesting {len(test_stocks)} NSE stocks:\n")

                    for i, symbol in enumerate(test_stocks, 1):
                        print(f"{'─'*70}")
                        print(f"NSE Stock #{i}: {symbol}")
                        print(f"{'─'*70}")

                        raw_row = raw_nse[raw_nse[col_symbol] == symbol].iloc[0]
                        proc_row = processed[processed['SYMBOL'] == symbol].iloc[0]

                        # DELIVERY_TURNOVER
                        print("\n🔍 Formula Check: DELIVERY_TURNOVER")
                        
                        raw_close = float(raw_row[col_close])
                        raw_deliv_qty = float(raw_row[col_deliv_qty])
                        
                        expected_dt = raw_deliv_qty * raw_close
                        actual_dt = float(proc_row['DELIVERY_TURNOVER'])
                        
                        print(f"   Raw: DELIV_QTY={raw_deliv_qty:,.0f}, CLOSE=₹{raw_close:.2f}")
                        print(f"   Expected: {raw_deliv_qty:,.0f} × ₹{raw_close:.2f} = ₹{expected_dt:,.2f}")
                        print(f"   Actual:   ₹{actual_dt:,.2f}")
                        
                        diff_dt = abs(expected_dt - actual_dt)
                        tolerance_dt = max(expected_dt * 0.01, 1)
                        
                        if diff_dt <= tolerance_dt:
                            print(f"   ✅ MATCH (diff: ₹{diff_dt:.2f})")
                            tests_run += 1
                        else:
                            print(f"   ❌ MISMATCH! Difference: ₹{diff_dt:,.2f}")
                            all_match = False
                            tests_run += 1

                        # ATW
                        print("\n🔍 Formula Check: ATW")
                        
                        raw_tottrdval = float(raw_row[col_tottrdval])
                        
                        expected_atw = raw_tottrdval / 1000
                        actual_atw = float(proc_row['ATW'])
                        
                        print(f"   Raw: TOTTRDVAL=₹{raw_tottrdval:,.2f}")
                        print(f"   Expected: ₹{raw_tottrdval:,.2f} ÷ 1000 = ₹{expected_atw:,.2f}")
                        print(f"   Actual:   ₹{actual_atw:,.2f}")
                        
                        diff_atw = abs(expected_atw - actual_atw)
                        tolerance_atw = max(expected_atw * 0.01, 0.1)
                        
                        if diff_atw <= tolerance_atw:
                            print(f"   ✅ MATCH (diff: ₹{diff_atw:.2f})\n")
                            tests_run += 1
                        else:
                            print(f"   ❌ MISMATCH! Difference: ₹{diff_atw:,.2f}\n")
                            all_match = False
                            tests_run += 1

        except Exception as e:
            print(f"\n❌ Error processing NSE: {e}")
            import traceback
            traceback.print_exc()

    # -----------------------------
    # AUDIT BSE DATA
    # -----------------------------
    if bse_bhav_file:
        print("\n" + "="*70)
        print("BSE RAW FORMULA AUDIT")
        print("="*70)

        try:
            raw_bse = pd.read_csv(bse_bhav_file)
            print(f"\nRaw BSE file: {len(raw_bse)} rows")
            print(f"Columns (first 15): {', '.join(raw_bse.columns[:15])}")

            # Auto-detect column names
            col_symbol = find_column(raw_bse, ['SYMBOL', 'TckrSymb', 'SCRIP_CD'])
            col_close = find_column(raw_bse, ['CLOSE', 'ClsgPric', 'CLOSE_PRICE'])
            col_deliv_qty = find_column(raw_bse, ['DELIV_QTY', 'DelQty', 'DELIVERY_QTY'])
            col_tottrdval = find_column(raw_bse, ['TOTTRDVAL', 'TtlTradgVal', 'TOTAL_TRADED_VALUE'])

            print(f"\n🔍 Detected columns:")
            print(f"   SYMBOL → {col_symbol}")
            print(f"   CLOSE → {col_close}")
            print(f"   DELIV_QTY → {col_deliv_qty}")
            print(f"   TOTTRDVAL → {col_tottrdval}")

            if not all([col_symbol, col_close, col_deliv_qty, col_tottrdval]):
                print("\n⚠️  Missing required columns in BSE file, skipping...")
            else:
                # Pick 3 test stocks
                test_stocks = []
                for symbol in raw_bse[col_symbol].sample(min(10, len(raw_bse))):
                    if symbol in processed['SYMBOL'].values:
                        test_stocks.append(symbol)
                        if len(test_stocks) >= 3:
                            break

                if not test_stocks:
                    print("⚠️  No common symbols found")
                else:
                    print(f"\nTesting {len(test_stocks)} BSE stocks:\n")

                    for i, symbol in enumerate(test_stocks, 1):
                        print(f"{'─'*70}")
                        print(f"BSE Stock #{i}: {symbol}")
                        print(f"{'─'*70}")

                        raw_row = raw_bse[raw_bse[col_symbol] == symbol].iloc[0]
                        proc_row = processed[processed['SYMBOL'] == symbol].iloc[0]

                        # DELIVERY_TURNOVER
                        print("\n🔍 Formula Check: DELIVERY_TURNOVER")
                        
                        raw_close = float(raw_row[col_close])
                        raw_deliv_qty = float(raw_row[col_deliv_qty])
                        
                        expected_dt = raw_deliv_qty * raw_close
                        actual_dt = float(proc_row['DELIVERY_TURNOVER'])
                        
                        print(f"   Raw: DELIV_QTY={raw_deliv_qty:,.0f}, CLOSE=₹{raw_close:.2f}")
                        print(f"   Expected: {raw_deliv_qty:,.0f} × ₹{raw_close:.2f} = ₹{expected_dt:,.2f}")
                        print(f"   Actual:   ₹{actual_dt:,.2f}")
                        
                        diff_dt = abs(expected_dt - actual_dt)
                        tolerance_dt = max(expected_dt * 0.01, 1)
                        
                        if diff_dt <= tolerance_dt:
                            print(f"   ✅ MATCH (diff: ₹{diff_dt:.2f})")
                            tests_run += 1
                        else:
                            print(f"   ❌ MISMATCH! Difference: ₹{diff_dt:,.2f}")
                            all_match = False
                            tests_run += 1

                        # ATW
                        print("\n🔍 Formula Check: ATW")
                        
                        raw_tottrdval = float(raw_row[col_tottrdval])
                        
                        expected_atw = raw_tottrdval / 1000
                        actual_atw = float(proc_row['ATW'])
                        
                        print(f"   Raw: TOTTRDVAL=₹{raw_tottrdval:,.2f}")
                        print(f"   Expected: ₹{raw_tottrdval:,.2f} ÷ 1000 = ₹{expected_atw:,.2f}")
                        print(f"   Actual:   ₹{actual_atw:,.2f}")
                        
                        diff_atw = abs(expected_atw - actual_atw)
                        tolerance_atw = max(expected_atw * 0.01, 0.1)
                        
                        if diff_atw <= tolerance_atw:
                            print(f"   ✅ MATCH (diff: ₹{diff_atw:.2f})\n")
                            tests_run += 1
                        else:
                            print(f"   ❌ MISMATCH! Difference: ₹{diff_atw:,.2f}\n")
                            all_match = False
                            tests_run += 1

        except Exception as e:
            print(f"\n❌ Error processing BSE: {e}")
            import traceback
            traceback.print_exc()

    # -----------------------------
    # FINAL VERDICT
    # -----------------------------
    print("\n" + "="*70)
    print("PHASE 4 FINAL VERDICT")
    print("="*70)

    if tests_run == 0:
        print("\n⚠️  NO TESTS WERE RUN")
        print("   Could not find matching raw files or columns.")
    elif all_match:
        print(f"\n✅ ALL {tests_run} FORMULA CHECKS PASSED!")
        print("   DELIVERY_TURNOVER = DELIV_QTY × CLOSE ✓")
        print("   ATW = TOTTRDVAL / 1000 ✓")
        print("\n🎉 Your system is calculating formulas correctly.")
        print("   No 'total turnover vs delivery turnover' style bugs detected.")
    else:
        print(f"\n❌ SOME FORMULA CHECKS FAILED (out of {tests_run} tests)")
        print("   There are calculation errors in your data pipeline.")
        print("   Review the ❌ lines above.")

if __name__ == "__main__":
    main()
    sys.exit(0)
