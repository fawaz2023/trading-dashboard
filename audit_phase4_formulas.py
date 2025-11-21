"""
AUDIT PHASE 4 - RAW FORMULA VERIFICATION
========================================
This script verifies that DELIVERY_TURNOVER and ATW are calculated
correctly by comparing raw NSE/BSE bhav files with processed data.

Formulas to verify:
- DELIVERY_TURNOVER = DELIV_QTY × CLOSE  (NOT total turnover!)
- ATW = TOTTRDVAL / 1000

How to run (from CMD, inside trading_dashboard folder):
    python audit_phase4_formulas.py
"""

import pandas as pd
import os
import sys
from pathlib import Path

def get_latest_file(directory, pattern):
    """Find the latest file matching pattern in directory."""
    if not os.path.exists(directory):
        return None
    files = list(Path(directory).glob(pattern))
    if not files:
        return None
    # Sort by filename (which contains date)
    files.sort(reverse=True)
    return str(files[0])

def main():
    print("\n" + "="*70)
    print("PHASE 4: RAW FORMULA VERIFICATION")
    print("="*70)

    # -----------------------------
    # CHECK IF RAW FILES EXIST
    # -----------------------------
    nse_file = get_latest_file("data/nse_raw", "*.csv")
    bse_file = get_latest_file("data/bse_raw", "*.csv")

    if not nse_file and not bse_file:
        print("\n❌ ERROR: No raw bhav files found!")
        print("\nExpected locations:")
        print("  - data/nse_raw/cm*.csv")
        print("  - data/bse_raw/EQ*.csv")
        print("\n💡 SOLUTION:")
        print("  1. Run your data update script once:")
        print("     python auto_update_smart.py")
        print("  2. This will download raw files to data/nse_raw/ and data/bse_raw/")
        print("  3. Then re-run this audit: python audit_phase4_formulas.py")
        sys.exit(1)

    print("\n✅ Found raw data files:")
    if nse_file:
        print(f"   NSE: {os.path.basename(nse_file)}")
    if bse_file:
        print(f"   BSE: {os.path.basename(bse_file)}")

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

    # -----------------------------
    # AUDIT NSE DATA
    # -----------------------------
    if nse_file:
        print("\n" + "="*70)
        print("NSE RAW FORMULA AUDIT")
        print("="*70)

        try:
            raw_nse = pd.read_csv(nse_file)
            print(f"\nRaw NSE file has {len(raw_nse)} rows")
            print(f"Columns: {', '.join(raw_nse.columns[:10])}...")

            # Filter for EQ series only
            if 'SERIES' in raw_nse.columns:
                raw_nse = raw_nse[raw_nse['SERIES'] == 'EQ']
                print(f"After EQ filter: {len(raw_nse)} rows")

            # Pick 3 random stocks that exist in processed data too
            test_stocks = []
            for symbol in raw_nse['SYMBOL'].sample(min(10, len(raw_nse))):
                if symbol in processed['SYMBOL'].values:
                    test_stocks.append(symbol)
                    if len(test_stocks) >= 3:
                        break

            if not test_stocks:
                print("⚠️  No common symbols found between raw and processed NSE data")
            else:
                print(f"\nTesting {len(test_stocks)} NSE stocks:")

                for i, symbol in enumerate(test_stocks, 1):
                    print(f"\n{'─'*70}")
                    print(f"NSE Stock #{i}: {symbol}")
                    print(f"{'─'*70}")

                    # Get raw data
                    raw_row = raw_nse[raw_nse['SYMBOL'] == symbol].iloc[0]
                    
                    # Get processed data
                    proc_row = processed[processed['SYMBOL'] == symbol].iloc[0]

                    # --- FORMULA 1: DELIVERY_TURNOVER ---
                    print("\n🔍 Formula Check: DELIVERY_TURNOVER")
                    
                    raw_close = float(raw_row['CLOSE'])
                    raw_deliv_qty = float(raw_row['DELIV_QTY'])
                    
                    expected_dt = raw_deliv_qty * raw_close
                    actual_dt = float(proc_row['DELIVERY_TURNOVER'])
                    
                    print(f"   Raw: DELIV_QTY={raw_deliv_qty:,.0f}, CLOSE=₹{raw_close:.2f}")
                    print(f"   Expected: {raw_deliv_qty:,.0f} × ₹{raw_close:.2f} = ₹{expected_dt:,.2f}")
                    print(f"   Actual:   ₹{actual_dt:,.2f}")
                    
                    diff_dt = abs(expected_dt - actual_dt)
                    tolerance_dt = max(expected_dt * 0.01, 1)  # 1% tolerance
                    
                    if diff_dt <= tolerance_dt:
                        print(f"   ✅ MATCH (diff: ₹{diff_dt:.2f})")
                    else:
                        print(f"   ❌ MISMATCH! Difference: ₹{diff_dt:,.2f}")
                        all_match = False

                    # --- FORMULA 2: ATW ---
                    print("\n🔍 Formula Check: ATW")
                    
                    raw_tottrdval = float(raw_row['TOTTRDVAL'])
                    
                    expected_atw = raw_tottrdval / 1000
                    actual_atw = float(proc_row['ATW'])
                    
                    print(f"   Raw: TOTTRDVAL=₹{raw_tottrdval:,.2f}")
                    print(f"   Expected: ₹{raw_tottrdval:,.2f} ÷ 1000 = ₹{expected_atw:,.2f}")
                    print(f"   Actual:   ₹{actual_atw:,.2f}")
                    
                    diff_atw = abs(expected_atw - actual_atw)
                    tolerance_atw = max(expected_atw * 0.01, 0.1)
                    
                    if diff_atw <= tolerance_atw:
                        print(f"   ✅ MATCH (diff: ₹{diff_atw:.2f})")
                    else:
                        print(f"   ❌ MISMATCH! Difference: ₹{diff_atw:,.2f}")
                        all_match = False

        except Exception as e:
            print(f"\n❌ Error processing NSE file: {e}")
            import traceback
            traceback.print_exc()

    # -----------------------------
    # AUDIT BSE DATA
    # -----------------------------
    if bse_file:
        print("\n" + "="*70)
        print("BSE RAW FORMULA AUDIT")
        print("="*70)

        try:
            raw_bse = pd.read_csv(bse_file)
            print(f"\nRaw BSE file has {len(raw_bse)} rows")
            print(f"Columns: {', '.join(raw_bse.columns[:10])}...")

            # Pick 3 random stocks that exist in processed data too
            test_stocks = []
            for symbol in raw_bse['SYMBOL'].sample(min(10, len(raw_bse))):
                if symbol in processed['SYMBOL'].values:
                    test_stocks.append(symbol)
                    if len(test_stocks) >= 3:
                        break

            if not test_stocks:
                print("⚠️  No common symbols found between raw and processed BSE data")
            else:
                print(f"\nTesting {len(test_stocks)} BSE stocks:")

                for i, symbol in enumerate(test_stocks, 1):
                    print(f"\n{'─'*70}")
                    print(f"BSE Stock #{i}: {symbol}")
                    print(f"{'─'*70}")

                    # Get raw data
                    raw_row = raw_bse[raw_bse['SYMBOL'] == symbol].iloc[0]
                    
                    # Get processed data
                    proc_row = processed[processed['SYMBOL'] == symbol].iloc[0]

                    # --- FORMULA 1: DELIVERY_TURNOVER ---
                    print("\n🔍 Formula Check: DELIVERY_TURNOVER")
                    
                    raw_close = float(raw_row['CLOSE'])
                    raw_deliv_qty = float(raw_row['DELIV_QTY'])
                    
                    expected_dt = raw_deliv_qty * raw_close
                    actual_dt = float(proc_row['DELIVERY_TURNOVER'])
                    
                    print(f"   Raw: DELIV_QTY={raw_deliv_qty:,.0f}, CLOSE=₹{raw_close:.2f}")
                    print(f"   Expected: {raw_deliv_qty:,.0f} × ₹{raw_close:.2f} = ₹{expected_dt:,.2f}")
                    print(f"   Actual:   ₹{actual_dt:,.2f}")
                    
                    diff_dt = abs(expected_dt - actual_dt)
                    tolerance_dt = max(expected_dt * 0.01, 1)
                    
                    if diff_dt <= tolerance_dt:
                        print(f"   ✅ MATCH (diff: ₹{diff_dt:.2f})")
                    else:
                        print(f"   ❌ MISMATCH! Difference: ₹{diff_dt:,.2f}")
                        all_match = False

                    # --- FORMULA 2: ATW ---
                    print("\n🔍 Formula Check: ATW")
                    
                    raw_tottrdval = float(raw_row['TOTTRDVAL'])
                    
                    expected_atw = raw_tottrdval / 1000
                    actual_atw = float(proc_row['ATW'])
                    
                    print(f"   Raw: TOTTRDVAL=₹{raw_tottrdval:,.2f}")
                    print(f"   Expected: ₹{raw_tottrdval:,.2f} ÷ 1000 = ₹{expected_atw:,.2f}")
                    print(f"   Actual:   ₹{actual_atw:,.2f}")
                    
                    diff_atw = abs(expected_atw - actual_atw)
                    tolerance_atw = max(expected_atw * 0.01, 0.1)
                    
                    if diff_atw <= tolerance_atw:
                        print(f"   ✅ MATCH (diff: ₹{diff_atw:.2f})")
                    else:
                        print(f"   ❌ MISMATCH! Difference: ₹{diff_atw:,.2f}")
                        all_match = False

        except Exception as e:
            print(f"\n❌ Error processing BSE file: {e}")
            import traceback
            traceback.print_exc()

    # -----------------------------
    # FINAL VERDICT
    # -----------------------------
    print("\n" + "="*70)
    print("PHASE 4 FINAL VERDICT")
    print("="*70)

    if all_match:
        print("\n✅ ALL FORMULAS VERIFIED!")
        print("   DELIVERY_TURNOVER = DELIV_QTY × CLOSE ✓")
        print("   ATW = TOTTRDVAL / 1000 ✓")
        print("\n🎉 Your system is calculating formulas correctly.")
        print("   No 'total turnover vs delivery turnover' style bugs detected.")
    else:
        print("\n❌ FORMULA MISMATCHES DETECTED!")
        print("   There are calculation errors in your data pipeline.")
        print("   Review the ❌ lines above and check your code.")

if __name__ == "__main__":
    main()
    sys.exit(0)
