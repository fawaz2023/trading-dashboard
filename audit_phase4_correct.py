"""
AUDIT PHASE 4 - RAW FORMULA VERIFICATION (CORRECT COLUMNS)
===========================================================
Uses actual column names from your files to verify formulas.

Formulas:
- DELIVERY_TURNOVER = DELIV_QTY × CLOSE
- ATW = TURNOVER_LACS × 100000 / 1000 = TURNOVER_LACS × 100

How to run:
    python audit_phase4_correct.py
"""

import pandas as pd
import sys

def main():
    print("\n" + "="*70)
    print("PHASE 4: RAW FORMULA VERIFICATION (CORRECT)")
    print("="*70)

    # Load processed data
    try:
        processed = pd.read_csv("data/combined_dashboard_live.csv")
        print(f"\n✅ Loaded processed data: {len(processed)} rows")
    except:
        print("\n❌ Could not load combined_dashboard_live.csv")
        sys.exit(1)

    all_match = True
    tests_run = 0

    # =====================================
    # NSE FORMULA VERIFICATION
    # =====================================
    print("\n" + "="*70)
    print("NSE RAW FORMULA AUDIT")
    print("="*70)

    try:
        # Load NSE delivery file (this has the raw data!)
        raw_nse = pd.read_csv("data/nse_raw/nse_delivery_20251119.csv")
        
        # Clean column names (remove leading/trailing spaces)
        raw_nse.columns = raw_nse.columns.str.strip()
        
        print(f"\n✅ Loaded NSE raw delivery file: {len(raw_nse)} rows")
        print(f"Columns: {', '.join(raw_nse.columns[:8])}")
        
        # Filter for EQ series only
        if 'SERIES' in raw_nse.columns:
            raw_nse = raw_nse[raw_nse['SERIES'] == 'EQ']
            print(f"After EQ filter: {len(raw_nse)} rows")
        
        # Pick 3 random stocks that exist in processed data
        test_symbols = []
        for sym in raw_nse['SYMBOL'].sample(min(20, len(raw_nse))):
            if sym in processed['SYMBOL'].values:
                test_symbols.append(sym)
                if len(test_symbols) >= 3:
                    break
        
        if not test_symbols:
            print("⚠️  No common symbols found")
        else:
            print(f"\n🎯 Testing {len(test_symbols)} NSE stocks:\n")
            
            for i, symbol in enumerate(test_symbols, 1):
                print(f"{'─'*70}")
                print(f"NSE Stock #{i}: {symbol}")
                print(f"{'─'*70}")
                
                raw = raw_nse[raw_nse['SYMBOL'] == symbol].iloc[0]
                proc = processed[processed['SYMBOL'] == symbol].iloc[0]
                
                # ===== TEST 1: DELIVERY_TURNOVER =====
                print("\n🔍 Formula 1: DELIVERY_TURNOVER = DELIV_QTY × CLOSE")
                
                deliv_qty = float(raw['DELIV_QTY'])
                close = float(raw['CLOSE_PRICE'])
                
                expected_dt = deliv_qty * close
                actual_dt = float(proc['DELIVERY_TURNOVER'])
                
                print(f"   Raw data:")
                print(f"      DELIV_QTY = {deliv_qty:,.0f} shares")
                print(f"      CLOSE_PRICE = ₹{close:.2f}")
                print(f"   Expected: {deliv_qty:,.0f} × ₹{close:.2f} = ₹{expected_dt:,.2f}")
                print(f"   Actual:   ₹{actual_dt:,.2f}")
                
                diff_dt = abs(expected_dt - actual_dt)
                tolerance_dt = max(expected_dt * 0.01, 1)  # 1% tolerance
                
                if diff_dt <= tolerance_dt:
                    print(f"   ✅ MATCH (diff: ₹{diff_dt:.2f})")
                    tests_run += 1
                else:
                    print(f"   ❌ MISMATCH! (diff: ₹{diff_dt:,.2f})")
                    print(f"   ⚠️  This could indicate a formula bug!")
                    all_match = False
                    tests_run += 1
                
                # ===== TEST 2: ATW =====
                print("\n🔍 Formula 2: ATW = (TURNOVER in Lakhs × 100,000) / 1000")
                
                turnover_lacs = float(raw['TURNOVER_LACS'])
                ttl_trd_qnty = float(raw['TTL_TRD_QNTY'])
                
                # TURNOVER_LACS is in lakhs, so multiply by 100,000 to get rupees
                # Then divide by 1000 to get ATW
                # Net: TURNOVER_LACS × 100
                expected_atw = turnover_lacs * 100
                actual_atw = float(proc['ATW'])
                
                print(f"   Raw data:")
                print(f"      TURNOVER_LACS = {turnover_lacs:.2f} L")
                print(f"      TTL_TRD_QNTY = {ttl_trd_qnty:,.0f} shares")
                print(f"   Expected: {turnover_lacs:.2f} × 100 = ₹{expected_atw:,.2f}")
                print(f"   Actual:   ₹{actual_atw:,.2f}")
                
                diff_atw = abs(expected_atw - actual_atw)
                tolerance_atw = max(expected_atw * 0.01, 0.1)
                
                if diff_atw <= tolerance_atw:
                    print(f"   ✅ MATCH (diff: ₹{diff_atw:.2f})\n")
                    tests_run += 1
                else:
                    print(f"   ❌ MISMATCH! (diff: ₹{diff_atw:,.2f})")
                    print(f"   ⚠️  ATW formula may be incorrect!\n")
                    all_match = False
                    tests_run += 1
    
    except FileNotFoundError as e:
        print(f"\n❌ File not found: {e}")
    except Exception as e:
        print(f"\n❌ Error in NSE audit: {e}")
        import traceback
        traceback.print_exc()

    # =====================================
    # FINAL VERDICT
    # =====================================
    print("\n" + "="*70)
    print("PHASE 4 FINAL VERDICT")
    print("="*70)

    if tests_run == 0:
        print("\n⚠️  NO TESTS RUN")
        print("   Could not process raw files or find matching symbols.")
    elif all_match:
        print(f"\n✅ ALL {tests_run} FORMULA CHECKS PASSED!")
        print("\n   Formula 1: DELIVERY_TURNOVER = DELIV_QTY × CLOSE ✓")
        print("   Formula 2: ATW = TURNOVER_LACS × 100 ✓")
        print("\n🎉 YOUR PIPELINE IS CALCULATING FORMULAS CORRECTLY!")
        print("   No 'total turnover vs delivery turnover' bugs detected.")
        print("\n💪 You can trust your system's calculations.")
    else:
        print(f"\n❌ FORMULA MISMATCHES DETECTED! ({tests_run} tests run)")
        print("\n   There are calculation errors in your pipeline.")
        print("   Review the ❌ lines above to see which formulas are wrong.")
        print("\n   🚨 DO NOT USE FOR REAL TRADING until bugs are fixed!")

if __name__ == "__main__":
    main()
    sys.exit(0)
