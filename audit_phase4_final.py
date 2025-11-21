"""
AUDIT PHASE 4 - RAW FORMULA VERIFICATION (FINAL)
================================================
This version properly handles:
- New ISO format column names (TckrSymb, ClsgPric, etc.)
- Separate bhav + delivery files
- Merges them like your pipeline does
- Verifies DELIVERY_TURNOVER and ATW formulas

How to run:
    python audit_phase4_final.py
"""

import pandas as pd
import os
import sys
from datetime import datetime

def find_latest_files(folder, pattern1, pattern2=None):
    """Find latest matching files in folder."""
    if not os.path.exists(folder):
        return None, None
    
    files = [f for f in os.listdir(folder) if f.endswith('.csv')]
    
    # Find bhav file
    bhav_files = [f for f in files if pattern1.lower() in f.lower() and 'bhav' in f.lower()]
    bhav_files.sort(reverse=True)
    bhav = os.path.join(folder, bhav_files[0]) if bhav_files else None
    
    # Find delivery file (if pattern2 provided)
    deliv = None
    if pattern2:
        deliv_files = [f for f in files if pattern2.lower() in f.lower()]
        deliv_files.sort(reverse=True)
        deliv = os.path.join(folder, deliv_files[0]) if deliv_files else None
    
    return bhav, deliv

def normalize_columns(df, exchange):
    """Normalize column names to standard format."""
    # New ISO format mapping
    col_map = {
        'TckrSymb': 'SYMBOL',
        'ClsgPric': 'CLOSE',
        'TtlTradgVal': 'TOTTRDVAL',
        'TtlTrfVal': 'TOTTRDVAL',
        'SctySrs': 'SERIES',
        'TtlTradgQty': 'TTL_TRD_QNTY',
    }
    
    df = df.rename(columns=col_map)
    return df

def main():
    print("\n" + "="*70)
    print("PHASE 4: RAW FORMULA VERIFICATION (FINAL)")
    print("="*70)

    # Find latest NSE bhav + delivery
    print("\n🔍 Searching for latest NSE files...")
    nse_bhav, nse_deliv = find_latest_files("data/nse_raw", "cm_bhav", "delivery")
    
    # Find latest BSE bhav
    print("🔍 Searching for latest BSE files...")
    bse_bhav, _ = find_latest_files("data/bse_raw", "bse_bhav")
    bse_deliv, _ = find_latest_files("data", "bse_delivery")
    
    if not nse_bhav and not bse_bhav:
        print("\n❌ No raw bhav files found!")
        sys.exit(1)
    
    print("\n✅ Found files:")
    if nse_bhav:
        print(f"   NSE bhav: {os.path.basename(nse_bhav)}")
    if nse_deliv:
        print(f"   NSE delivery: {os.path.basename(nse_deliv)}")
    if bse_bhav:
        print(f"   BSE bhav: {os.path.basename(bse_bhav)}")
    if bse_deliv:
        print(f"   BSE delivery: {os.path.basename(bse_deliv)}")

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
    # TEST NSE FORMULAS
    # =====================================
    if nse_bhav and nse_deliv:
        print("\n" + "="*70)
        print("NSE FORMULA AUDIT")
        print("="*70)

        try:
            # Load and normalize NSE bhav
            bhav = pd.read_csv(nse_bhav)
            bhav = normalize_columns(bhav, 'NSE')
            
            # Load and normalize NSE delivery
            deliv = pd.read_csv(nse_deliv)
            deliv = normalize_columns(deliv, 'NSE')
            
            print(f"\nNSE bhav: {len(bhav)} rows")
            print(f"NSE delivery: {len(deliv)} rows")
            
            # Check if delivery has DELIV_QTY
            if 'DELIV_QTY' not in deliv.columns:
                # Try alternate names
                for alt in ['DeliveryQuantity', 'QtyDlvrd', 'DELIVERY_QTY']:
                    if alt in deliv.columns:
                        deliv['DELIV_QTY'] = deliv[alt]
                        break
            
            if 'SYMBOL' not in deliv.columns and 'TckrSymb' in deliv.columns:
                deliv['SYMBOL'] = deliv['TckrSymb']
            
            # Merge bhav + delivery
            if 'SYMBOL' in bhav.columns and 'SYMBOL' in deliv.columns and 'DELIV_QTY' in deliv.columns:
                merged = bhav.merge(deliv[['SYMBOL', 'DELIV_QTY']], on='SYMBOL', how='left')
                merged['DELIV_QTY'] = merged['DELIV_QTY'].fillna(0)
                
                print(f"Merged: {len(merged)} rows")
                
                # Filter EQ series
                if 'SERIES' in merged.columns:
                    merged = merged[merged['SERIES'] == 'EQ']
                    print(f"After EQ filter: {len(merged)} rows")
                
                # Pick 3 test stocks
                test_symbols = []
                for sym in merged['SYMBOL'].sample(min(20, len(merged))):
                    if sym in processed['SYMBOL'].values:
                        test_symbols.append(sym)
                        if len(test_symbols) >= 3:
                            break
                
                if test_symbols:
                    print(f"\nTesting {len(test_symbols)} NSE stocks:\n")
                    
                    for i, symbol in enumerate(test_symbols, 1):
                        print(f"{'─'*70}")
                        print(f"NSE Stock #{i}: {symbol}")
                        print(f"{'─'*70}")
                        
                        raw = merged[merged['SYMBOL'] == symbol].iloc[0]
                        proc = processed[processed['SYMBOL'] == symbol].iloc[0]
                        
                        # Test DELIVERY_TURNOVER
                        print("\n🔍 DELIVERY_TURNOVER = DELIV_QTY × CLOSE")
                        
                        deliv_qty = float(raw['DELIV_QTY'])
                        close = float(raw['CLOSE'])
                        expected_dt = deliv_qty * close
                        actual_dt = float(proc['DELIVERY_TURNOVER'])
                        
                        print(f"   Raw: DELIV_QTY={deliv_qty:,.0f}, CLOSE=₹{close:.2f}")
                        print(f"   Expected: {deliv_qty:,.0f} × ₹{close:.2f} = ₹{expected_dt:,.2f}")
                        print(f"   Actual:   ₹{actual_dt:,.2f}")
                        
                        diff = abs(expected_dt - actual_dt)
                        tolerance = max(expected_dt * 0.01, 1)
                        
                        if diff <= tolerance:
                            print(f"   ✅ MATCH (diff: ₹{diff:.2f})")
                            tests_run += 1
                        else:
                            print(f"   ❌ MISMATCH! (diff: ₹{diff:,.2f})")
                            all_match = False
                            tests_run += 1
                        
                        # Test ATW
                        print("\n🔍 ATW = TOTTRDVAL / 1000")
                        
                        tottrdval = float(raw['TOTTRDVAL'])
                        expected_atw = tottrdval / 1000
                        actual_atw = float(proc['ATW'])
                        
                        print(f"   Raw: TOTTRDVAL=₹{tottrdval:,.2f}")
                        print(f"   Expected: ₹{tottrdval:,.2f} ÷ 1000 = ₹{expected_atw:,.2f}")
                        print(f"   Actual:   ₹{actual_atw:,.2f}")
                        
                        diff = abs(expected_atw - actual_atw)
                        tolerance = max(expected_atw * 0.01, 0.1)
                        
                        if diff <= tolerance:
                            print(f"   ✅ MATCH (diff: ₹{diff:.2f})\n")
                            tests_run += 1
                        else:
                            print(f"   ❌ MISMATCH! (diff: ₹{diff:,.2f})\n")
                            all_match = False
                            tests_run += 1
                else:
                    print("⚠️  No common symbols found between raw and processed NSE")
            else:
                print("⚠️  Could not merge NSE bhav + delivery (missing columns)")
                
        except Exception as e:
            print(f"\n❌ Error in NSE audit: {e}")
            import traceback
            traceback.print_exc()

    # =====================================
    # TEST BSE FORMULAS
    # =====================================
    if bse_bhav and bse_deliv:
        print("\n" + "="*70)
        print("BSE FORMULA AUDIT")
        print("="*70)

        try:
            # Load and normalize BSE bhav
            bhav = pd.read_csv(bse_bhav)
            bhav = normalize_columns(bhav, 'BSE')
            
            # Load and normalize BSE delivery
            deliv = pd.read_csv(bse_deliv)
            deliv = normalize_columns(deliv, 'BSE')
            
            print(f"\nBSE bhav: {len(bhav)} rows")
            print(f"BSE delivery: {len(deliv)} rows")
            
            # Try to merge
            if 'SYMBOL' in bhav.columns and 'SYMBOL' in deliv.columns and 'DELIV_QTY' in deliv.columns:
                merged = bhav.merge(deliv[['SYMBOL', 'DELIV_QTY']], on='SYMBOL', how='left')
                merged['DELIV_QTY'] = merged['DELIV_QTY'].fillna(0)
                
                print(f"Merged: {len(merged)} rows")
                
                # Pick 3 test stocks
                test_symbols = []
                for sym in merged['SYMBOL'].sample(min(20, len(merged))):
                    if sym in processed['SYMBOL'].values:
                        test_symbols.append(sym)
                        if len(test_symbols) >= 3:
                            break
                
                if test_symbols:
                    print(f"\nTesting {len(test_symbols)} BSE stocks:\n")
                    
                    for i, symbol in enumerate(test_symbols, 1):
                        print(f"{'─'*70}")
                        print(f"BSE Stock #{i}: {symbol}")
                        print(f"{'─'*70}")
                        
                        raw = merged[merged['SYMBOL'] == symbol].iloc[0]
                        proc = processed[processed['SYMBOL'] == symbol].iloc[0]
                        
                        # Test formulas (same as NSE)
                        print("\n🔍 DELIVERY_TURNOVER = DELIV_QTY × CLOSE")
                        
                        deliv_qty = float(raw['DELIV_QTY'])
                        close = float(raw['CLOSE'])
                        expected_dt = deliv_qty * close
                        actual_dt = float(proc['DELIVERY_TURNOVER'])
                        
                        print(f"   Expected: ₹{expected_dt:,.2f}")
                        print(f"   Actual:   ₹{actual_dt:,.2f}")
                        
                        diff = abs(expected_dt - actual_dt)
                        if diff <= max(expected_dt * 0.01, 1):
                            print(f"   ✅ MATCH")
                            tests_run += 1
                        else:
                            print(f"   ❌ MISMATCH (diff: ₹{diff:,.2f})")
                            all_match = False
                            tests_run += 1
                        
                        print("\n🔍 ATW = TOTTRDVAL / 1000")
                        
                        tottrdval = float(raw['TOTTRDVAL'])
                        expected_atw = tottrdval / 1000
                        actual_atw = float(proc['ATW'])
                        
                        print(f"   Expected: ₹{expected_atw:,.2f}")
                        print(f"   Actual:   ₹{actual_atw:,.2f}")
                        
                        diff = abs(expected_atw - actual_atw)
                        if diff <= max(expected_atw * 0.01, 0.1):
                            print(f"   ✅ MATCH\n")
                            tests_run += 1
                        else:
                            print(f"   ❌ MISMATCH (diff: ₹{diff:,.2f})\n")
                            all_match = False
                            tests_run += 1
                else:
                    print("⚠️  No common symbols found between raw and processed BSE")
            else:
                print("⚠️  Could not merge BSE bhav + delivery")
                
        except Exception as e:
            print(f"\n❌ Error in BSE audit: {e}")
            import traceback
            traceback.print_exc()

    # =====================================
    # FINAL VERDICT
    # =====================================
    print("\n" + "="*70)
    print("PHASE 4 FINAL VERDICT")
    print("="*70)

    if tests_run == 0:
        print("\n⚠️  NO TESTS RUN - Could not process raw files")
    elif all_match:
        print(f"\n✅ ALL {tests_run} FORMULA CHECKS PASSED!")
        print("\n   DELIVERY_TURNOVER = DELIV_QTY × CLOSE ✓")
        print("   ATW = TOTTRDVAL / 1000 ✓")
        print("\n🎉 Your pipeline calculates formulas CORRECTLY!")
        print("   No 'total turnover' bugs detected.")
    else:
        print(f"\n❌ SOME TESTS FAILED (out of {tests_run})")
        print("   Formula bugs detected in pipeline!")

if __name__ == "__main__":
    main()
    sys.exit(0)
