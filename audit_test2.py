"""
AUDIT PHASE 1 - TEST 2: RAW FILE MATCH VERIFICATION
===================================================
This script compares your processed data against the raw NSE/BSE files
to catch corruption during merge/processing.

Instructions:
1. Place this file in your trading_dashboard/ folder
2. Run: python audit_test2.py
3. Copy the output and share with me

Expected Output: All 3 stocks should show matching values
"""

import pandas as pd
import os
from datetime import datetime, timedelta
import sys

def get_latest_nse_file():
    """Find the latest NSE bhav file"""
    nse_dir = 'data/nse_raw'
    if not os.path.exists(nse_dir):
        return None
    
    files = [f for f in os.listdir(nse_dir) if f.endswith('bhav.csv')]
    if not files:
        return None
    
    files.sort(reverse=True)
    return os.path.join(nse_dir, files[0])

def get_latest_bse_file():
    """Find the latest BSE bhav file"""
    bse_dir = 'data/bse_raw'
    if not os.path.exists(bse_dir):
        return None
    
    files = [f for f in os.listdir(bse_dir) if f.endswith('bhav.csv')]
    if not files:
        return None
    
    files.sort(reverse=True)
    return os.path.join(bse_dir, files[0])

def audit_raw_match():
    try:
        print(f"\n{'='*70}")
        print(f"PHASE 1: RAW FILE MATCH VERIFICATION")
        print(f"{'='*70}\n")
        
        # Load processed data
        processed = pd.read_csv('data/combined_dashboard_live.csv')
        print(f"✓ Loaded processed data: {len(processed)} stocks\n")
        
        # ============= TEST WITH NSE DATA =============
        nse_file = get_latest_nse_file()
        if nse_file:
            print(f"{'='*70}")
            print(f"NSE DATA VERIFICATION")
            print(f"{'='*70}")
            print(f"File: {os.path.basename(nse_file)}\n")
            
            try:
                raw_nse = pd.read_csv(nse_file)
                
                # Get 3 random NSE stocks from raw file
                nse_stocks = raw_nse[raw_nse['SERIES'] == 'EQ']['SYMBOL'].unique()
                test_stocks_nse = list(nse_stocks)[:3] if len(nse_stocks) >= 3 else list(nse_stocks)
                
                all_nse_match = True
                
                for i, symbol in enumerate(test_stocks_nse, 1):
                    print(f"\n{'─'*70}")
                    print(f"Stock #{i}: {symbol}")
                    print(f"{'─'*70}")
                    
                    raw_row = raw_nse[raw_nse['SYMBOL'] == symbol].iloc[0]
                    proc_rows = processed[processed['SYMBOL'] == symbol]
                    
                    if len(proc_rows) == 0:
                        print(f"❌ NOT FOUND in processed data!")
                        all_nse_match = False
                        continue
                    
                    proc_row = proc_rows.iloc[0]
                    
                    # Compare key fields
                    comparisons = [
                        ('CLOSE', 'Stock Price'),
                        ('DELIV_PER', 'Delivery %'),
                        ('TTL_TRD_QNTY', 'Total Quantity'),
                    ]
                    
                    stock_match = True
                    for col, label in comparisons:
                        raw_val = float(raw_row[col])
                        proc_val = float(proc_row[col])
                        
                        # Allow small tolerance for floating point
                        tolerance = max(abs(raw_val) * 0.01, 0.1)
                        
                        if abs(raw_val - proc_val) <= tolerance:
                            print(f"   ✅ {label}: {raw_val:.2f} = {proc_val:.2f}")
                        else:
                            print(f"   ❌ {label}: Raw={raw_val:.2f}, Processed={proc_val:.2f} (Diff: {abs(raw_val - proc_val):.2f})")
                            stock_match = False
                            all_nse_match = False
                    
                    if stock_match:
                        print(f"   → Stock verified ✅")
                    else:
                        print(f"   → Data corruption detected ❌")
                
                print(f"\n{'─'*70}")
                if all_nse_match:
                    print("✅ NSE: All tested stocks match raw data!")
                else:
                    print("❌ NSE: Mismatches found!")
                    
            except Exception as e:
                print(f"⚠️  Error reading NSE file: {e}")
        else:
            print("⚠️  No NSE raw files found in data/nse_raw/")
        
        # ============= TEST WITH BSE DATA =============
        bse_file = get_latest_bse_file()
        if bse_file:
            print(f"\n{'='*70}")
            print(f"BSE DATA VERIFICATION")
            print(f"{'='*70}")
            print(f"File: {os.path.basename(bse_file)}\n")
            
            try:
                raw_bse = pd.read_csv(bse_file)
                
                # Get 3 random BSE stocks from raw file
                bse_stocks = raw_bse['SYMBOL'].unique()
                test_stocks_bse = list(bse_stocks)[:3] if len(bse_stocks) >= 3 else list(bse_stocks)
                
                all_bse_match = True
                
                for i, symbol in enumerate(test_stocks_bse, 1):
                    print(f"\n{'─'*70}")
                    print(f"Stock #{i}: {symbol}")
                    print(f"{'─'*70}")
                    
                    raw_row = raw_bse[raw_bse['SYMBOL'] == symbol].iloc[0]
                    proc_rows = processed[processed['SYMBOL'] == symbol]
                    
                    if len(proc_rows) == 0:
                        print(f"⚠️  Not in processed (may be filtered out)")
                        continue
                    
                    proc_row = proc_rows.iloc[0]
                    
                    # Compare key fields
                    comparisons = [
                        ('CLOSE', 'Stock Price'),
                        ('DELIV_PER', 'Delivery %'),
                        ('TTL_TRD_QNTY', 'Total Quantity'),
                    ]
                    
                    stock_match = True
                    for col, label in comparisons:
                        raw_val = float(raw_row[col])
                        proc_val = float(proc_row[col])
                        
                        tolerance = max(abs(raw_val) * 0.01, 0.1)
                        
                        if abs(raw_val - proc_val) <= tolerance:
                            print(f"   ✅ {label}: {raw_val:.2f} = {proc_val:.2f}")
                        else:
                            print(f"   ❌ {label}: Raw={raw_val:.2f}, Processed={proc_val:.2f}")
                            stock_match = False
                            all_bse_match = False
                    
                    if stock_match:
                        print(f"   → Stock verified ✅")
                    else:
                        print(f"   → Data corruption detected ❌")
                
                print(f"\n{'─'*70}")
                if all_bse_match:
                    print("✅ BSE: All tested stocks match raw data!")
                else:
                    print("⚠️  BSE: Some mismatches found (may be intentional filtering)")
                    
            except Exception as e:
                print(f"⚠️  Error reading BSE file: {e}")
        else:
            print("⚠️  No BSE raw files found in data/bse_raw/")
        
        print(f"\n{'='*70}")
        print("TEST COMPLETE - Share this output with your advisor")
        print(f"{'='*70}\n")
        
        return True
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: {e}")
        print("   Missing files:")
        print("   - data/combined_dashboard_live.csv")
        print("   - data/nse_raw/ or data/bse_raw/ folders")
        return False
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = audit_raw_match()
    sys.exit(0 if result else 1)
