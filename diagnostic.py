import pandas as pd
import sys
import os

def run_diagnostics():
    print("======================================================================")
    print("PIPELINE DIAGNOSTICS & SANITY CHECK")
    print("======================================================================")
    
    file_path = "data/combined_dashboard_live.csv"
    if not os.path.exists(file_path):
        print(f"[FAIL] ERROR: {file_path} not found!")
        sys.exit(1)
        
    df = pd.read_csv(file_path)
    print(f"Total rows in output: {len(df)}")
    
    # Check 1: Minimum total rows
    if len(df) < 3000:
        print(f"[FAIL] ERROR: Total rows ({len(df)}) abnormally low. Expected > 3000.")
        sys.exit(1)
        
    # Check 2: Minimum NSE and BSE rows
    exch_counts = df["EXCHANGE"].value_counts()
    nse_count = exch_counts.get("NSE", 0)
    bse_count = exch_counts.get("BSE", 0)
    
    print(f"NSE Stocks: {nse_count}")
    print(f"BSE Stocks: {bse_count}")
    
    if nse_count < 1500:
        print(f"[FAIL] ERROR: NSE row count ({nse_count}) abnormally low. Expected > 1500.")
        sys.exit(1)
        
    if bse_count < 1500:
        print(f"[FAIL] ERROR: BSE row count ({bse_count}) abnormally low. Expected > 1500.")
        sys.exit(1)
        
    # Check 3: Essential columns exist
    essential_cols = ["SYMBOL", "EXCHANGE", "CLOSE", "DELIV_PER", "ATW", "EVER_100_DELIV", 
                     "DELIV_PER_1W", "DELIV_PER_1M", "ATW_1W", "ATW_1M"]
    missing = [c for c in essential_cols if c not in df.columns]
    
    if missing:
        print(f"[FAIL] ERROR: Missing essential columns: {missing}")
        sys.exit(1)
        
    # Check 4: Date check (should be recent)
    if "DATE" in df.columns:
        # Check if max date is not completely empty
        if df["DATE"].isna().all():
            print("[FAIL] ERROR: All DATE values are missing!")
            sys.exit(1)
            
    print("[PASS] Diagnostics passed! Data looks sane.")
    print("======================================================================\n")

if __name__ == "__main__":
    run_diagnostics()
