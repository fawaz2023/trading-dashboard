#!/usr/bin/env python3
"""
Download Verification Script - Trading Dashboard
Confirms NSE Bhav, NSE Delivery, BSE Bhav, and BSE Delivery files exist and are valid
"""

import os
import glob
import pandas as pd
from datetime import datetime, timedelta

print("=" * 70)
print("DOWNLOAD VERIFICATION - NSE + BSE DATA")
print("=" * 70)
print(f"Run time: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
print("=" * 70)

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_FOLDER = "data"
NSE_RAW_FOLDER = os.path.join(DATA_FOLDER, "nse_raw")
BSE_RAW_FOLDER = os.path.join(DATA_FOLDER, "bse_raw")

# ============================================================================
# VERIFICATION FUNCTIONS
# ============================================================================

def verify_nse_bhav():
    """Verify NSE Bhavcopy files (.zip or .csv)"""
    print("\n📊 NSE BHAVCOPY VERIFICATION")
    print("-" * 70)

    # Check folder exists
    if not os.path.exists(NSE_RAW_FOLDER):
        print(f"❌ Folder not found: {NSE_RAW_FOLDER}")
        return False

    # Find all NSE bhav files (both .zip and extracted .csv)
    zip_files = glob.glob(os.path.join(NSE_RAW_FOLDER, "cm*.zip"))
    csv_files = glob.glob(os.path.join(NSE_RAW_FOLDER, "cm*.csv"))

    print(f"   Found: {len(zip_files)} ZIP files")
    print(f"   Found: {len(csv_files)} CSV files (extracted)")

    if len(csv_files) == 0:
        print("   ❌ No NSE bhavcopy files found!")
        return False

    # Test load one file
    try:
        sample_file = csv_files[-1]  # Most recent
        df = pd.read_csv(sample_file)
        print(f"   ✅ Sample file loaded: {os.path.basename(sample_file)}")
        print(f"   ✅ Records: {len(df):,}")
        print(f"   ✅ Columns: {list(df.columns)[:5]}...")
        return True
    except Exception as e:
        print(f"   ❌ Error loading sample: {e}")
    
