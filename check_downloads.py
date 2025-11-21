#!/usr/bin/env python3
# Trading Dashboard - Download Verification Script
# Copy this into Notepad and save as: check_downloads.py

import os
import glob

print("=" * 70)
print("TRADING DASHBOARD - DOWNLOAD VERIFICATION")
print("=" * 70)
print()

# Folder paths
DATA_FOLDER = "data"
NSE_RAW = os.path.join(DATA_FOLDER, "nse_raw")
BSE_RAW = os.path.join(DATA_FOLDER, "bse_raw")

# ============================================================================
# 1. NSE BHAVCOPY CHECK
# ============================================================================
print("1. NSE BHAVCOPY (data/nse_raw/)")
print("-" * 70)

if os.path.exists(NSE_RAW):
    nse_bhav_files = glob.glob(os.path.join(NSE_RAW, "cm*.csv"))
    print(f"   Found: {len(nse_bhav_files)} files")
    
    if len(nse_bhav_files) > 0:
        print(f"   Latest: {os.path.basename(nse_bhav_files[-1])}")
        print(f"   Status: OK")
    else:
        print(f"   Status: NO FILES FOUND")
else:
    print(f"   Status: FOLDER NOT FOUND")

print()

# ============================================================================
# 2. NSE DELIVERY CHECK
# ============================================================================
print("2. NSE DELIVERY (data/nse_raw/)")
print("-" * 70)

if os.path.exists(NSE_RAW):
    nse_deliv_csv = glob.glob(os.path.join(NSE_RAW, "*deliv*.csv"))
    nse_deliv_dat = glob.glob(os.path.join(NSE_RAW, "*.DAT"))
    nse_deliv_files = nse_deliv_csv + nse_deliv_dat
    
    print(f"   Found: {len(nse_deliv_files)} files")
    
    if len(nse_deliv_files) > 0:
        print(f"   Latest: {os.path.basename(nse_deliv_files[-1])}")
        print(f"   Status: OK")
    else:
        print(f"   Status: NO SEPARATE DELIVERY FILES")
        print(f"   Note: Delivery data might be in bhav files")
else:
    print(f"   Status: FOLDER NOT FOUND")

print()

# ============================================================================
# 3. BSE BHAVCOPY CHECK
# ============================================================================
print("3. BSE BHAVCOPY (data/bse_raw/)")
print("-" * 70)

if os.path.exists(BSE_RAW):
    bse_bhav_files = glob.glob(os.path.join(BSE_RAW, "bse_bhav_*.csv"))
    print(f"   Found: {len(bse_bhav_files)} files")
    
    if len(bse_bhav_files) > 0:
        print(f"   Latest: {os.path.basename(bse_bhav_files[-1])}")
        print(f"   Status: OK")
    else:
        print(f"   Status: NO FILES FOUND")
else:
    print(f"   Status: FOLDER NOT FOUND")

print()

# ============================================================================
# 4. BSE DELIVERY CHECK (in data folder root)
# ============================================================================
print("4. BSE DELIVERY (data/ - root folder)")
print("-" * 70)

bse_delivery_files = glob.glob(os.path.join(DATA_FOLDER, "bse_delivery_*.csv"))
print(f"   Found: {len(bse_delivery_files)} files")

if len(bse_delivery_files) > 0:
    print(f"   Latest: {os.path.basename(bse_delivery_files[-1])}")
    print(f"   Status: OK")
else:
    print(f"   Status: NO FILES FOUND")

print()

# ============================================================================
# SUMMARY
# ============================================================================
print("=" * 70)
print("SUMMARY")
print("=" * 70)

try:
    nse_bhav_count = len(glob.glob(os.path.join(NSE_RAW, "cm*.csv")))
    nse_deliv_count = len(glob.glob(os.path.join(NSE_RAW, "*deliv*.csv"))) + len(glob.glob(os.path.join(NSE_RAW, "*.DAT")))
    bse_bhav_count = len(glob.glob(os.path.join(BSE_RAW, "bse_bhav_*.csv")))
    bse_deliv_count = len(bse_delivery_files)
    
    print(f"NSE Bhavcopy:    {nse_bhav_count:4d} files")
    print(f"NSE Delivery:    {nse_deliv_count:4d} files")
    print(f"BSE Bhavcopy:    {bse_bhav_count:4d} files")
    print(f"BSE Delivery:    {bse_deliv_count:4d} files")
    print()
    
    if nse_bhav_count > 0 and bse_bhav_count > 0 and bse_deliv_count > 0:
        print("OVERALL STATUS: ALL DOWNLOADS PRESENT")
        print()
        print("Your data is complete!")
        print("NSE and BSE files are downloading correctly.")
    elif nse_bhav_count > 0 or bse_bhav_count > 0:
        print("OVERALL STATUS: PARTIAL DOWNLOADS")
        print()
        print("Some data is present but not complete.")
        print("Run: python auto_update_smart.py")
    else:
        print("OVERALL STATUS: NO DATA FOUND")
        print()
        print("No download files detected.")
        print("Run: python auto_update_smart.py")
        
except Exception as e:
    print(f"Error: {e}")

print("=" * 70)
print()
input("Press Enter to exit...")
