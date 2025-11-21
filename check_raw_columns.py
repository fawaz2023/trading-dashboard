"""Check what columns are actually in your raw files"""
import pandas as pd
import os

print("\n" + "="*70)
print("RAW FILE COLUMN INSPECTOR")
print("="*70)

files_to_check = [
    "data/nse_raw/cm_bhav_latest_normalized.csv",
    "data/nse_raw/nse_delivery_20251119.csv",
    "data/bse_raw/bse_bhav_20251119.csv",
]

for filepath in files_to_check:
    if os.path.exists(filepath):
        print(f"\n{'='*70}")
        print(f"File: {os.path.basename(filepath)}")
        print(f"{'='*70}")
        
        try:
            df = pd.read_csv(filepath, nrows=1)
            print(f"\nTotal columns: {len(df.columns)}")
            print("\nAll column names:")
            for i, col in enumerate(df.columns, 1):
                print(f"  {i:2d}. {col}")
            
            # Show first row values
            print("\nFirst row sample:")
            for col in df.columns[:15]:  # First 15 columns
                print(f"  {col}: {df[col].iloc[0]}")
                
        except Exception as e:
            print(f"❌ Error reading: {e}")
    else:
        print(f"\n⚠️  File not found: {filepath}")

print("\n" + "="*70)
