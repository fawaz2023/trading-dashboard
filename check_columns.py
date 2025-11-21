"""
COLUMN DISCOVERY SCRIPT
=======================
This script shows all columns in your data file so we can fix the audit scripts.

Instructions:
1. Place this file in your trading_dashboard/ folder
2. Run: python check_columns.py
3. Share the output with me
"""

import pandas as pd

try:
    df = pd.read_csv('data/combined_dashboard_live.csv')
    
    print("\n" + "="*70)
    print("COLUMN DISCOVERY - combined_dashboard_live.csv")
    print("="*70)
    
    print(f"\nTotal rows: {len(df)}")
    print(f"Total columns: {len(df.columns)}\n")
    
    print("="*70)
    print("ALL COLUMN NAMES:")
    print("="*70)
    
    for i, col in enumerate(df.columns, 1):
        print(f"{i:3d}. {col}")
    
    print("\n" + "="*70)
    print("SAMPLE DATA (first row):")
    print("="*70)
    
    if len(df) > 0:
        first_row = df.iloc[0]
        for col in df.columns:
            value = first_row[col]
            if pd.isna(value):
                print(f"{col}: [EMPTY/NaN]")
            else:
                print(f"{col}: {value}")
    
    print("\n" + "="*70)
    print("KEY COLUMNS CHECK:")
    print("="*70)
    
    # Check for common column name variations
    delivery_cols = [col for col in df.columns if 'DELIV' in col.upper()]
    print(f"\nDelivery-related columns ({len(delivery_cols)}):")
    for col in delivery_cols:
        print(f"  - {col}")
    
    turnover_cols = [col for col in df.columns if 'TURN' in col.upper() or 'TRD' in col.upper()]
    print(f"\nTurnover/Trade-related columns ({len(turnover_cols)}):")
    for col in turnover_cols:
        print(f"  - {col}")
    
    condition_cols = [col for col in df.columns if 'CONDITION' in col.upper() or 'ALL_12' in col.upper()]
    print(f"\nCondition-related columns ({len(condition_cols)}):")
    for col in condition_cols:
        print(f"  - {col}")
    
    print("\n" + "="*70)
    print("SHARE THIS OUTPUT WITH YOUR ADVISOR")
    print("="*70 + "\n")
    
except FileNotFoundError:
    print("\n❌ ERROR: combined_dashboard_live.csv not found!")
    print("   Make sure you're running this from trading_dashboard/ folder")
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
