"""
AUDIT PHASE 1 - TEST 3: DISTRIBUTION SANITY CHECK
=================================================
This script checks for impossible values and suspicious patterns
that indicate data quality issues.

Instructions:
1. Place this file in your trading_dashboard/ folder
2. Run: python audit_test3.py
3. Copy the output and share with me

Expected Output: Should show 0 for all ❌ items
"""

import pandas as pd
import sys

def audit_distributions():
    try:
        df = pd.read_csv('data/combined_dashboard_live.csv')
        
        print(f"\n{'='*70}")
        print(f"PHASE 1: DISTRIBUTION SANITY CHECK")
        print(f"{'='*70}")
        print(f"\nTotal stocks in dataset: {len(df)}\n")
        
        # ============= TEST 1: IMPOSSIBLE VALUES =============
        print("1️⃣ IMPOSSIBLE VALUES CHECK")
        print("-" * 70)
        
        impossible_checks = [
            ("DELIV_PER > 100%", (df['DELIV_PER'] > 100).sum()),
            ("DELIV_PER < 0%", (df['DELIV_PER'] < 0).sum()),
            ("CLOSE <= 0", (df['CLOSE'] <= 0).sum()),
            ("TTL_TRD_QNTY < 0", (df['TTL_TRD_QNTY'] < 0).sum()),
            ("DELIV_QTY < 0", (df['DELIV_QTY'] < 0).sum()),
            ("Negative DELIVERY_TURNOVER", (df['DELIVERY_TURNOVER'] < 0).sum()),
            ("Negative ATW", (df['ATW'] < 0).sum()),
        ]
        
        all_good = True
        for check_name, count in impossible_checks:
            if count == 0:
                print(f"   ✅ {check_name}: {count}")
            else:
                print(f"   ❌ {check_name}: {count} stocks found!")
                all_good = False
        
        # ============= TEST 2: LOGICAL VIOLATIONS =============
        print(f"\n2️⃣ LOGICAL VIOLATIONS CHECK")
        print("-" * 70)
        
        # Delivered quantity should never exceed total traded
        exceed = (df['DELIV_QTY'] > df['TTL_TRD_QNTY']).sum()
        print(f"   DELIV_QTY > TTL_TRD_QNTY: {exceed} stocks", end="")
        if exceed == 0:
            print(" ✅")
        else:
            print(" ❌ (CRITICAL: More delivered than traded!)")
            all_good = False
        
        # Delivery percent should equal DELIV_QTY / TTL_TRD_QNTY * 100
        df['calculated_deliv_per'] = (df['DELIV_QTY'] / df['TTL_TRD_QNTY'] * 100).fillna(0)
        deliv_per_mismatch = (abs(df['DELIV_PER'] - df['calculated_deliv_per']) > 0.1).sum()
        print(f"   DELIV_PER calculation wrong: {deliv_per_mismatch} stocks", end="")
        if deliv_per_mismatch == 0:
            print(" ✅")
        else:
            print(" ❌")
            all_good = False
        
        # ============= TEST 3: REALISTIC RANGES =============
        print(f"\n3️⃣ REALISTIC RANGES CHECK")
        print("-" * 70)
        
        avg_close = df['CLOSE'].mean()
        print(f"   Average stock price: ₹{avg_close:,.2f}")
        
        avg_deliv_per = df['DELIV_PER'].mean()
        print(f"   Average delivery %: {avg_deliv_per:.2f}%")
        
        # ATW extremes
        atw_anomalies = (df['ATW'] > 1000000).sum()
        print(f"   ATW > ₹1 Crore: {atw_anomalies} stocks (check if realistic)")
        
        # ============= TEST 4: SIGNAL RATE CHECK =============
        print(f"\n4️⃣ SIGNAL RATE ANALYSIS")
        print("-" * 70)
        
        try:
            all_12_conditions = df['ALL_12_CONDITIONS'].sum()
            signal_rate = (all_12_conditions / len(df)) * 100
            
            print(f"   Stocks passed ALL 12 conditions: {all_12_conditions}")
            print(f"   Signal rate: {signal_rate:.3f}%")
            
            if signal_rate < 0.01:
                print(f"   ⚠️  Extremely low signal rate (< 0.01%)")
                print(f"       → System might be too strict")
            elif signal_rate > 5:
                print(f"   ⚠️  High signal rate (> 5%)")
                print(f"       → System might be too loose")
            else:
                print(f"   ✅ Signal rate is reasonable (0.01% - 5%)")
        except:
            print("   ⚠️  ALL_12_CONDITIONS column not found")
        
        # ============= TEST 5: PROGRESSIVE LOGIC =============
        print(f"\n5️⃣ PROGRESSIVE AVERAGES LOGIC CHECK")
        print("-" * 70)
        
        try:
            df_with_history = df.dropna(subset=['DELIV_PER_1W', 'DELIV_PER_1M', 'DELIV_PER_3M'])
            
            if len(df_with_history) > 0:
                print(f"   Stocks with complete 3-month history: {len(df_with_history)}")
                
                # Median relationships
                median_today = df_with_history['DELIV_PER'].median()
                median_1w = df_with_history['DELIV_PER_1W'].median()
                median_1m = df_with_history['DELIV_PER_1M'].median()
                median_3m = df_with_history['DELIV_PER_3M'].median()
                
                print(f"\n   Median DELIV_PER across time:")
                print(f"   Today:  {median_today:.2f}%")
                print(f"   1W avg: {median_1w:.2f}%")
                print(f"   1M avg: {median_1m:.2f}%")
                print(f"   3M avg: {median_3m:.2f}%")
                
                # These should generally decrease (recent > older)
                if median_today > median_1w > median_1m > median_3m:
                    print(f"   ✅ Progressives show correct trend (recent higher than older)")
                elif median_today >= median_1w >= median_1m >= median_3m:
                    print(f"   ✅ Progressives monotonic (acceptable)")
                else:
                    print(f"   ⚠️  Progressives show unexpected trend")
            else:
                print(f"   ⚠️  No stocks with complete 3-month history (data too new)")
        except Exception as e:
            print(f"   ⚠️  Error in progressive analysis: {e}")
        
        # ============= FINAL VERDICT =============
        print(f"\n{'='*70}")
        print("📋 DISTRIBUTION AUDIT VERDICT")
        print("-" * 70)
        
        if all_good:
            print("✅ All sanity checks passed!")
            print("   Data quality appears good.")
        else:
            print("❌ Issues detected!")
            print("   Please investigate the marked failures above.")
        
        print(f"\n{'='*70}\n")
        return all_good
        
    except FileNotFoundError:
        print(f"\n❌ ERROR: combined_dashboard_live.csv not found!")
        print("   Make sure you're running this from trading_dashboard/ folder")
        return False
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = audit_distributions()
    sys.exit(0 if result else 1)
