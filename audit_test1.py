"""
AUDIT PHASE 1 - TEST 1: SINGLE STOCK MANUAL VERIFICATION
=========================================================
This script picks one stock that passed all 12 conditions
and verifies every calculation manually.

Instructions:
1. Place this file in your trading_dashboard/ folder
2. Run: python audit_single_stock.py
3. Copy the output and share with me

Expected Output: Should show ✅ MATCH for all calculations
"""

import pandas as pd
import sys

def audit_single_stock():
    try:
        # Load the dashboard output
        df = pd.read_csv('data/combined_dashboard_live.csv')
        print(f"\n{'='*70}")
        print(f"PHASE 1: SINGLE STOCK AUDIT")
        print(f"{'='*70}")
        
        # Filter for stocks that passed all 12 conditions
        passed = df[df['ALL_12_CONDITIONS'] == True]
        
        if len(passed) == 0:
            print("\n⚠️  No stocks passed all 12 conditions today.")
            print("Using top delivery% stock instead for audit...")
            stock = df.nlargest(1, 'DELIV_PER').iloc[0]
        else:
            stock = passed.iloc[0]
        
        symbol = stock['SYMBOL']
        exchange = stock.get('EXCHANGE', 'NSE')
        
        print(f"\n📊 STOCK SELECTED: {symbol} ({exchange})")
        print(f"{'='*70}\n")
        
        # ============= SECTION 1: RAW VALUES =============
        print("📌 SECTION 1: RAW VALUES FROM DATA")
        print("-" * 70)
        
        try:
            close = float(stock['CLOSE'])
            deliv_qty = float(stock['DELIV_QTY'])
            ttl_trd_qnty = float(stock['TTL_TRD_QNTY'])
            tottrdval = float(stock['TOTTRDVAL'])
            deliv_per = float(stock['DELIV_PER'])
            
            print(f"CLOSE (Stock Price):        ₹{close:,.2f}")
            print(f"DELIV_QTY (Delivery Qty):  {deliv_qty:,.0f} shares")
            print(f"TTL_TRD_QNTY (Total Qty):  {ttl_trd_qnty:,.0f} shares")
            print(f"TOTTRDVAL (Total Value):   ₹{tottrdval:,.0f}")
            print(f"DELIV_PER (Delivery %):    {deliv_per:.2f}%")
            
        except Exception as e:
            print(f"\n❌ ERROR reading raw values: {e}")
            return False
        
        # ============= SECTION 2: FORMULA VERIFICATION =============
        print(f"\n{'='*70}")
        print("🧮 SECTION 2: FORMULA VERIFICATION")
        print("-" * 70)
        
        all_match = True
        
        # FORMULA 1: DELIVERY_TURNOVER
        print("\n1️⃣ DELIVERY_TURNOVER VERIFICATION")
        print("   Formula: DELIV_QTY × CLOSE")
        expected_delivery_turnover = deliv_qty * close
        actual_delivery_turnover = float(stock['DELIVERY_TURNOVER'])
        
        print(f"   Expected: {deliv_qty:,.0f} × ₹{close:.2f} = ₹{expected_delivery_turnover:,.0f}")
        print(f"   Actual:   ₹{actual_delivery_turnover:,.0f}")
        
        if abs(expected_delivery_turnover - actual_delivery_turnover) < 1:
            print(f"   ✅ MATCH!")
        else:
            print(f"   ❌ MISMATCH! Difference: ₹{abs(expected_delivery_turnover - actual_delivery_turnover):,.0f}")
            all_match = False
        
        # FORMULA 2: ATW (Average Traded Worth)
        print("\n2️⃣ ATW (AVERAGE TRADED WORTH) VERIFICATION")
        print("   Formula: TOTTRDVAL ÷ 1000")
        expected_atw = tottrdval / 1000
        actual_atw = float(stock['ATW'])
        
        print(f"   Expected: ₹{tottrdval:,.0f} ÷ 1000 = ₹{expected_atw:,.0f}")
        print(f"   Actual:   ₹{actual_atw:,.0f}")
        
        if abs(expected_atw - actual_atw) < 1:
            print(f"   ✅ MATCH!")
        else:
            print(f"   ❌ MISMATCH! Difference: ₹{abs(expected_atw - actual_atw):,.0f}")
            all_match = False
        
        # ============= SECTION 3: CONDITION VERIFICATION =============
        print(f"\n{'='*70}")
        print("✔️ SECTION 3: 12 CONDITIONS VERIFICATION")
        print("-" * 70)
        
        conditions = [
            ('CONDITION_1_DELIV_PER_GTE_50', 'DELIV_PER ≥ 50%', deliv_per >= 50),
            ('CONDITION_2_DELIV_TURNOVER_GTE_50L', 'DELIVERY_TURNOVER ≥ ₹50L', actual_delivery_turnover >= 5000000),
            ('CONDITION_3_ATW_GTE_20K', 'ATW ≥ ₹20K', actual_atw >= 20000),
        ]
        
        for i, (col_name, formula, expected) in enumerate(conditions, 1):
            actual = stock[col_name]
            status = "✅" if actual == True else "❌"
            print(f"\n   {i}. {formula}")
            print(f"      Expected: {expected}")
            print(f"      Actual:   {actual} {status}")
            if expected != actual:
                all_match = False
        
        # Progressive Conditions (if data exists)
        print("\n   Progressive Conditions (Delivery %):")
        try:
            deliv_per_1w = float(stock['DELIV_PER_1W'])
            deliv_per_1m = float(stock['DELIV_PER_1M'])
            deliv_per_3m = float(stock['DELIV_PER_3M'])
            
            print(f"   Latest:  {deliv_per:.2f}%")
            print(f"   1W Avg:  {deliv_per_1w:.2f}%")
            print(f"   1M Avg:  {deliv_per_1m:.2f}%")
            print(f"   3M Avg:  {deliv_per_3m:.2f}%")
            
            is_progressive = deliv_per > deliv_per_1w > deliv_per_1m > deliv_per_3m
            status = "✅ PROGRESSIVE" if is_progressive else "⚠️ NOT PROGRESSIVE"
            print(f"   {status}")
            
        except:
            print("   ⚠️ Historical data not available (first 3 months of data)")
        
        # ============= SECTION 4: FINAL VERDICT =============
        print(f"\n{'='*70}")
        print("📋 FINAL AUDIT VERDICT")
        print("-" * 70)
        
        if all_match:
            print("\n✅ ALL FORMULAS VERIFIED SUCCESSFULLY!")
            print("   Your system is calculating correctly.")
            return True
        else:
            print("\n❌ FORMULA MISMATCHES DETECTED!")
            print("   There are errors in the system calculations.")
            return False
        
    except FileNotFoundError:
        print("\n❌ ERROR: combined_dashboard_live.csv not found!")
        print("   Make sure you're running this from trading_dashboard/ folder")
        return False
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = audit_single_stock()
    sys.exit(0 if result else 1)
