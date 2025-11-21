"""
AUDIT PHASE 3 - FULL 12-CONDITION STRATEGY CHECK
================================================
This script re-implements the 12 Baseline Conditions exactly as per
'12_Baseline_Conditions_Reference.pdf' and applies them on:

    data/dashboard_cloud.csv

It will:
1. Apply all 12 conditions (Groups A, B, C, D) on each row
2. Count how many stocks pass ALL 12 conditions (overall, NSE, BSE)
3. Show a few example rows of passing signals

How to run (from CMD, inside trading_dashboard folder):
    python audit_phase3_conditions.py
"""

import pandas as pd
import sys

def main():
    print("\n" + "="*70)
    print("PHASE 3: 12-CONDITION STRATEGY AUDIT (dashboard_cloud.csv)")
    print("="*70)

    # -----------------------------
    # LOAD DATA
    # -----------------------------
    try:
        df = pd.read_csv("data/dashboard_cloud.csv")
    except FileNotFoundError:
        print("\n❌ ERROR: data/dashboard_cloud.csv not found!")
        print("   Make sure this script is in C:\\Users\\fawaz\\Desktop\\trading_dashboard")
        print("   and that the data\\ folder contains dashboard_cloud.csv.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR reading dashboard_cloud.csv: {e}")
        sys.exit(1)

    print(f"\nTotal rows: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    print(f"Columns: {', '.join(df.columns)}")

    required_cols = [
        "DELIV_PER", "DELIVERY_TURNOVER", "ATW",
        "DELIV_PER_1W", "DELIV_PER_1M", "DELIV_PER_3M",
        "DELIVERY_TURNOVER_1W", "DELIVERY_TURNOVER_1M", "DELIVERY_TURNOVER_3M",
        "ATW_1W", "ATW_1M", "ATW_3M",
        "EXCHANGE", "SYMBOL", "CLOSE", "DATE"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print("\n❌ ERROR: Missing required columns for 12-condition logic:")
        for c in missing:
            print(f"   - {c}")
        sys.exit(1)
    else:
        print("\n✅ All required columns for 12 conditions are present.")

    # -----------------------------
    # APPLY 12 BASELINE CONDITIONS
    # -----------------------------
    print("\n" + "-"*70)
    print("APPLYING 12 BASELINE CONDITIONS")
    print("-"*70)

    # Group A: Absolute thresholds
    c1 = df["DELIV_PER"] >= 50           # Condition 1: Delivery % ≥ 50
    c2 = df["DELIVERY_TURNOVER"] >= 5_000_000   # Condition 2: Delivery Turnover ≥ ₹50L
    c3 = df["ATW"] >= 20_000            # Condition 3: ATW ≥ ₹20K

    # Group B: Progressive Delivery %
    c4 = df["DELIV_PER"] > df["DELIV_PER_1W"]       # Condition 4
    c5 = df["DELIV_PER_1W"] > df["DELIV_PER_1M"]    # Condition 5
    c6 = df["DELIV_PER_1M"] > df["DELIV_PER_3M"]    # Condition 6

    # Group C: Progressive Delivery Turnover
    c7  = df["DELIVERY_TURNOVER"] > df["DELIVERY_TURNOVER_1W"]      # Condition 7
    c8  = df["DELIVERY_TURNOVER_1W"] > df["DELIVERY_TURNOVER_1M"]   # Condition 8
    c9  = df["DELIVERY_TURNOVER_1M"] > df["DELIVERY_TURNOVER_3M"]   # Condition 9

    # Group D: Progressive ATW
    c10 = df["ATW"] > df["ATW_1W"]      # Condition 10
    c11 = df["ATW_1W"] > df["ATW_1M"]   # Condition 11
    c12 = df["ATW_1M"] > df["ATW_3M"]   # Condition 12

    # Combine into a single mask
    all_12 = (
        c1 & c2 & c3 &
        c4 & c5 & c6 &
        c7 & c8 & c9 &
        c10 & c11 & c12
    )

    df["__C1__"] = c1
    df["__C2__"] = c2
    df["__C3__"] = c3
    df["__C4__"] = c4
    df["__C5__"] = c5
    df["__C6__"] = c6
    df["__C7__"] = c7
    df["__C8__"] = c8
    df["__C9__"] = c9
    df["__C10__"] = c10
    df["__C11__"] = c11
    df["__C12__"] = c12
    df["__ALL_12__"] = all_12

    # -----------------------------
    # GLOBAL STATS
    # -----------------------------
    print("\n" + "-"*70)
    print("GLOBAL CONDITION STATISTICS")
    print("-"*70)

    for i in range(1, 13):
        col = f"__C{i}__"
        true_count = df[col].sum()
        print(f"Condition {i:2d}: True={true_count}, False={len(df) - true_count}")

    total_all = all_12.sum()
    print(f"\nTOTAL stocks passing ALL 12 conditions: {total_all} / {len(df)} "
          f"({(total_all / len(df) * 100) if len(df) else 0:.4f}%)")

    # -----------------------------
    # BREAKDOWN BY EXCHANGE
    # -----------------------------
    print("\n" + "-"*70)
    print("BREAKDOWN BY EXCHANGE")
    print("-"*70)

    for ex in sorted(df["EXCHANGE"].dropna().unique()):
        sub = df[df["EXCHANGE"] == ex]
        count = len(sub)
        all_count = sub["__ALL_12__"].sum()
        print(f"\nEXCHANGE = {ex}")
        print(f"  Rows: {count}")
        print(f"  ALL 12 PASS: {all_count} "
              f"({(all_count / count * 100) if count else 0:.4f}%)")

    # -----------------------------
    # SHOW SAMPLE SIGNALS
    # -----------------------------
    print("\n" + "-"*70)
    print("SAMPLE SIGNALS (FIRST FEW STOCKS PASSING ALL 12)")
    print("-"*70)

    signals = df[df["__ALL_12__"]].copy()

    if signals.empty:
        print("⚠️  No stocks passed all 12 conditions today.")
    else:
        # Only show a few key columns for readability
        cols_to_show = [
            "DATE", "SYMBOL", "EXCHANGE", "CLOSE",
            "DELIV_PER", "DELIV_PER_1W", "DELIV_PER_1M", "DELIV_PER_3M",
            "DELIVERY_TURNOVER", "DELIVERY_TURNOVER_1W",
            "DELIVERY_TURNOVER_1M", "DELIVERY_TURNOVER_3M",
            "ATW", "ATW_1W", "ATW_1M", "ATW_3M"
        ]
        existing_cols = [c for c in cols_to_show if c in signals.columns]
        print(signals[existing_cols].head(10).to_string(index=False))

    # -----------------------------
    # FINAL VERDICT
    # -----------------------------
    print("\n" + "="*70)
    print("PHASE 3 SUMMARY")
    print("="*70)

    if total_all > 0:
        print(f"\n✅ Strategy is producing {total_all} signals on this dataset.")
        print("   Compare NSE/BSE counts with what Streamlit shows on the same day.")
    else:
        print("\n⚠️  0 stocks passed all 12 conditions.")
        print("   This can happen on quiet days, but if it persists,")
        print("   we should double-check thresholds vs market regime.")

if __name__ == "__main__":
    main()
    sys.exit(0)
