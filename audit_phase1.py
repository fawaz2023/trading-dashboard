"""
AUDIT PHASE 1 - SINGLE FILE CHECK
=================================
This script works with your current combined_dashboard_live.csv
which has 17 columns:
DATE, SYMBOL, ISIN, EXCHANGE, CLOSE,
DELIV_PER, DELIVERY_TURNOVER, ATW,
and their 1W / 1M / 3M versions.

It will:
1. Check for impossible values (negative, >100% etc.)
2. Check basic sanity of progressive metrics (today vs 1W vs 1M vs 3M)
3. Print a clear verdict at the end.

How to run (from CMD, inside trading_dashboard folder):
    python audit_phase1.py
"""

import pandas as pd
import sys

def main():
    print("\n" + "="*70)
    print("PHASE 1: AUDIT ON combined_dashboard_live.csv")
    print("="*70)

    try:
        df = pd.read_csv("data/combined_dashboard_live.csv")
    except FileNotFoundError:
        print("\n❌ ERROR: data/combined_dashboard_live.csv not found!")
        print("   Make sure this script is in C:\\Users\\fawaz\\Desktop\\trading_dashboard")
        print("   and that the data\\ folder exists with combined_dashboard_live.csv inside it.")
        return

    print(f"\nTotal rows (stocks): {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    print(f"Columns: {', '.join(df.columns)}")

    # -----------------------------
    # TEST 1: IMPOSSIBLE VALUES
    # -----------------------------
    print("\n" + "-"*70)
    print("TEST 1: IMPOSSIBLE / INVALID VALUES")
    print("-"*70)

    all_good = True

    def safe_count(condition, description):
        nonlocal all_good
        count = condition.sum()
        if count == 0:
            print(f"✅ {description}: {count}")
        else:
            print(f"❌ {description}: {count} rows")
            all_good = False
        return count

    # These columns DEFINITELY exist in your file
    # DELIV_PER between 0 and 100
    safe_count(df["DELIV_PER"] < 0, "DELIV_PER < 0%")
    safe_count(df["DELIV_PER"] > 100, "DELIV_PER > 100%")

    # DELIVERY_TURNOVER and ATW should not be negative
    safe_count(df["DELIVERY_TURNOVER"] < 0, "DELIVERY_TURNOVER < 0")
    safe_count(df["ATW"] < 0, "ATW < 0")

    # Check NaN / missing values in critical columns
    critical_cols = [
        "DATE", "SYMBOL", "EXCHANGE", "CLOSE",
        "DELIV_PER", "DELIVERY_TURNOVER", "ATW"
    ]
    for col in critical_cols:
        if col in df.columns:
            count_na = df[col].isna().sum()
            if count_na == 0:
                print(f"✅ Missing values in {col}: {count_na}")
            else:
                print(f"❌ Missing values in {col}: {count_na}")
                all_good = False

    # -----------------------------
    # TEST 2: PROGRESSIVE LOGIC
    # -----------------------------
    print("\n" + "-"*70)
    print("TEST 2: PROGRESSIVE METRIC SANITY")
    print("-"*70)

    # Helper to check progressive pattern: Today, 1W, 1M, 3M
    def progressive_check(prefix, label):
        base = prefix
        cols_needed = [base, base + "_1W", base + "_1M", base + "_3M"]
        missing = [c for c in cols_needed if c not in df.columns]
        if missing:
            print(f"⚠️  Skipping {label} progressive check (missing columns: {missing})")
            return

        sub = df.dropna(subset=cols_needed).copy()
        if len(sub) == 0:
            print(f"⚠️  No rows with complete {label} history.")
            return

        cond_strict = (
            (sub[base] > sub[base + "_1W"]) &
            (sub[base + "_1W"] > sub[base + "_1M"]) &
            (sub[base + "_1M"] > sub[base + "_3M"])
        )
        cond_monotonic = (
            (sub[base] >= sub[base + "_1W"]) &
            (sub[base + "_1W"] >= sub[base + "_1M"]) &
            (sub[base + "_1M"] >= sub[base + "_3M"])
        )

        total = len(sub)
        strict_count = cond_strict.sum()
        mono_count = cond_monotonic.sum()

        print(f"\n{label} progressive stats (using {total} rows with full history):")
        print(f"   Strict progressive (Today > 1W > 1M > 3M): {strict_count} rows "
              f"({strict_count/total*100:.2f}%)")
        print(f"   Monotonic (Today ≥ 1W ≥ 1M ≥ 3M): {mono_count} rows "
              f"({mono_count/total*100:.2f}%)")

    # Check progressives for:
    # - DELIV_PER
    # - DELIVERY_TURNOVER
    # - ATW
    progressive_check("DELIV_PER", "Delivery %")
    progressive_check("DELIVERY_TURNOVER", "Delivery Turnover")
    progressive_check("ATW", "ATW")

    # -----------------------------
    # TEST 3: SAMPLE ROW DISPLAY
    # -----------------------------
    print("\n" + "-"*70)
    print("TEST 3: SAMPLE ROW (FIRST STOCK)")
    print("-"*70)

    if len(df) > 0:
        row = df.iloc[0]
        print(f"DATE: {row['DATE']}")
        print(f"SYMBOL: {row['SYMBOL']}")
        print(f"EXCHANGE: {row['EXCHANGE']}")
        print(f"CLOSE: {row['CLOSE']}")
        print(f"DELIV_PER (today): {row['DELIV_PER']}")
        print(f"DELIV_PER_1W: {row.get('DELIV_PER_1W', 'N/A')}")
        print(f"DELIV_PER_1M: {row.get('DELIV_PER_1M', 'N/A')}")
        print(f"DELIV_PER_3M: {row.get('DELIV_PER_3M', 'N/A')}")
        print(f"DELIVERY_TURNOVER (today): {row['DELIVERY_TURNOVER']}")
        print(f"ATW (today): {row['ATW']}")
    else:
        print("⚠️  Dataframe is empty, no rows to show.")

    # -----------------------------
    # FINAL VERDICT
    # -----------------------------
    print("\n" + "="*70)
    print("FINAL VERDICT")
    print("="*70)

    if all_good:
        print("\n✅ No impossible values or obvious data errors found.")
        print("   Phase 1 basic audit PASSED.")
    else:
        print("\n❌ Some issues were detected in the checks above.")
        print("   Please review the ❌ lines and share this output for deeper debugging.")

if __name__ == "__main__":
    main()
    # Exit code not strictly needed, but good practice
    sys.exit(0)
