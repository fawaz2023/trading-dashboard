"""
AUDIT PHASE 2 - DASHBOARD CLOUD (12-CONDITION CHECK)
====================================================
This script inspects data/dashboard_cloud.csv, which the Streamlit
dashboard uses for signals.

It will:
1. Show all columns and a sample row
2. Detect condition-related columns (c1..c12, ALL_12_CONDITIONS, etc.)
3. Count how many stocks pass each condition
4. Count how many pass ALL conditions (approximate if needed)
5. Split counts by EXCHANGE (NSE vs BSE) if that column exists

How to run (from CMD, inside trading_dashboard folder):
    python audit_phase2_cloud.py
"""

import pandas as pd
import sys

def is_bool_like(series):
    """Return True if a column looks like boolean/flag (True/False or 0/1)."""
    s = series.dropna()
    if s.empty:
        return False
    unique_vals = set(s.unique())
    # Accept typical boolean encodings
    allowed = {0, 1, True, False}
    return unique_vals.issubset(allowed)

def main():
    print("\n" + "="*70)
    print("PHASE 2: AUDIT ON dashboard_cloud.csv")
    print("="*70)

    # -----------------------------
    # LOAD FILE
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

    # -----------------------------
    # SAMPLE ROW
    # -----------------------------
    print("\n" + "-"*70)
    print("SAMPLE ROW (FIRST STOCK)")
    print("-"*70)

    if len(df) > 0:
        row = df.iloc[0]
        for col in df.columns:
            print(f"{col}: {row[col]}")
    else:
        print("⚠️  Dataframe is empty (0 rows).")
        sys.exit(0)

    # -----------------------------
    # DETECT CONDITION / FLAG COLUMNS
    # -----------------------------
    print("\n" + "-"*70)
    print("DETECTING CONDITION / FLAG COLUMNS")
    print("-"*70)

    bool_like_cols = [col for col in df.columns if is_bool_like(df[col])]
    print(f"\nBoolean-like columns (flags) detected ({len(bool_like_cols)}):")
    for col in bool_like_cols:
        print(f"  - {col}")

    # Likely condition columns:
    # - Names starting with 'c' followed by digits (c1, c2, ..., c12)
    # - Or containing 'cond'/'condition'
    cond_cols = []
    for col in bool_like_cols:
        name = col.lower()
        if name.startswith("c") and name[1:].isdigit():
            cond_cols.append(col)
        elif "cond" in name:
            cond_cols.append(col)

    cond_cols = sorted(cond_cols)

    print(f"\nCondition-like columns ({len(cond_cols)}):")
    if cond_cols:
        for col in cond_cols:
            print(f"  - {col}")
    else:
        print("  (None detected)")

    # Check if there is a direct ALL_12_CONDITIONS style column
    all_col = None
    for candidate in ["ALL_12_CONDITIONS", "ALL_12", "ALL_CONDITIONS", "ALL_TRUE"]:
        if candidate in df.columns and is_bool_like(df[candidate]):
            all_col = candidate
            break

    if all_col:
        print(f"\nDetected combined 'all conditions' column: {all_col}")
    else:
        print("\nNo explicit 'ALL_12_CONDITIONS' style column detected.")
        print("Will approximate ALL_PASS = all condition columns True, if possible.")

    # -----------------------------
    # CONDITION STATS (GLOBAL)
    # -----------------------------
    print("\n" + "-"*70)
    print("CONDITION STATISTICS - OVERALL")
    print("-"*70)

    if cond_cols:
        for col in cond_cols:
            col_true = (df[col] == True).sum()
            col_false = (df[col] == False).sum()
            print(f"{col}: True={col_true}, False={col_false}, Total={col_true + col_false}")
    else:
        print("⚠️  No condition-like columns found, cannot compute per-condition stats.")

    # Compute "all conditions passed" either from existing column or by AND-ing
    if all_col:
        df["__ALL_PASS__"] = df[all_col] == True
    elif len(cond_cols) >= 1:
        # Use AND across all detected condition columns
        df["__ALL_PASS__"] = df[cond_cols].all(axis=1)
    else:
        df["__ALL_PASS__"] = False  # Fallback

    total_all_pass = df["__ALL_PASS__"].sum()
    print(f"\nStocks passing ALL detected conditions: {total_all_pass} / {len(df)}"
          f" ({total_all_pass / len(df) * 100:.3f}%)")

    # -----------------------------
    # BREAKDOWN BY EXCHANGE
    # -----------------------------
    print("\n" + "-"*70)
    print("BREAKDOWN BY EXCHANGE (IF AVAILABLE)")
    print("-"*70)

    if "EXCHANGE" in df.columns:
        for ex in sorted(df["EXCHANGE"].dropna().unique()):
            sub = df[df["EXCHANGE"] == ex]
            count = len(sub)
            all_pass = sub["__ALL_PASS__"].sum()
            print(f"\nEXCHANGE = {ex}")
            print(f"  Rows: {count}")
            print(f"  ALL_PASS count: {all_pass} ({(all_pass / count * 100) if count else 0:.3f}%)")

            # If we have condition columns, show per-condition True count for this exchange
            if cond_cols:
                print("  Per-condition True counts:")
                for col in cond_cols:
                    ct = (sub[col] == True).sum()
                    print(f"    {col}: {ct}")
    else:
        print("⚠️  No EXCHANGE column found; cannot split NSE/BSE.")

    # -----------------------------
    # FINAL VERDICT
    # -----------------------------
    print("\n" + "="*70)
    print("PHASE 2 SUMMARY")
    print("="*70)

    if cond_cols:
        print("\n✅ Condition-like columns detected and analyzed.")
        print("   Use the ALL_PASS and per-condition counts above to verify expected behaviour.")
    else:
        print("\n⚠️  No condition-like columns detected.")
        print("   This may mean dashboard_cloud.csv does not store per-condition flags,")
        print("   or the column naming is very different from c1..c12 / CONDITION_*.")
    print("\nShare this full output so we can interpret the results together.")

if __name__ == "__main__":
    main()
    sys.exit(0)
