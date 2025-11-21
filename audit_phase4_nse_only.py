"""
PHASE 4: NSE-ONLY FORMULA VERIFICATION
- Compare raw NSE bhav + delivery vs combined_dashboard_live.csv
- Uses ONLY this file; no other helper imports.
"""

import os
import sys
import pandas as pd

# ---------- Utility ----------

def print_section(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

def find_file(folder: str, keywords):
    """
    Find latest CSV in folder whose filename contains all keywords.
    """
    if not os.path.exists(folder):
        return None
    candidates = []
    for f in os.listdir(folder):
        if not f.lower().endswith(".csv"):
            continue
        if all(k.lower() in f.lower() for k in keywords):
            candidates.append(f)
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return os.path.join(folder, candidates[0])

# ---------- Normalization ----------

def normalize_bhav(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Drop duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    # Make a lookup by lower-case name
    lower = {c.lower(): c for c in df.columns}

    # SYMBOL
    sym_col = None
    for key in ["symbol", "tckrsymb", "ticker"]:
        if key in lower:
            sym_col = lower[key]
            break
    if sym_col and sym_col != "SYMBOL":
        df = df.rename(columns={sym_col: "SYMBOL"})

    # CLOSE
    close_col = None
    for key in ["close", "clspric", "close_price"]:
        if key in lower:
            close_col = lower[key]
            break
    if close_col and close_col != "CLOSE":
        df = df.rename(columns={close_col: "CLOSE"})

    # TOTTRDVAL (optional)
    tval_col = None
    for key in ["tottrdval", "ttl_trd_val", "ttl_trf_val", "ttl_trfval", "ttl_trf_val"]:
        if key in lower:
            tval_col = lower[key]
            break
    if tval_col and tval_col != "TOTTRDVAL":
        df = df.rename(columns={tval_col: "TOTTRDVAL"})

    # SERIES
    series_col = None
    for key in ["series", "fininstrmtp", "fininstrmt"]:
        if key in lower:
            series_col = lower[key]
            break
    if series_col and series_col != "SERIES":
        df = df.rename(columns={series_col: "SERIES"})

    # Clean SERIES and numeric fields
    if "SERIES" in df.columns:
        df["SERIES"] = (
            df["SERIES"].astype(str).str.strip().str.upper()
        )

    if "CLOSE" in df.columns:
        df["CLOSE"] = pd.to_numeric(df["CLOSE"], errors="coerce")

    if "TOTTRDVAL" in df.columns:
        df["TOTTRDVAL"] = pd.to_numeric(df["TOTTRDVAL"], errors="coerce")

    return df

def normalize_delivery(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.loc[:, ~df.columns.duplicated()]

    # Strip column names
    df.columns = [c.strip() for c in df.columns]

    lower = {c.lower(): c for c in df.columns}

    # SYMBOL
    sym_col = None
    for key in ["symbol", "tckrsymb", "ticker"]:
        if key in lower:
            sym_col = lower[key]
            break
    if sym_col and sym_col != "SYMBOL":
        df = df.rename(columns={sym_col: "SYMBOL"})

    # SERIES
    series_col = None
    for key in ["series"]:
        if key in lower:
            series_col = lower[key]
            break
    if series_col and series_col != "SERIES":
        df = df.rename(columns={series_col: "SERIES"})

    # DELIV_QTY
    dq_col = None
    for key in lower:
        if "deliv" in key and "qty" in key:
            dq_col = lower[key]
            break
    if dq_col and dq_col != "DELIV_QTY":
        df = df.rename(columns={dq_col: "DELIV_QTY"})

    if "SERIES" in df.columns:
        df["SERIES"] = df["SERIES"].astype(str).str.strip().str.upper()

    if "DELIV_QTY" in df.columns:
        df["DELIV_QTY"] = pd.to_numeric(df["DELIV_QTY"], errors="coerce").fillna(0)
    else:
        raise ValueError("DELIV_QTY column not found in delivery file")

    return df

# ---------- Load & process NSE ----------

def load_and_process_nse(bhav_path: str, deliv_path: str):
    print_section("NSE RAW DATA LOAD & PROCESS")
    try:
        bhav = pd.read_csv(bhav_path)
        deliv = pd.read_csv(deliv_path)
        print(f"Loaded Bhav: {bhav_path} ({len(bhav)} rows)")
        print(f"Loaded Delivery: {deliv_path} ({len(deliv)} rows)")
    except Exception as e:
        print(f"❌ Error loading NSE files: {e}")
        return None, "ERROR_LOAD"

    bhav = normalize_bhav(bhav)
    deliv = normalize_delivery(deliv)

    # Filter EQ
    if "SERIES" in bhav.columns:
        before = len(bhav)
        bhav = bhav[bhav["SERIES"] == "EQ"].copy()
        print(f"Bhav EQ: {len(bhav)} (from {before})")
    else:
        print("❌ Bhav has no SERIES column")
        return None, "ERROR_SERIES"

    if "SERIES" in deliv.columns:
        before = len(deliv)
        deliv = deliv[deliv["SERIES"] == "EQ"].copy()
        print(f"Delivery EQ: {len(deliv)} (from {before})")
        if len(deliv) == 0:
            print("⚠️ No EQ rows in delivery — NA day")
            return None, "NA_NO_EQ"
    else:
        print("❌ Delivery has no SERIES column")
        return None, "ERROR_SERIES_DELIV"

    if "SYMBOL" not in bhav.columns or "SYMBOL" not in deliv.columns:
        print("❌ SYMBOL missing in Bhav or Delivery")
        return None, "ERROR_SYMBOL"

    # Merge on SYMBOL
    merged = bhav.merge(deliv[["SYMBOL", "DELIV_QTY"]], on="SYMBOL", how="left")
    merged["DELIV_QTY"] = pd.to_numeric(merged["DELIV_QTY"], errors="coerce").fillna(0)
    print(f"Merged rows: {len(merged)}")

    return merged, "OK"

# ---------- Verification ----------

def verify_formulas(merged: pd.DataFrame, processed: pd.DataFrame):
    print_section("NSE FORMULA VERIFICATION")
    tests, passed = 0, 0

    # Only rows with non-zero delivery
    m = merged[merged["DELIV_QTY"] > 0].copy()
    if m.empty:
        print("⚠️ No rows with DELIV_QTY > 0")
        return 0, 0

    if "SYMBOL" not in processed.columns:
        print("❌ Processed data has no SYMBOL column")
        return 0, 0

    common = set(m["SYMBOL"]) & set(processed["SYMBOL"])
    if not common:
        print("⚠️ No common symbols between raw + processed")
        return 0, 0

    # Take up to 3 symbols
    for i, sym in enumerate(list(common)[:3], start=1):
        raw_row = m[m["SYMBOL"] == sym].iloc[0]
        proc_row = processed[processed["SYMBOL"] == sym].iloc[0]

        print(f"\n{'-'*60}\nNSE Stock #{i}: {sym}\n{'-'*60}")

        # DELIVERY_TURNOVER
        try:
            if "CLOSE" not in raw_row or "DELIVERY_TURNOVER" not in proc_row:
                print("⚠️ Missing CLOSE or DELIVERY_TURNOVER; skipping")
            else:
                exp_dt = float(raw_row["DELIV_QTY"]) * float(raw_row["CLOSE"])
                act_dt = float(proc_row["DELIVERY_TURNOVER"])
                diff = abs(exp_dt - act_dt)
                tol = max(exp_dt * 0.01, 100.0)  # 1% or ₹100
                print(f"Delivery Turnover - Expected: ₹{exp_dt:,.2f}, Actual: ₹{act_dt:,.2f}, Diff: ₹{diff:,.2f}")
                if diff <= tol:
                    print("✅ DELIVERY_TURNOVER PASS")
                    passed += 1
                else:
                    print("❌ DELIVERY_TURNOVER FAIL")
                tests += 1
        except Exception as e:
            print(f"Error in DELIVERY_TURNOVER check: {e}")

        # ATW (if TOTTRDVAL and ATW exist)
        try:
            if "TOTTRDVAL" in raw_row and "ATW" in proc_row:
                exp_atw = float(raw_row["TOTTRDVAL"]) / 1000.0
                act_atw = float(proc_row["ATW"])
                diff = abs(exp_atw - act_atw)
                tol = max(exp_atw * 0.01, 10.0)
                print(f"ATW - Expected: ₹{exp_atw:,.2f}, Actual: ₹{act_atw:,.2f}, Diff: ₹{diff:,.2f}")
                if diff <= tol:
                    print("✅ ATW PASS")
                    passed += 1
                else:
                    print("❌ ATW FAIL")
                tests += 1
            else:
                print("⚠️ TOTTRDVAL or ATW missing; skipping ATW check")
        except Exception as e:
            print(f"Error in ATW check: {e}")

    return tests, passed

# ---------- Main ----------

def main():
    print_section("START")

    bhav_path = find_file("data/nse_raw", ["nse_bhav"])
    deliv_path = find_file("data/nse_raw", ["nse_delivery"])
    print(f"Bhav: {bhav_path}")
    print(f"Delivery: {deliv_path}")

    if not bhav_path or not deliv_path:
        print("❌ Missing NSE bhav or delivery files.")
        sys.exit(1)

    try:
        processed = pd.read_csv("data/combined_dashboard_live.csv")
        print(f"Dashboard: {len(processed)} rows")
    except Exception as e:
        print(f"❌ Could not load processed dashboard file: {e}")
        sys.exit(1)

    merged, status = load_and_process_nse(bhav_path, deliv_path)
    if status == "NA_NO_EQ":
        print_section("SUMMARY")
        print("Status: NA_NO_EQ (no EQ rows in delivery)")
        print("Tests: 0, Passed: 0")
        sys.exit(0)
    if merged is None:
        print(f"❌ Status: {status}")
        sys.exit(1)

    tests, passed = verify_formulas(merged, processed)

    print_section("SUMMARY")
    print(f"Total tests run: {tests}")
    print(f"Total tests passed: {passed}")
    if tests == 0:
        print("❌ No tests ran (no common symbols or no non-zero DELIV_QTY).")
    elif tests == passed:
        print("🎉 ALL FORMULA CHECKS PASSED")
    else:
        print("⚠️ Some formula checks failed; see details above.")

if __name__ == "__main__":
    main()
