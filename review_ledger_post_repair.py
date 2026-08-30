"""
Task 6 — Post-repair review of ACTIVE ledger positions.

For every ACTIVE row in data/sbia_ledger.csv and data/flexgate2_ledger.csv:
  - pull the symbol's CURRENT metrics from the repaired combined_dashboard_live.csv
  - re-check the entry engine's core conditions under repaired baselines
  - compare recorded entry trigger metrics vs current values
  - classify exposure to the 2025-11-17..2026-07-24 corrupted BSE window via
    1M (22 td) / 3M (66 td) lookbacks from the entry date
  - emit data/ledger_review_post_repair.csv with a KEEP / REVIEW / CLOSE verdict

This script NEVER edits the ledgers — it only produces a report for manual
trading decisions.
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

COMBINED = "data/combined_dashboard_live.csv"
SBIA_LEDGER = "data/sbia_ledger.csv"
FG2_LEDGER = "data/flexgate2_ledger.csv"
SBIA_WL = "data/sbia_alpha_watchlist.csv"
FG2_WL = "data/sbia_flexgate2_watchlist.csv"
FG2_ARCHIVE = "data/flexgate_archive.csv"
OUT = "data/ledger_review_post_repair.csv"

CORRUPT_START = datetime(2025, 11, 17)
CORRUPT_END = datetime(2026, 7, 24)


def load_combined():
    df = pd.read_csv(COMBINED)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    return df.set_index("SYMBOL")


def exposure_flag(entry_date):
    """How deep do the entry's lookbacks reach into the corrupted BSE window?
    (Repair only changed BSE delivery history; NSE-only positions unaffected.)"""
    d = pd.to_datetime(entry_date)
    lookback_1m = d - timedelta(days=31)
    lookback_3m = d - timedelta(days=95)
    if d >= CORRUPT_START and d <= CORRUPT_END:
        return "ENTRY_IN_WINDOW"
    if lookback_3m <= CORRUPT_END:
        return "3M_OVERLAP"
    if lookback_1m <= CORRUPT_END:
        return "1M_OVERLAP"
    return "NONE"


def pct(a, b):
    try:
        a, b = float(a), float(b)
        if b == 0 or pd.isna(b):
            return np.nan
        return (a - b) / b * 100.0
    except (TypeError, ValueError):
        return np.nan


def main():
    if not os.path.exists(COMBINED):
        print(f"ERROR: {COMBINED} missing")
        sys.exit(1)

    combined = load_combined()

    sbia_wl = set()
    if os.path.exists(SBIA_WL):
        sbia_wl = set(pd.read_csv(SBIA_WL)["SYMBOL"].dropna())
    fg2_wl = set()
    if os.path.exists(FG2_WL):
        fg2_wl = set(pd.read_csv(FG2_WL)["SYMBOL"].dropna())

    fg2_archive_syms = set()
    if os.path.exists(FG2_ARCHIVE):
        arch = pd.read_csv(FG2_ARCHIVE)
        arch["DATE"] = pd.to_datetime(arch["DATE"], errors="coerce")
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=14)
        fg2_archive_syms = set(arch[arch["DATE"] >= cutoff]["SYMBOL"].dropna())

    rows = []

    def review(ledger_name, path):
        if not os.path.exists(path):
            return
        led = pd.read_csv(path)
        act = led[led["STATUS"] == "ACTIVE"] if "STATUS" in led.columns else led
        for _, r in act.iterrows():
            sym = r.get("SYMBOL")
            entry_date = r.get("ENTRY_DATE")
            rec = {
                "LEDGER": ledger_name,
                "SYMBOL": sym,
                "EXCHANGE": "",
                "ENTRY_DATE": entry_date,
                "ENTRY_PRICE": round(float(r.get("ENTRY_PRICE", 0) or 0), 2),
                "CORRUPTION_EXPOSURE": exposure_flag(entry_date),
            }
            if sym not in combined.index:
                rec.update({
                    "VERDICT": "REVIEW",
                    "REASON": "Symbol not in current combined universe (delisted/renamed?)",
                })
                rows.append(rec)
                continue

            c = combined.loc[sym]
            if isinstance(c, pd.DataFrame):
                c = c.iloc[0]
            exch = str(c.get("EXCHANGE", "NSE"))
            curr_price = float(c.get("CLOSE", 0) or 0)
            deliv_per = float(c.get("DELIV_PER", 0) or 0)
            deliv_turn = float(c.get("DELIVERY_TURNOVER", 0) or 0)
            atw = float(c.get("ATW", 0) or 0)
            vwap = float(c.get("VWAP", 0) or 0)

            # FlexGate-2 engine formulas
            wd_fg2 = (atw / vwap) if vwap > 0 else np.nan
            it = (deliv_turn / atw) if atw > 0 else np.nan

            p1 = (deliv_per >= 50) and (deliv_turn >= 500_000) and (atw >= 25_000)
            dt_1m = float(c.get("DELIVERY_TURNOVER_1M", 0) or 0)
            dt_3m = float(c.get("DELIVERY_TURNOVER_3M", 0) or 0)
            atw_1m = float(c.get("ATW_1M", 0) or 0)
            atw_3m = float(c.get("ATW_3M", 0) or 0)
            dp_1m = float(c.get("DELIV_PER_1M", 0) or 0)
            consolidation = (0 < dt_1m <= dt_3m) and (0 < atw_1m <= atw_3m)
            trip_a = (dt_1m > 0) and (deliv_turn > 2.0 * dt_1m)
            wd_1m = float(c.get("WHALE_DENSITY_1M", 0) or 0)
            trip_b = (wd_1m > 0) and (wd_fg2 > 2.0 * wd_1m) if pd.notna(wd_fg2) else False
            trip_c = (dp_1m > 0) and (deliv_per > 1.5 * dp_1m)
            tripwire = trip_a or trip_b or trip_c
            fg2_signal = p1 and consolidation and tripwire

            in_wl = (sym in sbia_wl) if ledger_name == "SBIA" else (sym in fg2_wl)
            in_archive = sym in fg2_archive_syms

            pnl_since_entry = pct(curr_price, r.get("ENTRY_PRICE"))

            rec.update({
                "EXCHANGE": exch,
                "CURR_PRICE": round(curr_price, 2),
                "PCT_SINCE_ENTRY": round(pnl_since_entry, 1) if pd.notna(pnl_since_entry) else np.nan,
                "ENTRY_AI_PROB": round(float(r.get("ENTRY_AI_PROB", 0) or 0), 1),
                "ENTRY_WHALE_DENSITY": round(float(r.get("ENTRY_WHALE_DENSITY", 0) or 0), 1),
                "ENTRY_IMPLIED_TRADES": round(float(r.get("ENTRY_IMPLIED_TRADES", 0) or 0), 0),
                "CURR_DELIV_PER": round(deliv_per, 1),
                "CURR_DELIV_TURNOVER": round(deliv_turn, 0),
                "CURR_ATW": round(atw, 0),
                "CURR_WHALE_DENSITY_FG2": round(wd_fg2, 1) if pd.notna(wd_fg2) else np.nan,
                "CURR_IMPLIED_TRADES": round(it, 0) if pd.notna(it) else np.nan,
                "CURR_DELIV_PER_1M": round(dp_1m, 1),
                "CURR_DELIV_TURN_1M": round(dt_1m, 0),
                "CURR_DELIV_TURN_3M": round(dt_3m, 0),
                "FG2_P1_PASS": bool(p1),
                "FG2_TRIPWIRE": bool(tripwire),
                "FG2_SIGNAL_TODAY": bool(fg2_signal),
                "IN_CURRENT_WATCHLIST": bool(in_wl),
                "IN_FG2_ARCHIVE_10D": bool(in_archive),
            })

            reasons = []
            if ledger_name == "FLEXGATE2":
                if not fg2_signal:
                    reasons.append("FlexGate-2 signal no longer holds under repaired baselines")
                if not in_wl and not in_archive:
                    reasons.append("absent from current FlexGate-2 watchlist/archive")
                if sym == "NOVUS":
                    reasons.append("manually backfilled entry metrics (not engine-derived)")
            else:
                if not in_wl:
                    reasons.append("not in current SBIA Alpha watchlist")
                if deliv_turn <= 10_000_000:
                    reasons.append("delivery turnover below 1Cr sanity gate today")

            if exch == "BSE" and rec["CORRUPTION_EXPOSURE"] != "NONE":
                reasons.append(f"BSE entry with {rec['CORRUPTION_EXPOSURE'].lower().replace('_', ' ')} to corrupted window (pre-repair baselines)")

            if reasons:
                if pd.notna(pnl_since_entry) and pnl_since_entry <= -5:
                    rec["VERDICT"] = "CLOSE"
                else:
                    rec["VERDICT"] = "REVIEW"
                rec["REASON"] = "; ".join(reasons)
            else:
                rec["VERDICT"] = "KEEP"
                rec["REASON"] = "signal holds under repaired baselines"

            rows.append(rec)

    review("SBIA", SBIA_LEDGER)
    review("FLEXGATE2", FG2_LEDGER)

    df = pd.DataFrame(rows)
    df.to_csv(OUT, index=False)
    print(f"Wrote {OUT}: {len(df)} ACTIVE positions reviewed")
    print(df[["LEDGER", "SYMBOL", "EXCHANGE", "ENTRY_DATE", "PCT_SINCE_ENTRY", "CORRUPTION_EXPOSURE", "VERDICT"]].to_string(index=False))
    print("\nVerdict counts:", dict(df["VERDICT"].value_counts()))
    print("NOTE: No ledger rows were modified. Review REPORT entries before acting.")


if __name__ == "__main__":
    main()
