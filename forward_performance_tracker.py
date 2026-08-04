"""
Forward Performance Tracker for Institutional Survivors
========================================================
Simulates investing ₹5,000 per trade on EVERY trigger event.
Each row in survivors_archive.csv = 1 separate trade.
Fetches live prices via yfinance and simulates trailing stop losses.

Usage:
    python forward_performance_tracker.py
    python forward_performance_tracker.py --investment 10000
    python forward_performance_tracker.py --stop-loss 7
"""

import pandas as pd
import numpy as np
import yfinance as yf
import warnings
import sys
import os
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURATION
# ============================================================
ARCHIVE_FILE = "data/survivors_archive.csv"
RANKED_FILE = "data/active_signals_ranked.csv"
INVESTMENT_PER_TRADE = 5000  # ₹ per trade
STOP_LOSS_LEVELS = [0, 5, 7, 10, 15]  # % trailing stop loss scenarios
OUTPUT_DIR = "data"

# Known symbol mappings for yfinance (add edge cases here)
SYMBOL_OVERRIDES = {
    # "ARCHIVENAME": "YFINANCENAME"
}


def load_trades():
    """Load every trigger event as a separate trade."""
    if not os.path.exists(ARCHIVE_FILE):
        print(f"ERROR: {ARCHIVE_FILE} not found.")
        sys.exit(1)

    df = pd.read_csv(ARCHIVE_FILE)
    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
    df = df.dropna(subset=['DATE', 'SYMBOL', 'CLOSE'])

    # Load ranked scores if available
    if os.path.exists(RANKED_FILE):
        ranked = pd.read_csv(RANKED_FILE)
        ranked['DATE'] = pd.to_datetime(ranked['DATE'], errors='coerce')
        score_cols = ['COMBINED_SCORE', 'MOMENTUM_SCORE', 'FOOTPRINT_SCORE', 'STABILITY_SCORE']
        merge_cols = [c for c in score_cols if c in ranked.columns]
        if merge_cols:
            df = df.merge(
                ranked[['DATE', 'SYMBOL', 'EXCHANGE'] + merge_cols],
                on=['DATE', 'SYMBOL', 'EXCHANGE'],
                how='left'
            )

    # Build trade records
    trades = []
    for idx, row in df.iterrows():
        symbol = str(row['SYMBOL']).strip()
        exchange = str(row.get('EXCHANGE', 'NSE')).strip()

        # Convert to yfinance ticker
        if symbol in SYMBOL_OVERRIDES:
            yf_symbol = SYMBOL_OVERRIDES[symbol]
        elif exchange == 'BSE':
            yf_symbol = f"{symbol}.BO"
        else:
            yf_symbol = f"{symbol}.NS"

        trade = {
            'TRADE_ID': idx,
            'SYMBOL': symbol,
            'EXCHANGE': exchange,
            'YF_SYMBOL': yf_symbol,
            'ENTRY_DATE': row['DATE'],
            'ENTRY_PRICE': float(row['CLOSE']),
            'COMBINED_SCORE': row.get('COMBINED_SCORE', np.nan),
            'TRIGGERS_30D': df[df['SYMBOL'] == symbol].shape[0],
        }
        trades.append(trade)

    return pd.DataFrame(trades)


def fetch_price_history(trades_df):
    """Batch-download price history for all unique tickers."""
    unique_tickers = trades_df['YF_SYMBOL'].unique().tolist()
    earliest_date = trades_df['ENTRY_DATE'].min() - timedelta(days=1)

    print(f"  Downloading price history for {len(unique_tickers)} unique tickers...")
    print(f"  Date range: {earliest_date.strftime('%Y-%m-%d')} to today")
    print()

    # Download in one batch for efficiency
    data = yf.download(
        unique_tickers,
        start=earliest_date.strftime('%Y-%m-%d'),
        progress=False,
        auto_adjust=True,
        threads=True
    )

    if data.empty:
        print("ERROR: yfinance returned no data.")
        sys.exit(1)

    # Extract close prices
    if isinstance(data.columns, pd.MultiIndex):
        close_data = data['Close'] if 'Close' in data.columns.get_level_values(0) else data
    else:
        # Single ticker case
        close_data = data[['Close']].rename(columns={'Close': unique_tickers[0]})

    # Track which tickers failed
    failed_tickers = set()
    for ticker in unique_tickers:
        if ticker not in close_data.columns:
            failed_tickers.add(ticker)
        elif close_data[ticker].dropna().empty:
            failed_tickers.add(ticker)

    if failed_tickers:
        symbols = [t.replace('.NS', '').replace('.BO', '') for t in failed_tickers]
        print(f"  ⚠️ No price data for {len(failed_tickers)} ticker(s): {symbols}")
        print()

    return close_data, failed_tickers


def simulate_trailing_stop(daily_prices, entry_price, stop_pct):
    """
    Simulate a trailing stop loss on daily close prices.

    Returns: (exit_date, exit_price, peak_price, was_stopped)
    """
    if stop_pct <= 0:
        # No stop loss - hold to present
        if daily_prices.empty:
            return None, entry_price, entry_price, False
        return daily_prices.index[-1], float(daily_prices.iloc[-1]), float(daily_prices.max()), False

    peak = entry_price
    for date, price in daily_prices.items():
        price = float(price)
        if np.isnan(price):
            continue

        if price > peak:
            peak = price

        trailing_stop_level = peak * (1 - stop_pct / 100)
        if price <= trailing_stop_level:
            return date, price, peak, True

    # Never stopped out - still holding
    if daily_prices.empty:
        return None, entry_price, entry_price, False
    return daily_prices.index[-1], float(daily_prices.iloc[-1]), peak, False


def run_simulation(trades_df, close_data, failed_tickers, investment=INVESTMENT_PER_TRADE):
    """Run the full simulation across all stop loss levels."""

    # Filter out failed tickers
    valid_trades = trades_df[~trades_df['YF_SYMBOL'].isin(failed_tickers)].copy()
    skipped = len(trades_df) - len(valid_trades)
    if skipped > 0:
        print(f"  Skipping {skipped} trade(s) with no price data.")
        print()

    results = {}

    for sl in STOP_LOSS_LEVELS:
        sl_label = f"SL_{sl}%" if sl > 0 else "NO_SL"
        trade_results = []

        for _, trade in valid_trades.iterrows():
            ticker = trade['YF_SYMBOL']
            entry_date = trade['ENTRY_DATE']
            entry_price = trade['ENTRY_PRICE']

            # Get prices AFTER entry date
            if ticker in close_data.columns:
                ticker_prices = close_data[ticker].dropna()
                # Get prices from the day AFTER entry
                post_entry = ticker_prices[ticker_prices.index > entry_date.tz_localize(ticker_prices.index.tz) if ticker_prices.index.tz else ticker_prices.index > entry_date]
                if post_entry.empty:
                    # Try same day and after (in case entry_date has no exact match)
                    post_entry = ticker_prices[ticker_prices.index >= entry_date.tz_localize(ticker_prices.index.tz) if ticker_prices.index.tz else ticker_prices.index >= entry_date]
            else:
                post_entry = pd.Series(dtype=float)

            exit_date, exit_price, peak_price, was_stopped = simulate_trailing_stop(
                post_entry, entry_price, sl
            )

            shares = investment / entry_price
            current_value = shares * exit_price
            pnl = current_value - investment
            roi = (pnl / investment) * 100
            peak_roi = ((peak_price / entry_price) - 1) * 100 if entry_price > 0 else 0

            days_held = (exit_date - entry_date).days if exit_date is not None else 0

            trade_results.append({
                'TRADE_ID': trade['TRADE_ID'],
                'SYMBOL': trade['SYMBOL'],
                'EXCHANGE': trade['EXCHANGE'],
                'ENTRY_DATE': entry_date,
                'ENTRY_PRICE': entry_price,
                'EXIT_DATE': exit_date,
                'EXIT_PRICE': exit_price,
                'PEAK_PRICE': peak_price,
                'WAS_STOPPED': was_stopped,
                'DAYS_HELD': days_held,
                'SHARES': shares,
                'INVESTED': investment,
                'CURRENT_VALUE': current_value,
                'PNL': pnl,
                'ROI_PCT': roi,
                'PEAK_ROI_PCT': peak_roi,
                'COMBINED_SCORE': trade['COMBINED_SCORE'],
                'TRIGGERS_30D': trade['TRIGGERS_30D'],
                'STOP_LOSS': sl,
            })

        results[sl_label] = pd.DataFrame(trade_results)

    return results


def print_report(results, investment=INVESTMENT_PER_TRADE):
    """Print a comprehensive performance report."""

    print()
    print("=" * 72)
    print("  INSTITUTIONAL SURVIVORS — FORWARD PERFORMANCE SIMULATION")
    print("=" * 72)
    print(f"  Investment per trade: ₹{investment:,.0f}")
    print(f"  Data source: {ARCHIVE_FILE}")
    print(f"  Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 72)

    # ─── SECTION 1: Stop Loss Comparison ───
    print()
    print("┌─────────────────────────────────────────────────────────────────────┐")
    print("│  STOP LOSS COMPARISON                                              │")
    print("├──────────┬────────┬────────────┬────────────┬─────────┬────────────┤")
    print("│ Strategy │ Trades │  Invested  │  P&L (₹)   │ ROI (%) │  Win Rate  │")
    print("├──────────┼────────┼────────────┼────────────┼─────────┼────────────┤")

    for sl_label, df in results.items():
        total_trades = len(df)
        total_invested = total_trades * investment
        total_pnl = df['PNL'].sum()
        total_roi = (total_pnl / total_invested) * 100 if total_invested > 0 else 0
        winners = (df['PNL'] > 0).sum()
        win_rate = (winners / total_trades) * 100 if total_trades > 0 else 0
        stopped = df['WAS_STOPPED'].sum() if 'WAS_STOPPED' in df.columns else 0

        sl_display = sl_label.replace("SL_", "").replace("NO_SL", "Hold")
        pnl_sign = "+" if total_pnl >= 0 else ""

        print(f"│ {sl_display:>8s} │ {total_trades:>6d} │ ₹{total_invested:>8,.0f} │ {pnl_sign}₹{total_pnl:>8,.0f} │ {total_roi:>+6.2f}% │ {win_rate:>5.1f}% ({winners}/{total_trades}) │")

    print("└──────────┴────────┴────────────┴────────────┴─────────┴────────────┘")

    # ─── SECTION 2: Detailed Results for each scenario ───
    for sl_label, df in results.items():
        sl_display = sl_label.replace("SL_", "Trailing ").replace("NO_SL", "Buy & Hold (No SL)")
        if "%" in sl_label:
            sl_display += " Stop Loss"

        print()
        print(f"  ── {sl_display} ──")
        print()

        # Top 10 Winners
        top = df.nlargest(10, 'ROI_PCT')
        print("  🏆 TOP 10 WINNERS:")
        print(f"  {'SYMBOL':<14s} {'ENTRY DATE':<12s} {'ENTRY':>8s} {'EXIT/NOW':>9s} {'PEAK':>8s} {'ROI':>8s} {'P&L':>9s} {'DAYS':>5s} {'STATUS':<8s}")
        print(f"  {'─'*14} {'─'*12} {'─'*8} {'─'*9} {'─'*8} {'─'*8} {'─'*9} {'─'*5} {'─'*8}")
        for _, r in top.iterrows():
            status = "STOPPED" if r['WAS_STOPPED'] else "HOLDING"
            print(f"  {r['SYMBOL']:<14s} {r['ENTRY_DATE'].strftime('%Y-%m-%d'):<12s} ₹{r['ENTRY_PRICE']:>7,.0f} ₹{r['EXIT_PRICE']:>7,.0f} ₹{r['PEAK_PRICE']:>6,.0f} {r['ROI_PCT']:>+7.2f}% {r['PNL']:>+8,.0f} {r['DAYS_HELD']:>5d} {status:<8s}")

        print()

        # Bottom 5 Losers
        bottom = df.nsmallest(5, 'ROI_PCT')
        print("  💔 BOTTOM 5 LOSERS:")
        print(f"  {'SYMBOL':<14s} {'ENTRY DATE':<12s} {'ENTRY':>8s} {'EXIT/NOW':>9s} {'PEAK':>8s} {'ROI':>8s} {'P&L':>9s} {'DAYS':>5s} {'STATUS':<8s}")
        print(f"  {'─'*14} {'─'*12} {'─'*8} {'─'*9} {'─'*8} {'─'*8} {'─'*9} {'─'*5} {'─'*8}")
        for _, r in bottom.iterrows():
            status = "STOPPED" if r['WAS_STOPPED'] else "HOLDING"
            print(f"  {r['SYMBOL']:<14s} {r['ENTRY_DATE'].strftime('%Y-%m-%d'):<12s} ₹{r['ENTRY_PRICE']:>7,.0f} ₹{r['EXIT_PRICE']:>7,.0f} ₹{r['PEAK_PRICE']:>6,.0f} {r['ROI_PCT']:>+7.2f}% {r['PNL']:>+8,.0f} {r['DAYS_HELD']:>5d} {status:<8s}")

    # ─── SECTION 3: Score-Based Performance ───
    no_sl = results.get("NO_SL", list(results.values())[0])
    if 'COMBINED_SCORE' in no_sl.columns and no_sl['COMBINED_SCORE'].notna().any():
        print()
        print("  ── SCORE-BASED PERFORMANCE (No SL) ──")
        print()
        print("  Do higher-scored survivors actually perform better?")
        print()

        scored = no_sl.dropna(subset=['COMBINED_SCORE']).copy()
        scored['SCORE_BIN'] = pd.cut(
            scored['COMBINED_SCORE'],
            bins=[0, 0.3, 0.5, 0.7, 1.0],
            labels=['Low (0-0.3)', 'Mid (0.3-0.5)', 'High (0.5-0.7)', 'Elite (0.7+)'],
            include_lowest=True
        )

        print(f"  {'Score Range':<16s} {'Trades':>7s} {'Avg ROI':>9s} {'Win Rate':>10s} {'Avg P&L':>10s} {'Best':>9s} {'Worst':>9s}")
        print(f"  {'─'*16} {'─'*7} {'─'*9} {'─'*10} {'─'*10} {'─'*9} {'─'*9}")

        for bin_label in ['Low (0-0.3)', 'Mid (0.3-0.5)', 'High (0.5-0.7)', 'Elite (0.7+)']:
            grp = scored[scored['SCORE_BIN'] == bin_label]
            if grp.empty:
                print(f"  {bin_label:<16s} {'0':>7s} {'N/A':>9s} {'N/A':>10s} {'N/A':>10s} {'N/A':>9s} {'N/A':>9s}")
                continue

            avg_roi = grp['ROI_PCT'].mean()
            win_rate = (grp['PNL'] > 0).sum() / len(grp) * 100
            avg_pnl = grp['PNL'].mean()
            best = grp['ROI_PCT'].max()
            worst = grp['ROI_PCT'].min()

            print(f"  {bin_label:<16s} {len(grp):>7d} {avg_roi:>+8.2f}% {win_rate:>8.1f}% ₹{avg_pnl:>+9,.0f} {best:>+8.2f}% {worst:>+8.2f}%")

    # ─── SECTION 4: Repeat Trigger Analysis ───
    print()
    print("  ── REPEAT TRIGGER ANALYSIS (No SL) ──")
    print()
    print("  Do stocks that trigger multiple times outperform single triggers?")
    print()

    single = no_sl[no_sl['TRIGGERS_30D'] == 1]
    repeat = no_sl[no_sl['TRIGGERS_30D'] > 1]

    print(f"  {'Category':<20s} {'Trades':>7s} {'Avg ROI':>9s} {'Win Rate':>10s} {'Avg P&L':>10s}")
    print(f"  {'─'*20} {'─'*7} {'─'*9} {'─'*10} {'─'*10}")

    if not single.empty:
        s_roi = single['ROI_PCT'].mean()
        s_wr = (single['PNL'] > 0).sum() / len(single) * 100
        s_pnl = single['PNL'].mean()
        print(f"  {'Single Trigger':<20s} {len(single):>7d} {s_roi:>+8.2f}% {s_wr:>8.1f}% ₹{s_pnl:>+9,.0f}")

    if not repeat.empty:
        r_roi = repeat['ROI_PCT'].mean()
        r_wr = (repeat['PNL'] > 0).sum() / len(repeat) * 100
        r_pnl = repeat['PNL'].mean()
        print(f"  {'Repeat Trigger':<20s} {len(repeat):>7d} {r_roi:>+8.2f}% {r_wr:>8.1f}% ₹{r_pnl:>+9,.0f}")

    # ─── SECTION 5: Portfolio Summary Stats ───
    print()
    print("  ── STATISTICAL SUMMARY (No SL) ──")
    print()

    roi_series = no_sl['ROI_PCT']
    print(f"  Mean Return:     {roi_series.mean():>+.2f}%")
    print(f"  Median Return:   {roi_series.median():>+.2f}%")
    print(f"  Std Deviation:   {roi_series.std():>.2f}%")
    print(f"  Best Trade:      {roi_series.max():>+.2f}% ({no_sl.loc[roi_series.idxmax(), 'SYMBOL']})")
    print(f"  Worst Trade:     {roi_series.min():>+.2f}% ({no_sl.loc[roi_series.idxmin(), 'SYMBOL']})")
    print(f"  Avg Days Held:   {no_sl['DAYS_HELD'].mean():>.0f} days")
    print(f"  Max Unrealized:  {no_sl['PEAK_ROI_PCT'].mean():>+.2f}% avg peak before pullback")

    print()
    print("=" * 72)


def save_results(results):
    """Save detailed trade results to CSV."""
    for sl_label, df in results.items():
        outfile = os.path.join(OUTPUT_DIR, f"simulation_{sl_label.lower()}.csv")
        df.to_csv(outfile, index=False)

    # Save the no-SL results as the primary output
    primary = results.get("NO_SL", list(results.values())[0])
    primary_path = os.path.join(OUTPUT_DIR, "forward_performance.csv")
    primary.to_csv(primary_path, index=False)
    print(f"  Results saved to {primary_path}")
    print(f"  All scenarios saved to data/simulation_*.csv")


def main():
    """Main entry point."""
    investment = INVESTMENT_PER_TRADE

    # Parse CLI args
    if "--investment" in sys.argv:
        idx = sys.argv.index("--investment")
        if idx + 1 < len(sys.argv):
            investment = float(sys.argv[idx + 1])

    if "--stop-loss" in sys.argv:
        idx = sys.argv.index("--stop-loss")
        if idx + 1 < len(sys.argv):
            custom_sl = float(sys.argv[idx + 1])
            if custom_sl not in STOP_LOSS_LEVELS:
                STOP_LOSS_LEVELS.append(custom_sl)
                STOP_LOSS_LEVELS.sort()

    print()
    print("=" * 72)
    print("  LOADING INSTITUTIONAL SURVIVORS ARCHIVE")
    print("=" * 72)
    print()

    # Step 1: Load trades
    trades_df = load_trades()
    print(f"  Loaded {len(trades_df)} trades across {trades_df['SYMBOL'].nunique()} unique stocks")
    print(f"  Date range: {trades_df['ENTRY_DATE'].min().strftime('%Y-%m-%d')} to {trades_df['ENTRY_DATE'].max().strftime('%Y-%m-%d')}")
    print()

    # Step 2: Fetch price history
    close_data, failed_tickers = fetch_price_history(trades_df)

    # Step 3: Run simulation
    print("  Running simulation across all stop loss scenarios...")
    print()
    sim_results = run_simulation(trades_df, close_data, failed_tickers, investment)

    # Step 4: Print report
    print_report(sim_results, investment)

    # Step 5: Save results
    save_results(sim_results)

    print()


if __name__ == "__main__":
    main()
