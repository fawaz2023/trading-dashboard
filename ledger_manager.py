import pandas as pd
import numpy as np
import yfinance as yf
import os

def update_sbia_ledger(alpha_watchlist, latest_prices_df, ledger_path="data/sbia_ledger.csv"):
    """
    Updates the permanent trade ledger for SBIA Alpha signals.
    Calculates STATIC Stop Loss and Take Profit based on the Entry Date.
    Classifies trades into ACTIVE, HIT_TP, HIT_SL, MOMENTUM_LOST, SUSPENDED.
    """
    if alpha_watchlist.empty:
        return alpha_watchlist, pd.DataFrame()

    # 1. Load existing ledger
    if os.path.exists(ledger_path):
        ledger_df = pd.read_csv(ledger_path)
    else:
        ledger_df = pd.DataFrame(columns=[
            'ENTRY_DATE', 'SYMBOL', 'ENTRY_PRICE', 'ATR14', 'STOP_LOSS', 'TAKE_PROFIT', 'STATUS', 'EXIT_DATE', 'EXIT_PRICE'
        ])
        
    # Ensure dates are datetime for comparison
    ledger_df['ENTRY_DATE'] = pd.to_datetime(ledger_df['ENTRY_DATE'])
    if 'EXIT_DATE' in ledger_df.columns:
        ledger_df['EXIT_DATE'] = pd.to_datetime(ledger_df['EXIT_DATE'], errors='coerce')
        
    # 2. Identify NEW signals from alpha_watchlist
    alpha_watchlist = alpha_watchlist.copy()
    alpha_watchlist['DATE_DT'] = pd.to_datetime(alpha_watchlist['DATE'])
    
    new_signals = []
    for _, row in alpha_watchlist.iterrows():
        sym = row['SYMBOL']
        dt = row['DATE_DT']
        
        # Check if this exact signal is already in the ledger
        exists = ledger_df[(ledger_df['SYMBOL'] == sym) & (ledger_df['ENTRY_DATE'] == dt)].shape[0] > 0
        if not exists:
            new_signals.append(row)
            
    if new_signals:
        new_df = pd.DataFrame(new_signals)
        print(f"Adding {len(new_df)} new signals to the trade ledger...")
        
        symbols = new_df['SYMBOL'].unique().tolist()
        yf_symbols = [f"{s}.NS" for s in symbols]
        
        # Fetch historical data to guarantee we have enough history to calculate ATR on the entry date
        data = yf.download(yf_symbols, period="6mo", progress=False, group_by="ticker")
        
        new_records = []
        for _, row in new_df.iterrows():
            sym = row['SYMBOL']
            dt = row['DATE_DT']
            
            try:
                if len(symbols) == 1:
                    ticker_df = data
                else:
                    ticker_df = data[f"{sym}.NS"]
                    
                # Get data up to the signal date and drop NaNs from padding
                hist_up_to_dt = ticker_df[ticker_df.index.tz_localize(None) <= dt].copy()
                hist_up_to_dt = hist_up_to_dt.dropna(subset=['Close'])
                
                if len(hist_up_to_dt) > 0:
                    high_low = hist_up_to_dt['High'] - hist_up_to_dt['Low']
                    high_close = np.abs(hist_up_to_dt['High'] - hist_up_to_dt['Close'].shift())
                    low_close = np.abs(hist_up_to_dt['Low'] - hist_up_to_dt['Close'].shift())
                    ranges = pd.concat([high_low, high_close, low_close], axis=1)
                    true_range = np.max(ranges, axis=1)
                    
                    window = min(14, len(hist_up_to_dt))
                    atr14 = true_range.rolling(window).mean().iloc[-1]
                    entry_price = hist_up_to_dt['Close'].iloc[-1]
                    
                    if pd.notna(atr14) and atr14 > 0:
                        sl = entry_price - (2.0 * atr14)
                        tp = entry_price + (4.0 * atr14)
                    else:
                        sl = np.nan
                        tp = np.nan
                        
                    new_records.append({
                        'ENTRY_DATE': dt,
                        'SYMBOL': sym,
                        'ENTRY_PRICE': entry_price,
                        'ATR14': atr14,
                        'STOP_LOSS': sl,
                        'TAKE_PROFIT': tp,
                        'STATUS': 'ACTIVE',
                        'EXIT_DATE': pd.NaT,
                        'EXIT_PRICE': np.nan
                    })
            except Exception as e:
                print(f"Failed to initialize ledger for {sym} on {dt}: {e}")
                
        if new_records:
            ledger_df = pd.concat([ledger_df, pd.DataFrame(new_records)], ignore_index=True)
            
    # 3. Update status of ACTIVE trades
    latest_date = pd.to_datetime(latest_prices_df['DATE'].max()) if 'DATE' in latest_prices_df.columns else pd.Timestamp.now().normalize()
    latest_prices = latest_prices_df.set_index('SYMBOL') if 'SYMBOL' in latest_prices_df.columns else latest_prices_df
    
    # Active keys from the current watchlist (AI Win Prob >= 60%)
    watchlist_keys = set(zip(alpha_watchlist['SYMBOL'], alpha_watchlist['DATE_DT']))
    
    for idx, row in ledger_df.iterrows():
        if row['STATUS'] == 'ACTIVE':
            sym = row['SYMBOL']
            entry_dt = row['ENTRY_DATE']
            
            # If ATR was NaN initially (e.g. JBCHEPHARM), we immediately mark it SUSPENDED
            if pd.isna(row['STOP_LOSS']):
                ledger_df.at[idx, 'STATUS'] = 'SUSPENDED'
                ledger_df.at[idx, 'EXIT_DATE'] = latest_date
                ledger_df.at[idx, 'EXIT_PRICE'] = row['ENTRY_PRICE']
                continue

            # Hardcoded corporate actions / delistings
            if sym in ['JBCHEPHARM']:
                ledger_df.at[idx, 'STATUS'] = 'SUSPENDED'
                ledger_df.at[idx, 'EXIT_DATE'] = latest_date
                ledger_df.at[idx, 'EXIT_PRICE'] = row['ENTRY_PRICE']
                continue

            # Update current price
            if sym in latest_prices.index:
                curr_price = latest_prices.loc[sym, 'CLOSE']
                
                # Check for TP/SL hit
                if pd.notna(row['TAKE_PROFIT']) and curr_price >= row['TAKE_PROFIT']:
                    ledger_df.at[idx, 'STATUS'] = 'HIT_TP'
                    ledger_df.at[idx, 'EXIT_DATE'] = latest_date
                    ledger_df.at[idx, 'EXIT_PRICE'] = curr_price
                elif pd.notna(row['STOP_LOSS']) and curr_price <= row['STOP_LOSS']:
                    ledger_df.at[idx, 'STATUS'] = 'HIT_SL'
                    ledger_df.at[idx, 'EXIT_DATE'] = latest_date
                    ledger_df.at[idx, 'EXIT_PRICE'] = curr_price
                else:
                    # Still between SL/TP. Did it lose momentum?
                    if (sym, entry_dt) not in watchlist_keys:
                        ledger_df.at[idx, 'STATUS'] = 'MOMENTUM_LOST'
                        ledger_df.at[idx, 'EXIT_DATE'] = latest_date
                        ledger_df.at[idx, 'EXIT_PRICE'] = curr_price
            else:
                # Symbol not in latest prices?
                ledger_df.at[idx, 'STATUS'] = 'SUSPENDED'
                ledger_df.at[idx, 'EXIT_DATE'] = latest_date
                ledger_df.at[idx, 'EXIT_PRICE'] = row['ENTRY_PRICE']
                
    # 4. Filter the alpha_watchlist to ONLY include trades that are still ACTIVE
    active_ledger = ledger_df[ledger_df['STATUS'] == 'ACTIVE'].copy()
    active_keys = set(zip(active_ledger['SYMBOL'], pd.to_datetime(active_ledger['ENTRY_DATE'])))
    
    filtered_alpha = alpha_watchlist[
        alpha_watchlist.apply(lambda row: (row['SYMBOL'], row['DATE_DT']) in active_keys, axis=1)
    ].copy()
    
    # 5. Attach the static SL, TP, and ENTRY_PRICE to the filtered_alpha for the dashboard
    # Drop the trailing SL/TP if they exist so we can replace them
    cols_to_drop = [c for c in ['STOP_LOSS', 'TAKE_PROFIT', 'ENTRY_PRICE', 'ATR14'] if c in filtered_alpha.columns]
    if cols_to_drop:
        filtered_alpha = filtered_alpha.drop(columns=cols_to_drop)
        
    # Merge
    active_ledger_subset = active_ledger[['SYMBOL', 'ENTRY_DATE', 'ENTRY_PRICE', 'ATR14', 'STOP_LOSS', 'TAKE_PROFIT']]
    active_ledger_subset = active_ledger_subset.rename(columns={'ENTRY_DATE': 'DATE_DT'})
    
    filtered_alpha = pd.merge(filtered_alpha, active_ledger_subset, on=['SYMBOL', 'DATE_DT'], how='left')
    
    # Cleanup dates for saving
    ledger_df['ENTRY_DATE'] = ledger_df['ENTRY_DATE'].dt.strftime('%Y-%m-%d')
    if 'EXIT_DATE' in ledger_df.columns:
        ledger_df['EXIT_DATE'] = ledger_df['EXIT_DATE'].dt.strftime('%Y-%m-%d')
    
    ledger_df.to_csv(ledger_path, index=False)
    print(f"Ledger updated and saved to {ledger_path}")
    
    if 'DATE_DT' in filtered_alpha.columns:
        filtered_alpha = filtered_alpha.drop(columns=['DATE_DT'])
        
    return filtered_alpha, ledger_df
