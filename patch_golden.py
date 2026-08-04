with open('calculate_active_signals.py', 'r', encoding='utf-8') as f:
    content = f.read()

import_code = '''
    print(f"active_signals_ranked.csv (30-day view) updated and saved: {len(recent_pool)} rows")
    print(f"signal_scores_today.csv saved: {len(today_scores)} rows")
    
    # --- SAVE GOLDEN ML SIGNALS (GREEN STOCKS) ---
    # The user wants to archive stocks that meet the strict "Green" dashboard criteria 
    # (Fresh Trigger + High Stability) for future ML research.
    golden_signals = today_scores[(today_scores["TRIGGER_COUNT_30D"] == 1) & (today_scores["STABILITY_RAW"] > 3.16)].copy()
    
    if not golden_signals.empty:
        golden_path = "data/ml_golden_signals.csv"
        # We append today's golden signals to the master file
        if os.path.exists(golden_path):
            old_golden = pd.read_csv(golden_path)
            combined_golden = pd.concat([old_golden, golden_signals], ignore_index=True)
            # Remove any accidental duplicates by DATE and SYMBOL
            combined_golden = combined_golden.drop_duplicates(subset=["DATE", "SYMBOL"], keep="last")
        else:
            combined_golden = golden_signals
            
        combined_golden.to_csv(golden_path, index=False)
        print(f"ml_golden_signals.csv appended: {len(golden_signals)} new highly stable institutional footprints today!")
        
    # Process and save T2T rejected signals for ML training
'''

target_block = '''
    print(f"active_signals_ranked.csv (30-day view) updated and saved: {len(recent_pool)} rows")
    print(f"signal_scores_today.csv saved: {len(today_scores)} rows")
    
    # Process and save T2T rejected signals for ML training
'''

if target_block in content:
    new_content = content.replace(target_block, import_code)
    with open('calculate_active_signals.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Successfully patched calculate_active_signals.py for ML Golden Signals!")
else:
    print("Could not find the target block to patch.")
