with open('calculate_active_signals.py', 'r', encoding='utf-8') as f:
    content = f.read()

import_code = '''
        print(f"ml_golden_signals.csv appended: {len(golden_signals)} new highly stable institutional footprints today!")
        
    # --- SAVE LATE BLOOMERS ---
    # Stocks that weren't Golden on Day 1, but achieved STABILITY > 3.16 later within the 30-day window
    current_green = recent_pool[(recent_pool["TRIGGER_COUNT_30D"] == 1) & (recent_pool["STABILITY_RAW"] > 3.16)].copy()
    if not current_green.empty:
        golden_path = "data/ml_golden_signals.csv"
        if os.path.exists(golden_path):
            golden_df = pd.read_csv(golden_path)
            # Exclude stocks that are already purely Golden
            current_green = current_green[~current_green["SYMBOL"].isin(golden_df["SYMBOL"])]
            
        if not current_green.empty:
            current_green["LATE_BLOOMER_DATE"] = max_date.strftime("%Y-%m-%d")
            bloomer_path = "data/ml_late_bloomers.csv"
            if os.path.exists(bloomer_path):
                old_bloomers = pd.read_csv(bloomer_path)
                combined_bloomers = pd.concat([old_bloomers, current_green], ignore_index=True)
                # Keep the first time they bloomed
                combined_bloomers = combined_bloomers.drop_duplicates(subset=["SYMBOL"], keep="first")
            else:
                combined_bloomers = current_green
            combined_bloomers.to_csv(bloomer_path, index=False)
            print(f"ml_late_bloomers.csv updated with new late bloomers!")
'''

target = '        print(f"ml_golden_signals.csv appended: {len(golden_signals)} new highly stable institutional footprints today!")'

if target in content:
    new_content = content.replace(target, import_code)
    with open('calculate_active_signals.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Patched calculate_active_signals.py for Late Bloomers!")
else:
    print("Could not find target in calculate_active_signals.py")
