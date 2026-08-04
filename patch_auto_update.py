with open('auto_update_smart.py', 'r', encoding='utf-8') as f:
    content = f.read()

import_code = '''df_bse_deliv = pd.concat(bse_del_frames, ignore_index=True) if bse_del_frames else pd.DataFrame()
print(f"✅ BSE delivery rows: {len(df_bse_deliv)}")

# -------------------------------
# Save Exact Status JSON
# -------------------------------
import json
try:
    status_data = {
        'nse_bhav_date': df_nse['DATE'].max().strftime('%d %b %Y') if not df_nse.empty and 'DATE' in df_nse else 'Missing',
        'nse_deliv_date': df_nse_deliv['DATE'].max().strftime('%d %b %Y') if not df_nse_deliv.empty and 'DATE' in df_nse_deliv else 'Missing',
        'bse_bhav_date': df_bse['DATE'].max().strftime('%d %b %Y') if not df_bse.empty and 'DATE' in df_bse else 'Missing',
        'bse_deliv_date': df_bse_deliv['DATE'].max().strftime('%d %b %Y') if not df_bse_deliv.empty and 'DATE' in df_bse_deliv else 'Missing',
        'last_run': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    with open('data/data_status.json', 'w') as f:
        json.dump(status_data, f, indent=4)
except Exception as e:
    print(f'Error saving data_status.json: {e}')
'''
old_code = '''df_bse_deliv = pd.concat(bse_del_frames, ignore_index=True) if bse_del_frames else pd.DataFrame()
print(f"✅ BSE delivery rows: {len(df_bse_deliv)}")'''

if old_code in content:
    content = content.replace(old_code, import_code)
    with open('auto_update_smart.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Patch applied to auto_update_smart.py")
else:
    print("Target string not found in auto_update_smart.py")
