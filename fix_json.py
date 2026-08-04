import os
import re

with open('auto_update_smart.py', 'r', encoding='utf-8') as f:
    content = f.read()

import_code = '''
# -------------------------------
# Save Exact Status JSON
# -------------------------------
import json
import pandas as pd
try:
    def get_max_date(df):
        if df is None or df.empty or 'DATE' not in df.columns:
            return 'Missing'
        max_val = df['DATE'].max()
        if str(type(max_val)) == "<class 'numpy.int64'>" or isinstance(max_val, (int, str)):
            return pd.to_datetime(str(max_val), format='%Y%m%d', errors='coerce').strftime('%d %b %Y')
        else:
            return max_val.strftime('%d %b %Y')
            
    status_data = {
        'nse_bhav_date': get_max_date(df_nse),
        'nse_deliv_date': get_max_date(df_nse_deliv),
        'bse_bhav_date': get_max_date(df_bse),
        'bse_deliv_date': get_max_date(df_bse_deliv),
        'last_run': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    with open('data/data_status.json', 'w') as f:
        json.dump(status_data, f, indent=4)
except Exception as e:
    print(f'Error saving data_status.json: {e}')
'''

pattern = re.compile(r'# -------------------------------\n# Save Exact Status JSON\n# -------------------------------\n.*?print\(f\'Error saving data_status\.json: \{e\}\'\)\n', re.DOTALL)
new_content = pattern.sub(import_code, content)

if new_content != content:
    with open('auto_update_smart.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print('Patched data_status.json generation logic.')
else:
    print('Failed to patch data_status.json generation logic.')
