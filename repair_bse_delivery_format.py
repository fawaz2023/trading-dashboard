import pandas as pd
import os
import glob
import shutil

# Setup directories
DATA_DIR = 'data'
BACKUP_DIR = 'backups/bse_delivery_badformat'
os.makedirs(BACKUP_DIR, exist_ok=True)

files = sorted(glob.glob(os.path.join(DATA_DIR, 'bse_delivery_*.csv')))
print(f"Scanning {len(files)} files for date corruption...")

corrupt_count = 0
for fp in files:
    filename = os.path.basename(fp)
    # Extract date from filename (e.g. bse_delivery_20251117.csv -> 20251117)
    filename_date = filename.replace('bse_delivery_', '').replace('.csv', '')
    
    # Read first row to check format
    try:
        sample_df = pd.read_csv(fp, nrows=1, dtype=str)
        if sample_df.empty or 'DATE' not in sample_df.columns:
            continue
            
        date_val = str(sample_df['DATE'].iloc[0]).strip()
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        continue
        
    if date_val != filename_date:
        corrupt_count += 1
        print(f"Repairing {filename} (Internal: {date_val}, Expected: {filename_date})")
        
        # Backup
        backup_path = os.path.join(BACKUP_DIR, filename)
        shutil.copy2(fp, backup_path)
        
        # Read full file as string to preserve all data
        df = pd.read_csv(fp, dtype=str)
        orig_len = len(df)
        
        # Fix DATE column
        df['DATE'] = filename_date
        
        # Clean DELIV_QTY and DELIV_PER (strip leading zeros)
        if 'DELIV_QTY' in df.columns:
            df['DELIV_QTY'] = df['DELIV_QTY'].astype(str).str.lstrip('0').replace('', '0')
        if 'DELIV_PER' in df.columns:
            df['DELIV_PER'] = df['DELIV_PER'].astype(str).str.lstrip('0').replace('', '0.00')
            
        # Write back
        df.to_csv(fp, index=False)
        
        # Verify
        verify_df = pd.read_csv(fp, dtype=str)
        if len(verify_df) != orig_len:
            print(f"CRITICAL ERROR: Row count mismatch on {filename}! Restoring from backup.")
            shutil.copy2(backup_path, fp)

print(f"\nRepair complete. Fixed {corrupt_count} files.")
