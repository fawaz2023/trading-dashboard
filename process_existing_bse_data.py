import os
import pandas as pd

def process_bse_delivery(txt_file):
    """Process BSE delivery TXT file"""
    try:
        df = pd.read_csv(txt_file, sep='|', header=0)
        df.rename(columns={
            'SCRIP CODE': 'SC_CODE',
            'DELIVERY QTY': 'DELIV_QTY',
            'DELV. PER.': 'DELIV_PER',
            "DAY'S VOLUME": 'QTY_TRADED',
            "DAY'S TURNOVER": 'TURNOVER',
        }, inplace=True)
        
        df['SC_CODE'] = pd.to_numeric(df['SC_CODE'], errors='coerce')
        df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
        df['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
        df.dropna(subset=['SC_CODE'], inplace=True)
        
        print(f"✅ Processed {len(df)} records from {txt_file}")
        return df
    except Exception as e:
        print(f"❌ Error processing {txt_file}: {e}")
        return None

def merge_bhav_delivery(bhav_file, delivery_df):
    """Merge BSE bhav with delivery data"""
    try:
        bhav_df = pd.read_csv(bhav_file)
        
        # Rename bhav column to match delivery
        if 'FinInstrmId' in bhav_df.columns:
            bhav_df.rename(columns={'FinInstrmId': 'SC_CODE'}, inplace=True)
        
        # Convert to string for safe merge
        bhav_df['SC_CODE'] = bhav_df['SC_CODE'].astype(str)
        delivery_df['SC_CODE'] = delivery_df['SC_CODE'].astype(str)
        
        # Merge
        merged = bhav_df.merge(
            delivery_df[['SC_CODE', 'DELIV_PER', 'DELIV_QTY']],
            on='SC_CODE',
            how='left',
            suffixes=('_old', '')
        )
        
        # Keep new delivery data, drop old if exists
        if 'DELIV_PER_old' in merged.columns:
            merged.drop(columns=['DELIV_PER_old'], inplace=True)
        if 'DELIV_QTY_old' in merged.columns:
            merged.drop(columns=['DELIV_QTY_old'], inplace=True)
            
        print(f"✅ Merged {len(merged)} BSE stocks with delivery data")
        return merged
    except Exception as e:
        print(f"❌ Error merging: {e}")
        return None

def main():
    """Process all existing BSE files"""
    print("="*80)
    print("PROCESSING EXISTING BSE DATA (NO DOWNLOADS)")
    print("="*80)
    
    # Get all BSE delivery TXT files you already downloaded
    delivery_dir = 'data/bse_delivery'
    bhav_dir = 'data/bse_raw'
    
    if not os.path.exists(delivery_dir):
        print(f"❌ Directory not found: {delivery_dir}")
        return
    
    # Find all delivery TXT files
    txt_files = [f for f in os.listdir(delivery_dir) if f.endswith('.TXT')]
    
    if not txt_files:
        print(f"❌ No TXT files found in {delivery_dir}")
        return
    
    print(f"\n📁 Found {len(txt_files)} BSE delivery files")
    
    # Process each delivery file and merge with corresponding bhav
    for txt_file in sorted(txt_files):
        txt_path = os.path.join(delivery_dir, txt_file)
        
        # Extract date from filename (e.g., SCBSEALL1111.TXT -> 1111)
        date_code = txt_file.replace('SCBSEALL', '').replace('.TXT', '')
        
        print(f"\n📅 Processing date: {date_code}")
        
        # Process delivery data
        delivery_df = process_bse_delivery(txt_path)
        if delivery_df is None:
            continue
        
        # Find corresponding bhav file
        bhav_files = [f for f in os.listdir(bhav_dir) if date_code in f and f.endswith('.csv')]
        
        if not bhav_files:
            print(f"⚠️  No matching bhav file found for {date_code}")
            continue
        
        bhav_file = os.path.join(bhav_dir, bhav_files[0])
        
        # Merge
        merged_df = merge_bhav_delivery(bhav_file, delivery_df)
        
        if merged_df is not None:
            # Save merged file
            output_file = f'data/bse_merged/bse_merged_{date_code}.csv'
            os.makedirs('data/bse_merged', exist_ok=True)
            merged_df.to_csv(output_file, index=False)
            print(f"💾 Saved: {output_file}")
    
    print("\n" + "="*80)
    print("✅ PROCESSING COMPLETE!")
    print("="*80)
    print("\nMerged files saved in: data/bse_merged/")

if __name__ == "__main__":
    main()
