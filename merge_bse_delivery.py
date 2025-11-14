import pandas as pd

def merge_bhav_and_delivery(bhav_file, delivery_file, output_file):
    # Load BSE Bhav data
    bhav_df = pd.read_csv(bhav_file)
    
    # Load processed BSE delivery data
    delivery_df = pd.read_csv(delivery_file)
    
    # Rename Bhav data columns if necessary
    # Assuming Bhav has 'FinInstrmId' as stock code; adjust accordingly
    bhav_df.rename(columns={'FinInstrmId': 'SC_CODE'}, inplace=True)
    
    # Convert keys to common type
    bhav_df['SC_CODE'] = bhav_df['SC_CODE'].astype(str)
    delivery_df['SC_CODE'] = delivery_df['SC_CODE'].astype(str)
    
    # Merge on SC_CODE (stock code)
    merged_df = pd.merge(bhav_df, 
                         delivery_df[['SC_CODE', 'DELIV_PER', 'DELIV_QTY']], 
                         on='SC_CODE', how='left')
    
    # Use delivery data from delivery_df to update Bhav data delivery columns
    merged_df['DELIV_PER'] = merged_df['DELIV_PER_y'].fillna(merged_df.get('DELIV_PER_x', pd.NA))
    merged_df['DELIV_QTY'] = merged_df['DELIV_QTY'].fillna(pd.NA)
    
    # Drop extra columns created by merge
    merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_x') or col.endswith('_y')], inplace=True)
    
    # Save merged DataFrame
    merged_df.to_csv(output_file, index=False)
    print(f"Merged file saved to {output_file}")

if __name__ == "__main__":
    bhav_file = 'data/bse_raw/bse_bhav_20251111.csv'  # your BSE bhav CSV path
    delivery_file = 'data/bse_delivery/SCBSEALL1111_processed.csv'  # processed delivery CSV
    output_file = 'data/bse_merged_20251111.csv'

    merge_bhav_and_delivery(bhav_file, delivery_file, output_file)
