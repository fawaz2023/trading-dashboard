import pandas as pd

def merge_bhav_and_delivery(bhav_file, delivery_file, output_file):
    # Load BSE Bhav data
    bhav_df = pd.read_csv(bhav_file)

    # Load processed BSE delivery data
    delivery_df = pd.read_csv(delivery_file)

    # Rename Bhav data columns for merge consistency if needed
    if 'FinInstrmId' in bhav_df.columns:
        bhav_df.rename(columns={'FinInstrmId': 'SC_CODE'}, inplace=True)
    
    # Convert keys to string for safe merge
    bhav_df['SC_CODE'] = bhav_df['SC_CODE'].astype(str)
    delivery_df['SC_CODE'] = delivery_df['SC_CODE'].astype(str)
    
    # Merge on SC_CODE
    merged_df = bhav_df.merge(
        delivery_df[['SC_CODE', 'DELIV_PER', 'DELIV_QTY']],
        on='SC_CODE',
        how='left',
        suffixes=('', '_deliv')
    )
    
    # Replace or create delivery columns in merged dataframe
    merged_df['DELIV_PER'] = merged_df['DELIV_PER_deliv'].combine_first(merged_df.get('DELIV_PER'))
    merged_df['DELIV_QTY'] = merged_df['DELIV_QTY'].combine_first(pd.Series([None]*len(merged_df)))
    
    # Drop extra columns added by merge
    merged_df = merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_deliv')])

    # Save to output csv
    merged_df.to_csv(output_file, index=False)
    print(f"Merge complete! Output saved to {output_file}")

if __name__ == "__main__":
    bhav_file = 'data/bse_raw/bse_bhav_20251111.csv'  # BSE bhav copy you downloaded
    delivery_file = 'data/bse_delivery/SCBSEALL1111_processed.csv'  # processed delivery file
    output_file = 'data/bse_merged_20251111.csv'  # output merged file

    merge_bhav_and_delivery(bhav_file, delivery_file, output_file)
