import pandas as pd

def process_bse_delivery(csv_file):
    try:
        # Read the BSE delivery TXT file with header row, pipe-delimited
        df = pd.read_csv(csv_file, sep='|', header=0)
        
        # Rename columns to standard names
        df.rename(columns={
            'SCRIP CODE': 'SC_CODE',
            'DELIVERY QTY': 'DELIV_QTY',
            'DELV. PER.': 'DELIV_PER',
            "DAY'S VOLUME": 'QTY_TRADED',
            "DAY'S TURNOVER": 'TURNOVER',
            'DELIVERY VAL': 'DELIVERY_VAL',
            'DATE': 'DATE'
        }, inplace=True)

        # Convert columns to numeric where applicable
        df['SC_CODE'] = pd.to_numeric(df['SC_CODE'], errors='coerce')
        df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
        df['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
        df['QTY_TRADED'] = pd.to_numeric(df['QTY_TRADED'], errors='coerce')
        df['TURNOVER'] = pd.to_numeric(df['TURNOVER'], errors='coerce')

        # Drop rows where SC_CODE is NaN
        df.dropna(subset=['SC_CODE'], inplace=True)

        print(f"Processed {len(df)} BSE delivery records")
        return df

    except Exception as e:
        print(f"Error processing: {e}")
        return None

if __name__ == "__main__":
    csv_file = 'data/bse_delivery/SCBSEALL1111.TXT'  # Replace with your actual file
    df = process_bse_delivery(csv_file)
    print(df.head())
