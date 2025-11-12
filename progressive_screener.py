import pandas as pd

class ProgressiveSpiker:
    def __init__(self, df):
        self.df = df
    
    def get_signals(self):
        """Filter stocks passing all 12 conditions"""
        df = self.df.copy()
        
        required = ["SYMBOL", "CLOSE", "DELIV_PER", "DELIVERY_TURNOVER", "ATW"]
        if not all(col in df.columns for col in required):
            return pd.DataFrame()
        
        # Filter out bad data
        df = df[df["CLOSE"] > 0]
        df = df[df["DELIV_PER"] > 0]
        df = df[df["DELIVERY_TURNOVER"] > 0]
        df = df[df["ATW"] > 0]
        
        # Baseline 3 conditions (STRICT)
        df = df[df["DELIV_PER"] >= 50]
        df = df[df["DELIVERY_TURNOVER"] >= 5000000]
        df = df[df["ATW"] >= 20000]
        
        # Progressive 9 conditions (if columns exist)
        if all(col in df.columns for col in ["DELIV_PER_1W", "DELIV_PER_1M", "DELIV_PER_3M"]):
            df = df[(df["DELIV_PER"] > df["DELIV_PER_1W"]) & 
                    (df["DELIV_PER_1W"] > df["DELIV_PER_1M"]) & 
                    (df["DELIV_PER_1M"] > df["DELIV_PER_3M"])]
        
        if all(col in df.columns for col in ["DELIVERY_TURNOVER_1W", "DELIVERY_TURNOVER_1M", "DELIVERY_TURNOVER_3M"]):
            df = df[(df["DELIVERY_TURNOVER"] > df["DELIVERY_TURNOVER_1W"]) & 
                    (df["DELIVERY_TURNOVER_1W"] > df["DELIVERY_TURNOVER_1M"]) & 
                    (df["DELIVERY_TURNOVER_1M"] > df["DELIVERY_TURNOVER_3M"])]
        
        if all(col in df.columns for col in ["ATW_1W", "ATW_1M", "ATW_3M"]):
            df = df[(df["ATW"] > df["ATW_1W"]) & 
                    (df["ATW_1W"] > df["ATW_1M"]) & 
                    (df["ATW_1M"] > df["ATW_3M"])]
        
        return df.reset_index(drop=True)
