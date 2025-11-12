import pandas as pd

class ProgressiveSpiker:
    def __init__(self, df):
        self.df = df
    
    def get_signals(self):
        """BASELINE ONLY - for old data without progressive columns"""
        df = self.df.copy()
        
        required = ["SYMBOL", "CLOSE", "DELIV_PER", "DELIVERY_TURNOVER", "ATW"]
        if not all(col in df.columns for col in required):
            return pd.DataFrame()
        
        df = df[df["CLOSE"] > 0]
        df = df[df["DELIV_PER"] > 0]
        df = df[df["DELIVERY_TURNOVER"] > 0]
        df = df[df["ATW"] > 0]
        
        df = df[df["DELIV_PER"] >= 40]
        df = df[df["DELIVERY_TURNOVER"] >= 3000000]
        df = df[df["ATW"] >= 15000]
        
        return df.reset_index(drop=True)
