import pandas as pd
import os
from config import Config
from datetime import datetime, timedelta

class WatchlistManager:
    def __init__(self):
        os.makedirs(Config.WATCHLIST_DIR, exist_ok=True)
        self.watchlist_file = Config.WATCHLIST_FILE
        self.closed_file = Config.CLOSED_TRADES_FILE
        self.active = self.load_watchlist()
    
    def load_watchlist(self):
        if os.path.exists(self.watchlist_file):
            df = pd.read_csv(self.watchlist_file)
            if "status" not in df.columns:
                df["status"] = "active"
            if "current_price" not in df.columns:
                df["current_price"] = df["entry_price"].copy()
            return df
        return pd.DataFrame()
    
    def stock_exists(self, symbol):
        if len(self.active) == 0:
            return False
        return symbol in self.active["symbol"].values
    
    def add_stock(self, symbol, entry_price, delivery_pct, momentum):
        if self.stock_exists(symbol):
            return False, f"{symbol} already exists"
        
        new_data = {
            "symbol": [symbol],
            "entry_price": [entry_price],
            "entry_date": [datetime.now().strftime("%Y-%m-%d")],
            "delivery_pct": [delivery_pct],
            "momentum": [momentum],
            "current_price": [entry_price],
            "tp": [round(entry_price * 1.12, 2)],
            "sl": [round(entry_price * 0.85, 2)],
            "exit_date": [(datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")],
            "status": ["active"]
        }
        
        new_row = pd.DataFrame(new_data)
        self.active = pd.concat([self.active, new_row], ignore_index=True)
        self.active.to_csv(self.watchlist_file, index=False)
        return True, f"Added {symbol}"
    
    def close_position(self, symbol, exit_price, reason="manual"):
        """SAFELY close one position - bulletproof version"""
        try:
            if len(self.active) == 0:
                return False
            
            if symbol not in self.active["symbol"].values:
                return False
            
            row_data = self.active[self.active["symbol"] == symbol].iloc[0].to_dict()
            entry = float(row_data["entry_price"])
            pct_return = ((exit_price - entry) / entry) * 100
            
            closed_data = pd.DataFrame([{
                "symbol": symbol,
                "entry_price": entry,
                "exit_price": float(exit_price),
                "entry_date": row_data["entry_date"],
                "exit_date": datetime.now().strftime("%Y-%m-%d"),
                "return_pct": round(pct_return, 2),
                "reason": reason
            }])
            
            if os.path.exists(self.closed_file):
                closed = pd.read_csv(self.closed_file)
                closed = pd.concat([closed, closed_data], ignore_index=True)
            else:
                closed = closed_data
            
            closed.to_csv(self.closed_file, index=False)
            
            remaining = self.active[self.active["symbol"] != symbol].copy()
            remaining.reset_index(drop=True, inplace=True)
            remaining.to_csv(self.watchlist_file, index=False)
            self.active = remaining
            
            return True
            
        except Exception as e:
            print(f"Error in close_position: {str(e)}")
            return False
    
    def delete_stock(self, symbol):
        """SAFELY delete stock - bulletproof version"""
        try:
            if len(self.active) == 0:
                return False
            
            if symbol not in self.active["symbol"].values:
                return False
            
            remaining = self.active[self.active["symbol"] != symbol].copy()
            remaining.reset_index(drop=True, inplace=True)
            remaining.to_csv(self.watchlist_file, index=False)
            self.active = remaining
            
            return True
            
        except Exception as e:
            print(f"Error in delete_stock: {str(e)}")
            return False
    
    def auto_update_prices(self, price_data_df):
        """Update current prices from data"""
        if len(self.active) == 0:
            return
        
        for idx, row in self.active.iterrows():
            symbol = row["symbol"]
            if symbol in price_data_df["SYMBOL"].values:
                latest = price_data_df[price_data_df["SYMBOL"] == symbol]["CLOSE"].values[0]
                self.active.at[idx, "current_price"] = latest
        
        self.active.to_csv(self.watchlist_file, index=False)
    
    def get_win_rate(self):
        if not os.path.exists(self.closed_file):
            return {"total": 0, "winners": 0, "win_rate": 0, "avg_return": 0}
        
        closed = pd.read_csv(self.closed_file)
        if len(closed) == 0:
            return {"total": 0, "winners": 0, "win_rate": 0, "avg_return": 0}
        
        total = len(closed)
        winners = len(closed[closed["return_pct"] > 0])
        win_rate = (winners / total * 100) if total > 0 else 0
        avg_return = closed["return_pct"].mean()
        
        return {"total": total, "winners": winners, "win_rate": win_rate, "avg_return": avg_return}
