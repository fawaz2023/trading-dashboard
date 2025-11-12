import os

class Config:
    # ===== DIRECTORY STRUCTURE =====
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    NSE_RAW_DIR = os.path.join(DATA_DIR, 'nse_raw')
    BSE_RAW_DIR = os.path.join(DATA_DIR, 'bse_raw')
    WATCHLIST_DIR = os.path.join(BASE_DIR, 'watchlist')
    LOGS_DIR = os.path.join(BASE_DIR, 'logs')
    
    # ===== DATA FILES =====
    COMBINED_FILE = os.path.join(DATA_DIR, 'combined_2years.csv')
    WATCHLIST_FILE = os.path.join(WATCHLIST_DIR, 'active_watchlist.csv')
    CLOSED_TRADES_FILE = os.path.join(WATCHLIST_DIR, 'closed_trades.csv')
    
    # ===== PROGRESSIVE SPIKE STRATEGY - 12 CONDITIONS =====
    PROGRESSIVE_SPIKE = {
        # Baseline Conditions (3)
        "delivery_pct_min": 50,              # Condition 1: Delivery % >= 50
        "delivery_turnover_min": 5000000,    # Condition 2: Delivery Turnover >= 5M
        "atw_min": 20000,                    # Condition 3: ATW >= 20K
        
        # Conditions 4-12 are progression checks in screener
        # 4-6: Delivery % progression (Today > 1W > 1M > 3M)
        # 7-9: Delivery Turnover progression (Today > 1W > 1M > 3M)
        # 10-12: ATW progression (Today > 1W > 1M > 3M)
    }
    
    # ===== WATCHLIST EXIT STRATEGY =====
    EXIT_STRATEGY = {
        "take_profit_pct": 12,           # Exit at +12% profit
        "stop_loss_pct": -15,            # Exit at -15% loss
        "time_stop_days": 60             # Exit after 60 days regardless
    }
    
    # ===== CREATE DIRECTORIES IF NOT EXIST =====
    @staticmethod
    def ensure_dirs():
        for dir_path in [Config.DATA_DIR, Config.NSE_RAW_DIR, Config.BSE_RAW_DIR, 
                        Config.WATCHLIST_DIR, Config.LOGS_DIR]:
            os.makedirs(dir_path, exist_ok=True)

# Initialize directories
Config.ensure_dirs()
