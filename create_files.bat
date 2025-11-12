@echo off 
echo Creating all Python files... 
echo. 
(
"""Configuration file"""
from datetime import datetime
import os

class Config:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "data")
    NSE_RAW_DIR = os.path.join(DATA_DIR, "nse_raw")
    BSE_RAW_DIR = os.path.join(DATA_DIR, "bse_raw")
    WATCHLIST_DIR = os.path.join(BASE_DIR, "watchlist")
    LOGS_DIR = os.path.join(BASE_DIR, "logs")
    COMBINED_FILE = os.path.join(DATA_DIR, "combined_2years.csv")
    WATCHLIST_FILE = os.path.join(WATCHLIST_DIR, "active_watchlist.csv")
    CLOSED_TRADES_FILE = os.path.join(WATCHLIST_DIR, "closed_trades.csv")
    PROGRESSIVE_SPIKE = {"delivery_pct_min": 50, "delivery_turnover_min": 5000000, "atw_min": 20000}
    EXIT_RULES = {"take_profit": 0.12, "stop_loss": -0.15, "max_days": 60}
    @staticmethod
    def initialize():
        import pandas as pd
        for directory in [Config.DATA_DIR, Config.NSE_RAW_DIR, Config.BSE_RAW_DIR, Config.WATCHLIST_DIR, Config.LOGS_DIR]:
            os.makedirs(directory, exist_ok=True)
Config.initialize()
) > config.py
