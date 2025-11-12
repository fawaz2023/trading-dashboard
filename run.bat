@echo off
echo Starting Trading Dashboard...
echo.
call venv\Scripts\activate.bat
streamlit run dashboard_full.py
