@echo off
cd /d C:\Users\fawaz\Desktop\trading_dashboard
call venv\Scripts\activate
echo Checking Python path...
python --version
where python
venv\Scripts\python.exe auto_update_smart.py
echo.
echo Data update completed at %date% %time%
echo.
timeout /t 5
