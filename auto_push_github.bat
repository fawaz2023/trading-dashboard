@echo off
setlocal enabledelayedexpansion

REM ======================================================================
REM SMART AUTO-UPDATE AND PUSH TO GITHUB WITH EMAIL ALERT ON ERROR
REM Only pushes if download succeeds and changes are detected
REM ======================================================================

cd C:\Users\fawaz\Desktop\trading_dashboard
call venv\Scripts\activate

echo ======================================================================
echo TRADING DASHBOARD - SMART AUTO-UPDATE
echo ======================================================================
echo Start Time: %date% %time%
echo.

REM ===== STEP 1: Download NSE + BSE Data =====
echo [1/4] Downloading NSE + BSE data...
echo ----------------------------------------------------------------------
python auto_update_smart.py

REM Check if download was successful (errorlevel 0 = success)
if errorlevel 1 (
    echo.
    echo ======================================================================
    echo ERROR: Download failed!
    echo ======================================================================
    echo Reason: auto_update_smart.py returned error code
    echo Action: Skipping GitHub push to avoid pushing incomplete data
    echo Check:  logs/downloads.log for details
    echo ======================================================================
    goto :error
)

echo.
echo Download successful!
echo.

REM ===== STEP 2: Check if files actually changed =====
echo [2/4] Checking for file changes...
echo ----------------------------------------------------------------------

REM Check git status for ANY changes (staged or unstaged)
git status --porcelain | findstr /C:"data/combined_2years.csv data/signal_history.csv data/dashboard_cloud.csv data/combined_dashboard_live.csv auto_update_smart.py" >nul
set files_changed=%errorlevel%

if %files_changed% NEQ 0 (
    echo.
    echo ======================================================================
    echo NO CHANGES DETECTED
    echo ======================================================================
    echo Reason: Data files are identical to last commit
    echo Action: Skipping GitHub push (nothing to update)
    echo Note:   This is normal on weekends/holidays
    echo ======================================================================
    goto :end_success
)

echo Changes detected! Preparing to push...
echo.

REM ===== STEP 3: Add files to Git =====
echo [3/4] Staging changed files...
echo ----------------------------------------------------------------------
git add data/combined_2years.csv data/signal_history.csv data/dashboard_cloud.csv data/combined_dashboard_live.csv auto_update_smart.py auto_push_github.bat

if errorlevel 1 (
    echo ERROR: Failed to stage files
    goto :error
)

echo Files staged
echo.

REM ===== STEP 4: Commit and Push to GitHub =====
echo [4/4] Pushing to GitHub...
echo ----------------------------------------------------------------------

REM Create commit message with timestamp
set commit_msg=Auto-update: %date% %time%
git commit -m "%commit_msg%"

if errorlevel 1 (
    echo ERROR: Failed to commit changes
    goto :error
)

echo Committed locally
echo.
echo Pushing to GitHub...

git push origin main

if errorlevel 1 (
    echo.
    echo ======================================================================
    echo ERROR: GitHub push failed!
    echo ======================================================================
    echo Possible reasons:
    echo   - No internet connection
    echo   - GitHub authentication expired
    echo   - Repository permissions issue
    echo.
    echo To fix: Run 'git push origin main' manually
    echo ======================================================================
    goto :error
)

echo.
echo ======================================================================
echo SUCCESS! DASHBOARD UPDATED
echo ======================================================================
echo Commit: %commit_msg%
echo Status: Pushed to GitHub successfully
echo URL:    https://github.com/fawaz2023/trading-dashboard
echo.
echo Your Streamlit dashboard will refresh in 2-3 minutes
echo ======================================================================
goto :end_success

REM ===== ERROR HANDLER WITH EMAIL ALERT =====
:error
echo.
echo Sending error notification email...
python send_error_email.py
echo.
echo End Time: %date% %time%
echo ======================================================================
echo.
echo Press any key to close...
pause > nul
exit /b 1

REM ===== SUCCESS EXIT =====
:end_success
echo.
echo End Time: %date% %time%
echo ======================================================================
echo.
echo Press any key to close...
pause > nul
exit /b 0

endlocal
