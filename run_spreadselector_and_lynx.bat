@echo off
title AntiGravity Spread Selector ^& LYNX Paper Trade Launcher
echo ============================================================
echo   AntiGravity Project 2: Spread Selector ^& LYNX Paper Trade
echo ============================================================
echo.

cd /d "%~dp0"

:: 1. Start LYNX Paper Trading (tot gebruikersnaam ^& wachtwoord pop-up)
echo [1/2] LYNX Paper Trading opstarten...
if exist "C:\Jts\lynx10\tws.exe" (
    start "" "C:\Jts\lynx10\tws.exe" -J-DjtsConfigDir="C:\Jts\lynx10"
) else if exist "C:\Jts\tws.exe" (
    start "" "C:\Jts\tws.exe" -J-DjtsConfigDir="C:\Jts\lynx10"
) else (
    echo Waarschuwing: LYNX installatie niet gevonden in C:\Jts\lynx10 of C:\Jts.
)

:: 2. Start Spread Selector Streamlit Dashboard
echo [2/2] Spread Selector Dashboard opstarten...
echo Dashboard opent in browser (http://localhost:8501)...
echo.

where uv >nul 2>&1
if %errorlevel% equ 0 (
    uv run streamlit run app.py
) else if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
    streamlit run app.py
) else (
    streamlit run app.py
)

pause
