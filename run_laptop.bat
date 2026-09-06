@echo off
TITLE AntiGravity Project 2 - Spread Selector (Laptop)
echo ============================================================
echo   AntiGravity Project 2: Spread Selector (Laptop Mode)
echo ============================================================
echo.
cd /d "%~dp0"
if not exist ".venv\Scripts\activate.bat" (
    echo [EERSTE OPSTART] Virtuele omgeving nog niet gevonden.
    echo Bezig met eenmalige automatische installatie...
    call setup_laptop.bat
)
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
)
echo.
echo Bezig met starten van Streamlit Dashboard...
echo Applicatie opent in uw browser (http://localhost:8501)...
echo.
streamlit run app.py
pause
