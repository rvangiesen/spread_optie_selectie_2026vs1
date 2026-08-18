@echo off
TITLE AntiGravity Project 2 - Spread Selector
echo ============================================================
echo   AntiGravity Project 2: Spread Selector (Laptop Mode)
echo ============================================================
echo.
cd /d "C:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG"
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
)
echo Bezig met starten van Streamlit Dashboard...
streamlit run app.py
pause
