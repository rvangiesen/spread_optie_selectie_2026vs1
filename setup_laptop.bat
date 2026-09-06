@echo off
TITLE AntiGravity Project 2 - Laptop Setup
echo ============================================================
echo   AntiGravity Project 2: Spread Selector - Laptop Setup
echo ============================================================
echo.
cd /d "%~dp0"
echo Bezig met starten van setup_laptop.py...
where python >nul 2>&1
if %errorlevel% equ 0 (
    python setup_laptop.py
) else (
    where py >nul 2>&1
    if %errorlevel% equ 0 (
        py setup_laptop.py
    ) else (
        echo [FOUT] Python is niet gevonden in het Windows PATH.
        echo Installeer Python 3.10+ via https://www.python.org/ en vink "Add python.exe to PATH" aan!
    )
)
pause
