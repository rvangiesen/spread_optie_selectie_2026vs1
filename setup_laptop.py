import os
import sys
import subprocess
from pathlib import Path

def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', 'ignore').decode('ascii'))

def print_header(title):
    safe_print("\n" + "=" * 60)
    safe_print(f"  {title}")
    safe_print("=" * 60)

def main():
    print_header("AntiGravity Project 2: Spread Selector - Laptop Setup Script")
    
    project_dir = Path(__file__).parent.resolve()
    os.chdir(project_dir)
    safe_print(f"📂 Project Directory: {project_dir}")

    # 1. Check Python Version
    python_ver = sys.version_info
    safe_print(f"🐍 Geselecteerde Python Versie: {python_ver.major}.{python_ver.minor}.{python_ver.micro}")
    if python_ver.major < 3 or (python_ver.major == 3 and python_ver.minor < 10):
        safe_print("❌ FOUT: Python 3.10 of hoger is vereist. Installeer een recentere Python versie.")
        sys.exit(1)
        
    venv_dir = project_dir / ".venv"
    venv_python = venv_dir / "Scripts" / "python.exe" if os.name == 'nt' else venv_dir / "bin" / "python"
    
    # 2. Create Virtual Environment if missing or incomplete
    if not venv_python.exists():
        safe_print("🔨 Bezig met aanmaken van nieuwe virtuele omgeving (.venv)...")
        subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
        safe_print("✅ Virtuele omgeving (.venv) succesvol aangemaakt.")
    else:
        safe_print("✅ Bestaande .venv omgeving gevonden.")

    # Check pip inside venv
    res_pip = subprocess.run([str(venv_python), "-m", "pip", "--version"], capture_output=True)
    if res_pip.returncode != 0:
        safe_print("🔧 Pip initialiseren in de virtuele omgeving...")
        subprocess.run([str(venv_python), "-m", "ensurepip", "--upgrade"], check=False)

    # 3. Upgrade Pip & Install Dependencies
    safe_print("📦 Bezig met installeren/upgraden van vereiste Python pakketten uit requirements.txt...")
    req_file = project_dir / "requirements.txt"
    if req_file.exists():
        pip_cmd = [str(venv_python), "-m", "pip"] if res_pip.returncode == 0 else [sys.executable, "-m", "pip"]
        subprocess.run(pip_cmd + ["install", "-r", str(req_file)], check=True)
        safe_print("✅ Alle afhankelijkheden zijn succesvol geïnstalleerd!")
    else:
        safe_print("⚠️ WAARSCHUWING: requirements.txt niet gevonden.")

    # 4. Generate Launcher Batch Files (.bat) for Windows
    run_bat = project_dir / "run_laptop.bat"
    run_bat_content = f"""@echo off
TITLE AntiGravity Project 2 - Spread Selector
echo ============================================================
echo   AntiGravity Project 2: Spread Selector (Laptop Mode)
echo ============================================================
echo.
cd /d "{project_dir}"
if exist ".venv\\Scripts\\activate.bat" (
    call .venv\\Scripts\\activate.bat
)
echo Bezig met starten van Streamlit Dashboard...
streamlit run app.py
pause
"""
    with open(run_bat, "w", encoding="utf-8") as f:
        f.write(run_bat_content)
    safe_print("✅ Start-script aangemaakt: run_laptop.bat")

    setup_bat = project_dir / "setup_laptop.bat"
    setup_bat_content = f"""@echo off
TITLE AntiGravity Project 2 - Laptop Setup
cd /d "{project_dir}"
python setup_laptop.py
pause
"""
    with open(setup_bat, "w", encoding="utf-8") as f:
        f.write(setup_bat_content)
    safe_print("✅ Setup-script aangemaakt: setup_laptop.bat")

    # 5. Verify compilation of key python files
    safe_print("\n🔍 Bezig met verifiëren van bronbestanden...")
    files_to_check = ["app.py", "logic.py", "hitrate_backtester.py", "research_runner.py", "ib_client.py"]
    for fname in files_to_check:
        fpath = project_dir / fname
        if fpath.exists():
            res = subprocess.run([sys.executable, "-m", "py_compile", str(fpath)], capture_output=True, text=True)
            if res.returncode == 0:
                safe_print(f"  - {fname}: ✅ Syntax OK")
            else:
                safe_print(f"  - {fname}: ❌ FOUT:\n{res.stderr}")
        else:
            safe_print(f"  - {fname}: ⚠️ Niet gevonden")

    print_header("🎉 INSTALLATIE EN CONFIGURATIE VOLTOOID!")
    safe_print("U kunt de applicatie direct starten op de laptop door te dubbelklikken op:")
    safe_print(f"👉 {run_bat}")

if __name__ == "__main__":
    main()
