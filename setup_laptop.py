import os
import sys
import subprocess
import shutil
from pathlib import Path

def print_header(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

def main():
    print_header("AntiGravity Project 2: Spread Selector - Laptop Setup Script")
    
    project_dir = Path(__file__).parent.resolve()
    os.chdir(project_dir)
    print(f"📂 Project Directory: {project_dir}")

    # 1. Check Python Version
    python_ver = sys.version_info
    print(f"🐍 Geselecteerde Python Versie: {python_ver.major}.{python_ver.minor}.{python_ver.micro}")
    if python_ver.major < 3 or (python_ver.major == 3 and python_ver.minor < 10):
        print("❌ FOUT: Python 3.10 of hoger is vereist. Installeer een recentere Python versie.")
        sys.exit(1)
        
    venv_dir = project_dir / ".venv"
    venv_python = venv_dir / "Scripts" / "python.exe" if os.name == 'nt' else venv_dir / "bin" / "python"
    
    # 2. Create Virtual Environment if missing
    if not venv_python.exists():
        print("🔨 Bezig met aanmaken van nieuwe virtuele omgeving (.venv)...")
        subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
        print("✅ Virtuele omgeving (.venv) succesvol aangemaakt.")
    else:
        print("✅ Bestaande .venv omgeving gevonden.")

    # 3. Upgrade Pip & Install Dependencies
    print("📦 Bezig met installeren/upgraden van vereiste Python pakketten uit requirements.txt...")
    req_file = project_dir / "requirements.txt"
    if req_file.exists():
        subprocess.run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"], check=True)
        subprocess.run([str(venv_python), "-m", "pip", "install", "-r", str(req_file)], check=True)
        print("✅ Alle afhankelijkheden zijn succesvol geïnstalleerd!")
    else:
        print("⚠️ WAARSCHUWING: requirements.txt niet gevonden.")

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
    print("✅ Start-script aangemaakt: run_laptop.bat")

    setup_bat = project_dir / "setup_laptop.bat"
    setup_bat_content = f"""@echo off
TITLE AntiGravity Project 2 - Laptop Setup
cd /d "{project_dir}"
python setup_laptop.py
pause
"""
    with open(setup_bat, "w", encoding="utf-8") as f:
        f.write(setup_bat_content)
    print("✅ Setup-script aangemaakt: setup_laptop.bat")

    # 5. Verify compilation of key python files
    print("\n🔍 Bezig met verifiëren van bronbestanden...")
    files_to_check = ["app.py", "logic.py", "hitrate_backtester.py", "research_runner.py", "ib_client.py"]
    for fname in files_to_check:
        fpath = project_dir / fname
        if fpath.exists():
            res = subprocess.run([str(venv_python), "-m", "py_compile", str(fpath)], capture_output=True, text=True)
            if res.returncode == 0:
                print(f"  - {fname}: ✅ Syntax OK")
            else:
                print(f"  - {fname}: ❌ FOUT:\n{res.stderr}")
        else:
            print(f"  - {fname}: ⚠️ Niet gevonden")

    print_header("🎉 INSTALLATIE EN CONFIGURATIE VOLTOOID!")
    print("U kunt de applicatie direct starten op de laptop door te dubbelklikken op:")
    print(f"👉 {run_bat}")

if __name__ == "__main__":
    main()
