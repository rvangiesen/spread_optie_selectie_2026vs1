import sys
import subprocess

if __name__ == '__main__':
    # Use subprocess to run streamlit, avoiding direct import issues in some IDEs
    cmd = [sys.executable, "-m", "streamlit", "run", "app.py"]
    subprocess.run(cmd)
