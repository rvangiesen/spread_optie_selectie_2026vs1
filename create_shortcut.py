import os
import win32com.client

def create_desktop_shortcut():
    # 1. Project map en batch script pad
    project_dir = os.path.abspath(os.path.dirname(__file__))
    batch_file = os.path.join(project_dir, "run_spreadselector_and_lynx.bat")
    
    # 2. Bepaal Bureaublad pad via Windows Script Host
    wsh_shell = win32com.client.Dispatch("WScript.Shell")
    desktop_dir = wsh_shell.SpecialFolders("Desktop")
    shortcut_path = os.path.join(desktop_dir, "AntiGravity Spread Selector.lnk")
    
    print(f"Snelkoppeling wordt aangemaakt op: {shortcut_path}")
    
    # 3. Maak snelkoppeling object
    shortcut = wsh_shell.CreateShortcut(shortcut_path)
    
    # Doel instellen op cmd.exe voor optimale taakbalk pinning en compatibiliteit
    shortcut.TargetPath = os.path.expandvars("%SystemRoot%\\System32\\cmd.exe")
    shortcut.Arguments = f'/k "{batch_file}"'
    shortcut.WorkingDirectory = project_dir
    
    # Icoon instellen (Grafiek/Chart icoon uit shell32.dll, index 13)
    icon_source = os.path.expandvars("%SystemRoot%\\System32\\shell32.dll")
    shortcut.IconLocation = f"{icon_source}, 13"
    
    shortcut.Description = "AntiGravity Spread Selector & LYNX Paper Trade Launcher"
    
    # Opslaan
    shortcut.Save()
    print("[SUCCESS] Snelkoppeling succesvol aangemaakt op je Bureaublad!")
    print("-> Rechtsklik op 'AntiGravity Spread Selector' op je bureaublad en kies 'Aan taakbalk vastpinnen'.")

if __name__ == "__main__":
    create_desktop_shortcut()
