import sys

path = r'c:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG\app.py'

with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

def check_range(start, end):
    print(f"--- Range {start}-{end} ---")
    for i in range(start-1, end):
        if i < len(lines):
            try:
                line_repr = repr(lines[i]).encode('ascii', 'ignore').decode('ascii')
                print(f"Line {i+1:3}: {len(lines[i]) - len(lines[i].lstrip()):3} spaces | {line_repr}")
            except:
                print(f"Line {i+1:3}: (encode error)")

check_range(220, 245)
