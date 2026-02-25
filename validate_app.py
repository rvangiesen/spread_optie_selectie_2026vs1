import ast
import traceback

path = r'c:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG\app.py'

with open(path, 'r', encoding='utf-8') as f:
    source = f.read()

try:
    ast.parse(source)
    print("AST parse successful! No syntax errors.")
except SyntaxError as e:
    print(f"Syntax Error: {e.msg}")
    print(f"Line: {e.lineno}")
    print(f"Offset: {e.offset}")
    if e.text:
         print(f"Text: {repr(e.text)}")
    
    # Let's see the lines around the error
    lines = source.splitlines()
    start = max(0, e.lineno - 5)
    end = min(len(lines), e.lineno + 5)
    for i in range(start, end):
        prefix = "-> " if i + 1 == e.lineno else "   "
        print(f"{prefix}{i+1:3}: {len(lines[i]) - len(lines[i].lstrip()):3} spaces | {repr(lines[i])}")
except Exception:
    traceback.print_exc()
