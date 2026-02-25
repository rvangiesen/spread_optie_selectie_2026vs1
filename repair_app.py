path = r'c:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG\app.py'

with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
# Base level for 'else' body is 29 (since 'else:' is at 25)

for i, line in enumerate(lines):
    num = i + 1
    content = line.lstrip()
    if not content:
        new_lines.append(line)
        continue
    
    current_indent = len(line) - len(content)
    
    # Process reporting block after the loop
    if 572 <= num <= 610:
        # These lines are at 28, 32, 36... they should be at 29, 33, 37...
        if current_indent in [28, 32, 36, 40, 44]:
            new_lines.append(' ' * (current_indent + 1) + content)
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open(path, 'w', encoding='utf-8', newline='\n') as f:
    f.writelines(new_lines)

print("Reporting Block INDENT Repair completed.")
