import sys
path = r"c:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG\app.py"
with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Line 484 is index 483
if 'if price > 0:' in lines[483] and 'if price' not in lines[482]:
    lines[483] = "                                          if price > 0:\n"
    lines[484] = "                                              lower_bound = price * 0.70\n"
    lines[485] = "                                              upper_bound = price * 1.30\n"
    lines[486] = "                                              wide_strikes = [s for s in valid_strikes_for_exp if lower_bound <= s <= upper_bound]\n"
    lines[487] = "                                          else:\n"
    lines[488] = "                                              wide_strikes = valid_strikes_for_exp\n"
    
    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print("Fixed line 484 indentation")
else:
    print(f"Index 483 content: {repr(lines[483])}")
    print(f"Index 482 content: {repr(lines[482])}")
