import os

def fix_logic():
    path = r'c:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG\logic.py'
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Fix PoP round() lint
    old_pop = """                 if row['strategy'] in ['BullPut', 'BearCall']:
                     # Credit: Sell leg is usually OTM. PoP = 1 - Delta
                     pops.append(round((1.0 - ds) * 100, 1))
                 else:
                     # Debit: Buy leg is what matters. PoP approximates Delta of Buy leg if ITM.
                     # For the user's 650/660 SPY example, DeltaBuy would be ~0.8.
                     pops.append(round(db * 100, 1))"""
    
    new_pop = """                 if row['strategy'] in ['BullPut', 'BearCall']:
                     # Credit: Sell leg is usually OTM. PoP = 1 - Delta
                     pops.append(float(round(float((1.0 - ds) * 100), 1)))
                 else:
                     # Debit: Buy leg is what matters. PoP approximates Delta of Buy leg if ITM.
                     pops.append(float(round(float(db * 100), 1)))"""
    
    content = content.replace(old_pop, new_pop)
    
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed logic.py")

def fix_app():
    path = r'c:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG\app.py'
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Block 1: Technical & Support Check (Lines 276-282 approx)
    new_lines = []
    found_block1 = False
    for line in lines:
        if 'if use_ema and ema_spans:' in line and 'A. Technical Check' in lines[lines.index(line)-1]:
            indent = line[:line.find('if')]
            new_lines.append(f"{indent}# A. Technical & Support Check\n")
            new_lines.append(f"{indent}hist_data = scan_ib.get_historical_data(contract, duration='1 Y', bar_size='1 day')\n")
            new_lines.append(f"{indent}if hist_data.empty:\n")
            new_lines.append(f"{indent}    log(f'   ⚠️ Geen historische data beschikbaar voor {{sym}}')\n")
            new_lines.append(f"{indent}    continue\n")
            new_lines.append(f"{indent}\n")
            new_lines.append(f"{indent}# Zoek Significante Bodems (Support Levels)\n")
            new_lines.append(f"{indent}support_levels = scanner.find_support_levels(hist_data)\n")
            new_lines.append(f"{indent}if support_levels:\n")
            new_lines.append(f"{indent}    log(f'   📉 Significante bodems gevonden: {{ \", \".join([f\"${{s:.2f}}\" for s in support_levels[-3:]]) }}')\n")
            new_lines.append(f"\n")
            new_lines.append(line)
            found_block1 = True
            # Skip the next few lines of old block
            # We'll handle this by tracking current line in loop or just replacing the specific lines
        elif found_block1 and ('# log(f"   📊 Ophalen historische data' in line or 'hist_data = scan_ib.get_historical_data' in line or 'if hist_data.empty:' in line or 'log(f"   ⚠️ Geen historische' in line or 'continue' in line and 'Geen historische' in lines[lines.index(line)-1]):
            # Skip the old block
             continue
        else:
            new_lines.append(line)

    # Re-process to fix Block 2 (Strike Range)
    final_lines = []
    for line in new_lines:
        if 'lower_bound = price * 0.90' in line:
            final_lines.append(line.replace('0.90', '0.70'))
        elif 'upper_bound = price * 1.10' in line:
            final_lines.append(line.replace('1.10', '1.30'))
        else:
            final_lines.append(line)

    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(final_lines)
    print("Fixed app.py")

if __name__ == "__main__":
    fix_logic()
    fix_app()
