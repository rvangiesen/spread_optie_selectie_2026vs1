# 🚀 Handleiding: Project 2 Opstarten & GitHub Beheer

Deze handleiding legt stap-voor-stap uit hoe je de **AntiGravity Optie Contract Selectie Tool** (Project 2) opstart in Python en hoe je wijzigingen bijwerkt op GitHub.

---

## 1. Project 2 laden en opstarten in Python

Er zijn twee manieren om het project te laden: de moderne en snelle methode met **uv** (aanbevolen, aangezien er een `uv.lock` bestand in de map staat), of de traditionele methode met een standaard Python virtuele omgeving (`venv`).

### Optie A: Opstarten met `uv` (Snelle methode ⚡)
Als je **uv** (een moderne Python package manager) hebt geïnstalleerd, is het opstarten heel eenvoudig:

1. Open de commandline (PowerShell of Command Prompt) in de projectmap:
 `c:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG`
2. Run de app direct met:
 ```bash
 uv run streamlit run app.py
 ```
 *Dit commando zorgt er automatisch voor dat alle benodigde bibliotheken uit `pyproject.toml` in een tijdelijke, schone omgeving worden geladen en start Streamlit.*

---

### Optie B: Opstarten met een traditionele Virtuele Omgeving (`.venv` 🐍)
Als je liever met een standaard Python-omgeving werkt (bijvoorbeeld via **VS Code** of **PyCharm**):

#### Stap 1: Open de projectmap
Open je favoriete editor (VS Code, PyCharm) en kies **Open Folder** (Map openen). Selecteer de projectmap:
`c:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG`

#### Stap 2: Virtuele Omgeving activeren
Open de terminal in je editor. Als er al een `.venv` map bestaat, activeer deze dan:
* **In Windows (PowerShell)**:
 ```powershell
 .venv\Scripts\Activate.ps1
 ```
* **In Windows (CMD)**:
 ```cmd
 .venv\Scripts\activate.bat
 ```
*Zie je `(.venv)` voor je prompt staan? Dan is de activatie gelukt!*

*Als er nog geen virtuele omgeving is, maak er dan eerst een aan:*
```bash
python -m venv .venv
```
Activeer hem daarna en installeer de vereiste pakketten:
```bash
pip install -r requirements.txt
```

#### Stap 3: De applicatie starten
Zodra de omgeving is geactiveerd, start je de Streamlit interface met:
```bash
streamlit run app.py
```
Er opent nu automatisch een browservenster op `http://localhost:8501` met de AntiGravity tool.

---

### Optie C: Direct dubbelklikken via Batchbestand 🚀
In de projectmap staat een kant-en-klaar batchbestand:
* **`Start_SpreadSelectie_LYNX_Paper.bat`**: Start de virtuele omgeving en lanceert Streamlit automatisch met één dubbelklik!

---

## 2. Wijzigingen bijwerken op GitHub (Git Handleiding)

Als je code of de handleiding hebt aangepast en je wilt dit uploaden naar je GitHub repository (https://github.com/rvangiesen/spread_optie_selectie_2026vs1), volg dan deze stappen in de terminal van je projectmap.

> [!TIP]
> **Git niet herkend in PowerShell?**
> Op deze computer is Git meegeleverd via **GitHub Desktop**. Als het commando `git` niet direct reageert, kun je het volledige pad gebruiken of het eenmalig toevoegen aan je PATH:
> `& "$env:LOCALAPPDATA\GitHubDesktop\app-3.6.3\resources\app\git\cmd\git.exe" status`

### Stap 1: Controleer de status
Kijk welke bestanden zijn gewijzigd of nieuw zijn toegevoegd:
```bash
git status
```
Je ziet nu een lijst met gewijzigde bestanden (in het rood).

### Stap 2: Bestanden klaarzetten voor de commit (Staging)
* **Alle wijzigingen selecteren**:
 ```bash
 git add .
 ```
* **Specifieke bestanden selecteren** (bijvoorbeeld alleen de handleiding en de app):
 ```bash
 git add Handleiding_Optie_Contract_Selectie.md app.py
 ```

### Stap 3: De wijzigingen vastleggen (Commit)
Geef een duidelijke, beschrijvende boodschap mee aan je wijziging:
```bash
git commit -m "feat: early assignment risk engine, capital-aware bullput/bullcall selection, and tws error 201 fix"
```

### Stap 4: Uploaden naar GitHub (Push)
Stuur de opgeslagen wijzigingen naar de online GitHub-omgeving:
```bash
git push origin main
```
Je wijzigingen staan nu live op GitHub!

---

## 3. Handige Git commando's bij problemen

* **Wijzigingen ophalen van GitHub** (als je op een andere pc hebt gewerkt):
 ```bash
 git pull origin main
 ```
* **Tijdelijk je werk aan de kant zetten** (bijvoorbeeld als je een foutmelding krijgt dat je werkruimte niet schoon is):
 ```bash
 git stash -u
 ```
* **Je stashed werk weer terugzetten**:
 ```bash
 git stash pop
 ```

---

## 4. Belangrijkste Recente Updates in de Codebase

1. **🛡️ Early Assignment Risk Engine & Tijdswaardebewaking**:
 - Monitort continu de resterende extrinsieke waarde van de geschreven poot.
 - Bij 0,10 dollar of minder resterende tijdswaarde verschijnt een waarschuwing (`⚠️ GEVARENZONE`), bij 0,05 dollar of minder slaat het systeem alarm (`🚨 DIRECT SLUITEN`) en adviseert het direct sluiten als combinatieorder.
2. **⚖️ Kapitaalbewuste Selectie: Bull Put vs Bull Call**:
 - *Vuistregel: "Heb je genoeg geld staan dan bullputs, anders alleen maar bullcalls."*
 - Vergelijkt het nominale toewijzingskapitaal (Uitoefenprijs x 100 aandelen) met je beschikbare cash.
 - Bij onvoldoende cash krijgen veilige Bull Call debet spreads (maximaal risico beperkt tot debet) voorrang boven Bull Puts.
 - Met het nieuwe zijbalkfilter *'Strikt filteren: verberg ongedekte Bull Puts'* worden ongedekte Bull Puts zelfs 100% verborgen.
3. **🔧 TWS Error 201 Oplossing (Canonieke Pootdefinitie)**:
 - Verhelpt de melding *"Gegarandeerd-verlies of risicoloze combinatie-orders zijn niet toegestaan"*.
 - Bracket orders (Take Profit & Stop Loss) voeren nu direct uit zonder poot-inversie.
4. **📊 Option Chain Predictiemodel & Gamma/Theta Engine (V1 & V2)**:
 - 4-Kwadranten Delta OI analyse voor trendvoorspelling (`🚀 UPTREND`, `🎯 PINNING`, etc.).
 - Vested Value steun- en weerstandsmuren (Marge x Openstaande contracten).
 - Breakeven dagelijkse koersbeweging (dS_BE), Bayesiaanse winstkans (winstkans (PoP_adj)) en Verwachte Winst (EV).

