# 🚀 AntiGravity: Project 2 Spread Selectie Tool

Welkom bij de **AntiGravity Optie Contract Selectie Tool** (Project 2). Dit project haalt optie- en marktgegevens op van Interactive Brokers (TWS/Lynx) of Yahoo Finance, berekent geavanceerde risico- en winstparameters (waaronder Bjerksund-Stensland, Delta Koers en verwachte sluitingswinsten) en presenteert deze in een overzichtelijke Streamlit webapplicatie.

---

## 💻 1-Klik Laptop Installatie & Github Cloning
Wilt u dit project clonen naar een laptop of andere computer? 
Zie de volledige stap-voor-stap handleiding: **[Handleiding_Project2_Laptop_Installatie.md](Handleiding_Project2_Laptop_Installatie.md)**.

**Snelstart op Laptop:**
1. Kloon het project van GitHub: `git clone https://github.com/rvangiesen/infrasound_lfg_toolkit.git`
2. Dubbelklik op **`setup_laptop.bat`** (bouwt automatisch een schone `.venv` op en installeert alle vereiste pakketten).
3. Dubbelklik op **`run_laptop.bat`** om de app direct te starten!

---

## 📖 Gebruikershandleidingen
* **Laptop Installatie**: [Handleiding_Project2_Laptop_Installatie.md](Handleiding_Project2_Laptop_Installatie.md) (1-klik setup, `.venv` herstel & agent instructies).
* **Gebruik van de App**: [Handleiding_Optie_Contract_Selectie.md](Handleiding_Optie_Contract_Selectie.md) (uitleg van knoppen, EM85, filters, sentimentmodel, ranking en orderplaatsing).
* **Project Opstarten & GitHub**: [Handleiding_Project2_Opstarten_en_Git.md](Handleiding_Project2_Opstarten_en_Git.md).

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

## 2. Wijzigingen bijwerken op GitHub (Git Handleiding)

Als je code of de handleiding hebt aangepast en je wilt dit uploaden naar je GitHub repository (https://github.com/rvangiesen/spread_optie_selectie_2026vs1), volg dan deze stappen in de terminal van je projectmap.

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
Geef een korte, beschrijvende boodschap mee aan je wijziging:
```bash
git commit -m "Beschrijf hier kort wat je hebt aangepast (bijv: Update handleiding)"
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
