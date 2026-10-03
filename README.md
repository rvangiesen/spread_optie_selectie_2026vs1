# 🚀 AntiGravity: Project 2 Spread Selectie Tool

Welkom bij de **AntiGravity Optie Contract Selectie Tool** (Project 2). Dit project haalt optie- en marktgegevens op van Interactive Brokers (TWS/Lynx) of Yahoo Finance, berekent geavanceerde risico- en winstparameters (waaronder Bjerksund-Stensland, Delta Koers en verwachte sluitingswinsten) en presenteert deze in een overzichtelijke Streamlit webapplicatie.

---

## 💻 1-Klik Laptop Installatie & Github Cloning
Wilt u dit project clonen naar een laptop of andere computer? 
Zie de volledige stap-voor-stap handleiding: **[Handleiding_Project2_Laptop_Installatie.md](Handleiding_Project2_Laptop_Installatie.md)**.

**Snelstart op Laptop:**
1. Kloon het project van GitHub: `git clone https://github.com/rvangiesen/spread_optie_selectie_2026vs1.git`
2. Dubbelklik op **`run_laptop.bat`** (bouwt bij eerste opstart automatisch de `.venv` en start de app direct!).
   *(Of voer eenmalig handmatig **`setup_laptop.bat`** uit).*

---

## 🌟 Nieuwste Functies & Validaties
* **📈 1-Maands Trend Model met Stochastic RSI Filter**: Geavanceerd 5-pijler trendmodel met automatische dip- en duikvluchtbeveiliging. Voorkomt instappen op aandelen die tijdelijk wegzakken (vallende messen). In de empirische evaluatie over 150 trades (5 aandelen, 10 instapmomenten, Bull Calls, Bull Puts en Long Calls) onder de standaard $10 spreadbreedte steeg de totale winstkans van 70,8% naar 81,0%, meer dan verdubbelde de nettowinst van $2.599 naar $5.617 (en zelfs $5.863 bij koersafhankelijke breedtes met een Profit Factor van 2,04), en werden losse Long Calls omgebogen van zwaar verliesgevend (-$3.998) naar solide winstgevend (+$1.526).
* **🛡️ Preventieve Ex-Dividend Toewijzingsbewaking**: Detecteert het risico op vroege uitoefening (Early Assignment) op geschreven calls (Bear Call Spreads, Covered Calls). Slaat direct alarm met `🚨 EX-DIV ARBITRAGE` wanneer het dividend per aandeel gelijk is aan of groter is dan de resterende tijdswaarde (waarbij de koper vrijwel zeker daags vóór ex-dividend zal uitoefenen), toont de datum in de kolom *Ex-Div Datum* en biedt een preventief uitsluitingsfilter (`🛡️ Ex-Dividend Toewijzingsfilter`) in de zijbalk.
* **🎯 Horizon-Bescherming in Auto-Optimalisatie**: Garandeert dat de gekozen beleggingshorizon van de handelaar (zoals *Maand Spreads 30–75 DTE* of een handmatige looptijd van minimaal 28 dagen) altijd strikt gerespecteerd wordt en niet meer wordt overschreven door een korter week-profiel.
* **🔥 Dual-Trigger Entry Strategie (Squeeze & Trend Pullback)**: Kwantitatief instapmodel gericht op 80–90% winstkans. Combineert uitbraken na extreme marktcompressie (Bollinger Bands binnen Keltner Channels) met trend pullbacks (vroege instap in een hervatte opwaartse trend). Volledig in- en uitschakelbaar in de scanner.
* **🎯 Bewakende Profit Stop Engine**: Dynamische winstbewaking met vaste winstdoelen (60%, 70% aanbevolen, 100%) en actieve trailing momentum exits zodra de kortetermijntrend afzwakt. Berekent direct het concrete limietorderadvies en de dollarwinst in de resultatentabel.
* **🧪 10-Trades per Aandeel Backtest Engine**: Historische signaal-detectie met gekalibreerde dag-op-dag optieprijsbepaling. Analyseert vroege winstexits en toont de verkorting in gemiddelde bewaartijd (bijv. van 21 naar 13,7 dagen met meer dan 80% hit rate).
* **⚖️ Kapitaalbewuste Selectie: Bull Put vs Bull Call**: Automatische bescherming tegen aandelen-toewijzing (Early Assignment). Berekent het benodigde kapitaal (Uitoefenprijs x 100 aandelen) en geeft bij beperkte cash automatisch voorrang aan risico-gelimiteerde Bull Call debet spreads.
* **🛡️ Early Assignment Risk Engine**: Real-time berekening van de resterende tijdswaarde en toewijzingskans op geschreven opties. Waarschuwing bij $0,10 of minder tijdswaarde en direct noodsluitingsalarm bij $0,05 of minder.
* **🔧 TWS Error 201 Fix**: Canonieke pootdefinitie voor credit spreads, waardoor gecombineerde winst- en stoploss-orders direct en foutloos worden geaccepteerd door Interactive Brokers.
* **📊 Option Chain Predictiemodel**:
  - 4-Kwadranten Delta OI analyse voor richting- en regimedetectie (`🚀 UPTREND`, `🎯 PINNING`, etc.).
  - Institutionele verdedigingsmuren (Marge x Openstaande contracten) als werkelijke steun- en weerstandslijnen.
  - Berekening van de dagelijkse veilige breakeven koersuitslag (dS_BE), gecorrigeerde winstkans (PoP) en verwachte dollarwinst (EV).
* **🛡️ Anti-Assignment Verdedigingsroutine (Tab 0: Portfolio Bewaking)**: Live monitoring van alle openstaande optieposities in het handelsplatform met 1-klik noodsluiting als combinatieorder.
* **⚡ 1-Klik Optimalisatie & Benchmark**: Evalueer instellingen tegen de gevalideerde benchmark en backtest met 1 klik.

---

## 📖 Gebruikershandleidingen
* **Desktop Installatie (1-Klik na Thuiskomst)**: [Handleiding_Project2_Desktop_Installatie.md](Handleiding_Project2_Desktop_Installatie.md) (1-klik clonen en opstarten via AntiGravity Agent).
* **Laptop Installatie**: [Handleiding_Project2_Laptop_Installatie.md](Handleiding_Project2_Laptop_Installatie.md) (1-klik setup, `.venv` herstel & agent instructies).
* **Gebruik van de App & Resultaten Gids**: [Handleiding_Optie_Contract_Selectie.md](Handleiding_Optie_Contract_Selectie.md) (volledige uitleg van alle knoppen, EM85, filters, sentiment, orderplaatsing, en in **Sectie 22** een complete gids van alle resultatenkolommen, Gamma/Theta ratio en het A-B-C-D besluitvormingsmodel).
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
