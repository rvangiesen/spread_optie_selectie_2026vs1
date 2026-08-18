# 🚀 Handleiding: Direct Clonen & 1-Klik Installatie op Laptop

Deze handleiding legt stap-voor-stap uit hoe **AntiGravity Project 2 (Spread Selector)** 1-op-1 kan worden gekloond van GitHub en direct kan worden geïnstalleerd op een willekeurige laptop of PC, **zonder dat een AI-agent of ontwikkelaar code-aanpassingen hoeft te doen**.

---

## 📋 Inhoudsopgave
1. [Waarom Geen `.venv` in GitHub?](#1-waarom-geen-venv-in-github)
2. [Snelle Opstart (3 Eenvoudige Stappen)](#2-snelle-opstart-3-eenvoudige-stappen)
3. [Instructie voor AntiGravity / AI Agents op de Laptop](#3-instructie-voor-antigravity--ai-agents-op-de-laptop)
4. [Werken met Live TWS vs. Gratis yFinance Fallback](#4-werken-met-live-tws-vs-gratis-yfinance-fallback)
5. [Probleemoplossing (Troubleshooting)](#5-probleemoplossing-troubleshooting)

---

## 1. Waarom Geen `.venv` in GitHub?
Een Python virtuele omgeving (`.venv`) bevat **OS- en machinespecifieke binaire paden** (bijvoorbeeld naar `C:\Users\Gebruiker\...` op uw desktop). Als een `.venv` map rechtstreeks via Git gekloond zou worden naar een laptop met een andere merknaam, gebruikersnaam of mapstructuur, zou Python **onmiddellijk foutmeldingen geven** ("Path not found" of "Invalid executable").

**De Oplossing**:
Wij hebben een geautomatiseerd setup-script ontwikkeld (`setup_laptop.py` & `setup_laptop.bat`). Dit script bouwt op de laptop binnen **30 seconden** een verse, perfect afgestemde `.venv` op op basis van de exacte versies in `requirements.txt`.

---

## 2. Snelle Opstart (3 Eenvoudige Stappen)

### Stap 1: Kloon de Repository op de Laptop
Open een terminal/command prompt op uw laptop in de gewenste map (bijv. `C:\Users\Laptop\Documents\Python_Projecten`) en voer uit:

```bash
git clone https://github.com/rvangiesen/infrasound_lfg_toolkit.git "AntiGravity Project 2_ spreadselectie_ setup via AG"
```
*(Of gebruik uw GitHub Desktop applicatie om de repository naar de laptop te clonen).*

---

### Stap 2: Voer de 1-Klik Setup Uit
Ga op de laptop naar de gekloonde projectmap en dubbelklik op:

👉 **`setup_laptop.bat`**  *(of voer `python setup_laptop.py` uit in de terminal)*

**Wat doet dit automatisch voor u?**
- Controleert of Python 3.10+ aanwezig is.
- Maakt automatisch een schone virtuele omgeving (`.venv`) aan op de laptop.
- Installeert en upgrade alle benodigde pakketten (`streamlit`, `yfinance`, `ib_insync`, `plotly`, `openpyxl`, `python-docx`, `scipy`, etc.).
- Genereert het dubbelklikbare opstartbestand `run_laptop.bat`.
- Controleert alle Python-bestanden (`app.py`, `logic.py`, `hitrate_backtester.py`, etc.) op correcte syntaxis.

---

### Stap 3: Start de Applicatie
Dubbelklik op de laptop op:

👉 **`run_laptop.bat`**

De browser wordt automatisch geopend op `http://localhost:8501` en de Spread Selector is direct 100% operationeel!

---

## 3. Instructie voor AntiGravity / AI Agents op de Laptop

Als u op de laptop met een AI-agent (zoals AntiGravity of Claude) werkt, kunt u de agent de volgende instructie geven zodat deze **niets aan de code aanpast**:

> **Instructie voor AI Agent:**  
> *"Dit repository is reeds 100% geoptimaliseerd en gevalideerd. Alle functies (inclusief EM85-selectie, AG-Score, en de Hit-Rate test) zijn gereed. Gelieve de codebestanden `app.py`, `logic.py` en `hitrate_backtester.py` NIET te herschrijven of te wijzigen, tenzij ik expliciet om een nieuwe functie vraag. Om de app te starten of afhankelijkheden te herstellen, voer `python setup_laptop.py` of `streamlit run app.py` uit vanuit de `.venv`."*

---

## 4. Werken met Live TWS vs. Gratis yFinance Fallback

De applicatie werkt op de laptop in **twee modi**:

1. **Weekend- / Standalone-modus (Zonder TWS)**:
   - Heeft u op de laptop geen Interactive Brokers TWS/Gateway draaien?
   - Vink in de sidebar van de app de optie **"Gebruik Gratis yFinance Marktdata"** aan.
   - U kunt direct scannen, optiechains analyseren en de **Maandelijkse Hit-Rate Test** uitvoeren zonder dat TWS open hoeft te staan!

2. **Live TWS Modus (Met Interactive Brokers)**:
   - Zorg dat TWS of IB Gateway open staat op de laptop.
   - Controleer bij *TWS Settings -> API -> Settings*:
     - **Enable ActiveX and Socket Clients**: Aangevinkt ✅
     - **Socket Port**: `7496` (Live) of `7497` (Paper Trading)
     - **Allow connections from localhost only**: Aangevinkt ✅
   - Vul in de app-sidebar Host `127.0.0.1` en Port `7496` (of `7497`) in.

---

## 5. Probleemoplossing (Troubleshooting)

| Probleem | Oorzaak | Oplossing |
| :--- | :--- | :--- |
| `ModuleNotFoundError` | `.venv` niet geactiveerd of pakketten niet geïnstalleerd | Dubbelklik op `setup_laptop.bat` om de omgeving automatisch te herstellen. |
| `TimeoutError` bij TWS verbinding | TWS staat niet open of API poort is onjuist | Start TWS of vink in de sidebar *"Gebruik Gratis yFinance Marktdata"* aan. |
| `Python command not found` | Python staat niet in het Windows PATH | Installeer Python via [python.org](https://www.python.org) en vink *"Add python.exe to PATH"* aan. |
