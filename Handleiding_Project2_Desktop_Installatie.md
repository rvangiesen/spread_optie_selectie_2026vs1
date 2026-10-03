# 🖥️ Handleiding: 1-Klik Installatie & Clonen naar Desktop via AntiGravity Agent

Deze handleiding is speciaal geschreven voor wanneer u terugkeert van vakantie en de nieuwste versie van **AntiGravity Project 2 (Spread Optie Selectie)** in **één enkele stap** op uw desktopcomputer wilt overzetten en direct foutloos wilt gaan gebruiken.

---

## ⚡ De Snelste Methode: Geef 1 Prompt aan de AntiGravity Agent op de Desktop

Zodra u morgen achter uw desktopcomputer zit en de AntiGravity IDE opent, kopieert en plakt u simpelweg onderstaande tekst in het chatvenster van de AntiGravity Agent:

```text
Wil je de nieuwste versie van Project 2 ophalen van GitHub (https://github.com/rvangiesen/spread_optie_selectie_2026vs1.git), de virtuele omgeving herstellen/inrichten voor deze computer via python setup_laptop.py, controleren of alle bestanden up-to-date zijn, en de applicatie direct opstarten?
```

**Wat doet de AntiGravity Agent op de desktop vervolgens automatisch voor u?**
1. **GitHub Synchronisatie**: Haalt via `git pull` (of `git clone` als de map nog nieuw is) direct alle recente updates, analyses en handleidingen binnen.
2. **Schone Lokale Omgeving (`.venv`)**: Omdat een virtuele omgeving machinespecifieke systeempaden bevat, bouwt het script `setup_laptop.py` automatisch binnen 30 seconden een verse, perfect werkende omgeving op uw desktop.
3. **Validatie**: Het controleert of alle onderdelen foutloos compileren.
4. **Directe Opstart**: Lanceert de interactieve selectie-app in uw webbrowser op `http://localhost:8501`.

---

## 🛠️ Handmatige 1-Klik Methode (Zonder Agent)

Wilt u de applicatie liever zelf met één muisklik starten zonder de agent?

### Stap 1: Nieuwste wijzigingen ophalen
Staat de map al op uw desktop? Open een terminal/PowerShell in de projectmap en typ:
```bash
git pull origin main
```
*(Staat het project nog niet op uw desktop? Voer dan uit: `git clone https://github.com/rvangiesen/spread_optie_selectie_2026vs1.git`)*

### Stap 2: Dubbelklik op het Setup- of Startbestand
* **Eerste keer opstarten**: Dubbelklik op **`setup_laptop.bat`**. Dit zorgt ervoor dat alle vereiste bibliotheken voor de desktopcomputer worden klaargezet.
* **Daarna dagelijks starten**: Dubbelklik op **`Start_SpreadSelectie_LYNX_Paper.bat`** of **`run_laptop.bat`**.

De browser opent direct en u kunt meteen beginnen met scannen!

---

## 🌟 Wat is er nieuw in deze versie?

Wanneer u morgen opstart, profiteert u direct van de nieuwste innovaties:
1. **📈 1-Maands Trend Model met Stochastic RSI Filter**:
   - Uitgebreid naar een 5-pijler trendmodel met automatische dip-beveiliging.
   - **Voorkomt instappen op vallende messen**: Zelfs als een aandeel een positieve maandtrend heeft, wordt een kooptransactie direct tegengehouden zolang het kortetermijnmomentum omlaag wijst.
   - **Empirisch bewezen**: In een historische validatie over 150 trades onder de standaard $10 en koersafhankelijke spreadbreedtes steeg de winstkans naar **81,0%**, verdubbelde de nettowinst naar ruim **$5.600 tot $5.860** en werden losse Long Calls omgebogen van zwaar verlieslatend naar solide winstgevend.
2. **🛡️ Ex-Dividend Toewijzingsbewaking**:
   - Automatische detectie van toewijzingsgevaar op geschreven calls daags voor de ex-dividenddatum.
3. **⚖️ Kapitaalbewuste Selectie (Bull Call vs Bull Put)**:
   - Automatische afstemming van credit vs debet spreads op basis van uw beschikbare liquiditeiten.
4. **🛡️ Live Portfolio Bewakingsdashboard (Tab 0)**:
   - Real-time controle van al uw openstaande opties met OmniTrader trailing stops en 1-klik noodsluiting.

---

## 💡 Handige Tips voor een Vlekkeloze Start

* **Met Trader Workstation (TWS / LYNX)**: Start TWS op en controleer of in *Global Configuration -> API -> Settings* de optie *"Enable ActiveX and Socket Clients"* aanstaat met poort `7496` (Live) of `7497` (Paper).
* **Zonder TWS (Weekend of 's Avonds)**: Vink in de linker zijbalk van de app de optie *"Gebruik Gratis Yahoo Finance Data"* aan om direct koersen en analyses te bekijken zonder brokerverbinding.
