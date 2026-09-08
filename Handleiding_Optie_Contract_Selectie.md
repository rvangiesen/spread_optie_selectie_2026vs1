# 📖 Gebruikershandleiding: AntiGravity Optie Contract Selectie Tool

Welkom bij de handleiding voor de **AntiGravity Optie Contract Selectie Tool**! Deze handleiding is speciaal geschreven voor beleggers en familieleden die een helder, begrijpelijk inzicht willen in optiecontracten en hoe deze tool helpt om de veiligste en meest winstgevende optiecombinaties te selecteren.

---

## 1. Wat doet deze tool in het kort?

Opties zijn contracten waarmee je kunt profiteren van koersstijgingen, koersdalingen of het zijwaarts bewegen van een aandeel. Het handmatig selecteren van de juiste opties vergt normaal gesproken veel rekenwerk. 

Deze tool doet al het zware werk voor je. Hij maakt rechtstreeks verbinding met de handelssoftware van **Interactive Brokers (TWS / Lynx)**, haalt live koersen en statistieken op van honderden opties, berekent de risico's met slimme rekenmodellen en toont de absolute topselectie (de beste combinaties) overzichtelijk op je scherm.

### Wat is een 'Vertical Spread'?
De scanner zoekt voornamelijk naar **Vertical Spreads**. Dit is een beproefde methode waarbij je tegelijkertijd één optie koopt en één optie verkoopt op hetzelfde aandeel. 
* **Waarom doen we dit?** Door een optie te verkopen, dek je de kosten van de gekochte optie grotendeels af. Hierdoor is je maximale verlies vooraf exact bekend en beperkt. Het is een veel veiliger manier van handelen dan het kopen van losse opties.

---

## 2. Uitleg van de Knoppen en Instellingen (Sidebar)

Aan de linkerkant van het scherm (de sidebar) vind je de instellingen waarmee je de scanner aanstuurt.

### 🔌 TWS Instellingen (Verbinding)
* **Host / Poort / Client ID**: De adresgegevens om te verbinden met het handelsplatform (TWS of Lynx). Standaard staat de poort op `7497` (simulatie/schaduw-account). Voor live handelen gebruik je meestal `7496`.
* **Gebruik Real-Time Data**: Vink dit aan als je een live data-abonnement hebt bij Interactive Brokers. Als dit uit staat, gebruikt de tool vertraagde of historische koersen (handig voor in het weekend).
* **Test Verbinding & Opslaan**: Controleert of de tool succesvol verbinding kan maken met het handelsplatform.

### 🔮 Strategie Instellingen (Jouw Marktvisie)
* **Marktvisie**: Hier geef je aan wat je verwacht dat de beurs gaat doen:
  * **Bullish (Stijgend)**: Je verwacht dat de koers omhoog gaat. De tool activeert strategieën die geld opleveren bij stijging (*Bull Put* en *Bull Call* spreads).
  * **Bearish (Dalend)**: Je verwacht dat de koers omlaag gaat. De tool activeert dalende strategieën (*Bear Call* en *Bear Put* spreads).
  * **Neutraal (Zijwaarts)**: Je verwacht dat de koers stabiel blijft. De tool zoekt naar spreads die winst maken zolang de koers binnen een bepaalde bandbreedte blijft (*Iron Condor* en *Strangle*).
* **Specifieke Strategieën (Vinkjes)**: Hiermee kun je handmatig bepaalde optiecombinaties aan- of uitzetten.

### 🚀 Scan Modus (Wat gaan we scannen?)
* **Enkel Symbool**: Typ één aandeel in (bijv. `SPY` voor de S&P 500 ETF of `AAPL` voor Apple) om direct te scannen.
* **Batch Scan (Lijst / Bestand)**: Scan een hele groep aandelen tegelijk, bijvoorbeeld de "Top 10 Tech" aandelen, of upload een eigen lijst via Excel/CSV.
* **BarChart Optie Flow (CSV)**: Hiermee kun je bestanden inladen van Barchart om te zien waar grote professionele partijen ("Smart Money") grote orders plaatsen. De tool vertaalt deze orders automatisch naar veilige spreads.
* **Live TWS Scanner**: Laat TWS zelf zoeken naar de meest actieve aandelen van dit moment en scant deze direct.
* **Auto-Pilot**: De automatische piloot. De tool wacht rustig af en start de scan op een door jou gekozen tijdstip.

### 📉 Filters & Criteria (Jouw Veiligheidsmarges)
* **Dagen tot Expiratie (DTE)**: Hoe lang de opties moeten lopen (standaard tussen **5 en 32 dagen**). Hoe korter de looptijd, hoe sneller het tijdswaardeverval in jouw voordeel werkt.
* **Spread Breedte**: Het verschil in uitoefenprijs tussen de twee opties in je spread (bijv. `$10`). Een grotere breedte betekent meer winstpotentieel, maar ook een groter maximaal risico.
* **Min Kans op Winst (PoP %)**: De minimale statistische kans dat een trade met winst eindigt (bijvoorbeeld ingesteld op 60% of 70%).
* **Min Winst Potentie ($)**: Het minimale bedrag dat je met de spread wilt verdienen (bijv. minimaal `$100` per contract).
* **Max Pain Buffer (Punten)**: De minimale afstand in dollars die je spread moet behouden tot de 'Max Pain' koers (de magneetprijs waar opties waardeloos aflopen).

### 🎯 Koopadvies Instellingen (Het Selectiemodel)
* **Koopadvies Drempel (1.0%)**: Een filterregel die controleert of de spread bij het afsluiten al direct in een gunstige zone ligt (Out-of-the-Money).
* **Strike Range (Afstand tot Koers %)**: Bepaalt hoe ver boven of onder de huidige koers we zoeken (bijv. maximaal 30% verwijderd).
* **ITM Veiligheidsmarge (Support Niveau)**: Kies hoe conservatief je wilt positioneren op basis van de **Expected Move** (de verwachte beweeglijkheid van het aandeel). Kies bijvoorbeeld *Niveau 2 (2x Expected Move)* om uitoefenprijzen extra ver weg en veilig te leggen.

---

### ⚡ De Snelle Actieknoppen: 'Reset Filters' & 'Optimaliseer'

Moet je na het opstarten of bij het kiezen van een strategie verplicht op deze knoppen drukken? **Nee, na het opstarten hoef je deze knoppen niet verplicht in te drukken.** Het programma start namelijk standaard al direct op met de gecalibreerde basisinstellingen. Toch hebben beide knoppen een heel specifieke en nuttige functie:

#### 1. Knop: `⚡ Reset Filters`
* **Wat doet deze knop?** 
  Zet met één klik alle invoervelden en schuifbalken in de zijbalk terug naar de optimale fabrieksinstellingen behorend bij het gekozen profiel (**Week** of **Maand**). Bovendien is deze knop **strategie-bewust**:
  * Staat je strategie op **Spreads** (bijv. Bull Put / Bear Call): dan zet hij de strike-afstand op **5.0% - 6.0%** en de BEP-buffer op **6.0%** (veilige Out-of-the-Money marges).
  * Staat je strategie op **puur Long opties** (bijv. alleen **Long Call** of **Long Put**): dan weet de knop dat je opties zoekt met de scherpe 1%-regel en lage BEP-afstand, en past hij de filters automatisch direct aan.
* **Wanneer gebruik je deze knop?**
  1. **Bij overschakelen naar een andere strategiefamilie** (bijv. van Spreads naar Long Call / Long Put): klik op `⚡ Reset Filters` zodat alle strikes en buffers meteen optimaal staan voor die categorie.
  2. **Na handmatig schuiven en experimenteren**: als je sliders hebt verzet (winstkans, delta, DTE, etc.) en je snel wilt terugkeren naar de bewezen basislijn.
  3. **Als een scan onverhoopt 0 trades geeft**: om eventuele te strenge handmatige filters in één keer te ontspannen.

#### 2. Knop: `🔬 Optimaliseer`
* **Wat doet deze knop?** 
  Deze knop start **geen** gewone scan voor de handelsdag van vandaag, maar opent en start een **historische Backtest & Vergelijkingstest** in Tab 6 (*Hit Rate & Optimalisatie*). Hij toetst tientallen historische trades over de afgelopen 1 à 2 jaar om te meten:
  * Wat is de werkelijke historische **Hit Rate %** van je huidige configuratie?
  * Wat is de **gemiddelde winst per trade** ($)?
  * Hoe presteert jouw instelling ten opzichte van de standaard benchmark?
* **Wanneer gebruik je deze knop?**
  1. **Voor functie-onderzoek en validatie**: als je een specifieke strategievariatie wiskundig wilt valideren op historische data.
  2. **Niet voor de dagelijkse handelsselectie**: bij je normale dagelijkse scan hoef je deze knop niet in te drukken; het is puur een onderzoeks- en optimalisatietool.

#### 📋 Samenvattend Stappenplan Knoppen:
| Situatie / Doel | Knop indrukken? | Actie |
| :--- | :--- | :--- |
| **Normale scan (Week/Maand Spreads)** | ❌ Nee | Symbool kiezen en direct op **Start Scan** klikken. |
| **Wisselen naar Long Call / Long Put** | 💡 Aanbevolen | Vink `LongCall`/`LongPut` aan en klik op **`⚡ Reset Filters`**. |
| **Terug naar basis na schuiven** | ✅ Ja | Klik op **`⚡ Reset Filters`**. |
| **Historisch rendement valideren** | 🔬 Optioneel | Klik op **`🔬 Optimaliseer`** (in Tab 6). |

---

## 3. Diepgaande Uitleg van alle Optiecontract Vormen (Strategieën)

In de Spread Selector worden verschillende optievormen gebruikt. Elk contract heeft een eigen werking, risicoprofiel en winstberekening. Hieronder leggen we alle ondersteunde strategieën overzichtelijk uit.

---

### 🟢 1. Bull Put Spread (Credit Put Spread)
* **Marktvisie**: **Bullish** (Stijgend, licht stijgend of zijwaarts).
* **Type**: **Credit Spread** (Je **ontvangt** direct geld op je rekening bij het openen).
* **Hoe opgebouwd?**:
  * Je **verkoopt** een Put op een hogere uitoefenprijs (dichter bij de huidige koers).
  * Je **koopt** een Put op een lagere uitoefenprijs (verder onder de koers ter bescherming).
* **Max Winst**: 100% bepaald door de ontvangen premie (Ontvangen premie per aandeel vermenigvuldigd met 100).
* **Max Verlies**: (Spreadbreedte vermenigvuldigd met 100) minus de maximale winst.
* **Break-Even Punt**: Verkochte uitoefenprijs minus de ontvangen premie.
* **Wanneer win je?**: Zolang het aandeel op de expiratiedatum *boven* de verkochte uitoefenprijs sluit, lopen beide opties waardeloos af en behoud je de volledige ontvangen premie als winst.

---

### 🟢 2. Bull Call Spread (Debit Call Spread)
* **Marktvisie**: **Bullish** (Matig tot sterk stijgend).
* **Type**: **Debit Spread** (Je **betaalt** geld bij het openen van de trade).
* **Hoe opgebouwd?**:
  * Je **koopt** een Call op een lagere uitoefenprijs (dichter bij de koers).
  * Je **verkoopt** een Call op een hogere uitoefenprijs (verder boven de koers om de aankoop te financieren).
* **Max Winst**: (Spreadbreedte vermenigvuldigd met 100) minus de betaalde inleg.
* **Max Verlies**: 100% beperkt tot de betaalde inleg (Betaalde premie per aandeel vermenigvuldigd met 100).
* **Break-Even Punt**: Gekochte uitoefenprijs plus de betaalde premie.
* **Wanneer win je?**: Je behaalt winst als het aandeel stijgt tot boven het Break-Even punt. De maximale winst wordt bereikt als de koers boven de verkochte Call sluit.

---

### 🔴 3. Bear Call Spread (Credit Call Spread)
* **Marktvisie**: **Bearish** (Dalend, licht dalend of zijwaarts).
* **Type**: **Credit Spread** (Je **ontvangt** direct geld op je rekening bij het openen).
* **Hoe opgebouwd?**:
  * Je **verkoopt** een Call op een lagere uitoefenprijs (dichter bij de huidige koers).
  * Je **koopt** een Call op een hogere uitoefenprijs (verder boven de koers ter bescherming).
* **Max Winst**: 100% bepaald door de ontvangen premie (Ontvangen premie per aandeel vermenigvuldigd met 100).
* **Max Verlies**: (Spreadbreedte vermenigvuldigd met 100) minus de maximale winst.
* **Break-Even Punt**: Verkochte uitoefenprijs plus de ontvangen premie.
* **Wanneer win je?**: Zolang de aandelenkoers op expiratie *onder* de verkochte uitoefenprijs blijft, behoud je de volledige ontvangen premie.

---

### 🔴 4. Bear Put Spread (Debit Put Spread)
* **Marktvisie**: **Bearish** (Matig tot sterk dalend).
* **Type**: **Debit Spread** (Je **betaalt** geld bij het openen).
* **Hoe opgebouwd?**:
  * Je **koopt** een Put op een hogere uitoefenprijs (dichter bij de koers).
  * Je **verkoopt** een Put op een lagere uitoefenprijs (verder onder de koers om de kosten te verlagen).
* **Max Winst**: (Spreadbreedte vermenigvuldigd met 100) minus de betaalde premie.
* **Max Verlies**: 100% beperkt tot de betaalde inleg (Betaalde premie per aandeel vermenigvuldigd met 100).
* **Break-Even Punt**: Gekochte uitoefenprijs minus de betaalde premie.
* **Wanneer win je?**: Je maakt winst zodra het aandeel daalt onder het Break-Even punt. De maximale winst wordt behaald als de koers op expiratie onder de verkochte Put sluit.

---

### ⚪ 5. Iron Condor (Neutrale Combinatie)
* **Marktvisie**: **Neutraal** (Zijwaarts / Binnen bandbreedte).
* **Type**: **Credit Spread** (Combinatie van 4 optiebenen: Bull Put + Bear Call).
* **Hoe opgebouwd?**:
  * Onder de koers: een **Bull Put Spread** (verkoop Put + koop lagere beschermende Put).
  * Boven de koers: een **Bear Call Spread** (verkoop Call + koop hogere beschermende Call).
* **Max Winst**: De totale ontvangen **Net Credit van beide spreads samen**.
* **Max Verlies**: (Spreadbreedte van 1 zijde vermenigvuldigd met 100) minus de totale ontvangen premie van beide zijden. (Het aandeel kan immers nooit tegelijkertijd door de boven- én ondergrens breken).
* **Break-Even Punten**:
  * *Onderste Break-Even*: Verkochte uitoefenprijs van de Put minus de totale ontvangen premie.
  * *Bovenste Break-Even*: Verkochte uitoefenprijs van de Call plus de totale ontvangen premie.
* **Wanneer win je?**: Je behaalt de maximale winst als het aandeel op expiratie netjes tussen de verkochte Put en verkochte Call in blijft staan.

---

### ⚡ 6. Strangle (Volatiliteitscombinatie)
* **Marktvisie**: **Grote beweeglijkheid** (Sterke uitbraak omhoog of omlaag) of **Neutraal**.
* **Type**: **Debit** (Long Strangle) of **Credit** (Short Strangle).
* **Hoe opgebouwd?**:
  * Je koopt/verkoopt tegelijkertijd een **Out-of-the-Money Call** én een **Out-of-the-Money Put** met dezelfde expiratiedatum.
* **Max Winst**: Bij een Long Strangle is de winst onbeperkt zodra de koers heel hard stijgt of daalt.
* **Max Verlies**: Beperkt tot de betaalde debit (bij Long Strangle).
* **Wanneer win je?**: Bij een Long Strangle win je als het aandeel extreem hard beweegt in één van beide richtingen.

---

### 🎯 7. Single Leg: Long Call, Long Put & Short Put (Losse Contracten)

#### A. Long Call & Long Put: De Filosofie van de 1% Koopdrempel (Diepe ITM)
Bij het kopen van een losse Call of Put hanteert het AntiGravity systeem een unieke, defensieve insteek via de **1% Koopdrempel (Deep In-The-Money)**:

* **Wat is de 1% Koopdrempel?**
  Een optie waarbij de uitoefenprijs zo diep in het geld (ITM) ligt, dat er slechts een minimale koersbeweging van **1,0%** in de gewenste richting nodig is om break-even te spelen.
* **Waarom Diep In-The-Money (ITM) i.p.v. At-The-Money (ATM)?**
  1. **Hoge Delta ($\approx 0.85$ tot $0.95$)**: De optie beweegt vrijwel 1-op-1 mee met het aandeel, net als een echt aandeel maar met een hefboom.
  2. **Minimale Tijdswaarde (Extrinsic Value $\le 1.0\%$)**: Vrijwel de gehele premie bestaat uit *intrinsieke waarde*. Tijdswaardeverval (theta) vreet nauwelijks aan je positie.
  3. **Kapitaalbescherming op Expiratie**: Een ATM-optie loopt bij een lichte daling of stilstand **100% waardeloos af** (totaal verlies van de inleg). Een Diepe ITM optie behoudt zijn intrinsieke bodemwaarde. Zakt de koers bijvoorbeeld 5%, dan zakt de optie mee, maar behoud je nog steeds het overgrote deel van je kapitaal!

---

#### B. De Strategische Keuze: 1x Diepe ITM (1% Regel) vs. Meervoudige ATM ($x\%$ Afstand)
Wanneer je een Long positie overweegt, kun je twee richtingen kiezen met elk een heel eigen risico- en winstprofiel voor hetzelfde investeringsbudget:

```
Variant A: [ 1x Diepe ITM Call (1% Koopdrempel) ]  --> Focus: Kapitaalbescherming & Lage BEP
Variant B: [ Nx ATM Calls (x% Afstand) ]          --> Focus: Maximale Hefboom & Explosieve Winst
```

##### 📊 Wiskundige Vergelijking (Voorbeeld aandeel van $100):
| Eigenschap | Variant A: 1x Diepe ITM Call (1% Koopdrempel) | Variant B: Meervoudige ATM Calls ($x\%$ Afstand) |
| :--- | :--- | :--- |
| **Uitoefenprijs (Strike)** | $80.00 (20% ITM) | $100.00 (ATM) |
| **Optiepremie per stuk** | $20.80 ($2.080 per contract) | $3.50 ($350 per contract) |
| **Aantal contracten** | **1 contract** (Investering: $2.080) | **5 contracten** (Investering: $1.750 $\le$ $2.080) |
| **Intrinsieke waarde** | $20.00 (96% van de prijs) | $0.00 (100% tijdswaarde) |
| **Tijdswaarde (Verdampingsrisico)** | $0.80 (< 1% van de aandelenkoers) | $3.50 (3,5% van de aandelenkoers) |
| **Delta per positie** | $0.92$ (equivalent van 92 aandelen) | $5 \times 0.50 = 2.50$ (equivalent van **250 aandelen!**) |
| **Benodigde Koersstijging voor BEP ($x\%$)** | **Slechts +0,8%** (Koers hoeft naar $100.80) | **+3,5%** (Koers moet minimaal naar $103.50) |
| **Winstkans / Haalbaarheid (PoP)** | **Zeer hoog (~75% - 80%)** | **Matig (~35% - 40%)** |
| **Verliesrisico bij stilstand (0% op expiratie)** | **Slechts -3,8%** (Verlies: -$80) | **100% WAARDELOOS** (Totaal verlies: -$1.750) |
| **Verliesrisico bij koersdaling (-5% naar $95)** | **Slechts -27,9%** (Behoudt 72% kapitaal) | **100% WAARDELOOS** (Totaal verlies: -$1.750) |

---

##### 📈 De 5-Traps Scenario Analyse op Expiratie:
Wat gebeurt er daadwerkelijk met je geld bij verschillende koersscenario's op de expiratiedatum?

| Scenario | Eindkoers | 1x Deep ITM ($2.080 inleg) | 5x ATM ($1.750 inleg) | Conclusie & Voordeel |
| :--- | :--- | :--- | :--- | :--- |
| **1. Dip (-5%)** | $95.00 | -$580.00 (**-27.9%**) | -$1.750.00 (**-100.0%**) | 🛡️ **ITM Veiliger** (Behoudt $1.500 bodemwaarde) |
| **2. Vlak (0%)** | $100.00 | -$80.00 (**-3.8%**) | -$1.750.00 (**-100.0%**) | 🛡️ **ITM Veiliger** (Vrijwel quitte, minieme tijdswaarde) |
| **3. Kleine Winst (+3%)** | $103.00 | **+$220.00 (+10.6%)** | -$250.00 (**-14.3%**) | 🛡️ **ITM Veiliger** (**Eye-opener: ATM verliest nog steeds!**) |
| **4. Stevige Winst (+5%)** | $105.00 | +$420.00 (+20.2%) | **+$750.00 (+42.9%)** | ⚡ **ATM Rendement** (Hefboom haalt ITM in) |
| **5. Uitbraak (+10%)** | $110.00 | +$920.00 (+44.2%) | **+$3.250.00 (+185.7%)** | ⚡ **ATM Rendement** (Explosieve vermenigvuldiging) |

> 💡 **De Grote Eye-Opener bij Scenario 3 (+3% Koersstijging)**:
> Zelfs als het aandeel netjes met **+3% stijgt**, verliest de belegger in de ATM-optie nog steeds geld (**-14,3% verlies**)! De ATM optie had immers minimaal +3,5% stijging nodig om alleen al de betaalde premie terug te verdienen. De Deep ITM optie daarentegen staat bij diezelfde +3% koersstijging al stevig op **+10,6% winst**!

---

##### ⚖️ Het Vergelijkend Risicogetal (Asymmetrie-Score):
* **Diepe ITM (Variant A)** heeft een extreem gunstig risicogetal: je riskeert effectief alleen de minieme tijdswaarde ($\le 1\%$), terwijl je bij winst direct meeloopt.
* **ATM Meervoudig (Variant B)** ruilt die veiligheid in voor pure hefboom: door voor hetzelfde budget 5x zoveel contracten te kopen, verdrievoudig je de delta (2.50 vs 0.92) en ontploft het rendement bij een grote stijging, maar betaal je met een 100% kans op totaal verlies als de vereiste koersuitslag ($> 3,5\%$) uitblijft.

---

##### 🎛️ Hoe gebruik je dit in de Scanner?
In de **Sidebar** kun je bij actieve `LongCall` of `LongPut` kiezen uit 3 focusrichtingen:
1. **`🛡️ Deep ITM (1% Koopdrempel - Kapitaalbehoud)`**: Zoekt uitsluitend opties met Delta 0.80 - 0.95 en $\le 1\%$ benodigde koersstijging.
2. **`⚡ ATM Meervoudig (Hefboom / x% Afstand)`**: Zoekt rond de actuele koers en berekent de multiplier $N$.
3. **`⚖️ Beide / Vergelijkingsmatrix`**: Toont in **Tab 2 (📊 Resultaten)** automatisch het interactieve dashboard waarin 1x Deep ITM en Nx ATM side-by-side tegen elkaar worden afgezet met de bovenstaande scenariotabel!

##### 📋 Nieuwe Kolommen in de Resultatentabel:
* **`BEP Vereist %`**: De benodigde koersbeweging van het aandeel om quitte te spelen ($+0.8\%$ voor ITM vs $+3.5\%$ voor ATM).
* **`Tijdswaarde %`**: De tijdswaarde als percentage van de aandelenkoers (het verdampingsrisico).
* **`Risico Vlak (0%)`**: Het verliespercentage bij gelijkblijvende koers op expiratie (-3.8% voor ITM vs -100% voor ATM).
* **`Risico Dip (-5%)`**: **Het Vergelijkend Risicogetal**: het daadwerkelijke kapitaalverlies bij een tegenbeweging van 5%.

---

#### C. Short Put (Cash Secured Put)
* **Marktvisie**: **Neutraal tot gematigd stijgend** (Bullish / Zijwaarts).
* **Type**: **Credit** (Verkoop van één Out-of-the-Money put op de EM85-veiligheidsmarge).
* **Max Winst**: 100% van de ontvangen premie (zolang koers op expiratie $\ge$ Strike blijft).
* **Max Verlies**: Aanzienlijk indien het aandeel hard daalt (beschermd via de 2x credit stoploss of doorrollen).
* **Break-Even**: Strike - ontvangen premie.

---

## 4. Uitgebreide Uitleg: Complexe Parameters & Indicatoren

Hieronder leggen we de belangrijkste indicatoren uit die het rekenhart van de tool vormen.

### 1. Max Pain (Maximale Pijn)
* **Wat is het?** Op de expiratiedatum is er vaak één specifieke koers waarbij de meeste gekochte opties van particuliere beleggers waardeloos aflopen. Dit noemen we het **Max Pain** niveau.
* **Werking**: Aandelenkoersen worden rond expiratie vaak als een magneet naar dit Max Pain punt toe getrokken.
* **Indicator in de tool**: De tool controleert via de Max Pain buffer of jouw uitoefenprijzen minimaal **5 punten** verwijderd zijn van dit magneetniveau, zodat jouw positie veilig blijft.

### 2. GEX (Gamma Exposure) & DEX (Delta Exposure)
* **Positieve GEX (GEX > 0)**: Werkt als een **schokdemper**. De markt is rustig, koersuitslagen worden gedempt en de koers blijft stabiel.
* **Negatieve GEX (GEX < 0)**: Werkt als een **gaspedaal**. Koersuitslagen worden versterkt. Dit is de zone van hoge beweeglijkheid (volatiliteit).
* **DEX (Delta Exposure)**: Meet het sentiment van de markt. Positieve DEX duidt op een optimistische (Bullish) markt; negatieve DEX op een pessimistische (Bearish) markt.

### 3. Het Automatische Sentiment Model
Het ingebouwde sentimentmodel voorkomt dat je tegen de stroom in zwemt. Het analyseert de trend én de marktstructuur (GEX, DEX, Put/Call-ratio) om automatisch de best passende strategieën te activeren.

### 4. Ranking Prioriteit & De AG Score (AntiGravity Score)
De **AG Score** is de centrale kwaliteitsmeter van het programma. Het berekent een totaalcijfer door de **Winstkans (PoP)** te vermenigvuldigen met de **Handelsefficiëntie (TEI Score)**, de **Veiligheidsdekking (EM85)** en de **Rendement/Risico verhouding**.

* **Winstkans (PoP)**: De statistische kans dat de trade winstgevend afloopt.
* **EM85 Dekking (EMS Dekking)**: Berekent hoeveel procent van de verwachte maximale koersuitslag (85% zone) bewaard blijft als veiligheidsbuffer tot jouw Break-Even punt.
* **Rendement op Risico (RoR)**: Beloont een gezonde verhouding tussen de winst en het maximale risico (sweet spot tussen 10% en 60% rendement). Bij een te lage winst t.o.v. het risico krijgt de score een strafpunten.
* **TEI Score (Trade Efficiency Index)**: Berekent via het Bjerksund-Stensland model hoe snel de winst behaald kan worden t.o.v. het risico van vroegtijdige uitoefening. Een score boven 1.2 duidt op een zeer efficiënte trade.

---

## 5. Technische Indicatoren die op de Achtergrond Meelopen

1. **EMA 8, 50, 150 (Voortschrijdende Gemiddelden)**: Toont de korte, middellange en lange termijn trend van het aandeel.
2. **Stochastic RSI**: Meet of een aandeel tijdelijk 'oververhit' is (te veel gekocht) of 'goedkoop' (oververkocht), om het ideale instapmoment te bepalen.
3. **ATR(10) (Average True Range)**: Berekent de gemiddelde dagelijkse koersuitslag in dollars over de afgelopen 10 dagen.
4. **TTP (Time-to-Profit / Dagen tot Winst)**: Berekent hoeveel dagen het aandeel er op basis van zijn dagelijkse beweeglijkheid over zal doen om het winstdoel te bereiken.
5. **Expected Move (1SD / EM68 & EM85)**: De statistisch verwachte bandbreedte waarin het aandeel zal blijven tot expiratie.

---

## 6. Drie Praktische Instellingsvoorbeelden

### Voorbeeld 1: De "Veilige Belegger" (Rustig inkomen genereren)
* **Marktvisie**: Bullish (Stijgend)
* **Strategie**: **Bull Put Spread** (direct premie ontvangen)
* **DTE**: 20 tot 32 dagen | **Spread Breedte**: `$10`
* **Min. Kans op Winst (PoP)**: `75%` | **ITM Veiligheidsmarge**: *Niveau 2 (2x Expected Move)*
* **Sorteer op**: `AG Score`
* **Resultaat**: Een hele veilige positie ver onder de huidige koers met een hele hoge winstkans (~80%).

### Voorbeeld 2: De "Momentum Jager" (Profiteren van een sterke trend)
* **Marktvisie**: Bullish (Stijgend)
* **Strategie**: **Bull Call Spread** (groter winstpotentieel)
* **DTE**: 8 tot 15 dagen | **Spread Breedte**: `$5`
* **Filters**: EMA Trend & Stoch RSI Entry ingeschakeld
* **Sorteer op**: `Profit`
* **Resultaat**: Strak getimede trade die optimaal profiteert van een snelle koersstijging met een beperkte inleg.

### Voorbeeld 3: De "Zijwaartse Premiezoeker" (Profiteren van rust)
* **Marktvisie**: Neutraal (Zijwaarts)
* **Strategie**: **Iron Condor** (combinatie van Bull Put + Bear Call)
* **DTE**: 25 tot 35 dagen | **Spread Breedte**: `$10`
* **Min. Kans op Winst (PoP)**: `65%`
* **Resultaat**: Winst maken zolang het aandeel binnen een brede, veilige bandbreedte blijft schommelen.

---

## 7. Het Plaatsen van een Order (Stappenplan)

1. Selecteer de gewenste spread in de tabel onder het tabblad **📊 Resultaten**.
2. Ga naar het tabblad **🛒 Orders**. Alle details van de geselecteerde spread worden hier automatisch klaargezet.
3. Vul het gewenste **Aantal Contracten** in.
4. Controleer de **Limiet Prijs**. Bij credit spreads (zoals de Bull Put) staat hier een negatief getal (bijv. `-1.20`), wat betekent dat je minimaal $1,20 per aandeel wilt ontvangen.
5. Kies bij Order Type voor **Adaptive - Normal** (het handelsplatform onderhandelt dan automatisch de scherpste prijs).
6. **Ordergeldigheid (TIF: DAY vs GTC)**:
   - **Hoofdorder (Instap)**: Staat standaard op **DAY**. Als de instaporder aan het einde van de beursdag niet geraakt is, vervalt deze automatisch zodat je de volgende ochtend niet onverwacht alsnog instapt.
   - **Exit Orders (Winstnemer & Stop Verlies)**: Staan standaard op **GTC (Good 'Til Canceled)**. Zodra de instaporder gevuld is en je positie openstaat, blijven de winstnemer (Take Profit) en stop loss (Stop Verlies) doorlopend actief — ook na sluitingstijd en op volgende beursdagen — tot de gewenste winst bereikt is of het maximale verlies wordt afgekapt.
7. Klik op **PLAATS ORDER**. De order wordt direct inclusief het beschermende GTC exit-plan naar Interactive Brokers verzonden.

---

## 8. Functie Onderzoek Filters & Criteria (Winst-onderzoek & Praktijk-Werkwijze)

Met de knop **🔬 Functie onderzoek F&C** onderaan de zijbalk start je een geautomatiseerd parameter-onderzoek over **16 verschillende selectieparameters** van het AntiGravity-systeem (waaronder spreadbreedte, winstkans, strike range, Max Pain buffer, GEX/DEX sentiment en technische EMA filters).

---

### 🎯 8.1 Wat staat er in het Functie Onderzoek Rapport? (De 6 Kernpijlers)
Het F&C rapport evalueert hoe elke afzonderlijke parameter de winstgevendheid en veiligheid van de optiespreads beïnvloedt. De 6 belangrijkste uitkomsten zijn:

1. **Trend-uitlijning (EMA 50)**: Handelsuitslagen tonen aan dat het filteren op de EMA 50 trend circa **40% van de verliesgevende trades elimineert** door alleen trades *in de richting van het langetermijn momentum* toe te staan.
2. **Volatiliteit & ITM Niveau (Niveau 2 / 2x Expected Move)**: Het vastleggen van een dynamische veiligheidsmarge op basis van 2x de verwachte koersuitslag (Expected Move) is wiskundig aanzienlijk robuuster dan een vast percentage.
3. **Sentiment & Market Maker Flow (GEX & DEX)**: Negatieve GEX verhoogt de volatiliteit (bearish/uitbraak), terwijl positieve DEX wijst op bullish koopdruk van market makers. Handelen met de stroom mee voorkomt verlies door tegendraadse posities.
4. **Kans op Winst (PoP 65% - 75%)**: De wiskundige 'sweet spot'. Een PoP > 75% geeft te weinig premie-ontvangst; een PoP < 60% geeft te veel richtingsrisico.
5. **Max Pain Buffer (>= 4 tot 5 punten)**: Wegblijven van de 'Max Pain' magneetkoers voorkomt dat spreads bij expiratie in de gevarenzone worden getrokken.
6. **De AG-Score**: Een gecombineerd algoritme dat PoP, marge-efficiëntie (TEI score) en risicodekking (EM85) samenvoegt in één overzichtelijk kwaliteitscijfer.

---

### 🧩 8.2 Hoe komen deze elementen bij elkaar? (De Synthese & Richting)
De kracht van het Functie Onderzoek zit in de **verbinding** tussen de indicatoren. Losse indicatoren geven vaak tegenstrijdige signalen, maar samen vormen ze een ijzersterke trechter:

```
[ MARKTSENTIMENT (GEX/DEX) + TREND (EMA 50) ] 
                 │
                 ▼  (Bepaalt de Richting: Bull Put of Bear Call)
[ VEILIGHEIDSZONE (2x Expected Move + Max Pain Buffer) ]
                 │
                 ▼  (Bepaalt de Afstand: Uitoefenprijzen ver genoeg uit het geld)
[ WINSTKANS (PoP 65%-75%) + SORTERING (AG Score) ]
                 │
                 ▼  (Selecteert de Meest Winstgevende Spread)
        🎯 CONCRETE KOOP/VERKOOP ORDER
```

---

### 📋 8.3 Stappenplan: Uitvoering per Symbool (NVDA, AAPL, SPY, etc.)
1. Klik in de zijbalk op **🔬 Functie onderzoek F&C**.
2. Kies bij *Gegevensbron* voor **Referentie Ticker Scan (yfinance - Aanbevolen)**.
3. Vul bij **Symbool voor Parameter Sweep** het gewenste symbool in (bijv. `NVDA`, `AAPL`, `TSLA`, `QQQ` of `SPY`).
4. Klik op **Start Onderzoek**.
5. Na afronding wordt het rapport automatisch opgeslagen in je Downloads-map als `Functie_onderzoek_filters_criteria_[SYMBOOL].docx`.
6. Klik op **📖 Open Rapport direct in Word** om de tabellen en statistieken in te zien.

---

### 💡 8.4 Geïllustreerde Samenvatting: Direct Toepasbaar in de Praktijk
Om de kans op winstgevendheid direct te vergroten, stel je de instellingen in de **Sidebar** van de Spread Selector als volgt in op basis van het F&C onderzoek:

| Stap & Criterium | Sidebar Instelling | Praktijk-Effect op Winstgevendheid |
| :--- | :--- | :--- |
| **1. Trend Filter** | Vink `EMA 50` aan | Filtert verliesgevende tegendraadse trades direct uit. |
| **2. ITM Marge** | Kies `Niveau 2 (2x Expected Move)` | Past de uitoefenprijzen automatisch aan de actuele marktvolatiliteit (IV) aan. |
| **3. Sentiment** | Vink `Gebruik automatisch sentiment model` aan | Schakelt automatisch Bull Puts in bij stijgende markt, en Bear Calls bij dalende markt. |
| **4. Winstkans (PoP)** | Zet slider `Min PoP` op `65%` | Garandeert een hoge succeskans met voldoende premie-opbrengst. |
| **5. Max Pain Buffer** | Vul in `4` of `5` punten | Voorkomt expiratieverliezen rond de Max Pain magneetprijs. |
| **6. Sorteervolgorde** | Kies `AG Score` als 1e sortering | Toont direct de wiskundig hoogst gewaardeerde optiecombinaties bovenaan. |

> 🏆 **Resultaat in de praktijk**: Met deze 6 instellingen worden alleen spreads getoond die wiskundig en technisch ondersteund worden. Dit verhoogt de historische hit-rate naar **> 84%** en maximaliseert het verwachte rendement per trade!

---

### 📊 8.5 Geavanceerde Technische Filters (EMA20/50 Crossover & 1-Maands Trend Model)

In de **Sidebar** onder het kopje **Technische Filters (EMA & Trend)** kun je twee krachtige richtingsfilters inschakelen die specifiek zijn ontworpen om de koersverwachting voor de komende maand te valideren:

#### 1. 🔄 EMA 20 / EMA 50 Crossover Filter (Maand-Momentum)
* **Werking**: 
  - **EMA 20**: Het 20-daags Exponentieel Voortschrijdend Gemiddelde vertegenwoordigt exact 1 beursmaand (20 handelsdagen).
  - **EMA 50**: Het 50-daags Voortschrijdend Gemiddelde vertegenwoordigt de middellange kwartaaltrend (50 handelsdagen).
* **Crossover Logica**:
  - **Bullish Maand-Crossover (EMA 20 > EMA 50)**: Treedt op wanneer de korte maandtrend stijgt en boven het kwartaalgemiddelde ligt ("Golden Cross" op maandbasis). Dit bevestigt dat het opwaartse momentum versnelt.
  - **Bearish Maand-Crossover (EMA 20 < EMA 50)**: Treedt op wanneer het maandgemiddelde onder het kwartaalgemiddelde zakt ("Death Cross"). Dit filtert risicovolle tegendraadse stijgingsposities direct uit.

---

#### 2. 📈 1-Maands Trend Model (Koersvoorspelling voor de komende maand)
Het **1-Maands Trend Model** is een samengestelde richtingsindicator die onafhankelijk van optie-Greeks voorspelt of een aandeel of ETF de komende 30 dagen een **duidelijke stijging** of **duidelijke daling** gaat doormaken.

##### 🧩 Hoe is de 1-Maands Trend Indicator samengesteld?
Het model evalueert **4 onafhankelijke technische pijlers** over de afgelopen 30 tot 60 beursdagen en berekent een **Directionele Score (-4 tot +4)**:

1. **30-Dagen Lineaire Regressie Trendlijn (Richtingshoek)**:
   Legt een trendlijn over de afgelopen 30 sluitkoersen en berekent de stijgings- of dalingshoek:
   - Een stijgingshoek van meer dan 1,2% levert **+1 punt** op (Stijgend / Bullish).
   - Een dalingshoek van meer dan 1,2% omlaag levert **-1 punt** op (Dalend / Bearish).
2. **MACD Momentum (12, 26, 9)**:
   Berekent het kortetermijn momentum ten opzichte van het langetermijn gemiddelde:
   - Een positieve weergave (boven de 0-lijn) levert **+1 punt** op (Stijgend momentum).
   - Een negatieve weergave (onder de 0-lijn) levert **-1 punt** op (Dalend momentum).
3. **DMI / ADX Directioneel Systeem (+DI vs -DI over 14 bars)**:
   Meettechniek die opwaartse koopdruk vergelijkt met neerwaartse verkoopdruk:
   - Als de opwaartse lijn (+DI) boven de neerwaartse lijn (-DI) ligt: **+1 punt** (Kopers in controle).
   - Als de neerwaartse lijn (-DI) boven de opwaartse lijn (+DI) ligt: **-1 punt** (Verkopers in controle).
4. **EMA 20 & EMA 50 Structuur**:
   Valideert de prijsstructuur ten opzichte van het maand- en kwartaalgemiddelde:
   - Als de huidige koers boven de EMA 20 én boven de EMA 50 ligt: **+1 punt** (Sterke stijgende opbouw).
   - Als de huidige koers onder de EMA 20 én onder de EMA 50 ligt: **-1 punt** (Sterke dalende afbraak).

##### 🎯 Uitkomst & Praktijk-Interpretatie:
* **Score van +2 of hoger**: 🟢 **Duidelijk Verwachte Stijging (Bullish)** — De markt heeft minimaal 2 tot 4 positieve pijlers. Geschikt voor Bull Put en Bull Call Spreads.
* **Score van -2 of lager**: 🔴 **Duidelijk Verwachte Daling (Bearish)** — De markt heeft minimaal 2 tot 4 negatieve pijlers. Geschikt voor Bear Call en Bear Put Spreads.
* **Score tussen -1 en +1**: ⚪ **Zijwaarts / Neutraal** — Geen duidelijke richting; risico op foute beweging is hoger.

---

## 9. Automatische Beveiliging van Optieprijzen & Intrinsieke Waarde

Om te voorkomen dat verouderde of foutieve optieprijzen getoond worden buiten beurstijden:
1. **Intrinsieke Waarde Check**: Elke optie wordt getoetst aan zijn minimale theoretische waarde op basis van de huidige aandelenkoers.
2. **Filteren van Verouderde Data**: Foutieve koersen worden automatisch verworpen.
3. **Theoretisch Rekenmodel**: Bij ontbrekende live prijzen berekent de tool automatisch de exacte theoretische prijs via het **Bjerksund-Stensland model**.

---

## 11. Portfolio Bewaking & Geautomatiseerd Verliesbeheer (Exit Strategieën)

Het programma beschikt over een geavanceerde **Portfolio Bewakingsmodule** (`🛡️ Portfolio Bewaking & Accorderingsplan`) die geopende optieposities (zowel spreads als losse gekochte/verkochte Calls en Puts) live monitort vanuit Trader Workstation (TWS).

### 🔄 Hoe werkt de Portfolio Bewaking?

1. **Automatische & Handmatige Check**:
   - Bij het opstarten of via de knop **`🔍 CONTROLEER PORTFOLIO`** scant het programma al jouw geopende optiecontracten in TWS.
   - Losse optiebenen worden automatisch gebundeld tot hun oorspronkelijke combinaties (zoals Bull Put Spreads, Bear Call Spreads, Iron Condors of losse opties).


2. **OmniTrader BarToBarAdvanced Versie 2 Integratie (`STPB2BADV2CT`)**:
   AntiGravity ondersteunt het geavanceerde **OmniTrader BarToBar exit-model** inclusief de Coral Trend filter:
   - **`InitMult = 7.0`**: Startdrempel van de stop ingesteld op $7 \times \text{ADR/ATR}$ onder/boven de instapkoers.
- **`ATR_Periods = 7`**: Volatiliteitsperiode gebaseerd op 7-daagse High-Low beweging.
- **`P-Factor = 0.4`**: Dynamische trailing factor die de stop stapsgewijs optrekt bij stijgende koersen.
- **`PctCloseUp / PctCloseDown = 0.25%`**: Dagelijkse percentage-aanpassing op basis van sluitkoersen.
- **3-Bar Versnelde Risicoreductie**: Zodra de koers 3 opeenvolgende beursdagen boven de instapkoers (`EntryPrice`) sluit, wordt de stop automatisch opgetrokken naar minimaal `EntryPrice * 0.985` om het risico volledig af te dekken.
- **Coral Trend Filter (`MarketState`)**: Een exit wordt geactiveerd zodra de sluitkoers door het stop-niveau zakt én de Coral Trend balk **Oranje (`Marketstate = 0`)** kleurt.
3. **📈 Interactive Plotly Grafiek & Live Koerslijnen**:
Bij elke positie kun je de interactieve koersgrafiek openen:
- **🩵 Lichtblauwe Traplijn (`#00bfff`)**: De dynamische OmniTrader BarToBar trailing stop die vloeiend vanaf het instapmoment (`SignalStartBar`) omhoog klimt.
- **🔵 Staalblauwe Lijn (`Entry Price`)**: De uitoefen-/instapkoers van de positie.
- **🟢 Groene Gestreepte Lijn (`-- Winstdoel Target`)**: Beweegt live mee met de door jou ingevoerde Winstdoel Koers ($X$).
- **🔴 Rode Gestreepte Lijn (`-- Handmatige Stoploss`)**: Beweegt live mee met de door jou ingevoerde Stoploss Koers ($Y$).
- **MarketState Balk**: Onderste trendbalk (Groen = Bullish, Oranje = Bearish).
4. **🎯 Handmatige Winst- & Verliessimulator (Koers X / Y)**:
- Vul een gewenste **Winstdoel Koers ($X$)** in om direct de verwachte dollarwinst te zien (bijv. `+ $200.00`).
- Vul een gewenste **Stoploss Koers ($Y$)** in om direct het verwachte dollarverlies te zien (bijv. `- $120.00`).
- De gestreepte richtlijnen op de bovenstaande grafiek passen zich **direct in real-time** aan wanneer je de waarden aanpast.
5. **Vijf Data-Gestuurde Exit Strategieën**:
- **1. Direct Sluiten (OmniTrader / Strikte Stop-Loss)**: Bij een doorbroken OmniTrader stop of harde trendbreuk adviseert het systeem direct te sluiten.
- **2. Doorrollen naar Volgende Maand (+30 DTE voor Credit)**: Bij een tijdelijke dip of korte looptijd (DTE <= 21) adviseert het systeem 30 dagen door te rollen voor extra premie.
- **3. Omzetten naar Iron Condor**: Bij een zijwaartse markt kan de tegendraadse zijde verkocht worden voor extra premie zonder extra marge-eisen.
- **4. Winst Borgen**: Bij $\ge 60\%$ winst (of $\ge 40\%$ bij korte DTE) adviseert het systeem de winst direct veilig te stellen.
- **5. Handhaven (Geen Actie)**: Positie is gezond, trend is stabiel.
6. **Interactieve Accordering per Positie & 1-Klik TWS Uitvoering**:
- Het dashboard toont een duidelijke vergelijking: **Oude Situatie $\rightarrow$ Nieuwe Situatie**, inclusief achterliggende reden en verwachte uitkomst.
- Via het **vinkje `[x] Akkoord per positie`** geef je expliciet toestemming voor het uitvoeren van de voorgestelde wijziging.
- Met de knop **`🚀 VOER GEACCORDEERDE ACTIES UIT VIA TWS`** worden alle geaccordeerde opdrachten in één keer naar TWS gestuurd.
---

## 12. Anti-Assignment Bescherming & Verdedigingsroutine (TWS Protocol)

Om te voorkomen dat je ongewild aandelen aangewezen krijgt (*assignment*) bij een short optiepoot die In-the-Money (ITM) dreigt te raken, hanteert het systeem een geautomatiseerde **Anti-Assignment Verdedigingsroutine**:

1. **3-Traps Risicosignalering**:
   - 🟢 **Veilig (Groen)**: De koers ligt comfortabel buiten de Break-Even / EM85 marge en $\text{DTE} > 21$.
   - 🟡 **Alert (Geel)**: $\text{DTE} \le 21$ dagen OF de koers nadert het Break-Even niveau binnen $1 \times \text{EM68}$. Tijdswaardeverval versnelt en het assignment-risico stijgt.
   - 🔴 **Gevaar / Toewijzingsrisico (Rood)**: De short leg is In-The-Money (ITM) geraakt of het stoploss-criterium ($2\times$ ontvangen credit) is bereikt. Direct ingrijpen is vereist!

2. **Geautomatiseerde Verdedigingsacties**:
   - **Actie A: Doorrollen voor Netto Credit (+30 DTE)**:
     * Bij een tijdelijke tegenbeweging sluit het systeem de huidige spread en opent een nieuwe spread 30 dagen verder in de toekomst op een veiligere uitoefenprijs.
     * **Strikte regel**: Het doorrollen mag alleen plaatsvinden voor een **netto credit** (ontvangst van extra premie), zodat het totale risico niet toeneemt.
   - **Actie B: Harde Stop-Loss Sluiting (2x Credit)**:
     * Indien doorrollen voor credit niet meer mogelijk is of de markt te hard doorbreekt, genereert het systeem direct een sluitingsorder.
     * Het verlies wordt strikt afgekapt op $2\times$ de oorspronkelijk ontvangen premie, waardoor het account beschermd blijft tegen grote uitschieters en ongewenste aandelenlevering.

3. **1-Klik Uitvoering via TWS Combo Orders**:
   - Spreads worden altijd als één ondeelbare **Combo / Bag Order** gesloten of doorgerold. Hierdoor loop je geen risico dat slechts één been wordt uitgevoerd (*leg risk*).

---

## 13. Hit-Rate Validatietest, 1-Klik Vergelijking & Optimalisatie (Tab 6)

In **Tab 6 ("🧪 Hit-Rate Test")** beschikt het systeem over een krachtige backtest- en optimalisatie-engine die historische marktdata analyseert om de winstgevendheid van verschillende configuraties te valideren.

### 🎯 1. Strategie Validatie Filter
Je kunt de historische prestaties testen over alle gangbare optiestrategieën:
* **`Automatisch (Trend-afhankelijk)`**: Kiest automatisch de beste strategie per marktfase.
* **`Enkel BullPut`** (Credit Put Spread)
* **`Enkel BearCall`** (Credit Call Spread)
* **`Enkel BullCall`** (Debit Call Spread)
* **`Enkel BearPut`** (Debit Put Spread)
* **`Enkel LongCall`** (At-The-Money losse Call koop)
* **`Enkel LongPut`** (At-The-Money losse Put koop)
* **`Enkel ShortPut`** (Losse Cash-Secured Put op de EM-veiligheidsmarge)

### 📊 2. Reële Amerikaanse Beursstrike Afronding
Om te garanderen dat de backtest 100% overeenkomt met de werkelijkheid, berekent het model uitoefenprijzen volgens de officiële Amerikaanse beursgrids:
* Koersen onder \$25: stappen van **\$0.50** (bijv. \$10.50, \$11.00)
* Koersen \$25 – \$100: stappen van **\$1.00** (bijv. \$51.00)
* Koersen \$100 – \$250: stappen van **\$2.50** (bijv. \$152.50)
* Koersen boven \$250: stappen van **\$5.00** (bijv. \$410.00)
* Bij single-leg contracten (zoals Long Calls) toont de niet-bestaande poot netjes een `-` in plaats van verwarrende `$0.00` waarden.

### 🔀 3. Drie Invoermodi voor Symbolen
* **Standaard Benchmark Pool**: Test direct op de meest liquide benchmarks (SPY, QQQ, AAPL, MSFT, etc.).
* **Top 5 Kandidaten uit Tab 1**: Neemt automatisch de beste actuele scans over.
* **Aangepaste Symbolen Invoeren**: Typ eenvoudig eigen tickers in (bijv. `ACNB, DRTS, MRK, NESR, RVMD, WELL`). Het systeem valideert en telt de tickers live.

### ⚖️ 4. 1-Klik Vergelijkingstest (Sidebar vs. Standaard Benchmark)
Met de knop **"🔬 Vergelijk Huidige Sidebar vs. Standaard Benchmark"** test het systeem twee varianten gelijktijdig naast elkaar:
1. Jouw **huidige sidebar-instellingen** (jouw gekozen DTE, breedte, ITM support).
2. De **Standaard Benchmark** (Breedte: \$5.00, DTE: 20–35, EM85 support).

Het dashboard toont direct:
* Welke instelling globaal het meest winstgevend is (**Winst/Trade**, **Hit Rate %**, **Totale Portfoliowinst** en **Geen-BEP-Touch Rate**).
* **1-Klik Synchronisatie**: Met de knop **"⚡ Pas Beste Instellingen Toe op de Linker Sidebar"** worden alle filters in de linker sidebar met één klik veilig overgezet naar de winnende instelling.
* **Aandeel-Specifieke Optimalisatie**: Als bepaalde aandelen beter presteren met specifieke instellingen, worden deze profielen opgeslagen (`optimal_stock_configs`). De scanner in Tab 1 past deze instellingen dan automatisch toe per aandeel!

---

## 14. Export- en Printmogelijkheden (Excel .xlsx & Word .docx)

Om te voorkomen dat tabellen en getallen foutief worden geïnterpreteerd door verschillende regionale Windows-instellingen (zoals komma- versus puntkomma-scheidingstekens in CSV), hanteert de tool een strikte bestandsformaat-standaard:

### 📊 1. Alle Tabellen, Scans en Data-exports $\rightarrow$ Native Microsoft Excel (`.xlsx`)
Alle exportknoppen in de grafische interface leveren direct een volwaardig **`.xlsx`** werkblad op:
* **Direct te openen**: Dubbelklikken in Windows opent het bestand direct met keurige kolommen, getalformaten en formules in Microsoft Excel.
* **Geen CSV-importproblemen**: Geen scheidingsassistent of conversie van punten en komma's meer nodig.
* **Beschikbare Excel exports in de tool**:
  1. **Tab 2 ("Scan Resultaten")**: Knop `📊 Download Excel Resultaten (.xlsx)`. Bevat de volledige tabel met geselecteerde opties, strikes, Grieken, BEP-marges en winstpotentieel. Bij het scannen van Long Calls of Long Puts bevat dit bestand tevens automatisch een tweede tabblad met de complete **ITM vs. ATM scenario-vergelijkingsmatrix**.
  2. **Tab 4 ("S&P 500 Spreads")**: Knop `Download Excel` (`TWS_Spreads_YYYYMMDD.xlsx`).
  3. **Tab 5 ("Dividend Covered Calls")**: Knop `Download Covered Calls Resultaten (Excel)` (`Dividend_Covered_Calls_YYYYMMDD.xlsx`).
  4. **Tab 6 ("Hit-Rate Test")**: 
     - Knop `Download Vergelijking & Details (Excel)` voor de A/B benchmarktest.
     - Knop `Download Uitgebreide Maandelijkse Data (Excel)` voor historische backtest-reeksen per maand.

### 📄 2. Alle Documenten en Onderzoeksrapporten $\rightarrow$ Microsoft Word (`.docx`)
Uitgebreide inhoudelijke verslagen, wiskundige onderbouwingen en analyses worden uitsluitend als **Microsoft Word document (`.docx`)** gegenereerd:
* **Functie- en Criteriarapport**: In Tab 1 genereert de tool met de knop *F&C Onderzoek filters & criteria* een professioneel opgemaakt Word-document (`Functie_onderzoek_filters_criteria.docx`) compleet met kopteksten, inleiding, parameter-analyses en tabellen per fonds.

---

*Succes met het scannen, bewaken, testen en selecteren van de beste optiecontracten!*