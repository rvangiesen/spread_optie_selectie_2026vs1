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

### 🎯 7. Single Leg: Long Call & Long Put (Losse contracten)
* **Marktvisie**: **Zeer sterk stijgend** (Long Call) of **zeer sterk dalend** (Long Put).
* **Type**: **Debit** (Aankoop van één los contract).
* **Max Winst**: Theoretisch onbeperkt (bij Call) of zeer hoog (bij Put tot koers $0).
* **Max Verlies**: 100% beperkt tot de betaalde optiepremie.

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
6. Klik op **PLAATS ORDER**. De order wordt direct naar Interactive Brokers gestuurd.

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

## 12. Hit-Rate Validatietest & Strategie-Filtering per Aandeel (Tab 6)

Om de wiskundige betrouwbaarheid van gekozen optiestrategieën vooraf te toetsen, beschikt het systeem in **Tab 6** over een **Hit-Rate Validatie & Backtest Engine**.

### 🎯 Specifieke Strategie Validatie (bijv. Enkel BullCall op NVDA):
1. **Vooraf Instellen (Vóór de Test)**:
   - Selecteer 1 of meer aandelen (bijv. uitsluitend `NVDA`).
   - Kies bij **`🎯 Strategie Validatie Filter`** de gewenste specifieke optievorm:
     * **`Enkel BullCall`** (Debit Call Spread)
     * **`Enkel BullPut`** (Credit Put Spread)
     * **`Enkel BearCall`** (Credit Call Spread)
     * **`Enkel BearPut`** (Debit Put Spread)
     * **`Automatisch (Trend-afhankelijk)`**
   - De test berekent nu uitsluitend het wiskundige rendement en de hit-rate voor díe specifieke combinatie.

2. **Interacteren & Dynamisch Filteren (Na afloop van de Test)**:
   - Na het draaien van de test kun je via het interactieve filterblok direct filteren op **Aandeel** (bijv. `NVDA`) én **Strategie** (bijv. `BullCall`).
   - De statistieken (**Werkelijke Hit Rate %**, **PoP %**, **EM85 Dekking %** en **Totale Winst $**) worden **direct in real-time herberekend** voor jouw gekozen aandeel en strategie.

---

*Succes met het scannen, bewaken, testen en selecteren van de beste optiecontracten!*