import streamlit as st
import pandas as pd
import numpy as np
import asyncio
import nest_asyncio

# Fix for Streamlit's event loop issue with ib_insync
# Apply nest_asyncio to allow nested event loops (CRITICAL for Streamlit)
try:
    nest_asyncio.apply()
except Exception:
    pass

try:
    asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from ib_insync import IB, Stock, Index, Option # Explicit import here
from ib_client import IBClient
from logic import SpreadScanner

# Page config
st.set_page_config(page_title="Spread Selectie Tool", layout="wide")

# Initialize Session State for Config
if 'tws_configured' not in st.session_state:
    st.session_state.tws_configured = False
if 'results' not in st.session_state:
    st.session_state.results = pd.DataFrame()
if 'symbol_prices' not in st.session_state:
    st.session_state.symbol_prices = {}

# --- TOP HEADER: PRICE DASHBOARD (Live Updating) ---
price_dashboard = st.empty()

def update_price_dashboard():
    if st.session_state.symbol_prices:
        with price_dashboard.container():
            sorted_symbols = sorted(st.session_state.symbol_prices.items())
            cols = st.columns(min(len(sorted_symbols), 6)) 
            for i, (sym, price) in enumerate(sorted_symbols):
                col_idx = i % len(cols)
                cols[col_idx].metric(label=f"💰 {sym}", value=f"${float(price):.2f}")
    else:
        price_dashboard.info("Dashboard wordt gevuld tijdens de scan...")

update_price_dashboard()
st.divider()

# Sidebar - Settings
st.sidebar.title("TWS Instellingen")
tws_host = st.sidebar.text_input("Host", value="127.0.0.1")
tws_port = st.sidebar.number_input("Poort", value=7497)
# Use random client ID to avoid conflicts
import random
if 'default_client_id' not in st.session_state:
    st.session_state.default_client_id = random.randint(1000, 9999)
client_id = st.sidebar.number_input("Client ID", value=st.session_state.default_client_id, help="Wijzig dit als je 'client id already in use' errors krijgt")
use_live_data = st.sidebar.checkbox("Gebruik Real-Time Data (Abonnement vereist)", value=True)

if st.sidebar.button("Test Verbinding & Opslaan"):
    # Test connection ephemerally
    test_ib = IBClient()
    success, message = test_ib.connect(tws_host, tws_port, client_id)
    if success:
        test_ib.disconnect()
        st.session_state.tws_configured = True
        st.sidebar.success("✅ Verbinding geslaagd! Instellingen opgeslagen.")
    else:
         st.session_state.tws_configured = False
         st.sidebar.error(f"❌ Verbinding mislukt: {message}")

# Connection Status
status = "Gereed om te scannen" if st.session_state.tws_configured else "Niet geconfigureerd"
st.sidebar.markdown(f"**Status:** {status}")

# Strategy Settings (Marktvisie)
st.sidebar.title("Strategie Instellingen")
marktvisie = st.sidebar.selectbox("Marktvisie", ["Bullish (Stijgend)", "Bearish (Dalend)", "Neutraal (Zijwaarts)"])

# Determine strategies based on Outlook
active_strategies = []
if "Bullish" in marktvisie:
    active_strategies = ["BullCall", "BullPut", "LongCall"]
elif "Bearish" in marktvisie:
    active_strategies = ["BearCall", "BearPut", "LongPut"]
elif "Neutraal" in marktvisie:
    active_strategies = ["IronCondor", "Strangle"]

# Allow manual override if needed?
# For now, stick to user request: "Kies als selectie versnelling... stijgende of dalende koersverwachting"
st.sidebar.markdown(f"**Actieve Strategieën:** {', '.join(active_strategies)}")

# Batch Scanner Input
scan_mode = st.sidebar.selectbox("Scan Modus", ["Enkel Symbool", "Batch Scan (Lijst)", "Batch Scan (Bestand)", "Live TWS Scanner", "BarChart Optie Flow (CSV)", "Auto-Pilot (Downloads map)"])

symbols_to_scan = []
scan_code = "MOST_ACTIVE" 
num_rows = 20

if scan_mode == "Enkel Symbool":
    sec_type = st.sidebar.radio("Type Activa", ["Aandeel", "Index"])
    symbol_input = st.sidebar.text_input("Symbool (bijv. SPY)", value="SPY")
    if symbol_input:
        symbols_to_scan = [symbol_input]

elif scan_mode == "Batch Scan (Lijst)":
    # Pre-defined lists
    list_choice = st.sidebar.selectbox("Kies Lijst", ["S&P 100", "Top 10 Tech", "AEX"])
    if list_choice == "Top 10 Tech":
        symbols_to_scan = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "NFLX", "AMD", "INTC"]
        sec_type = "Aandeel"
    elif list_choice == "S&P 100":
        # Placeholder list - in real app fetch this or use larger list
        symbols_to_scan = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK.B", "UNH", "JNJ", "XOM", "JPM"] # Shortened for speed
        sec_type = "Aandeel"
    elif list_choice == "AEX":
        symbols_to_scan = ["ADYEN", "ASML", "UNA", "RDSA", "INGA"] # Note: RDSA ticker might be different on TWS (SHELL)
        sec_type = "Aandeel"

elif scan_mode == "Batch Scan (Bestand)":
    uploaded_file = st.sidebar.file_uploader("Upload Excel/CSV", type=['xlsx', 'csv'])
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            # Normalize columns to uppercase to avoid case-sensitivity issues
            df.columns = df.columns.str.upper().str.strip()
            
            if 'SYMBOL' in df.columns:
                symbols_to_scan = df['SYMBOL'].dropna().tolist()
                st.sidebar.success(f"{len(symbols_to_scan)} symbolen geladen.")
            else:
                st.sidebar.error("Bestand moet kolom 'Symbol' bevatten.")
        except Exception as e:
            st.sidebar.error(f"Fout bij laden: {e}")
    sec_type = "Aandeel" # Assume stocks for custom lists usually

elif scan_mode == "BarChart Optie Flow (CSV)":
    st.sidebar.info("Importeer Barchart CSV's om 'Smart Money' trade setups te genereren.")
    barchart_files = st.sidebar.file_uploader("Upload Barchart CSV", type=['csv'], accept_multiple_files=True)
    barchart_dfs = []
    
    if barchart_files:
        for f in barchart_files:
            try:
                df = pd.read_csv(f)
                # Normalize columns to uppercase
                df.columns = df.columns.str.upper().str.strip()
                if 'SYMBOL' in df.columns:
                    # Rename back to Symbol for compatibility downstream if necessary, or keep SYMBOL
                    df = df.rename(columns={'SYMBOL': 'Symbol'})
                    barchart_dfs.append(df)
            except Exception as e:
                st.sidebar.error(f"Fout in {f.name}: {e}")
        
        if barchart_dfs:
            combined_barchart = pd.concat(barchart_dfs, ignore_index=True)
            st.session_state['barchart_raw'] = combined_barchart
            symbols_to_scan = list(combined_barchart['Symbol'].dropna().unique())
            st.sidebar.success(f"{len(barchart_dfs)} bestand(en) ingeladen. {len(symbols_to_scan)} unieke symbolen.")
        else:
            st.session_state['barchart_raw'] = pd.DataFrame()
            st.sidebar.warning("Geen geldige symbolen in de CSV(s) gevonden.")
    sec_type = "Aandeel"

elif scan_mode == "Live TWS Scanner":
    st.sidebar.info("Haalt live 'Most Active', 'Top Gainers' etc. op van TWS")
    scan_code = st.sidebar.selectbox("Scan Criteria", ["MOST_ACTIVE", "TOP_PERC_GAIN", "HOT_BY_VOLUME", "OPT_VOLUME_MOST_ACTIVE"])
    num_rows = st.sidebar.slider("Aantal resultaten", 10, 50, 20)
    sec_type = "Aandeel"
    # Logic to fetch happens inside "Start Scan" to avoid premature connection

elif scan_mode == "Auto-Pilot (Downloads map)":
    st.sidebar.info("Wacht op een specifiek tijdstip en laadt dan de 'AG SYMBOLS VOOR SCANNER' file uit je Downloads map.")
    
    import datetime
    default_t = datetime.time(15, 40)
    auto_pilot_time = st.sidebar.time_input("Start Tijd (Uur/Min)", value=default_t, help="Kies bijv. 15:40 voor zomertijd of 16:40 / 14:40 voor wintertijd (10 min na beursopening).")
    
    # We delay loading symbols_to_scan until the actual execute phase so the user can replace the file while waiting
    sec_type = "Aandeel"

# Filters
st.sidebar.subheader("Filters & Criteria")
min_dte = st.sidebar.number_input("Min Dagen tot Expiratie", value=5)
max_dte = st.sidebar.number_input("Max Dagen tot Expiratie", value=32)
width = st.sidebar.number_input("Spread Breedte", value=10)
min_pop = st.sidebar.slider("Min Kans op Winst (PoP %)", 0, 100, 50)
min_profit = st.sidebar.number_input("Min Winst Potentie ($)", value=100)
max_pain_buffer = st.sidebar.number_input("Max Pain Buffer (Punten)", value=5, help="Minimale afstand tot Max Pain strike")

# Koopadvies (Buy Recommendation) Filters
st.sidebar.subheader("Koopadvies Instellingen")
koopadvies_p = st.sidebar.slider("Koopadvies Drempel (p %)", -5.0, 10.0, 1.0, step=0.5, help="Aandeel hoeft slechts p% te stijgen/dalen voor winst") / 100.0
only_koopadvies = st.sidebar.checkbox("Alleen Koopadvies tonen", value=False)
strike_range_pct = st.sidebar.number_input("Afstand tot Koers % (Strike Range)", min_value=-50.0, max_value=50.0, value=30.0, step=1.0, help="Positief = Bull Spreads ONDER de koers. Negatief = Bull Spreads BOVEN de koers.") / 100.0
min_strike_pct = st.sidebar.number_input("Min. afstand tot Koers %", min_value=-50.0, max_value=50.0, value=2.0, step=1.0, help="Minimale foutmarge (Positief = extra marge. Negatief = sta In The Money toe).") / 100.0
itm_support_level = st.sidebar.selectbox(
    "ITM Veiligheidsmarge (Support Niveau)", 
    ["Standaard (Min. afstand %)", "Niveau 1 (1x Expected Move)", "Niveau 2 (2x Expected Move)", "Niveau 3 (Extreme / 2.5x)"],
    help="Voor in-the-money (of veilige) positionering: kies de marge die bovenop de koers wordt gelegd."
)
# Additional Strategy Overrides/Toggles
with st.sidebar.expander("Specifieke Strategieën", expanded=True):
    strategy_options = ["BullCall", "BullPut", "BearCall", "BearPut", "LongCall", "LongPut", "IronCondor", "Strangle"]
    final_strategies = []
    for s in strategy_options:
        # Default checked based on Marktvisie, but user can override
        is_default = s in active_strategies
        if st.checkbox(s, value=is_default):
            final_strategies.append(s)
    active_strategies = final_strategies
with st.sidebar.expander("Greeks & Advanced Filters", expanded=False):
    min_delta = st.sidebar.slider("Min Delta (Short Leg)", 0.0, 1.0, 0.05, step=0.01)
    min_gamma = st.sidebar.number_input("Min Gamma Exposure", value=0.0, step=0.001, format="%.4f")

    st.markdown("---")
    st.markdown("**Markt Indicatoren (van Selectiemodel)**")
    curr_gex = st.sidebar.number_input("Huidige GEX", value=0.0, help="Negatief voor Bullish momentum")
    curr_dex = st.sidebar.number_input("Huidige DEX", value=0.0, help="Positief voor Bullish momentum")
    curr_pc = st.sidebar.number_input("Put/Call Ratio", value=1.0, step=0.1, help="< 0.7 voor Bullish")

    use_auto_sentiment = st.sidebar.checkbox("Gebruik automatisch sentiment model", value=True)

    # Max Pain Distance Filter
    use_max_pain_filter = st.checkbox("Filter op Max Pain Afstand", value=False)
    max_pain_dist = st.number_input("Max Afstand Spread tot Max Pain ($)", value=20.0) 

    # Use checkbox for auto-tuning
    auto_tune = st.checkbox("Auto-Tune (Versoepel filters indien geen resultaat)", value=True)

# Ranking
st.sidebar.subheader("Ranking Prioriteit")
ranking_criteria = st.sidebar.multiselect(
    "Sorteer op (in volgorde)",
    ["AG Score", "Profit", "PoP", "Max Pain Distance", "Gamma", "Delta", "Theta"],
    default=["AG Score"]
)

# Technical Filters (New)
st.sidebar.subheader("Technische Filters (EMA)")
use_ema = st.sidebar.checkbox("Filter op EMA Trend (Prijs > EMA)")
ema_spans = []
if use_ema:
    if st.sidebar.checkbox("EMA 8"): ema_spans.append(8)
    if st.sidebar.checkbox("EMA 50"): ema_spans.append(50)
    if st.sidebar.checkbox("EMA 150"): ema_spans.append(150)
    use_ema_crossover = st.sidebar.checkbox("EMA 8 > EMA 50 (Crossover)", value=False)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Stoch RSI (14, 9, 3, 6)**")
    use_stoch_rsi = st.sidebar.checkbox("Filter op Stoch RSI Entry")
    if use_stoch_rsi:
        stoch_entry_a = st.sidebar.checkbox("Entry A (Bull Cross < 20)", value=True)
        stoch_entry_b = st.sidebar.checkbox("Entry B (Stijgend 20-60)", value=True)
        stoch_entry_c = st.sidebar.checkbox("Entry C (Cross > 50)", value=False)
    else:
        stoch_entry_a = stoch_entry_b = stoch_entry_c = False
else:
    use_ema_crossover = False
    use_stoch_rsi = False
    stoch_entry_a = stoch_entry_b = stoch_entry_c = False

# Main Area
st.title("Optie Contract Selectie Tool - AntiGravity")

# Weekend Warning
import datetime
today = datetime.datetime.now().weekday()
if today >= 5: # 5 = Saturday, 6 = Sunday
    st.warning("⚠️ **Weekend Modus Actief**: TWS levert momenteel beperkte live data. De scanner gebruikt de prijzen van afgelopen vrijdag (sluiting) als fallback voor berekeningen.")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["🚀 Scanner", "📊 Resultaten", "🛒 Orders", "📈 S&P 500 Spreads", "💰 Dividend CC"])

# --- TAB 1: SCANNER ---
with tab1:
    if st.session_state.tws_configured:
        st.write(f"Marktvisie: {marktvisie} -> Strategieën: {', '.join([str(s) for s in active_strategies])}")
        if scan_mode == "Live TWS Scanner":
            st.write(f"Scan Modus: {scan_mode} ({scan_code}) - Wordt opgehaald bij start.")
        else:
            st.write(f"Scan Modus: {scan_mode} - {len(symbols_to_scan)} symbolen te scannen.")

        col1, col2, col3 = st.columns(3)
        with col1:
            start_pressed = st.button("Start Scan / Activeer Auto-Pilot", type="primary")

            if start_pressed:
                if scan_mode == "Auto-Pilot (Downloads map)":
                    import os, time, datetime
                    downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads')
                    csv_path = os.path.join(downloads_path, 'AG SYMBOLS VOOR SCANNER.csv')
                    xlsx_path = os.path.join(downloads_path, 'AG SYMBOLS VOOR SCANNER.xlsx')
                    
                    now = datetime.datetime.now()
                    
                    # Check if time already passed today (with a 5 min grace period)
                    if now.time() >= auto_pilot_time:
                        st.warning(f"Waarschuwing: Het is nu {now.strftime('%H:%M')}, wat al na de ingestelde tijd van {auto_pilot_time.strftime('%H:%M')} is. Scanner start direct!")
                        time.sleep(3)
                    else:
                        st.info(f"⏱️ **Auto-Pilot Actief**. Scherm open laten. Scanner pauzeert tot {auto_pilot_time.strftime('%H:%M')}.")
                        timer_ph = st.empty()
                        while True:
                            current_now = datetime.datetime.now()
                            if current_now.time() >= auto_pilot_time:
                                break
                            timer_ph.markdown(f"**Huidige tijd:** {current_now.strftime('%H:%M:%S')} - wacht tot {auto_pilot_time.strftime('%H:%M:00')} om TWS en de scan te starten...")
                            time.sleep(1)
                        timer_ph.empty()
                        st.success("Tijd bereikt! Scanner wordt gestart...")
                        
                    target_file = None
                    if os.path.exists(csv_path): target_file = csv_path
                    elif os.path.exists(xlsx_path): target_file = xlsx_path
                    
                    if not target_file:
                        st.error(f"Bestand niet gevonden! Controleer of '{csv_path}' of '.xlsx' bestaat.")
                        st.stop()
                    else:
                        try:
                            if target_file.endswith('.csv'): df_auto = pd.read_csv(target_file)
                            else: df_auto = pd.read_excel(target_file)
                            
                            # Normalize columns
                            df_auto.columns = df_auto.columns.str.upper().str.strip()
                            if 'SYMBOL' in df_auto.columns:
                                symbols_to_scan = df_auto['SYMBOL'].dropna().tolist()
                                st.success(f"{len(symbols_to_scan)} symbolen ingeladen uit {target_file}.")
                            else:
                                st.error("Bestand moet een kolom 'Symbol' bevatten.")
                                st.stop()
                        except Exception as e:
                            st.error(f"Fout bij lezen Auto-Pilot bestand: {e}")
                            st.stop()

                import random
                scan_client_id = random.randint(10000, 99999)

                scan_ib = IBClient()
                success, msg = scan_ib.connect(tws_host, tws_port, scan_client_id)

                if not success:
                    st.error(f"Kan geen verbinding maken voor scan (ID: {scan_client_id}): {msg}")
                else:
                     try:
                         # Set DataType
                         dtype = 1 if use_live_data else 3
                         scan_ib.set_data_type(dtype)

                         # Init Scanner
                         scanner = SpreadScanner(scan_ib)

                         # Check for Live Scanner Mode
                         current_symbols = list(symbols_to_scan) # copy
                         user_requested_strategies = list(active_strategies)
                         if scan_mode == "Live TWS Scanner":
                             status_ph = st.empty()
                             status_ph.text(f"📡 Ophalen live scanner data ({scan_code})...")
                             live_symbols = scan_ib.get_scanner_data(scan_code, rows=num_rows)
                             if live_symbols:
                                 current_symbols = live_symbols
                                 st.success(f"Opgehaald: {len(current_symbols)} symbolen: {current_symbols[:5]}...")
                             else:
                                 st.error("Scanner heeft geen resultaten teruggegeven.")
                                 current_symbols = []

                         if not current_symbols:
                             st.warning("Geen symbolen om te scannen.")
                         else:
                             # ... Proceed with Main Scan Logic ...
                             scan_status = st.empty()
                             scan_status.text("🚀 Bezig met scannen... (Even geduld)")

                             all_results = pd.DataFrame()
                             
                             # Core Hash Calculation for Caching
                             import hashlib, json
                             core_config = {
                                 'symbols': sorted(current_symbols), 'strategies': sorted(active_strategies),
                                 'min_dte': min_dte, 'max_dte': max_dte,
                                 'width': width, 'strike_range_pct': strike_range_pct, 'min_strike_pct': min_strike_pct, 'use_auto_sentiment': use_auto_sentiment,
                                 'use_ema': use_ema, 'use_ema_crossover': use_ema_crossover, 'use_stoch_rsi': use_stoch_rsi,
                                 'stoch_entry_a': stoch_entry_a, 'stoch_entry_b': stoch_entry_b, 'stoch_entry_c': stoch_entry_c
                             }
                             core_hash = hashlib.md5(json.dumps(core_config, sort_keys=True).encode()).hexdigest()
                             
                             use_cache = st.session_state.get('last_core_hash') == core_hash and 'all_unfiltered_global' in st.session_state
                             if use_cache:
                                 all_unfiltered_global = st.session_state['all_unfiltered_global']
                             else:
                                 all_unfiltered_global = pd.DataFrame()
                             # Progress bar
                             progress_bar = st.progress(0)
                             status_text = st.empty()
                             log_area = st.expander("📋 Scan Log (Debug)", expanded=True)
                             with log_area:
                                 log_placeholder = st.empty()

                             log_messages = []

                             def log(msg):
                                 """Helper to log messages to UI"""
                                 log_messages.append(msg)
                                 log_placeholder.text("\n".join(log_messages)) # Show all or large tail

                             log(f"🚀 Start scan: {len(current_symbols)} symbolen te verwerken")
                             log(f"📊 Strategieën: {', '.join(active_strategies)}")
                             log(f"⚙️ Filters: DTE={min_dte}-{max_dte}, Width={width}, MinPoP={min_pop}%, MinProfit=${min_profit}")
                             if koopadvies_p > 0:
                                 log(f"🎯 Koopadvies Drempel: {koopadvies_p*100:.1f}%")
                             if datetime.datetime.now().weekday() >= 5:
                                 log("📅 Weekend gedetecteerd: Gebruik 'Close' prijzen als fallback.")

                             barchart_df_parsed = pd.DataFrame()
                             if scan_mode == "BarChart Optie Flow (CSV)" and 'barchart_raw' in st.session_state:
                                 status_text.text("Parsen van Barchart Option Flow CSV via VBA logica...")
                                 log("📊 Barchart 'Smart Money' filters toepassen...")
                                 barchart_df_parsed = scanner.parse_barchart_flow(st.session_state['barchart_raw'])
                                 log(f"   ✅ {len(barchart_df_parsed)} Smart Money Setup(s) succesvol vertaald naar verticals.")
                                 # Limit current symbols to just the ones that actually passed the flow filters
                                 if not barchart_df_parsed.empty:
                                     current_symbols = list(barchart_df_parsed['symbol'].unique())
                                 else:
                                     current_symbols = []

                             # 1. Technical Filter (EMA) Batch
                             if use_ema and ema_spans:
                                 log(f"📈 EMA Filter actief: {ema_spans}")
                                 status_text.text("Bezig met ophalen historische data voor EMA filter...")

                             # 2. Main Loop
                             approved_symbols = []

                             for i, sym in enumerate(current_symbols):
                                 price = 0.0
                                 underlying_iv = 0.0
                                 progress = (i / len(current_symbols))
                                 progress_bar.progress(progress)
                                 status_text.text(f"Analyseren: {sym} ({i+1}/{len(current_symbols)})")
                                 log(f"\n🔍 [{i+1}/{len(current_symbols)}] Verwerken: {sym}")

                                 if use_cache:
                                     log(f"   ⚡ Gebruik cache voor {sym} (Alleen filters toepassen)")
                                     if all_unfiltered_global.empty or 'symbol' not in all_unfiltered_global.columns:
                                         continue
                                     processed_spreads = all_unfiltered_global[all_unfiltered_global['symbol'] == sym]
                                     if processed_spreads.empty: continue
                                     
                                     current_filters = {
                                         'min_pop': min_pop, 'min_profit': min_profit, 'min_delta': min_delta,
                                         'min_gamma': min_gamma, 'max_dte': max_dte, 'min_dte': min_dte, 
                                         'koopadvies_p': koopadvies_p, 'only_koopadvies': only_koopadvies
                                     }
                                     if use_max_pain_filter: current_filters['max_pain_dist'] = max_pain_dist
                                     
                                     filtered = scanner.filter_spreads(processed_spreads, current_filters, log_func=log)
                                     
                                     if auto_tune and len(filtered) < 5:
                                         ret_count = 0
                                         while len(filtered) < 5 and ret_count < 2:
                                             log(f"   🔄 Auto-Tune: Versoepelen filters (poging {ret_count+1})...")
                                             current_filters['min_profit'] *= 0.5
                                             if 'min_delta' in current_filters: current_filters['min_delta'] = max(0.05, current_filters['min_delta'] - 0.05)
                                             if 'max_pain_dist' in current_filters: current_filters['max_pain_dist'] += 10.0
                                             filtered = scanner.filter_spreads(processed_spreads, current_filters, log_func=log)
                                             log(f"   📊 {len(filtered)} spreads na versoepeling")
                                             ret_count += 1
                                             
                                     if not filtered.empty:
                                         all_results = pd.concat([all_results, filtered], ignore_index=True)
                                         log(f"   ✅ {len(filtered)} spreads toegevoegd aan resultaten (via cache)")
                                     continue

                                 # Check connection for non-cached TWS lookup
                                 if not scan_ib.is_connected():
                                     log("❌ Verbinding verloren!")
                                     st.error("Verbinding verloren.")
                                     break

                                 try:
                                     # Create contract - Robust classification for ETFs vs Indexes
                                     is_likely_index = sym.upper() in ['SPX', 'NDX', 'RUT', 'VIX', 'DAX']
                                     if sec_type == "Index" or is_likely_index:
                                         if sym.upper() == 'SPY': # SPY is a Stock (ETF), not an Index
                                             contract = Stock(sym, 'SMART', 'USD')
                                         else:
                                             contract = Index(sym, 'SMART', 'USD')
                                     else:
                                         contract = Stock(sym, 'SMART', 'USD')

                                     # 1. Get Real-time Price Snapshot
                                     market_data = scan_ib.get_market_data_snapshot(contract, use_hist_fallback=False)
                                     price = market_data.get('price', 0.0)
                                     underlying_iv = market_data.get('iv', 0.0)
                                     data_source = market_data.get('source', 'Unknown')

                                     # 2. Earnings Date & Technical Check
                                     earnings_date = scan_ib.get_earnings_date(sym)
                                     days_to_earnings = None
                                     if earnings_date:
                                         days_to_earnings = (earnings_date.normalize() - pd.Timestamp.now().normalize()).days
                                         log(f"   📅 Volgende earnings: {earnings_date.strftime('%Y-%m-%d')} ({days_to_earnings} dagen)")

                                     hist_data = scan_ib.get_historical_data(contract, duration='6 M', bar_size='1 day')
                                     hist_iv_df = scan_ib.get_historical_iv(contract, duration='1 Y')

                                     if hist_data.empty:
                                         log(f'   ⚠️ Geen historische data voor {sym}. EMA & Technical filters overgeslagen.')

                                     hist_price = float(hist_data['close'].iloc[-1]) if not hist_data.empty else 0.0

                                     # Consistency Check: Live vs Historical
                                     if price > 0 and hist_price > 0:
                                         price_diff_pct = abs(price - hist_price) / price
                                         if price_diff_pct > 0.50:
                                             log(f"   ⚠️ Prijs mismatch ({price_diff_pct:.0%}). Hist_price ${hist_price:.2f} is waarschijnlijk verouderd.")
                                             hist_data = pd.DataFrame() # Discard stale history
                                             hist_price = 0.0

                                     if price <= 0 and hist_price > 0:
                                         price = hist_price
                                         log(f"   ⚠️ Live prijs ontbreekt. Gebruik historische slotkoers: ${price:.2f}")

                                     if price > 0:
                                         st.session_state.symbol_prices[sym] = price
                                         update_price_dashboard() # TRIGGER LIVE UPDATE
                                         
                                         # NOW calculate tech levels with final price
                                         if not hist_data.empty:
                                             tech_levels = scanner.find_technical_levels(hist_data, ref_price=price)
                                             log(f"   📊 [DEBUG] Tech levels gevonden voor {sym} (Price ${price:.2f}): S:{len(tech_levels['supports'])}, R:{len(tech_levels['resistances'])}")
                                             if tech_levels['supports']:
                                                 log(f'   📉 Supports (Tier 1-3): { ", ".join([f"${s:.2f}" for s in tech_levels["supports"]]) }')
                                             if tech_levels['resistances']:
                                                 log(f'   📈 Resistances (Tier 1-3): { ", ".join([f"${r:.2f}" for r in tech_levels["resistances"]]) }')
                                         
                                         # IV Fallback from history if snapshot is zero or suspiciously low (< 5%)
                                         if underlying_iv < 0.05 and not hist_iv_df.empty:
                                             underlying_iv = float(hist_iv_df['iv'].iloc[-1])
                                             log(f"   💰 IV Fallback van historie: {underlying_iv*100:.2f}%")
                                     else:
                                         log(f"   ❌ Kan geen prijs ophalen for {sym} (Live noch Historisch). Overslaan.")
                                         continue

                                     # 3. EMA Check
                                     if use_ema and ema_spans:
                                         if hist_data.empty:
                                             log(f"   ⚠️ Kan EMA niet checken zonder historie. Filter genegeerd.")
                                         else:
                                             check_price = price if (price and price > 0) else (hist_data['close'].iloc[-1] if not hist_data.empty else 0)
                                             if check_price <= 0:
                                                 log(f"   ❌ Geen prijs beschikbaar voor EMA check op {sym}")
                                                 continue

                                             sym_data = {sym: {'price': check_price, 'history': hist_data}}
                                             passed = scanner.filter_symbols_by_ema(sym_data, ema_spans, ema_crossover=use_ema_crossover)
                                             if not passed:
                                                 log(f"   ⛔ {sym} gefilterd door EMA check")
                                                 continue # Skip this symbol
                                             log(f"   ✅ {sym} doorstaat EMA filter")

                                     if use_auto_sentiment:
                                         indicators = {'gex': curr_gex, 'dex': curr_dex, 'pc_ratio': curr_pc}
                                         auto_sentiment = scanner.assess_market_sentiment(price, hist_data, indicators)
                                         log(f"   🤖 Automatisch sentiment gedetecteerd: {auto_sentiment}")

                                         if auto_sentiment == 'Bullish':
                                             suggested = ['BullCall', 'BullPut', 'LongCall']
                                         elif auto_sentiment == 'Bearish':
                                             suggested = ['BearCall', 'BearPut', 'LongPut']
                                         else:
                                             suggested = ['IronCondor', 'Strangle']

                                         overlap = [s for s in user_requested_strategies if s in suggested]
                                         if overlap:
                                             active_strategies = overlap
                                             log(f'   🤖 Automatisch sentiment ({auto_sentiment}) verfijnd naar: {active_strategies}')
                                         else:
                                             active_strategies = user_requested_strategies
                                             log(f'   ⚠️ Sentiment is {auto_sentiment}, maar gebruiker koos {user_requested_strategies}. Intentie behouden.')
                                     else:
                                         auto_sentiment = "Neutral"

                                     # --- New Technical Signals Entry Check ---
                                     tech_signals = scanner.get_technical_signals(hist_data, price)
                                     log(f"   📉 EMA Status: {tech_signals['ema_status']}")
                                     log(f"   📊 Stoch RSI: {tech_signals['stoch_rsi_status']}")

                                     if use_stoch_rsi:
                                         match_a = stoch_entry_a and tech_signals['entry_a']
                                         match_b = stoch_entry_b and tech_signals['entry_b']
                                         match_c = stoch_entry_c and tech_signals['entry_c']
                                         
                                         if not (match_a or match_b or match_c):
                                             log(f"   ⛔ {sym} gefilterd door Stoch RSI entry voorwaarden")
                                             continue
                                         log(f"   ✅ {sym} doorstaat Stoch RSI filter")

                                     log(f"   💰 Prijs: ${price:.2f}, IV: {underlying_iv:.2%} ({data_source})")

                                     if (int(max_dte) - int(min_dte)) <= 3:
                                         log(f"   ⚠️ Nauwe DTE range ({min_dte}-{max_dte}d). Dit kan leiden tot 0 resultaten.")

                                     log(f"   Optieketens opvragen...")
                                     sec_type_str = 'IND' if contract.secType == 'IND' else 'STK'
                                     chains = scan_ib.get_option_chains_params(sym, sec_type=sec_type_str)

                                     if not chains:
                                         log(f"   ⚠️ Geen optie chains gevonden voor {sym}")
                                         continue
                                     log(f"   ✅ {len(chains)} chain(s) gevonden")

                                     log(f"   🎯 Genereren spreads voor strategieën: {active_strategies}...")

                                     max_dte_to_use = max_dte
                                     is_relaxed_earnings = False
                                     if days_to_earnings is not None and days_to_earnings - 2 >= min_dte:
                                         max_dte_to_use = min(max_dte, days_to_earnings - 2)
                                         log(f"   🛡️ Earnings-beperking actief: Max DTE ingesteld op {max_dte_to_use}")

                                     def run_gen(d_max):
                                         res = pd.DataFrame()
                                         
                                         if scan_mode == "BarChart Optie Flow (CSV)":
                                             if not barchart_df_parsed.empty:
                                                 sym_spreads = barchart_df_parsed[barchart_df_parsed['symbol'] == sym].copy()
                                                 if not sym_spreads.empty:
                                                     if underlying_iv > 0: sym_spreads['iv'] = underlying_iv
                                                     res = sym_spreads
                                         else:
                                             widths_to_check = [int(width)]
                                             if price > 0:
                                                 if price < 50 and 5 not in widths_to_check:
                                                     widths_to_check.append(5)
                                                 if price > 400 and 15 not in widths_to_check:
                                                     widths_to_check.append(15)
                                                     
                                             for w in widths_to_check:
                                                 p = {'symbol': sym, 'min_dte': min_dte, 'koopadvies_p': koopadvies_p, 'only_koopadvies': only_koopadvies, 'max_dte': d_max, 
                                                     'width': w, 'iv': underlying_iv, 'strike_range_pct': strike_range_pct, 'min_strike_pct': min_strike_pct,
                                                     'itm_support_level': itm_support_level}
                                                 for strat in active_strategies:
                                                     fs = scanner.generate_spreads(chains, strat, price, p, log_func=log)
                                                     if fs is not None and not fs.empty:
                                                         res = pd.concat([res, fs], ignore_index=True)
                                         return res

                                     raw_spreads_all = run_gen(max_dte_to_use)

                                     if raw_spreads_all.empty and max_dte_to_use < max_dte:
                                         log("   ⚠️ Geen resultaten binnen earnings-beperking, DTE filter versoepelen...")
                                         raw_spreads_all = run_gen(max_dte)
                                         is_relaxed_earnings = True

                                     if not raw_spreads_all.empty:
                                         raw_spreads_all['relaxed_earnings'] = is_relaxed_earnings
                                         s_str = ", ".join([f"{s:.2f}" for s in tech_levels.get('supports', [])])
                                         r_str = ", ".join([f"{r:.2f}" for r in tech_levels.get('resistances', [])])
                                         raw_spreads_all['supports'] = s_str
                                         raw_spreads_all['resistances'] = r_str
                                         log(f"   📝 {len(raw_spreads_all)} kandidaten gegenereerd")
                                     else:
                                         log(f"   ⚠️ Geen {sym} kandidaten (check DTE of Breedte).")
                                         continue

                                     processed_spreads = pd.DataFrame()
                                     unique_expirations = raw_spreads_all['expiry'].unique()
                                     log(f"   📡 Ophalen Greeks & Max Pain ({len(unique_expirations)} expiraties)...")

                                     for idx, exp in enumerate(unique_expirations):
                                         exp_spreads = raw_spreads_all[raw_spreads_all['expiry'] == exp].copy()
                                         if exp_spreads.empty: continue
                                         log(f'      [{idx+1}/{len(unique_expirations)}] Download data voor expiratie {exp}...')

                                         valid_strikes_for_exp = []
                                         for chain in chains:
                                             if exp in chain.expirations:
                                                 valid_strikes_for_exp.extend(chain.strikes)
                                         valid_strikes_for_exp = sorted(list(set(valid_strikes_for_exp)))

                                         if price > 0:
                                             lower_bound = price * 0.70
                                             upper_bound = price * 1.30
                                             wide_strikes = [s for s in valid_strikes_for_exp if lower_bound <= s <= upper_bound]
                                         else:
                                             wide_strikes = valid_strikes_for_exp

                                         found_strikes = set()
                                         for col in ['strike_buy', 'strike_sell', 'strike_p_buy', 'strike_p_sell', 'strike_c_buy', 'strike_c_sell']:
                                             if col in exp_spreads.columns:
                                                 found_strikes.update(exp_spreads[col].dropna().unique().tolist())
                                         found_strikes.discard(0.0)
                                         spread_strikes = found_strikes
                                         final_strikes = sorted(list(set(wide_strikes) | spread_strikes))

                                         chain_data = scan_ib.get_chain_greeks_and_oi(sym, exp, final_strikes)

                                         if not chain_data.empty:
                                             m_struct = scanner.analyze_market_structure(chain_data)
                                             mp = m_struct.get('max_pain', 0)
                                             cw = m_struct.get('call_wall', 0)
                                             pw = m_struct.get('put_wall', 0)
                                             gw = m_struct.get('gex_wall', 0)
                                             log(f"      Expiratie {exp}: Max Pain=${mp:.2f}, Call Wall=${cw}, Put Wall=${pw}, GEX Wall=${gw}")

                                         log(f"      [DEBUG] Expiratie {exp}: chain_data rijen={len(chain_data)}")
                                         if not chain_data.empty:
                                             # Check for Greeks presence
                                             has_greeks = (chain_data['delta'] != 0).any()
                                             log(f"      [DEBUG] chain_data heeft greeks: {has_greeks}")

                                         enriched = scanner.calculate_metrics(
                                             exp_spreads, scan_ib, sym, 
                                             underlying_price=price, 
                                             chain_data=chain_data,
                                             underlying_iv=underlying_iv,
                                             hist_iv_df=hist_iv_df,
                                             log_func=log, koopadvies_p=koopadvies_p
                                         )

                                         if scan_mode == "BarChart Optie Flow (CSV)":
                                             d_min_dl, d_min_gm, d_max_dt, d_min_dt = -1.0, -1.0, 9999, 0
                                         else:
                                             d_min_dl, d_min_gm, d_max_dt, d_min_dt = min_delta, min_gamma, max_dte, min_dte
                                             
                                         current_filters = {
                                             'min_pop': min_pop,
                                             'min_profit': min_profit,
                                             'min_delta': d_min_dl,
                                             'min_gamma': d_min_gm,
                                             'max_dte': d_max_dt,
                                             'min_dte': d_min_dt, 
                                             'koopadvies_p': koopadvies_p, 'only_koopadvies': only_koopadvies
                                         }
                                         if use_max_pain_filter:
                                             current_filters['max_pain_dist'] = max_pain_dist

                                         if not enriched.empty:
                                             # 1. Add technical info
                                             enriched['EMA_Cross'] = tech_signals['ema_status']
                                             enriched['Stoch_RSI'] = tech_signals['stoch_rsi_status']
                                             enriched['Sentiment'] = auto_sentiment
                                             
                                             # 2. Add technical levels and other metrics
                                             s_str = ", ".join([f"${s:.2f}" for s in tech_levels.get('supports', [])])
                                             r_str = ", ".join([f"${r:.2f}" for r in tech_levels.get('resistances', [])])
                                             if not s_str: s_str = "Geen"
                                             if not r_str: r_str = "Geen"

                                             enriched['supports'] = s_str
                                             enriched['resistances'] = r_str

                                             gf = scanner.calculate_gamma_flip(chain_data)
                                             enriched['gamma_flip'] = gf
                                             
                                             # 3. Collect for global results
                                             all_unfiltered_global = pd.concat([all_unfiltered_global, enriched], ignore_index=True)
                                             processed_spreads = pd.concat([processed_spreads, enriched])
                                             log(f"      [DEBUG] Expiratie {exp}: {len(enriched)} spreads verwerkt.")

                                     log(f"   ✅ Totaal {len(processed_spreads)} kandidaten for {sym} na TWS verificatie.")

                                     guidance = scanner.get_filter_guidance(processed_spreads, target_n=10)
                                     if guidance:
                                         guidance_msg = f"💡 **Target 10 Guidance**: Voor ~10 resultaten, probeer: "
                                         parts = []
                                         if 'suggested_pop' in guidance: parts.append(f"PoP > {guidance['suggested_pop']}%")
                                         if 'suggested_profit' in guidance: parts.append(f"Winst > ${guidance['suggested_profit']}")
                                         if 'suggested_delta' in guidance: parts.append(f"Delta Sell ~ {guidance['suggested_delta']}")
                                         log(guidance_msg + " | ".join(parts))

                                     filtered = scanner.filter_spreads(processed_spreads, current_filters, log_func=log)

                                     if auto_tune and len(filtered) < 5:
                                         ret_count = 0
                                         while len(filtered) < 5 and ret_count < 2:
                                             log(f"   🔄 Auto-Tune: Versoepelen filters (poging {ret_count+1})...")
                                             current_filters['min_profit'] *= 0.5
                                             if 'min_delta' in current_filters:
                                                 current_filters['min_delta'] = max(0.05, current_filters['min_delta'] - 0.05)
                                             if 'max_pain_dist' in current_filters:
                                                 current_filters['max_pain_dist'] += 10.0
                                             filtered = scanner.filter_spreads(processed_spreads, current_filters, log_func=log)
                                             log(f"   📊 {len(filtered)} spreads na versoepeling")
                                             ret_count += 1

                                     if not filtered.empty:
                                         all_results = pd.concat([all_results, filtered], ignore_index=True)
                                         log(f"   ✅ {len(filtered)} spreads toegevoegd aan resultaten")
                                     else:
                                         log(f"   ⚠️ Geen spreads voldoen aan criteria voor {sym}")

                                 except Exception as e:
                                     # Streamlit needs to be allowed to halt the script if the user stops the app
                                     if type(e).__name__ in ['StopException', 'RerunException']:
                                         raise
                                     log(f"   ❌ ERROR bij verwerken {sym}: {str(e)}")
                                     import traceback
                                     log(f'   📋 Details: {str(traceback.format_exc())[:200]}')
                                     continue
                                     
                             if not use_cache:
                                 st.session_state['last_core_hash'] = core_hash
                                 st.session_state['all_unfiltered_global'] = all_unfiltered_global
                                 
                             if not all_results.empty:
                                 # Rank global results
                                 # Default to Profit if empty
                                 if not ranking_criteria:
                                     ranking_criteria = ["Profit", "PoP"]
                                     log("⚠️ Geen sorteer criteria geselecteerd. Default: Profit, PoP")

                                 log(f"🏆 Ranking spreads op criteria: {ranking_criteria}...")
                                 # Calculate Sort Criteria based on user selection in sidebar
                                 criteria = []
                                 for c in ranking_criteria:
                                     if c == "Profit": criteria.append("max_profit")
                                     elif c == "PoP": criteria.append("pop")
                                     elif c == "Max Pain Distance": criteria.append("max_pain") 
                                     elif c == "Gamma": criteria.append("gamma")
                                     elif c == "Delta": criteria.append("delta")
                                     elif c == "Theta": criteria.append("theta")

                                 # Calculate global guidance for Target 10
                                 if not all_unfiltered_global.empty:
                                     global_guidance = scanner.get_filter_guidance(all_unfiltered_global, target_n=10)
                                     st.session_state['filter_guidance'] = global_guidance

                                 ranked = scanner.rank_spreads(all_results, sort_criteria=criteria, top_n=100) 
                                 st.session_state['results'] = ranked
                                 log(f"✅ Top {len(ranked)} spreads geselecteerd")
                                 st.success(f"{len(ranked)} optie contracten gevonden!")
                             else:
                                 log(f"⚠️ Geen optie contracten gevonden")
                                 st.warning("Geen optie contracten gevonden. Probeer parameters te verruimen.")
                     finally:
                         scan_ib.disconnect()

        with col2:
            if st.button("Stop"):
                st.warning("Scan gestopt.")

        # Persistent Display in Tab 1
        if 'results' in st.session_state and not st.session_state['results'].empty:
             stats_container = st.container()
             with stats_container:
                 if 'filter_guidance' in st.session_state and st.session_state['filter_guidance']:
                     g = st.session_state['filter_guidance']
                     st.info(f"💡 **Target 10 Filter Suggestie**: Om ongeveer 10 spreads over te houden, probeer: "
                             f"**PoP > {g.get('suggested_pop', '??')}%** | "
                             f"**Winst > ${g.get('suggested_profit', '??')}** | "
                             f"**Delta Sell ~ {g.get('suggested_delta', '??')}**")

             st.divider()
             st.subheader("Snel Overzicht")
             st.info("Voor details en filtering, ga naar tabblad **'📊 Resultaten'**")

             # Group by Strategy for separate tables
             df_res = st.session_state['results']
             preview_cols = ['symbol', 'strategy', 'expiry', 'strike_buy', 'strike_sell', 'max_profit', 'pop', 'expected_move', 'call_wall', 'put_wall', 'TTP (D)', 'TEI Score']
             preview_cols = [c for c in preview_cols if c in df_res.columns]

             strategies_found = df_res['strategy'].unique()
             for strat in strategies_found:
                 st.markdown(f"**Top 5: {strat}**")
                 df_strat = df_res[df_res['strategy'] == strat].head(5)
                 st.dataframe(df_strat[preview_cols], width='content', hide_index=True)

    else:
        st.warning("Configureer en test eerst de TWS verbinding in de Sidebar.")

# --- TAB 2: RESULTATEN ---
with tab2:
    if 'results' in st.session_state and not st.session_state['results'].empty:
        results = st.session_state['results']
        st.subheader(f"Gevonden Resultaten ({len(results)})")

        # Display Columns
        display_cols = [
            'symbol', 'underlying_price', 'AG_Score', 'strategy', 'expiry', 'strike_buy', 'strike_sell', 'width', 
            'strike_p_buy', 'strike_p_sell', 'strike_c_sell', 'strike_c_buy',
            'spread_mid_abs', 'spread_ask_abs', 'b_l_verschil', 'max_profit', 'pop',
            'TTP (D)', 'TEI Score', 'Efficient',
            'BEP', 'bep_afstand_pct', 'koopadvies', 'supports', 'resistances',
            'Sentiment', 'price_buy', 'price_sell', 'net_extrinsic',
            'delta_buy', 'delta_sell', 'delta', 'gamma', 'theta', 'dte', 
            'EMA_Cross', 'Stoch_RSI', 'iv_percentile', 'iv_rank', 'underlying_iv', 'expected_move', 
            'gamma_flip', 'call_wall', 'put_wall', 'gex_wall'
        ]

        # [DEBUG] Show column presence if requested
        if st.checkbox("Debug Columns", False):
            st.write(f"Columns in results: {results.columns.tolist()}")

        # Ensure columns exist before displaying
        final_cols = [c for c in display_cols if c in results.columns]
        
        # Convert Efficient bool to visual block
        if 'Efficient' in results.columns:
            results['Efficient'] = np.where(results['Efficient'] == True, "🟦", "⬜")


        # Column Configuration for Streamlit (Autosizing & Formatting)
        # Note: Removing most 'width' params to allow Streamlit's internal autosizing.
        col_cfg = {
            "symbol": st.column_config.TextColumn("Symbool"),
            "underlying_price": st.column_config.NumberColumn("Koers", format="$%.2f"),
            "AG_Score": st.column_config.NumberColumn("AG Score", format="⭐ %.1f"),
            "strategy": st.column_config.TextColumn("Strategie"),
            "expiry": st.column_config.TextColumn("Expiratie"),
            "strike_buy": st.column_config.NumberColumn("Buy Strike", format="$%.2f"),
            "strike_sell": st.column_config.NumberColumn("Sell Strike", format="$%.2f"),
            "strike_p_buy": st.column_config.NumberColumn("Put Buy", format="$%.2f"),
            "strike_p_sell": st.column_config.NumberColumn("Put Sell", format="$%.2f"),
            "strike_c_sell": st.column_config.NumberColumn("Call Sell", format="$%.2f"),
            "strike_c_buy": st.column_config.NumberColumn("Call Buy", format="$%.2f"),
            "width": st.column_config.NumberColumn("Breedte", format="$%.2f"),
            "supports": st.column_config.TextColumn("Supports (1-3)"),
            "resistances": st.column_config.TextColumn("Resistances (1-3)"),
            "iv_percentile": st.column_config.NumberColumn("IV Percentiel", format="%.1f%%"),
            "iv_rank": st.column_config.NumberColumn("IV Rank", format="%.1f%%"),
            "underlying_iv": st.column_config.NumberColumn("IV", format="%.1f%%"),
            "expected_move": st.column_config.NumberColumn("Exp. Move (1SD)", format="$%.2f"),
            "gamma_flip": st.column_config.NumberColumn("Gamma Flip", format="$%.2f"),
            "call_wall": st.column_config.NumberColumn("Call Wall", format="$%.2f"),
            "put_wall": st.column_config.NumberColumn("Put Wall", format="$%.2f"),
            "gex_wall": st.column_config.NumberColumn("GEX Wall", format="$%.2f"),
            "spread_mid_abs": st.column_config.NumberColumn("Middenprijs", format="$%.2f"),
            "spread_ask_abs": st.column_config.NumberColumn("Laatprijs", format="$%.2f"),
            "BEP": st.column_config.NumberColumn("BEP", format="$%.2f", help="Break Even Point gebaseerd op Laat prijs"),
            "bep_afstand_pct": st.column_config.NumberColumn("BEP Afstand", format="%.1f%%", help="Afstand in % tot het Break-Even Punt"),
            "koopadvies": st.column_config.TextColumn("K", help="✅ als winst >= Target 1% marge op Laatprijs"),
            "EMA_Cross": st.column_config.TextColumn("EMA 8/50"),
            "Stoch_RSI": st.column_config.TextColumn("Stoch RSI Status"),
            "Sentiment": st.column_config.TextColumn("Sentiment"),
            "price_buy": st.column_config.NumberColumn("Prijs Buy", format="$%.2f"),
            "price_sell": st.column_config.NumberColumn("Prijs Sell", format="$%.2f"),
            "net_extrinsic": st.column_config.NumberColumn("Net Extrin.", format="$%.2f"),
            "delta_buy": st.column_config.NumberColumn("Delta Buy", format="%.3f"),
            "delta_sell": st.column_config.NumberColumn("Delta Sell", format="%.3f"),
            "delta": st.column_config.NumberColumn("Net Delta", format="%.3f"),
            "gamma": st.column_config.NumberColumn("Gamma", format="%.4f"),
            "theta": st.column_config.NumberColumn("Theta", format="%.3f"),
            "dte": st.column_config.NumberColumn("DTE", format="%d", help="Rood = Earnings beperking kon niet worden gehaald"),
            "b_l_verschil": st.column_config.NumberColumn("Slip M/L", format="$%.2f", help="Verschil Midden vs Laat (Laag is beter)"),
            "max_pain": st.column_config.NumberColumn("Max Pain 1", format="$%.2f"),
            "max_pain_selection": st.column_config.NumberColumn("Max Pain 2", format="$%.2f"),
            "max_pain_buffer_ok": st.column_config.CheckboxColumn("MP Buffer OK", help="Spread is > 5 punten van Max Pain"),
            "dist_max_pain": st.column_config.NumberColumn("MP Afstand", format="$%.2f"),
            "koopadvies": st.column_config.TextColumn("Koopadvies", help="✅ als winstgevend bij p% beweging"),
            "TTP (D)": st.column_config.NumberColumn("TTP (Dagen)", format="%.1f", help="Days to Profit ($5 doel)"),
            "TEI Score": st.column_config.NumberColumn("TEI Score", format="%.2f", help="Target Efficiency Index (BS Model)"),
            "Efficient": st.column_config.TextColumn("Efficiënt", help="Blauw = TEI Score > 1.2 en TTP < DTE/2"),
        }

        # Ensure columns exist before displaying
        final_cols = [c for c in display_cols if c in results.columns]

        # Helper for Red DTE when relaxed_earnings is True
        def style_results(row):
            styles = [''] * len(row)
            if 'relaxed_earnings' in row.index and row['relaxed_earnings'] == True:
                # Find index of 'dte' in row
                if 'dte' in row.index:
                    try:
                        idx = row.index.get_loc('dte')
                        styles[idx] = 'color: red; font-weight: bold'
                    except:
                        pass
            return styles

        # Filter config to only existing columns
        final_cfg = {k: v for k, v in col_cfg.items() if k in final_cols}

        # Apply style and display
        styled_df = results[final_cols].style.apply(style_results, axis=1)

        # Consistent label generation
        results['label'] = results.apply(lambda x: f"#{x.name} {x['symbol']} {x['expiry']} {x['strategy']} {x['strike_buy']}/{x['strike_sell']} (max ${x.get('max_profit', 0):.0f})", axis=1)
        labels = results['label'].unique()

        # Shared state initialization
        if 'selected_trade_label' not in st.session_state:
            st.session_state['selected_trade_label'] = labels[0] if len(labels) > 0 else None

        st.dataframe(
            styled_df, 
            column_config=final_cfg,
            width='content',
            hide_index=False,
            height=600
        )

        # --- NEW: Direct Selection for Orders ---
        st.divider()
        st.subheader("🎯 Contract Selecteren voor Order")


        # Callback to sync results to shared state and other widget
        def on_results_change():
            sel = st.session_state.res_sel
            st.session_state['selected_trade_label'] = sel
            # Force other widget key to update if it exists
            st.session_state['ord_sel'] = sel

        selected_label_results = st.selectbox(
            "Kies een optie contract uit bovenstaande lijst om direct klaar te zetten in het Order-tabblad:",
            labels,
            index=list(labels).index(st.session_state['selected_trade_label']) if st.session_state['selected_trade_label'] in labels else 0,
            key="res_sel",
            on_change=on_results_change
        )

        if selected_label_results:
            st.info(f"✅ **{selected_label_results}** geselecteerd. Ga naar het **'Orders'** tabblad om de order te plaatsen.")

        st.divider()
        # Exporteer in Europees/Nederlands Excel formaat (puntkomma en komma als decimaal) met UTF-8 BOM
        csv = results.to_csv(index=False, sep=';', decimal=',').encode('utf-8-sig')
        file_name = f"{datetime.date.today().strftime('%Y%m%d')} RESULTATEN OPTIE SELECTIE SCAN.csv"
        st.download_button("Download CSV Resultaten (Voor Excel)", csv, file_name, "text/csv")
    else:
        st.info("Start een scan om resultaten te zien.")

# --- TAB 3: ORDERS ---
with tab3:
    st.header("🛒 Order Uitvoering")

    # Open Orders Section
    st.subheader("📋 Openstaande Orders")
    if st.button("Ververs Orders"):
        import random
        # Use unique range for order fetching to avoid conflicts
        client_id_orders = random.randint(30000, 39999)
        order_ib = IBClient()
        # Connect
        success, msg = order_ib.connect(tws_host, tws_port, client_id_orders)
        if success:
            try:
                orders_df = order_ib.get_open_orders()
                if not orders_df.empty:
                    st.dataframe(orders_df, width='stretch')
                else:
                    st.info("Geen openstaande orders gevonden.")
            except Exception as e:
                st.error(f"Fout bij ophalen orders: {e}")
            finally:
                order_ib.disconnect()
        else:
            st.error(f"Kan geen verbinding maken: {msg}")

    st.divider()
    st.subheader("Nieuwe Order Plaatsen")

    if 'results' in st.session_state and not st.session_state['results'].empty:
        df_orders = st.session_state['results']

        # Consistent label generation mapping to Tab 2
        df_orders['label'] = df_orders.apply(lambda x: f"#{x.name} {x['symbol']} {x['expiry']} {x['strategy']} {x['strike_buy']}/{x['strike_sell']} (max ${x.get('max_profit', 0):.0f})", axis=1)
        valid_labels = df_orders['label'].unique()

        # Shared state initialization
        if 'selected_trade_label' not in st.session_state:
            st.session_state['selected_trade_label'] = valid_labels[0] if len(valid_labels) > 0 else None

        # Callback to sync orders to shared state and other widget
        def on_orders_change():
            sel = st.session_state.ord_sel
            st.session_state['selected_trade_label'] = sel
            # Force other widget key to update if it exists
            st.session_state['res_sel'] = sel

        selected_label = st.selectbox(
            "Selecteer Spread om te handelen", 
            valid_labels,
            index=list(valid_labels).index(st.session_state['selected_trade_label']) if st.session_state['selected_trade_label'] in valid_labels else 0,
            key="ord_sel",
            on_change=on_orders_change
        )

        if selected_label:
            selected_row = df_orders[df_orders['label'] == selected_label].iloc[0]

            st.divider()
            col_ord1, col_ord2 = st.columns(2)

            with col_ord1:
                st.markdown(f"### Contract Details: {selected_row['symbol']}")
                st.write(f"**Strategie:** {selected_row['strategy']}")
                st.write(f"**Expiratie:** {selected_row['expiry']}")
                st.write(f"**Buy Strike:** {selected_row['strike_buy']}")
                st.write(f"**Sell Strike:** {selected_row['strike_sell']}")
                st.write(f"**Max. Winst:** ${selected_row['max_profit']:.2f}")
                st.write(f"**TTP (Dagen tot $5 koerswinst):** {selected_row.get('TTP (D)', 'N/A')}")
                st.write(f"**Risk Efficiency (TEI Score):** {selected_row.get('TEI Score', 'N/A')}")
                st.write(f"**Middenprijs:** ${selected_row.get('spread_mid_abs', 0):.2f}")
                st.write(f"**Laatprijs (Ask):** ${selected_row.get('spread_ask_abs', 0):.2f}")
                st.write(f"**Prijs Buy-leg:** ${selected_row.get('price_buy', 0):.2f}")
                st.write(f"**Prijs Sell-leg:** ${selected_row.get('price_sell', 0):.2f}")

            with col_ord2:
                st.markdown("### Handelen")
                order_qty = st.number_input("Aantal Contracten", min_value=1, value=1)

                # Display Limit Price natively signed (negative for credit)
                strat = selected_row['strategy']
                is_credit = strat not in ['LongCall', 'LongPut', 'BullCall', 'BearPut', 'Strangle']
                
                # Default limit price logic: use Laatprijs (Ask price / Worst entry)
                raw_ask = selected_row.get('spread_ask_abs', 0)
                # Standard default of $0.10 if no price data, else use the realistic target ask price
                default_price = float(raw_ask) if raw_ask > 0 else 0.10
                default_price_signed = -default_price if is_credit else default_price

                limit_price_signed = st.number_input("Limiet Prijs ($)", value=default_price_signed, step=0.01, format="%.2f", help="Standaard ingevuld met Laatprijs. Let op: negatief is Credit.")

                order_type_ui = st.selectbox("Order Type (Executie)", 
                    ["LMT (Standaard Limiet)", "Adaptive - Normal", "Adaptive - Urgent", "Adaptive - Patient"],
                    index=1,
                    help="Kies Adaptive Algo om TWS de beste prijs binnen de spread te laten onderhandelen zonder de max limiet te overschrijden."
                )

                # Dynamic Profit Projection
                n_price = selected_row.get('net_price', 0.0)
                base_winst = selected_row.get('winst_laat', 0.0)
                
                # Difference in price * 100 * contract qty
                # Both n_price and limit_price_signed are negative for credit spreads.
                # If n_price is -9.70 and user sets limit to -10.00 (more credit), -9.70 - (-10.00) = +0.30.
                winst_verschuiving = (n_price - limit_price_signed) * 100
                verwachte_winst_1pct = (base_winst + winst_verschuiving) * order_qty
                
                st.info(f"💡 **Verwachte winst (bij 1% move):** ${verwachte_winst_1pct:.0f} (Totaal voor {order_qty}x)")

                if st.button("🔄 Prijzen Verversen (Live TWS)", help="Haal de meest recente Bied/Laat prijzen voor dit contract op."):
                    refresh_client = IBClient()
                    success, msg = refresh_client.connect(tws_host, tws_port, random.randint(15000, 19999))
                    if success:
                        try:
                            st.toast("⏳ Ophalen live data...")
                            
                            # Construct strikes to check
                            strikes_to_check = []
                            for k in ['strike_buy', 'strike_sell', 'strike_p_buy', 'strike_p_sell', 'strike_c_sell', 'strike_c_buy']:
                                v = selected_row.get(k, 0)
                                if pd.notna(v) and float(v) > 0:
                                    strikes_to_check.append(float(v))
                            
                            if strikes_to_check:
                                live_data = refresh_client.get_chain_greeks_and_oi(selected_row['symbol'], selected_row['expiry'], strikes_to_check)
                                
                                if not live_data.empty:
                                    import numpy as np
                                    strat_type = selected_row['strategy']
                                    
                                    def get_leg_prices(strike, right):
                                        leg_row = live_data[(live_data['strike'] == float(strike)) & (live_data['right'] == right)]
                                        if not leg_row.empty:
                                            b = leg_row.iloc[0].get('bid', 0.0)
                                            a = leg_row.iloc[0].get('ask', 0.0)
                                            if np.isnan(b) or b < 0: b = 0.0
                                            if np.isnan(a) or a < 0: a = 0.0
                                            mid = b + (a - b) / 2 if (b > 0 and a > 0) else max(b, a)
                                            return b, a, mid
                                        return 0.0, 0.0, 0.0

                                    new_pb = 0.0
                                    new_ps = 0.0
                                    new_b = 0.0
                                    new_a = 0.0
                                    
                                    if strat_type in ['LongCall']:
                                        _, _, new_pb = get_leg_prices(selected_row.get('strike_buy', 0), 'C')
                                    elif strat_type in ['LongPut']:
                                        _, _, new_pb = get_leg_prices(selected_row.get('strike_buy', 0), 'P')
                                    elif strat_type in ['BullCall', 'BearCall']:
                                        b1, a1, new_pb = get_leg_prices(selected_row.get('strike_buy', 0), 'C')
                                        b2, a2, new_ps = get_leg_prices(selected_row.get('strike_sell', 0), 'C')
                                        new_b = b1 - a2  # approximate
                                        new_a = a1 - b2
                                    elif strat_type in ['BullPut', 'BearPut']:
                                        b1, a1, new_pb = get_leg_prices(selected_row.get('strike_buy', 0), 'P')
                                        b2, a2, new_ps = get_leg_prices(selected_row.get('strike_sell', 0), 'P')
                                        new_b = b1 - a2
                                        new_a = a1 - b2
                                    elif strat_type == 'Strangle':
                                        _, _, pb_p = get_leg_prices(selected_row.get('strike_p_buy', 0), 'P')
                                        _, _, pb_c = get_leg_prices(selected_row.get('strike_c_buy', 0), 'C')
                                        new_pb = pb_p + pb_c
                                    elif strat_type == 'IronCondor':
                                        _, _, pb_p = get_leg_prices(selected_row.get('strike_p_buy', 0), 'P')
                                        _, _, ps_p = get_leg_prices(selected_row.get('strike_p_sell', 0), 'P')
                                        _, _, ps_c = get_leg_prices(selected_row.get('strike_c_sell', 0), 'C')
                                        _, _, pb_c = get_leg_prices(selected_row.get('strike_c_buy', 0), 'C')
                                        new_pb = pb_p + pb_c
                                        new_ps = ps_p + ps_c

                                    net_mid = abs(new_pb - new_ps)
                                    strat_is_credit = strat_type not in ['LongCall', 'LongPut', 'BullCall', 'BearPut', 'Strangle']
                                    new_net_signed = -net_mid if strat_is_credit else net_mid
                                    
                                    idx = selected_row.name
                                    st.session_state['results'].at[idx, 'price_buy'] = new_pb
                                    st.session_state['results'].at[idx, 'price_sell'] = new_ps
                                    st.session_state['results'].at[idx, 'spread_mid_abs'] = net_mid
                                    st.session_state['results'].at[idx, 'net_price'] = new_net_signed
                                    if new_a != 0 or new_b != 0:
                                        st.session_state['results'].at[idx, 'spread_ask_abs'] = abs(new_a)
                                    
                                    st.toast("✅ Prijzen succesvol actueel gemaakt!")
                                    import time
                                    time.sleep(0.5)
                                    st.rerun()
                        finally:
                            refresh_client.disconnect()
                    else:
                        st.error("Verbinding voor verversen mislukt.")

                if st.button("PLAATS ORDER", type="primary"):
                    client_id_order = random.randint(20000, 29999)
                    order_ib = IBClient()
                    success, msg = order_ib.connect(tws_host, tws_port, client_id_order)

                    if not success:
                        st.error(f"Kan geen verbinding maken voor order: {msg}")
                    else:
                        try:
                            st.write("Verbinding gemaakt. Strategie opbouwen...")

                            # Extract all possible strikes for any strategy
                            strikes_dict = {
                                'strike_buy': selected_row.get('strike_buy', 0),
                                'strike_sell': selected_row.get('strike_sell', 0),
                                'strike_p_buy': selected_row.get('strike_p_buy', 0),
                                'strike_p_sell': selected_row.get('strike_p_sell', 0),
                                'strike_c_sell': selected_row.get('strike_c_sell', 0),
                                'strike_c_buy': selected_row.get('strike_c_buy', 0)
                            }

                            # Determine Overall Action
                            # Since ib_client.py explicitly defines the legs representing the final position we want,
                            # we must always BUY the combination. Credit spreads will use a negative limit price.
                            strat = selected_row['strategy']
                            action = 'BUY'

                            st.write(f"Plaatsen order ({strat}) voor {order_qty} stuks. Actie: {action} Combo (Prijs: {limit_price_signed})...")

                            trade = order_ib.place_strategy_order(
                                symbol=selected_row['symbol'],
                                expiry=selected_row['expiry'],
                                right=selected_row['right'],
                                strategy=strat,
                                strikes_dict=strikes_dict,
                                action=action,
                                quantity=order_qty,
                                price=limit_price_signed,
                                order_type=order_type_ui
                            )

                            if trade:
                                st.success(f"✅ **Order ingediend bij TWS!**")
                                st.markdown(f"**Status:** `{trade.orderStatus.status}`")
                                st.markdown(f"**Order ID:** `{trade.order.orderId}`")

                                st.warning("⚠️ **Belangrijk:** Controleer in TWS het tabblad **'Orders'**, niet 'Transacties'. Transacties verschijnen pas na uitvoering (Fill).")

                                with st.expander("🔍 Technische Details (voor verificatie in TWS)"):
                                    st.write(f"**Symbool:** {selected_row['symbol']}")
                                    st.write(f"**Strategie:** {strat}")
                                    st.write(f"**Netto Actie:** {action}")
                                    st.write(f"**Limit Prijs:** ${limit_price_signed}")
                                    st.write(f"**Port:** {tws_port} ({'Paper' if tws_port==7497 else 'Live/Custom'})")

                                # Show logs for diagnostics
                                if trade.log:
                                    with st.expander("📝 TWS Communicatie Logboek"):
                                        for entry in trade.log:
                                            # Enhance with error codes if available
                                            st.write(f"- {entry.time.strftime('%H:%M:%S')}: {entry.message}")
                                            if hasattr(entry, 'errorCode') and entry.errorCode:
                                                st.write(f"  (Code: {entry.errorCode})")

                                if trade.isDone() and trade.orderStatus.status in ('Cancelled', 'Inactive'):
                                    st.error(f"❌ Order Geweigerd/Geannuleerd door TWS. Status: {trade.orderStatus.status}")
                                    if trade.orderStatus.status == 'Cancelled':
                                        st.warning("⚠️ **Opmerking:** De order is geannuleerd. Dit gebeurt vaak door TWS 'Order Precautions' (zoals prijslimieten te ver van de koers).")
                                    st.info("Tip: Controleer in TWS 'Global Configuration -> API -> Precautions' of 'Bypass Order Precautions' aanstaat.")
                            else:
                                st.error("❌ Order plaatsen mislukt (geen antwoord van TWS).")

                        except Exception as e:
                            st.error(f"Fout bij order uitvoer: {e}")
                            st.exception(e) # More detail in app
                        finally:
                            order_ib.disconnect()
    else:
        st.info("Geen resultaten beschikbaar om te handelen. Start eerst een scan.")



# --- TAB 4: S&P 500 SPREADS ---
with tab4:
    st.subheader("Opties Spreads Exporter (TWS)")
    st.write("Exporteer Bied/Laat/Spread voor ATM & 15% ITM Strikes via de IBKR/TWS data feed.")
    
    # Custom symbols vs S&P 500 list
    scan_method = st.radio("Selecteer Input Methode", ["Upload Excel/CSV", "S&P 500 (Wikipedia)", "Custom Lijst (Handmatig)"])
    
    custom_symbols_input = ""
    uploaded_file = None
    
    if scan_method == "Upload Excel/CSV":
        uploaded_file = st.file_uploader("Upload Excel of CSV met een kolom 'Symbol'", type=['xlsx', 'csv'])
    elif scan_method == "Custom Lijst (Handmatig)":
        custom_symbols_input = st.text_area("Voer symbolen in (komma gescheiden):", "AAPL, MSFT, TSLA, NVDA")
        
    if st.button("Genereer Spreads Excel", type="primary"):
        if not st.session_state.tws_configured:
            st.error("Vereiste TWS verbinding niet gevonden. Test deze eerst in het menu links onder 'TWS Instellingen'.")
        else:
            import io
            import datetime
            import urllib.request
            import random
            from ib_insync import Stock
            
            status_text = st.empty()
            progress_bar = st.progress(0)
            
            symbols = []
            if scan_method == "S&P 500 (Wikipedia)":
                try:
                    status_text.text("S&P 500 symbolen ophalen via Wikipedia...")
                    req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})
                    html = urllib.request.urlopen(req).read()
                    df_sp500 = pd.read_html(html)[0]
                    symbols = df_sp500['Symbol'].tolist()
                    symbols = [s.replace('.', ' ') for s in symbols] # IB assigns BRK B instead of BRK.B
                except Exception as e:
                    st.error(f"Fout bij ophalen S&P 500: {e}")
            elif scan_method == "Custom Lijst (Handmatig)":
                symbols = [s.strip() for s in custom_symbols_input.split(',') if s.strip()]
            elif scan_method == "Upload Excel/CSV":
                if uploaded_file is not None:
                    try:
                        df_up = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
                        if 'Symbol' in df_up.columns:
                            symbols = df_up['Symbol'].dropna().astype(str).tolist()
                        else:
                            st.error("Het bestand moet een kolom genaamd 'Symbol' bevatten.")
                    except Exception as e:
                        st.error(f"Fout bij lezen bestand: {e}")
                else:
                    st.error("Upload eerst een bestand aub.")
            
            if symbols:
                client_id_export = random.randint(30000, 39999)
                export_ib = IBClient()
                success, msg = export_ib.connect(tws_host, tws_port, client_id_export)
                
                if not success:
                    st.error(f"Kan geen TWS verbinding maken: {msg}")
                else:
                    try:
                        dtype = 1 if use_live_data else 3
                        export_ib.set_data_type(dtype)
                        
                        status_text.text(f"Start ophalen {len(symbols)} symbolen via TWS...")
                        results_data = []
                        
                        for i, sym in enumerate(symbols):
                            progress_bar.progress(i / len(symbols))
                            status_text.text(f"Analyseren: {sym} ({i+1}/{len(symbols)})")
                            
                            try:
                                contract = Stock(sym, 'SMART', 'USD')
                                price_data = export_ib.get_market_data_snapshot(contract, use_hist_fallback=False)
                                price = price_data.get('price', 0.0)
                                
                                if price <= 0:
                                    continue
                                
                                chains = export_ib.get_option_chains_params(sym, sec_type='STK')
                                if not chains:
                                    continue
                                    
                                valid_strikes = []
                                for chain in chains:
                                    valid_strikes.extend(chain.strikes)
                                valid_strikes = sorted(list(set(valid_strikes)))
                                
                                if not valid_strikes:
                                    continue
                                
                                def find_closest_strikes(val, count=5):
                                    return sorted(valid_strikes, key=lambda x: abs(x - val))[:count]
                                
                                atm_strike_target = price
                                itm_call_target = price * 0.85
                                itm_put_target = price * 1.15
                                
                                # Find Friday expirations roughly 7-30 days out
                                smart_chains = [c for c in chains if c.exchange == 'SMART']
                                best_chain = max(smart_chains, key=lambda x: len(x.expirations)) if smart_chains else chains[0]
                                exp_targets = sorted(best_chain.expirations)
                                today = datetime.date.today()
                                valid_exps = []
                                for exp in exp_targets:
                                    exp_date = datetime.datetime.strptime(exp, "%Y%m%d").date()
                                    # Alleen expiraties die op een vrijdag vallen en > 7 DTE
                                    if (exp_date - today).days >= 7 and exp_date.weekday() == 4:
                                        valid_exps.append(exp)
                                
                                if not valid_exps:
                                    continue
                                    
                                target_strikes_set = set()
                                target_strikes_set.update(find_closest_strikes(atm_strike_target))
                                target_strikes_set.update(find_closest_strikes(itm_call_target))
                                target_strikes_set.update(find_closest_strikes(itm_put_target))
                                target_strikes = sorted(list(target_strikes_set))
                                
                                chosen_exp = None
                                chain_data = pd.DataFrame()
                                # Probeer maximaal de eerste 4 expiraties tot we er één vinden met actieve Bied/Laat prijzen
                                for attempt_exp in valid_exps[:4]:
                                    temp_data = export_ib.get_chain_greeks_and_oi(sym, attempt_exp, target_strikes)
                                    if temp_data.empty:
                                        continue
                                    
                                    # Controleer of deze date WEL actieve quotes heeft in TWS
                                    if temp_data['bid'].sum() > 0 or temp_data['ask'].sum() > 0:
                                        chosen_exp = attempt_exp
                                        chain_data = temp_data
                                        break
                                
                                if not chosen_exp or chain_data.empty:
                                    continue
                                
                                def find_spread(df, target_strike, right):
                                    if df.empty:
                                        return target_strike, 0.0, 0.0, 0.0
                                    
                                    # Zoek alleen in strikes die daadwerkelijk in de greeks df zitten (dus geldige quotes hebben)
                                    available_matches = df[df['right'] == right]
                                    if available_matches.empty:
                                        return target_strike, 0.0, 0.0, 0.0
                                        
                                    valid_strikes = sorted(available_matches['strike'].unique(), key=lambda x: abs(x - target_strike))
                                    
                                    # Loop door strikes in volgorde van dichtstbijzijnd, tot we een geldige Bied + Laat vinden
                                    for try_strike in valid_strikes:
                                        row = available_matches[available_matches['strike'] == try_strike].iloc[0]
                                        bid = row.get('bid', 0.0)
                                        ask = row.get('ask', 0.0)
                                        
                                        if bid > 0 and ask > 0:
                                            spread = max(0.0, ask - bid)
                                            return try_strike, bid, ask, spread
                                            
                                    return target_strike, 0.0, 0.0, 0.0
                                    
                                # ATM
                                atm_c_strk, atm_c_b, atm_c_a, _ = find_spread(chain_data, atm_strike_target, 'C')
                                atm_p_strk, atm_p_b, atm_p_a, _ = find_spread(chain_data, atm_strike_target, 'P')
                                
                                # ITM Call/Put
                                itm_c_strk, itm_c_b, itm_c_a, _ = find_spread(chain_data, itm_call_target, 'C')
                                itm_p_strk, itm_p_b, itm_p_a, _ = find_spread(chain_data, itm_put_target, 'P')
                                
                                # Excel row index (1-based, +1 for header, dus len + 2)
                                r = len(results_data) + 2
                                
                                results_data.append({
                                    'Symbol': sym, 'Price': price, 'Expiration': chosen_exp,
                                    'ATM_Call_Strike': atm_c_strk, 'ATM_Call_Bid': atm_c_b, 'ATM_Call_Ask': atm_c_a, 'ATM_Call_Spread': f"=MAX(0, F{r}-E{r})",
                                    'ATM_Put_Strike': atm_p_strk, 'ATM_Put_Bid': atm_p_b, 'ATM_Put_Ask': atm_p_a, 'ATM_Put_Spread': f"=MAX(0, J{r}-I{r})",
                                    'ITM_Call_Strike': itm_c_strk, 'ITM_Call_Bid': itm_c_b, 'ITM_Call_Ask': itm_c_a, 'ITM_Call_Spread': f"=MAX(0, N{r}-M{r})",
                                    'ITM_Put_Strike': itm_p_strk, 'ITM_Put_Bid': itm_p_b, 'ITM_Put_Ask': itm_p_a, 'ITM_Put_Spread': f"=MAX(0, R{r}-Q{r})"
                                })
                            except Exception as e:
                                # Streamlit needs to be allowed to halt the script if the user stops the app
                                if type(e).__name__ in ['StopException', 'RerunException']:
                                    raise
                                pass
                            
                            # Throttle significantly to keep TWS API from dropping connection/freezing!
                            export_ib.ib.sleep(0.5)
                        
                        progress_bar.progress(1.0)
                        
                        if results_data:
                            status_text.text("Genereren van Excel bestand...")
                            final_df = pd.DataFrame(results_data)
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                final_df.to_excel(writer, index=False, sheet_name='Ogenblikkelijke Spreads')
                            st.success(f"Klaar! Gegevens voor {len(final_df)} fondsen uit TWS opgehaald.")
                            st.download_button(
                                label="Download Excel", data=buffer.getvalue(),
                                file_name=f"TWS_Spreads_{datetime.date.today().strftime('%Y%m%d')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary"
                            )
                        else:
                            st.error("Kon voor geen van deze fondsen optiedata vinden op TWS.")
                    finally:
                        export_ib.disconnect()

# --- TAB 5: DIVIDEND COVERED CALLS ---
with tab5:
    st.subheader("💰 Dividend Covered Call Scanner")
    st.markdown("Scan lijsten op aandelen die aankomende week dividend uitkeren, op zoek naar Covered Call kansen. Ideaal voor het opvangen van dividend én premie waarbij de strike buiten bereik (OTM) wordt gekozen.")
    
    col_div1, col_div2 = st.columns([1, 2])
    with col_div1:
        div_list_choice = st.selectbox("Kies of plak symbolen", ["S&P 100", "Top 10 Tech", "AEX", "Eigen (upload/barchart)"], key="div_list_choice")
        if div_list_choice == "S&P 100":
            div_symbols = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK.B", "UNH", "JNJ", "XOM", "JPM", "V", "PG", "MA", "HD", "CVX", "ABBV", "LLY", "MRK"]
        elif div_list_choice == "Top 10 Tech":
            div_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "NFLX", "AMD", "INTC"]
        elif div_list_choice == "AEX":
            div_symbols = ["ADYEN", "ASML", "UNA", "RDSA", "INGA", "HEIA", "KPN", "DSM", "RAND", "MT", "AKZA", "PHIA"]
        else:
            div_symbols = symbols_to_scan # Fallback from main sidebar

        target_gain_pct = st.slider("Beoogde Covered Call Winstmarge (%)", min_value=1, max_value=20, value=5, step=1, help="De aanbevolen Call strike wordt op deze afstand boven de huidige koers gelegd.")
        days_ahead = st.slider("Zoek Ex-Dividend in komende X dagen", min_value=2, max_value=60, value=14)
    
    if st.button("Scan Aankomende Dividenden 🚀", type="primary"):
        if not div_symbols:
            st.error("Selecteer een lijst met symbolen om te scannen.")
        else:
            st.info(f"Start zoeken naar dividenden in {len(div_symbols)} aandelen...")
            div_ib = IBClient()
            success, msg = div_ib.connect(tws_host, tws_port, random.randint(100, 999))
            
            dividend_results = []
            if success:
                div_ib.set_data_type(1 if use_live_data else 3)
                progress_bar_div = st.progress(0)
                status_text_div = st.empty()
                
                try:
                    import yfinance as yf
                    import datetime
                    import pandas as pd
                    now = pd.Timestamp.now().date()
                    for i, sym in enumerate(div_symbols):
                        progress_bar_div.progress((i + 1) / len(div_symbols))
                        status_text_div.text(f"Gegevens ophalen voor {sym}...")
                        
                        div_info = div_ib.get_dividend_info(sym)
                        ex_div = div_info.get('ex_div_date')
                        
                        if ex_div:
                            days_to_ex = (ex_div - now).days
                            if 0 <= days_to_ex <= days_ahead:
                                contract = Stock(sym, 'SMART', 'USD')
                                mkt = div_ib.get_market_data_snapshot(contract, use_hist_fallback=True)
                                price = mkt.get('price', 0.0)
                                if price <= 0:
                                    try:
                                        t = yf.Ticker(sym)
                                        df_hist = t.history(period="1d")
                                        if not df_hist.empty:
                                            price = df_hist['Close'].iloc[-1]
                                    except:
                                        price = 0.0
                                
                                if price > 0:
                                    target_strike = price * (1 + target_gain_pct / 100.0)
                                    
                                    # Voeg greeks toe om de echte call premie te vinden
                                    sec_type_str = 'STK'
                                    chains = div_ib.get_option_chains_params(sym, sec_type=sec_type_str)
                                    real_exp = None
                                    final_strike = target_strike
                                    call_ask = 0.0
                                    call_bid = 0.0
                                    
                                    if chains:
                                        # Pak de chain (voorkeur SMART)
                                        chain = chains[0]
                                        for c in chains:
                                            if getattr(c, 'exchange', '') == 'SMART':
                                                chain = c
                                                break
                                                
                                        valid_exps = sorted(chain.expirations)
                                        # Zoek een expiratie datum NA de verwachte dividend payout, standaard 14 dagen
                                        target_exp_date = now + datetime.timedelta(days=int(max(14, days_to_ex + 2)))
                                        target_exp_str = target_exp_date.strftime('%Y%m%d')
                                        
                                        for exp in valid_exps:
                                            if exp >= target_exp_str:
                                                real_exp = exp
                                                break
                                        if not real_exp and valid_exps:
                                            real_exp = valid_exps[-1]
                                            
                                        if real_exp:
                                            valid_strikes = sorted([s for s in chain.strikes if s >= target_strike])
                                            if valid_strikes:
                                                final_strike = valid_strikes[0]
                                            elif chain.strikes:
                                                final_strike = max(chain.strikes)
                                                
                                            # Haal prijs op van de target Call
                                            greeks = div_ib.get_chain_greeks_and_oi(sym, real_exp, [final_strike])
                                            if not greeks.empty:
                                                # C voor Call
                                                calls = greeks[greeks['right'] == 'C']
                                                if not calls.empty:
                                                    call_ask = float(calls['ask'].iloc[0])
                                                    call_bid = float(calls['bid'].iloc[0])
                                                    
                                    div_rate = div_info.get('dividend_rate', 0.0)
                                    used_premie = call_bid if call_bid > 0 else call_ask
                                    
                                    # Berekeningen
                                    # Winst bij uitoefening (als koers > strike)
                                    winst_executie = ((final_strike - price) + used_premie + div_rate) * 100
                                    # Winst bij koersstijging van 5% ZONDER uitoefening (optie loopt waardeloos of gedeeltelijk af)
                                    # (Koers stijgt puur 5% plus premie ontvangen) - we gaan er vanuit dat je de winst behoudt
                                    winst_5pct_stijging = ((price * 0.05) + used_premie + div_rate) * 100
                                    # BEP (De kostprijs - ontvangen premie - dividend_bijdrage)
                                    bep = price - used_premie - div_rate
                                    
                                    dividend_results.append({
                                        'Symbol': sym,
                                        'Koers': round(price, 2),
                                        'Ex-Div Datum': ex_div.strftime('%d-%m-%Y'),
                                        'Dagen tot Ex-Div': days_to_ex,
                                        'Div. Yield (%)': round(div_info.get('dividend_yield', 0.0) * 100, 2),
                                        'Div. Bedrag': round(div_rate, 2),
                                        'Call Exp': real_exp if real_exp else 'N/A',
                                        'Call Strike': round(final_strike, 2),
                                        'Call Bied (Premie)': round(call_bid, 2) if call_bid > 0 else 'N/A',
                                        'Call Laatprijs': round(call_ask, 2) if call_ask > 0 else 'N/A',
                                        'BEP': round(bep, 2),
                                        'Winst bij Uitoefening ($)': round(winst_executie, 2),
                                        'Winst +5% Stijging ($)': round(winst_5pct_stijging, 2)
                                    })
                                    
                except Exception as e:
                    st.error(f"Fout tijdens ophalen dividenden: {e}")
                finally:
                    div_ib.disconnect()
                    
                status_text_div.text("Gereed!")
                if dividend_results:
                    df_div = pd.DataFrame(dividend_results)
                    df_div = df_div.sort_values(by='Dagen tot Ex-Div')
                    st.session_state['dividend_results'] = df_div
                    st.success(f"{len(dividend_results)} aandelen gevonden die dividend uitkeren!")
                    st.dataframe(df_div, use_container_width=True, hide_index=True)
                else:
                    st.warning("Geen dividenden gevonden voor de geselecteerde aandelen in deze periode.")
            else:
                st.error("Kon niet verbinden met TWS voor prijsinformatie.")

    if 'dividend_results' in st.session_state and not st.session_state['dividend_results'].empty:
        import io
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            st.session_state['dividend_results'].to_excel(writer, index=False, sheet_name='Dividend_CC')
        
        st.download_button(
            label="Exporteer Dividend Lijst (Excel)",
            data=buffer.getvalue(),
            file_name=f"Dividend_Covered_Calls_{datetime.date.today().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
