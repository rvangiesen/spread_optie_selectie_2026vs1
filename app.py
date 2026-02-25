import streamlit as st
import pandas as pd
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
            cols = st.columns(min(len(st.session_state.symbol_prices), 6)) # Cap at 6 per row or let it wrap?
            for i, (sym, price) in enumerate(st.session_state.symbol_prices.items()):
                # Use current column in carousel-like fashion or just fill 
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
scan_mode = st.sidebar.selectbox("Scan Modus", ["Enkel Symbool", "Batch Scan (Lijst)", "Batch Scan (Bestand)", "Live TWS Scanner"])

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
            
            if 'Symbol' in df.columns:
                symbols_to_scan = df['Symbol'].tolist()
                st.sidebar.success(f"{len(symbols_to_scan)} symbolen geladen.")
            else:
                st.sidebar.error("Bestand moet kolom 'Symbol' bevatten.")
        except Exception as e:
            st.sidebar.error(f"Fout bij laden: {e}")
    sec_type = "Aandeel" # Assume stocks for custom lists usually

elif scan_mode == "Live TWS Scanner":
    st.sidebar.info("Haalt live 'Most Active', 'Top Gainers' etc. op van TWS")
    scan_code = st.sidebar.selectbox("Scan Criteria", ["MOST_ACTIVE", "TOP_PERC_GAIN", "HOT_BY_VOLUME", "OPT_VOLUME_MOST_ACTIVE"])
    num_rows = st.sidebar.slider("Aantal resultaten", 10, 50, 20)
    sec_type = "Aandeel"
    # Logic to fetch happens inside "Start Scan" to avoid premature connection
    
# Filters
st.sidebar.subheader("Filters & Criteria")
min_dte = st.sidebar.number_input("Min Dagen tot Expiratie", value=5)
max_dte = st.sidebar.number_input("Max Dagen tot Expiratie", value=32)
width = st.sidebar.number_input("Spread Breedte", value=10)
min_pop = st.sidebar.slider("Min Kans op Winst (PoP %)", 0, 100, 50)
min_profit = st.sidebar.number_input("Min Winst Potentie ($)", value=100)
strike_range_pct = st.sidebar.slider("Strike Selectie Range %", 10, 50, 30, help="Bereik rondom koers (default 30%, max 50%)") / 100.0
max_pain_buffer = st.sidebar.number_input("Max Pain Buffer (Punten)", value=5, help="Minimale afstand tot Max Pain strike")
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
    ["Profit", "PoP", "Max Pain Distance", "Gamma", "Delta", "Theta"],
    default=["Profit", "PoP"]
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
else:
    use_ema_crossover = False

# Main Area
st.title("Spread Selectie Tool - AntiGravity")

# Weekend Warning
import datetime
today = datetime.datetime.now().weekday()
if today >= 5: # 5 = Saturday, 6 = Sunday
    st.warning("⚠️ **Weekend Modus Actief**: TWS levert momenteel beperkte live data. De scanner gebruikt de prijzen van afgelopen vrijdag (sluiting) als fallback voor berekeningen.")

tab1, tab2, tab3 = st.tabs(["🚀 Scanner", "📊 Resultaten", "🛒 Orders"])

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
             if st.button("Start Scan", type="primary"):
                 # EXECUTION logic...
                 
                 # Use a random client ID to avoid "Client ID already in use" from zombie sessions
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
                             if datetime.datetime.now().weekday() >= 5:
                                 log("📅 Weekend gedetecteerd: Gebruik 'Close' prijzen als fallback.")
                             
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
                                 
                                 # Check connection
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

                                     tech_levels = {'supports': [], 'resistances': []}
                                     if not hist_data.empty:
                                         tech_levels = scanner.find_technical_levels(hist_data, ref_price=price)
                                         log(f"   📊 [DEBUG] Tech levels gevonden voor {sym} (Price ${price:.2f}): S:{len(tech_levels['supports'])}, R:{len(tech_levels['resistances'])}")
                                         if tech_levels['supports']:
                                             log(f'   📉 Supports (Tier 1-3): { ", ".join([f"${s:.2f}" for s in tech_levels["supports"]]) }')
                                         else:
                                             log(f'   📉 No Supports found for {sym} at reference price ${price:.2f}')
                                         if tech_levels['resistances']:
                                             log(f'   📈 Resistances (Tier 1-3): { ", ".join([f"${r:.2f}" for r in tech_levels["resistances"]]) }')
                                         else:
                                             log(f'   📈 No Resistances found for {sym} at reference price ${price:.2f}')
                                     else:
                                         log(f'   ⚠️ Geen historische data voor {sym}. EMA & Technical filters overgeslagen.')

                                     hist_price = float(hist_data['close'].iloc[-1]) if not hist_data.empty else 0.0

                                     # Consistency Check: Live vs Historical
                                     if price > 0 and hist_price > 0:
                                         price_diff_pct = abs(price - hist_price) / price
                                         if price_diff_pct > 0.50:
                                             log(f"   ⚠️ Prijs mismatch ({price_diff_pct:.0%}). Hist_price ${hist_price:.2f} is waarschijnlijk verouderd.")
                                             hist_data = pd.DataFrame() # Discard stale history
                                             hist_price = 0.0

                                     # Final Price Decision: Live -> Hist
                                     if price <= 0 and hist_price > 0:
                                         price = hist_price
                                         data_source = 'TWS Historical (Fallback)'

                                     if price > 0:
                                         st.session_state.symbol_prices[sym] = price
                                         update_price_dashboard() # TRIGGER LIVE UPDATE
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
                                         p = {'symbol': sym, 'min_dte': min_dte, 'max_dte': d_max, 
                                             'width': width, 'iv': underlying_iv, 'strike_range_pct': strike_range_pct}
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
                                             log_func=log
                                         )

                                         current_filters = {
                                             'min_pop': min_pop,
                                             'min_profit': min_profit,
                                             'min_delta': min_delta,
                                             'min_gamma': min_gamma,
                                             'max_dte': max_dte,
                                             'min_dte': min_dte
                                         }
                                         if use_max_pain_filter:
                                             current_filters['max_pain_dist'] = max_pain_dist

                                         log(f"   🔍 Filteren spreads for {exp}...")

                                         if not enriched.empty:
                                             s_str = ", ".join([f"${s:.2f}" for s in tech_levels.get('supports', [])])
                                             r_str = ", ".join([f"${r:.2f}" for r in tech_levels.get('resistances', [])])
                                             if not s_str: s_str = "Geen"
                                             if not r_str: r_str = "Geen"
                                             
                                             enriched['supports'] = s_str
                                             enriched['resistances'] = r_str
                                             
                                             gf = scanner.calculate_gamma_flip(chain_data)
                                             enriched['gamma_flip'] = gf

                                         all_unfiltered_global = pd.concat([all_unfiltered_global, enriched], ignore_index=True)
                                         processed_spreads = pd.concat([processed_spreads, enriched])

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
                                     log(f"   ❌ ERROR bij verwerken {sym}: {str(e)}")
                                     import traceback
                                     log(f'   📋 Details: {str(traceback.format_exc())[:200]}')
                                     continue
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
                                 st.success(f"{len(ranked)} spreads gevonden!")
                             else:
                                 log(f"⚠️ Geen spreads gevonden")
                                 st.warning("Geen spreads gevonden. Probeer parameters te verruimen.")
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
             preview_cols = ['symbol', 'strategy', 'expiry', 'strike_buy', 'strike_sell', 'max_profit', 'pop', 'max_pain']
             preview_cols = [c for c in preview_cols if c in df_res.columns]
             
             strategies_found = df_res['strategy'].unique()
             for strat in strategies_found:
                 st.markdown(f"**Top 5: {strat}**")
                 df_strat = df_res[df_res['strategy'] == strat].head(5)
                 st.dataframe(df_strat[preview_cols], width='stretch')

    else:
        st.warning("Configureer en test eerst de TWS verbinding in de Sidebar.")

# --- TAB 2: RESULTATEN ---
with tab2:
    if 'results' in st.session_state and not st.session_state['results'].empty:
        results = st.session_state['results']
        st.subheader(f"Gevonden Resultaten ({len(results)})")
        
        # Display Columns
        display_cols = ['symbol', 'strategy', 'expiry', 'strike_buy', 'strike_sell', 'width', 
                      'strike_p_buy', 'strike_p_sell', 'strike_c_sell', 'strike_c_buy',
                      'net_price', 'max_profit', 'pop', 
                      'price_buy', 'price_sell', 
                      'net_extrinsic',
                      'delta_buy', 'delta_sell', 'delta', 'gamma', 'theta', 'dte', 
                      'supports', 'resistances',
                      'max_pain', 'max_pain_selection', 'max_pain_buffer_ok', 'dist_max_pain',
                      'iv_rank', 'underlying_iv', 'expected_move', 'gamma_flip']
        
        # [DEBUG] Show column presence if requested
        if st.checkbox("Debug Columns", False):
            st.write(f"Columns in results: {results.columns.tolist()}")
        
        # Ensure columns exist before displaying
        final_cols = [c for c in display_cols if c in results.columns]
        
        # Helper for Red DTE when relaxed_earnings is True
        def style_results(row):
            styles = [''] * len(row)
            if 'relaxed_earnings' in row.index and row['relaxed_earnings'] == True:
                # Find index of 'dte' in row
                if 'dte' in row.index:
                    idx = row.index.get_loc('dte')
                    styles[idx] = 'color: red; font-weight: bold'
            return styles

        # Column Configuration for Streamlit (Autosizing & Formatting)
        # Note: Removing most 'width' params to allow Streamlit's internal autosizing.
        col_cfg = {
            "symbol": st.column_config.TextColumn("Symbool"),
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
            "iv_rank": st.column_config.NumberColumn("IV Rank", format="%.1f%%"),
            "underlying_iv": st.column_config.NumberColumn("IV", format="%.1f%%"),
            "expected_move": st.column_config.NumberColumn("Exp. Move (1SD)", format="$%.2f"),
            "gamma_flip": st.column_config.NumberColumn("Gamma Flip", format="$%.2f"),
            "net_price": st.column_config.NumberColumn("Spread Prijs", format="$%.2f"),
            "max_profit": st.column_config.NumberColumn("Max Winst", format="$%.2f"),
            "pop": st.column_config.NumberColumn("PoP %", format="%.1f%%"),
            "price_buy": st.column_config.NumberColumn("Prijs Buy", format="$%.2f"),
            "price_sell": st.column_config.NumberColumn("Prijs Sell", format="$%.2f"),
            "net_extrinsic": st.column_config.NumberColumn("Net Extrin.", format="$%.2f"),
            "delta_buy": st.column_config.NumberColumn("Delta Buy", format="%.3f"),
            "delta_sell": st.column_config.NumberColumn("Delta Sell", format="%.3f"),
            "delta": st.column_config.NumberColumn("Net Delta", format="%.3f"),
            "gamma": st.column_config.NumberColumn("Gamma", format="%.4f"),
            "theta": st.column_config.NumberColumn("Theta", format="%.3f"),
            "dte": st.column_config.NumberColumn("DTE", format="%d", help="Rood = Earnings beperking kon niet worden gehaald"),
            "max_pain": st.column_config.NumberColumn("Max Pain 1", format="$%.2f"),
            "max_pain_selection": st.column_config.NumberColumn("Max Pain 2", format="$%.2f"),
            "max_pain_buffer_ok": st.column_config.CheckboxColumn("MP Buffer OK", help="Spread is > 5 punten van Max Pain"),
            "dist_max_pain": st.column_config.NumberColumn("MP Afstand", format="$%.2f"),
        }

        # Filter config to only existing columns
        final_cfg = {k: v for k, v in col_cfg.items() if k in final_cols}
        
        # Apply style and display
        styled_df = results[final_cols].style.apply(style_results, axis=1)
        
        st.dataframe(
            styled_df, 
            column_config=final_cfg,
            width='stretch', 
            height=600 
        )
        
        # --- NEW: Direct Selection for Orders ---
        st.divider()
        st.subheader("🎯 Spread Selecteren voor Order")
        
        # Consistent label generation
        results['label'] = results.apply(lambda x: f"{x['symbol']} {x['expiry']} {x['strategy']} {x['strike_buy']}/{x['strike_sell']} (Profit: ${x['max_profit']:.0f})", axis=1)
        labels = results['label'].unique()
        
        # Shared state initialization
        if 'selected_trade_label' not in st.session_state:
            st.session_state['selected_trade_label'] = labels[0] if len(labels) > 0 else None
        
        # Callback to sync results to shared state and other widget
        def on_results_change():
            sel = st.session_state.res_sel
            st.session_state['selected_trade_label'] = sel
            # Force other widget key to update if it exists
            st.session_state['ord_sel'] = sel

        selected_label_results = st.selectbox(
            "Kies een spread uit bovenstaande lijst om direct klaar te zetten in het Order-tabblad:",
            labels,
            index=list(labels).index(st.session_state['selected_trade_label']) if st.session_state['selected_trade_label'] in labels else 0,
            key="res_sel",
            on_change=on_results_change
        )
        
        if selected_label_results:
            st.info(f"✅ **{selected_label_results}** geselecteerd. Ga naar het **'Orders'** tabblad om de order te plaatsen.")

        st.divider()
        csv = results.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV Resultaten", csv, "spreads.csv", "text/csv")
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
        
        # Consistent label generation
        df_orders['label'] = df_orders.apply(lambda x: f"{x['symbol']} {x['expiry']} {x['strategy']} {x['strike_buy']}/{x['strike_sell']} (Profit: ${x['max_profit']:.0f})", axis=1)
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
                st.markdown(f"### Spread Details: {selected_row['symbol']}")
                st.write(f"**Strategie:** {selected_row['strategy']}")
                st.write(f"**Expiratie:** {selected_row['expiry']}")
                st.write(f"**Buy Strike:** {selected_row['strike_buy']}")
                st.write(f"**Sell Strike:** {selected_row['strike_sell']}")
                st.write(f"**Max. Winst:** ${selected_row['max_profit']:.2f}")
            
            with col_ord2:
                st.markdown("### Handelen")
                order_qty = st.number_input("Aantal Contracten", min_value=1, value=1)
                
                # Default limit price logic (midpoint estimate)
                pb = selected_row.get('price_buy', 0)
                ps = selected_row.get('price_sell', 0)
                raw_mid = abs(pb - ps)
                # Standard default of $0.10 if no price data, else use the estimate
                default_price = float(raw_mid) if raw_mid > 0 else 0.10
                
                limit_price = st.number_input("Limiet Prijs ($)", value=default_price, step=0.01, format="%.2f")
                
                if st.button("PLAATS ORDER (Limiet)", type="primary"):
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
                            strat = selected_row['strategy']
                            if strat in ['LongCall', 'LongPut', 'BullCall', 'BearPut', 'Strangle']:
                                action = 'BUY' # Net Debit
                            else:
                                action = 'SELL' # Net Credit
                            
                            st.write(f"Plaatsen {action} order ({strat}) voor {order_qty} stuks op {limit_price}...")
                            
                            trade = order_ib.place_strategy_order(
                                symbol=selected_row['symbol'],
                                expiry=selected_row['expiry'],
                                right=selected_row['right'],
                                strategy=strat,
                                strikes_dict=strikes_dict,
                                action=action,
                                quantity=order_qty,
                                price=limit_price
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
                                    st.write(f"**Limit Prijs:** ${limit_price}")
                                    st.write(f"**Port:** {tws_port} ({'Paper' if tws_port==7497 else 'Live/Custom'})")
                                
                                # Show logs for diagnostics
                                if trade.log:
                                    with st.expander("📝 TWS Communicatie Logboek"):
                                        for entry in trade.log:
                                            st.write(f"- {entry.time.strftime('%H:%M:%S')}: {entry.message}")
                                
                                if trade.isDone() and trade.orderStatus.status in ('Cancelled', 'Inactive'):
                                    st.error(f"❌ Order Geweigerd/Geannuleerd door TWS. Status: {trade.orderStatus.status}")
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
