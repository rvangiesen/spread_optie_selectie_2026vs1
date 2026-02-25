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

# Strategy Settings
st.sidebar.title("Strategie Instellingen")
strategy = st.sidebar.selectbox("Strategie", ["BullCall", "BullPut", "BearCall", "BearPut"])

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
min_dte = st.sidebar.number_input("Min Dagen tot Expiratie", value=14)
max_dte = st.sidebar.number_input("Max Dagen tot Expiratie", value=45)
width = st.sidebar.number_input("Spread Breedte", value=5)
min_pop = st.sidebar.slider("Min Kans op Winst (PoP %)", 0, 100, 50)
min_profit = st.sidebar.number_input("Min Winst Potentie ($)", value=20)

# Advanced Filters
with st.sidebar.expander("Greeks & Advanced Filters", expanded=False):
    min_delta = st.sidebar.slider("Min Delta (Short Leg)", 0.0, 1.0, 0.15, step=0.01)
    min_gamma = st.sidebar.number_input("Min Gamma Exposure", value=0.0, step=0.001, format="%.4f")
    max_pain_dist = st.sidebar.number_input("Max Afstand tot Max Pain ($)", value=10.0) 
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

# Main Area
st.title("Spread Selectie Tool - AntiGravity")

tab1, tab2, tab3 = st.tabs(["🚀 Scanner", "📊 Resultaten", "🛒 Orders"])

# --- TAB 1: SCANNER ---
with tab1:
    if st.session_state.tws_configured:
        st.write(f"Huidige selectie: {strategy}")
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
                             
                             # Progress bar
                             progress_bar = st.progress(0)
                             status_text = st.empty()
                             log_area = st.expander("📋 Scan Log (Debug)", expanded=True)
                             log_messages = []
                             
                             def log(msg):
                                 """Helper to log messages to UI"""
                                 log_messages.append(msg)
                                 with log_area:
                                     st.text("\n".join(log_messages[-20:]))  # Show last 20 messages
                             
                             log(f"🚀 Start scan: {len(current_symbols)} symbolen te verwerken")
                             log(f"📊 Strategie: {strategy}")
                             log(f"⚙️ Filters: DTE={min_dte}-{max_dte}, Width={width}, MinPoP={min_pop}%, MinProfit=${min_profit}")
                             
                             # 1. Technical Filter (EMA) Batch
                             if use_ema and ema_spans:
                                 log(f"📈 EMA Filter actief: {ema_spans}")
                                 status_text.text("Bezig met ophalen historische data voor EMA filter...")
                             
                             # 2. Main Loop
                             approved_symbols = []
                             
                             for i, sym in enumerate(current_symbols):
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
                                     # log(f"   📝 Contract aanmaken: {sym} ({sec_type})")
                                     
                                     # Create contract - NO qualification here, handled inside calls
                                     if sec_type == "Aandeel":
                                         contract = Stock(sym, 'SMART', 'USD')
                                     else:
                                         contract = Index(sym, 'SMART', 'USD')
                                     
                                     # log(f"   ✅ Contract aangemaakt: {sym}")
                                     
                                     # A. Technical Check
                                     if use_ema and ema_spans:
                                         # log(f"   📊 Ophalen historische data voor EMA check...")
                                         hist_data = scan_ib.get_historical_data(contract, duration='1 Y', bar_size='1 day')
                                         if hist_data.empty:
                                             log(f"   ⚠️ Geen historische data beschikbaar voor {sym}")
                                             continue
                                         # log(f"   ✅ Historische data opgehaald ({len(hist_data)} bars)")
                                         
                                         # log(f"   💰 Ophalen huidige prijs...")
                                         price = scan_ib.get_market_price(contract)
                                         if not price or price <= 0:
                                             log(f"   ❌ Kan prijs niet ophalen voor {sym}")
                                             continue
                                         # log(f"   💰 Prijs: ${price:.2f}")
                                         
                                         sym_data = {sym: {'price': price, 'history': hist_data}}
                                         passed = scanner.filter_symbols_by_ema(sym_data, ema_spans)
                                         if not passed:
                                             log(f"   ⛔ {sym} gefilterd door EMA check")
                                             continue # Skip this symbol
                                         log(f"   ✅ {sym} doorstaat EMA filter")
                                             
                                     # 3. Marktdata ophalen
                                     log(f"   Ophalen marktdata snapshot...")
                                     market_data = scan_ib.get_market_data_snapshot(contract)
                                     
                                     price = market_data.get('price', 0.0)
                                     underlying_iv = market_data.get('iv', 0.0)
                                     data_source = market_data.get('source', 'Unknown')
                                     
                                     if price <= 0:
                                         log(f"❌ Geen geldige prijs gevonden voor {sym}. Sla over.")
                                         continue
                                         
                                     log(f"   💰 Prijs: ${price:.2f}, IV: {underlying_iv:.2%} ({data_source})")
                                     
                                     # 4. Optieketens ophalen
                                     log(f"   Optieketens opvragen...")
                                     sec_type_str = 'IND' if sec_type == "Index" else 'STK'
                                     chains = scan_ib.get_option_chains_params(sym, sec_type=sec_type_str)
                                     
                                     if not chains:
                                         log(f"   ⚠️ Geen optie chains gevonden voor {sym}")
                                         continue
                                     log(f"   ✅ {len(chains)} chain(s) gevonden")
                                         
                                     # C. Generate Spreads (Lightweight - Logic Only)
                                     log(f"   🎯 Genereren spreads...")
                                     params = {'symbol': sym, 'min_dte': min_dte, 'max_dte': max_dte, 'width': width, 'iv': underlying_iv}
                                     raw_spreads = scanner.generate_spreads(chains, strategy, price, params)
                                     log(f"   📝 {len(raw_spreads)} kandidaten gegenereerd")
                                     
                                     if raw_spreads.empty:
                                         continue

                                     # D. Enrich & Calculate Metrics (Heavy - Data Fetching)
                                     # Group by Expiration to fetch Chain Data efficiently
                                     processed_spreads = pd.DataFrame()
                                     unique_expirations = raw_spreads['expiry'].unique()
                                     
                                     log(f"   📡 Ophalen Greeks & Max Pain ({len(unique_expirations)} expiraties)...")
                                     
                                     for exp in unique_expirations:
                                         # Filter spreads for this expiration
                                         exp_spreads = raw_spreads[raw_spreads['expiry'] == exp].copy()
                                         if exp_spreads.empty: continue

                                         # Identify needed strikes
                                         needed_strikes = set(exp_spreads['strike_buy'].unique()) | set(exp_spreads['strike_sell'].unique())
                                         strikes_list = sorted(list(needed_strikes))
                                         
                                         # Fetch Chain Data (Single Batch Request)
                                         # log(f"      Expiry {exp}: Fetching data for {len(strikes_list)} strikes...")
                                         chain_data = scan_ib.get_chain_greeks_and_oi(sym, exp, strikes_list)
                                         
                                         # Determine Max Pain for this expiry
                                         mp = 0.0
                                         if not chain_data.empty:
                                             mp = scanner.calculate_max_pain(chain_data)
                                             log(f"      Expiratie {exp}: Max Pain = ${mp:.2f}")
                                         
                                         # Calculate Metrics using Cached Data
                                         enriched = scanner.calculate_metrics(exp_spreads, scan_ib, sym, underlying_price=price, chain_data=chain_data)
                                         processed_spreads = pd.concat([processed_spreads, enriched])

                                     # E. Filter with Advanced Logic
                                     current_filters = {
                                         'min_pop': min_pop, 
                                         'min_profit': min_profit,
                                         'min_delta': min_delta,
                                         'max_dte': max_dte,
                                         'min_dte': min_dte
                                     }
                                     
                                     log(f"   🔍 Filteren spreads...")
                                     filtered = scanner.filter_spreads(processed_spreads, current_filters)
                                     
                                     # Extra Filters
                                     if 'dist_max_pain' in filtered.columns and max_pain_dist and max_pain_dist > 0:
                                          pass # Placeholder

                                     log(f"   ✅ {len(filtered)} spreads na filtering")
                                     
                                     # Auto-Tune Logic
                                     retries = 0
                                     if auto_tune and len(filtered) < 5:
                                         while len(filtered) < 5 and retries < 2:
                                              log(f"   🔄 Auto-Tune: Versoepelen filters (poging {retries+1})...")
                                              current_filters['min_profit'] *= 0.5
                                              if 'min_delta' in current_filters:
                                                  current_filters['min_delta'] = max(0.05, current_filters['min_delta'] - 0.05)
                                              
                                              filtered = scanner.filter_spreads(processed_spreads, current_filters)
                                              log(f"   📊 {len(filtered)} spreads na versoepeling")
                                              retries += 1
                                          
                                     if not filtered.empty:
                                         all_results = pd.concat([all_results, filtered], ignore_index=True)
                                         log(f"   ✅ {len(filtered)} spreads toegevoegd aan resultaten")
                                     else:
                                         log(f"   ⚠️ Geen spreads voldoen aan criteria voor {sym}")
                                 
                                 except Exception as e:
                                     log(f"   ❌ ERROR bij verwerken {sym}: {str(e)}")
                                     import traceback
                                     log(f"   📋 Details: {traceback.format_exc()[:200]}")
                                     continue
                             
                             progress_bar.progress(1.0)
                             status_text.text("Scan voltooid.")
                             log(f"\n✅ Scan voltooid!")
                             log(f"📊 Totaal {len(all_results)} spreads verzameld")
                             
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
             st.divider()
             st.subheader("Snel Overzicht (Top 5)")
             st.info("Voor details en filtering, ga naar tabblad **'📊 Resultaten'**")
             
             # Show simple top 5
             df_prev = st.session_state['results'].head(5)
             # Select key columns for preview
             preview_cols = ['symbol', 'strategy', 'expiry', 'strike_buy', 'strike_sell', 'max_profit', 'pop', 'max_pain']
             # Filter to existing columns
             preview_cols = [c for c in preview_cols if c in df_prev.columns]
             st.dataframe(df_prev[preview_cols], use_container_width=True)

    else:
        st.warning("Configureer en test eerst de TWS verbinding in de Sidebar.")

# --- TAB 2: RESULTATEN ---
with tab2:
    if 'results' in st.session_state and not st.session_state['results'].empty:
        st.subheader("Resultaten")
        
        # Reorder columns for better view
        df = st.session_state['results']
        cols = ['symbol', 'strategy', 'expiry', 'strike_buy', 'strike_sell', 'max_profit', 'pop', 'delta', 'gamma', 'theta']
        # Add dynamic cols if present
        if 'max_pain' in df.columns: cols.append('max_pain')
        if 'dist_max_pain' in df.columns: cols.append('dist_max_pain')
        
        # Filter cols to those that exist
        valid_cols = [c for c in cols if c in df.columns]
        remaining = [c for c in df.columns if c not in valid_cols]
        final_cols = valid_cols + remaining
        
        st.dataframe(df[final_cols])
        
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", csv, "spreads.csv", "text/csv")
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
                    st.dataframe(orders_df)
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
        
        # Create a selectbox or multiselect to choose spread
        # Creating a unique label for dropdown
        df_orders['label'] = df_orders.apply(lambda x: f"{x['symbol']} {x['expiry']} {x['strategy']} {x['strike_buy']}/{x['strike_sell']} (Profit: ${x['max_profit']:.0f})", axis=1)
        
        selected_label = st.selectbox("Selecteer Spread om te handelen", df_orders['label'].unique())
        
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
                mid_est = selected_row['max_profit'] / 100.0 # Approximation if spread priced perfectly? No.
                # Only real way to get price is fetch ticker. For now manual input.
                limit_price = st.number_input("Limiet Prijs ($)", value=0.50, step=0.05, format="%.2f")
                
                if st.button("PLAATS ORDER (Limiet)", type="primary"):
                    client_id_order = random.randint(20000, 29999)
                    order_ib = IBClient()
                    success, msg = order_ib.connect(tws_host, tws_port, client_id_order)
                    
                    if not success:
                        st.error(f"Kan geen verbinding maken voor order: {msg}")
                    else:
                        try:
                            st.write("Verbinding gemaakt. Contract opbouwen...")
                            
                            # Reconstruct Contracts
                            contract_buy = Option(selected_row['symbol'], selected_row['expiry'], selected_row['strike_buy'], selected_row['right'], 'SMART', multiplier='100', currency='USD')
                            contract_sell = Option(selected_row['symbol'], selected_row['expiry'], selected_row['strike_sell'], selected_row['right'], 'SMART', multiplier='100', currency='USD')
                            
                            # Determine Action
                            # Bull Call: Debit -> BUY
                            # Bull Put: Credit -> SELL
                            # Bear Call: Credit -> SELL
                            # Bear Put: Debit -> BUY
                            strat = selected_row['strategy']
                            action = 'BUY' if 'BullCall' in strat or 'BearPut' in strat else 'SELL'
                            
                            st.write(f"Plaatsen {action} Combo Order voor {order_qty} stuks op {limit_price}...")
                            
                            trade = order_ib.place_spread_order(contract_buy, contract_sell, action, order_qty, limit_price)
                            
                            if trade:
                                st.success(f"Order Geplaatst! ID: {trade.order.orderId}")
                                st.info("Check TWS voor status.")
                            else:
                                st.error("Order plaatsen mislukt.")
                                
                        except Exception as e:
                            st.error(f"Fout bij order uitvoer: {e}")
                        finally:
                            order_ib.disconnect()
    else:
        st.info("Geen resultaten beschikbaar om te handelen. Start eerst een scan.")
