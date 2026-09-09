import streamlit as st
import pandas as pd
import numpy as np
import asyncio
import nest_asyncio
import datetime
import io
import yfinance as yf

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
from logic import SpreadScanner, PortfolioAnalyzer

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

def create_omnitrader_plotly_chart(df_hist, omni_info, symbol, entry_price, target_price=None, stop_price=None):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    
    if df_hist is None or df_hist.empty:
        return None
        
    df = df_hist.copy().reset_index(drop=True)
    df.columns = [c.lower() for c in df.columns]
    
    if 'date' in df.columns:
        date_col = df['date']
    else:
        date_col = pd.Series(df.index)
    
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.82, 0.18], vertical_spacing=0.04)
    
    # 1. Candlesticks
    fig.add_trace(go.Candlestick(
        x=date_col, open=df['open'], high=df['high'], low=df['low'], close=df['close'],
        name=f'{symbol} Koers',
        increasing_line_color='#26a69a', decreasing_line_color='#ef5350'
    ), row=1, col=1)
    
    # 2a. Entry Price Horizontal Line (SteelBlue)
    if entry_price > 0:
        fig.add_trace(go.Scatter(
            x=[date_col.iloc[0], date_col.iloc[-1]],
            y=[entry_price, entry_price],
            mode='lines',
            line=dict(color='SteelBlue', width=2),
            name=f'Entry Price (${entry_price:.2f})'
        ), row=1, col=1)

    # 2b. Handmatige Winstdoel Koers Line (Groen Gestreept)
    if target_price and target_price > 0:
        fig.add_trace(go.Scatter(
            x=[date_col.iloc[0], date_col.iloc[-1]],
            y=[target_price, target_price],
            mode='lines',
            line=dict(color='#00e676', width=2, dash='dash'),
            name=f'Winstdoel Target (${target_price:.2f})'
        ), row=1, col=1)

    # 2c. Handmatige Stoploss Koers Line (Rood Gestreept)
    if stop_price and stop_price > 0:
        fig.add_trace(go.Scatter(
            x=[date_col.iloc[0], date_col.iloc[-1]],
            y=[stop_price, stop_price],
            mode='lines',
            line=dict(color='#ff1744', width=2, dash='dash'),
            name=f'Handmatige Stoploss (${stop_price:.2f})'
        ), row=1, col=1)
        
    # 3. Trapsgewijze Stoplijn (Stepped Line shape='vh')
    d_hist = omni_info.get('drempel_history', [])
    if d_hist and len(d_hist) == len(df):
        fig.add_trace(go.Scatter(
            x=date_col, y=d_hist, mode='lines',
            line=dict(color='#00bfff', width=2.5, shape='vh'),
            name='OmniTrader Trapsgewijze Stop (Trailing Stop)'
        ), row=1, col=1)

    # 4. MarketState Bar at bottom (MediumSeaGreen = Bull, Orange = Bear)
    mstate_list = omni_info.get('marketstate_history', [omni_info.get('marketstate', 1)] * len(df))
    ms_colors = ['MediumSeaGreen' if m == 1 else 'Orange' for m in mstate_list]
    fig.add_trace(go.Bar(
        x=date_col,
        y=[1] * len(df),
        marker_color=ms_colors,
        name='MarketState (Coral Trend)'
    ), row=2, col=1)
    
    fig.update_layout(
        title=f"🛡️ OmniTrader BarToBar Advanced Grafiek: {symbol}",
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=480,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

def run_portfolio_check_action(tws_host, tws_port):
    import random
    client_id = random.randint(10000, 99999)
    ib_p = IBClient()
    success, msg = ib_p.connect(tws_host, tws_port, client_id)
    if not success:
        st.sidebar.error(f"Kan geen verbinding maken met TWS: {msg}")
        return
    
    with st.spinner("🔍 Bezig met ophalen en analyseren van TWS portfolio posities..."):
        positions = ib_p.get_account_portfolio_spreads() or []
        acc_summary = ib_p.get_account_summary() or {}
        analyzer = PortfolioAnalyzer(ib_p)
        
        exposure = PortfolioAnalyzer.calculate_portfolio_exposure(positions, net_liquidation=acc_summary.get('NetLiquidation', 0.0))
        st.session_state['portfolio_exposure'] = exposure
        st.session_state['portfolio_acc_summary'] = acc_summary
        
        evaluations = []
        for pos in positions:
            sym = pos['symbol']
            hist_df = pd.DataFrame()
            try:
                hist_df = ib_p.get_historical_data(Stock(symbol=sym, exchange='SMART', currency='USD'), duration='3 M', bar_size='1 day')
            except Exception:
                hist_df = pd.DataFrame()

            # yfinance Fallback if TWS returns empty
            if hist_df is None or hist_df.empty:
                try:
                    yf_t = yf.Ticker(sym)
                    yf_h = yf_t.history(period="3m")
                    if not yf_h.empty:
                        hist_df = yf_h.reset_index()
                        hist_df.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
                except Exception:
                    hist_df = pd.DataFrame()
                
            eval_res = analyzer.evaluate_position_health(pos, hist_df)
            eval_res['pos_data'] = pos
            eval_res['hist_df'] = hist_df
            evaluations.append(eval_res)
            
        st.session_state['portfolio_evaluations'] = evaluations
        st.session_state['portfolio_checked_time'] = datetime.datetime.now().strftime("%H:%M:%S")
        ib_p.disconnect()
        st.sidebar.success(f"✅ Portfolio gecontroleerd ({len(evaluations)} posities) om {st.session_state['portfolio_checked_time']}!")


def render_portfolio_management_dashboard(tws_host, tws_port):
    st.markdown("## 🛡️ Portfolio Bewaking & Geautomatiseerd Verliesbeheer")
    st.caption("Controleert live TWS posities en berekent indicator-gestuurde verliesbeperking en winstborging voor spreads én losse opties.")

    # Check US Options Market Status
    ib_temp = IBClient()
    mkt_info = ib_temp.is_us_options_market_open()
    if mkt_info.get('is_open'):
        st.success(f"🟢 **Amerikaanse Optiemarkt: GEOPEND** ({mkt_info.get('current_et', '')}) — {mkt_info.get('reason', '')}")
    else:
        st.info(f"🕒 **Amerikaanse Optiemarkt: GESLOTEN** ({mkt_info.get('current_et', '')}) — {mkt_info.get('reason', '')}\n\n*Orders geplaatst buiten openingstijden (15:30 - 22:00 CET) worden als BAG Limit order klaargezet in TWS en automatisch geactiveerd bij marktopening.*")

    col_cp1, col_cp2 = st.columns([3, 1])
    with col_cp1:
        if st.button("🔍 Voer Live Portfolio Check Uit", type="primary", key="btn_run_port_check"):
            run_portfolio_check_action(tws_host, tws_port)
            st.rerun()
    with col_cp2:
        if 'portfolio_checked_time' in st.session_state:
            st.caption(f"Laatste check: {st.session_state['portfolio_checked_time']}")

    evaluations = st.session_state.get('portfolio_evaluations', [])
    if not evaluations:
        if 'portfolio_checked_time' in st.session_state:
            st.warning("⚠️ **0 Openstaande Optieposities Gevonden**: De controle is uitgevoerd om " + st.session_state['portfolio_checked_time'] + ", maar er zijn momenteel geen actieve optieposities (spreads of losse opties) in jouw verbonden TWS account.\n\n"
                       "**Tips als je wél openstaande optieposities in TWS hebt staan:**\n"
                       "1. Controleer of de juiste TWS account is verbonden in de zijbalk (Paper Trading vs Real Trading op poort 7497 / 7496).\n"
                       "2. Controleer in TWS of de posities daadwerkelijk opties zijn (`OPT` / `FOP`) en niet enkel aandelen (`STK`).")
        else:
            st.info("ℹ️ Klik op **'🔍 Voer Live Portfolio Check Uit'** (of de knop in de zijbalk) om jouw TWS posities en risico-status in te laden.")
        return

    n_tot = len(evaluations)
    action_evals = [e for e in evaluations if e['action_code'] != 'HANDHAVEN']
    n_action = len(action_evals)
    threatened_evals = [e for e in evaluations if e.get('anti_assignment', {}).get('risk_level') in ['CRITICAL', 'HIGH'] and e['strategy'] != 'Stock']
    n_threatened = len(threatened_evals)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Totaal Open Posities", n_tot)
    m2.metric("🚨 Aanwijzingsgevaar", n_threatened, delta=f"{n_threatened} bedreigd" if n_threatened > 0 else "0", delta_color="inverse" if n_threatened > 0 else "normal")
    m3.metric("Actievereiste Totaal", n_action, delta=f"{n_action} te wijzigen" if n_action > 0 else "0", delta_color="inverse" if n_action > 0 else "normal")
    total_pnl = sum([e['pnl_usd'] for e in evaluations])
    m4.metric("Totale Ongerealiseerde P&L", f"${total_pnl:,.2f}")

    # Account Vermogensbewaking Banner
    exposure = st.session_state.get('portfolio_exposure')
    if exposure:
        net_liq = exposure.get('net_liquidation', 0.0)
        tot_put_obl = exposure.get('total_put_obligation', 0.0)
        lev_ratio = exposure.get('leverage_ratio', 0.0)
        max_loss = exposure.get('total_max_loss', 0.0)
        status_icon = exposure.get('status_icon', '🟢')
        status_msg = exposure.get('message', '')
        
        st.markdown("---")
        st.markdown("#### ⚖️ Account Vermogensbewaking & Hefboom Controle")
        c_v1, c_v2, c_v3, c_v4 = st.columns(4)
        c_v1.metric("Netto Liquidatie (Accountwaarde)", f"${net_liq:,.2f}" if net_liq > 0 else "Niet beschikbaar")
        c_v2.metric("Totale Put Aankoopverplichting", f"${tot_put_obl:,.2f}", help="Totale nominale waarde van 100 aandelen per short put bij verplichte levering (Assignment).")
        c_v3.metric(f"{status_icon} Hefboom (Obligatie / Net Liq)", f"{lev_ratio:.1f}%" if net_liq > 0 else "N/A", delta=f"{exposure['status']}", delta_color="normal" if exposure['status'] == 'VEILIG' else "inverse")
        c_v4.metric("Gedefinieerd Max Spread Verlies", f"${max_loss:,.2f}", help="Maximaal verlies bij faillissement onderliggende waarden zolang long spreads intact blijven.")
        
        if exposure['status'] == 'GEVAARLIJK':
            st.error(f"🚨 **{status_msg}**\n\n*Waarschuwing:* Bij een marktdaling kan IBKR direct overgaan tot automatische liquidatie als uw aankoopverplichting te groot is ten opzichte van uw rekening.")
        elif exposure['status'] == 'AANDACHT':
            st.warning(f"⚠️ **{status_msg}**")

    if n_threatened > 0:
        st.error(
            f"🚨 **CRITIEK AANWIJZINGSRISICO GEDETECTEERD BIJ {n_threatened} POSITIE(S)**\n\n"
            "Conform de richtlijnen uit het document (*'Bull Put- en Bear Call spread en TWS'*) dreigt voor deze posities ongewenste automatische uitoefening (verdampte extrinsieke waarde < $0.10, pin risk tussen strikes, of naderende broker-liquidatiedeadline).\n\n"
            "**Gevaar bij niets doen**: Automatische uitoefening leidt tot verplichte afname/levering van 100 aandelen per contract ($10.000+ marginbeslag), weekend gap-risico en geforceerde broker-liquidaties tegen slechte prijzen!"
        )
        if st.button(f"🛡️ Actie Uitvoeren: Sluit Alle {n_threatened} Bedreigde Posities Direct (TWS Combo Orders)", type="primary", width='stretch', key="btn_close_all_threatened"):
            exec_ib = IBClient()
            import random
            exec_id = random.randint(10000, 99999)
            s_ok, s_msg = exec_ib.connect(tws_host, tws_port, exec_id)
            if s_ok:
                actions_to_exec = []
                for th in threatened_evals:
                    actions_to_exec.append({
                        'symbol': th['symbol'],
                        'strategy': th['strategy'],
                        'selected_action': th['action_code'],
                        'action_code': th['action_code'],
                        'legs': th['pos_data'].get('legs', []),
                        'qty': th['pos_data'].get('qty', 1),
                        'market_price': th['pos_data'].get('market_price', 0.50)
                    })
                with st.spinner("Bezig met verzenden van beschermingsorders naar TWS..."):
                    res = exec_ib.execute_portfolio_adjustments(actions_to_exec)
                    for r in res:
                        st.write(f"• **{r['symbol']}**: {r['message']}")
                    st.success("🎉 Alle beschermingsorders zijn succesvol ingediend bij TWS!")
                exec_ib.ib.sleep(1.0)
                exec_ib.disconnect()
                st.rerun()
            else:
                st.error(f"Fout bij verbinden met TWS: {s_msg}")

    elif n_action > 0:
        st.warning(f"⚠️ **Portfolio Check Afgerond**: {n_action} van de {n_tot} openstaande posities vereisen een aanpassing van het exit-plan om verlies te minimaliseren of winst te borgen.")
    else:
        st.success("✅ **Portfolio Gezond**: Alle openstaande posities liggen momenteel in de veilige zone. Geen toewijzingsgevaar gedetecteerd.")

    st.markdown("---")
    st.markdown("### 📋 Positie Overzicht, Risico-Meldingen & Uitklikbare Bescherming")

    approved_actions = []

    for idx, item in enumerate(evaluations):
        sym = item['symbol']
        strat = item['strategy']
        expiry = item['expiry']
        dte = item['dte']
        pnl_usd = item['pnl_usd']
        pnl_pct = item['pnl_pct']
        act_code = item['action_code']
        act_title = item['action_title']
        old_to_new = item['old_to_new']
        reasoning = item['reasoning']
        result_desc = item['result_desc']
        alternatives = item['alternatives']
        tech_sum = item['technical_summary']
        anti_assign = item.get('anti_assignment', {})
        risk_lvl = anti_assign.get('risk_level', item.get('urgency', 'LOW'))

        is_stock = strat == 'Stock'
        if is_stock:
            urgency_badge = "🟢 AANDELENPOSITIE"
            header_icon = "📦"
        else:
            urgency_badge = "🔴 CRITICAL (AANWIJZINGSGEVAAR)" if risk_lvl == "CRITICAL" else ("🟠 HIGH (DEADLINE)" if risk_lvl == "HIGH" else ("🟡 MEDIUM" if risk_lvl == "MEDIUM" else "🟢 LOW (VEILIG)"))
            header_icon = '🚨' if risk_lvl in ['CRITICAL', 'HIGH'] else ('⚠️' if act_code != 'HANDHAVEN' else '✅')

        fin = item.get('financials', {})
        und_p = fin.get('underlying_price', item.get('underlying_price', 0.0))
        entry_p = fin.get('entry_price', item.get('entry_price', 0.0))
        mkt_p = fin.get('market_price', item.get('market_price', 0.0))
        bep_p = fin.get('bep_price', 0.0)
        bep_dist_usd = fin.get('bep_dist_usd', 0.0)
        bep_dist_pct = fin.get('bep_dist_pct', 0.0)
        bep_status = fin.get('bep_status', '')
        t1_stock = fin.get('t1_stock', 0.0)
        t1_status = fin.get('t1_status', '')
        t5_stock = fin.get('t5_stock', 0.0)
        t5_status = fin.get('t5_status', '')
        legs_bd = fin.get('legs_breakdown', [])

        with st.expander(f"{header_icon} **#{idx+1} {sym} - {strat} ({item['pos_data']['strikes_str']})** | Koers: ${und_p:,.2f} | P&L: ${pnl_usd:,.2f} ({pnl_pct:.1f}%) | DTE: {dte}d | Status: {urgency_badge}", expanded=(not is_stock and (risk_lvl in ['CRITICAL', 'HIGH'] or act_code != 'HANDHAVEN'))):
            # 1. Prominent Financial Overview (Koers Aandeel, Contract, P&L, BEP)
            f_col1, f_col2, f_col3, f_col4 = st.columns(4)
            with f_col1:
                st.metric("📈 Koers Aandeel", f"${und_p:,.2f}")
            with f_col2:
                st.metric(
                    f"🏷️ {fin.get('contract_label', 'Contractwaarde')}", 
                    f"${mkt_p:,.2f}", 
                    delta=f"Instap: ${entry_p:,.2f}", 
                    delta_color="off"
                )
            with f_col3:
                st.metric(
                    "💰 Huidige P&L", 
                    f"${pnl_usd:,.2f}", 
                    delta=f"{pnl_pct:+.1f}%", 
                    delta_color="normal" if pnl_usd >= 0 else "inverse"
                )
            with f_col4:
                st.metric(
                    "🎯 Break-Even (BEP)", 
                    f"${bep_p:,.2f}", 
                    delta=f"Afstand: {bep_dist_usd:+.2f} ({bep_dist_pct:+.1f}%)" if bep_p > 0 else "N/A",
                    delta_color="normal" if fin.get('is_in_profit') else "inverse"
                )

            # 2. Break-Even & Winstdoelen (1% en 5%) Card
            st.info(f"🎯 **Break-Even Point (BEP):** {bep_status}")
            tg_col1, tg_col2 = st.columns(2)
            with tg_col1:
                st.markdown(f"**🎯 1% Winstdoel**: Aandeel naar **`${t1_stock:,.2f}`**")
                st.caption(f"👉 {t1_status}")
            with tg_col2:
                st.markdown(f"**🎯 5% Winstdoel**: Aandeel naar **`${t5_stock:,.2f}`**")
                st.caption(f"👉 {t5_status}")

            # 3. Aparte Poten Weergave (als NIET LongCall / LongPut / Stock)
            if strat not in ['LongCall', 'LongPut', 'Stock'] and legs_bd:
                st.markdown("##### 🧩 Aparte Specificatie per Optiepoot:")
                legs_rows = []
                for l in legs_bd:
                    legs_rows.append({
                        "Poot": l['desc'],
                        "Richting": f"{'🟢' if l['action']=='LONG' else '🔴'} {l['action']}",
                        "Aantal": f"{l['position']}x",
                        "Instapprijs": f"${l['entry_price']:.2f}",
                        "Huidige Marktprijs": f"${l['market_price']:.2f}",
                        "Ongerealiseerde P&L": f"${l['pnl_usd']:+,.2f} ({l['pnl_pct']:+.1f}%)"
                    })
                st.dataframe(pd.DataFrame(legs_rows), use_container_width=True, hide_index=True)

            st.markdown("---")

            c1, c2 = st.columns([2, 1])
            with c1:
                # 1. Anti-Assignment Risicomelding (Conform Gemini / Roland van Giesen PDF)
                anti_assign = item.get('anti_assignment', {})
                triggers = anti_assign.get('triggers', [])
                consequences = anti_assign.get('consequences', '')
                rec_action = anti_assign.get('recommended_action', '')
                exec_type = anti_assign.get('execution_type', 'COMBO_CLOSE')
                ext_val = anti_assign.get('extrinsic_val', 0.0)
                intr_val = anti_assign.get('intrinsic_val', 0.0)

                if triggers:
                    if is_stock:
                        st.markdown("##### 📦 Positie Status:")
                        for trg in triggers:
                            st.info(trg)
                    else:
                        if risk_lvl == "SAFE":
                            st.markdown("##### 🟢 Expiratie & Winst Status:")
                            for trg in triggers:
                                st.success(trg)
                        else:
                            st.markdown("##### 🛡️ Anti-Assignment Risico Melding:")
                            for trg in triggers:
                                if "🟢" in trg or "✅" in trg:
                                    st.success(trg)
                                elif "💰" in trg or "⚠️" in trg:
                                    st.warning(trg)
                                else:
                                    st.error(trg)
                        
                        c_ext1, c_ext2 = st.columns(2)
                        with c_ext1:
                            st.metric("Resterende Tijdswaarde Short Leg", f"${ext_val:.2f}", delta="Gevarenzone (<$0.10)" if ext_val < 0.10 else "Veilig", delta_color="inverse" if ext_val < 0.10 else "normal")
                        with c_ext2:
                            st.metric("Intrinsieke Waarde Short Leg", f"${intr_val:.2f}", delta="In-The-Money" if intr_val > 0 else "Out-of-the-Money", delta_color="inverse" if intr_val > 0 else "normal")

                if consequences:
                    if is_stock:
                        st.caption(f"ℹ️ {consequences}")
                    else:
                        st.warning(f"⚠️ **Wat gebeurt er bij niets doen?**\n\n{consequences}")

                st.info(f"👉 **Geadviseerde Actie (Voorstel)**:\n\n{rec_action}")

                st.markdown(f"**Oude Situatie $\\rightarrow$ Nieuwe Situatie:**")
                st.info(f"👉 `{old_to_new}`")
                st.markdown(f"**Onderbouwing (Indicatoren):** {reasoning}")
                st.markdown(f"**Technisch Overzicht:** `{tech_sum}`")
                st.markdown(f"**Verwacht Resultaat:** {result_desc}")

                # OmniTrader BarToBar Advanced Exit Details Box
                omni_info = item.get('omnitrader_b2b', {})
                if omni_info:
                    st_price = omni_info.get('stop_price', 0.0)
                    mstate = omni_info.get('marketstate', 1)
                    trig = omni_info.get('exit_signal', False)
                    ms_color = "🟢 Coral Bullish (1)" if mstate == 1 else "🟠 Coral Red / Bearish (0)"
                    trig_color = "🔴 EXIT SIGNAAL ACTIEF" if trig else "🟢 POSITIE VEILIG"
                    st.caption(f"🛡️ **OmniTrader BarToBar Exit Model**: Stop-Niveau = **${st_price:.2f}** | Trend = **{ms_color}** | Status = **{trig_color}**")
                    
                    with st.expander(f"🎯 Handmatige Winst- & Verliessimulator (Koers X / Y)", expanded=True):
                        # Preset buttons to quickly set target price
                        btn_c1, btn_c2, btn_c3, btn_c4 = st.columns(4)
                        with btn_c1:
                            st.caption("⚡ Snelle Keuze:")
                        with btn_c2:
                            if st.button(f"🎯 BEP (${bep_p:.2f})", key=f"btn_bep_{idx}_{sym}", help="Zet winstdoel op Break-Even"):
                                st.session_state[f"tp_stock_{idx}_{sym}"] = float(bep_p)
                                st.rerun()
                        with btn_c3:
                            if st.button(f"🎯 1% Winst (${t1_stock:.2f})", key=f"btn_1p_{idx}_{sym}", help="Zet winstdoel op 1% winst"):
                                st.session_state[f"tp_stock_{idx}_{sym}"] = float(t1_stock)
                                st.rerun()
                        with btn_c4:
                            if st.button(f"🎯 5% Winst (${t5_stock:.2f})", key=f"btn_5p_{idx}_{sym}", help="Zet winstdoel op 5% winst"):
                                st.session_state[f"tp_stock_{idx}_{sym}"] = float(t5_stock)
                                st.rerun()

                        sold_k = item.get('sold_strike', 0.0)
                        bought_k = item.get('bought_strike', 0.0)
                        pos_qty = item['pos_data'].get('qty', 1)
                        is_long_stock = is_stock and item['pos_data'].get('is_long', True)
                        is_bullish = is_long_stock or strat in ['BullPut', 'BullCall', 'LongCall', 'ShortPut']
                        is_credit_pos = strat in ['BullPut', 'BearCall', 'ShortPut', 'ShortCall', 'IronCondor']

                        csim1, csim2 = st.columns(2)
                        with csim1:
                            def_tp_price = round(und_p * 1.05, 2) if is_bullish else round(und_p * 0.95, 2)
                            tp_stock_p = st.number_input(f"Winstdoel Koers {sym} ($)", min_value=0.01, value=def_tp_price, step=0.50, key=f"tp_stock_{idx}_{sym}")
                            
                            # Calculate profit estimate at target price
                            if is_credit_pos:
                                max_p_usd = abs(entry_p) * 100.0 * pos_qty if entry_p != 0 else 100.0 * pos_qty
                                if (is_bullish and tp_stock_p >= sold_k) or (not is_bullish and tp_stock_p <= sold_k):
                                    est_tp_pnl = max_p_usd
                                else:
                                    dist = abs(sold_k - tp_stock_p)
                                    est_tp_pnl = max(-max_p_usd * 2, max_p_usd - (dist * 100.0 * pos_qty))
                            elif is_stock:
                                # Pure aandelen: koersverschil * aantal aandelen (geen x100 factor)
                                est_tp_pnl = (tp_stock_p - und_p) * pos_qty if is_bullish else (und_p - tp_stock_p) * pos_qty
                            else:
                                est_tp_pnl = (abs(tp_stock_p - und_p) if (is_bullish and tp_stock_p > und_p) else -abs(tp_stock_p - und_p)) * 100.0 * pos_qty

                            st.success(f"💰 **Winst op Koers ${tp_stock_p:.2f}**: **+${est_tp_pnl:,.2f}**")

                        with csim2:
                            if is_stock and is_bullish:
                                omni_stop = omni_info.get('stop_price', 0.0) if omni_info else 0.0
                                def_sl_price = round(omni_stop, 2) if (0 < omni_stop < und_p) else round(und_p * 0.95, 2)
                            else:
                                def_sl_price = round(und_p * 0.95, 2) if is_bullish else round(und_p * 1.05, 2)

                            sl_stock_p = st.number_input(f"Stoploss Koers {sym} ($)", min_value=0.01, value=def_sl_price, step=0.50, key=f"sl_stock_{idx}_{sym}")

                            # Calculate loss estimate at stop price
                            if is_credit_pos:
                                max_p_usd = abs(entry_p) * 100.0 * pos_qty if entry_p != 0 else 100.0 * pos_qty
                                spread_width = abs(sold_k - bought_k) if (sold_k > 0 and bought_k > 0) else 5.0
                                max_loss_usd = max(100.0, (spread_width * 100.0 * pos_qty) - max_p_usd)
                                if (is_bullish and sl_stock_p <= bought_k) or (not is_bullish and sl_stock_p >= bought_k):
                                    est_sl_pnl = -max_loss_usd
                                else:
                                    dist_sl = abs(sold_k - sl_stock_p)
                                    est_sl_pnl = max(-max_loss_usd, max_p_usd - (dist_sl * 100.0 * pos_qty))
                            elif is_stock:
                                # Pure aandelen: verlies op stoploss koers * aantal aandelen
                                est_sl_pnl = (sl_stock_p - und_p) * pos_qty if is_bullish else (und_p - sl_stock_p) * pos_qty
                            else:
                                est_sl_pnl = -abs(und_p - sl_stock_p) * 100.0 * pos_qty

                            st.error(f"🛑 **Verlies op Koers ${sl_stock_p:.2f}**: **-${abs(est_sl_pnl):,.2f}**")
                            
                        st.caption("🔒 *Pas Winstdoel Koers ($X) of Stoploss Koers ($Y) aan om de gestreepte lijnen op de grafiek hieronder direct te laten bewegen.*")

                    with st.expander(f"📈 Bekijk OmniTrader Trapsgewijze Stop Grafiek ({sym})", expanded=True):
                        fig_omni = create_omnitrader_plotly_chart(
                            item.get('hist_df'), omni_info, sym, item.get('underlying_price', 0.0),
                            target_price=tp_stock_p, stop_price=sl_stock_p
                        )
                        if fig_omni:
                            st.plotly_chart(fig_omni, width='stretch', key=f"plotly_omni_{idx}_{sym}")
                        else:
                            st.caption("ℹ️ Geen historische koersdata beschikbaar voor deze grafiek.")
            with c2:
                # Directe 1-klik Actieknoppen ("Uitklikbare Bescherming")
                st.markdown("#### ⚡ 1-Klik Bescherming:")
                if exec_type == 'STOCK_CLOSE':
                    stock_qty = item['pos_data'].get('qty', 100)
                    is_long_stock = item['pos_data'].get('is_long', True)
                    stock_act_label = f"📉 Verkoop {stock_qty}x Aandelen {sym} Nu" if is_long_stock else f"📈 Koop {stock_qty}x Short Aandelen {sym} Terug"
                    if st.button(stock_act_label, type="primary", key=f"btn_stock_rec_{idx}_{sym}"):
                        single_act = [{
                            'symbol': sym,
                            'strategy': strat,
                            'selected_action': act_code,
                            'action_code': act_code,
                            'legs': item['pos_data'].get('legs', []),
                            'qty': stock_qty
                        }]
                        exec_ib = IBClient()
                        import random
                        s_ok, s_msg = exec_ib.connect(tws_host, tws_port, random.randint(10000, 99999))
                        if s_ok:
                            res = exec_ib.execute_portfolio_adjustments(single_act)
                            for r in res:
                                st.success(f"✅ {r['message']}")
                            exec_ib.ib.sleep(1.0)
                            exec_ib.disconnect()
                            st.rerun()
                elif exec_type == 'SHORT_LEG_ONLY' or act_code == 'LAAT_EXPIREEREN':
                    st.success("🟢 **Maximale Winst Bereikt!**")
                    st.caption("Beide poten lopen vanavond om 22:00 uur (NL tijd) gratis waardeloos af. Geen actie vereist.")
                    if st.button(f"🛡️ Koop Alleen Short Leg Terug ($0.01 Limit)", key=f"btn_quick_short_{idx}_{sym}", help="Koopt alleen de verkochte optiepoot terug op $0.01 Limit om 100% weekendrust te borgen, zonder risico op vastlopende combo orders."):
                        single_act = [{
                            'symbol': sym,
                            'strategy': strat,
                            'selected_action': 'SHORT_LEG_ONLY',
                            'action_code': 'SHORT_LEG_ONLY',
                            'legs': item['pos_data'].get('legs', []),
                            'qty': item['pos_data'].get('qty', 1),
                            'market_price': 0.01
                        }]
                        exec_ib = IBClient()
                        import random
                        s_ok, s_msg = exec_ib.connect(tws_host, tws_port, random.randint(10000, 99999))
                        if s_ok:
                            res = exec_ib.execute_portfolio_adjustments(single_act)
                            for r in res:
                                st.success(f"✅ {r['message']}")
                            exec_ib.ib.sleep(1.0)
                            exec_ib.disconnect()
                            st.rerun()
                        else:
                            st.error(f"Verbinding mislukt: {s_msg}")

                elif risk_lvl in ['CRITICAL', 'HIGH']:
                    close_btn_label = f"🛡️ Sluit Positie Nu (Combo Order)" if mkt_info.get('is_open') else f"🛡️ Sluit Positie (Wachtrij tot 15:30 CET)"
                    if st.button(close_btn_label, type="primary", key=f"btn_quick_close_{idx}_{sym}", help="Sluit beide optiebenen tegelijk als één combinatieorder (BAG Limit) in TWS om legging-in risico te vermijden."):
                        single_act = [{
                            'symbol': sym,
                            'strategy': strat,
                            'selected_action': 'TIJDIG_SLUITEN',
                            'action_code': 'TIJDIG_SLUITEN',
                            'legs': item['pos_data'].get('legs', []),
                            'qty': item['pos_data'].get('qty', 1),
                            'market_price': item['pos_data'].get('market_price', 0.50)
                        }]
                        exec_ib = IBClient()
                        import random
                        s_ok, s_msg = exec_ib.connect(tws_host, tws_port, random.randint(10000, 99999))
                        if s_ok:
                            res = exec_ib.execute_portfolio_adjustments(single_act)
                            for r in res:
                                st.success(f"✅ {r['message']}")
                            exec_ib.ib.sleep(1.0)
                            exec_ib.disconnect()
                            st.rerun()
                        else:
                            st.error(f"Verbinding mislukt: {s_msg}")
                    
                    if st.button(f"🔄 Rol Door naar Volgende Maand", key=f"btn_quick_roll_{idx}_{sym}", help="Sluit huidige expiratie en opent nieuwe legs op +30 DTE voor credit."):
                        single_act = [{
                            'symbol': sym,
                            'strategy': strat,
                            'selected_action': 'DOORROLLEN_CREDIT',
                            'action_code': 'DOORROLLEN_CREDIT',
                            'legs': item['pos_data'].get('legs', []),
                            'qty': item['pos_data'].get('qty', 1),
                            'market_price': item['pos_data'].get('market_price', 0.50)
                        }]
                        exec_ib = IBClient()
                        import random
                        s_ok, s_msg = exec_ib.connect(tws_host, tws_port, random.randint(10000, 99999))
                        if s_ok:
                            res = exec_ib.execute_portfolio_adjustments(single_act)
                            for r in res:
                                st.success(f"✅ {r['message']}")
                            exec_ib.ib.sleep(1.0)
                            exec_ib.disconnect()
                            st.rerun()
                        else:
                            st.error(f"Verbinding mislukt: {s_msg}")

                st.markdown("---")
                st.markdown("#### ⚙️ Batch Accordering:")
                is_approved = st.checkbox(f" Accordeer selectie (#{idx+1} {sym})", value=(act_code != 'HANDHAVEN'), key=f"chk_app_{idx}_{sym}")
                
                selected_alt = st.selectbox(
                    "Gekozen Actie:",
                    options=alternatives,
                    index=0,
                    key=f"sel_alt_{idx}_{sym}"
                )
                
                if is_approved:
                    approved_actions.append({
                        'symbol': sym,
                        'strategy': strat,
                        'selected_action': selected_alt,
                        'action_code': act_code,
                        'legs': item['pos_data'].get('legs', []),
                        'qty': item['pos_data'].get('qty', 1),
                        'market_price': item['pos_data'].get('market_price', 0.50)
                    })

    st.markdown("---")
    if approved_actions:
        st.success(f" Er zijn **{len(approved_actions)} posities** geselecteerd voor uitvoering in TWS.")
        if st.button("🚀 Voer Geselecteerde Portfolio-Wijzigingen Uit in TWS", type="primary", width='stretch', key="btn_exec_port_adj"):
            exec_ib = IBClient()
            import random
            exec_id = random.randint(10000, 99999)
            s_ok, s_msg = exec_ib.connect(tws_host, tws_port, exec_id)
            if s_ok:
                with st.spinner("Bezig met uitvoeren van orders in TWS..."):
                    res = exec_ib.execute_portfolio_adjustments(approved_actions)
                    for r in res:
                        st.write(f"• **{r['symbol']}**: {r['message']}")
                    st.success("🎉 Alle geselecteerde portfolio-wijzigingen zijn succesvol verzonden naar TWS!")
                exec_ib.ib.sleep(1.0)
                exec_ib.disconnect()
            else:
                st.error(f"Fout bij verbinden met TWS voor orderuitvoering: {s_msg}")


def render_filter_diagnostics_ui(diagnostics, expanded=True):

    """
    Displays a comprehensive filter rejection report:
    1. Reason breakdown (count and percentage dropped per filter rule).
    2. Top 3 primary bottlenecks showing sidebar setting vs average market values.
    3. Minimum required threshold adjustments (actionable advice for user).
    """
    if not diagnostics or diagnostics.get('total_generated', 0) == 0:
        st.info("ℹ️ Geen diagnostische scangegevens beschikbaar. Voer een scan uit om het uitfilterproces te analyseren.")
        return

    n_tot = diagnostics.get('total_generated', 0)
    top_3 = diagnostics.get('top_bottlenecks', [])
    breakdown = diagnostics.get('breakdown', [])

    with st.expander("🔍 **Uitfilter Analyse & Drempel Advies** (Waarom zijn trades afgekeurd?)", expanded=expanded):
        st.markdown(
            f"In totaal zijn er **{n_tot} kandidaten** gegenereerd door de optieketens en geëvalueerd op greeks, winstkans en rendement.\n\n"
            "Hieronder zie je precies welke filters de trades hebben tegengehouden en wat de werkelijke waarden in de markt waren:"
        )

        # 1. Top 3 Bottlenecks Highlight Cards
        if top_3:
            st.markdown("### 🏆 Top 3 Voornaamste Uitfilter Oorzaken")
            col_b1, col_b2, col_b3 = st.columns(3)
            cols = [col_b1, col_b2, col_b3]

            for idx, item in enumerate(top_3):
                with cols[idx % len(cols)]:
                    st.markdown(f"#### #{idx+1} {item['name']}")
                    st.metric(
                        label="Afgekeurde kandidaten",
                        value=f"{item['dropped_count']} / {n_tot}",
                        delta=f"-{item['dropped_pct']}% van totaal",
                        delta_color="inverse"
                    )
                    st.markdown(f"• **Ingestelde Waarde**: `{item['setting_str']}`")
                    st.markdown(f"• **Gemiddelde in Markt**: `{item['actual_avg']}`")
                    st.markdown(f"• **Minimaal Nodig (Top 5)**: `{item['suggested_min']}`")

            st.divider()

        # 2. Detailed Breakdown Table
        if breakdown:
            st.markdown("### 📊 Volledig Uitfilter Overzicht per Filter Criterium")
            df_diag = pd.DataFrame(breakdown)
            display_df = df_diag.rename(columns={
                'name': 'Filter Criterium',
                'setting_str': 'Ingestelde Waarde / Drempel',
                'dropped_count': 'Aantal Afgekeurd',
                'dropped_pct': 'Afgekeurd (%)',
                'actual_avg': 'Gemiddelde Waarde in Markt',
                'suggested_min': 'Minimaal Nodig voor Top 5 Spreads'
            })[['Filter Criterium', 'Ingestelde Waarde / Drempel', 'Aantal Afgekeurd', 'Afgekeurd (%)', 'Gemiddelde Waarde in Markt', 'Minimaal Nodig voor Top 5 Spreads']]
            st.dataframe(display_df, width='stretch', hide_index=True)

        st.info(
            "💡 **Advies voor meer kandidaten**: Als de grafiek mogelijkheden biedt maar er 0 resultaten verschijnen, "
            "pas dan de top 3 instellingen in de sidebar aan naar de **'Minimaal Nodig'** waarden om potentieel kansrijke trades direct zichtbaar te maken!"
        )

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
data_type_mode = st.sidebar.radio(
    "IBKR Marktdata Type",
    options=["Real-Time (Live)", "Vertraagd (Delayed)", "Bevroren (Delayed Frozen)"],
    index=0,
    help="Real-Time is standaard. Kies Vertraagd indien u geen betaald IBKR optie-abonnement heeft."
)

if data_type_mode == "Real-Time (Live)":
    selected_dtype = 1
    use_live_data = True
elif data_type_mode == "Vertraagd (Delayed)":
    selected_dtype = 3
    use_live_data = False
else:
    selected_dtype = 4
    use_live_data = False

use_free_data = st.sidebar.checkbox("Gebruik Gratis Yahoo Finance Data (Opties)", value=False, help="Haalt optieketens op via Yahoo Finance. Geen IBKR data abonnement nodig.")


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
if st.sidebar.button("🔍 Check Portfolio & Posities", width='stretch', key="btn_sb_port_check"):
    run_portfolio_check_action(tws_host, tws_port)

# Strategy Settings (Marktvisie)
st.sidebar.title("Strategie Instellingen")
marktvisie = st.sidebar.selectbox("Marktvisie", ["Bullish (Stijgend)", "Bearish (Dalend)", "Neutraal (Zijwaarts)"])

# Determine strategies based on Outlook
active_strategies = []
if "Bullish" in marktvisie:
    active_strategies = ["BullCall", "BullPut", "LongCall", "SynthCoveredCall"]
elif "Bearish" in marktvisie:
    active_strategies = ["BearCall", "BearPut", "LongPut", "SynthCoveredPut"]
elif "Neutraal" in marktvisie:
    active_strategies = ["IronCondor", "Strangle"]

# Allow manual override if needed?
# For now, stick to user request: "Kies als selectie versnelling... stijgende of dalende koersverwachting"
st.sidebar.markdown(f"**Actieve Strategieën:** {', '.join(active_strategies)}")

# S&P 500 Symbol List Definition & Caching Helper
STANDARD_SP500 = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "UNH", "JNJ", "XOM", "JPM", "V", "PG", "MA", "AVGO", "HD", "CVX", "MRK", "ABBV", "COST",
    "PEP", "ADBE", "WMT", "KO", "BAC", "ACN", "MCD", "CSCO", "TMO", "CRM", "ABT", "LIN", "ORCL", "NFLX", "AMD", "DIS", "PM", "PFE", "TXN", "DHR",
    "INTC", "CAT", "VZ", "AMGN", "IBM", "UNP", "SPGI", "LOW", "NOW", "HON", "BA", "COP", "GE", "AMAT", "GS", "QCOM", "BKNG", "NKE", "SBUX", "ELV",
    "INTU", "PLD", "BLK", "RTX", "ISR", "MDLZ", "TJX", "AXP", "GILD", "DE", "ADI", "ISRG", "MMC", "T", "LRCX", "SCHW", "C", "VRTX", "LMT", "EOG", "PGR"
]
OPTIONABLE_ETFS = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLK", "XLE", "XLV", "XLI", "XLY", "XLP", "XLB", "XLU", "XLRE", "GDX", "GLD", "TLT", "SLV", "USO", "UNG", "KRE", "SMH", "IBB", "XOP", "ARKK", "EEM", "FXI", "EWZ"]

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_sp500_symbols_raw():
    try:
        import urllib.request
        req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})
        html = urllib.request.urlopen(req, timeout=8).read()
        df_sp = pd.read_html(html)[0]
        syms = df_sp['Symbol'].dropna().astype(str).str.strip().tolist()
        if syms:
            return syms
    except Exception:
        pass
    return list(STANDARD_SP500)

def get_sp500_symbols(include_etfs=False, for_ib=False):
    raw_syms = fetch_sp500_symbols_raw()
    if for_ib:
        clean_syms = [s.replace('.', ' ') for s in raw_syms]
    else:
        clean_syms = [s.replace('.', '-') for s in raw_syms]
    if include_etfs:
        clean_syms = sorted(list(set(clean_syms + OPTIONABLE_ETFS)))
    else:
        clean_syms = sorted(list(set(clean_syms)))
    return clean_syms

# Batch Scanner Input
scan_modes_list = ["Enkel Symbool", "Batch Scan (Lijst)", "Batch Scan (Bestand)", "Live TWS Scanner", "BarChart Optie Flow (CSV)", "Auto-Pilot (Downloads map)", "Super-Fast ATM Long Scan (1% Koop)"]
current_scan_mode = st.session_state.get('scan_mode_choice', "Enkel Symbool")
if current_scan_mode not in scan_modes_list:
    current_scan_mode = "Enkel Symbool"
scan_mode_idx = scan_modes_list.index(current_scan_mode)
scan_mode = st.sidebar.selectbox("Scan Modus", scan_modes_list, index=scan_mode_idx, key="scan_mode_choice")

symbols_to_scan = []
scan_code = "MOST_ACTIVE" 
num_rows = 20

if scan_mode == "Enkel Symbool":
    sec_type = st.sidebar.radio("Type Activa", ["Aandeel", "Index"])
    symbol_input = st.sidebar.text_input("Symbool (bijv. SPY)", value="SPY")
    if symbol_input:
        symbols_to_scan = [symbol_input.strip().upper()]

elif scan_mode == "Batch Scan (Lijst)":
    # Pre-defined lists
    list_opts = ["S&P 500 (Wikipedia)", "S&P 100", "Top 10 Tech", "AEX"]
    cur_list_choice = st.session_state.get('batch_list_choice', "S&P 500 (Wikipedia)")
    if cur_list_choice not in list_opts:
        cur_list_choice = "S&P 500 (Wikipedia)"
    list_choice = st.sidebar.selectbox("Kies Lijst", list_opts, index=list_opts.index(cur_list_choice), key="batch_list_choice")
    if list_choice == "S&P 500 (Wikipedia)":
        symbols_to_scan = get_sp500_symbols(include_etfs=False)
        sec_type = "Aandeel"
        st.sidebar.caption(f"ℹ️ {len(symbols_to_scan)} S&P 500 aandelen geladen.")
    elif list_choice == "Top 10 Tech":
        symbols_to_scan = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "NFLX", "AMD", "INTC"]
        sec_type = "Aandeel"
    elif list_choice == "S&P 100":
        symbols_to_scan = [
            "AAPL", "ABBV", "ABT", "ACN", "ADBE", "AIG", "AMD", "AMGN", "AMT", "AMZN", "AVGO", "AXP", "BA", "BAC", "BK", "BKNG", "BLK", "BMY", "BRK.B", "C",
            "CAT", "CHTR", "CL", "CMCSA", "COF", "COP", "COST", "CRM", "CSCO", "CVS", "CVX", "DE", "DHR", "DIS", "DOW", "DUK", "EMR", "EXC", "F", "FDX",
            "GD", "GE", "GILD", "GM", "GOOG", "GOOGL", "GS", "HD", "HON", "IBM", "INTC", "INTU", "ISRG", "JNJ", "JPM", "KHC", "KO", "LIN", "LLY", "LMT",
            "LOW", "MA", "MCD", "MDLZ", "MDT", "MET", "META", "MMM", "MO", "MRK", "MS", "MSFT", "NEE", "NFLX", "NKE", "NVDA", "ORCL", "PEP", "PFE", "PG",
            "PM", "PYPL", "QCOM", "RTX", "SBUX", "SCHW", "SO", "SPG", "T", "TGT", "TMO", "TMUS", "TSLA", "TXN", "UNH", "UNP", "UPS", "USB", "V", "VZ", "WBA", "WFC", "WMT", "XOM"
        ]
        sec_type = "Aandeel"
        st.sidebar.caption(f"ℹ️ {len(symbols_to_scan)} S&P 100 aandelen geladen.")
    elif list_choice == "AEX":
        symbols_to_scan = ["ADYEN", "ASML", "UNA", "RDSA", "INGA"]
        sec_type = "Aandeel"

    if len(symbols_to_scan) >= 50:
        st.sidebar.info("💡 **Grote Batch Tip**: Bij 50+ aandelen (zoals S&P 100/500) duurt het opvragen van optiecontracten via TWS lang wegens broker rate limits. Vink bij **TWS Instellingen** 'Gebruik Gratis Yahoo Finance Data' aan om de scan binnen 1-2 minuten af te ronden!")

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
    
    col_bc1, col_bc2 = st.sidebar.columns(2)
    with col_bc1:
        barchart_min_size = st.sidebar.number_input("Min Block Size", value=100, step=50, help="Minimale trade grootte voor Smart Money filter. Zet op 0 voor alle maten.")
    with col_bc2:
        barchart_strict_codes = st.sidebar.checkbox("Strikte Codes", value=False, help="Filter op specifieke Barchart codes (MLCT, MLFT, etc.)")

    st.session_state['barchart_min_size'] = barchart_min_size
    st.session_state['barchart_strict_codes'] = barchart_strict_codes

    barchart_dfs = []
    
    if barchart_files:
        for f in barchart_files:
            try:
                df = pd.read_csv(f)
                # Find symbol column robustly regardless of casing or name
                sym_col = None
                for col in df.columns:
                    if str(col).strip().upper() in ['SYMBOL', 'TICKER', 'SYM']:
                        sym_col = col
                        break
                if sym_col:
                    df['Symbol'] = df[sym_col].astype(str).str.strip()
                    barchart_dfs.append(df)
                else:
                    st.sidebar.warning(f"Bestand '{f.name}' bevat geen 'Symbol' of 'Ticker' kolom.")
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
    
    auto_pilot_type = st.sidebar.selectbox(
        "Auto-Pilot Regel",
        ["Standaard Vertical Spreads", "Super-Fast ATM Long Scan (1% Koop)"],
        key="auto_pilot_type",
        help="Kies welke regels/modus de automatische scan moet uitvoeren."
    )
    
    # We delay loading symbols_to_scan until the actual execute phase so the user can replace the file while waiting
    sec_type = "Aandeel"

elif scan_mode == "Super-Fast ATM Long Scan (1% Koop)":
    st.sidebar.info("⚡ Super-snel scannen van aandelen en ETF's voor Long Call/Put opties via het 1% koop proces.")
    fast_universe = st.sidebar.selectbox(
        "Selecteer Universe", 
        ["S&P 500 (Wikipedia + Top ETF's)", "S&P 100", "Top 10 Tech", "Enkel Symbool (bijv. SPY)"],
        key="fast_atm_universe"
    )
    if fast_universe == "S&P 500 (Wikipedia + Top ETF's)":
        symbols_to_scan = get_sp500_symbols(include_etfs=True)
    elif fast_universe == "S&P 100":
        symbols_to_scan = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK.B", "UNH", "JNJ", "XOM", "JPM"]
    elif fast_universe == "Top 10 Tech":
        symbols_to_scan = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "NFLX", "AMD", "INTC"]
    else:
        cust_sym = st.sidebar.text_input("Symbool voor 1% Scan", value="SPY", key="fast_single_sym")
        symbols_to_scan = [cust_sym.strip().upper()] if cust_sym else ["SPY"]
    sec_type = "Aandeel"
    st.sidebar.caption(f"ℹ️ {len(symbols_to_scan)} symbolen geselecteerd voor ATM 1% scan.")

# Process any pending sidebar updates (safe from StreamlitAPIException)
if 'pending_sidebar_updates' in st.session_state:
    for k, v in st.session_state['pending_sidebar_updates'].items():
        st.session_state[k] = v
    del st.session_state['pending_sidebar_updates']

# Auto-Migration & Calibration Initialization (ensures existing browser sessions adopt optimal presets)
if st.session_state.get('calibration_version') != 'v3.2':
    st.session_state['calibration_version'] = 'v3.2'
    st.session_state['preset_min_strike'] = 5.0
    st.session_state['preset_min_bep_dist'] = 6.0
    st.session_state['sb_min_dte'] = 14
    st.session_state['sb_max_dte'] = 25
    st.session_state['sb_width'] = 10
    st.session_state['sb_min_delta'] = 0.10
    st.session_state['sb_max_delta'] = 0.30
    st.session_state['sb_min_pop'] = 60
    st.session_state['sb_min_profit'] = 40
    st.session_state['sb_itm_support'] = "Standaard (Min. afstand %)"
    st.session_state['active_profile_choice'] = "📅 Week Spreads (3 Weken, 14–25 DTE)"
    st.session_state['last_applied_profile'] = "📅 Week Spreads (3 Weken, 14–25 DTE)"

# Strategy Baseline Preset Initialization
if 'preset_min_strike' not in st.session_state:
    st.session_state['preset_min_strike'] = 5.0  # Spreads baseline: 5.0%
if 'preset_koopadvies_p' not in st.session_state:
    st.session_state['preset_koopadvies_p'] = 1.0 # Longs/Spreads baseline: 1.0%
if 'sb_min_dte' not in st.session_state:
    st.session_state['sb_min_dte'] = 14
if 'sb_max_dte' not in st.session_state:
    st.session_state['sb_max_dte'] = 25
if 'sb_width' not in st.session_state:
    st.session_state['sb_width'] = 10
if 'sb_itm_support' not in st.session_state:
    st.session_state['sb_itm_support'] = "Standaard (Min. afstand %)"
if 'use_stock_profiles' not in st.session_state:
    st.session_state['use_stock_profiles'] = True
if 'optimal_stock_configs' not in st.session_state:
    st.session_state['optimal_stock_configs'] = {}

# Filters & Profile Selector
st.sidebar.subheader("🎯 Beleggingsprofiel & Horizon")
profile_options = [
    "📅 Week Spreads (3 Weken, 14–25 DTE)",
    "📆 Maand Spreads (1–3 Maanden, 30–75 DTE)",
    "⚙️ Aangepast / Handmatig"
]
current_prof = st.session_state.get('active_profile_choice', profile_options[0])
chosen_prof = st.sidebar.selectbox(
    "Kies Horizon Profiel:", 
    profile_options, 
    index=profile_options.index(current_prof) if current_prof in profile_options else 0, 
    key='active_profile_choice',
    help="Week Spreads: snelle theta-decay over ~3 weken. Maand Spreads: ruimere foutmarge over 1-3 maanden met vroege 50% winstneming."
)

if 'last_applied_profile' not in st.session_state or st.session_state['last_applied_profile'] != chosen_prof:
    st.session_state['last_applied_profile'] = chosen_prof
    if "Week" in chosen_prof:
        st.session_state['sb_min_dte'] = 14
        st.session_state['sb_max_dte'] = 25
        st.session_state['preset_min_bep_dist'] = 6.0
        st.session_state['preset_min_strike'] = 5.0
        st.session_state['sb_min_delta'] = 0.10
        st.session_state['sb_max_delta'] = 0.30
        st.session_state['sb_width'] = 10
        st.session_state['sb_min_pop'] = 60
        st.session_state['sb_min_profit'] = 40
        st.session_state['sb_itm_support'] = "Standaard (Min. afstand %)"
        st.rerun()
    elif "Maand" in chosen_prof:
        st.session_state['sb_min_dte'] = 30
        st.session_state['sb_max_dte'] = 75
        st.session_state['preset_min_bep_dist'] = 8.0
        st.session_state['preset_min_strike'] = 6.0
        st.session_state['sb_min_delta'] = 0.10
        st.session_state['sb_max_delta'] = 0.28
        st.session_state['sb_width'] = 10
        st.session_state['sb_min_pop'] = 65
        st.session_state['sb_min_profit'] = 60
        st.session_state['sb_itm_support'] = "Standaard (Min. afstand %)"
        st.rerun()

if 'sidebar_reset_feedback' in st.session_state:
    st.sidebar.success(st.session_state.pop('sidebar_reset_feedback'))

col_sb_res1, col_sb_res2 = st.sidebar.columns(2)
with col_sb_res1:
    if st.button("⚡ Reset Filters", width='stretch', help="Reset alle filters naar de optimale basisinstellingen per strategie (8% BEP afstand voor Spreads, 1% voor Longs)"):
        is_long_only = any(s in active_strategies for s in ["LongCall", "LongPut"]) and not any(s in active_strategies for s in ["BullCall", "BullPut", "BearCall", "BearPut", "IronCondor", "Strangle"])
        reset_dict = {
            'preset_koopadvies_p': 1.0,
            'sb_itm_support': "Standaard (Min. afstand %)"
        }
        if "Maand" in chosen_prof:
            reset_dict.update({'sb_min_dte': 30, 'sb_max_dte': 75, 'preset_min_bep_dist': 8.0, 'preset_min_strike': 6.0, 'sb_width': 10, 'sb_min_pop': 65, 'sb_min_profit': 60, 'sb_min_delta': 0.10, 'sb_max_delta': 0.28})
        else:
            reset_dict.update({'sb_min_dte': 14, 'sb_max_dte': 25, 'preset_min_bep_dist': 6.0, 'preset_min_strike': 5.0, 'sb_width': 10, 'sb_min_pop': 60, 'sb_min_profit': 40, 'sb_min_delta': 0.10, 'sb_max_delta': 0.30})
        if is_long_only:
            reset_dict['preset_min_strike'] = 25.0  # Ruimte voor Deep ITM strikes (tot 25% ITM)
            reset_dict['preset_min_bep_dist'] = 0.0  # Geen spread-buffer uitsluiting voor Longs
            reset_dict['preset_koopadvies_p'] = 1.0  # Strikte 1% drempel
            reset_dict['sb_width'] = 5
            reset_dict['sb_min_pop'] = 65
        st.session_state['pending_sidebar_updates'] = reset_dict
        st.session_state['sidebar_reset_feedback'] = "✅ Filters gereset!"
        st.rerun()

with col_sb_res2:
    if st.button("🔬 Optimaliseer", width='stretch', help="Test huidige sidebar instellingen tegen de standaard EM85 benchmark en optimaliseer"):
        st.session_state['run_comparison_test_trigger'] = True

if st.session_state.get('optimal_stock_configs'):
    st.sidebar.caption(f"🎯 **{len(st.session_state['optimal_stock_configs'])} aandeel-profielen** actief in scanner")

st.sidebar.markdown("---")
st.sidebar.markdown("#### 🟢 Must-Have Filters (Primair)")
use_cache_toggle = st.sidebar.checkbox("Gebruik Cache (Indien parameters gelijk blijven)", value=True)
col_dte1, col_dte2 = st.sidebar.columns(2)
with col_dte1:
    min_dte = st.number_input("Min DTE", value=int(st.session_state.get('sb_min_dte', 14)), key='sb_min_dte')
with col_dte2:
    max_dte = st.number_input("Max DTE", value=int(st.session_state.get('sb_max_dte', 25)), key='sb_max_dte')
width = st.sidebar.number_input("Spread Breedte ($)", value=int(st.session_state.get('sb_width', 10)), key='sb_width')
min_bep_dist_pct = st.sidebar.number_input("Min. BEP Buffer Afstand %", min_value=0.0, max_value=30.0, value=float(st.session_state.get('preset_min_bep_dist', 6.0)), step=0.5, key='preset_min_bep_dist', help="Strikt filter: de koers moet minimaal dit percentage boven het Break-Even Punt liggen (bijv. 6.0%).")

delta_range = st.sidebar.slider(
    "Delta Bereik (Short Leg)", 0.01, 0.60, 
    (float(st.session_state.get('sb_min_delta', 0.10)), float(st.session_state.get('sb_max_delta', 0.30))), 
    step=0.01, 
    help="Gewenste Delta bandbreedte voor de verkochte optie (standaard 0.10 tot 0.30). Voorkomt illiquide fantoom-trades met minieme delta."
)
min_delta, max_delta = delta_range

st.sidebar.markdown("---")
st.sidebar.markdown("#### 🟡 Aanbevolen Filters (Strategie & Trend)")
min_pop = st.sidebar.slider("Min Kans op Winst (PoP %)", 0, 100, int(st.session_state.get('sb_min_pop', 60)), key='sb_min_pop')
min_profit = st.sidebar.number_input("Min Winst Potentie ($)", value=int(st.session_state.get('sb_min_profit', 40)), key='sb_min_profit')

# Koopadvies (Buy Recommendation) Filters
koopadvies_p = st.sidebar.slider("Koopadvies Drempel (1% Regel)", -5.0, 10.0, float(st.session_state.get('preset_koopadvies_p', 1.0)), step=0.5, key='preset_koopadvies_p', help="Aandeel hoeft slechts p% te stijgen/dalen voor winst (Basissetting = 1.0%).") / 100.0
only_koopadvies = st.sidebar.checkbox("Alleen Koopadvies tonen", value=False)
strike_range_pct = st.sidebar.number_input("Afstand tot Koers % (Strike Range)", min_value=-50.0, max_value=50.0, value=30.0, step=1.0, help="Positief = Bull Spreads ONDER de koers. Negatief = Bull Spreads BOVEN de koers.") / 100.0
min_strike_pct = st.sidebar.number_input("Min. afstand tot Koers % (Initiële Strike Afstand)", min_value=-50.0, max_value=50.0, value=float(st.session_state.get('preset_min_strike', 5.0)), step=1.0, key='preset_min_strike', help="Minimale strike afstand bij zoeken (Basissetting Spreads = 5.0%, Longs = 1.0%).") / 100.0

itm_options = ["Standaard (Min. afstand %)", "EM85 Optimaal (1.44x Expected Move)", "Niveau 1 (1x Expected Move)", "Niveau 2 (2x Expected Move)", "Niveau 3 (Extreme / 2.5x)"]
curr_itm_val = st.session_state.get('sb_itm_support', "Standaard (Min. afstand %)")
curr_itm_idx = itm_options.index(curr_itm_val) if curr_itm_val in itm_options else 0
itm_support_level = st.sidebar.selectbox(
    "ITM Veiligheidsmarge (Support Niveau)", 
    itm_options,
    index=curr_itm_idx,
    key='sb_itm_support',
    help="Gevalideerd optimum: EM85 (1.44x EM68) biedt de hoogste gemiddelde winst per trade ($100/trade) met 84% Hit Rate."
)

# Additional Strategy Overrides/Toggles
with st.sidebar.expander("Specifieke Strategieën", expanded=True):
    strategy_options = ["BullCall", "BullPut", "BearCall", "BearPut", "LongCall", "LongPut", "SynthCoveredCall", "SynthCoveredPut", "IronCondor", "Strangle"]
    final_strategies = []
    for s in strategy_options:
        is_default = s in active_strategies
        if st.checkbox(s, value=is_default):
            final_strategies.append(s)
    active_strategies = final_strategies

    long_focus_mode = "Beide / Vergelijking"
    if any(s in active_strategies for s in ['LongCall', 'LongPut']):
        st.markdown("---")
        long_focus_mode = st.selectbox(
            "🎯 Long Optie Focus / Richting",
            [
                "🛡️ Deep ITM (1% Koopdrempel - Kapitaalbehoud)",
                "⚡ ATM Meervoudig (Hefboom / x% Afstand)",
                "⚖️ Beide / Vergelijkingsmatrix"
            ],
            index=0,
            key="sb_long_focus",
            help="Deep ITM zoekt opties met Delta 0.80-0.95 en <= 1% BEP beweging. ATM zoekt rond de koers en berekent hefboom bij meerdere contracten."
        )

    synth_pmcc_min_long_dte = 180
    synth_pmcc_min_short_dte = 20
    synth_pmcc_max_short_dte = 50
    synth_target_delta = 0.25
    synth_target_otm = 8.0
    synth_min_premium = 2.00
    synth_min_roc = 5.0
    if any(s in active_strategies for s in ['SynthCoveredCall', 'SynthCoveredPut']):
        st.markdown("---")
        st.markdown("##### 🏛️ Synthetische Covered (PMCC/PMCP)")
        synth_pmcc_min_long_dte = int(st.number_input(
            "Minimaal LEAPS DTE (Long Leg)", 
            min_value=90, max_value=730, value=180, step=30,
            key="sb_pmcc_long_dte",
            help="Minimale looptijd van de diep ITM long optie (aandelenvervanger, conform regel: minimaal 6-12 maanden)."
        ))
        c_pmcc1, c_pmcc2 = st.columns(2)
        with c_pmcc1:
            synth_pmcc_min_short_dte = int(st.number_input(
                "Min Short DTE", min_value=7, max_value=60, value=20, step=5,
                key="sb_pmcc_short_min_dte",
                help="Minimale looptijd van de te verkopen OTM optie (inkomstenbron)."
            ))
        with c_pmcc2:
            synth_pmcc_max_short_dte = int(st.number_input(
                "Max Short DTE", min_value=14, max_value=90, value=45, step=5,
                key="sb_pmcc_short_max_dte",
                help="Maximale looptijd van de te verkopen OTM optie (sweet spot 30-45 DTE voor optimale theta decay)."
            ))
        c_pmcc3, c_pmcc4 = st.columns(2)
        with c_pmcc3:
            synth_target_delta = float(st.number_input(
                "Doel Delta Short", min_value=0.10, max_value=0.40, value=0.25, step=0.05, format="%.2f",
                key="sb_pmcc_delta",
                help="Standaard 0.25 voor gezonde cashflow en solide 75% PoP."
            ))
        with c_pmcc4:
            synth_target_otm = float(st.number_input(
                "Doel OTM %", min_value=2.0, max_value=20.0, value=8.0, step=1.0, format="%.1f",
                key="sb_pmcc_otm",
                help="Standaard 8.0% afstand van de short strike boven de koers."
            ))
        c_pmcc5, c_pmcc6 = st.columns(2)
        with c_pmcc5:
            synth_min_premium = float(st.number_input(
                "Min. Premie ($)", min_value=0.50, max_value=10.0, value=2.00, step=0.25, format="%.2f",
                key="sb_pmcc_min_prem",
                help="Minimale premieopbrengst van de short optie (standaard >= $2.00)."
            ))
        with c_pmcc6:
            synth_min_roc = float(st.number_input(
                "Min. Rendement (%)", min_value=1.0, max_value=25.0, value=5.0, step=0.5, format="%.1f",
                key="sb_pmcc_min_roc",
                help="Minimale cyclus-cashflow t.o.v. netto debit (standaard >= 5.0%)."
            ))

st.sidebar.markdown("---")
st.sidebar.markdown("#### ⚪ Handig / Geavanceerd (Optioneel)")
with st.sidebar.expander("Geavanceerde Marktfilters & Greeks", expanded=False):
    min_gamma = st.number_input("Min Gamma Exposure", value=0.0, step=0.001, format="%.4f")
    max_pain_buffer = st.number_input("Max Pain Buffer (Punten)", value=5, help="Minimale afstand tot Max Pain strike")

    st.markdown("---")
    st.markdown("**Markt Indicatoren (van Selectiemodel)**")
    curr_gex = st.number_input("Huidige GEX", value=0.0, help="Negatief voor Bullish momentum")
    curr_dex = st.number_input("Huidige DEX", value=0.0, help="Positief voor Bullish momentum")
    curr_pc = st.number_input("Put/Call Ratio", value=1.0, step=0.1, help="< 0.7 voor Bullish")

    use_auto_sentiment = st.checkbox("Gebruik automatisch sentiment model", value=True)

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

# Technical Filters (EMA & 1-Maands Trend)
st.sidebar.subheader("Technische Filters (EMA & Trend)")
use_ema = st.sidebar.checkbox("Filter op EMA Trend (Prijs > EMA)")
ema_spans = []
if use_ema:
    if st.sidebar.checkbox("EMA 8"): ema_spans.append(8)
    if st.sidebar.checkbox("EMA 20 (Maand-trend)"): ema_spans.append(20)
    if st.sidebar.checkbox("EMA 50 (Kwartaal-trend)"): ema_spans.append(50)
    if st.sidebar.checkbox("EMA 150"): ema_spans.append(150)
    use_ema_crossover = st.sidebar.checkbox("EMA 8 > EMA 50 (Snel Crossover)", value=False)
    use_ema20_50_crossover = st.sidebar.checkbox("EMA 20 > EMA 50 (Maand Crossover)", value=False, help="Controleert of het 20-daags gem. boven de 50-daagse trend ligt.")
    
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
    use_ema20_50_crossover = False
    use_stoch_rsi = False
    stoch_entry_a = stoch_entry_b = stoch_entry_c = False

st.sidebar.markdown("---")
st.sidebar.markdown("**📈 1-Maands Trend Model (Koersvoorspelling)**")
use_1m_trend_filter = st.sidebar.checkbox(
    "Filter op 1-Maands Trend (Stijging/Daling)",
    value=False,
    help="Analyseert 30-d Regressie, MACD, DMI en EMA-structuur om een duidelijke stijgings- of dalingsrichting te eisen."
)
trend_expected_direction = st.sidebar.selectbox(
    "Verwachte Koersrichting",
    ["Automatisch (Matchend met Marktvisie)", "Alleen Stijging (Bullish)", "Alleen Daling (Bearish)"],
    help="Matcht automatisch met de gekozen marktvisie of filtert strikt op stijgers/dalers."
)

@st.dialog("🔬 Functie onderzoek Filters & Criteria")
def run_research_dialog():
    st.write(
        "Dit onderzoek voert een geautomatiseerde parameter-sweep uit over 16 verschillende selectieparameters van het AntiGravity-systeem "
        "(waaronder spreadbreedte, winstkans, strike range, Max Pain buffer, GEX/DEX sentiment en technische filters).\n\n"
        "Er wordt een wiskundig onderbouwd Word-rapport (.docx) gegenereerd met statistieken en de top 10 spreads per parameter-instelling."
    )
    
    # Check if we have cached scan results
    has_cache = 'results' in st.session_state and not st.session_state.results.empty
    
    mode = st.radio(
        "Kies Gegevensbron:",
        ["Referentie Ticker Scan (yfinance - Aanbevolen)", "Huidige Gecachte Scangegevens gebruiken"] if has_cache else ["Referentie Ticker Scan (yfinance - Aanbevolen)"]
    )
    
    target_symbol = "SPY"
    if "Referentie Ticker Scan" in mode:
        target_symbol = st.text_input("Symbool voor Parameter Sweep:", value="SPY", help="Voer een ticker in om de sweep specifiek voor dat symbool uit te voeren (bijv. SPY, AAPL, QQQ, TSLA, NVDA)").upper().strip()
        if not target_symbol:
            target_symbol = "SPY"
    
    if st.button("Start Onderzoek", type="primary", width='stretch'):
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        log_ph = st.expander("Gedetailleerde Logboeken", expanded=True)
        
        log_lines = []
        def log_cb(msg):
            log_lines.append(msg)
            log_ph.code("\n".join(log_lines))
            
        def progress_cb(pct, msg):
            progress_bar.progress(pct)
            status_text.text(f"{int(pct*100)}% - {msg}")
            
        try:
            import os
            downloads_dir = os.path.join(os.path.expanduser('~'), 'Downloads')
            output_file = os.path.join(downloads_dir, f"Functie_onderzoek_filters_criteria_{target_symbol}.docx")
            
            # Check if output_file is writable (if it exists and is open/locked in Word)
            if os.path.exists(output_file):
                try:
                    with open(output_file, 'r+'):
                        pass
                except PermissionError:
                    # File is locked, find a writable fallback name
                    base_name = f"Functie_onderzoek_filters_criteria_{target_symbol}"
                    ext = ".docx"
                    idx = 1
                    while True:
                        fallback_file = os.path.join(downloads_dir, f"{base_name}_{idx}{ext}")
                        if not os.path.exists(fallback_file):
                            output_file = fallback_file
                            break
                        try:
                            with open(fallback_file, 'r+'):
                                output_file = fallback_file
                                break
                        except PermissionError:
                            idx += 1
                    log_cb(f"⚠️ Waarschuwing: Bestand is geopend in Word.")
                    log_cb(f"   Rapport wordt opgeslagen als: {os.path.basename(output_file)}")

            from research_runner import FCResearchRunner
            runner = FCResearchRunner()
            
            # Fetch target symbol chain
            ref_data = runner.fetch_reference_data(target_symbol, log_callback=log_cb)
            progress_cb(0.1, f"Gegevens voor {target_symbol} geladen. Sweeps starten...")
            
            results = runner.run_all_sweeps(ref_data, progress_callback=progress_cb, log_callback=log_cb)
            
            runner.build_docx_report(ref_data, results, output_file)
            
            # Also save a copy in workspace for application download button fallback
            try:
                import shutil
                shutil.copy(output_file, "Functie_onderzoek_filters_criteria.docx")
            except Exception:
                pass
                
            st.session_state.research_completed = True
            st.session_state.research_file_path = output_file
            st.success(f"✅ Winst-onderzoek succesvol voltooid!\n\nHebt rapport is automatisch opgeslagen in je Downloads map:\n`{output_file}`")
            
            # Button to open file directly in Microsoft Word
            if st.button("📖 Open Rapport direct in Word", type="primary", width='stretch'):
                try:
                    os.startfile(output_file)
                    st.success("Word wordt gestart...")
                except Exception as e:
                    st.error(f"Kon Word niet automatisch openen: {e}")
            
            with open(output_file, "rb") as f:
                st.download_button(
                    label="📥 Handmatig Downloaden (indien nodig)",
                    data=f,
                    file_name=os.path.basename(output_file),
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    width='stretch'
                )
        except Exception as e:
            st.error(f"Er is een fout opgetreden: {e}")

st.sidebar.markdown("---")
if st.sidebar.button("🔬 Functie onderzoek F&C", help="Voer een statistische sweep uit over alle F&C-parameters en genereer een Word-rapport"):
    run_research_dialog()

# Show persistent download & open buttons in sidebar if research has been completed
import os
if st.session_state.get('research_completed'):
    local_output_file = st.session_state.get('research_file_path')
    if not local_output_file:
        downloads_dir = os.path.join(os.path.expanduser('~'), 'Downloads')
        local_output_file = os.path.join(downloads_dir, "Functie_onderzoek_filters_criteria.docx")
    
    if os.path.exists(local_output_file):
        col_dl1, col_dl2 = st.sidebar.columns(2)
        with col_dl1:
            with open(local_output_file, "rb") as f:
                st.sidebar.download_button(
                    label="📥 Download",
                    data=f,
                    file_name=os.path.basename(local_output_file),
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    width='stretch'
                )
        with col_dl2:
            if st.sidebar.button("📖 Open Word", width='stretch'):
                try:
                    os.startfile(local_output_file)
                except Exception:
                    pass

# Main Area
st.title("Optie Contract Selectie Tool - AntiGravity")

# Weekend Warning
import datetime
today = datetime.datetime.now().weekday()
if today >= 5: # 5 = Saturday, 6 = Sunday
    st.warning("⚠️ **Weekend Modus Actief**: TWS levert momenteel beperkte live data. De scanner gebruikt de prijzen van afgelopen vrijdag (sluiting) als fallback voor berekeningen.")

tab0, tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🛡️ Portfolio Bewaking", "🚀 Scanner", "📊 Resultaten", "🛒 Orders", "📈 S&P 500 Spreads", "💰 Dividend CC", "🧪 Hit-Rate Test"])

# --- TAB 0: PORTFOLIO BEWAKING ---
with tab0:
    render_portfolio_management_dashboard(tws_host, tws_port)

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
                         dtype = selected_dtype
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

                         if scan_mode == "Super-Fast ATM Long Scan (1% Koop)":
                             current_symbols = list(symbols_to_scan) if symbols_to_scan else get_sp500_symbols(include_etfs=True)

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
                                 'use_ema': use_ema, 'use_ema_crossover': use_ema_crossover, 'use_ema20_50_crossover': use_ema20_50_crossover,
                                 'use_1m_trend_filter': use_1m_trend_filter, 'trend_expected_direction': trend_expected_direction,
                                 'use_stoch_rsi': use_stoch_rsi, 'stoch_entry_a': stoch_entry_a, 'stoch_entry_b': stoch_entry_b, 'stoch_entry_c': stoch_entry_c
                             }
                             core_hash = hashlib.md5(json.dumps(core_config, sort_keys=True).encode()).hexdigest()
                             
                             use_cache = use_cache_toggle and st.session_state.get('last_core_hash') == core_hash and 'all_unfiltered_global' in st.session_state
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
                             if scan_mode == "BarChart Optie Flow (CSV)" and 'barchart_raw' in st.session_state and not st.session_state['barchart_raw'].empty:
                                 status_text.text("Parsen van Barchart Option Flow CSV via VBA logica...")
                                 log("📊 Barchart 'Smart Money' filters toepassen op CSV data...")
                                 bc_min_sz = st.session_state.get('barchart_min_size', 100)
                                 bc_strict_c = st.session_state.get('barchart_strict_codes', False)
                                 barchart_df_parsed = scanner.parse_barchart_flow(
                                     st.session_state['barchart_raw'],
                                     min_size=bc_min_sz,
                                     strict_codes=bc_strict_c,
                                     log_func=log
                                 )
                                 if not barchart_df_parsed.empty:
                                     current_symbols = list(barchart_df_parsed['symbol'].unique())
                                     log(f"   ✅ {len(barchart_df_parsed)} Smart Money Setup(s) over {len(current_symbols)} unieke symbolen gevonden.")
                                 else:
                                     log(f"   ℹ️ Geen specifieke Option Flow setups in CSV gedetecteerd (waarschijnlijk een Aandelenlijst CSV of versoepel de block size).")
                                     log(f"   🔄 Automatische Fallback: Reguliere spread scanner wordt gestart voor alle {len(current_symbols)} symbolen uit de CSV!")

                             # 1. Technical Filter (EMA) Batch
                             if use_ema and ema_spans:
                                 log(f"📈 EMA Filter actief: {ema_spans} (Crossover 8/50: {use_ema_crossover}, 20/50: {use_ema20_50_crossover})")
                                 status_text.text("Bezig met ophalen historische data voor EMA filter...")

                             is_fast_atm = (scan_mode == "Super-Fast ATM Long Scan (1% Koop)") or (scan_mode == "Auto-Pilot (Downloads map)" and st.session_state.get('auto_pilot_type') == "Super-Fast ATM Long Scan (1% Koop)")

                             # 2. Main Loop
                             approved_symbols = []

                             for i, sym in enumerate(current_symbols):
                                 if is_fast_atm:
                                     fast_symbols = list(symbols_to_scan) if symbols_to_scan else get_sp500_symbols(include_etfs=True)
                                     
                                     fast_strategies = [s for s in active_strategies if s in ['LongCall', 'LongPut']]
                                     if not fast_strategies:
                                         if "Bullish" in marktvisie: fast_strategies = ["LongCall"]
                                         elif "Bearish" in marktvisie: fast_strategies = ["LongPut"]
                                         else: fast_strategies = ["LongCall", "LongPut"]
                                         log(f"ℹ️ Geen specifieke Long Call/Put aangevinkt: 'Super-Fast ATM Long Scan' activeert automatisch: {', '.join(fast_strategies)}.")
                                         
                                     progress_bar.progress(5)
                                     status_text.text("Starten van de supersnelle TWS scan...")
                                     
                                     def progress_cb(pct, msg, est_rem):
                                         progress_bar.progress(pct)
                                         if est_rem is not None:
                                             status_text.text(f"⏳ {msg} ({pct}%) - Nog ca. {est_rem} seconden")
                                         else:
                                             status_text.text(f"⏳ {msg} ({pct}%)")
                                     
                                     all_results = scanner.run_fast_atm_scan(
                                         symbols=fast_symbols,
                                         strategies=fast_strategies,
                                         min_dte=min_dte,
                                         max_dte=max_dte,
                                         koopadvies_p=koopadvies_p,
                                         log_func=log,
                                         progress_callback=progress_cb
                                     )
                                     
                                     if only_koopadvies and not all_results.empty:
                                         all_results = all_results[all_results['koopadvies'] == "✅"]
                                         
                                     all_unfiltered_global = all_results
                                     
                                     progress_bar.progress(100)
                                     status_text.text("Supersnelle scan voltooid!")
                                     break

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
                                         'max_delta': max_delta, 'min_bep_dist_pct': min_bep_dist_pct,
                                         'min_gamma': min_gamma, 'max_dte': max_dte, 'min_dte': min_dte, 
                                         'min_short_dte': synth_pmcc_min_short_dte, 'max_short_dte': synth_pmcc_max_short_dte,
                                         'synth_min_premium': synth_min_premium, 'synth_min_roc': synth_min_roc,
                                         'koopadvies_p': koopadvies_p, 'only_koopadvies': only_koopadvies
                                     }
                                     if use_max_pain_filter: current_filters['max_pain_dist'] = max_pain_dist
                                     
                                     filtered = scanner.filter_spreads(processed_spreads, current_filters, log_func=log)
                                     
                                     if auto_tune and len(filtered) < 5:
                                         ret_count = 0
                                         while len(filtered) < 5 and ret_count < 2:
                                             log(f"   🔄 Auto-Tune: Versoepelen filters (poging {ret_count+1})...")
                                             if 'min_delta' in current_filters: current_filters['min_delta'] = max(0.08, current_filters['min_delta'] - 0.03)
                                             if 'max_delta' in current_filters: current_filters['max_delta'] = min(0.35, current_filters['max_delta'] + 0.05)
                                             if 'min_bep_dist_pct' in current_filters: current_filters['min_bep_dist_pct'] = max(4.0, current_filters['min_bep_dist_pct'] - 2.0)
                                             if 'min_profit' in current_filters: current_filters['min_profit'] = max(25, current_filters['min_profit'] - 20)
                                             if 'min_pop' in current_filters: current_filters['min_pop'] = max(50, current_filters['min_pop'] - 5)
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
                                     is_likely_index = sym.upper() in ['SPX', 'NDX', 'RUT', 'VIX', 'DAX', 'DJI']
                                     if is_likely_index:
                                         contract = Index(sym, 'SMART', 'USD')
                                     else:
                                         contract = Stock(sym, 'SMART', 'USD')

                                     # 1. Get Real-time Price Snapshot
                                     market_data = scan_ib.get_market_data_snapshot(contract, use_hist_fallback=True, use_yf=use_free_data)
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

                                             ema_direction = 'bear' if 'Bearish' in marktvisie else 'bull'
                                             sym_data = {sym: {'price': check_price, 'history': hist_data}}
                                             passed = scanner.filter_symbols_by_ema(
                                                 sym_data, ema_spans, direction=ema_direction, 
                                                 ema_crossover=use_ema_crossover, ema20_50_crossover=use_ema20_50_crossover
                                             )
                                             if not passed:
                                                 log(f"   ⛔ {sym} gefilterd door EMA check ({'EMA20>50' if use_ema20_50_crossover else 'EMA trend'})")
                                                 continue # Skip this symbol
                                             log(f"   ✅ {sym} doorstaat EMA filter")

                                     # --- 1-Maands Trend Model (Koersvoorspelling) Check ---
                                     if use_1m_trend_filter and not hist_data.empty:
                                         trend_1m = scanner.predict_1month_trend(hist_data)
                                         log(f"   📈 1-Maands Trend Model: {trend_1m['forecast']} (Score: {trend_1m['score']}/+4)")
                                         log(f"      Pijlers: Regressie={trend_1m['details'].get('regression', 'N/A')}, MACD={trend_1m['details'].get('macd', 'N/A')}, DMI={trend_1m['details'].get('dmi', 'N/A')}")
                                         
                                         req_bull = ("Bullish" in trend_expected_direction) or ("Automatisch" in trend_expected_direction and "Bullish" in marktvisie)
                                         req_bear = ("Bearish" in trend_expected_direction) or ("Automatisch" in trend_expected_direction and "Bearish" in marktvisie)
                                         
                                         if req_bull and not trend_1m['passed_bullish']:
                                             log(f"   ⛔ {sym} gefilterd door 1-Maands Trend Model (Verwachte Stijging niet overtuigend, Score {trend_1m['score']})")
                                             continue
                                         elif req_bear and not trend_1m['passed_bearish']:
                                             log(f"   ⛔ {sym} gefilterd door 1-Maands Trend Model (Verwachte Daling niet overtuigend, Score {trend_1m['score']})")
                                             continue
                                         log(f"   ✅ {sym} doorstaat 1-Maands Trend Model filter")

                                     if use_auto_sentiment:
                                         indicators = {'gex': curr_gex, 'dex': curr_dex, 'pc_ratio': curr_pc}
                                         auto_sentiment = scanner.assess_market_sentiment(price, hist_data, indicators)
                                         log(f"   🤖 Automatisch sentiment gedetecteerd: {auto_sentiment}")

                                         if auto_sentiment == 'Bullish':
                                             suggested = ['BullCall', 'BullPut', 'LongCall']
                                         elif auto_sentiment == 'Bearish':
                                             suggested = ['BearCall', 'BearPut', 'LongPut']
                                         else:
                                             suggested = ['BullPut', 'BearCall', 'IronCondor', 'Strangle']

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
                                     log(f"   📉 EMA Status: {tech_signals['ema_status']} | EMA20/50: {tech_signals.get('ema20_50_status', 'N/A')}")
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

                                     if price < 8.0:
                                         log(f"   ⚠️ {sym} (${price:.2f}) is een penny-stock (< $8.00). Geen liquide verticale spreads mogelijk. Wordt overgeslagen.")
                                         continue

                                     if (int(max_dte) - int(min_dte)) <= 3:
                                         log(f"   ⚠️ Nauwe DTE range ({min_dte}-{max_dte}d). Dit kan leiden tot 0 resultaten.")

                                     log(f"   Optieketens opvragen...")
                                     sec_type_str = 'IND' if contract.secType == 'IND' else 'STK'
                                     chains = scan_ib.get_option_chains_params(sym, sec_type=sec_type_str, use_yf=use_free_data)

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
                                         
                                         if scan_mode == "BarChart Optie Flow (CSV)" and not barchart_df_parsed.empty:
                                             sym_spreads = barchart_df_parsed[barchart_df_parsed['symbol'] == sym].copy()
                                             if not sym_spreads.empty:
                                                 if underlying_iv > 0: sym_spreads['iv'] = underlying_iv
                                                 res = sym_spreads
                                         
                                         if res.empty:
                                             widths_to_check = [int(width)]
                                             if st.session_state.get('use_stock_profiles', True) and sym in st.session_state.get('optimal_stock_configs', {}):
                                                 opt_prof = st.session_state['optimal_stock_configs'][sym]
                                                 opt_w = opt_prof.get('winning_params', {}).get('spread_width')
                                                 if opt_w:
                                                     widths_to_check = [int(opt_w)]
                                                     log(f"   🎯 Aandeel-optimalisatie actief voor {sym}: Breedte geoptimaliseerd naar ${int(opt_w)}")
                                             elif price > 0:
                                                 if price < 25 and 2.5 not in widths_to_check:
                                                     widths_to_check.append(2.5)
                                                 if price < 50 and 5 not in widths_to_check:
                                                     widths_to_check.append(5)
                                                 if price > 400 and 15 not in widths_to_check:
                                                     widths_to_check.append(15)
                                                     
                                             for w in widths_to_check:
                                                p = {'symbol': sym, 'min_dte': min_dte, 'koopadvies_p': koopadvies_p, 'only_koopadvies': only_koopadvies, 'max_dte': d_max, 
                                                    'width': w, 'iv': underlying_iv, 'strike_range_pct': strike_range_pct, 'min_strike_pct': min_strike_pct,
                                                    'itm_support_level': itm_support_level, 'long_focus': long_focus_mode,
                                                    'min_long_dte': synth_pmcc_min_long_dte, 'min_short_dte': synth_pmcc_min_short_dte, 'max_short_dte': synth_pmcc_max_short_dte,
                                                    'synth_target_delta': synth_target_delta, 'synth_target_otm_pct': synth_target_otm, 'synth_min_premium': synth_min_premium, 'synth_min_roc': synth_min_roc}
                                                for strat in active_strategies:
                                                    # Single-leg and Synthetic Covered strategies do not use width. Only generate them for the first width.
                                                    if strat in ['LongCall', 'LongPut', 'SynthCoveredCall', 'SynthCoveredPut'] and w != widths_to_check[0]:
                                                        continue
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
                                     chain_cache = {}

                                     for idx, exp in enumerate(unique_expirations):
                                         exp_spreads = raw_spreads_all[raw_spreads_all['expiry'] == exp].copy()
                                         if exp_spreads.empty: continue
                                         log(f'      [{idx+1}/{len(unique_expirations)}] Download data voor expiratie {exp}...')

                                         def get_chain_for_exp(target_exp, target_spreads):
                                             if target_exp in chain_cache:
                                                 return chain_cache[target_exp]
                                             valid_strikes_for_exp = []
                                             for chain in chains:
                                                 if target_exp in chain.expirations:
                                                     valid_strikes_for_exp.extend(chain.strikes)
                                             valid_strikes_for_exp = sorted(list(set(valid_strikes_for_exp)))

                                             found_strikes = set()
                                             for col in ['strike_buy', 'strike_sell', 'strike_p_buy', 'strike_p_sell', 'strike_c_buy', 'strike_c_sell']:
                                                 if col in target_spreads.columns:
                                                     found_strikes.update(target_spreads[col].dropna().unique().tolist())
                                             found_strikes.discard(0.0)
                                             spread_strikes = found_strikes

                                             # Strike-optimalisatie: vraag enkel de benodigde spread-strikes op om pacing violations en 4-uur vertraging te voorkomen
                                             if use_max_pain_filter:
                                                 if price > 0:
                                                     lower_bound = price * 0.88
                                                     upper_bound = price * 1.12
                                                     mp_strikes = [s for s in valid_strikes_for_exp if lower_bound <= s <= upper_bound][:25]
                                                 else:
                                                     mp_strikes = valid_strikes_for_exp[:25]
                                                 final_strikes = sorted(list(set(mp_strikes) | spread_strikes))
                                             else:
                                                 if spread_strikes:
                                                     final_strikes = sorted(list(spread_strikes))
                                                 elif price > 0:
                                                     final_strikes = sorted(valid_strikes_for_exp, key=lambda s: abs(s - price))[:10]
                                                 else:
                                                     final_strikes = valid_strikes_for_exp[:10]

                                             cd = scan_ib.get_chain_greeks_and_oi(sym, target_exp, final_strikes, use_yf=use_free_data)
                                             if not cd.empty and 'expiration' not in cd.columns:
                                                 cd['expiration'] = target_exp
                                             chain_cache[target_exp] = cd
                                             return cd

                                         chain_data = get_chain_for_exp(exp, exp_spreads)

                                         # Multi-expiratie support voor diagonale spreads (SynthCoveredCall / SynthCoveredPut)
                                         if 'expiry_long' in exp_spreads.columns:
                                             diag_rows = exp_spreads[exp_spreads['expiry_long'].notna()]
                                             long_exps = diag_rows['expiry_long'].unique().tolist()
                                             for l_exp in long_exps:
                                                 if l_exp != exp:
                                                     sub_df = exp_spreads[exp_spreads['expiry_long'] == l_exp]
                                                     cd_long = get_chain_for_exp(l_exp, sub_df)
                                                     if not cd_long.empty:
                                                         chain_data = pd.concat([chain_data, cd_long], ignore_index=True)

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

                                         if scan_mode == "BarChart Optie Flow (CSV)" and not barchart_df_parsed.empty:
                                             d_min_dl, d_max_dl, d_min_gm, d_max_dt, d_min_dt = -1.0, 1.0, -1.0, 9999, 0
                                             d_min_bep = 0.0
                                         else:
                                             d_min_dl, d_max_dl, d_min_gm, d_max_dt, d_min_dt = min_delta, max_delta, min_gamma, max_dte, min_dte
                                             d_min_bep = min_bep_dist_pct
                                             
                                         current_filters = {
                                             'min_pop': min_pop,
                                             'min_profit': min_profit,
                                             'min_delta': d_min_dl,
                                             'max_delta': d_max_dl,
                                             'min_bep_dist_pct': d_min_bep,
                                             'min_gamma': d_min_gm,
                                             'max_dte': d_max_dt,
                                             'min_dte': d_min_dt, 
                                             'min_short_dte': synth_pmcc_min_short_dte, 'max_short_dte': synth_pmcc_max_short_dte,
                                             'synth_min_premium': synth_min_premium, 'synth_min_roc': synth_min_roc,
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
                                             if 'min_delta' in current_filters:
                                                 current_filters['min_delta'] = max(0.08, current_filters['min_delta'] - 0.03)
                                             if 'max_delta' in current_filters:
                                                 current_filters['max_delta'] = min(0.35, current_filters['max_delta'] + 0.05)
                                             if 'min_bep_dist_pct' in current_filters:
                                                 current_filters['min_bep_dist_pct'] = max(4.0, current_filters['min_bep_dist_pct'] - 2.0)
                                             if 'min_profit' in current_filters:
                                                 current_filters['min_profit'] = max(25, current_filters['min_profit'] - 20)
                                             if 'min_pop' in current_filters:
                                                 current_filters['min_pop'] = max(50, current_filters['min_pop'] - 5)
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

                                 # Calculate global guidance for Target 10 & filter diagnostics
                                 if not all_unfiltered_global.empty:
                                     global_guidance = scanner.get_filter_guidance(all_unfiltered_global, target_n=10)
                                     st.session_state['filter_guidance'] = global_guidance
                                     st.session_state['filter_diagnostics'] = scanner.analyze_filter_bottlenecks(all_unfiltered_global, {'min_pop': min_pop, 'min_profit': min_profit, 'min_delta': min_delta, 'max_delta': max_delta, 'min_bep_dist_pct': min_bep_dist_pct, 'min_gamma': min_gamma, 'max_dte': max_dte, 'min_dte': min_dte, 'min_short_dte': synth_pmcc_min_short_dte, 'max_short_dte': synth_pmcc_max_short_dte, 'only_koopadvies': only_koopadvies}, target_n=5)

                                 ranked = scanner.rank_spreads(all_results, sort_criteria=criteria, top_n=100) 
                                 st.session_state['results'] = ranked
                                 log(f"✅ Top {len(ranked)} spreads geselecteerd")
                                 st.success(f"{len(ranked)} optie contracten gevonden!")
                             else:
                                 log(f"⚠️ Geen optie contracten gevonden")
                                 st.warning("Geen optie contracten gevonden. Probeer parameters te verruimen.")
                                 if not all_unfiltered_global.empty:
                                     st.session_state['filter_diagnostics'] = scanner.analyze_filter_bottlenecks(all_unfiltered_global, {'min_pop': min_pop, 'min_profit': min_profit, 'min_delta': min_delta, 'max_delta': max_delta, 'min_bep_dist_pct': min_bep_dist_pct, 'min_gamma': min_gamma, 'max_dte': max_dte, 'min_dte': min_dte, 'min_short_dte': synth_pmcc_min_short_dte, 'max_short_dte': synth_pmcc_max_short_dte, 'only_koopadvies': only_koopadvies}, target_n=5)
                             
                             # Update GUI: scan klaar
                             scan_status.success("✅ Scan voltooid!")
                             progress_bar.progress(100)
                     finally:
                         scan_ib.disconnect()
                     st.rerun()
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
                 if 'filter_diagnostics' in st.session_state:
                     render_filter_diagnostics_ui(st.session_state['filter_diagnostics'], expanded=False)

             st.divider()
             st.subheader("Snel Overzicht")
             st.info("Voor details en filtering, ga naar tabblad **'📊 Resultaten'**")

             # Group by Strategy for separate tables
             df_res = st.session_state['results']
             is_fast_atm = (scan_mode == "Super-Fast ATM Long Scan (1% Koop)") or (scan_mode == "Auto-Pilot (Downloads map)" and st.session_state.get('auto_pilot_type') == "Super-Fast ATM Long Scan (1% Koop)")
             if is_fast_atm:
                 preview_cols = ['symbol', 'strategy', 'expiry', 'strike_buy', 'spread_ask_abs', 'winst_laat', 'winst_midden', 'winst_laatste', 'dte']
             else:
                 preview_cols = ['symbol', 'strategy', 'expiry', 'strike_buy', 'strike_sell', 'max_profit', 'pop', 'EM68', 'EM85', 'call_wall', 'put_wall', 'TTP (D)', 'TEI Score']
             preview_cols = [c for c in preview_cols if c in df_res.columns]

             strategies_found = df_res['strategy'].unique()
             for strat in strategies_found:
                 st.markdown(f"**Top 5 Unieke Symbolen: {strat}**")
                 df_strat = df_res[df_res['strategy'] == strat]
                 if 'symbol' in df_strat.columns:
                     df_strat = df_strat.drop_duplicates(subset=['symbol'], keep='first')
                 df_strat = df_strat.head(5)
                 st.dataframe(df_strat[preview_cols], width='content', hide_index=True)
        else:
             if 'filter_diagnostics' in st.session_state:
                 render_filter_diagnostics_ui(st.session_state['filter_diagnostics'], expanded=True)

    else:
        st.warning("Configureer en test eerst de TWS verbinding in de Sidebar.")

# --- TAB 2: RESULTATEN ---
with tab2:
    if 'results' in st.session_state and not st.session_state['results'].empty:
        results = st.session_state['results'].copy()

        # Checkboxes in dezelfde rij
        col_ag, col_dbg = st.columns([2.5, 1])
        with col_ag:
            max_per_sym_selection = st.selectbox(
                "🎯 Diversificatie: Max. resultaten per symbool",
                options=["1 per symbool (Maximale variatie - Aanbevolen)", "2 per symbool", "3 per symbool", "Alle opties tonen"],
                index=0,
                key="sel_max_per_sym"
            )
        with col_dbg:
            show_debug = st.checkbox("Debug Columns", False, key="chk_debug_cols")

        # Pas filtering toe op basis van diversificatie selectie
        if 'symbol' in results.columns:
            score_col = 'AG_Score' if 'AG_Score' in results.columns else ('winst_laat' if 'winst_laat' in results.columns else ('max_profit' if 'max_profit' in results.columns else None))
            if "1 per symbool" in max_per_sym_selection:
                max_k = 1
            elif "2 per symbool" in max_per_sym_selection:
                max_k = 2
            elif "3 per symbool" in max_per_sym_selection:
                max_k = 3
            else:
                max_k = None
                
            if max_k is not None and not results.empty:
                if score_col:
                    results['_rank_num'] = pd.to_numeric(results[score_col], errors='coerce').fillna(-999999.0)
                    results = results.sort_values(by='_rank_num', ascending=False).groupby('symbol', as_index=False, group_keys=False).head(max_k).drop(columns=['_rank_num'])
                else:
                    results = results.groupby('symbol', as_index=False, group_keys=False).head(max_k)

        # --- LONG CALL / LONG PUT VERGELIJKINGSMATRIX ---
        has_longs = any(results['strategy'].isin(['LongCall', 'LongPut']))
        scenario_dfs_to_save = []
        if has_longs and not is_fast_atm:
            with st.expander("⚖️ Vergelijkingsmatrix: 1x Deep ITM (1% Drempel) vs. Meervoudige ATM (Hefboom)", expanded=True):
                scanner_comp = SpreadScanner()
                long_syms = results[results['strategy'].isin(['LongCall', 'LongPut'])]['symbol'].unique()
                for sym_val in long_syms:
                    sym_long_df = results[results['symbol'] == sym_val]
                    u_px = float(sym_long_df['underlying_price'].iloc[0]) if 'underlying_price' in sym_long_df.columns and float(sym_long_df['underlying_price'].iloc[0]) > 0 else float(st.session_state.get('symbol_prices', {}).get(sym_val, 100.0))
                    comp_matrix = scanner_comp.build_long_comparison_matrix(sym_long_df, u_px)
                    if comp_matrix:
                        st.markdown(f"#### 📊 {sym_val} ({comp_matrix['strategy']}) – Vergelijking bij Koers ${u_px:.2f}")
                        m_col1, m_col2, m_col3, m_col4 = st.columns(4)
                        m_col1.metric("1x Deep ITM Investering", f"${comp_matrix['itm_cost']:.2f}", f"Strike ${comp_matrix['itm_contract']['strike_buy']:.1f} (Delta {comp_matrix['itm_delta_total']:.2f})")
                        m_col2.metric(f"{comp_matrix['atm_multiplier']}x ATM Investering", f"${comp_matrix['atm_total_cost']:.2f}", f"Strike ${comp_matrix['atm_contract']['strike_buy']:.1f} (Delta {comp_matrix['atm_delta_total']:.2f})")
                        m_col3.metric("BEP Vereiste Beweging", f"+{comp_matrix['itm_bep_move_pct']:.1f}% (ITM)", f"+{comp_matrix['atm_bep_move_pct']:.1f}% (ATM)", delta_color="inverse")
                        m_col4.metric("Risico bij -5% Dip", f"-{comp_matrix['itm_capital_risk_dip5']:.1f}% (ITM)", f"-{comp_matrix['atm_capital_risk_dip5']:.1f}% (ATM)", delta_color="inverse")
                        st.dataframe(comp_matrix['scenario_df'], use_container_width=True, hide_index=True)
                        st.caption("💡 **Conclusie**: De Deep ITM optie biedt kapitaalbescherming (bodemwaarde blijft behouden bij stilstand of dip) en is al quitte bij een kleine koersstap. De meervoudige ATM opties geven een veel grotere hefboom bij een sterke koersuitbraak, maar verliezen 100% van het kapitaal als de koers niet ver genoeg beweegt.")
                        sc_df = comp_matrix['scenario_df'].copy()
                        sc_df.insert(0, 'Symbool', sym_val)
                        sc_df.insert(1, 'Strategie', comp_matrix['strategy'])
                        scenario_dfs_to_save.append(sc_df)
        st.session_state['last_long_scenarios'] = scenario_dfs_to_save

        st.subheader(f"Gevonden Resultaten ({len(results)})")

        if show_debug:
            st.write(f"Columns in results: {results.columns.tolist()}")

        # Transformeer koopadvies om de strike-waarden en status te tonen
        if 'koopadvies' in results.columns and 'koopadvies_status' not in results.columns:
            results['koopadvies_status'] = results['koopadvies']
            
            # Helper om strike-waarden netjes te formatteren
            def format_koopadvies_strikes(row):
                strat = row.get('strategy', '')
                right = row.get('right', '')
                status = row.get('koopadvies_status', '❌')
                icon = "🟢" if status == "✅" else "🔴"
                
                if strat == 'IronCondor':
                    p_buy = row.get('strike_p_buy', 0.0)
                    p_sell = row.get('strike_p_sell', 0.0)
                    c_sell = row.get('strike_c_sell', 0.0)
                    c_buy = row.get('strike_c_buy', 0.0)
                    strikes_str = f"P {p_buy:.1f}/{p_sell:.1f} | C {c_sell:.1f}/{c_buy:.1f}"
                elif strat == 'Strangle':
                    p_buy = row.get('strike_p_buy', 0.0)
                    c_buy = row.get('strike_c_buy', 0.0)
                    strikes_str = f"P {p_buy:.1f} | C {c_buy:.1f}"
                else:
                    buy = row.get('strike_buy', 0.0)
                    sell = row.get('strike_sell', 0.0)
                    if sell > 0:
                        strikes_str = f"{right} {buy:.1f}/{sell:.1f}"
                    else:
                        strikes_str = f"{right} {buy:.1f}"
                return f"{icon} {strikes_str}"
                
            results['koopadvies'] = results.apply(format_koopadvies_strikes, axis=1)

        # Eerste kolom Selecteer initialiseren: ALTIJD standaard op False (veiligheid: nooit blind orders pre-selecteren)
        if 'bulk_selected_labels' not in st.session_state:
            st.session_state['bulk_selected_labels'] = set()

        if 'Selecteer' not in results.columns:
            results.insert(0, 'Selecteer', False)

        is_fast_atm = (scan_mode == "Super-Fast ATM Long Scan (1% Koop)") or (scan_mode == "Auto-Pilot (Downloads map)" and st.session_state.get('auto_pilot_type') == "Super-Fast ATM Long Scan (1% Koop)")
        if is_fast_atm:
            display_cols = [
                'Selecteer', 'koopadvies', 'symbol', 'underlying_price', 'strategy', 'expiry', 'strike_buy',
                'spread_ask_abs', 'spread_mid_abs', 'spread_last_abs',
                'winst_laat', 'winst_midden', 'winst_laatste', 'dte'
            ]
        else:
            display_cols = [
                'Selecteer', 'koopadvies', 'symbol', 'underlying_price', 'AG_Score', 'strategy', 'expiry', 'strike_buy', 'strike_sell', 'width', 
                'strike_p_buy', 'strike_p_sell', 'strike_c_sell', 'strike_c_buy',
                'spread_mid_abs', 'spread_ask_abs', 'b_l_verschil', 'max_profit', 'sluitingswinst', 'sluitingswinst_em85', 'pop',
                'TTP (D)', 'TEI Score', 'Efficient',
                'BEP', 'bep_afstand_pct', 'req_bep_move_pct', 'extrinsic_pct', 'capital_risk_flat_pct', 'capital_risk_dip5_pct', 'em85_dekking_pct', 'supports', 'resistances',
                'Sentiment', 'price_buy', 'price_sell', 'net_extrinsic', 'EM68', 'EM85',
                'delta_buy', 'delta_sell', 'delta', 'delta_koers', 'gamma', 'theta', 'dte', 
                'EMA_Cross', 'Stoch_RSI', 'iv_percentile', 'iv_rank', 'underlying_iv', 
                'gamma_flip', 'call_wall', 'put_wall', 'gex_wall'
            ]

        # Ensure columns exist before displaying
        final_cols = [c for c in display_cols if c in results.columns]
        
        # Convert Efficient bool to visual block
        if 'Efficient' in results.columns:
            results['Efficient'] = np.where(results['Efficient'] == True, "🟦", "⬜")


        # Column Configuration for Streamlit (Autosizing & Formatting)
        # Note: Removing most 'width' params to allow Streamlit's internal autosizing.
        col_cfg = {
            "Selecteer": st.column_config.CheckboxColumn("Selecteer", default=False, help="Vink aan om op te nemen in 'Plaats orders'", pinned=True),
            "symbol": st.column_config.TextColumn("Symbool"),
            "underlying_price": st.column_config.NumberColumn("Koers", format="$%.2f"),
            "spread_last_abs": st.column_config.NumberColumn("Laatste Prijs", format="$%.2f"),
            "winst_laat": st.column_config.NumberColumn("Winst (Laat)", format="$%.2f"),
            "winst_midden": st.column_config.NumberColumn("Winst (Midden)", format="$%.2f"),
            "winst_laatste": st.column_config.NumberColumn("Winst (Laatste)", format="$%.2f"),
            "AG_Score": st.column_config.NumberColumn("AG Score", format="⭐ %.1f"),
            "strategy": st.column_config.TextColumn("Strategie"),
            "expiry": st.column_config.TextColumn("Expiratie (Kort)", help="Expiratiedatum van de korte geschreven optie"),
            "expiry_long": st.column_config.TextColumn("Expiratie Long (LEAPS)", help="Expiratiedatum van de diep ITM long optie (LEAPS)"),
            "dte_long": st.column_config.NumberColumn("DTE Long", format="%d", help="Dagen tot expiratie van de long LEAPS optie"),
            "roc_cyclus": st.column_config.NumberColumn("ROC Cyclus %", format="%.1f%%", help="Rendement op netto debit per short cyclus (premie sell / netto debit)"),
            "roc_jaars": st.column_config.NumberColumn("ROC Jaar %", format="%.1f%%", help="Geannualiseerd rendement op kapitaal op basis van de short cyclus"),
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
            "EM68": st.column_config.NumberColumn("EM68", format="$%.2f", help="Expected Move 68% kans (1 Standard Deviation): verwachte koersuitslag"),
            "EM85": st.column_config.NumberColumn("EM85", format="$%.2f", help="Expected Move 85% kans (1.44 Standard Deviation): ruimere verwachte koersuitslag"),
            "expected_move": st.column_config.NumberColumn("EM68", format="$%.2f", help="Expected Move 68% kans"),
            "gamma_flip": st.column_config.NumberColumn("Gamma Flip", format="$%.2f"),
            "call_wall": st.column_config.NumberColumn("Call Wall", format="$%.2f"),
            "put_wall": st.column_config.NumberColumn("Put Wall", format="$%.2f"),
            "gex_wall": st.column_config.NumberColumn("GEX Wall", format="$%.2f"),
            "spread_mid_abs": st.column_config.NumberColumn("Middenprijs", format="$%.2f"),
            "spread_ask_abs": st.column_config.NumberColumn("Laatprijs", format="$%.2f"),
            "BEP": st.column_config.NumberColumn("BEP", format="$%.2f", help="Break Even Point gebaseerd op Laat prijs"),
            "bep_afstand_pct": st.column_config.NumberColumn("BEP Afstand", format="%.1f%%", help="Afstand in % tot het Break-Even Punt"),
            "req_bep_move_pct": st.column_config.NumberColumn("BEP Vereist %", format="%.2f%%", help="Benodigde koersbeweging van het aandeel om break-even te spelen (voor Deep ITM <= 1%)"),
            "extrinsic_pct": st.column_config.NumberColumn("Tijdswaarde %", format="%.2f%%", help="Tijdswaarde (verdampingsrisico) als percentage van de aandelenkoers"),
            "capital_risk_flat_pct": st.column_config.NumberColumn("Risico Vlak (0%)", format="%.1f%%", help="Verliespercentage van de inleg bij gelijkblijvende koers op expiratie"),
            "capital_risk_dip5_pct": st.column_config.NumberColumn("Risico Dip (-5%)", format="%.1f%%", help="Verliespercentage van de inleg bij een 5% tegenbeweging van het aandeel (Vergelijkend Risicogetal)"),
            "em85_dekking_pct": st.column_config.NumberColumn("EM85 Dekking", format="%.1f%%", help="Afstand van BEP tot koers als % van EM85 (>100% betekent dat BEP buiten 85% kansbereik ligt)"),
            "EMA_Cross": st.column_config.TextColumn("EMA 8/50"),
            "Stoch_RSI": st.column_config.TextColumn("Stoch RSI Status"),
            "Sentiment": st.column_config.TextColumn("Sentiment"),
            "price_buy": st.column_config.NumberColumn("Prijs Buy", format="$%.2f"),
            "price_sell": st.column_config.NumberColumn("Prijs Sell", format="$%.2f"),
            "net_extrinsic": st.column_config.NumberColumn("Net Extrin.", format="$%.2f"),
            "delta_buy": st.column_config.NumberColumn("Delta Buy", format="%.3f"),
            "delta_sell": st.column_config.NumberColumn("Delta Sell", format="%.3f"),
            "delta": st.column_config.NumberColumn("Net Delta", format="%.3f"),
            "delta_koers": st.column_config.NumberColumn("Delta Koers", format="%.3f", help="Snelheid en versnelling van winstrespons: abs(Net Delta) + Net Gamma"),
            "gamma": st.column_config.NumberColumn("Gamma", format="%.4f"),
            "theta": st.column_config.NumberColumn("Theta", format="%.3f"),
            "dte": st.column_config.NumberColumn("DTE", format="%d", help="Rood = Earnings beperking kon niet worden gehaald"),
            "b_l_verschil": st.column_config.NumberColumn("Slip M/L", format="$%.2f", help="Verschil Midden vs Laat (Laag is beter)"),
            "max_pain": st.column_config.NumberColumn("Max Pain 1", format="$%.2f"),
            "max_pain_selection": st.column_config.NumberColumn("Max Pain 2", format="$%.2f"),
            "max_pain_buffer_ok": st.column_config.CheckboxColumn("MP Buffer OK", help="Spread is > 5 punten van Max Pain"),
            "dist_max_pain": st.column_config.NumberColumn("MP Afstand", format="$%.2f"),
            "max_profit": st.column_config.NumberColumn("Max Winst", format="$%.2f", help="Maximale winst in dollars"),
            "sluitingswinst": st.column_config.NumberColumn("Sluitingswinst (EM68)", format="$%.2f", help="Geschatte winst/verlies bij vervroegde sluiting (na 5 dagen of halverwege) bij 1 EM68 koersstijging/-daling in gunstige richting"),
            "sluitingswinst_em85": st.column_config.NumberColumn("Sluitingswinst (EM85)", format="$%.2f", help="Geschatte winst/verlies bij vervroegde sluiting (na 5 dagen of halverwege) bij 1 EM85 koersstijging/-daling in gunstige richting"),
            "pop": st.column_config.NumberColumn("Kans op Winst (PoP)", format="%.1f%%", help="Probability of Profit"),
            "koopadvies": st.column_config.TextColumn("Koopadvies", help="Groen = Koopadvies (winstgevend bij p% beweging), Rood = Geen koopadvies", pinned=True),
            "TTP (D)": st.column_config.NumberColumn("TTP (Dagen)", format="%.1f", help="Days to Profit ($5 doel)"),
            "TEI Score": st.column_config.NumberColumn("TEI Score", format="%.2f", help="Target Efficiency Index (BS Model)"),
            "Efficient": st.column_config.TextColumn("Efficiënt", help="Blauw = TEI Score > 1.2 en TTP < DTE/2"),
        }

        # Ensure columns exist before displaying
        final_cols = [c for c in display_cols if c in results.columns]

        # Helper voor rode DTE (bij relaxed_earnings) en Koopadvies achtergrondkleur
        def style_results(row):
            styles = [''] * len(row)
            if 'relaxed_earnings' in row.index and row['relaxed_earnings'] == True:
                # Vind index van 'dte' in rij
                if 'dte' in row.index:
                    try:
                        idx = row.index.get_loc('dte')
                        styles[idx] = 'color: red; font-weight: bold'
                    except:
                        pass
            # Kleur achtergrond van Koopadvies kolom op basis van status
            if 'koopadvies' in row.index and 'koopadvies_status' in row.index:
                try:
                    k_idx = row.index.get_loc('koopadvies')
                    status = row['koopadvies_status']
                    if status == "✅":
                        styles[k_idx] = 'background-color: #15803d; color: white; font-weight: bold;'
                    else:
                        styles[k_idx] = 'background-color: #b91c1c; color: white; font-weight: bold;'
                except:
                    pass
            return styles

        # Filter config to only existing columns
        final_cfg = {k: v for k, v in col_cfg.items() if k in final_cols}

        # Consistent label generation
        def _make_trade_label(x):
            exp_txt = f"{x['expiry']} / Long:{x['expiry_long']}" if ('expiry_long' in x and pd.notna(x['expiry_long']) and str(x['expiry_long']) != str(x['expiry'])) else f"{x['expiry']}"
            return f"#{x.name} {x['symbol']} {exp_txt} {x['strategy']} {x['strike_buy']}/{x['strike_sell']} (max ${x.get('max_profit', 0):.0f})"
        results['label'] = results.apply(_make_trade_label, axis=1)
        labels = results['label'].unique()

        # Shared state initialization
        if 'selected_trade_label' not in st.session_state or st.session_state['selected_trade_label'] not in labels:
            st.session_state['selected_trade_label'] = labels[0] if len(labels) > 0 else None

        # Synchroniseer de 'Selecteer' kolom met de daadwerkelijk door de gebruiker gekozen labels
        if 'bulk_selected_labels' in st.session_state:
            results['Selecteer'] = results['label'].isin(st.session_state['bulk_selected_labels'])
        else:
            results['Selecteer'] = False

        # Apply style and display
        styled_df = results[final_cols].style.apply(style_results, axis=1)

        editor_key = f"results_matrix_editor_{max_per_sym_selection}_{len(results)}"
        edited_results = st.data_editor(
            styled_df, 
            column_config=final_cfg,
            width='content',
            hide_index=False,
            height=550,
            disabled=[c for c in final_cols if c != 'Selecteer'],
            key=editor_key
        )

        # 1. Lees de aangevinkte rijen direct uit edited_results
        new_selected_labels = set()
        if 'Selecteer' in edited_results.columns:
            for r_idx in edited_results[edited_results['Selecteer'] == True].index:
                if r_idx in results.index and 'label' in results.columns:
                    new_selected_labels.add(results.loc[r_idx, 'label'])

        # 2. Check ook de actieve widget state voor eventuele directe edits in Glide Data Grid
        if editor_key in st.session_state and isinstance(st.session_state[editor_key], dict):
            ed_rows = st.session_state[editor_key].get('edited_rows', {})
            for r_pos_str, changes in ed_rows.items():
                if 'Selecteer' in changes:
                    try:
                        r_pos = int(r_pos_str)
                        if 0 <= r_pos < len(results):
                            r_lbl = results['label'].iloc[r_pos]
                            if changes['Selecteer']:
                                new_selected_labels.add(r_lbl)
                            else:
                                new_selected_labels.discard(r_lbl)
                    except Exception:
                        pass

        st.session_state['bulk_selected_labels'] = new_selected_labels

        # Bepaal de definitieve lijst met geselecteerde rijen uit results
        selected_rows = results[results['label'].isin(st.session_state['bulk_selected_labels'])].copy()

        # Als er precies 1 rij geselecteerd is, synchroniseer direct met de selectbox en het Orders-tabblad
        if len(selected_rows) == 1:
            one_lbl = selected_rows['label'].iloc[0]
            st.session_state['selected_trade_label'] = one_lbl
            st.session_state['res_sel'] = one_lbl
            st.session_state['ord_sel'] = one_lbl

        # --- BULK ORDER UTILITIES & BUTTON ---
        st.write("")
        if not selected_rows.empty:
            summary_symbols = ", ".join([f"**{r['symbol']}** ({r['strategy']} ${r['strike_buy']:.1f})" for _, r in selected_rows.iterrows()])
            st.success(f"🎯 **Geselecteerd voor order ({len(selected_rows)}):** {summary_symbols}")
        else:
            st.info("💡 **Geen contract aangevinkt:** Vink in de linkerkolom ('Selecteer') het contract aan dat je wilt kopen.")

        if not selected_rows.empty:
            bulk_put_obl = 0.0
            for _, r in selected_rows.iterrows():
                r_strat = str(r.get('strategy', ''))
                if 'Put' in r_strat or 'PUT' in r_strat.upper():
                    r_sell_k = float(r.get('strike_sell', 0.0) or 0.0)
                    if r_sell_k > 0:
                        bulk_put_obl += r_sell_k * 100.0 * bulk_qty
            if bulk_put_obl > 0:
                st.warning(f"⚖️ **Totale Potentiële Aankoopverplichting bij Uitoefening (Bulk):** **${bulk_put_obl:,.2f}** ({bulk_qty}x per positie). Voorkom overleverage!")

        col_b1, col_b2, col_b3, col_b4 = st.columns([1, 1.3, 1, 1.7])
        with col_b1:
            bulk_qty = st.number_input("Aantal stuks", min_value=1, value=1, key="bulk_order_qty")
        with col_b2:
            bulk_order_type = st.selectbox("Order Type (Executie)", 
                ["Adaptive - Normal", "LMT (Standaard Limiet)", "Adaptive - Urgent", "Adaptive - Patient"],
                index=0,
                key="bulk_order_type"
            )
        with col_b3:
            bulk_bracket_tif = st.selectbox("Exit TIF", ["GTC", "DAY"], index=0, key="bulk_bracket_tif", help="GTC (Good 'Til Canceled) voor Take Profit en Stop Loss orders.")
        with col_b4:
            st.write("")
            st.write("")
            btn_title = f"🚀 PLAATS ORDER(S) ({len(selected_rows)} stuks)" if not selected_rows.empty else "🚀 PLAATS ORDERS"
            btn_place_bulk = st.button(btn_title, type="primary", width='stretch', disabled=selected_rows.empty)

        if btn_place_bulk:
            if selected_rows.empty:
                st.warning("⚠️ **Geen regels geselecteerd.** Vink in de eerste kolom ('Selecteer') minstens één order aan.")
            else:
                st.info(f"⏳ **Plaatsen van {len(selected_rows)} order(s) bij TWS gestart...**")
                import random
                client_id_bulk = random.randint(20000, 29999)
                bulk_ib = IBClient()
                success, msg = bulk_ib.connect(tws_host, tws_port, client_id_bulk)
                if not success:
                    st.error(f"❌ Kan geen verbinding maken voor order: {msg}")
                else:
                    try:
                        placed_count = 0
                        for idx_num, (orig_idx, row) in enumerate(selected_rows.iterrows()):
                            symbol = str(row['symbol']) if pd.notna(row.get('symbol')) else ''
                            strat = str(row['strategy']) if pd.notna(row.get('strategy')) else ''
                            expiry = str(row['expiry']) if pd.notna(row.get('expiry')) else ''
                            
                            right = str(row.get('right')) if pd.notna(row.get('right')) and str(row.get('right')).strip() else ''
                            if not right or right not in ['C', 'P']:
                                if any(k in strat for k in ['Put', 'PUT', 'put']):
                                    right = 'P'
                                else:
                                    right = 'C'
                            
                            strikes_dict = {
                                'strike_buy': float(row.get('strike_buy')) if pd.notna(row.get('strike_buy')) else 0.0,
                                'strike_sell': float(row.get('strike_sell')) if pd.notna(row.get('strike_sell')) else 0.0,
                                'strike_p_buy': float(row.get('strike_p_buy')) if pd.notna(row.get('strike_p_buy')) else 0.0,
                                'strike_p_sell': float(row.get('strike_p_sell')) if pd.notna(row.get('strike_p_sell')) else 0.0,
                                'strike_c_sell': float(row.get('strike_c_sell')) if pd.notna(row.get('strike_c_sell')) else 0.0,
                                'strike_c_buy': float(row.get('strike_c_buy')) if pd.notna(row.get('strike_c_buy')) else 0.0
                            }
                            
                            raw_price = row.get('spread_ask_abs', 0)
                            if not raw_price or pd.isna(raw_price) or float(raw_price) <= 0:
                                raw_price = row.get('spread_mid_abs', 0)
                            if not raw_price or pd.isna(raw_price) or float(raw_price) <= 0:
                                raw_price = 0.10
                            limit_price_val = abs(float(raw_price))
                            
                            trade = bulk_ib.place_strategy_order(
                                symbol=symbol,
                                expiry=expiry,
                                right=right,
                                strategy=strat,
                                strikes_dict=strikes_dict,
                                action='BUY',
                                quantity=bulk_qty,
                                price=limit_price_val,
                                order_type=bulk_order_type,
                                enable_bracket=True,
                                tp_pct=0.20,
                                sl_pct=0.20,
                                tif='DAY',
                                bracket_tif=bulk_bracket_tif
                            )
                            
                            if trade:
                                if trade.orderStatus.status in ('Cancelled', 'Inactive'):
                                    reason = ""
                                    if trade.log:
                                        for entry in trade.log:
                                            if entry.message:
                                                reason = entry.message
                                                break
                                    err_msg = f"\n> {reason}" if reason else ""
                                    st.error(f"❌ **Order #{idx_num+1} ({symbol} {strat} {expiry}) geweigerd door TWS!** Status: `{trade.orderStatus.status}`{err_msg}")
                                elif trade.orderStatus.status in ('PreSubmitted', 'Submitted', 'Filled'):
                                    st.success(f"✅ **Order #{idx_num+1} ({symbol} {strat} {expiry}) succesvol in TWS orderboek!** Order ID: `{trade.order.orderId}`, Status: `{trade.orderStatus.status}`")
                                    placed_count += 1
                                else:
                                    # Order staat nog even in PendingSubmit: geef TWS extra tijd om te bevestigen
                                    bulk_ib.ib.sleep(1.0)
                                    if trade.orderStatus.status in ('PreSubmitted', 'Submitted', 'Filled'):
                                        st.success(f"✅ **Order #{idx_num+1} ({symbol} {strat} {expiry}) bevestigd door TWS!** Order ID: `{trade.order.orderId}`, Status: `{trade.orderStatus.status}`")
                                        placed_count += 1
                                    else:
                                        st.warning(f"⚠️ **Order #{idx_num+1} ({symbol} {strat} {expiry}) verstuurd**, maar TWS wachtrij-status is nog `{trade.orderStatus.status}`. Controleer TWS.")
                                        placed_count += 1
                            else:
                                st.error(f"❌ **Order #{idx_num+1} ({symbol} {strat}) mislukt:** {getattr(bulk_ib, 'last_error', 'geen antwoord van TWS')}")
                                
                        if placed_count > 0:
                            # CRUCIAAL: wacht 1.5 seconde zodat TWS socket-buffers leegt en alle orders definitief opslaat
                            bulk_ib.ib.sleep(1.5)
                            st.balloons()
                            st.success(f"🎉 In totaal **{placed_count}/{len(selected_rows)}** order(s) succesvol ingediend bij TWS!")
                    except Exception as e:
                        st.error(f"Fout bij order uitvoering: {e}")
                    finally:
                        try:
                            bulk_ib.ib.sleep(0.5)
                        except:
                            pass
                        bulk_ib.disconnect()

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
        # Exporteer in native Excel (.xlsx) formaat voor vlekkeloze weergave in MS Excel
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            results.to_excel(writer, index=False, sheet_name='Scan_Resultaten')
            if 'last_long_scenarios' in st.session_state and st.session_state['last_long_scenarios']:
                try:
                    combined_scenarios = pd.concat(st.session_state['last_long_scenarios'], ignore_index=True)
                    combined_scenarios.to_excel(writer, index=False, sheet_name='ITM_vs_ATM_Scenario')
                except Exception:
                    pass

        file_name = f"{datetime.date.today().strftime('%Y%m%d')} RESULTATEN OPTIE SELECTIE SCAN.xlsx"
        st.download_button(
            label="📊 Download Excel Resultaten (.xlsx)",
            data=buffer.getvalue(),
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
    else:
        st.info("Start een scan of versoepel de filterinstellingen om resultaten te zien.")
        if 'filter_diagnostics' in st.session_state:
            render_filter_diagnostics_ui(st.session_state['filter_diagnostics'], expanded=True)

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

                sell_k = float(selected_row.get('strike_sell', 0.0) or 0.0)
                strat = selected_row['strategy']
                if sell_k > 0 and ('Put' in strat or 'PUT' in strat.upper()):
                    tot_notional = sell_k * 100.0 * order_qty
                    st.warning(
                        f"⚖️ **Vermogenswaarschuwing (Aankoopverplichting)**:\n"
                        f"Bij onverhoopte uitoefening (Assignment) bent u verplicht **{order_qty * 100} aandelen** te kopen tegen ${sell_k:.2f} = **${tot_notional:,.2f}**.\n\n"
                        f"*Tip:* Stem het aantal contracten zorgvuldig af op uw beschikbare vermogen om overmatige hefboomwerking (overleverage) te voorkomen."
                    )
                is_credit = strat not in ['LongCall', 'LongPut', 'BullCall', 'BearPut', 'Strangle', 'SynthCoveredCall', 'SynthCoveredPut']
                tws_action = "SELL" if is_credit else "BUY"
                cashflow_label = "Credit Ontvangen" if is_credit else "Debit Betalen"

                st.info(
                    f"🏷️ **TWS Orderstructuur** (Conform handleiding Roland van Giesen, Pg 1–2):\n"
                    f"- **Strategie**: `{strat}`\n"
                    f"- **Actie in TWS**: **`{tws_action}`** ({cashflow_label})\n"
                    f"- *Uitleg*: Credit spreads (zoals Bull Put & Bear Call) worden geopend met **SELL** om netto premie te ontvangen. Sluiten gebeurt later met **BUY**."
                )

                raw_ask = selected_row.get('spread_ask_abs', 0)
                default_price = float(raw_ask) if raw_ask > 0 else 0.10

                limit_price_input = st.number_input(
                    f"Netto Premie per Aandeel ($) [{cashflow_label}]",
                    min_value=0.01,
                    value=default_price,
                    step=0.01,
                    format="%.2f",
                    help=f"Standaard Laatprijs (${default_price:.2f}). Dit is het netto {'creditbedrag dat je ontvangt' if is_credit else 'debitbedrag dat je betaalt'}."
                )
                limit_price_signed = -limit_price_input if is_credit else limit_price_input

                c_ot1, c_ot2 = st.columns(2)
                with c_ot1:
                    order_type_ui = st.selectbox("Order Type (Executie)", 
                        ["LMT (Standaard Limiet)", "Adaptive - Normal", "Adaptive - Urgent", "Adaptive - Patient"],
                        index=1,
                        help="Kies Adaptive Algo om TWS de beste prijs binnen de spread te laten onderhandelen zonder de max limiet te overschrijden."
                    )
                with c_ot2:
                    order_tif_ui = st.selectbox("Geldigheid Hoofdorder (TIF)", 
                        ["DAY", "GTC"], 
                        index=0, 
                        help="Geldigheid van de instaporder (DAY vervalt aan het einde van de beursdag als deze nog niet gevuld is; GTC blijft actief)."
                    )

                st.markdown("### 🎯 Exit-Plan & Risk Management (Bracket Order)")
                c_brk1, c_brk2 = st.columns([3, 2])
                with c_brk1:
                    single_bracket = st.checkbox("Voeg Automatisch Exit-Plan toe (Take Profit & Stop Loss Orders in TWS)", value=True, key="single_bracket")
                with c_brk2:
                    bracket_tif_ui = st.selectbox("Geldigheid Exit Orders (TIF)", 
                        ["GTC", "DAY"], 
                        index=0, 
                        key="single_bracket_tif",
                        help="GTC (Good 'Til Canceled) zorgt dat de Take Profit en Stop Loss orders doorlopend actief blijven tot ze geraakt worden, ook op volgende beursdagen."
                    ) if single_bracket else "GTC"

                calc_tp_price = None
                calc_sl_price = None

                if single_bracket:
                    bracket_mode = st.radio("Exit Type", ["Dollar Bedrag ($)", "Percentage (%)", "OmniTrader BarToBar (ATR + Coral)", "Exacte Limietprijs ($)"], index=0, horizontal=True)
                    p_entry = abs(float(limit_price_signed)) if limit_price_signed else 0.10
                    qty = int(order_qty)

                    if bracket_mode == "Dollar Bedrag ($)":
                        c_tp, c_sl = st.columns(2)
                        with c_tp:
                            tp_dollar = st.number_input("Winstdoel (Take Profit $)", min_value=5.0, max_value=10000.0, value=200.0, step=10.0, help="Automatisch sluiten bij dit winstbedrag in USD.")
                        with c_sl:
                            sl_dollar = st.number_input("Stop Loss (Max. Verlies $)", min_value=5.0, max_value=10000.0, value=100.0, step=10.0, help="Automatisch sluiten bij dit verliesbedrag in USD.")

                        tp_per_share = (tp_dollar / qty) / 100.0
                        sl_per_share = (sl_dollar / qty) / 100.0

                        if is_credit: # Credit spread: sell to open, buy to close
                            calc_tp_price = max(0.01, round(p_entry - tp_per_share, 2))
                            calc_sl_price = round(p_entry + sl_per_share, 2)
                            st.caption(f"📊 **Exit Plan Preview ({qty}x contract)**:\n- **Take Profit**: Sluit spread zodra prijs $\\le$ **${calc_tp_price:.2f}** (+${tp_dollar:.0f} winst)\n- **Stop Loss**: Sluit spread zodra prijs $\\ge$ **${calc_sl_price:.2f}** (-${sl_dollar:.0f} verlies)")
                        else: # Debit / Long: buy to open, sell to close
                            calc_tp_price = round(p_entry + tp_per_share, 2)
                            calc_sl_price = max(0.01, round(p_entry - sl_per_share, 2))
                            st.caption(f"📊 **Exit Plan Preview ({qty}x contract)**:\n- **Take Profit**: Verkoop contract zodra koers $\\ge$ **${calc_tp_price:.2f}** (+${tp_dollar:.0f} winst)\n- **Stop Loss**: Verkoop contract zodra koers $\\le$ **${calc_sl_price:.2f}** (-${sl_dollar:.0f} verlies)")

                    elif bracket_mode == "Percentage (%)":
                        c_tp, c_sl = st.columns(2)
                        with c_tp:
                            tp_pct_val = st.number_input("Take Profit %", min_value=5.0, max_value=500.0, value=20.0, step=5.0) / 100.0
                        with c_sl:
                            sl_pct_val = st.number_input("Stop Loss %", min_value=5.0, max_value=100.0, value=20.0, step=5.0) / 100.0

                        if is_credit:
                            calc_tp_price = max(0.01, round(p_entry * (1.0 - tp_pct_val), 2))
                            calc_sl_price = round(p_entry * (1.0 + sl_pct_val), 2)
                        else:
                            calc_tp_price = round(p_entry * (1.0 + tp_pct_val), 2)
                            calc_sl_price = max(0.01, round(p_entry * (1.0 - sl_pct_val), 2))
                        st.caption(f"📊 **Exit Plan Preview**:\n- **Take Profit Order**: ${calc_tp_price:.2f}\n- **Stop Loss Order**: ${calc_sl_price:.2f}")

                    elif bracket_mode == "OmniTrader BarToBar (ATR + Coral)":
                        st.markdown("##### 🛡️ OmniTrader BarToBar Instellingen")
                        c_omni1, c_omni2, c_omni3 = st.columns(3)
                        with c_omni1:
                            init_mult_ui = st.number_input("Init Mult (ATR)", min_value=0.5, max_value=15.0, value=7.0, step=0.5)
                        with c_omni2:
                            atr_period_ui = st.number_input("ATR Periodes", min_value=3, max_value=30, value=7, step=1)
                        with c_omni3:
                            p_factor_ui = st.number_input("P Factor", min_value=0.1, max_value=2.0, value=0.4, step=0.05)
                        
                        und_p = selected_row.get('stock_price', selected_row.get('underlying_price', 100.0))
                        est_atr = und_p * 0.015
                        stop_dist = init_mult_ui * est_atr

                        if is_credit:
                            calc_tp_price = max(0.01, round(p_entry * 0.50, 2))
                            calc_sl_price = round(p_entry + (stop_dist / 100.0), 2)
                        else:
                            calc_tp_price = round(p_entry * 1.50, 2)
                            calc_sl_price = max(0.01, round(p_entry - (stop_dist / 100.0), 2))
                        
                        st.caption(f"📊 **OmniTrader BarToBar Preview**:\n- **Take Profit Order**: ${calc_tp_price:.2f}\n- **Dynamic BarToBar Stop**: ${calc_sl_price:.2f} (Startwaarde op {init_mult_ui}x ATR + Coral Trend filter)")

                    else: # Exacte Limietprijs
                        c_tp, c_sl = st.columns(2)
                        with c_tp:
                            calc_tp_price = st.number_input("Take Profit Limietprijs ($)", value=round(p_entry*1.2, 2), step=0.05)
                        with c_sl:
                            calc_sl_price = st.number_input("Stop Loss Limietprijs ($)", value=round(p_entry*0.8, 2), step=0.05)

                # Dynamic Profit Projection
                worst_entry = selected_row.get('worst_entry_signed', 0.0)
                base_winst = selected_row.get('winst_laat', 0.0)
                
                # Difference in price * 100 * contract qty
                winst_verschuiving = (worst_entry - limit_price_signed) * 100
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
                                live_data = refresh_client.get_chain_greeks_and_oi(selected_row['symbol'], selected_row['expiry'], strikes_to_check, use_yf=use_free_data)
                                
                                if not live_data.empty:
                                    import numpy as np
                                    strat_type = selected_row['strategy']
                                    
                                    def get_leg_prices(strike, right):
                                        leg_row = live_data[(live_data['strike'] == float(strike)) & (live_data['right'] == right)]
                                        if not leg_row.empty:
                                            r = leg_row.iloc[0]
                                            b = r.get('bid', 0.0)
                                            a = r.get('ask', 0.0)
                                            m = r.get('mid', 0.0)
                                            opt_p = r.get('opt_price', 0.0)
                                            if np.isnan(b) or b < 0: b = 0.0
                                            if np.isnan(a) or a < 0: a = 0.0
                                            mid = (b + a) / 2 if (b > 0 and a > 0) else (m if m > 0 else opt_p)
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
                                    strat_is_credit = strat_type not in ['LongCall', 'LongPut', 'BullCall', 'BearPut', 'Strangle', 'SynthCoveredCall', 'SynthCoveredPut']
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
                                'strike_c_buy': selected_row.get('strike_c_buy', 0),
                                'expiry_long': selected_row.get('expiry_long', selected_row.get('expiry')),
                                'expiry': selected_row.get('expiry')
                            }

                            # Determine Overall Action
                            # Since ib_client.py explicitly defines the legs representing the final position we want,
                            # we must always BUY the combination. Credit spreads will use a negative limit price.
                            strat = selected_row['strategy']
                            right_val = selected_row.get('right', '')
                            if not right_val or right_val not in ['C', 'P']:
                                if any(k in strat for k in ['Put', 'PUT', 'put']):
                                    right_val = 'P'
                                else:
                                    right_val = 'C'
                                    
                            action = tws_action
                            limit_price_val = abs(float(limit_price_signed)) if limit_price_signed else 0.10

                            st.write(f"Plaatsen order ({strat}) voor {order_qty} stuks. TWS Actie: **{action}** ({cashflow_label}, Premie: ${limit_price_val:.2f})...")

                            trade = order_ib.place_strategy_order(
                                symbol=selected_row['symbol'],
                                expiry=selected_row['expiry'],
                                right=right_val,
                                strategy=strat,
                                strikes_dict=strikes_dict,
                                action=action,
                                quantity=order_qty,
                                price=limit_price_val,
                                order_type=order_type_ui,
                                enable_bracket=single_bracket,
                                custom_tp_price=calc_tp_price,
                                custom_sl_price=calc_sl_price,
                                tif=order_tif_ui,
                                bracket_tif=bracket_tif_ui
                            )

                            if trade:
                                st.success(f"✅ **Order ingediend bij TWS!**")
                                st.markdown(f"**Status:** `{trade.orderStatus.status}`")
                                st.markdown(f"**Order ID:** `{trade.order.orderId}`")

                                st.warning("⚠️ **Belangrijk:** Controleer in TWS het tabblad **'Orders'**, niet 'Transacties'. Transacties verschijnen pas na uitvoering (Fill).")

                                with st.expander("🔍 Technische Details (voor verificatie in TWS)"):
                                    st.write(f"**Symbool:** {selected_row['symbol']}")
                                    st.write(f"**Strategie:** {strat}")
                                    st.write(f"**TWS Actie:** {action} ({cashflow_label})")
                                    st.write(f"**Netto Premie:** ${limit_price_val:.2f}")
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
                                last_err = getattr(order_ib, 'last_error', '')
                                err_suffix = f": {last_err}" if last_err else " (geen antwoord van TWS)"
                                st.error(f"❌ Order plaatsen mislukt{err_suffix}.")

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
    
    st.info("💡 **Tip**: Wil je de S&P 500 scannen op zoek naar winstgevende optie-spreads of 1% ATM opties? Kies in de linker Sidebar bij **Scan Modus** voor **'Batch Scan (Lijst)' -> S&P 500 (Wikipedia)** of **'Super-Fast ATM Long Scan (1% Koop)'** en klik op **'Start Scan'** in Tab 1 (Scanner).")
    col_t4_act, _ = st.columns([2, 3])
    with col_t4_act:
        if st.button("🚀 Activeer S&P 500 direct voor Tab 1 (Scanner)", key="btn_activate_sp500_scanner"):
            st.session_state['scan_mode_choice'] = "Batch Scan (Lijst)"
            st.session_state['batch_list_choice'] = "S&P 500 (Wikipedia)"
            st.success("✅ S&P 500 (Wikipedia) lijst succesvol geactiveerd voor Tab 1! Schakel over naar Tab 1 om de scan te starten.")
            st.rerun()

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
            import random
            from ib_insync import Stock
            
            status_text = st.empty()
            progress_bar = st.progress(0)
            
            symbols = []
            if scan_method == "S&P 500 (Wikipedia)":
                symbols = get_sp500_symbols(include_etfs=False, for_ib=True)
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
                        dtype = selected_dtype
                        export_ib.set_data_type(dtype)
                        
                        status_text.text(f"Start ophalen {len(symbols)} symbolen via TWS...")
                        results_data = []
                        
                        for i, sym in enumerate(symbols):
                            progress_bar.progress(i / len(symbols))
                            status_text.text(f"Analyseren: {sym} ({i+1}/{len(symbols)})")
                            
                            try:
                                contract = Stock(sym, 'SMART', 'USD')
                                price_data = export_ib.get_market_data_snapshot(contract, use_hist_fallback=False, use_yf=use_free_data)
                                price = price_data.get('price', 0.0)
                                
                                if price <= 0:
                                    continue
                                
                                chains = export_ib.get_option_chains_params(sym, sec_type='STK', use_yf=use_free_data)
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
                                otm_call_target = price * 1.15
                                itm_put_target = price * 1.15
                                otm_put_target = price * 0.85
                                
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
                                target_strikes_set.update(find_closest_strikes(otm_call_target))
                                target_strikes_set.update(find_closest_strikes(itm_put_target))
                                target_strikes_set.update(find_closest_strikes(otm_put_target))
                                target_strikes = sorted(list(target_strikes_set))
                                
                                chosen_exp = None
                                chain_data = pd.DataFrame()
                                # Probeer maximaal de eerste 4 expiraties tot we er één vinden met actieve Bied/Laat prijzen
                                for attempt_exp in valid_exps[:4]:
                                    temp_data = export_ib.get_chain_greeks_and_oi(sym, attempt_exp, target_strikes, use_yf=use_free_data)
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
                                
                                # Call Spread (Poot 1: ITM Call, Poot 2: OTM Call)
                                itm_c_strk, itm_c_b, itm_c_a, _ = find_spread(chain_data, itm_call_target, 'C')
                                otm_c_strk, otm_c_b, otm_c_a, _ = find_spread(chain_data, otm_call_target, 'C')
                                
                                # Put Spread (Poot 1: ITM Put, Poot 2: OTM Put)
                                itm_p_strk, itm_p_b, itm_p_a, _ = find_spread(chain_data, itm_put_target, 'P')
                                otm_p_strk, otm_p_b, otm_p_a, _ = find_spread(chain_data, otm_put_target, 'P')
                                
                                # Excel row index (1-based, +1 for header, dus len + 2)
                                r = len(results_data) + 2
                                
                                results_data.append({
                                    'Symbol': sym, 'Price': price, 'Expiration': chosen_exp,
                                    'ATM_Call_Strike': atm_c_strk, 'ATM_Call_Bid': atm_c_b, 'ATM_Call_Ask': atm_c_a, 'ATM_Call_Spread': f"=MAX(0, F{r}-E{r})",
                                    'ATM_Put_Strike': atm_p_strk, 'ATM_Put_Bid': atm_p_b, 'ATM_Put_Ask': atm_p_a, 'ATM_Put_Spread': f"=MAX(0, J{r}-I{r})",
                                    'ITM_Call_Strike': itm_c_strk, 'ITM_Call_Bid': itm_c_b, 'ITM_Call_Ask': itm_c_a, 'ITM_Call_Spread': f"=MAX(0, N{r}-M{r})",
                                    'OTM_Call_Strike': otm_c_strk, 'OTM_Call_Bid': otm_c_b, 'OTM_Call_Ask': otm_c_a, 'OTM_Call_Spread': f"=MAX(0, R{r}-Q{r})",
                                    'ITM_Put_Strike': itm_p_strk, 'ITM_Put_Bid': itm_p_b, 'ITM_Put_Ask': itm_p_a, 'ITM_Put_Spread': f"=MAX(0, V{r}-U{r})",
                                    'OTM_Put_Strike': otm_p_strk, 'OTM_Put_Bid': otm_p_b, 'OTM_Put_Ask': otm_p_a, 'OTM_Put_Spread': f"=MAX(0, Z{r}-Y{r})",
                                    'Bull_Call_Spread_Prijs': f"=MAX(0, N{r}-Q{r})",
                                    'Bull_Put_Spread_Credit': f"=MAX(0, U{r}-Z{r})"
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
                div_ib.set_data_type(selected_dtype)
                progress_bar_div = st.progress(0)
                status_text_div = st.empty()
                
                try:
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
                                mkt = div_ib.get_market_data_snapshot(contract, use_hist_fallback=True, use_yf=use_free_data)
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
                                    chains = div_ib.get_option_chains_params(sym, sec_type=sec_type_str, use_yf=use_free_data)
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
                                            greeks = div_ib.get_chain_greeks_and_oi(sym, real_exp, [final_strike], use_yf=use_free_data)
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
                    st.dataframe(df_div, width='stretch', hide_index=True)
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


# --- TAB 6: HIT-RATE TEST & MAANDELIJKSE VALIDATIE ---
with tab6:
    st.header("🧪 Spread Hit-Rate Validatietest & Maandelijkse Check")
    st.markdown("""
    Valideer de prestaties van de huidige **Spread Selector instellingen** op een dataset van **25 historische gelopen spreads** (5 per aandeel over 5 bekende benchmark indices/aandelen).
    Dit geeft u hard wiskundig en statistisch bewijs van de **werkelijke winstratio (Hit Rate %)**, de **EM85 veiligheidsscore** en de **totale winstgevendheid**.
    """)
    
    col_hr1, col_hr2 = st.columns([2.2, 1])
    with col_hr1:
        preset_symbols = ['SPY', 'QQQ', 'IWM', 'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AMD', 'NFLX', 'PLTR', 'XLV', 'XLF', 'XLE', 'SMH', 'GLD', 'SLV', 'JPM', 'BA']
        
        sym_input_mode = st.radio(
            "Kies Invoermethode voor Symbolen:",
            ["✍️ Eigen Symbolen (Direct typen/plakken)", "📋 Kiezen uit Benchmark Lijst", "📡 Overnemen uit Scanner (Tab 1)"],
            index=0,
            horizontal=True
        )

        if sym_input_mode == "✍️ Eigen Symbolen (Direct typen/plakken)":
            c_text_col1, c_text_col2 = st.columns([3.5, 1.2])
            with c_text_col1:
                custom_input = st.text_input(
                    "Voer eigen symbolen in (gescheiden door komma's of spaties):",
                    value=st.session_state.get('custom_symbols_text', "ACNB, DRTS, MRK, NESR, RVMD, WELL"),
                    placeholder="bijv. ACNB, DRTS, MRK, NESR, RVMD, WELL",
                    key="hr_custom_input_field"
                )
                st.session_state['custom_symbols_text'] = custom_input
            with c_text_col2:
                st.write("")
                st.write("")
                if st.button("➕ Laad Symbolen", width='stretch', key="btn_reload_syms"):
                    st.rerun()

            import re
            raw_tokens = re.split(r'[\s,;]+', custom_input.strip().upper()) if custom_input else []
            test_symbols = [t.strip() for t in raw_tokens if t.strip()]

        elif sym_input_mode == "📋 Kiezen uit Benchmark Lijst":
            dynamic_options = list(preset_symbols)
            if 'custom_symbols_text' in st.session_state and st.session_state['custom_symbols_text']:
                import re
                for t in re.split(r'[\s,;]+', st.session_state['custom_symbols_text'].strip().upper()):
                    if t and t not in dynamic_options:
                        dynamic_options.append(t)

            selected_presets = st.multiselect(
                "Selecteer Aandelen / Indices uit de lijst:",
                options=dynamic_options,
                default=['SPY', 'AAPL', 'MSFT', 'NVDA', 'QQQ'],
                help="Kies 1 aandeel (bijv. enkel NVDA), 3, 5, 10 of meer aandelen voor de test."
            )
            test_symbols = list(selected_presets)

        else: # Overnemen uit Scanner
            scanner_syms = []
            if 'results' in st.session_state and not st.session_state['results'].empty and 'symbol' in st.session_state['results'].columns:
                scanner_syms = list(st.session_state['results']['symbol'].unique())
            elif 'symbols_to_scan' in locals() and symbols_to_scan:
                scanner_syms = list(symbols_to_scan)

            if scanner_syms:
                st.success(f"📡 {len(scanner_syms)} symbolen overgenomen uit Scanner: `{', '.join(scanner_syms[:8])}`...")
                test_symbols = scanner_syms
            else:
                st.info("ℹ️ Nog geen scanresultaten in Tab 1. Standaard selectie geladen.")
                test_symbols = ['SPY', 'QQQ', 'AAPL', 'MSFT', 'NVDA']

    with col_hr2:
        trades_per_sym = st.number_input("Aantal spreads per aandeel", min_value=1, max_value=20, value=5)
        target_strat_choice = st.selectbox(
            "🎯 Strategie Validatie Filter:",
            options=[
                "Automatisch (Trend-afhankelijk)", 
                "Enkel BullCall", 
                "Enkel BullPut", 
                "Enkel BearCall", 
                "Enkel BearPut",
                "Enkel LongCall",
                "Enkel LongPut",
                "Enkel ShortPut"
            ],
            index=0,
            help="Kies een specifieke strategie om de test uitsluitend voor die strategie (bijv. enkel BullCall, LongCall of losse ShortPut) uit te voeren."
        )

    n_syms = len(test_symbols)
    total_test_trades = n_syms * trades_per_sym

    if n_syms == 0:
        st.warning("⚠️ Voer ten minste 1 aandeel in (bijv. ACNB, MRK of SPY) of selecteer uit de lijst om de test te starten.")
    else:
        st.info(f"📊 **Test Configuratie**: {total_test_trades} spreads over **{n_syms} aandeel/aandelen**: `{', '.join(test_symbols)}` ({trades_per_sym} spreads per aandeel) | Strategie: **{target_strat_choice}**")

    col_act1, col_act2 = st.columns(2)
    with col_act1:
        start_hitrate_btn = st.button(
            f"🧪 Start Standaard Hit-Rate Test ({total_test_trades} Spreads)", 
            type="secondary",
            width='stretch',
            disabled=False
        )
    with col_act2:
        start_comp_btn = st.button(
            f"⚖️ Test & Optimaliseer: Sidebar vs. Standaard ({total_test_trades} Spreads)", 
            type="primary",
            width='stretch',
            disabled=False,
            help="Test uw huidige sidebar-instellingen (DTE, breedte, EM) tegen de standaard benchmark over dezelfde aandelen en optimaliseer direct."
        )

    if st.session_state.get('run_comparison_test_trigger', False):
        start_comp_btn = True
        st.session_state['run_comparison_test_trigger'] = False

    target_strat_key = 'AUTO' if "Automatisch" in target_strat_choice else target_strat_choice.replace("Enkel ", "").strip()

    if start_comp_btn and n_syms > 0:
        from hitrate_backtester import SpreadHitRateTester
        tester = SpreadHitRateTester()
        
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        log_box = st.expander("Bekijk optimalisatie test-logboeken", expanded=True)
        log_messages = []
        
        def comp_progress(pct, msg):
            progress_bar.progress(pct)
            status_text.text(msg)
            
        def comp_log(msg):
            log_messages.append(msg)
            with log_box:
                st.text(msg)
                
        sb_avg_dte = max(10, (int(min_dte) + int(max_dte)) // 2)
        sb_width = float(width)
        if "Niveau 1" in itm_support_level:
            sb_em_mult = 1.0
        elif "Niveau 2" in itm_support_level:
            sb_em_mult = 2.0
        elif "Niveau 3" in itm_support_level:
            sb_em_mult = 2.5
        else:
            sb_em_mult = 1.439535

        with st.spinner(f"Vergelijkingstest wordt uitgevoerd voor {n_syms} aandeel/aandelen (Sidebar vs. Standaard)..."):
            comp_dict = tester.compare_sidebar_vs_standard(
                symbols=test_symbols,
                trades_per_symbol=trades_per_sym,
                sidebar_params={
                    'dte': sb_avg_dte,
                    'spread_width': sb_width,
                    'em_multiplier': sb_em_mult,
                    'target_strategy': target_strat_key,
                    'name': f"Sidebar ({sb_avg_dte}d / ${sb_width:.0f} / {sb_em_mult:.2f}x)"
                },
                standard_params={
                    'dte': 30,
                    'spread_width': 5.0,
                    'em_multiplier': 1.439535,
                    'target_strategy': 'AUTO',
                    'name': "Standaard (30d / $5.0 / 1.44x)"
                },
                progress_callback=comp_progress,
                log_callback=comp_log
            )
            
        st.session_state['comparison_results'] = comp_dict
        st.session_state['optimal_stock_configs'] = comp_dict['stock_profiles']
        st.success(f"✅ Vergelijkingstest & Optimalisatie over {n_syms} aandeel/aandelen voltooid!")

    if start_hitrate_btn and n_syms > 0:
        from hitrate_backtester import SpreadHitRateTester
        tester = SpreadHitRateTester()
        
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        log_box = st.expander("Bekijk gedetailleerde test-logboeken", expanded=True)
        log_messages = []
        
        def hr_progress(pct, msg):
            progress_bar.progress(pct)
            status_text.text(msg)
            
        def hr_log(msg):
            log_messages.append(msg)
            with log_box:
                st.text(msg)
                
        with st.spinner(f"Hit-Rate test wordt uitgevoerd voor {n_syms} aandeel/aandelen op historische marktdata..."):
            res_dict = tester.run_backtest(
                symbols=test_symbols,
                trades_per_symbol=trades_per_sym,
                target_strategy=target_strat_key,
                progress_callback=hr_progress,
                log_callback=hr_log
            )
            
        st.session_state['hitrate_results'] = res_dict
        st.success(f"✅ Hit-Rate test over {n_syms} aandeel/aandelen ({total_test_trades} spreads) succesvol voltooid!")

    if 'comparison_results' in st.session_state and st.session_state['comparison_results'].get('comparison_df') is not None:
        cres = st.session_state['comparison_results']
        df_comp = cres['comparison_df']
        sb_sum = cres['sidebar_summary']
        std_sum = cres['standard_summary']
        glob_best = cres['global_best']
        pnl_diff = cres['global_pnl_diff']

        st.markdown("---")
        st.subheader("⚖️ Resultaten Vergelijkingstest: Sidebar vs. Standaard Benchmark")
        
        if glob_best == "Standaard":
            st.warning(f"🏆 **Standaard Benchmark is Globaal Winstgevender!** Gemiddelde winst: **${std_sum.get('avg_pnl', 0):.2f} / trade** (Standaard) vs. **${sb_sum.get('avg_pnl', 0):.2f} / trade** (Sidebar). Winstverschil: **+${pnl_diff:.2f} / trade** ten gunste van de Standaard Benchmark.")
        elif glob_best == "Sidebar":
            st.success(f"🚀 **Uw Huidige Sidebar Instellingen zijn Globaal Winstgevender!** Gemiddelde winst: **${sb_sum.get('avg_pnl', 0):.2f} / trade** (Sidebar) vs. **${std_sum.get('avg_pnl', 0):.2f} / trade** (Standaard). Winstverschil: **+${pnl_diff:.2f} / trade** ten gunste van uw Sidebar.")
        else:
            st.info("⚖️ **Gelijkwaardige Prestaties**: Beide configuraties leveren vrijwel dezelfde gemiddelde winst op.")

        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric(
            "Gem. Winst / Trade",
            f"${std_sum.get('avg_pnl', 0):.2f} (Std)",
            f"${sb_sum.get('avg_pnl', 0):.2f} (Sidebar)"
        )
        mc2.metric(
            "Hit Rate (%)",
            f"{std_sum.get('hit_rate', 0):.1f}% (Std)",
            f"{sb_sum.get('hit_rate', 0):.1f}% (Sidebar)"
        )
        mc3.metric(
            "Totale Winst Portfolio",
            f"${std_sum.get('total_pnl', 0):,.2f} (Std)",
            f"${sb_sum.get('total_pnl', 0):,.2f} (Sidebar)"
        )
        mc4.metric(
            "Geen-BEP-Touch Rate",
            f"{std_sum.get('em85_safe_rate', 0):.1f}% (Std)",
            f"{sb_sum.get('em85_safe_rate', 0):.1f}% (Sidebar)"
        )

        st.markdown("### 🔄 1-Klik Synchronisatie & Optimalisatie")

        if 'sidebar_update_feedback' in st.session_state:
            fb_type, fb_msg = st.session_state.pop('sidebar_update_feedback')
            if fb_type == "success":
                st.success(fb_msg)
            else:
                st.info(fb_msg)

        act_col1, act_col2 = st.columns(2)
        with act_col1:
            btn_label = "⚡ Pas Standaard Benchmark Toe op de Linker Sidebar" if glob_best == "Standaard" else "⚡ Pas Beste Instellingen Toe op de Linker Sidebar"
            if st.button(btn_label, type="primary", width='stretch', key="btn_apply_best_global"):
                st.session_state['pending_sidebar_updates'] = {
                    'sb_width': 5,
                    'sb_min_dte': 20,
                    'sb_max_dte': 35,
                    'sb_itm_support': "EM85 Optimaal (1.44x Expected Move)",
                    'preset_min_strike': 8.0
                }
                if glob_best == "Standaard":
                    st.session_state['sidebar_update_feedback'] = ("success", "✅ Sidebar filters direct bijgewerkt naar Standaard Benchmark waarden (Breedte: $5.00, DTE: 20-35, EM85)!")
                else:
                    st.session_state['sidebar_update_feedback'] = ("info", "ℹ️ Sidebar filters bijgewerkt naar Standaard Benchmark waarden (Breedte: $5.00, DTE: 20-35, EM85).")
                st.rerun()

        with act_col2:
            use_profiles = st.checkbox(
                "🎯 Gebruik Aandeel-Specifieke Instellingen in Scanner",
                value=st.session_state.get('use_stock_profiles', True),
                key="chk_use_profiles",
                help="Wanneer ingeschakeld, gebruikt de live scanner in Tab 1 voor elk individueel aandeel automatisch de winnende instelling uit deze test!"
            )
            st.session_state['use_stock_profiles'] = use_profiles
            if use_profiles:
                st.caption(f"✅ Scanner hanteert nu per aandeel het hoogst renderende profiel ({len(cres['stock_profiles'])} aandelen).")

        st.markdown("### 📋 Resultaten & Advies per Aandeel")
        st.dataframe(
            df_comp,
            width='stretch',
            column_config={
                "symbol": "Aandeel",
                "sb_avg_pnl": st.column_config.NumberColumn("Sidebar Winst/Trade", format="$%.2f"),
                "sb_hit_rate": st.column_config.NumberColumn("Sidebar Hit Rate", format="%.1f%%"),
                "sb_total_pnl": st.column_config.NumberColumn("Sidebar Totaal ($)", format="$%.2f"),
                "std_avg_pnl": st.column_config.NumberColumn("Standaard Winst/Trade", format="$%.2f"),
                "std_hit_rate": st.column_config.NumberColumn("Standaard Hit Rate", format="%.1f%%"),
                "std_total_pnl": st.column_config.NumberColumn("Standaard Totaal ($)", format="$%.2f"),
                "winner": "Beste Keuze",
                "advies": "Advies & Winstverbetering"
            }
        )

        import io
        buf_comp = io.BytesIO()
        with pd.ExcelWriter(buf_comp, engine='openpyxl') as writer:
            df_comp.to_excel(writer, index=False, sheet_name='Vergelijking_Per_Aandeel')
            if not cres['sidebar_details'].empty:
                cres['sidebar_details'].to_excel(writer, index=False, sheet_name='Trades_Sidebar')
            if not cres['standard_details'].empty:
                cres['standard_details'].to_excel(writer, index=False, sheet_name='Trades_Standaard')

        st.download_button(
            label="💾 Download Vergelijkingsrapport (Excel)",
            data=buf_comp.getvalue(),
            file_name=f"Optimalisatie_Sidebar_vs_Standaard_{datetime.date.today().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="secondary"
        )
            
    if 'hitrate_results' in st.session_state and st.session_state['hitrate_results'].get('summary'):
        res = st.session_state['hitrate_results']
        df_details = res['details_df']
        
        st.markdown("---")
        st.subheader("🎯 Interactiviteit: Filter Hit-Rate Specifiek per Aandeel & Strategie")
        st.caption("Gebruik onderstaande filters om de Hit-Rate, PoP en PnL statistieken uitsluitend te herberekenen voor een gekozen aandeel en strategie (bijv. uitsluitend BullCall op NVDA).")

        f_col1, f_col2, f_col3 = st.columns([1.5, 1.5, 1.2])
        with f_col1:
            all_sym_options = ["Alle Symbolen"] + sorted(list(df_details['symbol'].unique()))
            filter_sym = st.selectbox("Aandeel Filter:", options=all_sym_options, index=0, key="hr_filter_sym")
        with f_col2:
            all_strat_options = ["Alle Strategieën"] + sorted(list(df_details['strategy'].unique()))
            filter_strat = st.selectbox("Strategie Filter:", options=all_strat_options, index=0, key="hr_filter_strat")
        with f_col3:
            only_win_chk = st.checkbox("Enkel Gewonnen Trades", value=False, key="hr_only_win")

        # Apply filtering on the result dataframe
        df_filtered = df_details.copy()
        if filter_sym != "Alle Symbolen":
            df_filtered = df_filtered[df_filtered['symbol'] == filter_sym]
        if filter_strat != "Alle Strategieën":
            df_filtered = df_filtered[df_filtered['strategy'] == filter_strat]
        if only_win_chk:
            df_filtered = df_filtered[df_filtered['win'] == True]

        if not df_filtered.empty:
            tot_cnt = len(df_filtered)
            win_cnt = int(df_filtered['win'].sum())
            hr_pct = round((win_cnt / tot_cnt) * 100.0, 1)
            pop_avg = round(float(df_filtered['pop'].mean()), 1)
            safe_cnt = int(df_filtered['em85_safe'].sum())
            safe_pct = round((safe_cnt / tot_cnt) * 100.0, 1)
            tot_pnl = round(float(df_filtered['realized_pnl'].sum()), 2)
            avg_pnl = round(float(df_filtered['realized_pnl'].mean()), 2)
        else:
            tot_cnt, win_cnt, hr_pct, pop_avg, safe_pct, tot_pnl, avg_pnl = 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0

        st.subheader(f"📊 Testresultaten Samenvatting ({filter_sym} | {filter_strat})")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Werkelijke Hit Rate", f"{hr_pct}%", f"{win_cnt}/{tot_cnt} Gewonnen")
        m2.metric("Verwachte Kans (PoP)", f"{pop_avg}%", f"Verschil: {round(hr_pct - pop_avg, 1)}%")
        m3.metric("EM85 Safe Rate", f"{safe_pct}%", "Geen BEP Touch")
        m4.metric("Totale Realiseerde Winst", f"${tot_pnl:,.2f}", f"Gem. ${avg_pnl:.2f} / trade")
        
        st.markdown("### 📋 Overzicht van de Gelopen Spreads (Gefilterd)")
        
        # Configure columns for display
        st.dataframe(
            df_filtered,
            width='stretch',
            column_config={
                "symbol": "Aandeel",
                "entry_date": "Entry Datum",
                "exp_date": "Expiratie",
                "strategy": "Strategie",
                "underlying_entry": st.column_config.NumberColumn("Koers In", format="$%.2f"),
                "underlying_exp": st.column_config.NumberColumn("Koers Uit", format="$%.2f"),
                "optie_strike": st.column_config.TextColumn("Optie Strike", help="Gekozen optiecontract strike(s)"),
                "short_strike": st.column_config.NumberColumn("Short Strike", format="$%.2f"),
                "long_strike": st.column_config.NumberColumn("Long Strike", format="$%.2f"),
                "bep": st.column_config.NumberColumn("BEP", format="$%.2f"),
                "credit": st.column_config.NumberColumn("Premie ($)", format="$%.2f", help="Ontvangen credit (+) of betaalde debit (-)"),
                "EM68": st.column_config.NumberColumn("EM68", format="$%.2f"),
                "EM85": st.column_config.NumberColumn("EM85", format="$%.2f"),
                "em85_dekking_pct": st.column_config.NumberColumn("EM85 Dekking", format="%.1f%%"),
                "pop": st.column_config.NumberColumn("PoP", format="%.1f%%"),
                "status": "Resultaat",
                "realized_pnl": st.column_config.NumberColumn("Winst ($)", format="$%.2f")
            }
        )
        
        # Export button for hitrate report
        import io
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_details.to_excel(writer, index=False, sheet_name='HitRate_Backtest')
            
        st.download_button(
            label="💾 Download Maandelijks Hit-Rate Rapport (Excel)",
            data=buffer.getvalue(),
            file_name=f"Maandelijkse_HitRate_Test_{datetime.date.today().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )

    st.markdown("---")
    st.subheader("🔍 Sequentiële EM Multiplier Optimalisatie Engine")
    st.markdown("""
    Test de prestaties van **verschillende EM veiligheidsmultipliers** ($0.80\\times$ tot $2.00\\times \\text{EM68}$) om exact de **meest winstgevende sweet spot** te bepalen op basis van de volatiliteit.
    - **EM68** ($1.00\\times \\text{EM68}$): $68.3\\%$ wiskundig bereik
    - **EM85** ($1.44\\times \\text{EM68}$): $85.0\\%$ wiskundig bereik (Standaard AntiGravity instelling)
    - **Diepe Spreads** ($1.65\\times - 2.00\\times \\text{EM68}$): Extreem hoge veiligheidsmarge
    """)

    opt_btn = st.button("🔍 Start Sequentiële EM Optimalisatie Sweep", type="secondary")

    if opt_btn:
        from hitrate_backtester import SpreadHitRateTester
        tester = SpreadHitRateTester()

        prog_opt = st.progress(0.0)
        status_opt = st.empty()

        def opt_progress(pct, msg):
            prog_opt.progress(pct)
            status_opt.text(msg)

        with st.spinner("Sequentiële optimalisatie sweep uitvoeren over 7 EM-niveaus..."):
            opt_res = tester.optimize_em_multipliers(
                symbols=test_symbols if test_symbols else ['SPY', 'AAPL', 'MSFT', 'NVDA', 'QQQ'],
                trades_per_symbol=trades_per_sym,
                multipliers=[0.8, 1.0, 1.2, 1.44, 1.65, 1.8, 2.0],
                progress_callback=opt_progress
            )

        st.session_state['opt_results'] = opt_res
        st.success("✅ Sequentiële optimalisatie sweep voltooid!")

    if 'opt_results' in st.session_state and st.session_state['opt_results']:
        opt_res = st.session_state['opt_results']
        df_sweep = opt_res['sweep_df']

        st.success(f"🎯 **Optimum Gevonden**: Multiplier **{opt_res['best_multiplier']:.2f}x EM68** (EM85 zone) levert de hoogste winst op van **${opt_res['best_pnl']:.2f} per trade** met een Hit Rate van **{opt_res['best_hit_rate']}%**!")

        # Interactive Plotly Chart
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(
                x=df_sweep['multiplier_name'], 
                y=df_sweep['avg_pnl'], 
                name="Gem. Winst / Trade ($)",
                line=dict(color='#00CC96', width=3),
                mode='lines+markers'
            ),
            secondary_y=False
        )
        fig.add_trace(
            go.Scatter(
                x=df_sweep['multiplier_name'], 
                y=df_sweep['hit_rate'], 
                name="Hit Rate (%)",
                line=dict(color='#636EFA', width=3, dash='dash'),
                mode='lines+markers'
            ),
            secondary_y=True
        )

        fig.update_layout(
            title_text="📈 EM Multiplier vs. Gemiddelde Winst & Hit Rate",
            hovermode="x unified",
            template="plotly_dark"
        )
        fig.update_xaxes(title_text="EM Veiligheids-Multiplier")
        fig.update_yaxes(title_text="<b>Gemiddelde Winst ($)</b>", secondary_y=False)
        fig.update_yaxes(title_text="<b>Hit Rate (%)</b>", secondary_y=True)

        st.plotly_chart(fig, width='stretch')

        st.markdown("#### 📊 Overzichtstabel per Multipliersweep")
        st.dataframe(
            df_sweep[['multiplier_name', 'hit_rate', 'avg_pop', 'avg_credit', 'avg_pnl', 'total_pnl', 'em85_safe_rate']],
            width='stretch',
            column_config={
                "multiplier_name": "EM Multiplier",
                "hit_rate": st.column_config.NumberColumn("Hit Rate", format="%.1f%%"),
                "avg_pop": st.column_config.NumberColumn("Gem. PoP", format="%.1f%%"),
                "avg_credit": st.column_config.NumberColumn("Gem. Credit", format="$%.2f"),
                "avg_pnl": st.column_config.NumberColumn("Gem. Winst / Trade", format="$%.2f"),
                "total_pnl": st.column_config.NumberColumn("Totale Winst (25 Spreads)", format="$%.2f"),
                "em85_safe_rate": st.column_config.NumberColumn("Geen Touch Rate", format="%.1f%%")
            }
        )