import datetime
import numpy as np
import pandas as pd
import yfinance as yf

def round_to_strike(price):
    """
    Rounds a calculated price to a realistic US exchange option strike:
    - price < 25: $0.50 intervals (e.g. 10.0, 10.5, 11.0...)
    - 25 <= price < 100: $1.00 intervals (e.g. 48.0, 49.0, 50.0, 51.0...)
    - 100 <= price < 250: $2.50 intervals (e.g. 150.0, 152.5, 155.0...)
    - price >= 250: $5.00 intervals (e.g. 400.0, 405.0, 410.0...)
    """
    if price is None or pd.isna(price) or price <= 0:
        return 0.0
    if price < 25.0:
        return round(round(price * 2.0) / 2.0, 2)
    elif price < 100.0:
        return float(round(price))
    elif price < 250.0:
        return round(round(price / 2.5) * 2.5, 2)
    else:
        return round(round(price / 5.0) * 5.0, 2)

class SpreadHitRateTester:
    """
    Backtests historical spreads across liquid benchmark stocks to evaluate
    actual hit rate, EM68/EM85 safety coverage, total PnL performance, and 
    sequentially optimizes EM multipliers based on volatility regimes.
    """
    def __init__(self):
        pass

    def run_backtest(self, symbols=['SPY', 'AAPL', 'MSFT', 'NVDA', 'QQQ'], trades_per_symbol=5, em_multiplier=1.439535, target_strategy='AUTO', dte=30, spread_width=5.0, progress_callback=None, log_callback=None):
        def log(msg):
            if log_callback:
                log_callback(msg)
            else:
                try:
                    print(msg)
                except UnicodeEncodeError:
                    print(msg.encode('ascii', 'ignore').decode('ascii'))

        results = []
        total_steps = len(symbols) * trades_per_symbol
        current_step = 0

        dte_val = int(dte) if dte and dte > 0 else 30
        width_val = float(spread_width) if spread_width and spread_width > 0 else 5.0

        for sym in symbols:
            try:
                ticker = yf.Ticker(sym)
                df_hist = ticker.history(period="1y")
            except Exception as e:
                log(f"   ⚠️ Kon data niet ophalen voor {sym}: {e}")
                continue

            if df_hist.empty or len(df_hist) < 100:
                continue

            close_series = df_hist['Close'].ffill()
            df_hist['returns'] = close_series.pct_change(fill_method=None)
            df_hist['hv30'] = df_hist['returns'].rolling(window=30).std() * np.sqrt(252)

            total_bars = len(df_hist)
            
            # Start 14 calendar days back from the latest available date in dataset
            latest_date = df_hist.index[-1]
            target_start_date = latest_date - pd.Timedelta(days=14)
            
            try:
                start_entry_idx = df_hist.index.get_indexer([target_start_date], method='nearest')[0]
            except Exception:
                start_entry_idx = total_bars - 15

            start_entry_idx = min(total_bars - 1, max(60, start_entry_idx))

            # Step backward in time (~21 trading days = 1 month per trade)
            step_size = 21
            entry_indices = []
            for i in range(trades_per_symbol):
                idx = start_entry_idx - (i * step_size)
                if idx >= 60:
                    entry_indices.append(idx)

            # Sort chronologically so report flows naturally from past to present
            entry_indices.sort()

            for idx_entry in entry_indices:
                current_step += 1
                if progress_callback:
                    progress_callback(current_step / max(1, total_steps), f"Testen spread {current_step}/{total_steps} ({sym})")

                row_entry = df_hist.iloc[idx_entry]
                date_entry = df_hist.index[idx_entry]
                price_entry = float(row_entry['Close'])
                hv_val = float(row_entry['hv30']) if not pd.isna(row_entry['hv30']) and row_entry['hv30'] > 0 else 0.20

                # Volatility Regime
                if hv_val < 0.15:
                    vol_regime = "Laag (<15%)"
                elif hv_val <= 0.30:
                    vol_regime = "Normaal (15-30%)"
                else:
                    vol_regime = "Hoog (>30%)"

                # Trend (EMA20 vs EMA50)
                sub_close = close_series.iloc[max(0, idx_entry-50):idx_entry+1]
                ema20 = sub_close.ewm(span=20).mean().iloc[-1]
                ema50 = sub_close.ewm(span=50).mean().iloc[-1]

                is_bullish = price_entry >= ema50
                
                # Determine strategy: AUTO or user-selected target_strategy
                if target_strategy and str(target_strategy).upper() in ['BULLPUT', 'BEARCALL', 'BULLCALL', 'BEARPUT', 'LONGCALL', 'LONGPUT', 'SHORTPUT']:
                    st_clean = str(target_strategy).upper()
                    if st_clean == 'BULLPUT': strat = 'BullPut'
                    elif st_clean == 'BEARCALL': strat = 'BearCall'
                    elif st_clean == 'BULLCALL': strat = 'BullCall'
                    elif st_clean == 'BEARPUT': strat = 'BearPut'
                    elif st_clean == 'LONGCALL': strat = 'LongCall'
                    elif st_clean == 'LONGPUT': strat = 'LongPut'
                    elif st_clean == 'SHORTPUT': strat = 'ShortPut'
                    else: strat = 'BullPut'
                else:
                    strat = 'BullPut' if is_bullish else 'BearCall'

                em68 = price_entry * hv_val * np.sqrt(dte_val / 365.0)
                em85 = em68 * 1.439535

                # Distance used for strike placement based on passed em_multiplier
                em_safety_dist = em68 * em_multiplier
                width_scale = width_val / 5.0

                if strat == 'BullPut':
                    short_strike = round_to_strike(price_entry - em_safety_dist)
                    long_strike = round(short_strike - width_val, 2)
                    optie_strike = f"Short ${short_strike:.2f} / Long ${long_strike:.2f}"
                    dist_factor = max(0.4, 2.0 - (em_multiplier * 0.75))
                    credit = round(min(width_val * 0.36, max(0.15, (em68 * 0.22) * dist_factor * width_scale)), 2)
                    bep = round(short_strike - credit, 2)
                    bep_dist = price_entry - bep
                    max_profit = credit * 100.0
                    max_loss = (width_val - credit) * 100.0
                elif strat == 'BearCall':
                    short_strike = round_to_strike(price_entry + em_safety_dist)
                    long_strike = round(short_strike + width_val, 2)
                    optie_strike = f"Short ${short_strike:.2f} / Long ${long_strike:.2f}"
                    dist_factor = max(0.4, 2.0 - (em_multiplier * 0.75))
                    credit = round(min(width_val * 0.36, max(0.15, (em68 * 0.22) * dist_factor * width_scale)), 2)
                    bep = round(short_strike + credit, 2)
                    bep_dist = bep - price_entry
                    max_profit = credit * 100.0
                    max_loss = (width_val - credit) * 100.0
                elif strat == 'BullCall':
                    long_strike = round_to_strike(price_entry - (em_safety_dist * 0.2))
                    short_strike = round(long_strike + width_val, 2)
                    optie_strike = f"Long ${long_strike:.2f} / Short ${short_strike:.2f}"
                    credit = round(min(width_val * 0.56, max(0.50, (em68 * 0.35) * width_scale)), 2)
                    bep = round(long_strike + credit, 2)
                    bep_dist = max(0.1, bep - price_entry)
                    max_profit = (width_val - credit) * 100.0
                    max_loss = credit * 100.0
                elif strat == 'BearPut':
                    long_strike = round_to_strike(price_entry + (em_safety_dist * 0.2))
                    short_strike = round(long_strike - width_val, 2)
                    optie_strike = f"Long ${long_strike:.2f} / Short ${short_strike:.2f}"
                    credit = round(min(width_val * 0.56, max(0.50, (em68 * 0.35) * width_scale)), 2)
                    bep = round(long_strike - credit, 2)
                    bep_dist = max(0.1, price_entry - bep)
                    max_profit = (width_val - credit) * 100.0
                    max_loss = credit * 100.0
                elif strat == 'LongCall':
                    # Single-leg ATM Long Call
                    long_strike = round_to_strike(price_entry)
                    short_strike = None
                    optie_strike = f"Long Call ${long_strike:.2f}"
                    credit = round(max(0.50, em68 * 0.40), 2)
                    bep = round(long_strike + credit, 2)
                    bep_dist = max(0.1, bep - price_entry)
                    max_profit = 9999.0
                    max_loss = credit * 100.0
                elif strat == 'LongPut':
                    # Single-leg ATM Long Put
                    long_strike = round_to_strike(price_entry)
                    short_strike = None
                    optie_strike = f"Long Put ${long_strike:.2f}"
                    credit = round(max(0.50, em68 * 0.40), 2)
                    bep = round(long_strike - credit, 2)
                    bep_dist = max(0.1, price_entry - bep)
                    max_profit = round((long_strike - credit) * 100.0, 2)
                    max_loss = credit * 100.0
                else: # ShortPut (Losse Short Put / Cash Secured Put)
                    short_strike = round_to_strike(price_entry - em_safety_dist)
                    long_strike = None
                    optie_strike = f"Short Put ${short_strike:.2f}"
                    dist_factor = max(0.4, 2.0 - (em_multiplier * 0.75))
                    credit = round(min(price_entry * 0.08, max(0.25, (em68 * 0.32) * dist_factor)), 2)
                    bep = round(short_strike - credit, 2)
                    bep_dist = price_entry - bep
                    max_profit = credit * 100.0
                    max_loss = (short_strike - credit) * 100.0

                em85_dekking = (bep_dist / max(0.01, em85)) * 100.0
                if strat in ['LongCall', 'LongPut']:
                    pop_est = min(75.0, max(25.0, 50.0 - (bep_dist / price_entry) * 200.0))
                else:
                    pop_est = min(96.0, max(60.0, 50.0 + (bep_dist / price_entry) * 350.0))

                trading_days = max(5, int(round(dte_val * 21.0 / 30.0)))
                idx_exp = min(total_bars - 1, idx_entry + trading_days)
                df_trade_period = df_hist.iloc[idx_entry+1:idx_exp+1]
                price_exp = float(df_hist.iloc[idx_exp]['Close'])
                date_exp = df_hist.index[idx_exp]

                min_price_during = float(df_trade_period['Low'].min())
                max_price_during = float(df_trade_period['High'].max())

                if strat == 'BullPut':
                    touched_bep = min_price_during <= bep
                    breached_short = min_price_during <= short_strike
                    win = price_exp > short_strike
                elif strat == 'BearCall':
                    touched_bep = max_price_during >= bep
                    breached_short = max_price_during >= short_strike
                    win = price_exp < short_strike
                elif strat == 'BullCall':
                    touched_bep = min_price_during <= bep
                    breached_short = min_price_during <= long_strike
                    win = price_exp >= bep
                elif strat == 'BearPut':
                    touched_bep = max_price_during >= bep
                    breached_short = max_price_during >= long_strike
                    win = price_exp <= bep
                elif strat == 'LongCall':
                    touched_bep = max_price_during >= bep
                    breached_short = False
                    win = price_exp > bep
                elif strat == 'LongPut':
                    touched_bep = min_price_during <= bep
                    breached_short = False
                    win = price_exp < bep
                else: # ShortPut
                    touched_bep = min_price_during <= bep
                    breached_short = min_price_during <= short_strike
                    win = price_exp >= short_strike

                if strat == 'LongCall':
                    intrinsic_exp = max(0.0, price_exp - long_strike)
                    realized_pnl = round((intrinsic_exp - credit) * 100.0, 2)
                    em85_safe = win or (price_exp > long_strike)
                    if win:
                        status = "✅ Winst (Koers > BEP)"
                    elif intrinsic_exp > 0:
                        status = "🟡 Deels Verlies (Tussen Strike en BEP)"
                    else:
                        status = "🔴 Verlies (Waardeloos OTM)"
                elif strat == 'LongPut':
                    intrinsic_exp = max(0.0, long_strike - price_exp)
                    realized_pnl = round((intrinsic_exp - credit) * 100.0, 2)
                    em85_safe = win or (price_exp < long_strike)
                    if win:
                        status = "✅ Winst (Koers < BEP)"
                    elif intrinsic_exp > 0:
                        status = "🟡 Deels Verlies (Tussen Strike en BEP)"
                    else:
                        status = "🔴 Verlies (Waardeloos OTM)"
                elif strat == 'ShortPut':
                    em85_safe = not touched_bep
                    if win:
                        realized_pnl = max_profit
                        status = "✅ Winst (Expiratie OTM / Premie Behouden)"
                    elif price_exp >= bep:
                        realized_pnl = round((price_exp - bep) * 100.0, 2)
                        status = "🟡 Deelwinst (Tussen Strike en BEP)"
                    else:
                        actual_loss = (bep - price_exp) * 100.0
                        loss_capped = min(actual_loss, max_profit * 2.0)
                        realized_pnl = -round(loss_capped, 2)
                        status = "🔴 Verlies (ITM / Aanwijzing)"
                else:
                    em85_safe = not touched_bep
                    if win:
                        realized_pnl = max_profit
                        status = "✅ Winst (Expiratie OTM)"
                    elif touched_bep and not breached_short:
                        realized_pnl = max_profit * 0.5
                        status = "🟡 BEP Touch (Gered)"
                    else:
                        realized_pnl = -max_loss
                        status = "🔴 Verlies (ITM)"

                results.append({
                    'symbol': sym,
                    'entry_date': date_entry.strftime('%Y-%m-%d'),
                    'exp_date': date_exp.strftime('%Y-%m-%d'),
                    'strategy': strat,
                    'vol_regime': vol_regime,
                    'hv30_%': round(hv_val * 100, 1),
                    'dte': dte_val,
                    'spread_width': width_val,
                    'underlying_entry': price_entry,
                    'underlying_exp': price_exp,
                    'optie_strike': optie_strike,
                    'short_strike': short_strike,
                    'long_strike': long_strike,
                    'bep': bep,
                    'credit': credit,
                    'EM68': round(em68, 2),
                    'EM85': round(em85, 2),
                    'em_multiplier_used': round(em_multiplier, 2),
                    'em85_dekking_pct': round(em85_dekking, 1),
                    'pop': round(pop_est, 1),
                    'status': status,
                    'win': win,
                    'em85_safe': em85_safe,
                    'realized_pnl': realized_pnl
                })

        df_res = pd.DataFrame(results)
        if df_res.empty:
            return {'summary': {}, 'details_df': pd.DataFrame()}

        win_count = int(df_res['win'].sum())
        total_count = len(df_res)
        hit_rate = (win_count / total_count) * 100.0
        em85_safe_count = int(df_res['em85_safe'].sum())
        em85_safe_rate = (em85_safe_count / total_count) * 100.0
        total_pnl = float(df_res['realized_pnl'].sum())
        avg_pnl = float(df_res['realized_pnl'].mean())
        avg_pop = float(df_res['pop'].mean())

        summary = {
            'total_trades': total_count,
            'wins': win_count,
            'losses': total_count - win_count,
            'hit_rate': round(hit_rate, 1),
            'avg_pop': round(avg_pop, 1),
            'em85_safe_rate': round(em85_safe_rate, 1),
            'total_pnl': round(total_pnl, 2),
            'avg_pnl': round(avg_pnl, 2)
        }

        return {
            'summary': summary,
            'details_df': df_res
        }

    def compare_sidebar_vs_standard(self, symbols=['SPY', 'AAPL', 'MSFT', 'NVDA', 'QQQ'], trades_per_symbol=5, sidebar_params=None, standard_params=None, progress_callback=None, log_callback=None):
        """
        Runs a side-by-side comparative backtest between the user's current Sidebar settings
        and the Standard optimal Benchmark (EM85 / 30 DTE / $5 width).
        Produces stock-by-stock recommendations and determines optimal parameters per ticker.
        """
        def log(msg):
            if log_callback:
                log_callback(msg)
            else:
                try:
                    print(msg)
                except UnicodeEncodeError:
                    print(msg.encode('ascii', 'ignore').decode('ascii'))

        if standard_params is None:
            standard_params = {
                'dte': 30,
                'spread_width': 5.0,
                'em_multiplier': 1.439535,
                'target_strategy': 'AUTO',
                'name': 'Standaard (EM85 / 30DTE / $5.00)'
            }

        if sidebar_params is None:
            sidebar_params = {
                'dte': 30,
                'spread_width': 10.0,
                'em_multiplier': 1.439535,
                'target_strategy': 'AUTO',
                'name': 'Huidige Sidebar Instellingen'
            }

        log(f"⚖️ Vergelijkingstest gestart voor {len(symbols)} symbolen...")
        log(f"   Configuratie A (Sidebar): DTE={sidebar_params.get('dte', 30)}, Breedte=${sidebar_params.get('spread_width', 10.0)}, EM={sidebar_params.get('em_multiplier', 1.44):.2f}x, Strat={sidebar_params.get('target_strategy', 'AUTO')}")
        log(f"   Configuratie B (Standaard): DTE={standard_params.get('dte', 30)}, Breedte=${standard_params.get('spread_width', 5.0)}, EM={standard_params.get('em_multiplier', 1.44):.2f}x, Strat={standard_params.get('target_strategy', 'AUTO')}")

        def p_sb(pct, msg):
            if progress_callback:
                progress_callback(pct * 0.5, f"Testen Sidebar Instellingen: {msg}")

        def p_std(pct, msg):
            if progress_callback:
                progress_callback(0.5 + (pct * 0.5), f"Testen Standaard Benchmark: {msg}")

        res_sb = self.run_backtest(
            symbols=symbols,
            trades_per_symbol=trades_per_symbol,
            em_multiplier=sidebar_params.get('em_multiplier', 1.439535),
            target_strategy=sidebar_params.get('target_strategy', 'AUTO'),
            dte=sidebar_params.get('dte', 30),
            spread_width=sidebar_params.get('spread_width', 10.0),
            progress_callback=p_sb,
            log_callback=log_callback
        )

        res_std = self.run_backtest(
            symbols=symbols,
            trades_per_symbol=trades_per_symbol,
            em_multiplier=standard_params.get('em_multiplier', 1.439535),
            target_strategy=standard_params.get('target_strategy', 'AUTO'),
            dte=standard_params.get('dte', 30),
            spread_width=standard_params.get('spread_width', 5.0),
            progress_callback=p_std,
            log_callback=log_callback
        )

        df_sb = res_sb['details_df']
        df_std = res_std['details_df']

        comp_rows = []
        stock_profiles = {}

        for sym in symbols:
            sub_sb = df_sb[df_sb['symbol'] == sym] if not df_sb.empty else pd.DataFrame()
            sub_std = df_std[df_std['symbol'] == sym] if not df_std.empty else pd.DataFrame()

            if sub_sb.empty and sub_std.empty:
                continue

            sb_trades = len(sub_sb)
            sb_wins = int(sub_sb['win'].sum()) if not sub_sb.empty else 0
            sb_hr = (sb_wins / sb_trades * 100.0) if sb_trades > 0 else 0.0
            sb_avg_pnl = float(sub_sb['realized_pnl'].mean()) if sb_trades > 0 else 0.0
            sb_tot_pnl = float(sub_sb['realized_pnl'].sum()) if sb_trades > 0 else 0.0
            sb_safe_rate = (float(sub_sb['em85_safe'].sum()) / sb_trades * 100.0) if sb_trades > 0 else 0.0

            std_trades = len(sub_std)
            std_wins = int(sub_std['win'].sum()) if not sub_std.empty else 0
            std_hr = (std_wins / std_trades * 100.0) if std_trades > 0 else 0.0
            std_avg_pnl = float(sub_std['realized_pnl'].mean()) if std_trades > 0 else 0.0
            std_tot_pnl = float(sub_std['realized_pnl'].sum()) if std_trades > 0 else 0.0
            std_safe_rate = (float(sub_std['em85_safe'].sum()) / std_trades * 100.0) if std_trades > 0 else 0.0

            pnl_delta = sb_avg_pnl - std_avg_pnl

            # Determine best model for this specific stock
            if std_avg_pnl > (sb_avg_pnl + 2.0):
                winner = "Standaard"
                gain_per_trade = std_avg_pnl - sb_avg_pnl
                advies = f"Standaard is beter (+${gain_per_trade:.2f}/trade)"
                winning_params = standard_params
            elif sb_avg_pnl > (std_avg_pnl + 2.0):
                winner = "Sidebar"
                gain_per_trade = sb_avg_pnl - std_avg_pnl
                advies = f"Sidebar is beter (+${gain_per_trade:.2f}/trade)"
                winning_params = sidebar_params
            else:
                # Close in profit, use hit rate / safe rate to break tie
                if std_hr >= sb_hr:
                    winner = "Standaard"
                    advies = "Gelijkwaardig (Standaard heeft hogere/gelijke Hit Rate)"
                    winning_params = standard_params
                else:
                    winner = "Sidebar"
                    advies = "Gelijkwaardig (Sidebar heeft hogere Hit Rate)"
                    winning_params = sidebar_params

            stock_profiles[sym] = {
                'winner': winner,
                'winning_params': winning_params,
                'sb_avg_pnl': sb_avg_pnl,
                'std_avg_pnl': std_avg_pnl,
                'sb_hr': sb_hr,
                'std_hr': std_hr,
                'pnl_gain': abs(pnl_delta)
            }

            comp_rows.append({
                'symbol': sym,
                'sb_hit_rate': round(sb_hr, 1),
                'sb_avg_pnl': round(sb_avg_pnl, 2),
                'sb_total_pnl': round(sb_tot_pnl, 2),
                'sb_safe_rate': round(sb_safe_rate, 1),
                'std_hit_rate': round(std_hr, 1),
                'std_avg_pnl': round(std_avg_pnl, 2),
                'std_total_pnl': round(std_tot_pnl, 2),
                'std_safe_rate': round(std_safe_rate, 1),
                'winner': winner,
                'pnl_delta': round(pnl_delta, 2),
                'advies': advies
            })

        df_comp = pd.DataFrame(comp_rows)

        sb_glob_pnl = res_sb['summary'].get('avg_pnl', 0.0)
        std_glob_pnl = res_std['summary'].get('avg_pnl', 0.0)

        if std_glob_pnl > (sb_glob_pnl + 1.0):
            global_best = "Standaard"
        elif sb_glob_pnl > (std_glob_pnl + 1.0):
            global_best = "Sidebar"
        else:
            sb_glob_hr = res_sb['summary'].get('hit_rate', 0.0)
            std_glob_hr = res_std['summary'].get('hit_rate', 0.0)
            global_best = "Standaard" if std_glob_hr >= sb_glob_hr else "Sidebar"

        return {
            'sidebar_summary': res_sb['summary'],
            'standard_summary': res_std['summary'],
            'sidebar_details': df_sb,
            'standard_details': df_std,
            'comparison_df': df_comp,
            'global_best': global_best,
            'global_pnl_diff': round(abs(sb_glob_pnl - std_glob_pnl), 2),
            'stock_profiles': stock_profiles,
            'sidebar_params': sidebar_params,
            'standard_params': standard_params
        }

    def optimize_em_multipliers(self, symbols=['SPY', 'AAPL', 'MSFT', 'NVDA', 'QQQ'], trades_per_symbol=5, multipliers=[0.8, 1.0, 1.2, 1.44, 1.65, 1.8, 2.0], progress_callback=None, log_callback=None):
        """
        Sequentially sweeps EM multipliers (from 0.8x EM68 up to 2.0x EM68) across volatility regimes
        to find the exact sweet spot that maximizes Average PnL ($/trade) and Win Rate.
        """
        def log(msg):
            if log_callback:
                log_callback(msg)
            else:
                try:
                    print(msg)
                except UnicodeEncodeError:
                    print(msg.encode('ascii', 'ignore').decode('ascii'))

        log(f"🔎 Starten van Sequentiële EM Multiplier Optimalisatie over {len(multipliers)} niveaus...")

        sweep_results = []
        total_runs = len(multipliers)

        for idx, mult in enumerate(multipliers):
            if progress_callback:
                progress_callback((idx + 1) / total_runs, f"Optimaliseren EM Multiplier = {mult:.2f}x EM68")

            res = self.run_backtest(symbols=symbols, trades_per_symbol=trades_per_symbol, em_multiplier=mult)
            sum_dict = res['summary']
            df_det = res['details_df']

            if df_det.empty:
                continue

            avg_credit = float(df_det['credit'].mean())

            # Performance per Volatility Regime
            regime_stats = {}
            for reg, group in df_det.groupby('vol_regime'):
                reg_win = (group['win'].sum() / len(group)) * 100.0 if len(group) > 0 else 0
                reg_pnl = group['realized_pnl'].mean() if len(group) > 0 else 0
                regime_stats[reg] = {
                    'count': len(group),
                    'win_rate': round(reg_win, 1),
                    'avg_pnl': round(reg_pnl, 2)
                }

            sweep_results.append({
                'multiplier': mult,
                'multiplier_name': f"{mult:.2f}x EM68" + (" (EM85)" if abs(mult-1.439535)<0.05 else ""),
                'hit_rate': sum_dict['hit_rate'],
                'avg_pop': sum_dict['avg_pop'],
                'avg_credit': round(avg_credit, 2),
                'total_pnl': sum_dict['total_pnl'],
                'avg_pnl': sum_dict['avg_pnl'],
                'em85_safe_rate': sum_dict['em85_safe_rate'],
                'regime_stats': regime_stats
            })

        df_sweep = pd.DataFrame(sweep_results)
        
        # Determine overall best multiplier (highest avg_pnl)
        best_row = df_sweep.loc[df_sweep['avg_pnl'].idxmax()] if not df_sweep.empty else None

        log(f"✅ Optimalisatie voltooid! Beste Multiplier: {best_row['multiplier_name'] if best_row is not None else 'N/A'} met gem. winst van ${best_row['avg_pnl']:.2f}/trade!")

        return {
            'sweep_df': df_sweep,
            'best_multiplier': best_row['multiplier'] if best_row is not None else 1.44,
            'best_pnl': best_row['avg_pnl'] if best_row is not None else 0.0,
            'best_hit_rate': best_row['hit_rate'] if best_row is not None else 0.0
        }

if __name__ == '__main__':
    tester = SpreadHitRateTester()
    opt_res = tester.optimize_em_multipliers()
    print("Sweep results:\n", opt_res['sweep_df'][['multiplier_name', 'hit_rate', 'avg_credit', 'avg_pnl', 'total_pnl']])
