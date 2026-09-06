import datetime
import numpy as np
import pandas as pd
import yfinance as yf

class SpreadHitRateTester:
    """
    Backtests historical spreads across liquid benchmark stocks to evaluate
    actual hit rate, EM68/EM85 safety coverage, total PnL performance, and 
    sequentially optimizes EM multipliers based on volatility regimes.
    """
    def __init__(self):
        pass

    def run_backtest(self, symbols=['SPY', 'AAPL', 'MSFT', 'NVDA', 'QQQ'], trades_per_symbol=5, em_multiplier=1.439535, target_strategy='AUTO', progress_callback=None, log_callback=None):
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
                if target_strategy and str(target_strategy).upper() in ['BULLPUT', 'BEARCALL', 'BULLCALL', 'BEARPUT']:
                    st_clean = str(target_strategy).upper()
                    if st_clean == 'BULLPUT': strat = 'BullPut'
                    elif st_clean == 'BEARCALL': strat = 'BearCall'
                    elif st_clean == 'BULLCALL': strat = 'BullCall'
                    elif st_clean == 'BEARPUT': strat = 'BearPut'
                    else: strat = 'BullPut'
                else:
                    strat = 'BullPut' if is_bullish else 'BearCall'

                dte = 30
                em68 = price_entry * hv_val * np.sqrt(dte / 365.0)
                em85 = em68 * 1.439535

                # Distance used for strike placement based on passed em_multiplier
                em_safety_dist = em68 * em_multiplier

                if strat == 'BullPut':
                    short_strike = round(price_entry - em_safety_dist, 1)
                    long_strike = short_strike - 5.0
                    dist_factor = max(0.4, 2.0 - (em_multiplier * 0.75))
                    credit = round(min(1.80, max(0.15, (em68 * 0.22) * dist_factor)), 2)
                    bep = short_strike - credit
                    bep_dist = price_entry - bep
                    max_profit = credit * 100.0
                    max_loss = (5.0 - credit) * 100.0
                elif strat == 'BearCall':
                    short_strike = round(price_entry + em_safety_dist, 1)
                    long_strike = short_strike + 5.0
                    dist_factor = max(0.4, 2.0 - (em_multiplier * 0.75))
                    credit = round(min(1.80, max(0.15, (em68 * 0.22) * dist_factor)), 2)
                    bep = short_strike + credit
                    bep_dist = bep - price_entry
                    max_profit = credit * 100.0
                    max_loss = (5.0 - credit) * 100.0
                elif strat == 'BullCall':
                    long_strike = round(price_entry - (em_safety_dist * 0.2), 1)
                    short_strike = round(long_strike + 5.0, 1)
                    credit = round(min(2.80, max(0.50, (em68 * 0.35))), 2)
                    bep = long_strike + credit
                    bep_dist = max(0.1, bep - price_entry)
                    max_profit = (5.0 - credit) * 100.0
                    max_loss = credit * 100.0
                else: # BearPut
                    long_strike = round(price_entry + (em_safety_dist * 0.2), 1)
                    short_strike = round(long_strike - 5.0, 1)
                    credit = round(min(2.80, max(0.50, (em68 * 0.35))), 2)
                    bep = long_strike - credit
                    bep_dist = max(0.1, price_entry - bep)
                    max_profit = (5.0 - credit) * 100.0
                    max_loss = credit * 100.0

                em85_dekking = (bep_dist / max(0.01, em85)) * 100.0
                pop_est = min(96.0, max(60.0, 50.0 + (bep_dist / price_entry) * 350.0))

                idx_exp = min(total_bars - 1, idx_entry + 22)
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
                else: # BearPut
                    touched_bep = max_price_during >= bep
                    breached_short = max_price_during >= long_strike
                    win = price_exp <= bep

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
                    'underlying_entry': price_entry,
                    'underlying_exp': price_exp,
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
                    'em85_safe': not touched_bep,
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
