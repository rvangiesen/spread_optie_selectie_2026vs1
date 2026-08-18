import datetime
import numpy as np
import pandas as pd
import yfinance as yf

class SpreadHitRateTester:
    """
    Backtests historical spreads across liquid benchmark stocks to evaluate
    actual hit rate, EM68/EM85 safety coverage, and total PnL performance.
    """
    def __init__(self):
        pass

    def run_backtest(self, symbols=['SPY', 'AAPL', 'MSFT', 'NVDA', 'QQQ'], trades_per_symbol=5, progress_callback=None, log_callback=None):
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

        log(f"🧪 Starten van Hit-Rate Validatietest voor {len(symbols)} aandelen ({total_steps} spreads)...")

        for sym in symbols:
            log(f"   📈 Ophalen historische koersdata voor {sym}...")
            try:
                ticker = yf.Ticker(sym)
                df_hist = ticker.history(period="1y")
            except Exception as e:
                log(f"   ⚠️ Kon data niet ophalen voor {sym}: {e}")
                continue

            if df_hist.empty or len(df_hist) < 100:
                log(f"   ⚠️ Onvoldoende historische data voor {sym}.")
                continue

            # Calculate returns & 30-day Historical Volatility (HV30) without deprecated fill_method warning
            close_series = df_hist['Close'].ffill()
            df_hist['returns'] = close_series.pct_change(fill_method=None)
            df_hist['hv30'] = df_hist['returns'].rolling(window=30).std() * np.sqrt(252)

            total_bars = len(df_hist)
            # Pick evenly spaced entry dates across past 10 months
            step_size = max(1, (total_bars - 95) // trades_per_symbol)
            entry_indices = [60 + i * step_size for i in range(trades_per_symbol)]
            entry_indices = [idx for idx in entry_indices if idx < total_bars - 30]

            for idx_entry in entry_indices:
                current_step += 1
                if progress_callback:
                    progress_callback(current_step / max(1, total_steps), f"Testen spread {current_step}/{total_steps} ({sym})")

                row_entry = df_hist.iloc[idx_entry]
                date_entry = df_hist.index[idx_entry]
                price_entry = float(row_entry['Close'])
                hv_val = float(row_entry['hv30']) if not pd.isna(row_entry['hv30']) and row_entry['hv30'] > 0 else 0.20

                # Determine Trend (EMA20 vs EMA50)
                sub_close = close_series.iloc[max(0, idx_entry-50):idx_entry+1]
                ema20 = sub_close.ewm(span=20).mean().iloc[-1]
                ema50 = sub_close.ewm(span=50).mean().iloc[-1]

                is_bullish = price_entry >= ema50
                strat = 'BullPut' if is_bullish else 'BearCall'

                # DTE = 30 days
                dte = 30

                # Calculate EM68 and EM85
                em68 = price_entry * hv_val * np.sqrt(dte / 365.0)
                em85 = em68 * 1.439535

                # Strike selection: 1.1x EM68 safety margin (Niveau 2 buffer)
                if strat == 'BullPut':
                    short_strike = round(price_entry - (em68 * 1.1), 1)
                    long_strike = short_strike - 5.0
                    credit = round(min(1.50, max(0.25, em68 * 0.15)), 2)
                    bep = short_strike - credit
                    bep_dist = price_entry - bep
                else: # BearCall
                    short_strike = round(price_entry + (em68 * 1.1), 1)
                    long_strike = short_strike + 5.0
                    credit = round(min(1.50, max(0.25, em68 * 0.15)), 2)
                    bep = short_strike + credit
                    bep_dist = bep - price_entry

                em85_dekking = (bep_dist / max(0.01, em85)) * 100.0
                pop_est = min(94.0, max(65.0, 50.0 + (bep_dist / price_entry) * 350.0))

                # Track price development over next 30 calendar days (~22 trading bars)
                idx_exp = min(total_bars - 1, idx_entry + 22)
                df_trade_period = df_hist.iloc[idx_entry+1:idx_exp+1]
                price_exp = float(df_hist.iloc[idx_exp]['Close'])
                date_exp = df_hist.index[idx_exp]

                min_price_during = float(df_trade_period['Low'].min())
                max_price_during = float(df_trade_period['High'].max())

                # Outcome evaluation
                if strat == 'BullPut':
                    touched_bep = min_price_during <= bep
                    breached_short = min_price_during <= short_strike
                    win = price_exp > short_strike
                else: # BearCall
                    touched_bep = max_price_during >= bep
                    breached_short = max_price_during >= short_strike
                    win = price_exp < short_strike

                max_profit = credit * 100.0
                max_loss = (5.0 - credit) * 100.0

                if win:
                    realized_pnl = max_profit
                    status = "✅ Winst (Expiratie OTM)"
                elif touched_bep and not breached_short:
                    realized_pnl = max_profit * 0.5 # Early exit profit
                    status = "🟡 BEP Touch (Gered)"
                else:
                    realized_pnl = -max_loss
                    status = "🔴 Verlies (ITM)"

                results.append({
                    'symbol': sym,
                    'entry_date': date_entry.strftime('%Y-%m-%d'),
                    'exp_date': date_exp.strftime('%Y-%m-%d'),
                    'strategy': strat,
                    'underlying_entry': price_entry,
                    'underlying_exp': price_exp,
                    'short_strike': short_strike,
                    'long_strike': long_strike,
                    'bep': bep,
                    'credit': credit,
                    'EM68': round(em68, 2),
                    'EM85': round(em85, 2),
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

        log(f"✅ Hit-Rate Validatie voltooid! Hit Rate: {hit_rate:.1f}% ({win_count}/{total_count} winstgevend). Totale Winst: ${total_pnl:.2f}")

        return {
            'summary': summary,
            'details_df': df_res
        }

if __name__ == '__main__':
    tester = SpreadHitRateTester()
    res = tester.run_backtest()
    print("Summary:", res['summary'])
