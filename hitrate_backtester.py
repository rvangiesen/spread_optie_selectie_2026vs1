import datetime
import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm
from risk_model import AntiGravityGammaThetaEngine
from logic import analyze_dual_trigger_spreads, calculate_profit_stop_levels

def bs_call_price(S, K, T, r=0.04, sigma=0.20):
    """Black-Scholes analytical Call price."""
    if T <= 1e-5:
        return max(0.0, float(S) - float(K))
    sigma = max(1e-4, float(sigma))
    S_f = max(1e-4, float(S))
    K_f = max(1e-4, float(K))
    d1 = (np.log(S_f / K_f) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return max(0.0, float(S_f * norm.cdf(d1) - K_f * np.exp(-r * T) * norm.cdf(d2)))

def bs_put_price(S, K, T, r=0.04, sigma=0.20):
    """Black-Scholes analytical Put price."""
    if T <= 1e-5:
        return max(0.0, float(K) - float(S))
    sigma = max(1e-4, float(sigma))
    S_f = max(1e-4, float(S))
    K_f = max(1e-4, float(K))
    d1 = (np.log(S_f / K_f) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return max(0.0, float(K_f * np.exp(-r * T) * norm.cdf(-d2) - S_f * norm.cdf(-d1)))

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

    def run_backtest(
        self,
        symbols=['SPY', 'AAPL', 'MSFT', 'NVDA', 'QQQ'],
        trades_per_symbol=10,
        em_multiplier=1.439535,
        target_strategy='AUTO',
        dte=30,
        spread_width=5.0,
        use_dual_trigger=False,
        profit_target_pct=70.0,
        exit_on_momentum_drop=True,
        progress_callback=None,
        log_callback=None
    ):
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
                hist_period = "2y" if (use_dual_trigger or trades_per_symbol > 5) else "1y"
                df_hist = ticker.history(period=hist_period)
                if (df_hist is None or df_hist.empty or len(df_hist) < 60) and hist_period == "2y":
                    df_hist = ticker.history(period="1y")
            except Exception as e:
                log(f"   ⚠️ Kon data niet ophalen voor {sym}: {e}")
                continue

            if df_hist is None or df_hist.empty or len(df_hist) < 60:
                continue

            if use_dual_trigger:
                df_dual = analyze_dual_trigger_spreads(df_hist)
                if not df_dual.empty:
                    df_hist = df_dual

            close_series = df_hist['Close'].ffill()
            df_hist['returns'] = close_series.pct_change(fill_method=None)
            df_hist['hv30'] = df_hist['returns'].rolling(window=30).std() * np.sqrt(252)

            total_bars = len(df_hist)
            trading_days_est = max(5, int(round(dte_val * 21.0 / 30.0)))

            if use_dual_trigger and 'Long_Signal' in df_hist.columns:
                raw_sig_indices = [
                    i for i in range(40, total_bars - trading_days_est)
                    if bool(df_hist['Long_Signal'].iloc[i])
                ]
                # Filter indices so trades are at least 4 bars apart (prevents duplicate same-move clustering)
                spaced_indices = []
                for s_i in raw_sig_indices:
                    if not spaced_indices or (s_i - spaced_indices[-1] >= 4):
                        spaced_indices.append(s_i)
                entry_indices = spaced_indices[-trades_per_symbol:]
                if not entry_indices and raw_sig_indices:
                    entry_indices = raw_sig_indices[-trades_per_symbol:]
            else:
                # Start 14 calendar days back from latest available date
                latest_date = df_hist.index[-1]
                target_start_date = latest_date - pd.Timedelta(days=14)
                try:
                    start_entry_idx = df_hist.index.get_indexer([target_start_date], method='nearest')[0]
                except Exception:
                    start_entry_idx = total_bars - 15

                start_entry_idx = min(total_bars - 1, max(60, start_entry_idx))

                step_size = 21
                entry_indices = []
                for i in range(trades_per_symbol):
                    idx = start_entry_idx - (i * step_size)
                    if idx >= 60:
                        entry_indices.append(idx)
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

                entry_signal_type = str(row_entry.get('Signal_Type', 'None')) if use_dual_trigger else 'Regulier'
                early_exit = False
                exit_reason = "Expiratie Afloop"
                days_held = trading_days
                actual_exit_date = date_exp

                # Calibreer theoretische optiewaarde op dag van instap (t=0)
                t_entry_yrs = max(1e-4, dte_val / 365.0)
                if strat == 'BullPut':
                    bs_entry_short = bs_put_price(price_entry, short_strike, t_entry_yrs, 0.04, hv_val)
                    bs_entry_long = bs_put_price(price_entry, long_strike, t_entry_yrs, 0.04, hv_val)
                    bs_spread_entry = max(0.05, bs_entry_short - bs_entry_long)
                elif strat == 'BearCall':
                    bs_entry_short = bs_call_price(price_entry, short_strike, t_entry_yrs, 0.04, hv_val)
                    bs_entry_long = bs_call_price(price_entry, long_strike, t_entry_yrs, 0.04, hv_val)
                    bs_spread_entry = max(0.05, bs_entry_short - bs_entry_long)
                elif strat == 'BullCall':
                    bs_entry_long = bs_call_price(price_entry, long_strike, t_entry_yrs, 0.04, hv_val)
                    bs_entry_short = bs_call_price(price_entry, short_strike, t_entry_yrs, 0.04, hv_val)
                    bs_spread_entry = max(0.05, bs_entry_long - bs_entry_short)
                elif strat == 'BearPut':
                    bs_entry_long = bs_put_price(price_entry, long_strike, t_entry_yrs, 0.04, hv_val)
                    bs_entry_short = bs_put_price(price_entry, short_strike, t_entry_yrs, 0.04, hv_val)
                    bs_spread_entry = max(0.05, bs_entry_long - bs_entry_short)
                elif strat == 'LongCall':
                    bs_spread_entry = max(0.10, bs_call_price(price_entry, long_strike, t_entry_yrs, 0.04, hv_val))
                elif strat == 'LongPut':
                    bs_spread_entry = max(0.10, bs_put_price(price_entry, long_strike, t_entry_yrs, 0.04, hv_val))
                else: # ShortPut
                    bs_spread_entry = max(0.10, bs_put_price(price_entry, short_strike, t_entry_yrs, 0.04, hv_val))

                # Bewakende Profit Stop monitoring (60%, 70%, 100% of trendverzwakking EMA5 < EMA13)
                if profit_target_pct is not None and profit_target_pct > 0:
                    target_frac = float(profit_target_pct) / 100.0
                    for day_idx, (bar_dt, bar_row) in enumerate(df_trade_period.iterrows(), start=1):
                        p_curr = float(bar_row['Close'])
                        rem_days = max(0, trading_days - day_idx)
                        t_rem_yrs = max(1e-4, (rem_days * (dte_val / trading_days)) / 365.0)

                        if strat == 'BullPut':
                            p_short = bs_put_price(p_curr, short_strike, t_rem_yrs, 0.04, hv_val)
                            p_long = bs_put_price(p_curr, long_strike, t_rem_yrs, 0.04, hv_val)
                            curr_spread_val = max(0.0, min(width_val, p_short - p_long))
                            profit_pct_achieved = ((bs_spread_entry - curr_spread_val) / bs_spread_entry) * 100.0
                            curr_unrealized_profit = (profit_pct_achieved / 100.0) * max_profit
                        elif strat == 'BearCall':
                            p_short = bs_call_price(p_curr, short_strike, t_rem_yrs, 0.04, hv_val)
                            p_long = bs_call_price(p_curr, long_strike, t_rem_yrs, 0.04, hv_val)
                            curr_spread_val = max(0.0, min(width_val, p_short - p_long))
                            profit_pct_achieved = ((bs_spread_entry - curr_spread_val) / bs_spread_entry) * 100.0
                            curr_unrealized_profit = (profit_pct_achieved / 100.0) * max_profit
                        elif strat == 'BullCall':
                            p_long = bs_call_price(p_curr, long_strike, t_rem_yrs, 0.04, hv_val)
                            p_short = bs_call_price(p_curr, short_strike, t_rem_yrs, 0.04, hv_val)
                            curr_spread_val = max(0.0, min(width_val, p_long - p_short))
                            spread_gain = curr_spread_val - bs_spread_entry
                            profit_pct_achieved = (spread_gain / max(0.1, (width_val - bs_spread_entry))) * 100.0
                            curr_unrealized_profit = (profit_pct_achieved / 100.0) * max_profit
                        elif strat == 'BearPut':
                            p_long = bs_put_price(p_curr, long_strike, t_rem_yrs, 0.04, hv_val)
                            p_short = bs_put_price(p_curr, short_strike, t_rem_yrs, 0.04, hv_val)
                            curr_spread_val = max(0.0, min(width_val, p_long - p_short))
                            spread_gain = curr_spread_val - bs_spread_entry
                            profit_pct_achieved = (spread_gain / max(0.1, (width_val - bs_spread_entry))) * 100.0
                            curr_unrealized_profit = (profit_pct_achieved / 100.0) * max_profit
                        elif strat == 'LongCall':
                            curr_call_val = bs_call_price(p_curr, long_strike, t_rem_yrs, 0.04, hv_val)
                            profit_pct_achieved = ((curr_call_val - bs_spread_entry) / bs_spread_entry) * 100.0
                            curr_unrealized_profit = (profit_pct_achieved / 100.0) * (credit * 100.0)
                        elif strat == 'LongPut':
                            curr_put_val = bs_put_price(p_curr, long_strike, t_rem_yrs, 0.04, hv_val)
                            profit_pct_achieved = ((curr_put_val - bs_spread_entry) / bs_spread_entry) * 100.0
                            curr_unrealized_profit = (profit_pct_achieved / 100.0) * (credit * 100.0)
                        else: # ShortPut
                            p_short = bs_put_price(p_curr, short_strike, t_rem_yrs, 0.04, hv_val)
                            profit_pct_achieved = ((bs_spread_entry - p_short) / bs_spread_entry) * 100.0
                            curr_unrealized_profit = (profit_pct_achieved / 100.0) * max_profit

                        # Check 1: Profit Target hit (bijv 60% of 70%)
                        if profit_target_pct < 100.0 and profit_pct_achieved >= profit_target_pct:
                            early_exit = True
                            exit_reason = f"🎯 Profit Target {profit_target_pct:.0f}% ({day_idx}d)"
                            days_held = day_idx
                            actual_exit_date = bar_dt
                            realized_pnl = round(max_profit * target_frac, 2) if strat in ['BullPut', 'BearCall', 'ShortPut'] else round(curr_unrealized_profit, 2)
                            win = True
                            status = f"✅ Winst ({profit_target_pct:.0f}% Target na {day_idx}d)"
                            em85_safe = True
                            break

                        # Check 2: Trend Momentum Exit (EMA5 < EMA13)
                        if exit_on_momentum_drop and bool(bar_row.get('Exit_Signal', False)):
                            if curr_unrealized_profit > 0 and day_idx >= 2:
                                early_exit = True
                                exit_reason = f"📈 Trend Exit EMA5<13 ({day_idx}d)"
                                days_held = day_idx
                                actual_exit_date = bar_dt
                                realized_pnl = round(curr_unrealized_profit, 2)
                                win = True
                                status = f"✅ Trend Exit ({day_idx}d, +${realized_pnl:.0f})"
                                em85_safe = True
                                break
                            elif curr_unrealized_profit < -max_loss * 0.75 and day_idx >= 3:
                                early_exit = True
                                exit_reason = f"⚠️ Stoploss Trend Keert ({day_idx}d)"
                                days_held = day_idx
                                actual_exit_date = bar_dt
                                realized_pnl = round(curr_unrealized_profit, 2)
                                win = False
                                status = f"🔴 Stoploss ({day_idx}d, -${abs(realized_pnl):.0f})"
                                em85_safe = False
                                break

                if not early_exit:
                    if strat == 'LongCall':
                        intrinsic_exp = max(0.0, price_exp - long_strike)
                        realized_pnl = round((intrinsic_exp - credit) * 100.0, 2)
                        em85_safe = win or (price_exp > long_strike)
                        if win:
                            status = "✅ Winst (Koers > BEP)"
                            exit_reason = "✅ Expiratie Winst (ITM)"
                        elif intrinsic_exp > 0:
                            status = "🟡 Deels Verlies (Tussen Strike en BEP)"
                            exit_reason = "🟡 Deels Verlies (Expiratie)"
                        else:
                            status = "🔴 Verlies (Waardeloos OTM)"
                            exit_reason = "🔴 Waardeloos OTM (Expiratie)"
                    elif strat == 'LongPut':
                        intrinsic_exp = max(0.0, long_strike - price_exp)
                        realized_pnl = round((intrinsic_exp - credit) * 100.0, 2)
                        em85_safe = win or (price_exp < long_strike)
                        if win:
                            status = "✅ Winst (Koers < BEP)"
                            exit_reason = "✅ Expiratie Winst (ITM)"
                        elif intrinsic_exp > 0:
                            status = "🟡 Deels Verlies (Tussen Strike en BEP)"
                            exit_reason = "🟡 Deels Verlies (Expiratie)"
                        else:
                            status = "🔴 Verlies (Waardeloos OTM)"
                            exit_reason = "🔴 Waardeloos OTM (Expiratie)"
                    elif strat == 'ShortPut':
                        em85_safe = not touched_bep
                        if win:
                            realized_pnl = max_profit
                            status = "✅ Winst (Expiratie OTM / Premie Behouden)"
                            exit_reason = "✅ Expiratie OTM (100% Premie)"
                        elif price_exp >= bep:
                            realized_pnl = round((price_exp - bep) * 100.0, 2)
                            status = "🟡 Deelwinst (Tussen Strike en BEP)"
                            exit_reason = "🟡 Deelwinst (Expiratie)"
                        else:
                            actual_loss = (bep - price_exp) * 100.0
                            loss_capped = min(actual_loss, max_profit * 2.0)
                            realized_pnl = -round(loss_capped, 2)
                            status = "🔴 Verlies (ITM / Aanwijzing)"
                            exit_reason = "🔴 Verlies (ITM / Toewijzing)"
                    else:
                        em85_safe = not touched_bep
                        if win:
                            realized_pnl = max_profit
                            status = "✅ Winst (Expiratie OTM)"
                            exit_reason = "✅ Expiratie OTM (100% Premie)"
                        elif touched_bep and not breached_short:
                            realized_pnl = max_profit * 0.5
                            status = "🟡 BEP Touch (Gered)"
                            exit_reason = "🟡 BEP Touch (Gered)"
                        else:
                            realized_pnl = -max_loss
                            status = "🔴 Verlies (ITM)"
                            exit_reason = "🔴 Verlies (ITM Expiratie)"

                # Bereken Ultieme AG-Score (0-100) voor de trade
                ev_est = (pop_est / 100.0 * max_profit) - ((1.0 - pop_est / 100.0) * max_loss)
                bep_dist_pct = (bep_dist / max(0.01, price_entry)) * 100.0
                score_res = AntiGravityGammaThetaEngine.calculate_ultimate_ag_score(
                    pop_adj=pop_est,
                    max_profit=max_profit,
                    max_loss=max_loss,
                    expected_value=ev_est,
                    tei_score=1.2,
                    ttp_days=dte_val * 0.5,
                    dte=dte_val,
                    bep_dist_pct=bep_dist_pct,
                    ds_be=em68 * 1.5,
                    atr_10=max(0.1, price_entry * hv_val / np.sqrt(252)),
                    gamma_cliff=(dte_val <= 3),
                    cue="UPTREND" if is_bullish else "DOWNTREND",
                    max_pain_ok=True,
                    koopadvies_ok=(bep_dist_pct >= 5.0),
                    strategy_type=strat
                )
                ag_score_val = score_res['ultimate_ag_score']

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
                    'AG_Score': ag_score_val,
                    'status': status,
                    'win': win,
                    'em85_safe': em85_safe,
                    'realized_pnl': realized_pnl,
                    'entry_signal': entry_signal_type,
                    'exit_reason': exit_reason,
                    'days_held': days_held,
                    'early_exit': early_exit,
                    'profit_target_hit': "🎯 Profit Target" in exit_reason
                })

        df_res = pd.DataFrame(results)
        if df_res.empty:
            empty_cols = [
                'symbol', 'entry_date', 'exp_date', 'strategy', 'vol_regime', 'hv30_%', 'dte',
                'spread_width', 'underlying_entry', 'underlying_exp', 'optie_strike', 'short_strike',
                'long_strike', 'bep', 'credit', 'EM68', 'EM85', 'em_multiplier_used',
                'em85_dekking_pct', 'pop', 'AG_Score', 'status', 'win', 'em85_safe',
                'realized_pnl', 'entry_signal', 'exit_reason', 'days_held', 'early_exit', 'profit_target_hit'
            ]
            return {
                'summary': {'total_trades': 0, 'wins': 0, 'losses': 0, 'hit_rate': 0.0, 'total_pnl': 0.0, 'avg_pnl': 0.0, 'avg_days_held': 0.0, 'profit_target_hits': 0, 'early_exits': 0},
                'details_df': pd.DataFrame(columns=empty_cols)
            }

        win_count = int(df_res['win'].sum())
        total_count = len(df_res)
        hit_rate = (win_count / total_count) * 100.0
        em85_safe_count = int(df_res['em85_safe'].sum())
        em85_safe_rate = (em85_safe_count / total_count) * 100.0
        total_pnl = float(df_res['realized_pnl'].sum())
        avg_pnl = float(df_res['realized_pnl'].mean())
        avg_pop = float(df_res['pop'].mean())

        # Deciel en Categorie Validatie van de Ultieme AG-Score
        ag_high = df_res[df_res['AG_Score'] >= 80.0]
        ag_mid = df_res[(df_res['AG_Score'] >= 65.0) & (df_res['AG_Score'] < 80.0)]
        ag_low = df_res[df_res['AG_Score'] < 65.0]

        summary = {
            'total_trades': total_count,
            'wins': win_count,
            'losses': total_count - win_count,
            'hit_rate': round(hit_rate, 1),
            'avg_pop': round(avg_pop, 1),
            'avg_ag_score': round(float(df_res['AG_Score'].mean()), 1) if 'AG_Score' in df_res.columns else 0.0,
            'hitrate_ag_80_plus': round((float(ag_high['win'].sum()) / len(ag_high) * 100.0), 1) if not ag_high.empty else 0.0,
            'count_ag_80_plus': len(ag_high),
            'hitrate_ag_65_80': round((float(ag_mid['win'].sum()) / len(ag_mid) * 100.0), 1) if not ag_mid.empty else 0.0,
            'count_ag_65_80': len(ag_mid),
            'hitrate_ag_sub_65': round((float(ag_low['win'].sum()) / len(ag_low) * 100.0), 1) if not ag_low.empty else 0.0,
            'count_ag_sub_65': len(ag_low),
            'em85_safe_rate': round(em85_safe_rate, 1),
            'total_pnl': round(total_pnl, 2),
            'avg_pnl': round(avg_pnl, 2),
            'avg_days_held': round(float(df_res['days_held'].mean()), 1) if 'days_held' in df_res.columns else dte_val,
            'profit_target_hits': int(df_res['profit_target_hit'].sum()) if 'profit_target_hit' in df_res.columns else 0,
            'early_exits': int(df_res['early_exit'].sum()) if 'early_exit' in df_res.columns else 0
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
            use_dual_trigger=sidebar_params.get('use_dual_trigger', False),
            profit_target_pct=sidebar_params.get('profit_target_pct', 70.0),
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
            use_dual_trigger=standard_params.get('use_dual_trigger', False),
            profit_target_pct=standard_params.get('profit_target_pct', 70.0),
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

    def quick_optimize_stock(self, symbol: str, trades_per_symbol: int = 3, log_callback=None) -> dict:
        """
        Voert een snelle, aandeel-specifieke sweep uit (in ca. 10-20 sec) over 5 EM-niveaus
        en geschikte breedtes om het optimale handelsprofiel voor dit aandeel te bepalen.
        Retourneert een kant-en-klaar profiel-dictionary voor StockProfileManager.
        """
        def log(msg):
            if log_callback:
                log_callback(msg)
            else:
                try:
                    print(msg)
                except UnicodeEncodeError:
                    print(msg.encode('ascii', 'ignore').decode('ascii'))

        sym = symbol.upper().strip()
        log(f"⚡ Snelle optimalisatiesweep starten voor {sym}...")

        # 1. Haal koers op om verstandige breedte-range te bepalen
        best_width = 10.0
        try:
            t = yf.Ticker(sym)
            hist = t.history(period="5d")
            if not hist.empty:
                cur_price = float(hist['Close'].iloc[-1])
                if cur_price < 30:
                    best_width = 2.5
                elif cur_price < 60:
                    best_width = 5.0
                elif cur_price > 350:
                    best_width = 15.0
                else:
                    best_width = 10.0
        except Exception:
            best_width = 10.0

        # 2. Sweep over 5 representatieve EM-multipliers
        multipliers = [1.0, 1.20, 1.44, 1.65, 1.85]
        best_mult = 1.44
        best_pnl = -99999.0
        best_hit_rate = 0.0

        for mult in multipliers:
            try:
                res = self.run_backtest(
                    symbols=[sym], 
                    trades_per_symbol=trades_per_symbol, 
                    em_multiplier=mult, 
                    spread_width=best_width,
                    dte=21
                )
                s = res.get('summary', {})
                pnl = s.get('avg_pnl', 0.0)
                hr = s.get('hit_rate', 0.0)
                
                # Winst maximaliseren met minstens 60% hit rate als voorkeur
                if (pnl > best_pnl and hr >= 60.0) or (best_pnl == -99999.0):
                    best_pnl = pnl
                    best_mult = mult
                    best_hit_rate = hr
                elif pnl > (best_pnl + 15.0):
                    best_pnl = pnl
                    best_mult = mult
                    best_hit_rate = hr
            except Exception as e:
                log(f"   ⚠️ Fout tijdens sweep {mult}x EM voor {sym}: {e}")

        # 3. Bereken dynamisch winstdoel (profit target)
        if best_mult <= 1.20:
            profit_target = 50.0
        elif best_mult >= 1.60:
            profit_target = 75.0
        else:
            profit_target = 65.0

        today_str = datetime.date.today().strftime('%Y-%m-%d')
        profile_data = {
            'symbol': sym,
            'best_em_multiplier': round(float(best_mult), 2),
            'best_width': float(best_width),
            'best_min_dte': 14,
            'best_max_dte': 25,
            'best_min_bep_dist': 6.0 if best_mult <= 1.44 else 7.5,
            'profit_target_pct': float(profit_target),
            'last_sweep_date': today_str,
            'hit_rate': round(float(best_hit_rate), 1),
            'avg_pnl': round(float(best_pnl if best_pnl != -99999.0 else 0.0), 2),
            'total_trades': int(trades_per_symbol),
            'status': 'VALID',
            'notes': f'Geoptimaliseerd via snelle sweep ({best_mult:.2f}x EM, ${best_width:.1f} breedte)'
        }

        log(f"🎯 Optimum voor {sym}: {best_mult:.2f}x EM, ${best_width:.1f} breedte, winstdoel {profit_target:.0f}% (Hit Rate: {best_hit_rate:.1f}%, Gem PnL: ${best_pnl:.2f})")
        return profile_data

if __name__ == '__main__':
    tester = SpreadHitRateTester()
    opt_res = tester.optimize_em_multipliers()
    print("Sweep results:\n", opt_res['sweep_df'][['multiplier_name', 'hit_rate', 'avg_credit', 'avg_pnl', 'total_pnl']])
