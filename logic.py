import pandas as pd
import numpy as np
import math
from py_vollib.black_scholes.greeks.analytical import delta, gamma, vega, theta
from py_vollib.black_scholes import black_scholes
from risk_model import get_bs_risk_metrics

class BjerksundStensland2002:
    @staticmethod
    def norm_cdf(x):
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    @staticmethod
    def bvn_cdf(a, b, rho):
        x = [0.24840615, 0.39233107, 0.21141819, 0.03324666, 0.00082485]
        y = [0.10024215, 0.48281397, 1.06094980, 1.77972940, 2.73970440]
        def g(h, k, r):
            if h <= 0 and k <= 0:
                res = 0.0
                r_prime = math.sqrt(max(1e-10, 1 - r**2))
                for i in range(5):
                    for j in range(5):
                        res += x[i] * x[j] * math.exp(-(y[i]**2 + y[j]**2 - 2*r*y[i]*y[j]) / (2 * r_prime**2))
                return (r_prime / (2 * math.pi)) * res
            return 0
        if a <= 0 and b <= 0: return g(float(-a), float(-b), rho)
        elif a <= 0 and b >= 0: return BjerksundStensland2002.norm_cdf(a) - g(float(-a), float(b), -rho)
        elif a >= 0 and b <= 0: return BjerksundStensland2002.norm_cdf(b) - g(float(a), float(-b), -rho)
        else: return BjerksundStensland2002.norm_cdf(a) + BjerksundStensland2002.norm_cdf(b) - 1 + g(float(a), float(b), rho)

    @staticmethod
    def phi(S, T, gamma, H, I, r, b, sigma):
        if T <= 0: return 0
        lam = -r + gamma * b + 0.5 * gamma * (gamma - 1) * sigma**2
        kappa = 2 * b / sigma**2 + (2 * gamma - 1)
        def d_val(S_in, T_in, H_in):
            return -(math.log(max(1e-10, S_in / H_in)) + (b + (gamma - 0.5) * sigma**2) * T_in) / (max(1e-10, sigma * math.sqrt(T_in)))
        d1 = d_val(S, T, H)
        d2 = d_val(I**2 / S if S > 0 else 1e-10, T, H)
        return math.exp(lam * T) * (S**gamma) * (BjerksundStensland2002.norm_cdf(d1) - (I / S)**kappa * BjerksundStensland2002.norm_cdf(d2))

    @staticmethod
    def psi(S, T, t1, gamma, H, I, I1, r, b, sigma):
        if T <= 0 or t1 <= 0: return 0
        lam = -r + gamma * b + 0.5 * gamma * (gamma - 1) * sigma**2
        kappa = 2 * b / sigma**2 + (2 * gamma - 1)
        rho = math.sqrt(t1 / T)
        def e_val(S_in, T_in, H_in):
            return -(math.log(max(1e-10, S_in / H_in)) + (b + (gamma - 0.5) * sigma**2) * T_in) / (max(1e-10, sigma * math.sqrt(T_in)))
        e1, e2, e3, e4 = e_val(S, t1, I1), e_val(S, T, H), e_val(I1**2 / S if S > 0 else 1e-10, t1, I1), e_val(I1**2 / S if S > 0 else 1e-10, T, H)
        term1, term2 = BjerksundStensland2002.bvn_cdf(e1, e2, rho), (I1 / S)**kappa * BjerksundStensland2002.bvn_cdf(e3, e4, rho)
        return math.exp(lam * T) * (S**gamma) * (term1 - term2)

    @staticmethod
    def price_american_option(right, S, K, T, r, q, sigma):
        intrinsic = max(0.0, S - K if right.lower().startswith('c') else K - S)
        if T <= 0: return intrinsic
        if right.lower().startswith('p'):
            # Put-Call Symmetry for Put bounded by intrinsic & BS
            try:
                bs_put = black_scholes('p', S, K, T, r, sigma)
            except Exception:
                bs_put = intrinsic
            try:
                bj_put = BjerksundStensland2002.price_american_option('c', K, S, T, q, r, sigma)
            except Exception:
                bj_put = intrinsic
            return max(intrinsic, bs_put, bj_put)
        
        b = r - q
        if b >= r: return BjerksundStensland2002.black_scholes_call(S, K, T, r, q, sigma)
        beta = (0.5 - b / sigma**2) + math.sqrt((b / sigma**2 - 0.5)**2 + 2 * r / sigma**2)
        B_inf, B_0 = K * beta / (beta - 1), max(K, (r / q) * K if q > 0 else K)
        t1 = 0.5 * (math.sqrt(5) - 1) * T
        def get_I(time):
            h = -(b * time + 2 * sigma * math.sqrt(time)) * (K**2 / ((B_inf - B_0) * B_0))
            return B_0 + (B_inf - B_0) * (1 - math.exp(h))
        I1, I2 = get_I(t1), get_I(T)
        alpha1, alpha2 = (I1 - K) * I1**(-beta), (I2 - K) * I2**(-beta)
        c1 = alpha2 * S**beta - alpha2 * BjerksundStensland2002.phi(S, t1, beta, I2, I2, r, b, sigma)
        c2 = BjerksundStensland2002.phi(S, t1, 1, I2, I2, r, b, sigma) - BjerksundStensland2002.phi(S, t1, 1, I1, I2, r, b, sigma)
        c3 = K * BjerksundStensland2002.phi(S, t1, 0, I2, I2, r, b, sigma) - K * BjerksundStensland2002.phi(S, t1, 0, I1, I2, r, b, sigma)
        c4 = alpha1 * BjerksundStensland2002.phi(S, t1, beta, I1, I2, r, b, sigma)
        c5 = alpha1 * BjerksundStensland2002.psi(S, T, t1, beta, I1, I2, I1, r, b, sigma)
        c6 = BjerksundStensland2002.psi(S, T, t1, 1, I1, I2, I1, r, b, sigma) - BjerksundStensland2002.psi(S, T, t1, 1, K, I2, I1, r, b, sigma)
        c7 = K * BjerksundStensland2002.psi(S, T, t1, 0, I1, I2, I1, r, b, sigma) - K * BjerksundStensland2002.psi(S, T, t1, 0, K, I2, I1, r, b, sigma)
        return max(intrinsic, c1 + c2 - c3 + c4 - c5 + c6 - c7)

    @staticmethod
    def black_scholes_call(S, K, T, r, q, sigma):
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return S * math.exp(-q * T) * BjerksundStensland2002.norm_cdf(d1) - K * math.exp(-r * T) * BjerksundStensland2002.norm_cdf(d2)

class SpreadScanner:
    def __init__(self, ib_client=None):
        self.ib_client = ib_client
        self.log_func = None

    def calculate_ema(self, df, span):
        """Calculates Exponential Moving Average."""
        if 'close' not in df.columns: return pd.Series()
        return df['close'].ewm(span=span, adjust=False).mean()

    def calculate_rsi(self, series, period=14):
        """Calculates Relative Strength Index (RSI)."""
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        # Exponentially weighted if preferred, but simple rolling mean is standard for basic RSI
        # TradingView uses RMA (Running Moving Average) which is like an EMA with alpha=1/n
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()

        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50)

    def calculate_stoch_rsi(self, df, rsi_period=14, stoch_period=9, k_period=3, d_period=6):
        """
        Calculates Stochastic RSI (similar to TradingView).
        Parameters: 14, 9, 3, 6 (RSI, Stoch, K_smooth, D_smooth)
        """
        if 'close' not in df.columns: return pd.DataFrame()
        
        rsi = self.calculate_rsi(df['close'], rsi_period)
        
        # Stochastic component: (RSI - Lowest RSI) / (Highest RSI - Lowest RSI)
        rsi_min = rsi.rolling(window=stoch_period).min()
        rsi_max = rsi.rolling(window=stoch_period).max()
        
        stoch_rsi = (rsi - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan)
        stoch_rsi = stoch_rsi.fillna(0) * 100
        
        # K and D smoothing
        # TradingView uses SMA for smoothing K and D in Stochastic RSI
        k = stoch_rsi.rolling(window=k_period).mean()
        d = k.rolling(window=d_period).mean()
        
        return pd.DataFrame({'k': k, 'd': d}, index=df.index)

    def get_technical_signals(self, hist_df, price):
        """
        Analyzes historical data for Stoch RSI entry signals and EMA crossovers.
        Returns a dict with signal status and descriptive text.
        """
        signals = {
            'ema_status': "N/A",
            'stoch_rsi_status': "N/A",
            'entry_a': False, # Bullish Cross < 20
            'entry_b': False, # Blues (K) rising 20-60
            'entry_c': False, # K crosses 50
            'passed': False
        }
        
        if hist_df.empty: return signals
        
        # 1. EMA 8/50 and EMA 20/50 Crossover Analysis
        ema8 = self.calculate_ema(hist_df, 8)
        ema20 = self.calculate_ema(hist_df, 20)
        ema50 = self.calculate_ema(hist_df, 50)
        
        if len(ema8) > 2 and len(ema50) > 2:
            current_8 = float(ema8.iloc[-1])
            current_50 = float(ema50.iloc[-1])
            
            # Find the most recent crossover
            cross_diff = (ema8 > ema50).astype(int).diff()
            cross_indices = cross_diff[cross_diff != 0].index
            
            if not cross_indices.empty:
                last_cross_idx = cross_indices[-1]
                # Calculate distance in bars
                bars_ago = (len(hist_df.index) - 1) - hist_df.index.get_loc(last_cross_idx)
                direction = "Bullish" if cross_diff.loc[last_cross_idx] == 1 else "Bearish"
                
                fresh_tag = " [FRESH]" if bars_ago <= 3 else ""
                signals['ema_status'] = f"{direction} Cross ({bars_ago}d ago){fresh_tag}"
            else:
                signals['ema_status'] = "Positive" if current_8 > current_50 else "Negative"

        if len(ema20) > 2 and len(ema50) > 2:
            c_ema20 = float(ema20.iloc[-1])
            c_ema50 = float(ema50.iloc[-1])
            cross_20_50 = (ema20 > ema50).astype(int).diff()
            c_indices = cross_20_50[cross_20_50 != 0].index
            if not c_indices.empty:
                l_idx = c_indices[-1]
                b_ago = (len(hist_df.index) - 1) - hist_df.index.get_loc(l_idx)
                dir_2050 = "Bullish (EMA20 > EMA50)" if cross_20_50.loc[l_idx] == 1 else "Bearish (EMA20 < EMA50)"
                signals['ema20_50_status'] = f"{dir_2050} ({b_ago}d ago)"
            else:
                signals['ema20_50_status'] = "Bullish (EMA20 > EMA50)" if c_ema20 > c_ema50 else "Bearish (EMA20 < EMA50)"
        else:
            signals['ema20_50_status'] = "N/A"

        # 2. Stoch RSI Analysis (14, 9, 3, 6)
        stoch = self.calculate_stoch_rsi(hist_df, 14, 9, 3, 6)
        if not stoch.empty and len(stoch) > 2:
            k = stoch['k'].values
            d = stoch['d'].values
            
            curr_k, curr_d = k[-1], d[-1]
            prev_k, prev_d = k[-2], d[-2]
            
            # Entry A: Bullish Cross (K > D) below 20
            signals['entry_a'] = (curr_k > curr_d) and (prev_k <= prev_d) and (curr_k < 20)
            
            # Entry B: Blauwe lijn (K) goes up above 20 to 60
            # Condition: K is currently between 20 and 60 AND K is higher than previous bar
            signals['entry_b'] = (curr_k > 20) and (curr_k < 60) and (curr_k > prev_k)
            
            # Entry C: Momentum shift above 50
            signals['entry_c'] = (curr_k > 50) and (prev_k <= 50)

            # Combined status text
            status_parts = []
            if signals['entry_a']: status_parts.append("Entry A (Cross < 20)")
            if signals['entry_b']: status_parts.append("Entry B (Rising 20-60)")
            if signals['entry_c']: status_parts.append("Entry C (Cross > 50)")
            
            if status_parts:
                signals['stoch_rsi_status'] = " + ".join(status_parts)
                signals['passed'] = True
            else:
                signals['stoch_rsi_status'] = f"Neutral (K={curr_k:.1f})"
                
        return signals

    def predict_1month_trend(self, hist_df):
        """
        Calculates a multi-factor 1-month directional forecast (Stijging vs Daling) for the next 30 days.
        Combines 4 independent technical pillars:
        1. 30-Day Linear Regression Slope (% angle)
        2. MACD (12, 26, 9) Momentum & Histogram
        3. DMI / ADX Direction (+DI vs -DI) over 14 bars
        4. EMA 20 & EMA 50 Structural Alignment (Price > EMA20 > EMA50)
        
        Returns a dict with score (-4 to +4) and forecast.
        """
        result = {
            'score': 0,
            'forecast': "Zijwaarts / Neutraal",
            'confidence': 0.5,
            'passed_bullish': False,
            'passed_bearish': False,
            'details': {}
        }
        
        if hist_df is None or hist_df.empty or 'close' not in hist_df.columns or len(hist_df) < 30:
            return result

        closes = hist_df['close'].astype(float).values
        price = closes[-1]
        score = 0
        details = {}

        # 1. 30-Day Linear Regression Slope
        recent_closes = closes[-30:]
        x = np.arange(len(recent_closes))
        slope, intercept = np.polyfit(x, recent_closes, 1)
        slope_pct = (slope * 30 / price) * 100.0 if price > 0 else 0.0
        
        if slope_pct >= 1.2:
            score += 1
            details['regression'] = f"Bullish (Stijgingshoek +{slope_pct:.1f}%)"
        elif slope_pct <= -1.2:
            score -= 1
            details['regression'] = f"Bearish (Dalingshoek {slope_pct:.1f}%)"
        else:
            details['regression'] = f"Neutraal (Hoek {slope_pct:.1f}%)"

        # 2. MACD (12, 26, 9)
        ema12 = hist_df['close'].ewm(span=12, adjust=False).mean()
        ema26 = hist_df['close'].ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - signal_line
        
        curr_macd_hist = float(macd_hist.iloc[-1])
        if curr_macd_hist > 0:
            score += 1
            details['macd'] = f"Bullish (Hist +{curr_macd_hist:.2f})"
        else:
            score -= 1
            details['macd'] = f"Bearish (Hist {curr_macd_hist:.2f})"

        # 3. DMI (+DI vs -DI over 14 bars)
        if 'high' in hist_df.columns and 'low' in hist_df.columns:
            highs = hist_df['high'].astype(float).values
            lows = hist_df['low'].astype(float).values
            
            up_move = highs[1:] - highs[:-1]
            down_move = lows[:-1] - lows[1:]
            
            plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
            minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
            
            tr1 = highs[1:] - lows[1:]
            tr2 = np.abs(highs[1:] - closes[:-1])
            tr3 = np.abs(lows[1:] - closes[:-1])
            tr = np.maximum(tr1, np.maximum(tr2, tr3))
            
            period = 14
            if len(tr) >= period:
                tr_smooth = pd.Series(tr).ewm(alpha=1/period, adjust=False).mean().values
                plus_di = 100 * (pd.Series(plus_dm).ewm(alpha=1/period, adjust=False).mean().values / np.maximum(1e-5, tr_smooth))
                minus_di = 100 * (pd.Series(minus_dm).ewm(alpha=1/period, adjust=False).mean().values / np.maximum(1e-5, tr_smooth))
                
                curr_p_di = float(plus_di[-1])
                curr_m_di = float(minus_di[-1])
                
                if curr_p_di > curr_m_di:
                    score += 1
                    details['dmi'] = f"Bullish (+DI {curr_p_di:.1f} > -DI {curr_m_di:.1f})"
                else:
                    score -= 1
                    details['dmi'] = f"Bearish (-DI {curr_m_di:.1f} > +DI {curr_p_di:.1f})"
            else:
                details['dmi'] = "N/A"
        else:
            details['dmi'] = "N/A"

        # 4. EMA 20 vs EMA 50 Alignment
        ema20 = self.calculate_ema(hist_df, 20)
        ema50 = self.calculate_ema(hist_df, 50)
        
        if not ema20.empty and not ema50.empty:
            c_ema20 = float(ema20.iloc[-1])
            c_ema50 = float(ema50.iloc[-1])
            if price > c_ema20 and c_ema20 > c_ema50:
                score += 1
                details['ema_structure'] = f"Bullish (Koers ${price:.2f} > EMA20 ${c_ema20:.2f} > EMA50 ${c_ema50:.2f})"
            elif price < c_ema20 and c_ema20 < c_ema50:
                score -= 1
                details['ema_structure'] = f"Bearish (Koers ${price:.2f} < EMA20 ${c_ema20:.2f} < EMA50 ${c_ema50:.2f})"
            else:
                details['ema_structure'] = f"Neutraal (EMA20 ${c_ema20:.2f}, EMA50 ${c_ema50:.2f})"
        else:
            details['ema_structure'] = "N/A"

        # Final Evaluation
        if score >= 2:
            forecast = "Duidelijk Verwachte Stijging (Bullish)"
        elif score <= -2:
            forecast = "Duidelijk Verwachte Daling (Bearish)"
        else:
            forecast = "Zijwaarts / Neutraal"

        result['score'] = score
        result['forecast'] = forecast
        result['confidence'] = min(1.0, abs(score) / 4.0)
        result['passed_bullish'] = score >= 2
        result['passed_bearish'] = score <= -2
        result['details'] = details
        return result

    def filter_symbols_by_ema(self, symbols_data, ema_spans, direction='bull', ema_crossover=False, ema20_50_crossover=False):
        """
        Filters symbols based on EMA trend and optionally crossovers.
        direction: 'bull' (Price > EMA) or 'bear' (Price < EMA)
        ema_crossover: If True, checks if EMA 8 > EMA 50.
        ema20_50_crossover: If True, checks if EMA 20 > EMA 50 (for bull) or EMA 20 < EMA 50 (for bear).
        """
        passed_symbols = []
        
        for symbol, data in symbols_data.items():
            price = data.get('price', 0)
            hist_df = data.get('history', pd.DataFrame())
            
            if hist_df.empty or price <= 0:
                continue
            
            # Check standard EMAs
            passes_all = True
            for span in ema_spans:
                ema_series = self.calculate_ema(hist_df, span)
                if ema_series.empty:
                    passes_all = False
                    break
                
                ema_value = ema_series.iloc[-1]
                if direction == 'bull':
                    if price < ema_value:
                        passes_all = False
                        break
                elif direction == 'bear':
                    if price > ema_value:
                        passes_all = False
                        break
            
            if not passes_all:
                continue

            # Check Crossover (EMA 8 > EMA 50)
            if ema_crossover:
                ema8 = self.calculate_ema(hist_df, 8)
                ema50 = self.calculate_ema(hist_df, 50)
                
                if ema8.empty or ema50.empty or len(ema8) < 1:
                    continue
                
                if ema8.iloc[-1] <= ema50.iloc[-1]:
                    continue  # Fail crossover

            # Check Crossover (EMA 20 vs EMA 50)
            if ema20_50_crossover:
                ema20 = self.calculate_ema(hist_df, 20)
                ema50 = self.calculate_ema(hist_df, 50)
                
                if ema20.empty or ema50.empty or len(ema20) < 1:
                    continue
                
                if direction == 'bull' and ema20.iloc[-1] <= ema50.iloc[-1]:
                    continue  # Fail bull crossover
                elif direction == 'bear' and ema20.iloc[-1] >= ema50.iloc[-1]:
                    continue  # Fail bear crossover
            
            passed_symbols.append(symbol)
        
        return passed_symbols

    def find_technical_levels(self, hist_df, ref_price=None):
        """
        Identifies significant support and resistance levels from historical data.
        Returns a dict with 'supports' and 'resistances' lists (3 each, sorted).
        Enforces a minimum spacing between levels.
        """
        if hist_df.empty:
            return {'supports': [], 'resistances': []}
            
        # Case-insensitive column handling
        df = hist_df.copy()
        df.columns = [c.lower() for c in df.columns]
        
        if 'low' not in df.columns or 'high' not in df.columns:
            return {'supports': [], 'resistances': []}
            
        lows = df['low'].values
        highs = df['high'].values
        
        # Use provided ref_price or fallback to latest close from history
        if ref_price is None or ref_price <= 0:
            ref_price = float(df['close'].iloc[-1]) if 'close' in df.columns else float(lows[-1])
        
        raw_supports = []
        raw_resistances = []
        
        # Window for pivot detection
        window = 3 # Reduced from 5 for more sensitivity
        for i in range(window, len(lows) - window):
            # Pivot Low (Support)
            if lows[i] == min(lows[i-window : i+window+1]):
                raw_supports.append(float(lows[i]))
            # Pivot High (Resistance)
            if highs[i] == max(highs[i-window : i+window+1]):
                raw_resistances.append(float(highs[i]))
                
        raw_supports = sorted(list(set(raw_supports)), reverse=True)
        raw_resistances = sorted(list(set(raw_resistances)))
        
        def filter_levels(levels, start_price, is_support=True):
            filtered = []
            last_level = start_price
            
            for lvl in levels:
                # Keep only relevant levels (supports < price, resistances > price)
                if is_support and lvl >= start_price: continue
                if not is_support and lvl <= start_price: continue
                
                # Check spacing (relaxed for more results)
                diff_pct = abs(lvl - last_level) / last_level if last_level != 0 else 1.0
                
                if diff_pct >= 0.002: # Relaxed from 0.02
                    filtered.append(lvl)
                    last_level = lvl
                
                if len(filtered) >= 5: # Increased count
                    break
            return filtered

        return {
            'supports': sorted(filter_levels(raw_supports, ref_price, True)),
            'resistances': sorted(filter_levels(raw_resistances, ref_price, False))
        }

    def assess_market_sentiment(self, price, hist_df, indicators):
        """
        Implements the decision tree from the user document.
        Returns 'Bullish', 'Bearish', or 'Neutral'.
        """
        if hist_df is None or hist_df.empty:
            return "Neutral"
            
        ema20_series = self.calculate_ema(hist_df, 20)
        ema50_series = self.calculate_ema(hist_df, 50)
        
        if len(ema20_series) < 1 or len(ema50_series) < 1:
            return "Neutral"
            
        ema20 = ema20_series.iloc[-1]
        ema50 = ema50_series.iloc[-1]
        
        # 1. Trend Direction
        trend_bull = price > ema20 and price > ema50
        trend_bear = price < ema20 and price < ema50
        
        # 2. Market Structure (GEX/DEX/Max Pain)
        # Indicators from PDF
        gex = indicators.get('gex', 0)     # Negative for Bullish
        dex = indicators.get('dex', 0)     # Positive for Bullish
        pc_ratio = indicators.get('pc_ratio', 1.0) # < 0.7 for Bullish
        
        # Bullish rules: Trend Up + GEX < 0 + DEX > 0 + P/C < 0.7
        is_bullish = trend_bull and gex < 0 and dex > 0 and pc_ratio < 0.7
        
        # Bearish rules: Trend Down + GEX > 0 + DEX < 0 + P/C > 1.0
        is_bearish = trend_bear and gex > 0 and dex < 0 and pc_ratio > 1.0
        
        if is_bullish: return "Bullish"
        if is_bearish: return "Bearish"
        return "Neutral"

    def calculate_greeks(self, row, underlying_price, risk_free_rate=0.04):
        """
        Calculates Greeks using py_vollib.
        Rounds to 3 decimals.
        """
        try:
            # T = Time to expiration in years
            T = row['dte'] / 365.0
            if T <= 0: T = 0.001
            
            # Sigma = IV
            sigma = row.get('iv', 0.0) 
            if sigma == 0: sigma = 0.2 # Fallback
            
            flag = row['right'].lower() # 'c' or 'p'
            K = row['strike_buy'] # Strike
            S = underlying_price
            
            # Use Bjerksund-Stensland for American Options Price
            # But for Greeks, analytical BS is usually a good enough proxy if we don't have analytical B-S 2002 greeks.
            # We will use finite difference for Delta if needed, but for now analytical BS is fine.

            q = 0.015 # Target average dividend yield for SPY/Market as base fallback
            d = delta(flag, S, K, T, risk_free_rate, sigma)
            # Adjust delta slightly for dividends if flag is 'c'
            if flag == 'c': d *= math.exp(-q * T)
            else: d *= math.exp(-q * T)
            
            g = gamma(flag, S, K, T, risk_free_rate, sigma) * math.exp(-q * T)
            v = vega(flag, S, K, T, risk_free_rate, sigma) * math.exp(-q * T)
            t = theta(flag, S, K, T, risk_free_rate, sigma)
            
            return pd.Series([round(d, 3), round(g, 3), round(v, 3), round(t, 3)], index=['delta', 'gamma', 'vega', 'theta'])
        except Exception as e:
            # print(f"Greek Calc Error: {e}")
            return pd.Series([0.0, 0.0, 0.0, 0.0], index=['delta', 'gamma', 'vega', 'theta'])

    def generate_spreads(self, chains, strategy, component_price, params, log_func=None):
        """
        Generates spreads based on the provided strategy and parameters.
        chains: List of SecDefOptParams objects (from TWS).
        strategy: 'BullCall', 'BullPut', 'BearCall', 'BearPut'
        component_price: Current price of the underlying.
        params: dict of parameters (width, min_dte, max_dte, etc.)
        log_func: optional callback for debugging
        """
        spreads = []
        
        # Performance/Transparency: Track why we skip
        skips = {
            'dte': 0,
            'strike_range': 0,
            'strategy': 0
        }
        
        # [NEW] Enforce strike range filter (+/- 30% standard, +/- 50% max)
        strike_range_raw = params.get('strike_range_pct', 0.30)
        strike_range_abs = abs(strike_range_raw)
        # Limit between 0.05 and 0.50 for safety
        strike_range_abs = max(0.05, min(0.50, strike_range_abs))
        
        lower_bound = component_price * (1 - strike_range_abs)
        upper_bound = component_price * (1 + strike_range_abs)
        
        valid_expirations = sorted(list(set([exp for chain in chains for exp in chain.expirations])))

        # Handle Synthetic Covered Spreads (PMCC / PMCP - Multi-expiration Diagonals)
        if strategy in ['SynthCoveredCall', 'SynthCoveredPut']:
            now_date = pd.Timestamp.now().normalize()
            all_exp_dtes = []
            for exp in valid_expirations:
                try:
                    t_date = pd.to_datetime(exp)
                    d_val = (t_date - now_date).days
                    all_exp_dtes.append((exp, d_val))
                except Exception:
                    pass
            
            # Short leg candidates: 20 to 50 DTE (sweet spot 30-45 DTE)
            min_short = params.get('min_short_dte', params.get('min_dte', 20))
            max_short = params.get('max_short_dte', params.get('max_dte', 50))
            if min_short < 7: min_short = 14
            short_cands = [e for e in all_exp_dtes if min_short <= e[1] <= max_short]
            if not short_cands:
                short_cands = [e for e in all_exp_dtes if 10 <= e[1] <= 60]
            if not short_cands and all_exp_dtes:
                short_cands = [min(all_exp_dtes, key=lambda x: abs(x[1] - 30))]
                
            # Long leg candidates (LEAPS): >= 180 DTE (typically 180 - 730 DTE)
            min_long = params.get('min_long_dte', 180)
            long_cands = [e for e in all_exp_dtes if e[1] >= min_long]
            if not long_cands:
                long_cands = [e for e in all_exp_dtes if e[1] >= 120]
            if not long_cands and all_exp_dtes:
                longest_exp = max(all_exp_dtes, key=lambda x: x[1])
                if longest_exp[1] >= 60:
                    long_cands = [longest_exp]
            
            # Focus on the most optimal expirations (top 2 short and top 2 long)
            short_cands = sorted(short_cands, key=lambda x: abs(x[1] - 35))[:2]
            long_cands = sorted(long_cands, key=lambda x: abs(x[1] - 300))[:2]
            
            iv_val = params.get('iv', 0.25)
            if iv_val <= 0: iv_val = 0.25
            r = 0.04
            q = 0.015
            
            for s_exp, s_dte in short_cands:
                strikes_short = sorted(list(set([s for chain in chains for s in chain.strikes if s_exp in chain.expirations])))
                t_short = max(0.001, float(s_dte) / 365.0)
                
                for l_exp, l_dte in long_cands:
                    if l_dte <= s_dte: continue # Long must be further out than short
                    strikes_long = sorted(list(set([s for chain in chains for s in chain.strikes if l_exp in chain.expirations])))
                    t_long = max(0.001, float(l_dte) / 365.0)
                    
                    if strategy == 'SynthCoveredCall':
                        # Long Call: Deep ITM (Delta 0.75 to 0.95, sweet spot 0.85-0.90, strike < component_price)
                        long_cands_strikes = []
                        for k in strikes_long:
                            if k >= component_price or k < component_price * 0.50: continue
                            try:
                                d_val = delta('c', component_price, k, t_long, r, iv_val)
                            except Exception:
                                d_val = 0.85
                            if 0.70 <= d_val <= 0.95:
                                p_theo = BjerksundStensland2002.price_american_option('c', component_price, k, t_long, r, q, iv_val)
                                long_cands_strikes.append((k, d_val, p_theo))
                                
                        # Short Call: Standaard OTM ~8%, Delta ~0.25 (range 0.16 - 0.35), premie >= $2.00
                        target_otm = 0.08
                        target_delta = 0.25
                        min_target_prem = 2.0
                        short_cands_strikes = []
                        for k in strikes_short:
                            if k <= component_price or k > component_price * 1.35: continue
                            try:
                                d_val = delta('c', component_price, k, t_short, r, iv_val)
                            except Exception:
                                d_val = 0.25
                            otm_ratio = (k - component_price) / component_price
                            if 0.15 <= d_val <= 0.35:
                                p_theo = BjerksundStensland2002.price_american_option('c', component_price, k, t_short, r, q, iv_val)
                                diff_score = abs(d_val - target_delta) * 100.0 + abs(otm_ratio - target_otm) * 50.0
                                if p_theo < min_target_prem:
                                    diff_score += (min_target_prem - p_theo) * 20.0
                                short_cands_strikes.append((k, d_val, p_theo, diff_score))
                                
                        short_cands_strikes = sorted(short_cands_strikes, key=lambda x: x[3])
                        
                        # Pair them up and enforce IJzeren Regel 1: Strikeverschil > Netto Debit
                        for k_buy, d_buy, p_buy in long_cands_strikes:
                            for k_sell, d_sell, p_sell, _ in short_cands_strikes:
                                width = k_sell - k_buy
                                if width <= 0: continue
                                est_debit = p_buy - p_sell
                                if width <= est_debit:
                                    continue
                                
                                spreads.append({
                                    'symbol': params.get('symbol', ''),
                                    'strategy': strategy,
                                    'expiry': s_exp,
                                    'expiry_long': l_exp,
                                    'dte': s_dte,
                                    'dte_long': l_dte,
                                    'strike_buy': k_buy,
                                    'strike_sell': k_sell,
                                    'right': 'C',
                                    'width': width,
                                    'iv': iv_val
                                })
                                
                    elif strategy == 'SynthCoveredPut':
                        # Long Put: Deep ITM (Delta -0.95 to -0.70, sweet spot -0.90 to -0.85, strike > component_price)
                        long_cands_strikes = []
                        for k in strikes_long:
                            if k <= component_price or k > component_price * 1.50: continue
                            try:
                                d_val = delta('p', component_price, k, t_long, r, iv_val)
                            except Exception:
                                d_val = -0.85
                            if -0.95 <= d_val <= -0.70:
                                p_theo = BjerksundStensland2002.price_american_option('p', component_price, k, t_long, r, q, iv_val)
                                long_cands_strikes.append((k, d_val, p_theo))
                                
                        # Short Put: Standaard OTM ~8% onder koers, Delta ~ -0.25 (range -0.35 tot -0.15)
                        target_otm = 0.08
                        target_delta = -0.25
                        min_target_prem = 2.0
                        short_cands_strikes = []
                        for k in strikes_short:
                            if k >= component_price or k < component_price * 0.65: continue
                            try:
                                d_val = delta('p', component_price, k, t_short, r, iv_val)
                            except Exception:
                                d_val = -0.25
                            otm_ratio = (component_price - k) / component_price
                            if -0.35 <= d_val <= -0.15:
                                p_theo = BjerksundStensland2002.price_american_option('p', component_price, k, t_short, r, q, iv_val)
                                diff_score = abs(d_val - target_delta) * 100.0 + abs(otm_ratio - target_otm) * 50.0
                                if p_theo < min_target_prem:
                                    diff_score += (min_target_prem - p_theo) * 20.0
                                short_cands_strikes.append((k, d_val, p_theo, diff_score))
                                
                        short_cands_strikes = sorted(short_cands_strikes, key=lambda x: x[3])
                        
                        # Pair them up and enforce IJzeren Regel 1: Strikeverschil > Netto Debit
                        for k_buy, d_buy, p_buy in long_cands_strikes:
                            for k_sell, d_sell, p_sell, _ in short_cands_strikes:
                                width = k_buy - k_sell
                                if width <= 0: continue
                                est_debit = p_buy - p_sell
                                if width <= est_debit:
                                    continue
                                
                                spreads.append({
                                    'symbol': params.get('symbol', ''),
                                    'strategy': strategy,
                                    'expiry': s_exp,
                                    'expiry_long': l_exp,
                                    'dte': s_dte,
                                    'dte_long': l_dte,
                                    'strike_buy': k_buy,
                                    'strike_sell': k_sell,
                                    'right': 'P',
                                    'width': width,
                                    'iv': iv_val
                                })
                                
            if spreads:
                return pd.DataFrame(spreads)
            elif log_func:
                log_func(f"   ⚠️ 0 {strategy} kandidaten die voldoen aan de 3 IJzeren Regels (DTE/Delta/Breedte>Debit).")
            return pd.DataFrame(spreads)

        for expiration in valid_expirations:
            # Filter by DTE (Normalized to date to avoid afternoon/evening bias)
            # Use .ceil() or normalization to ensure a Friday-to-Friday count is consistent.
            target_date = pd.to_datetime(expiration)
            now_date = pd.Timestamp.now().normalize()
            dte = (target_date - now_date).days
            
            if dte < params.get('min_dte', 7) or dte > params.get('max_dte', 45):
                skips['dte'] += 1
                continue
                
            # Get strikes for this expiration
            strikes = sorted(list(set([strike for chain in chains for strike in chain.strikes if expiration in chain.expirations])))
            
            # Calculate dynamic Support Level based on UI setting
            min_strike_raw = params.get('min_strike_pct', 0.0)
            itm_level = params.get('itm_support_level', "Standaard")
            if "Standaard" not in itm_level and component_price > 0:
                iv_val = params.get('iv', 0.2) if params.get('iv', 0.2) > 0 else 0.2
                if iv_val < 0.10: iv_val = 0.15 # Vang lege of extreem lage IV spikes op via een realistische bodem
                em_value = component_price * iv_val * ((max(1, dte) / 365.0) ** 0.5)
                if "Niveau 1" in itm_level:
                    safe_dist = em_value
                elif "EM85" in itm_level:
                    safe_dist = em_value * 1.439535
                elif "Niveau 2" in itm_level:
                    safe_dist = em_value * 2.0
                else: 
                    safe_dist = em_value * 2.5
                sign = -1.0 if min_strike_raw < 0 else 1.0
                min_strike_raw = sign * (safe_dist / component_price)

            # 1. Handle Long Strategies
            if strategy in ['LongCall', 'LongPut']:
                right = 'C' if strategy == 'LongCall' else 'P'
                long_focus = params.get('long_focus', 'Beide / Vergelijking')
                for s in strikes:
                    if s < lower_bound or s > upper_bound:
                        skips['strike_range'] += 1
                        continue
                    
                    # Focus filtering: Deep ITM vs ATM vs Beide
                    dist_from_spot = abs(s - component_price) / max(0.01, component_price)
                    if "Deep ITM" in long_focus:
                        # Deep ITM: Calls strikes under spot (delta ~0.80-0.95), Puts strikes above spot
                        if strategy == 'LongCall' and (s > component_price * 0.98 or s < component_price * 0.70):
                            skips['strategy'] += 1
                            continue
                        elif strategy == 'LongPut' and (s < component_price * 1.02 or s > component_price * 1.30):
                            skips['strategy'] += 1
                            continue
                    elif "ATM" in long_focus:
                        # ATM: Near spot (within 5% distance)
                        if dist_from_spot > 0.05:
                            skips['strategy'] += 1
                            continue
                    else:
                        # Beide / Vergelijking: Allow both ITM and ATM strikes
                        if strategy == 'LongCall':
                            if s > component_price * (1 + min_strike_raw):
                                skips['strategy'] += 1
                                continue
                        elif strategy == 'LongPut':
                            if s < component_price * (1 - min_strike_raw):
                                skips['strategy'] += 1
                                continue
                        
                    # In Long strategies, we don't have a width, but we might filter by distance from spot
                    spreads.append({
                        'symbol': params.get('symbol', ''),
                        'strategy': strategy,
                        'expiry': expiration,
                        'dte': dte,
                        'strike_buy': s,
                        'strike_sell': 0.0, # Single leg
                        'right': right,
                        'width': 0.0,
                        'iv': params.get('iv', 0.0)
                    })
                continue # Done with Longs for this expiry

            # 2. Handle Iron Condor
            if strategy == 'IronCondor':
                # An IC is a Bull Put + Bear Call
                # We need to find pairs of spreads
                width = params.get('width', 5)
                for low_strike in strikes:
                    if low_strike < lower_bound or low_strike > upper_bound: continue
                    # Put Spread (Bull Put)
                    strike_p_buy = low_strike
                    strike_p_sell = low_strike + width
                    # Constraint: The entire short Put spread must be below the current spot price
                    if strike_p_sell in strikes and strike_p_sell <= upper_bound and strike_p_sell < component_price:
                        # Call Spread (Bear Call)
                        for high_strike in [s for s in strikes if s > component_price and lower_bound <= s <= upper_bound]:
                            strike_c_sell = high_strike
                            strike_c_buy = high_strike + width
                            if strike_c_buy in strikes and strike_c_buy <= upper_bound:
                                spreads.append({
                                    'symbol': params.get('symbol', ''),
                                    'strategy': 'IronCondor',
                                    'expiry': expiration,
                                    'dte': dte,
                                    'strike_p_buy': strike_p_buy,
                                    'strike_p_sell': strike_p_sell,
                                    'strike_c_sell': strike_c_sell,
                                    'strike_c_buy': strike_c_buy,
                                    'strike_buy': strike_p_buy,  # Legacy field for compatibility
                                    'strike_sell': strike_p_sell, # Legacy field for compatibility
                                    'right': 'IC',
                                    'width': width,
                                    'iv': params.get('iv', 0.0)
                                })
                continue
            
            if strategy in ['BullCall', 'BullPut', 'BearCall', 'BearPut']:
                target_width = params.get('width', 5.0)
                for i, long_strike in enumerate(strikes):
                    if long_strike < lower_bound or long_strike > upper_bound:
                        skips['strike_range'] += 1
                        continue
                        
                    # [IMPROVEMENT] Instead of checking for a single target strike,
                    # find ALL strikes that match the requested width.
                    # This handles non-standard intervals (e.g. 111/119 for width 8)
                    for short_strike in strikes:
                        # Check if this pair matches the width with a small tolerance for floats
                        actual_width = abs(long_strike - short_strike)
                        if abs(actual_width - target_width) > 0.001:
                            continue
                            
                        # Use the pre-calculated dynamic minimum distance offset
                        # min_strike_raw was already calculated at the top of the expiration loop
                        
                        # Pattern check: Bull vs Bear
                        if strategy in ['BullCall', 'BullPut']:
                            strike_range_raw = params.get('strike_range_pct', 0.30)
                            is_below = True if strike_range_raw >= 0 else False
                            
                            if is_below:
                                # Spread placed BELOW the current price
                                if short_strike > component_price * (1 - min_strike_raw):
                                    skips['strategy'] += 1
                                    continue
                                if short_strike <= long_strike: # Buy is further from price
                                    skips['strategy'] += 1
                                    continue
                            else:
                                # Spread placed ABOVE the current price
                                if short_strike < component_price * (1 + min_strike_raw):
                                    skips['strategy'] += 1
                                    continue
                                if long_strike <= short_strike: # Buy is further from price
                                    skips['strategy'] += 1
                                    continue
                        elif strategy in ['BearCall', 'BearPut']:
                            # Rules for Bear spreads (strict according to user):
                            # Buy (long_strike) > Sell (short_strike) > koers (component_price) safely buffered
                            min_allowed_short = component_price * (1 + min_strike_raw)
                            if long_strike <= short_strike:
                                skips['strategy'] += 1
                                continue
                            if short_strike < min_allowed_short:
                                skips['strategy'] += 1
                                continue
                        if short_strike < lower_bound or short_strike > upper_bound:
                            skips['strike_range'] += 1
                            continue
                        
                        right = 'C' if 'Call' in strategy else 'P'
                        
                        spreads.append({
                            'symbol': params.get('symbol', ''),
                            'strategy': strategy,
                            'expiry': expiration,
                            'dte': dte,
                            'strike_buy': long_strike,
                            'strike_sell': short_strike,
                            'right': right,
                            'width': actual_width,
                            'iv': params.get('iv', 0.0)
                        })
                continue
        
        # Log Summary if 0 results
        if not spreads and log_func:
            summary = []
            if skips.get('dte', 0) > 0: summary.append(f"DTE ({skips['dte']})")
            if skips.get('strike_range', 0) > 0: summary.append(f"Strikes ({skips['strike_range']})")
            if skips.get('strategy', 0) > 0: summary.append(f"OTM/ITM ({skips['strategy']})")
            
            if summary:
                log_func(f"   ⚠️ 0 {strategy} kandidaten. Skips door: {', '.join(summary)}")
              
        return pd.DataFrame(spreads)
    def parse_barchart_flow(self, df, min_size=100, strict_codes=False, log_func=None):
        """
        Parses a Barchart Option Flow DataFrame, filters for 'Smart Money' trades
        (large size + specific execution codes), and proposes vertical spreads.
        Supports case-insensitive column mapping and aliases.
        """
        spreads = []
        if df is None or df.empty:
            return pd.DataFrame()

        def get_val(row_dict, aliases, default=None):
            row_keys_lower = {str(k).lower().strip(): k for k in row_dict.keys()}
            for alias in aliases:
                a_lower = alias.lower().strip()
                if a_lower in row_keys_lower:
                    val = row_dict[row_keys_lower[a_lower]]
                    if pd.notna(val):
                        return val
            return default

        def safe_float(v, default=0.0):
            if v is None or pd.isna(v): return default
            try:
                clean_str = str(v).replace('$', '').replace('%', '').replace(',', '').strip()
                return float(clean_str)
            except (ValueError, TypeError):
                return default

        def safe_int(v, default=0):
            if v is None or pd.isna(v): return default
            try:
                clean_str = str(v).replace(',', '').strip()
                return int(float(clean_str))
            except (ValueError, TypeError):
                return default

        total_rows = len(df)
        skipped_type = 0
        skipped_dte = 0
        skipped_size = 0
        skipped_code = 0
        skipped_delta = 0

        valid_codes = ["MLCT", "MLFT", "MLAT", "TLFT", "TLAT", "SLCN", "ISOI", "SLAN", "SLAI"]

        for idx, row in df.iterrows():
            try:
                row_dict = row.to_dict()
                symbol = str(get_val(row_dict, ['Symbol', 'SYMBOL', 'Ticker', 'TICKER', 'Sym'], '')).strip()
                t_type = str(get_val(row_dict, ['Type', 'TYPE', 'Option Type', 'Call/Put', 'Right', 'RIGHT'], '')).upper().strip()
                t_strike = safe_float(get_val(row_dict, ['Strike', 'STRIKE', 'Strike Price'], 0))
                t_price = safe_float(get_val(row_dict, ['Price~', 'PRICE~', 'Price', 'PRICE', 'Underlying Price', 'Latest', 'LATEST', 'Last', 'Spot'], 0))
                t_dte = safe_int(get_val(row_dict, ['DTE', 'dte', 'Days to Expiration', 'Days', 'EXP DTE'], 0))
                t_size = safe_int(get_val(row_dict, ['Size', 'SIZE', 'Volume', 'VOLUME', 'Qty', 'QTY'], 0))
                t_delta = safe_float(get_val(row_dict, ['Delta', 'DELTA'], 0))
                t_code = str(get_val(row_dict, ['Code', 'CODE', 'Trade Code', 'Flags', 'FLAGS'], '')).strip()

                if not symbol or symbol.lower() == 'nan':
                    continue

                if t_type not in ["CALL", "PUT", "C", "P"]:
                    skipped_type += 1
                    continue
                
                if t_type in ["CALL", "C"]: t_type = "CALL"
                elif t_type in ["PUT", "P"]: t_type = "PUT"

                # DTE filter
                if t_dte > 0 and (t_dte < 5 or t_dte > 365):
                    skipped_dte += 1
                    continue

                # Size filter
                if min_size > 0 and t_size > 0 and t_size < min_size:
                    skipped_size += 1
                    continue

                # Code filter
                if strict_codes and t_code and t_code not in valid_codes:
                    skipped_code += 1
                    continue

                short_strike = None
                strategy = None

                # ITM / OTM CALL -> Bull Call Vertical
                if t_type == "CALL":
                    if t_delta != 0 and not (0.15 <= abs(t_delta) <= 0.95):
                        skipped_delta += 1
                        continue
                    strategy = 'BullCall'
                    if t_strike < 180: short_strike = t_strike + 20
                    elif t_strike < 200: short_strike = t_strike + 15
                    else: short_strike = t_strike + 10

                # ITM / OTM PUT -> Bear Put Vertical
                elif t_type == "PUT":
                    if t_delta != 0 and not (0.15 <= abs(t_delta) <= 0.95):
                        skipped_delta += 1
                        continue
                    strategy = 'BearPut'
                    if t_strike > 220: short_strike = t_strike - 20
                    elif t_strike > 200: short_strike = t_strike - 15
                    else: short_strike = t_strike - 10

                if strategy and short_strike:
                    raw_exp = str(get_val(row_dict, ['Expires', 'EXPIRES', 'Expiration', 'EXPIRATION', 'Expiry', 'EXPIRY'], ''))
                    exp_date = ''
                    if raw_exp and raw_exp.lower() != 'nan':
                        if 'T' in raw_exp:
                            exp_date = raw_exp.split('T')[0].replace('-', '')
                        else:
                            exp_date = raw_exp.replace('-', '').replace('/', '')

                    iv_val = safe_float(get_val(row_dict, ['IV', 'iv', 'Implied Volatility', 'IV %'], 0))
                    if iv_val > 5.0: iv_val = iv_val / 100.0

                    prem_val = safe_float(get_val(row_dict, ['Premium', 'PREMIUM', 'Value', 'VALUE'], 0))

                    spreads.append({
                        'symbol': symbol,
                        'strategy': strategy,
                        'expiry': exp_date,
                        'dte': t_dte,
                        'strike_buy': t_strike,
                        'strike_sell': short_strike,
                        'right': 'C' if strategy == 'BullCall' else 'P',
                        'width': abs(t_strike - short_strike),
                        'iv': iv_val,
                        'barchart_size': t_size,
                        'barchart_premium': prem_val
                    })
            except Exception as e:
                if log_func: log_func(f"Fout bij parsen rij {idx}: {e}")
                continue

        if log_func:
            try:
                log_func(f"📊 Barchart Flow Analyse: {total_rows} rijen in CSV -> {len(spreads)} Smart Money Option Flow setup(s) goedgekeurd.")
                if len(spreads) == 0 and total_rows > 0:
                    log_func(f"   ℹ️ Afwijkingen details: Geen Type/Optie kolommen={skipped_type}, DTE={skipped_dte}, Size<{min_size}={skipped_size}, Code={skipped_code}, Delta={skipped_delta}")
            except Exception:
                pass

        return pd.DataFrame(spreads)

    def analyze_market_structure(self, chain_data):
        """
        Analyzes the option chain to find key market levels:
        - Max Pain: Strike with minimum total pain.
        - Call Wall: Strike with maximum Call Open Interest (Resistance).
        - Put Wall: Strike with maximum Put Open Interest (Support).
        - GEX Wall: Strike with maximum Gamma Exposure (Volatility Magnet).
        
        chain_data: DataFrame with columns [strike, right, oi, gamma]
        Returns: dict with keys 'max_pain', 'call_wall', 'put_wall', 'gex_wall'
        """
        result = {'max_pain': 0.0, 'call_wall': 0.0, 'put_wall': 0.0, 'gex_wall': 0.0}
        
        if chain_data.empty or 'openInterest' not in chain_data.columns:
            return result
            
        strikes = sorted(chain_data['strike'].unique())
        
        # 1. Max Pain Calculation
        pain_values = {}
        for price_point in strikes:
            total_pain = 0
            
            # Pain for Calls (if price > strike)
            calls = chain_data[chain_data['right'] == 'C']
            calls = calls[calls['openInterest'] > 0]
            itm_calls = calls[calls['strike'] < price_point]
            if not itm_calls.empty:
                total_pain += ((price_point - itm_calls['strike']) * itm_calls['openInterest']).sum()
            
            # Pain for Puts (if price < strike)
            puts = chain_data[chain_data['right'] == 'P']
            puts = puts[puts['openInterest'] > 0]
            itm_puts = puts[puts['strike'] > price_point]
            if not itm_puts.empty:
                total_pain += ((itm_puts['strike'] - price_point) * itm_puts['openInterest']).sum()
            
            pain_values[price_point] = total_pain
            
        if pain_values:
            # Sort by pain value to find the global minimum and potentially secondary levels
            sorted_pain = sorted(pain_values.items(), key=lambda x: x[1])
            result['max_pain'] = sorted_pain[0][0]
            
            # Find a 'selection' max pain: maybe the most significant local minimum near price?
            # User example: price 125, max pain 95. 
            # For now, let's just provide the top 2 lowest pain levels.
            if len(sorted_pain) > 1:
                result['max_pain_selection'] = sorted_pain[1][0]
            else:
                result['max_pain_selection'] = result['max_pain']
            
        # 2. Call Wall (Max Call OI)
        calls = chain_data[chain_data['right'] == 'C']
        if not calls.empty and calls['openInterest'].max() > 0:
            result['call_wall'] = calls.loc[calls['openInterest'].idxmax()]['strike']
            
        # 3. Put Wall (Max Put OI)
        puts = chain_data[chain_data['right'] == 'P']
        if not puts.empty and puts['openInterest'].max() > 0:
            result['put_wall'] = puts.loc[puts['openInterest'].idxmax()]['strike']
            
        # 4. GEX Wall (Max Gamma * OI) -- simplified proxy for GEX
        # Total GEX per strike = (Gamma * OI * Spot). Spot is constant, so prioritize Gamma*OI.
        # Sum absolute gamma? Calls + Puts? Usually net gamma exposure matters.
        # For a "Magnet", absolute gamma is often used (high liquidity/hedging activity).
        if 'gamma' in chain_data.columns:
             # Calculate GEX proxy per row
             # Use absolute gamma * OI
             chain_data['gex_proxy'] = chain_data['gamma'].abs() * chain_data['openInterest']
             
             # Group by strike
             gex_by_strike = chain_data.groupby('strike')['gex_proxy'].sum()
             if not gex_by_strike.empty and gex_by_strike.max() > 0:
                  result['gex_wall'] = gex_by_strike.idxmax()
                  
             # 5. Gamma Flip (Zero Gamma Level)
             result['gamma_flip'] = self.calculate_gamma_flip(chain_data)
        
        return result

    def calculate_gamma_flip(self, chain_data):
        """
        Calculates the price level where net Market Maker Gamma exposure crosses zero.
        Simplified version based on Call Gamma - Put Gamma.
        """
        if 'gamma' not in chain_data.columns or 'openInterest' not in chain_data.columns:
            return 0.0
            
        # Group by strike
        by_strike = chain_data.groupby(['strike', 'right'])['gamma'].sum()
        strikes = sorted(chain_data['strike'].unique())
        
        net_gamma_by_strike = {}
        for s in strikes:
            # GEX = (Call Gamma - Put Gamma) * OI * Spot^2 or simplified magnitude
            # We use (Call Gamma * OI - Put Gamma * OI) as a proxy
            cg = chain_data[(chain_data['strike'] == s) & (chain_data['right'] == 'C')]
            pg = chain_data[(chain_data['strike'] == s) & (chain_data['right'] == 'P')]
            
            c_gex = (cg['gamma'] * cg['openInterest']).sum() if not cg.empty else 0
            p_gex = (pg['gamma'] * pg['openInterest']).sum() if not pg.empty else 0
            
            net_gamma_by_strike[s] = c_gex - p_gex
            
        # Find where it crosses zero (closest strike)
        if not net_gamma_by_strike: return 0.0
        
        # Simple linear approximation or just the closest strike to zero
        flip_strike = min(net_gamma_by_strike, key=lambda k: abs(net_gamma_by_strike[k]))
        return flip_strike

    def calculate_metrics(self, spreads_df, ib_client, symbol, underlying_price=None, chain_data=None, underlying_iv=0.0, hist_iv_df=None, log_func=None, koopadvies_p=0.01, atr_10=0.0, target_profit_usd=5.0):
        """
        Enriches spreads using Real Prices (Bid/Ask) if available in chain_data.
        OPTIMIZED: Uses dictionary lookups and avoids iterrows for high performance.
        """
        self.log_func = log_func
        if spreads_df.empty:
            return spreads_df
        
        if underlying_price is None:
             underlying_price = 100.0 
             
        # 1. Market Structure Analysis (cached)
        market_structure = {'max_pain': 0.0, 'call_wall': 0.0, 'put_wall': 0.0, 'gex_wall': 0.0}
        if chain_data is not None and not chain_data.empty:
            market_structure = self.analyze_market_structure(chain_data)
        
        # 2. Preparation: Build Greek/Price Lookup Table
        # Key: (strike, right) -> (price, series_of_greeks)
        lookup = {}
        if chain_data is not None and not chain_data.empty:
            for _, row in chain_data.iterrows():
                bid = row.get('bid', 0.0)
                ask = row.get('ask', 0.0)
                close = row.get('close', 0.0)
                last = row.get('last', 0.0)
                model_p = row.get('opt_price', 0.0)
                strike = float(row.get('strike', 0.0))
                right = str(row.get('right', 'C')).upper()[0]
                
                # Calculate Intrinsic Value Threshold
                if underlying_price > 0 and strike > 0:
                    intr = max(0.0, underlying_price - strike) if right == 'C' else max(0.0, strike - underlying_price)
                else:
                    intr = 0.0
                min_valid = max(0.0, intr - 0.50)
                
                mid = (bid + ask) / 2 if (bid > 0 and ask > 0 and ((bid + ask) / 2) >= min_valid) else 0.0
                last_p = last if (last > 0 and last >= min_valid) else 0.0
                
                # Default price selection
                price = 0.0
                if bid > 0 and ask > 0 and ((bid + ask) / 2) >= min_valid:
                    price = (bid + ask) / 2
                elif last > 0 and last >= min_valid:
                    price = last
                elif close > 0 and close >= min_valid:
                    price = close
                elif model_p > 0 and model_p >= min_valid:
                    price = model_p
                    
                # Final check: if zero or below intrinsic threshold, use local Bjerksund-Stensland fallback
                if price < min_valid and float(underlying_price) > 0 and strike > 0:
                    try:
                        iv = row.get('iv', 0.2)
                        if iv == 0: iv = underlying_iv if underlying_iv > 0 else 0.2
                        
                        # Use accurate DTE from the spread data
                        dte_val = spreads_df['dte'].iloc[0] if not spreads_df.empty else 30
                        t_years = max(0.001, float(dte_val)) / 365.0 
                        q = 0.015 # Estimate div yield
                        calc_p = BjerksundStensland2002.price_american_option(right, underlying_price, strike, t_years, 0.04, q, iv)
                        price = max(min_valid, float(calc_p))
                    except:
                        price = max(0.01, intr)
                        
                if mid <= 0: mid = price
                if last_p <= 0: last_p = price
                if bid <= 0: bid = price
                if ask <= 0: ask = price

                greeks = row[['delta', 'gamma', 'vega', 'theta']]
                
                # [GREEK FALLBACK] If TWS greeks are missing (all 0), calculate locally
                if greeks['delta'] == 0 and greeks['gamma'] == 0:
                    try:
                        iv = row.get('iv', underlying_iv)
                        if iv == 0: iv = underlying_iv if underlying_iv > 0 else 0.2
                        
                        dte_val = spreads_df['dte'].iloc[0] if not spreads_df.empty else 30
                        temp_row = {'dte': dte_val, 'iv': iv, 'right': str(row['right'])[0].lower(), 'strike_buy': float(row['strike'])}
                        greeks = self.calculate_greeks(temp_row, underlying_price)
                    except:
                        pass # Keep original zeros if calc fails

                # Normalize right to 'C' or 'P' for consistent lookup
                r_norm = 'C' if str(row['right']).upper().startswith('C') else 'P'
                # [FIX] Round strike to 4 decimals for robust lookup
                # Store (Effective Price, Greeks, Mid, Last, Bid, Ask)
                k_round = round(float(row['strike']), 4)
                entry_data = (float(price), greeks, float(mid), float(last_p), float(bid), float(ask))
                exp_clean = str(row.get('expiration', '')).replace('-', '').strip()
                if exp_clean:
                    lookup[(k_round, r_norm, exp_clean)] = entry_data
                lookup[(k_round, r_norm)] = entry_data
        
        # Helper for vectorizable lookup met theoretische fallback voor weekend/ontbrekende data
        def get_data(strike, right, dte, expiry=None):
            # Normalizeer right naar C/P
            r_norm = 'C' if right.upper().startswith('C') else 'P'
            # Rond strike af voor betrouwbare lookup
            k_round = round(float(strike), 4)
            res = None
            if expiry:
                exp_clean = str(expiry).replace('-', '').strip()
                res = lookup.get((k_round, r_norm, exp_clean))
            if not res:
                res = lookup.get((k_round, r_norm))
            if res: return res, True # Gevonden in echte TWS data
            
            # Als we in live mode zijn (echte chain_data beschikbaar) en het contract is niet gevonden,
            # dan bestaat het contract niet in TWS. We mogen hier GEEN fallback gebruiken.
            if chain_data is not None and not chain_data.empty:
                return None, False
                
            # Theoretische fallback als data niet in TWS staat (bijv. in het weekend)
            if underlying_price > 0:
                try:
                    t_years = max(0.001, float(dte)) / 365.0
                    r = 0.04
                    q = 0.015
                    iv_val = underlying_iv if underlying_iv > 0 else 0.2
                    
                    price_calc = BjerksundStensland2002.price_american_option(
                        r_norm.lower(), float(underlying_price), float(strike), t_years, r, q, iv_val
                    )
                    price = max(0.01, float(price_calc))
                    
                    # Bereken Grieken lokaal
                    temp_row = {'dte': dte, 'iv': iv_val, 'right': r_norm.lower(), 'strike_buy': float(strike)}
                    greeks = self.calculate_greeks(temp_row, underlying_price)
                    
                    # Retourneer (prijs, greeks, mid, last, bid, ask)
                    return (price, greeks, price, price, price, price), True
                except Exception as ex:
                    if self.log_func:
                        self.log_func(f"      ⚠️ Fout bij genereren data voor strike {strike}: {ex}")
            
            return None, False

        # 3. Fast Vectorized Collection
        # Pre-allocate arrays for speed
        n = len(spreads_df)
        net_delta = np.zeros(n)
        net_gamma = np.zeros(n)
        net_theta = np.zeros(n)
        net_vega = np.zeros(n)
        prices_buy = np.zeros(n)
        prices_sell = np.zeros(n)
        mids_buy = np.zeros(n)
        mids_sell = np.zeros(n)
        lasts_buy = np.zeros(n)
        lasts_sell = np.zeros(n)
        deltas_buy = np.zeros(n)
        deltas_sell = np.zeros(n)
        bids_buy = np.zeros(n)
        asks_buy = np.zeros(n)
        bids_sell = np.zeros(n)
        asks_sell = np.zeros(n)
        
        # Tracking valid rows
        valid_mask = np.ones(n, dtype=bool)

        it = spreads_df.itertuples(index=True)
        for i, row in enumerate(it):
            # Check strategy type
            if row.right == 'STR':
                res_p = get_data(row.strike_p_buy, 'P', row.dte)
                res_c = get_data(row.strike_c_buy, 'C', row.dte)
                
                if not res_p[1] or not res_c[1]:
                    valid_mask[i] = False
                    continue
                    
                pb, gb, mb, lb, bb, ab = res_p[0]
                cb, cb_greeks, cmb, clb, cbb, cab = res_c[0]
                
                prices_buy[i] = pb + cb
                mids_buy[i] = mb + cmb
                lasts_buy[i] = lb + clb
                prices_sell[i] = 0.0
                net_delta[i] = gb['delta'] + cb_greeks['delta']
                net_gamma[i] = gb['gamma'] + cb_greeks['gamma']
                net_theta[i] = gb['theta'] + cb_greeks['theta']
                net_vega[i] = gb['vega'] + cb_greeks['vega']
                deltas_buy[i] = gb['delta']
                bids_buy[i] = bb + cbb
                asks_buy[i] = ab + cab
            elif row.right == 'IC':
                res_pb = get_data(row.strike_p_buy, 'P', row.dte)
                res_ps = get_data(row.strike_p_sell, 'P', row.dte)
                res_cs = get_data(row.strike_c_sell, 'C', row.dte)
                res_cb = get_data(row.strike_c_buy, 'C', row.dte)
                
                if not res_pb[1] or not res_ps[1] or not res_cs[1] or not res_cb[1]:
                    valid_mask[i] = False
                    continue
                
                # Safer Unpacking
                res_pb_dat, found_pb = res_pb
                res_ps_dat, found_ps = res_ps
                res_cs_dat, found_cs = res_cs
                res_cb_dat, found_cb = res_cb
                
                pb, pg_buy, pmb, plb, pbb, pab = res_pb_dat
                ps, pg_sell, pms, pls, pbs, pas = res_ps_dat
                cs, cg_sell, cms, cls, cbs, cas = res_cs_dat
                cb, cg_buy, cmb, clb, cbb, cab = res_cb_dat
                prices_buy[i] = pb + cb
                prices_sell[i] = ps + cs
                mids_buy[i] = pmb + cmb
                mids_sell[i] = pms + cms
                lasts_buy[i] = plb + clb
                lasts_sell[i] = pls + cls
                net_delta[i] = pg_buy['delta'] - pg_sell['delta'] + cg_buy['delta'] - cg_sell['delta']
                net_gamma[i] = pg_buy['gamma'] - pg_sell['gamma'] + cg_buy['gamma'] - cg_sell['gamma']
                net_theta[i] = pg_buy['theta'] - pg_sell['theta'] + cg_buy['theta'] - cg_sell['theta']
                net_vega[i] =  pg_buy['vega'] - pg_sell['vega'] + cg_buy['vega'] - cg_sell['vega']
                deltas_buy[i] = pg_buy['delta']
                deltas_sell[i] = pg_sell['delta']
                bids_buy[i] = pbb + cbb
                asks_buy[i] = pab + cab
                bids_sell[i] = pbs + cbs
                asks_sell[i] = pas + cas
            else:
                # Vertical Spreads, Diagonals or Single Legs
                dte_buy = getattr(row, 'dte_long', row.dte)
                exp_buy = getattr(row, 'expiry_long', row.expiry)
                res_b = get_data(row.strike_buy, row.right, dte_buy, exp_buy)
                is_single_leg = (getattr(row, 'strike_sell', 0.0) == 0.0)
                
                if is_single_leg:
                    if not res_b[1]:
                        valid_mask[i] = False
                        continue
                    pb, gb, mb, lb, bb, ab = res_b[0]
                    prices_buy[i] = pb
                    mids_buy[i] = mb
                    lasts_buy[i] = lb
                    prices_sell[i] = 0.0
                    net_delta[i] = gb['delta']
                    net_gamma[i] = gb['gamma']
                    net_theta[i] = gb['theta']
                    net_vega[i] = gb['vega']
                    deltas_buy[i] = gb['delta']
                    bids_buy[i] = bb
                    asks_buy[i] = ab
                else:
                    res_s = get_data(row.strike_sell, row.right, row.dte, row.expiry)
                    if not res_b[1] or not res_s[1]:
                        valid_mask[i] = False
                        continue
                    
                    pb, gb, mb, lb, bb, ab = res_b[0]
                    ps, gs, ms, ls, bs, a_s = res_s[0]
                    
                    prices_buy[i] = pb
                    prices_sell[i] = ps
                    mids_buy[i] = mb
                    mids_sell[i] = ms
                    lasts_buy[i] = lb
                    lasts_sell[i] = ls
                    net_delta[i] = gb['delta'] - gs['delta']
                    net_gamma[i] = gb['gamma'] - gs['gamma']
                    net_theta[i] = gb['theta'] - gs['theta']
                    net_vega[i] = gb['vega'] - gs['vega']
                    deltas_buy[i] = gb['delta']
                    deltas_sell[i] = gs['delta']
                    bids_buy[i] = bb
                    asks_buy[i] = ab
                    bids_sell[i] = bs
                    asks_sell[i] = a_s
        
        # 4. Integrate back and FILTER
        spreads_df['price_buy'] = prices_buy
        spreads_df['price_sell'] = prices_sell
        spreads_df['net_price'] = prices_buy - prices_sell
        spreads_df['mid_price'] = mids_buy - mids_sell
        spreads_df['last_price'] = lasts_buy - lasts_sell
        spreads_df['delta'] = net_delta
        spreads_df['gamma'] = net_gamma
        spreads_df['theta'] = net_theta
        spreads_df['vega'] = net_vega
        spreads_df['delta_buy'] = deltas_buy
        spreads_df['delta_sell'] = deltas_sell
        spreads_df['delta_koers'] = np.abs(net_delta) + net_gamma
        
        # Calculate Spread Natural Bid and Ask
        # If buying the spread: Pay Ask for long legs, receive Bid for short legs. So Cost (Ask) = asks_buy - bids_sell
        # If selling the spread: Receive Bid for long legs, pay Ask for short legs. So Credit (Bid) = bids_buy - asks_sell
        # Since logic treats debits as spreads_df['net_price'], net_price > 0 is debit, < 0 is credit.
        # We will expose spread_ask (the price to buy the spread) and spread_bid (the price to sell the spread) explicitly.
        # But wait, net_price is price_buy - price_sell.
        # When net_price < 0, it means we receive money (Credit Spread).
        # We will provide absolute values for Mid and Bid/Ask of the premium to avoid confusion.
        
        spreads_df['spread_mid_abs'] = np.abs(mids_buy - mids_sell)
        
        # Credit spread: We receive money. Credit = prices_sell - prices_buy.
        # Natural Credit Bid (what we get) = bids_sell - asks_buy
        # Natural Credit Ask (what buyers want) = asks_sell - bids_buy
        
        # Debit spread: We pay money. Debit = prices_buy - prices_sell.
        # Natural Debit Ask (what we pay) = asks_buy - bids_sell
        # Natural Debit Bid = bids_buy - asks_sell
        
        is_credit = spreads_df['net_price'] < 0
        
        # Store absolute magnitude of the Ask and Mid for GUI
        # For Credit spreads, the "Ask" is the Natural Ask of the spread (asks_sell - bids_buy)
        # For Debit spreads, the "Ask" is the Natural Ask of the spread (asks_buy - bids_sell)
        spread_ask_abs = np.where(is_credit, asks_sell - bids_buy, asks_buy - bids_sell)
        spread_ask_abs = np.maximum(0.0, spread_ask_abs) # ensure positive display
        spreads_df['spread_ask_abs'] = spread_ask_abs
        
        # For worst-case entry (Laat): Debit pays Ask (asks_buy - bids_sell), Credit receives Bid (bids_sell - asks_buy)
        worst_entry_signed = np.where(is_credit, -(bids_sell - asks_buy), asks_buy - bids_sell)
        spreads_df['worst_entry_signed'] = worst_entry_signed
        
        # Difference between worst-case Natural Ask and Mid (represents Slippage/Liquidity)
        spreads_df['b_l_verschil'] = spreads_df['spread_ask_abs'] - spreads_df['spread_mid_abs']
        
        # Apply filter to remove phantom rows
        original_count = len(spreads_df)
        spreads_df = spreads_df[valid_mask].copy()
        removed = original_count - len(spreads_df)
        
        if removed > 0 and self.log_func:
            self.log_func(f"      🚫 {removed} fantoom-strikes verwijderd (geen Bid/Ask in TWS).")
            
        # --- FILTER STALE/INVALID OPTIONS PREMIUMS ---
        # Geen enkele verticale spread (of Iron Condor) premie kan groter zijn dan de breedte van de strikes.
        # Als dat wel zo is, komt dit door stale/ontbrekende marktdata (bijv. long poot gewaardeerd op 0).
        is_invalid_premium = (spreads_df['width'] > 0) & (~spreads_df['strategy'].isin(['Strangle'])) & (spreads_df['spread_mid_abs'] > spreads_df['width'] + 0.05)
        
        # Voor single-leg opties (LongCall / LongPut): check of de gekochte optieprijs niet onder de intrinsieke waarde ligt
        if underlying_price > 0:
            is_call = spreads_df['right'] == 'C'
            intr_val = np.where(is_call, np.maximum(0.0, underlying_price - spreads_df['strike_buy'].values), np.maximum(0.0, spreads_df['strike_buy'].values - underlying_price))
            is_below_intrinsic = (spreads_df['width'] == 0) & (spreads_df['price_buy'].values < (intr_val - 0.50))
            is_invalid_premium = is_invalid_premium | is_below_intrinsic
        
        # Ook debit spreads die als credit worden weergegeven (of vice versa) zijn stale/foutief.
        is_debit_strat = spreads_df['strategy'].isin(['BullCall', 'BearPut', 'SynthCoveredCall', 'SynthCoveredPut'])
        is_credit_strat = spreads_df['strategy'].isin(['BearCall', 'BullPut', 'IronCondor'])
        is_stale_direction = (is_debit_strat & (spreads_df['net_price'] < -0.10)) | (is_credit_strat & (spreads_df['net_price'] > 0.10))
        
        # Credit spreads moeten een positieve natuurlijke ontvangst hebben (geen debit op Bied/Laat)
        # Bij illiquide fantoom-trades is Bied(verkoop) < Laat(koop) waardoor er bij de marktprijs geld bijbetaald moet worden.
        is_negative_natural_credit = is_credit_strat & (spreads_df['worst_entry_signed'] > 0.0)
        
        invalid_mask = is_invalid_premium | is_stale_direction | is_negative_natural_credit
        
        removed_invalid = invalid_mask.sum()
        if removed_invalid > 0 and self.log_func:
            self.log_func(f"      🚫 {removed_invalid} trades met ongeldige/illiquide premies (bijv. Midden > Breedte of negatieve Bied/Laat credit) verwijderd.")
            
        spreads_df = spreads_df[~invalid_mask].copy()

        # --- NEW ARBITRAGE/RISK-FREE FILTER ---
        # A credit spread is risk free if the credit received (-debit) is >= the spread width
        debits_temp = spreads_df['net_price'].values
        widths_temp = spreads_df['width'].values
        is_risk_free = (debits_temp < 0) & (widths_temp > 0) & (-debits_temp >= widths_temp)
        
        removed_rf = is_risk_free.sum()
        if removed_rf > 0 and self.log_func:
            self.log_func(f"      ⚠️ {removed_rf} risicoloze trades (credit >= width) verwijderd (TWS weigert deze).")
            
        spreads_df = spreads_df[~is_risk_free].copy()
        
        if original_count > 0 and self.log_func and removed == 0 and removed_rf == 0:
            self.log_func(f"      ✅ Alle {len(spreads_df)} strikes geverifieerd in TWS.")
        
        # Financial Metrics (Use filtered DataFrame columns to avoid length mismatch)
        debits_mid = spreads_df['net_price'].values
        debits_worst = spreads_df['worst_entry_signed'].values
        widths = spreads_df['width'].values
        
        # Max Profit Calculation (Vectorized) using WORST entry for realistic max profit
        is_long = (widths == 0) | (spreads_df['strategy'] == 'Strangle')
        profits = np.where(debits_worst < 0, -debits_worst * 100, (widths - debits_worst) * 100)
        
        # Override for infinite max profit legs (Long Call / Long Put)
        # Assuming a 10% favorable move in underlying price to cap max profit
        s_buy = spreads_df['strike_buy'].values
        right = spreads_df['right'].values
        
        call_val = np.maximum(0, (underlying_price * 1.10) - s_buy)
        put_val = np.maximum(0, s_buy - (underlying_price * 0.90))
        
        long_profits = np.where(right == 'C', call_val * 100, put_val * 100) - (debits_worst * 100)
        
        profits = np.where(is_long, long_profits, profits)
        
        # Handle data missing (using filtered columns)
        p_buy_f = spreads_df['price_buy'].values
        p_sell_f = spreads_df['price_sell'].values
        mask_missing = (p_buy_f == 0) | ((p_sell_f == 0) & (spreads_df['strike_sell'] > 0))
        # For STR, check p_buy and c_buy
        if 'strike_p_buy' in spreads_df.columns:
            mask_missing |= (spreads_df['right'] == 'STR') & (p_buy_f == 0)
            
        profits[mask_missing] = 0.0
        spreads_df['max_profit'] = profits

        # Extrinsic (Vectorized)
        def get_extrinsic_vec(prices, strikes, rights, spot):
            # intrinsic call = max(0, spot - strike)
            # intrinsic put = max(0, strike - spot)
            intr_call = np.maximum(0.0, spot - strikes)
            intr_put = np.maximum(0.0, strikes - spot)
            intr = np.where(rights == 'C', intr_call, intr_put)
            return np.maximum(0.0, prices - intr)

        spread_rights = spreads_df['right'].values
        # For IC, we sum extensics of all 4 legs? Simplified: sum of net buy/sell extrinsic.
        # But let's keep it simple for now as it's a proxy.
        spreads_df['extrinsic_buy'] = get_extrinsic_vec(spreads_df['price_buy'].values, spreads_df['strike_buy'].values, spread_rights, underlying_price)
        spreads_df['extrinsic_sell'] = get_extrinsic_vec(spreads_df['price_sell'].values, spreads_df['strike_sell'].values, spread_rights, underlying_price)
        spreads_df['net_extrinsic'] = spreads_df['extrinsic_buy'] - spreads_df['extrinsic_sell']

        # Market Structure (Scalar)
        for k, v in market_structure.items(): spreads_df[k] = v
        
        # Center & Distances
        if 'strike_p_sell' in spreads_df.columns:
            sc = (spreads_df['strike_p_sell'] + spreads_df['strike_c_sell']) / 2
        else:
            sc = (spreads_df['strike_buy'] + spreads_df['strike_sell']) / 2
            sc = np.where(spreads_df['strike_sell'] == 0, spreads_df['strike_buy'], sc)
        
        spreads_df['dist_max_pain'] = np.abs(sc - market_structure['max_pain'])
        spreads_df['dist_call_wall'] = sc - market_structure['call_wall']
        spreads_df['dist_put_wall'] = sc - market_structure['put_wall']
        
        # PoP (Vectorized) - Nearest Leg Delta
        # Voor credit spreads, PoP = (1 - abs(delta)) * 100. Voor debit, PoP = abs(delta) * 100.
        dist_buy = np.abs(spreads_df['strike_buy'].values - underlying_price)
        dist_sell = np.abs(spreads_df['strike_sell'].values - underlying_price)
        
        # nearest_leg_delta
        nld = np.where(dist_buy <= dist_sell, spreads_df['delta_buy'].values, spreads_df['delta_sell'].values)
        is_credit_strat = spreads_df['net_price'].values < 0
        is_synth = spreads_df['strategy'].isin(['SynthCoveredCall', 'SynthCoveredPut'])
        # Voor credit spreads en synthetische covered spreads (PMCC/PMCP): PoP = (1 - abs(delta_sell)) * 100
        # Dit waarborgt wiskundig de 80-90% winstkans doordat de short leg OTM met lage delta geschreven wordt.
        pop_standard = np.where(is_credit_strat, (1.0 - np.abs(nld)) * 100, np.abs(nld) * 100)
        pop_synth = (1.0 - np.abs(spreads_df['delta_sell'].values)) * 100
        pop_vals = np.where(is_synth, pop_synth, pop_standard)
        spreads_df['pop'] = np.round(pop_vals, 1)

        # Safety Buffer: Distance to Max Pain
        # "wil ik dan altijd 5 punten van de max pain wegblijven"
        spreads_df['max_pain_spot'] = market_structure.get('max_pain', 0.0)
        spreads_df['max_pain_selection'] = market_structure.get('max_pain_selection', 0.0)
        
        def check_buffer(row):
            b_dist = abs(row['strike_buy'] - row['max_pain_spot'])
            s_dist = abs(row['strike_sell'] - row['max_pain_spot']) if row['strike_sell'] > 0 else 999.0
            return min(b_dist, s_dist) >= 5.0
            
        spreads_df['max_pain_buffer_ok'] = spreads_df.apply(check_buffer, axis=1)

        # 5. Advanced Metrics (IV Rank, Expected Move EM68 & EM85)
        if underlying_iv > 0:
            dte_val = spreads_df['dte'].iloc[0] if not spreads_df.empty else 30
            # Expected Move 68% (1SD) = Spot * IV * sqrt(DTE/365)
            em68 = underlying_price * underlying_iv * np.sqrt(dte_val / 365.0)
            # Expected Move 85% (1.44SD) = em68 * 1.439535
            em85 = em68 * 1.439535
            
            spreads_df['expected_move'] = np.round(em68, 2)
            spreads_df['EM68'] = np.round(em68, 2)
            spreads_df['EM85'] = np.round(em85, 2)
            spreads_df['underlying_iv'] = underlying_iv * 100 # Display as percentage
            
            # IV Rank & Percentile (only if hist_iv_df provided and not empty)
            if hist_iv_df is not None and not hist_iv_df.empty:
                ivr, ivp = self.calculate_iv_indices(underlying_iv, hist_iv_df)
                spreads_df['iv_rank'] = ivr
                spreads_df['iv_percentile'] = ivp
            else:
                spreads_df['iv_rank'] = 0.0
                spreads_df['iv_percentile'] = 0.0
        else:
            spreads_df['expected_move'] = 0.0
            spreads_df['EM68'] = 0.0
            spreads_df['EM85'] = 0.0
            spreads_df['underlying_iv'] = 0.0
            spreads_df['iv_rank'] = 0.0
            spreads_df['iv_percentile'] = 0.0

        # --- CALCULATE SLUITINGSWINST (EXPECTED MOVE CLOSING PROFIT EM68 & EM85) ---
        # Berekent de geschatte winst ($) bij een vervroegde sluiting (na 5 dagen of halverwege)
        # als de koers met 1x EM68 of 1x EM85 in de gunstige richting beweegt.
        closing_profits_68 = []
        closing_profits_85 = []
        if not spreads_df.empty:
            r = 0.04
            q = 0.015
            iv_val = underlying_iv if underlying_iv > 0 else 0.2
            
            for idx, row in spreads_df.iterrows():
                # Bepaal de gunstige koersrichting (S_target)
                strat = str(row['strategy']).lower()
                is_bullish = ('bull' in strat) or ('longcall' in strat) or ('synthcoveredcall' in strat)
                is_bearish = ('bear' in strat) or ('longput' in strat) or ('synthcoveredput' in strat)
                
                em68_val = float(row.get('EM68', row.get('expected_move', 0.0)))
                em85_val = float(row.get('EM85', em68_val * 1.439535))
                if em68_val == 0.0 and underlying_price > 0:
                    em68_val = underlying_price * iv_val * np.sqrt(float(row['dte']) / 365.0)
                    em85_val = em68_val * 1.439535
                
                if is_bullish:
                    s_target_68 = underlying_price + em68_val
                    s_target_85 = underlying_price + em85_val
                elif is_bearish:
                    s_target_68 = underlying_price - em68_val
                    s_target_85 = underlying_price - em85_val
                else: # Neutraal / overig
                    s_target_68 = underlying_price
                    s_target_85 = underlying_price
                
                # Resterende looptijd bij sluiten (bijv. na 5 dagen, of halverwege als dte < 10)
                dte_val = float(row['dte'])
                dte_long_val = float(row.get('dte_long', dte_val))
                dte_target = dte_val - 5.0 if dte_val > 10.0 else dte_val * 0.5
                dte_target = max(0.1, dte_target)
                t_target_short = dte_target / 365.0
                
                dte_target_long = max(0.1, dte_long_val - 5.0)
                t_target_long = dte_target_long / 365.0
                
                def get_theo_price(strike, right_str, s_tgt, t_override=None):
                    if pd.isna(strike) or strike <= 0:
                        return 0.0
                    r_norm = 'c' if str(right_str).upper().startswith('C') else 'p'
                    t_eval = t_override if t_override is not None else t_target_short
                    try:
                        price_calc = BjerksundStensland2002.price_american_option(
                            r_norm, float(s_tgt), float(strike), t_eval, r, q, iv_val
                        )
                        return max(0.01, float(price_calc))
                    except:
                        return 0.01

                right = str(row['right']).upper()
                def calc_net_new(s_tgt):
                    if right == 'STR':
                        pb_new = get_theo_price(row.get('strike_p_buy', row['strike_buy']), 'P', s_tgt, t_target_short)
                        cb_new = get_theo_price(row.get('strike_c_buy', row['strike_buy']), 'C', s_tgt, t_target_short)
                        return pb_new + cb_new
                    elif right == 'IC':
                        pb_new = get_theo_price(row.get('strike_p_buy', 0.0), 'P', s_tgt, t_target_short)
                        ps_new = get_theo_price(row.get('strike_p_sell', 0.0), 'P', s_tgt, t_target_short)
                        cs_new = get_theo_price(row.get('strike_c_sell', 0.0), 'C', s_tgt, t_target_short)
                        cb_new = get_theo_price(row.get('strike_c_buy', 0.0), 'C', s_tgt, t_target_short)
                        return (pb_new + cb_new) - (ps_new + cs_new)
                    else: # Vertical Spread, Diagonal of Single Leg
                        pb_new = get_theo_price(row['strike_buy'], row['right'], s_tgt, t_target_long)
                        ps_new = get_theo_price(row['strike_sell'], row['right'], s_tgt, t_target_short) if row['strike_sell'] > 0 else 0.0
                        return pb_new - ps_new
                
                net_price_entry = float(row['net_price'])
                p68 = (calc_net_new(s_target_68) - net_price_entry) * 100
                p85 = (calc_net_new(s_target_85) - net_price_entry) * 100
                closing_profits_68.append(p68)
                closing_profits_85.append(p85)
        
        spreads_df['sluitingswinst'] = closing_profits_68
        spreads_df['sluitingswinst_em85'] = closing_profits_85

        # 6. Universeel Koopadvies (1% Target Rule)
        # Evaluates the spread's mathematical payout against +/- 1% targets.
        if spreads_df.empty:
            spreads_df['koopadvies'] = pd.Series(dtype=str)
            return spreads_df

        p = koopadvies_p
        u_price = float(underlying_price) if underlying_price is not None else 0.0
        
        spreads_df['koopadvies'] = ""
        spreads_df['winst_midden'] = 0.0
        spreads_df['winst_laat'] = 0.0
        
        if u_price > 0:
            target_up = u_price * (1 + p)
            target_down = u_price * (1 - p)
            
            # Universal Variables
            s_buy = spreads_df['strike_buy'].values
            s_sell = spreads_df['strike_sell'].values
            s_p_buy = spreads_df.get('strike_p_buy', spreads_df['strike_buy']).values
            s_p_sell = spreads_df.get('strike_p_sell', spreads_df['strike_sell']).values
            s_c_buy = spreads_df.get('strike_c_buy', spreads_df['strike_buy']).values
            s_c_sell = spreads_df.get('strike_c_sell', spreads_df['strike_sell']).values
            
            right = spreads_df['right'].values
            is_C = right == 'C'
            is_P = right == 'P'
            is_IC = right == 'IC'
            is_STR = right == 'STR'
            
            def get_payout(tgt):
                # Single and Vertical Spreads
                v_long_c = np.where(is_C, np.maximum(0, tgt - s_buy), 0)
                v_long_p = np.where(is_P, np.maximum(0, s_buy - tgt), 0)
                v_long = v_long_c + v_long_p
                
                v_short_c = np.where((is_C) & (s_sell > 0), np.maximum(0, tgt - s_sell), 0)
                v_short_p = np.where((is_P) & (s_sell > 0), np.maximum(0, s_sell - tgt), 0)
                v_short = v_short_c + v_short_p
                
                v_out = v_long - v_short
                
                # Multi-leg Combinations
                ic_out = (np.maximum(0, s_p_buy - tgt) - np.maximum(0, s_p_sell - tgt)) + (np.maximum(0, tgt - s_c_buy) - np.maximum(0, tgt - s_c_sell))
                str_out = np.maximum(0, s_p_buy - tgt) + np.maximum(0, tgt - s_c_buy)
                
                return np.where(is_IC, ic_out, np.where(is_STR, str_out, v_out))
                
            payout_up = get_payout(target_up)
            payout_down = get_payout(target_down)
            
            # Identify Directional Expectation
            strat = spreads_df['strategy']
            is_bull = strat.isin(['BullCall', 'BullPut', 'LongCall', 'SynthCoveredCall'])
            is_bear = strat.isin(['BearCall', 'BearPut', 'LongPut', 'SynthCoveredPut'])
            # Neutral/Volatile (IronCondor, Strangle) inherently check both directions and take the worst case.
            
            final_payout = np.where(is_bull, payout_up, np.where(is_bear, payout_down, np.minimum(payout_up, payout_down)))
            
            n_price = spreads_df['net_price'].values
            n_price_worst = spreads_df['worst_entry_signed'].values
            
            # Since n_price represents the direct Entry price mapping (negative means credit received)
            profit_mid = final_payout - n_price
            profit_worst = final_payout - n_price_worst
            
            is_synth = strat.isin(['SynthCoveredCall', 'SynthCoveredPut'])
            
            # Calculate ROC metrics for synthetic covered spreads (Return on Capital per short cycle & annualized)
            price_sell_arr = spreads_df['price_sell'].values
            entry_cost = np.maximum(0.01, n_price_worst)
            roc_cyclus = np.where(is_synth, (price_sell_arr / entry_cost) * 100.0, 0.0)
            dte_vals = np.maximum(1.0, spreads_df['dte'].values)
            roc_jaars = np.where(is_synth, roc_cyclus * (365.0 / dte_vals), 0.0)
            spreads_df['roc_cyclus'] = np.round(roc_cyclus, 1)
            spreads_df['roc_jaars'] = np.round(roc_jaars, 1)

            # Nieuwe Standaard voor Synthetische Covered Spreads:
            # Regel 1: Breedte > Netto Debit (worst entry)
            # Regel 2: Premie Short Leg >= $2.00
            # Regel 3: Rendement per Cyclus (ROC Cyclus) >= 5.0%
            # Regel 4: PoP >= 70% (consequent met Delta ~0.25)
            synth_pass = (
                (spreads_df['width'].values > n_price_worst) & 
                (price_sell_arr >= 2.0) & 
                (roc_cyclus >= 5.0) & 
                (spreads_df['pop'].values >= 70.0)
            )
            standard_pass = (profit_worst > 0)
            spreads_df['koopadvies'] = np.where(is_synth, np.where(synth_pass, "✅", "❌"), np.where(standard_pass, "✅", "❌"))
            
            spreads_df['winst_midden'] = profit_mid * 100
            spreads_df['winst_laat'] = profit_worst * 100
            
            bep = np.zeros_like(s_buy, dtype=float)
            st_vals = strat.values
            for idx, st_val in enumerate(st_vals):
                entry = n_price_worst[idx]
                if st_val in ['LongCall', 'BullCall', 'SynthCoveredCall']:
                    bep[idx] = s_buy[idx] + entry
                elif st_val in ['LongPut', 'BearPut', 'SynthCoveredPut']:
                    bep[idx] = s_buy[idx] - entry
                elif st_val == 'BearCall':
                    bep[idx] = s_sell[idx] + (-entry)
                elif st_val == 'BullPut':
                    bep[idx] = s_sell[idx] - (-entry)
                else:
                    bep[idx] = 0.0 # IC/Strangle have two BEPs or are too complex to display in 1 num
            spreads_df['BEP'] = bep
            
            # Calculate distance to BEP in percentage
            # For Spreads: positive cushion_pct = safe buffer away from spot
            # For Long Single Legs: req_bep_move_pct = required underlying move to reach BEP
            st_vals = strat.values
            cushion_pct = np.zeros_like(bep)
            req_bep_move_pct = np.zeros_like(bep)
            extrinsic_pct = np.zeros_like(bep)
            capital_risk_flat_pct = np.zeros_like(bep)
            capital_risk_dip5_pct = np.zeros_like(bep)

            for idx, st_val in enumerate(st_vals):
                b = bep[idx]
                if b <= 0 or u_price <= 0:
                    cushion_pct[idx] = 0.0
                    continue
                if st_val in ['BullCall', 'BullPut', 'LongCall', 'SynthCoveredCall']:
                    cushion_pct[idx] = ((u_price - b) / u_price) * 100.0
                    req_bep_move_pct[idx] = max(0.0, ((b - u_price) / u_price) * 100.0)
                elif st_val in ['BearCall', 'BearPut', 'LongPut', 'SynthCoveredPut']:
                    cushion_pct[idx] = ((b - u_price) / u_price) * 100.0
                    req_bep_move_pct[idx] = max(0.0, ((u_price - b) / u_price) * 100.0)
                else:
                    cushion_pct[idx] = 0.0

                # Single-leg specific risk & capital retention metrics (LongCall / LongPut)
                if st_val in ['LongCall', 'LongPut']:
                    cost = max(0.01, float(n_price_worst[idx]))
                    sb = float(s_buy[idx])
                    if st_val == 'LongCall':
                        val_flat = max(0.0, u_price - sb)
                        capital_risk_flat_pct[idx] = max(0.0, min(100.0, ((cost - val_flat) / cost) * 100.0))
                        val_dip5 = max(0.0, (u_price * 0.95) - sb)
                        capital_risk_dip5_pct[idx] = max(0.0, min(100.0, ((cost - val_dip5) / cost) * 100.0))
                        extr_val = max(0.0, cost - max(0.0, u_price - sb))
                        extrinsic_pct[idx] = (extr_val / u_price) * 100.0
                    else:
                        val_flat = max(0.0, sb - u_price)
                        capital_risk_flat_pct[idx] = max(0.0, min(100.0, ((cost - val_flat) / cost) * 100.0))
                        val_dip5 = max(0.0, sb - (u_price * 1.05))
                        capital_risk_dip5_pct[idx] = max(0.0, min(100.0, ((cost - val_dip5) / cost) * 100.0))
                        extr_val = max(0.0, cost - max(0.0, sb - u_price))
                        extrinsic_pct[idx] = (extr_val / u_price) * 100.0

            spreads_df['bep_afstand_pct'] = cushion_pct
            spreads_df['req_bep_move_pct'] = np.round(req_bep_move_pct, 2)
            spreads_df['extrinsic_pct'] = np.round(extrinsic_pct, 2)
            spreads_df['capital_risk_flat_pct'] = np.round(capital_risk_flat_pct, 1)
            spreads_df['capital_risk_dip5_pct'] = np.round(capital_risk_dip5_pct, 1)

        # --- Part 6: Risk Metrics (Bjerksund-Stensland 2002) ---
        n = len(spreads_df)
        ttp_days = np.full(n, 99.0)
        tei_scores = np.zeros(n)
        is_efficient = np.zeros(n, dtype=bool)
        
        # Auto-fetch ATR if not provided
        if (atr_10 is None or atr_10 == 0) and ib_client is not None:
             try:
                 atr_10 = ib_client.get_atr(symbol)
                 if log_func: log_func(f"      [RiskModel] ATR(10) opgehaald: {atr_10:.2f}")
             except:
                 atr_10 = 0.0

        if atr_10 > 0 and underlying_price > 0:
            # We need to ensure we have the strikes in a compatible format
            s_buy_vals = spreads_df['strike_buy'].values
            dte_vals = spreads_df['dte'].values
            
            # Directions for Risk Model
            strat_vals = spreads_df['strategy'].values
            
            for i in range(n):
                try:
                    # Evaluate risk relative to the 'Buy' leg strike
                    k_val = float(s_buy_vals[i])
                    iv_val = float(underlying_iv) # Use underlying IV as proxy if row IV missing
                    
                    # Target direction: Bearish strategies profit on price DROP
                    s_type = strat_vals[i]
                    signed_target = 5.0
                    if s_type in ['BearCall', 'BearPut', 'LongPut']:
                        signed_target = -5.0
                    
                    rm = get_bs_risk_metrics(
                        S=float(underlying_price),
                        K=k_val,
                        T=max(0.001, float(dte_vals[i]))/365.0,
                        r=0.04,
                        q=0.015,
                        sigma=max(0.05, iv_val),
                        atr_10=float(atr_10),
                        target_profit_usd=signed_target
                    )
                    ttp_days[i] = rm['days_to_profit']
                    tei_scores[i] = rm['tei_score']
                    is_efficient[i] = rm['is_efficient']
                except Exception:
                    pass

        # Assignment of new Risk Columns
        spreads_df['TTP (D)'] = ttp_days
        spreads_df['TEI Score'] = tei_scores
        spreads_df['Efficient'] = is_efficient

        # --- Part 7: AntiGravity Score (AG Score) ---
        # Combineert winstkans, TEI, EM85 veiligheidsdekking, Max Pain en Risk/Reward verhouding
        
        # 1. Bereken EM85 Dekkingsverhouding (EM85 Coverage Ratio)
        em85_vals = np.maximum(0.01, spreads_df['EM85'].values)
        bep_vals = spreads_df['BEP'].values if 'BEP' in spreads_df.columns else spreads_df['strike_buy'].values
        strat_vals = spreads_df['strategy'].values
        
        # Afstand van Spot tot BEP
        bep_dists = np.zeros(n)
        for i in range(n):
            st_val = strat_vals[i]
            b_val = bep_vals[i]
            if st_val in ['BullCall', 'BullPut', 'LongCall']:
                bep_dists[i] = underlying_price - b_val if b_val > 0 else underlying_price - spreads_df['strike_sell'].iloc[i]
            elif st_val in ['BearCall', 'BearPut', 'LongPut']:
                bep_dists[i] = b_val - underlying_price if b_val > 0 else spreads_df['strike_sell'].iloc[i] - underlying_price
            else:
                bep_dists[i] = np.abs(underlying_price - spreads_df['strike_buy'].iloc[i])
                
        em85_coverage_ratio = bep_dists / em85_vals
        spreads_df['em85_dekking_pct'] = np.round(em85_coverage_ratio * 100.0, 1)
        
        # EM85 Multiplier: Beloon trades waar BEP buiten het 85% EM bereik ligt (> 1.0)
        # Bij ratio = 1.0 (BEP exact op 85% grens) -> 1.30x multiplier
        # Bij ratio >= 1.5 (BEP diep beschermd achter EM85 & supports) -> 1.60x multiplier
        em85_multiplier = 1.0 + np.clip((em85_coverage_ratio - 0.4) * 0.6, -0.3, 0.7)
        
        # 2. Risk/Reward Balans Multiplier (Winst vs Verlies)
        # Voorkom extreem scheve verhoudingen (bijv. $5 winst vs $495 verlies = RoR < 0.05)
        profits = spreads_df['max_profit'].values
        widths = spreads_df['width'].values
        max_loss = np.where(widths > 0, (widths * 100) - profits, 1.0)
        max_loss = np.maximum(1.0, max_loss)
        ror = profits / max_loss
        
        # Beloon de 'sweet spot' (10% - 50% rendement op risico) i.c.m. hoge EM85 dekking
        ror_multiplier = np.where((ror >= 0.10) & (ror <= 0.60), 1.25, np.where(ror < 0.04, 0.50, 1.0))

        # Base Score
        ag_score = spreads_df['pop'] * np.maximum(spreads_df['TEI Score'], 0.1) * em85_multiplier * ror_multiplier
        
        # Bonus voor 1% regel (koopadvies)
        ag_score = np.where(spreads_df['koopadvies'] == "✅", ag_score * 1.3, ag_score)
        
        # Bonus voor Max Pain buffer
        if 'max_pain_buffer_ok' in spreads_df.columns:
            ag_score = np.where(spreads_df['max_pain_buffer_ok'], ag_score * 1.2, ag_score)
            
        # Bonus voor BEP Afstand
        if 'bep_afstand_pct' in spreads_df.columns:
            bep_buffer_pct = np.maximum(0, spreads_df['bep_afstand_pct'])
            bep_bonus_multiplier = 1.0 + (bep_buffer_pct / 100.0) * 2.0
            ag_score = ag_score * bep_bonus_multiplier
        
        # Penalty voor heel krappe afstand tot koers (strike_buy dicht op koers)
        dist_pct = np.abs(spreads_df['strike_buy'] - underlying_price) / max(0.001, underlying_price)
        ag_score = np.where(dist_pct < 0.01, ag_score * 0.8, ag_score)
        
        spreads_df['AG_Score'] = np.round(ag_score, 1)
        spreads_df['underlying_price'] = underlying_price

        return spreads_df

    def calculate_iv_indices(self, current_iv, hist_iv_df):
        """
        Calculates IV Rank and IV Percentile from historical IV series.
        """
        if hist_iv_df.empty or 'iv' not in hist_iv_df.columns:
            return 0.0, 0.0
            
        ivs = hist_iv_df['iv'].dropna().values
        if len(ivs) < 10:
            return 0.0, 0.0
            
        # IV Rank: (Current - Min) / (Max - Min)
        low = np.min(ivs)
        high = np.max(ivs)
        
        # Enforce realistic range to avoid division by near-zero (common in paper trading/spotty history)
        if (high - low) > 0.001: 
            ivr = (current_iv - low) / (high - low) * 100
        else:
            ivr = 100.0 if current_iv > high else 0.0
            
        # Traditional IV Rank is capped between 0 and 100%
        ivr = np.clip(ivr, 0, 100)
            
        # IV Percentile: Percentage of days where IV was lower than current
        ivp = (ivs < current_iv).sum() / len(ivs) * 100
        ivp = np.clip(ivp, 0, 100)
        
        return np.round(ivr, 1), np.round(ivp, 1)
    def filter_spreads(self, spreads_df, filters, log_func=None):
        """
        Filters the generated spreads based on user criteria.
        filters: dict (min_pop, min_max_profit, etc.)
        log_func: optional callback to log debug info
        """
        if spreads_df.empty:
            return spreads_df
            
        df = spreads_df.copy()
        initial_count = len(df)
        
        # Track drops per filter
        drop_stats = {}
        
        if log_func:
            log_func(f"🔎 Filtering {initial_count} spreads...")
        
        if 'min_pop' in filters and filters['min_pop'] > 0:
            before = len(df)
            df = df[df['pop'] >= filters['min_pop']]
            dropped = before - len(df)
            if dropped > 0:
                drop_stats['PoP'] = dropped
                if log_func: log_func(f"   🔻 Filter PoP < {filters['min_pop']}: {dropped} dropped")
            
        # Isolated failure rate tracking for true bottleneck visibility
        isolated_drops = {}
        if 'min_profit' in filters and filters['min_profit'] > 0:
            eff_p = np.minimum(filters['min_profit'], spreads_df['width'] * 100 * 0.08) if 'width' in spreads_df.columns else filters['min_profit']
            fp = int((spreads_df['max_profit'] < eff_p).sum())
            if fp > 0: isolated_drops['Winst'] = f"{fp}/{initial_count} ({fp/initial_count*100:.0f}%)"
        if 'min_pop' in filters and filters['min_pop'] > 0:
            fpop = int((spreads_df['pop'] < filters['min_pop']).sum())
            if fpop > 0: isolated_drops['PoP'] = f"{fpop}/{initial_count} ({fpop/initial_count*100:.0f}%)"
        if 'delta_sell' in spreads_df.columns and ('min_delta' in filters or 'max_delta' in filters):
            min_d, max_d = filters.get('min_delta', 0.0), filters.get('max_delta', 1.0)
            fd = int(((spreads_df['delta_sell'].abs() < min_d) | (spreads_df['delta_sell'].abs() > max_d)).sum())
            if fd > 0: isolated_drops['Delta'] = f"{fd}/{initial_count} ({fd/initial_count*100:.0f}%)"
        if 'bep_afstand_pct' in spreads_df.columns and 'min_bep_dist_pct' in filters and filters['min_bep_dist_pct'] > 0:
            is_credit_spread = spreads_df['strategy'].isin(['BullPut', 'BearCall'])
            fbep = int(((spreads_df['bep_afstand_pct'] < filters['min_bep_dist_pct']) & is_credit_spread).sum())
            if fbep > 0: isolated_drops['BEP Buffer'] = f"{fbep}/{initial_count} ({fbep/initial_count*100:.0f}%)"

        if 'min_profit' in filters and filters['min_profit'] > 0:
            before = len(df)
            min_p = filters['min_profit']
            if 'width' in df.columns:
                # Scale profit requirement for narrow spreads ($2.5 width needs $20, not $40-$50)
                eff_thresh = np.minimum(min_p, df['width'] * 100 * 0.08)
                df = df[df['max_profit'] >= eff_thresh]
            else:
                df = df[df['max_profit'] >= min_p]
            dropped = before - len(df)
            if dropped > 0:
                drop_stats['Profit'] = dropped
                if log_func: log_func(f"   🔻 Filter Profit < {filters['min_profit']}: {dropped} dropped")
            
        if 'min_delta' in filters or 'max_delta' in filters:
            # For Iron Condors and Strangles, delta_sell might be different or multiple.
            # For now, skip delta filter if it's a multi-leg complex strategy OR a single long.
            before = len(df)
            mask = pd.Series([True]*len(df), index=df.index)
            
            if 'delta_sell' in df.columns:
                # Vertical Spreads: Filter by Delta Sell within range [min_delta, max_delta]
                vertical_mask = df['strategy'].isin(['BullCall', 'BullPut', 'BearCall', 'BearPut'])
                min_d = filters.get('min_delta', 0.0)
                max_d = filters.get('max_delta', 1.0)
                mask &= ~vertical_mask | ((df['delta_sell'].abs() >= min_d) & (df['delta_sell'].abs() <= max_d))
                
            df = df[mask]
            dropped = before - len(df)
            if dropped > 0:
                drop_stats['Delta'] = dropped
                if log_func: log_func(f"   🔻 Filter Delta Sell [{filters.get('min_delta', 0.0):.2f} - {filters.get('max_delta', 1.0):.2f}]: {dropped} dropped")

        # Filter Minimale BEP Afstand (Geldt voor credit spreads als veiligheidsbuffer; Longs & Synthetics zijn vrijgesteld)
        if 'min_bep_dist_pct' in filters and filters['min_bep_dist_pct'] > 0:
            before = len(df)
            if 'bep_afstand_pct' in df.columns:
                is_exempt_bep = df['strategy'].isin(['LongCall', 'LongPut', 'SynthCoveredCall', 'SynthCoveredPut'])
                df = df[is_exempt_bep | (df['bep_afstand_pct'] >= filters['min_bep_dist_pct'])]
                dropped = before - len(df)
                if dropped > 0:
                    drop_stats['BEP Afstand'] = dropped
                    if log_func: log_func(f"   🔻 Filter BEP Afstand < {filters['min_bep_dist_pct']:.1f}%: {dropped} dropped")
                    
        # Filter Min Gamma
        if 'min_gamma' in filters and filters['min_gamma'] != 0: 
             before = len(df)
             df = df[df['gamma'] >= filters['min_gamma']]
             dropped = before - len(df)
             if dropped > 0:
                 drop_stats['Gamma'] = dropped
                 if log_func: log_func(f"   🔻 Filter Gamma >= {filters['min_gamma']}: {dropped} dropped")
        
        if 'max_dte' in filters:
            before = len(df)
            is_synth = df['strategy'].isin(['SynthCoveredCall', 'SynthCoveredPut'])
            synth_max = filters.get('max_short_dte', 60)
            df = df[(is_synth & (df['dte'] <= synth_max)) | (~is_synth & (df['dte'] <= filters['max_dte']))]
            dropped = before - len(df)
            if dropped > 0:
                drop_stats['Max DTE'] = dropped
                if log_func: log_func(f"   🔻 Filter DTE > {filters['max_dte']}: {dropped} dropped")
            
        if 'min_dte' in filters:
            before = len(df)
            is_synth = df['strategy'].isin(['SynthCoveredCall', 'SynthCoveredPut'])
            synth_min = filters.get('min_short_dte', 14)
            df = df[(is_synth & (df['dte'] >= synth_min)) | (~is_synth & (df['dte'] >= filters['min_dte']))]
            dropped = before - len(df)
            if dropped > 0:
                drop_stats['Min DTE'] = dropped
                if log_func: log_func(f"   🔻 Filter DTE < {filters['min_dte']}: {dropped} dropped")
                
        # Max Pain Distance Filter
        if 'max_pain_dist' in filters and filters['max_pain_dist'] > 0:
            if 'spread_dist_max_pain' in df.columns:
                before = len(df)
                df = df[df['spread_dist_max_pain'] <= filters['max_pain_dist']]
                dropped = before - len(df)
                if dropped > 0:
                    drop_stats['Max Pain Dist'] = dropped
                    if log_func: log_func(f"   🔻 Filter Max Pain Dist > {filters['max_pain_dist']}: {dropped} dropped")
            
        # Filter Only Koopadvies
        if filters.get('only_koopadvies', False):
            before = len(df)
            df = df[df['koopadvies'] == "✅"]
            dropped = before - len(df)
            if dropped > 0:
                drop_stats['Koopadvies'] = dropped
                if log_func: log_func(f"   🔻 Filter Niet-Koopadvies: {dropped} dropped")
            
        if log_func:
            if df.empty and drop_stats:
                iso_str = ", ".join([f"{k} ({v})" for k, v in isolated_drops.items()])
                log_func(f"   ⚠️ Alle {initial_count} kandidaten gefilterd!")
                if iso_str:
                    log_func(f"   📊 Zelfstandige uitval per regel: {iso_str}")
                sorted_drops = sorted(drop_stats.items(), key=lambda x: x[1], reverse=True)
                drop_str = ", ".join([f"{k} ({v})" for k, v in sorted_drops])
                log_func(f"   📉 Sequentiële volgorde uitval: {drop_str}")
                
                # Near-Miss / Runner-up detectie
                try:
                    cands = spreads_df.copy()
                    min_p_val = filters.get('min_profit', 40.0)
                    min_pop_val = filters.get('min_pop', 60.0)
                    min_bep_val = filters.get('min_bep_dist_pct', 6.0)
                    min_d_val = filters.get('min_delta', 0.10)
                    
                    p_sc = (cands['max_profit'] / max(1.0, min_p_val)).clip(upper=1.0)
                    pop_sc = (cands['pop'] / max(1.0, min_pop_val)).clip(upper=1.0)
                    bep_sc = (cands['bep_afstand_pct'] / max(1.0, min_bep_val)).clip(upper=1.0) if 'bep_afstand_pct' in cands.columns else 1.0
                    d_sc = (cands['delta_sell'].abs() / max(0.01, min_d_val)).clip(upper=1.0) if 'delta_sell' in cands.columns else 1.0
                    
                    cands['match_pct'] = p_sc * 0.35 + pop_sc * 0.25 + bep_sc * 0.25 + d_sc * 0.15
                    runners = cands.sort_values('match_pct', ascending=False).head(2)
                    for _, r_miss in runners.iterrows():
                        log_func(f"   💡 Runner-up (Near-miss): Strike {r_miss['strike_buy']:.0f}/{r_miss['strike_sell']:.0f} | Winst: ${r_miss['max_profit']:.1f} | BEP: {r_miss.get('bep_afstand_pct', 0.0):.1f}% | Delta: {r_miss.get('delta_sell', 0.0):.3f} | PoP: {r_miss['pop']:.1f}% ({r_miss['match_pct']*100:.0f}% match)")
                except Exception:
                    pass
            else:
                log_func(f"✅ Filtered down to {len(df)} spreads")
            
        return df

    def get_filter_guidance(self, df, target_n=10):
        """
        Analyzes unfiltered results and suggests filter values to reach target_n spreads.
        Uses quantiles to find the 'Top X' thresholds.
        """
        if df.empty or len(df) <= target_n:
            return {}
            
        n_total = len(df)
        # We want the top (target_n / n_total) fraction.
        # So we want the (1 - fraction) quantile.
        q_target = 1.0 - (target_n / n_total)
        q_target = max(0.0, min(0.99, q_target)) # Clip to sane range
        
        guidance = {}
        
        # 1. PoP Guidance
        if 'pop' in df.columns:
            guidance['suggested_pop'] = round(df['pop'].quantile(q_target), 1)
            
        # 2. Profit Guidance
        if 'max_profit' in df.columns:
            guidance['suggested_profit'] = round(df['max_profit'].quantile(q_target), 2)
            
        # 3. Delta Sell Guidance (Median of top results is often better than quantile if looking for 'typical')
        if 'delta_sell' in df.columns:
            # Sort by profit and take top target_n, then get median delta
            top_by_profit = df.sort_values('max_profit', ascending=False).head(target_n)
            guidance['suggested_delta'] = round(top_by_profit['delta_sell'].abs().median(), 3)
            
        return guidance

    def build_long_comparison_matrix(self, spreads_df, underlying_price):
        """
        Berekent een side-by-side vergelijkingsmatrix tussen 1x Deep ITM (1% koopdrempel)
        en Nx ATM (meervoudig) op basis van een gelijk investeringsbudget.
        """
        if spreads_df is None or spreads_df.empty or underlying_price <= 0:
            return None
        
        long_df = spreads_df[spreads_df['strategy'].isin(['LongCall', 'LongPut'])].copy()
        if long_df.empty:
            return None

        strat = long_df['strategy'].iloc[0]
        u_price = float(underlying_price)

        # Separate candidates into Deep ITM and ATM
        if strat == 'LongCall':
            itm_pool = long_df[(long_df['strike_buy'] < u_price) & (long_df['delta_buy'] >= 0.70)]
            atm_pool = long_df[(abs(long_df['strike_buy'] - u_price) / u_price <= 0.05) & (long_df['delta_buy'] >= 0.35) & (long_df['delta_buy'] <= 0.65)]
        else:
            itm_pool = long_df[(long_df['strike_buy'] > u_price) & (long_df['delta_buy'].abs() >= 0.70)]
            atm_pool = long_df[(abs(long_df['strike_buy'] - u_price) / u_price <= 0.05) & (long_df['delta_buy'].abs() >= 0.35) & (long_df['delta_buy'].abs() <= 0.65)]

        if itm_pool.empty:
            itm_pool = long_df.sort_values('strike_buy', ascending=(strat == 'LongCall')).head(3)
        if atm_pool.empty:
            dist = (long_df['strike_buy'] - u_price).abs()
            atm_pool = long_df.loc[[dist.idxmin()]]

        # Best ITM (highest AG_Score or lowest req_bep_move)
        best_itm = itm_pool.sort_values('AG_Score', ascending=False).iloc[0]
        
        # Best ATM with matching expiry if possible
        atm_same_exp = atm_pool[atm_pool['expiry'] == best_itm['expiry']]
        best_atm = atm_same_exp.sort_values('AG_Score', ascending=False).iloc[0] if not atm_same_exp.empty else atm_pool.sort_values('AG_Score', ascending=False).iloc[0]

        itm_cost = float(best_itm['worst_entry_signed']) * 100.0
        atm_cost_single = float(best_atm['worst_entry_signed']) * 100.0
        n_atm = max(1, int(itm_cost // max(1.0, atm_cost_single)))
        atm_total_cost = n_atm * atm_cost_single

        # Scenarios: Dip -5%, Vlak 0%, Winst +3%, Winst +5%, Uitbraak +10%
        scenarios = [-0.05, 0.0, 0.03, 0.05, 0.10] if strat == 'LongCall' else [0.05, 0.0, -0.03, -0.05, -0.10]
        labels = ['Dip -5%', 'Vlak 0%', 'Winst +3%', 'Winst +5%', 'Uitbraak +10%']

        scenario_rows = []
        for sc, lab in zip(scenarios, labels):
            s_end = u_price * (1.0 + sc)
            if strat == 'LongCall':
                val_itm = max(0.0, s_end - float(best_itm['strike_buy'])) * 100.0
                val_atm = max(0.0, s_end - float(best_atm['strike_buy'])) * 100.0 * n_atm
            else:
                val_itm = max(0.0, float(best_itm['strike_buy']) - s_end) * 100.0
                val_atm = max(0.0, float(best_atm['strike_buy']) - s_end) * 100.0 * n_atm

            pnl_itm = val_itm - itm_cost
            pnl_atm = val_atm - atm_total_cost
            pct_itm = (pnl_itm / itm_cost) * 100.0
            pct_atm = (pnl_atm / atm_total_cost) * 100.0

            scenario_rows.append({
                'Scenario': lab,
                'Eindkoers': f"${s_end:.2f}",
                '1x Deep ITM ($)': f"{pnl_itm:+.2f}",
                '1x Deep ITM (%)': f"{pct_itm:+.1f}%",
                f'{n_atm}x ATM ($)': f"{pnl_atm:+.2f}",
                f'{n_atm}x ATM (%)': f"{pct_atm:+.1f}%",
                'Voordeel': '🛡️ ITM Veiliger' if pnl_itm > pnl_atm else '⚡ ATM Rendement'
            })

        return {
            'strategy': strat,
            'underlying_price': u_price,
            'itm_contract': best_itm,
            'atm_contract': best_atm,
            'itm_cost': itm_cost,
            'atm_single_cost': atm_cost_single,
            'atm_multiplier': n_atm,
            'atm_total_cost': atm_total_cost,
            'itm_bep_move_pct': float(best_itm.get('req_bep_move_pct', 0.0)),
            'atm_bep_move_pct': float(best_atm.get('req_bep_move_pct', 0.0)),
            'itm_pop': float(best_itm.get('pop', 0.0)),
            'atm_pop': float(best_atm.get('pop', 0.0)),
            'itm_delta_total': float(best_itm.get('delta_buy', 0.0)),
            'atm_delta_total': float(best_atm.get('delta_buy', 0.0)) * n_atm,
            'itm_capital_risk_dip5': float(best_itm.get('capital_risk_dip5_pct', 0.0)),
            'atm_capital_risk_dip5': float(best_atm.get('capital_risk_dip5_pct', 100.0)),
            'scenario_df': pd.DataFrame(scenario_rows)
        }

    def analyze_filter_bottlenecks(self, unfiltered_df, filters, target_n=5):
        """
        Analyzes why candidate spreads were filtered out and computes:
        1. Exact count & percentage of candidates dropped per filter rule.
        2. Average (and median) actual metric values of the generated candidates.
        3. Top 3 bottleneck filter reasons (ranked by rejection impact).
        4. Minimum required threshold adjustments (minimaal benodigde instellingen) to yield at least target_n candidates.
        """
        if unfiltered_df is None or unfiltered_df.empty:
            return {
                'total_generated': 0,
                'breakdown': [],
                'top_bottlenecks': []
            }

        n_total = len(unfiltered_df)
        df = unfiltered_df.copy()

        filter_specs = [
            {
                'key': 'only_koopadvies',
                'name': 'Koopadvies (1% Regel)',
                'col': 'koopadvies',
                'type': 'bool',
                'setting_str': 'Alleen ✅ Koopadvies' if filters.get('only_koopadvies') else 'Alle trades',
                'active': bool(filters.get('only_koopadvies'))
            },
            {
                'key': 'min_profit',
                'name': 'Minimaal Rendement / Profit ($)',
                'col': 'max_profit',
                'type': 'min',
                'setting_str': f"${filters.get('min_profit', 0):.2f}",
                'active': filters.get('min_profit', 0) > 0
            },
            {
                'key': 'min_pop',
                'name': 'Winstkans (PoP %)',
                'col': 'pop',
                'type': 'min',
                'setting_str': f"{filters.get('min_pop', 0):.1f}%",
                'active': filters.get('min_pop', 0) > 0
            },
            {
                'key': 'min_bep_dist_pct',
                'name': 'Min. BEP Buffer Afstand %',
                'col': 'bep_afstand_pct',
                'type': 'min',
                'setting_str': f">= {filters.get('min_bep_dist_pct', 0):.1f}%",
                'active': filters.get('min_bep_dist_pct', 0) > 0
            },
            {
                'key': 'delta_range',
                'name': 'Delta Sell Bereik',
                'col': 'delta_sell',
                'type': 'range_abs',
                'setting_str': f"[{filters.get('min_delta', 0.10):.2f} - {filters.get('max_delta', 0.30):.2f}]",
                'active': filters.get('min_delta', 0) > 0 or filters.get('max_delta', 1) < 1
            },
            {
                'key': 'max_pain_dist',
                'name': 'Max Pain Afstand ($)',
                'col': 'spread_dist_max_pain',
                'type': 'max',
                'setting_str': f"<= ${filters.get('max_pain_dist', 0):.2f}",
                'active': filters.get('max_pain_dist', 0) > 0
            },
            {
                'key': 'min_gamma',
                'name': 'Minimaal Gamma',
                'col': 'gamma',
                'type': 'min',
                'setting_str': f">= {filters.get('min_gamma', 0):.3f}",
                'active': filters.get('min_gamma', 0) != 0
            },
            {
                'key': 'dte_range',
                'name': 'DTE Looptijd Range',
                'col': 'dte',
                'type': 'range',
                'setting_str': f"{filters.get('min_dte', 0)} - {filters.get('max_dte', 999)} d",
                'active': True
            }
        ]

        breakdown = []
        q_target = max(0.0, min(0.99, 1.0 - (target_n / n_total))) if n_total > 0 else 0.5

        for spec in filter_specs:
            col = spec['col']
            if col not in df.columns and spec['type'] != 'bool':
                continue

            isolated_fails = 0
            avg_val = "-"
            suggested_val = "-"

            if spec['type'] == 'bool' and col in df.columns:
                isolated_fails = int((df[col] != "✅").sum())
                pass_ratio = float((df[col] == "✅").mean() * 100.0)
                avg_val = f"{pass_ratio:.1f}% met ✅"
                suggested_val = "Vink 'Alleen Koopadvies' uit" if filters.get('only_koopadvies') else "N.v.t."
            elif spec['type'] == 'min' and col in df.columns:
                thresh = float(filters.get(spec['key'], 0))
                vals = df[col].dropna()
                if not vals.empty:
                    if spec['key'] == 'min_bep_dist_pct':
                        # Exclude strategies where positive BEP buffer does not apply (Longs & Synthetics)
                        applicable_mask = ~df['strategy'].isin(['LongCall', 'LongPut', 'SynthCoveredCall', 'SynthCoveredPut'])
                        vals = df.loc[applicable_mask, col].dropna() if applicable_mask.any() else pd.Series(dtype=float)
                        isolated_fails = int((vals < thresh).sum()) if thresh > 0 and not vals.empty else 0
                        if not vals.empty:
                            mean_val = float(vals.mean())
                            avg_val = f"{mean_val:.1f}%"
                            q_val = float(vals.quantile(q_target))
                            suggested_val = f"{q_val:.1f}%"
                        else:
                            avg_val = "N.v.t. (Alleen debit/synth)"
                            suggested_val = "N.v.t."
                    else:
                        isolated_fails = int((vals < thresh).sum()) if thresh > 0 else 0
                        mean_val = float(vals.mean())
                        avg_val = f"${mean_val:.2f}" if 'profit' in col else f"{mean_val:.1f}%"
                        q_val = float(vals.quantile(q_target))
                        suggested_val = f"${q_val:.2f}" if 'profit' in col else f"{q_val:.1f}%"
            elif spec['type'] == 'range_abs' and col in df.columns:
                min_d = float(filters.get('min_delta', 0.0))
                max_d = float(filters.get('max_delta', 1.0))
                abs_vals = df[col].abs().dropna()
                if not abs_vals.empty:
                    isolated_fails = int(((abs_vals < min_d) | (abs_vals > max_d)).sum())
                    avg_val = f"{abs_vals.mean():.3f}"
                    q_low = float(abs_vals.quantile(0.1))
                    q_high = float(abs_vals.quantile(0.9))
                    suggested_val = f"[{q_low:.2f} - {q_high:.2f}]"
            elif spec['type'] == 'min_abs' and col in df.columns:
                thresh = float(filters.get(spec['key'], 0))
                abs_vals = df[col].abs().dropna()
                if not abs_vals.empty:
                    isolated_fails = int((abs_vals < thresh).sum()) if thresh > 0 else 0
                    avg_val = f"{abs_vals.mean():.3f}"
                    q_val = float(abs_vals.quantile(q_target))
                    suggested_val = f">= {q_val:.3f}"
            elif spec['type'] == 'max' and col in df.columns:
                thresh = float(filters.get(spec['key'], 999))
                vals = df[col].dropna()
                if not vals.empty:
                    isolated_fails = int((vals > thresh).sum()) if thresh > 0 else 0
                    avg_val = f"${vals.mean():.2f}"
                    q_val = float(vals.quantile(min(1.0, target_n / n_total)))
                    suggested_val = f"<= ${q_val:.2f}"
            elif spec['type'] == 'range' and col in df.columns:
                min_d = float(filters.get('min_dte', 0))
                max_d = float(filters.get('max_dte', 999))
                synth_min = float(filters.get('min_short_dte', 20))
                synth_max = float(filters.get('max_short_dte', 50))
                
                is_synth = df['strategy'].isin(['SynthCoveredCall', 'SynthCoveredPut'])
                fails_std = ((df.loc[~is_synth, col] < min_d) | (df.loc[~is_synth, col] > max_d)).sum() if (~is_synth).any() else 0
                fails_synth = ((df.loc[is_synth, col] < synth_min) | (df.loc[is_synth, col] > synth_max)).sum() if is_synth.any() else 0
                isolated_fails = int(fails_std + fails_synth)
                vals = df[col].dropna()
                if not vals.empty:
                    avg_val = f"{vals.mean():.1f} d"
                    suggested_val = f"{int(vals.min())} - {int(vals.max())} d"

            pct_failed = float((isolated_fails / n_total) * 100.0) if n_total > 0 else 0.0

            breakdown.append({
                'key': spec['key'],
                'name': spec['name'],
                'setting_str': spec['setting_str'],
                'active': spec['active'],
                'dropped_count': isolated_fails,
                'dropped_pct': round(pct_failed, 1),
                'actual_avg': avg_val,
                'suggested_min': suggested_val
            })

        sorted_bottlenecks = sorted(breakdown, key=lambda x: x['dropped_count'], reverse=True)
        top_3 = sorted_bottlenecks[:3]

        return {
            'total_generated': n_total,
            'breakdown': sorted_bottlenecks,
            'top_bottlenecks': top_3
        }
    
    def rank_spreads(self, spreads_df, sort_criteria=None, top_n=100, max_per_symbol=None):
        """
        Ranks spreads based on a list of criteria.
        sort_criteria: list of strings, e.g. ['expected_move', 'gamma', 'delta', 'max_pain']
        max_per_symbol: max number of results to keep per symbol for diversification
        """
        if spreads_df.empty:
            return spreads_df
            
        if not sort_criteria:
            sort_criteria = ['max_profit'] # Default
            
        # Map user criteria to DataFrame columns and sort direction
        # True = Ascending, False = Descending
        sort_cols = []
        sort_asc = []
        
        for crit in sort_criteria:
            if crit == 'AG Score':
                sort_cols.append('AG_Score')
                sort_asc.append(False)
            elif crit == 'expected_move':
                # Placeholder: prioritize spreads covering expected move?
                pass 
            elif crit == 'gamma':
                # Gamma Exposure: Usually want High Gamma (Long) or Low Gamma (Short)?
                # User says: "exposure". Let's assume Sorting by Magnitude (descending)
                sort_cols.append('gamma')
                sort_asc.append(False)
            elif crit == 'delta':
                sort_cols.append('delta')
                sort_asc.append(False) # Highest Delta first?
            elif crit == 'max_pain':
                # "Distance to max pain" -> Smallest distance preferred?
                # "spread_dist_max_pain" (ASC)
                if 'spread_dist_max_pain' in spreads_df.columns:
                    sort_cols.append('spread_dist_max_pain')
                    sort_asc.append(True)
            elif crit == 'min_delta_buy':
                # "minimum delta buy optie" -> Actually a filter?
                # If sorting, maybe descending delta_buy?
                if 'delta_buy' in spreads_df.columns:
                    sort_cols.append('delta_buy')
                    sort_asc.append(False)
            elif crit == 'profit':
               sort_cols.append('max_profit')
               sort_asc.append(False)
            elif crit == 'pop':
               sort_cols.append('pop')
               sort_asc.append(False) 
            elif crit == 'theta':
                sort_cols.append('theta')
                sort_asc.append(False)
        
        if not sort_cols:
            sort_cols = ['pop', 'max_profit']
            sort_asc = [False, False]

        try:
            sorted_df = spreads_df.sort_values(by=sort_cols, ascending=sort_asc)
            if max_per_symbol and max_per_symbol > 0 and 'symbol' in sorted_df.columns:
                sorted_df = sorted_df.groupby('symbol', group_keys=False).head(max_per_symbol).sort_values(by=sort_cols, ascending=sort_asc)
            return sorted_df.head(top_n)
        except KeyError:
            # Fallback if specific columns missing
            return spreads_df.head(top_n)

    def get_target_expirations(self, min_dte, max_dte):
        import datetime
        import pandas as pd
        today = datetime.date.today()
        
        # Helper to get the third Friday of a month
        def get_third_friday(year, month):
            first_day = datetime.date(year, month, 1)
            first_weekday = first_day.weekday()
            days_to_first_friday = (4 - first_weekday) % 7
            return first_day + datetime.timedelta(days=days_to_first_friday + 14)
            
        # 1. Find monthly expirations in range
        monthly_expirations = []
        current_date = today
        for _ in range(4): # Check 4 months ahead
            tf = get_third_friday(current_date.year, current_date.month)
            dte = (tf - today).days
            if min_dte <= dte <= max_dte:
                monthly_expirations.append(tf.strftime('%Y%m%d'))
            # Move to next month
            year, month = current_date.year, current_date.month
            if month == 12:
                current_date = datetime.date(year + 1, 1, 1)
            else:
                current_date = datetime.date(year, month + 1, 1)
                
        if monthly_expirations:
            return monthly_expirations
            
        # 2. Fallback: all Fridays in range
        fridays = []
        for dte in range(int(min_dte), int(max_dte) + 1):
            target_date = today + datetime.timedelta(days=dte)
            if target_date.weekday() == 4: # Friday
                fridays.append(target_date.strftime('%Y%m%d'))
                
        return fridays

    def get_candidate_strikes(self, symbol, price):
        if price <= 0:
            return []
        candidates = set()
        
        # Determine likely intervals based on price and symbol
        is_etf = symbol.upper() in ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLK", "XLE", "XLV", "XLI", "XLY", "XLP", "XLB", "XLU", "XLRE", "GDX", "GLD", "TLT", "SLV", "USO", "UNG", "KRE", "SMH", "IBB", "XOP", "ARKK", "EEM", "FXI", "EWZ"]
        
        if price > 250:
            intervals = [2.5, 5.0, 10.0]
            if is_etf:
                intervals.extend([1.0])
        elif price > 50:
            intervals = [1.0, 2.5, 5.0]
            if is_etf:
                intervals.extend([0.5])
        else:
            intervals = [0.5, 1.0, 2.5, 5.0]
            
        for inv in intervals:
            lower = math.floor(price / inv) * inv
            upper = math.ceil(price / inv) * inv
            candidates.add(float(lower))
            candidates.add(float(upper))
            
        valid_candidates = sorted([c for c in candidates if c > 0])
        # Keep only the closest 3 strikes to minimize options to qualify
        close_candidates = sorted(valid_candidates, key=lambda c: abs(c - price))[:3]
        return close_candidates

    def run_fast_atm_scan(self, symbols, strategies, min_dte, max_dte, koopadvies_p, log_func=None, progress_callback=None):
        """
        Executes a super-fast, stripped-down ATM Long Call/Put scan.
        """
        import yfinance as yf
        import pandas as pd
        import numpy as np
        import time
        import math
        import datetime
        import concurrent.futures
        from ib_insync import Option, Stock
        
        def log(msg):
            if log_func: 
                try:
                    log_func(msg)
                except Exception:
                    pass
            else: 
                try:
                    print(msg)
                except UnicodeEncodeError:
                    print(str(msg).encode('ascii', errors='replace').decode('ascii'))
            
        start_time = time.time()
        if progress_callback:
            progress_callback(5, "Koersen ophalen via yfinance (bulk)...", None)
            
        if not strategies or not any(s in ['LongCall', 'LongPut'] for s in strategies):
            strategies = ['LongCall', 'LongPut']
            log("ℹ️ Geen specifieke Long-strategie geselecteerd. Supersnelle scan activeert automatisch LongCall en LongPut.")
            
        log(f"🚀 Start Supersnelle Scan op {len(symbols)} symbolen...")
        
        # Helper for Index -> yfinance symbol mapping
        YF_INDEX_MAP = {
            'SPX': '^SPX',
            'NDX': '^NDX',
            'RUT': '^RUT',
            'VIX': '^VIX',
            'DAX': '^GDAXI',
            'DJI': '^DJI'
        }
        def to_yf_sym(s):
            return YF_INDEX_MAP.get(s.upper(), s)

        # 1. Fetch stock prices via yfinance bulk download & parallel fallback
        log("📊 Koersen ophalen via yfinance (bulk)...")
        symbol_prices = {}
        try:
            yf_symbols = [to_yf_sym(s) for s in symbols]
            tickers_str = " ".join(yf_symbols)
            prices_df = yf.download(tickers_str, period="1d", group_by="ticker", progress=False, threads=True, timeout=25)
            
            if isinstance(prices_df.columns, pd.MultiIndex):
                lvl0 = prices_df.columns.levels[0]
                lvl1 = prices_df.columns.levels[1]
                for sym in symbols:
                    yf_s = to_yf_sym(sym)
                    try:
                        c_series = None
                        if yf_s in lvl0:
                            c_series = prices_df[yf_s]['Close'].dropna()
                        elif 'Close' in lvl0 and yf_s in lvl1:
                            c_series = prices_df['Close'][yf_s].dropna()
                        if c_series is not None and not c_series.empty:
                            symbol_prices[sym] = float(c_series.iloc[-1])
                    except Exception:
                        pass
            elif not prices_df.empty and 'Close' in prices_df.columns:
                c_series = prices_df['Close'].dropna()
                if not c_series.empty:
                    symbol_prices[symbols[0]] = float(c_series.iloc[-1])
        except Exception as e:
            log(f"⚠️ yfinance bulk download melding: {e}")
            
        # Parallel fallback for missing prices
        missing_symbols = [s for s in symbols if s not in symbol_prices or symbol_prices[s] <= 0]
        if missing_symbols:
            log(f"📡 Ophalen van {len(missing_symbols)} ontbrekende koersen via snelle parallelle fallback...")
            def fetch_single_price(sym):
                try:
                    yf_s = to_yf_sym(sym)
                    t = yf.Ticker(yf_s)
                    h = t.history(period="1d")
                    if not h.empty:
                        return sym, float(h['Close'].iloc[-1])
                except Exception:
                    pass
                return sym, 0.0

            max_workers = min(30, len(missing_symbols))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futs = [executor.submit(fetch_single_price, s) for s in missing_symbols]
                for fut in concurrent.futures.as_completed(futs):
                    s, p = fut.result()
                    if p > 0:
                        symbol_prices[s] = p
                        
        log(f"✅ Koersen opgehaald voor {len(symbol_prices)}/{len(symbols)} symbolen.")
        if not symbol_prices:
            log("❌ Geen koersen kunnen ophalen.")
            if progress_callback:
                progress_callback(100, "Geen koersen opgehaald.", 0)
            return pd.DataFrame()
            
        # 2. Get target expirations
        expirations = self.get_target_expirations(min_dte, max_dte)
        if not expirations:
            # Fallback: estimate Friday expirations if range was too narrow
            today_date = datetime.date.today()
            for dte in range(max(1, int(min_dte)), int(max_dte) + 60):
                target_date = today_date + datetime.timedelta(days=dte)
                if target_date.weekday() == 4:
                    expirations.append(target_date.strftime('%Y%m%d'))
                    break
        log(f"📅 Doel-expiratie(s): {', '.join(expirations) if expirations else 'Automatisch dynamisch per symbool'}")
        
        # 3. Option Market Data Retrieval
        log(f"📡 Optiedata ophalen voor {len(symbol_prices)} symbolen...")
        if progress_callback:
            progress_callback(30, "Optie-koersen ophalen...", None)
            
        option_data = []
        
        # TWS option retrieval if connected and candidate list is reasonably sized
        if self.ib_client.is_connected() and len(symbol_prices) <= 20 and expirations:
            candidate_options = []
            for sym, price in symbol_prices.items():
                strikes = self.get_candidate_strikes(sym, price)
                for exp in expirations:
                    for strike in strikes:
                        if 'LongCall' in strategies:
                            candidate_options.append(Option(symbol=sym, lastTradeDateOrContractMonth=exp, strike=float(strike), right='C', multiplier='100', exchange='SMART', currency='USD'))
                        if 'LongPut' in strategies:
                            candidate_options.append(Option(symbol=sym, lastTradeDateOrContractMonth=exp, strike=float(strike), right='P', multiplier='100', exchange='SMART', currency='USD'))
                            
            if candidate_options:
                chunk_size = 50
                try:
                    for i in range(0, len(candidate_options), chunk_size):
                        chunk = candidate_options[i:i+chunk_size]
                        tickers = [self.ib_client.ib.reqMktData(c, '', False, False) for c in chunk]
                        self.ib_client.ib.sleep(0.8)
                        for t in tickers:
                            bid = t.bid if t.bid > 0 else 0.0
                            ask = t.ask if t.ask > 0 else 0.0
                            last = t.last if t.last > 0 else 0.0
                            mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else (last if last > 0 else t.close)
                            if mid > 0:
                                option_data.append({
                                    'conId': t.contract.conId,
                                    'symbol': t.contract.symbol,
                                    'strike': t.contract.strike,
                                    'right': t.contract.right,
                                    'expiry': t.contract.lastTradeDateOrContractMonth,
                                    'bid': bid, 'ask': ask if ask > 0 else mid, 'mid': mid, 'last': last if last > 0 else mid
                                })
                            self.ib_client.ib.cancelMktData(t.contract)
                except Exception as e:
                    log(f"⚠️ TWS optiedata ophalen afgebroken: {e}")
                    
        # 4. yfinance parallel option data retrieval (super fast & accurate ATM matching)
        if len(option_data) < len(symbol_prices) * 0.5:
            log("⚡ Snel parallel optie-ketens analyseren via yfinance (ATM matching)...")
            today_date = datetime.date.today()
            
            def fetch_yf_option_data(sym):
                rows = []
                s_price = symbol_prices.get(sym, 0.0)
                if s_price <= 0:
                    return rows
                try:
                    yf_s = to_yf_sym(sym)
                    ticker = yf.Ticker(yf_s)
                    available_exps = list(ticker.options) if ticker.options else []
                    if not available_exps:
                        return rows
                        
                    target_exps = []
                    for exp_str in available_exps:
                        try:
                            exp_date = datetime.datetime.strptime(exp_str, '%Y-%m-%d').date()
                            dte = (exp_date - today_date).days
                            if min_dte <= dte <= max_dte:
                                target_exps.append((exp_str, dte))
                        except Exception:
                            pass
                            
                    if not target_exps and available_exps:
                        mid_target_dte = (min_dte + max_dte) / 2
                        best_exp, best_diff, best_dte = None, 9999, 30
                        for exp_str in available_exps:
                            try:
                                exp_date = datetime.datetime.strptime(exp_str, '%Y-%m-%d').date()
                                dte = (exp_date - today_date).days
                                if dte > 0:
                                    diff = abs(dte - mid_target_dte)
                                    if diff < best_diff:
                                        best_diff, best_exp, best_dte = diff, exp_str, dte
                            except Exception:
                                pass
                        if best_exp:
                            target_exps.append((best_exp, best_dte))
                            
                    for exp_str, dte in target_exps[:1]:
                        try:
                            chain = ticker.option_chain(exp_str)
                            exp_clean = exp_str.replace('-', '')
                            
                            # True ATM LongCall matching
                            if 'LongCall' in strategies and not chain.calls.empty:
                                calls = chain.calls
                                idx = (calls['strike'] - s_price).abs().idxmin()
                                row = calls.loc[idx]
                                strike_val = round(float(row['strike']), 4)
                                bid = float(row.get('bid', 0.0))
                                ask = float(row.get('ask', 0.0))
                                last = float(row.get('lastPrice', 0.0))
                                intr = max(0.0, s_price - strike_val)
                                min_valid = max(0.0, intr - 0.50)
                                
                                mid = (bid + ask) / 2 if (bid > 0 and ask > 0 and ((bid + ask)/2) >= min_valid) else (last if last >= min_valid and last > 0 else max(min_valid, 0.05))
                                bid_val = bid if bid >= min_valid and bid > 0 else mid
                                ask_val = ask if ask >= min_valid and ask > 0 else mid
                                
                                rows.append({
                                    'conId': 0, 'symbol': sym, 'strike': strike_val, 'right': 'C',
                                    'expiry': exp_clean, 'bid': bid_val, 'ask': ask_val, 'mid': mid, 'last': mid
                                })
                                
                            # True ATM LongPut matching
                            if 'LongPut' in strategies and not chain.puts.empty:
                                puts = chain.puts
                                idx = (puts['strike'] - s_price).abs().idxmin()
                                row = puts.loc[idx]
                                strike_val = round(float(row['strike']), 4)
                                bid = float(row.get('bid', 0.0))
                                ask = float(row.get('ask', 0.0))
                                last = float(row.get('lastPrice', 0.0))
                                intr = max(0.0, strike_val - s_price)
                                min_valid = max(0.0, intr - 0.50)
                                
                                mid = (bid + ask) / 2 if (bid > 0 and ask > 0 and ((bid + ask)/2) >= min_valid) else (last if last >= min_valid and last > 0 else max(min_valid, 0.05))
                                bid_val = bid if bid >= min_valid and bid > 0 else mid
                                ask_val = ask if ask >= min_valid and ask > 0 else mid
                                
                                rows.append({
                                    'conId': 0, 'symbol': sym, 'strike': strike_val, 'right': 'P',
                                    'expiry': exp_clean, 'bid': bid_val, 'ask': ask_val, 'mid': mid, 'last': mid
                                })
                        except Exception:
                            pass
                except Exception:
                    pass
                return rows

            symbols_to_fetch = list(symbol_prices.keys())
            yf_results = []
            max_workers = min(35, len(symbols_to_fetch))
            if max_workers > 0:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(fetch_yf_option_data, s): s for s in symbols_to_fetch}
                    for fut in concurrent.futures.as_completed(futures):
                        yf_results.extend(fut.result())
                        
            existing_keys = set((opt['symbol'], opt['right'], opt['expiry'], round(opt['strike'], 4)) for opt in option_data)
            for opt in yf_results:
                key = (opt['symbol'], opt['right'], opt['expiry'], round(opt['strike'], 4))
                if key not in existing_keys:
                    option_data.append(opt)
                    existing_keys.add(key)
                    
            log(f"✅ Optiedata ophalen voltooid. Totaal {len(option_data)} ATM optie-contracten geanalyseerd.")
            
        if not option_data:
            log("❌ Geen marktdata kunnen ophalen voor opties.")
            if progress_callback:
                progress_callback(100, "Geen marktdata opgehaald.", 0)
            return pd.DataFrame()
            
        # 5. Keep best ATM option per (symbol, right, expiry)
        grouped = {}
        for opt in option_data:
            sym = opt['symbol']
            r = opt['right']
            exp = opt['expiry']
            key = (sym, r, exp)
            
            s_price = symbol_prices.get(sym, 0.0)
            if s_price <= 0: continue
            
            dist = abs(opt['strike'] - s_price)
            if key not in grouped or dist < grouped[key]['dist']:
                grouped[key] = {
                    'opt': opt,
                    'dist': dist
                }
                
        # 6. Build the final results DataFrame
        final_rows = []
        for key, val in grouped.items():
            opt = val['opt']
            sym = opt['symbol']
            s_price = symbol_prices[sym]
            strike = opt['strike']
            r = opt['right']
            exp = opt['expiry']
            mid = opt['mid']
            ask = opt['ask']
            last = opt['last']
            
            try:
                target_date = pd.to_datetime(exp)
                now_date = pd.Timestamp.now().normalize()
                dte = (target_date - now_date).days
            except Exception:
                dte = 30
                
            p = koopadvies_p
            if r == 'C':
                target_price = s_price * (1 + p)
                payout = max(0.0, target_price - strike)
                strat = "LongCall"
            else:
                target_price = s_price * (1 - p)
                payout = max(0.0, strike - target_price)
                strat = "LongPut"
                
            winst_laat = (payout - ask) * 100
            winst_midden = (payout - mid) * 100
            winst_laatste = (payout - last) * 100
            koopadvies = "✅" if winst_laat > 0 else "❌"
            
            final_rows.append({
                'koopadvies': koopadvies,
                'koopadvies_status': koopadvies,
                'symbol': sym,
                'underlying_price': s_price,
                'strategy': strat,
                'expiry': exp,
                'strike_buy': strike,
                'strike_sell': 0.0,
                'right': r,
                'width': 0.0,
                'spread_ask_abs': ask,
                'spread_mid_abs': mid,
                'spread_last_abs': last,
                'price_buy': ask,
                'price_sell': 0.0,
                'net_price': mid,
                'winst_laat': winst_laat,
                'winst_midden': winst_midden,
                'winst_laatste': winst_laatste,
                'dte': dte,
                'pop': 50.0,
                'max_profit': winst_laat,
                'TTP (D)': 99.0,
                'TEI Score': 1.0,
                'AG_Score': round(winst_laat, 1)
            })
            
        df_results = pd.DataFrame(final_rows)
        log(f"✨ Supersnelle scan voltooid. {len(df_results)} ATM opties geëvalueerd.")
        if progress_callback:
            progress_callback(100, "Supersnelle scan voltooid!", 0)
        return df_results


class PortfolioAnalyzer:
    """
    Analyzes open portfolio positions (Spreads & Single Leg Options) 
    using technical indicators and option metrics to recommend optimal loss-mitigation 
    and profit-locking exit strategies.
    """
    _dividend_cache = {}

    def __init__(self, ib_client=None):
        self.ib_client = ib_client

    def get_dividend_info(self, symbol):
        if not symbol or symbol in ['N/A', '']:
            return {'div_date': None, 'div_amount': 0.0}
        if symbol in self._dividend_cache:
            return self._dividend_cache[symbol]
        try:
            import yfinance as yf
            t = yf.Ticker(symbol)
            div_date = None
            div_amount = 0.0
            cal = getattr(t, 'calendar', None)
            if cal is not None and not (hasattr(cal, 'empty') and cal.empty):
                if isinstance(cal, dict):
                    div_date = cal.get('Ex-Dividend Date') or cal.get('Dividend Date')
                elif hasattr(cal, 'get'):
                    div_date = cal.get('Ex-Dividend Date')
            divs = getattr(t, 'dividends', None)
            if divs is not None and len(divs) > 0:
                div_amount = float(divs.iloc[-1])
            res = {'div_date': str(div_date) if div_date else None, 'div_amount': div_amount}
            self._dividend_cache[symbol] = res
            return res
        except Exception:
            res = {'div_date': None, 'div_amount': 0.0}
            self._dividend_cache[symbol] = res
            return res

    def calculate_position_financials(self, pos):
        """
        Berekent diepgaande financiële metrics voor de portfolio check:
        - Koers van de onderliggende waarde (aandeel)
        - Koers / instapwaarde van het contract (of spread / aandeel)
        - Aparte specificatie van elke afzonderlijke optiepoot (voor multi-leg spreads)
        - Huidige winst/verlies ($ en %)
        - Break-Even Point (BEP) van de onderliggende waarde + afstand tot BEP ($ en %)
        - Koersdoelen van het aandeel voor 1% en 5% winst + benodigde afstand ($ en %)
        """
        sym = pos.get('symbol', 'N/A')
        strat = pos.get('strategy', 'Spread')
        und_p = float(pos.get('underlying_price', 0.0) or 0.0)
        entry_p = float(pos.get('entry_price', 0.0) or 0.0)
        mkt_p = float(pos.get('market_price', 0.0) or 0.0)
        qty = int(pos.get('qty', 1) or 1)
        pnl_usd = float(pos.get('unrealized_pnl', 0.0) or 0.0)
        pnl_pct = float(pos.get('pnl_pct', 0.0) or 0.0)
        is_long = pos.get('is_long', True)
        sold_k = float(pos.get('sold_strike', 0.0) or 0.0)
        bought_k = float(pos.get('bought_strike', 0.0) or 0.0)
        spread_w = abs(sold_k - bought_k) if (sold_k > 0 and bought_k > 0) else 5.0
        legs = pos.get('legs', [])

        legs_breakdown = []
        for l in legs:
            c = getattr(l, 'contract', None)
            if not c:
                continue
            sec_type = getattr(c, 'secType', 'OPT')
            right = getattr(c, 'right', '')
            strike = float(getattr(c, 'strike', 0.0) or 0.0)
            pos_qty = float(getattr(l, 'position', 0.0) or 0.0)
            is_leg_long = pos_qty > 0
            leg_act = "LONG" if is_leg_long else "SHORT"
            
            raw_cost = float(getattr(l, 'averageCost', 0.0) or 0.0)
            leg_cost = (raw_cost / 100.0) if (sec_type in ['OPT', 'FOP'] and raw_cost > 0) else raw_cost
            leg_mkt = float(getattr(l, 'marketPrice', 0.0) or 0.0)
            leg_pnl = float(getattr(l, 'unrealizedPNL', 0.0) or 0.0)
            
            leg_desc = f"{strike:.1f} {right}" if sec_type in ['OPT', 'FOP'] else f"Aandeel {sym}"
            legs_breakdown.append({
                'desc': leg_desc,
                'sec_type': sec_type,
                'right': right,
                'strike': strike,
                'action': leg_act,
                'position': int(pos_qty),
                'entry_price': round(leg_cost, 2),
                'market_price': round(leg_mkt, 2),
                'pnl_usd': round(leg_pnl, 2),
                'pnl_pct': round((leg_pnl / (leg_cost * abs(pos_qty) * (100.0 if sec_type in ['OPT', 'FOP'] else 1.0)) * 100.0), 1) if leg_cost > 0 else 0.0
            })

        contract_label = "Contractwaarde"
        entry_label = "Instapprijs"
        mkt_label = "Huidige Koers"

        bep_price = 0.0
        bep_dist_usd = 0.0
        bep_dist_pct = 0.0
        bep_status = ""
        is_in_profit = pnl_usd > 0

        t1_stock = 0.0
        t1_dist_usd = 0.0
        t1_dist_pct = 0.0
        t1_status = ""

        t5_stock = 0.0
        t5_dist_usd = 0.0
        t5_dist_pct = 0.0
        t5_status = ""

        if strat == 'Stock':
            contract_label = "Aandeelwaarde"
            entry_label = "Gem. Aankoopkoers"
            mkt_label = "Huidige Aandeelkoers"
            bep_price = round(entry_p, 2)

            if is_long:
                bep_dist_usd = round(bep_price - und_p, 2)
                bep_dist_pct = round((bep_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                if und_p >= bep_price:
                    bep_status = f"✅ In winstzone (+${und_p - bep_price:.2f} / +{((und_p - bep_price)/bep_price)*100:.1f}% boven BEP)"
                else:
                    bep_status = f"⚠️ Nog +${bep_dist_usd:.2f} (+{abs(bep_dist_pct):.1f}%) stijging nodig voor BEP (${bep_price:.2f})"
                
                t1_stock = round(entry_p * 1.01, 2)
                t5_stock = round(entry_p * 1.05, 2)
                t1_dist_usd = round(t1_stock - und_p, 2)
                t1_dist_pct = round((t1_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                t5_dist_usd = round(t5_stock - und_p, 2)
                t5_dist_pct = round((t5_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                
                t1_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 1.0 else f"Nog +${t1_dist_usd:.2f} (+{t1_dist_pct:.1f}%) stijging nodig tot ${t1_stock:.2f}"
                t5_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 5.0 else f"Nog +${t5_dist_usd:.2f} (+{t5_dist_pct:.1f}%) stijging nodig tot ${t5_stock:.2f}"
            else:
                bep_dist_usd = round(und_p - bep_price, 2)
                bep_dist_pct = round((bep_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                if und_p <= bep_price:
                    bep_status = f"✅ In winstzone (+${bep_price - und_p:.2f} / +{((bep_price - und_p)/bep_price)*100:.1f}% onder BEP)"
                else:
                    bep_status = f"⚠️ Nog -${bep_dist_usd:.2f} (-{abs(bep_dist_pct):.1f}%) daling nodig voor BEP (${bep_price:.2f})"
                
                t1_stock = round(entry_p * 0.99, 2)
                t5_stock = round(entry_p * 0.95, 2)
                t1_dist_usd = round(und_p - t1_stock, 2)
                t1_dist_pct = round((t1_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                t5_dist_usd = round(und_p - t5_stock, 2)
                t5_dist_pct = round((t5_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                
                t1_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 1.0 else f"Nog -${t1_dist_usd:.2f} (-{t1_dist_pct:.1f}%) daling nodig tot ${t1_stock:.2f}"
                t5_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 5.0 else f"Nog -${t5_dist_usd:.2f} (-{t5_dist_pct:.1f}%) daling nodig tot ${t5_stock:.2f}"

        elif strat in ['LongCall', 'LongPut']:
            is_call = strat == 'LongCall'
            contract_label = f"Optiepremie ({'Call' if is_call else 'Put'})"
            entry_label = "Betaalde Optiepremie"
            mkt_label = "Huidige Optiewaarde"
            strike = bought_k if bought_k > 0 else float(pos.get('strike', 0.0) or 0.0)
            
            bep_price = round((strike + entry_p) if is_call else (strike - entry_p), 2)
            if is_call:
                bep_dist_usd = round(bep_price - und_p, 2)
                bep_dist_pct = round((bep_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                if und_p >= bep_price:
                    bep_status = f"✅ In winstzone (+${und_p - bep_price:.2f} / +{((und_p - bep_price)/bep_price)*100:.1f}% boven BEP van ${bep_price:.2f})"
                else:
                    bep_status = f"⚠️ Nog +${bep_dist_usd:.2f} (+{abs(bep_dist_pct):.1f}%) stijging nodig voor BEP op expiratie (${bep_price:.2f})"
                
                t1_opt = entry_p * 1.01
                t5_opt = entry_p * 1.05
                t1_stock = round(strike + t1_opt, 2)
                t5_stock = round(strike + t5_opt, 2)
                t1_dist_usd = round(t1_stock - und_p, 2)
                t1_dist_pct = round((t1_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                t5_dist_usd = round(t5_stock - und_p, 2)
                t5_dist_pct = round((t5_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                
                t1_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 1.0 else f"Nog +${t1_dist_usd:.2f} (+{t1_dist_pct:.1f}%) stijging nodig tot ${t1_stock:.2f}"
                t5_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 5.0 else f"Nog +${t5_dist_usd:.2f} (+{t5_dist_pct:.1f}%) stijging nodig tot ${t5_stock:.2f}"
            else:
                bep_dist_usd = round(und_p - bep_price, 2)
                bep_dist_pct = round((bep_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                if und_p <= bep_price:
                    bep_status = f"✅ In winstzone (+${bep_price - und_p:.2f} / +{((bep_price - und_p)/bep_price)*100:.1f}% onder BEP van ${bep_price:.2f})"
                else:
                    bep_status = f"⚠️ Nog -${bep_dist_usd:.2f} (-{abs(bep_dist_pct):.1f}%) daling nodig voor BEP op expiratie (${bep_price:.2f})"
                
                t1_opt = entry_p * 1.01
                t5_opt = entry_p * 1.05
                t1_stock = round(strike - t1_opt, 2)
                t5_stock = round(strike - t5_opt, 2)
                t1_dist_usd = round(und_p - t1_stock, 2)
                t1_dist_pct = round((t1_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                t5_dist_usd = round(und_p - t5_stock, 2)
                t5_dist_pct = round((t5_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                
                t1_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 1.0 else f"Nog -${t1_dist_usd:.2f} (-{t1_dist_pct:.1f}%) daling nodig tot ${t1_stock:.2f}"
                t5_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 5.0 else f"Nog -${t5_dist_usd:.2f} (-{t5_dist_pct:.1f}%) daling nodig tot ${t5_stock:.2f}"

        elif strat in ['BullPut', 'BearCall', 'BullCall', 'BearPut']:
            is_credit = strat in ['BullPut', 'BearCall']
            contract_label = "Netto Spreadwaarde"
            entry_label = "Ontvangen Net Credit" if is_credit else "Betaalde Net Debit"
            mkt_label = "Huidige Sluitprijs (Debit)" if is_credit else "Huidige Sluitprijs (Credit)"
            margin_risk = max(1.0, (spread_w - entry_p) if is_credit else entry_p)

            if strat == 'BullPut':
                bep_price = round(sold_k - entry_p, 2)
                buffer_usd = round(und_p - bep_price, 2)
                buffer_pct = round((buffer_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                bep_dist_usd = buffer_usd
                bep_dist_pct = buffer_pct
                if und_p >= bep_price:
                    bep_status = f"🛡️ Veiligheidsbuffer: Koers ligt ${buffer_usd:+.2f} ({buffer_pct:+.1f}%) boven BEP van ${bep_price:.2f}"
                else:
                    bep_status = f"⚠️ Onder BEP: Koers moet nog +${abs(buffer_usd):.2f} (+{abs(buffer_pct):.1f}%) stijgen tot BEP (${bep_price:.2f})"
                
                t1_stock = round(bep_price + (0.01 * margin_risk), 2)
                t5_stock = round(bep_price + (0.05 * margin_risk), 2)
                t1_dist_usd = round(t1_stock - und_p, 2)
                t1_dist_pct = round((t1_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                t5_dist_usd = round(t5_stock - und_p, 2)
                t5_dist_pct = round((t5_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                
                t1_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 1.0 else f"Nog +${t1_dist_usd:.2f} (+{t1_dist_pct:.1f}%) stijging nodig tot ${t1_stock:.2f}"
                t5_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 5.0 else f"Nog +${t5_dist_usd:.2f} (+{t5_dist_pct:.1f}%) stijging nodig tot ${t5_stock:.2f}"

            elif strat == 'BearCall':
                bep_price = round(sold_k + entry_p, 2)
                buffer_usd = round(bep_price - und_p, 2)
                buffer_pct = round((buffer_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                bep_dist_usd = buffer_usd
                bep_dist_pct = buffer_pct
                if und_p <= bep_price:
                    bep_status = f"🛡️ Veiligheidsbuffer: Koers ligt ${buffer_usd:+.2f} ({buffer_pct:+.1f}%) onder BEP van ${bep_price:.2f}"
                else:
                    bep_status = f"⚠️ Boven BEP: Koers moet nog -${abs(buffer_usd):.2f} (-{abs(buffer_pct):.1f}%) dalen tot BEP (${bep_price:.2f})"
                
                t1_stock = round(bep_price - (0.01 * margin_risk), 2)
                t5_stock = round(bep_price - (0.05 * margin_risk), 2)
                t1_dist_usd = round(und_p - t1_stock, 2)
                t1_dist_pct = round((t1_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                t5_dist_usd = round(und_p - t5_stock, 2)
                t5_dist_pct = round((t5_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                
                t1_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 1.0 else f"Nog -${t1_dist_usd:.2f} (-{t1_dist_pct:.1f}%) daling nodig tot ${t1_stock:.2f}"
                t5_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 5.0 else f"Nog -${t5_dist_usd:.2f} (-{t5_dist_pct:.1f}%) daling nodig tot ${t5_stock:.2f}"

            elif strat == 'BullCall':
                bep_price = round(bought_k + entry_p, 2)
                bep_dist_usd = round(bep_price - und_p, 2)
                bep_dist_pct = round((bep_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                if und_p >= bep_price:
                    bep_status = f"✅ In winstzone (+${und_p - bep_price:.2f} / +{((und_p - bep_price)/bep_price)*100:.1f}% boven BEP van ${bep_price:.2f})"
                else:
                    bep_status = f"⚠️ Nog +${bep_dist_usd:.2f} (+{abs(bep_dist_pct):.1f}%) stijging nodig voor BEP op expiratie (${bep_price:.2f})"
                
                t1_stock = round(bep_price + (0.01 * entry_p), 2)
                t5_stock = round(bep_price + (0.05 * entry_p), 2)
                t1_dist_usd = round(t1_stock - und_p, 2)
                t1_dist_pct = round((t1_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                t5_dist_usd = round(t5_stock - und_p, 2)
                t5_dist_pct = round((t5_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                
                t1_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 1.0 else f"Nog +${t1_dist_usd:.2f} (+{t1_dist_pct:.1f}%) nodig tot ${t1_stock:.2f}"
                t5_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 5.0 else f"Nog +${t5_dist_usd:.2f} (+{t5_dist_pct:.1f}%) nodig tot ${t5_stock:.2f}"

            elif strat == 'BearPut':
                bep_price = round(bought_k - entry_p, 2)
                bep_dist_usd = round(und_p - bep_price, 2)
                bep_dist_pct = round((bep_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                if und_p <= bep_price:
                    bep_status = f"✅ In winstzone (+${bep_price - und_p:.2f} / +{((bep_price - und_p)/bep_price)*100:.1f}% onder BEP van ${bep_price:.2f})"
                else:
                    bep_status = f"⚠️ Nog -${bep_dist_usd:.2f} (-{abs(bep_dist_pct):.1f}%) daling nodig voor BEP op expiratie (${bep_price:.2f})"
                
                t1_stock = round(bep_price - (0.01 * entry_p), 2)
                t5_stock = round(bep_price - (0.05 * entry_p), 2)
                t1_dist_usd = round(und_p - t1_stock, 2)
                t1_dist_pct = round((t1_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                t5_dist_usd = round(und_p - t5_stock, 2)
                t5_dist_pct = round((t5_dist_usd / und_p * 100.0), 1) if und_p > 0 else 0.0
                
                t1_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 1.0 else f"Nog -${t1_dist_usd:.2f} (-{t1_dist_pct:.1f}%) nodig tot ${t1_stock:.2f}"
                t5_status = f"✅ Reeds bereikt ({pnl_pct:+.1f}% winst)" if pnl_pct >= 5.0 else f"Nog -${t5_dist_usd:.2f} (-{t5_dist_pct:.1f}%) nodig tot ${t5_stock:.2f}"

        return {
            'contract_label': contract_label,
            'entry_label': entry_label,
            'mkt_label': mkt_label,
            'underlying_price': und_p,
            'entry_price': entry_p,
            'market_price': mkt_p,
            'pnl_usd': pnl_usd,
            'pnl_pct': pnl_pct,
            'is_in_profit': is_in_profit,
            'bep_price': bep_price,
            'bep_dist_usd': bep_dist_usd,
            'bep_dist_pct': bep_dist_pct,
            'bep_status': bep_status,
            't1_stock': t1_stock,
            't1_dist_usd': t1_dist_usd,
            't1_dist_pct': t1_dist_pct,
            't1_status': t1_status,
            't5_stock': t5_stock,
            't5_dist_usd': t5_dist_usd,
            't5_dist_pct': t5_dist_pct,
            't5_status': t5_status,
            'legs_breakdown': legs_breakdown
        }

    def evaluate_anti_assignment_routine(self, pos, underlying_p, dte, pnl_usd, pnl_pct):
        """
        Anti-Assignment Risico Evaluatieroutine gebaseerd op het 21-pagina document:
        Detecteert de 7 gevarenzones en geeft directe melding, voorstel en actiecode.
        """
        sym = pos.get('symbol', 'N/A')
        strat = pos.get('strategy', 'Spread')
        sold_strike = float(pos.get('sold_strike', 0.0) or 0.0)
        bought_strike = float(pos.get('bought_strike', 0.0) or 0.0)
        right = str(pos.get('right', '')).upper()
        qty = int(pos.get('qty', 1))
        entry_price = float(pos.get('entry_price', 0.0) or 0.0)
        mkt_price = float(pos.get('market_price', 0.0) or 0.0)
        is_credit_strat = strat in ['BullPut', 'BearCall', 'ShortPut', 'ShortCall', 'IronCondor']

        triggers = []
        risk_level = "SAFE"
        consequences = ""
        action_code = "HANDHAVEN"
        action_title = "Handhaven (Geen Aanwijzingsrisico)"
        recommended_action = "Geen actie vereist: positie ligt in de veilige zone."
        execution_type = "HANDHAVEN"

        # 1. Reguliere Aandelenpositie in de Portefeuille (Geen Optie / Geen Aanwijzingsrisico)
        if strat == 'Stock' or right == 'STK':
            is_long = pos.get('is_long', True)
            risk_level = "SAFE"
            triggers.append(f"📦 AANDELENPOSITIE: Account bezit {'+' if is_long else '-'}{qty} {'long' if is_long else 'short'} aandelen {sym}.")
            consequences = f"Account bezit {qty} aandelen {sym} (totale marktwaarde: ${underlying_p * qty:,.2f}, ongerealiseerde P&L: ${pnl_usd:+,.2f} / {pnl_pct:+.1f}%). Er is geen optie-aanwijzingsrisico. Het positierisico wordt bewaakt via technische trendindicatoren en het OmniTrader BarToBar stopniveau."
            action_code = "HANDHAVEN"
            action_title = f"Aandelen {sym} Handhaven ({'Long' if is_long else 'Short'})"
            recommended_action = f"Geen actie vereist. De positie wordt bewaakt via het OmniTrader BarToBar dynamische trailing stop-niveau."
            execution_type = "STOCK_MONITOR"
            return {
                'risk_level': risk_level,
                'triggers': triggers,
                'consequences': consequences,
                'action_code': action_code,
                'action_title': action_title,
                'recommended_action': recommended_action,
                'execution_type': execution_type,
                'extrinsic_val': 0.0,
                'intrinsic_val': 0.0,
                'is_itm': False,
                'is_pin_risk': False,
                'is_between_strikes': False,
                'ex_dividend_risk': False,
                'deadline_status': 'AANDELEN_PORTFOLIO'
            }

        # Bereken intrinsieke en extrinsieke waarde van de geschreven optie
        is_put = 'P' in right or 'Put' in strat
        is_call = 'C' in right or 'Call' in strat
        is_itm = False
        intrinsic_val = 0.0
        extrinsic_val = 0.50 # default veilige aanname

        if sold_strike > 0 and underlying_p > 0:
            if is_put:
                is_itm = underlying_p < sold_strike
                intrinsic_val = max(0.0, sold_strike - underlying_p)
            elif is_call:
                is_itm = underlying_p > sold_strike
                intrinsic_val = max(0.0, underlying_p - sold_strike)

            short_p = float(pos.get('short_leg_price', 0.0) or 0.0)
            if short_p <= 0:
                short_p = max(mkt_price, intrinsic_val + 0.08)
            extrinsic_val = max(0.0, short_p - intrinsic_val)

        # 2. EXTRINSIEKE WAARDE & EARLY ASSIGNMENT EVALUATIE (Pg 19-20)
        if is_itm and is_credit_strat:
            if extrinsic_val < 0.05:
                risk_level = "CRITICAL"
                triggers.append(f"🚨 TIJDWAARDE VERDAMPT: Resterende extrinsieke waarde short leg is slechts ${extrinsic_val:.2f} (< $0.05).")
                consequences = "De tegenpartij verliest vrijwel niets aan tijdswaarde bij uitoefening. Vroegtijdige aanwijzing kan elke nacht plaatsvinden!"
                action_code = "TIJDIG_SLUITEN"
                action_title = "Direct Sluiten als Combo Order (Verdampte Tijdswaarde)"
                recommended_action = "Sluit direct de hele spread als combinatie (BUY combo) om toewijzing voor te zijn. Wacht niet af tot expiratie."
                execution_type = "COMBO_CLOSE"
            elif extrinsic_val < 0.10:
                if risk_level != "CRITICAL": risk_level = "HIGH"
                triggers.append(f"⚠️ GEVARENZONE TIJDWAARDE: Extrinsieke waarde short leg is ${extrinsic_val:.2f} (< $0.10).")
                consequences = "Tijdswaarde is zeer laag. Bij lichte koersschommeling volgt direct uitoefening door tegenpartij."
                action_code = "TIJDIG_SLUITEN"
                action_title = "Direct Sluiten of Doorrollen (+30 DTE)"
                recommended_action = "Sluit de positie of rol de spread door naar de volgende maand (+30 dagen) voor extra credit en hersteltijd."
                execution_type = "COMBO_CLOSE"

        # 3. EX-DIVIDEND RISICO BIJ BEAR CALL SPREADS (Pg 20)
        ex_div_risk = False
        if is_call and is_credit_strat and is_itm:
            div_info = self.get_dividend_info(sym)
            div_amt = div_info.get('div_amount', 0.0)
            if div_amt > 0 and div_amt >= extrinsic_val:
                ex_div_risk = True
                risk_level = "CRITICAL"
                triggers.append(f"🚨 100% EX-DIVIDEND RISICO: Dividendbedrag (${div_amt:.2f}) >= Tijdswaarde (${extrinsic_val:.2f}).")
                consequences = "Vrijwel 100% van de short calls wordt de avond vóór de ex-dividenddatum uitgeoefend omdat de optiehouder het dividend wil incasseren!"
                action_code = "TIJDIG_SLUITEN"
                action_title = "Direct Sluiten vóór 22:00 uur (Ex-Dividend Arbitrage)"
                recommended_action = "Sluit de Bear Call spread direct vóór 22:00 uur op de dag voorafgaand aan de ex-dividenddatum!"
                execution_type = "COMBO_CLOSE"

        # 4. DELTA & DIEP ITM STATUS (Pg 21)
        short_delta = abs(float(pos.get('short_delta', 0.0) or 0.0))
        if short_delta >= 0.80:
            risk_level = "CRITICAL"
            triggers.append(f"🚨 DIEP ITM (Delta = {short_delta:.2f} >= 0.80): Extreem hoog toewijzingsrisico.")
            consequences = "Bij een delta van 0.80+ beweegt de optie vrijwel 1-op-1 met het aandeel mee en is de extrinsieke waarde nagenoeg nihil."
            action_code = "TIJDIG_SLUITEN"
            action_title = "Direct Sluiten (Delta >= 0.80 Stop)"
            recommended_action = "Sluit de spread om te voorkomen dat de positie ongecontroleerd in aandelenlevering resulteert."
            execution_type = "COMBO_CLOSE"

        # 5. STOP-LOSS OP SPREAD-WAARDE (Pg 21: 1.5x tot 2.0x premie)
        rec_prem_usd = abs(entry_price) * 100.0 * qty if entry_price != 0 else 100.0 * qty
        if is_credit_strat and pnl_usd <= -1.5 * rec_prem_usd:
            risk_level = "CRITICAL"
            triggers.append(f"🛑 STOP-LOSS BEREIKT: Verlies bedraagt ${abs(pnl_usd):,.2f} (>= 1.5x ontvangen premie van ${rec_prem_usd:.2f}).")
            consequences = "Wacht bij credit spreads nooit tot de expiratiedag als de trade fout zit. Het risico escaleert exponentieel."
            action_code = "TIJDIG_SLUITEN"
            action_title = "Direct Sluiten op Marktprijs (Vaste 1.5x Premie Stop-Loss)"
            recommended_action = "Sluit de spread direct als combinatieorder om maximaal spreadverlies en leveringsgevaar af te kappen."
            execution_type = "COMBO_CLOSE"

        # 6. PIN RISK & TUSSEN DE STRIKES (Pg 8, 17-18, 20)
        is_between = False
        is_pin = False
        if sold_strike > 0 and bought_strike > 0 and underlying_p > 0:
            if strat == 'BullPut' and bought_strike < underlying_p < sold_strike:
                is_between = True
            elif strat == 'BearCall' and sold_strike < underlying_p < bought_strike:
                is_between = True
            
            dist_to_sold = abs(underlying_p - sold_strike) / sold_strike
            if dist_to_sold <= 0.015:
                is_pin = True

        if is_between and dte <= 5:
            risk_level = "CRITICAL"
            triggers.append(f"⚡ EXTREEM PIN RISK: Koers (${underlying_p:.2f}) staat TUSSEN de strikes ({sold_strike:.2f} / {bought_strike:.2f}).")
            consequences = f"De long optie ({bought_strike:.2f}) vervalt waardeloos ($0,00) en beschermt je NIET meer. De short optie ({sold_strike:.2f}) wordt 100% aangewezen! Resultaat: maandagochtend zit je met 100 aandelen en een gigantisch margintekort."
            action_code = "TIJDIG_SLUITEN"
            action_title = "Direct Sluiten als Combo Order (Pin Risk Noodingreep)"
            recommended_action = "Sluit de volledige spread (koop short terug, verkoop long) direct als combinatieorder vóór de uiterste deadline!"
            execution_type = "COMBO_CLOSE"
        elif is_pin and dte <= 2:
            if risk_level != "CRITICAL": risk_level = "HIGH"
            triggers.append(f"⚠️ PIN RISK ZONE: Koers (${underlying_p:.2f}) ligt binnen 1.5% van de short strike ({sold_strike:.2f}).")
            consequences = "After-hours nieuws en koerssprongen tussen 22:00 en 23:30 uur kunnen alsnog tot onverwachte aanwijzing leiden."
            action_code = "TIJDIG_SLUITEN"
            action_title = "Sluiten vóór 21:30 uur (Pin Risk Vermijden)"
            recommended_action = "Sluit de positie vóór 21:30 uur NL tijd. Die laatste paar euro winst wegen niet op tegen een margin call."
            execution_type = "COMBO_CLOSE"

        # 7. EXSPIRATIEDEADLINE TIJDPAD (Pg 13-15, 20)
        import datetime
        now_dt = datetime.datetime.now()
        weekday = now_dt.weekday() # 3=Thursday, 4=Friday
        hour = now_dt.hour

        deadline_status = "REGULIER"
        # Donderdagavond routine
        if (weekday == 3 or dte == 1) and pnl_pct >= 80.0:
            if risk_level == "SAFE": risk_level = "HIGH"
            triggers.append(f"💰 DONDERDAG SWEET SPOT: Spread staat op {pnl_pct:.0f}% winst (>= 80%).")
            consequences = "De Time Decay (Theta) heeft 90% van zijn werk gedaan. Wachten op de laatste 10% op vrijdag is 'picking up pennies in front of a steamroller'."
            action_code = "WINST_BORGEN"
            action_title = "Winst Borgen & Sluiten (Donderdagavond Routine)"
            recommended_action = "Sluit de Combo vanavond en ga met een gerust hart het weekend in."
            execution_type = "COMBO_CLOSE"
            deadline_status = "DONDERDAG_WINST_80"

        # Vrijdag expiratiedag routine
        if weekday == 4 or dte == 0:
            if hour < 18:
                deadline_status = "VRIJDAG_OPENING"
                risk_level = "HIGH" if risk_level == "SAFE" else risk_level
                triggers.append("📅 VRIJDAG EXSPIRATIEDAG (DTE = 0): Optie expireert vandaag! Sluit tijdig vóór 18:00 CET om margin-liquidatie of weekend-aanwijzing te voorkomen.")
                consequences = "Op expiratiedag neemt pin-risk en assignment risico exponentieel toe. IBKR scant vanaf 18:00 uur accounts op margin-tekorten."
                action_code = "TIJDIG_SLUITEN"
                action_title = "Direct Sluiten op Expiratiedag (DTE = 0 Protocol)"
                recommended_action = "Plaats een Combo Limit Order om beide poten tegelijk te sluiten en het weekend risicovrij in te gaan."
                execution_type = "COMBO_CLOSE"
            elif 18 <= hour < 20:
                deadline_status = "VRIJDAG_DEADLINE_18_20"
                risk_level = "CRITICAL"
                triggers.append("🚨 VRIJDAG DEADLINE (18:00 - 20:00 NL tijd): IBKR risico-algoritme scant actieve accounts!")
                consequences = "Tussen 18:00 en 21:00 liquideert IBKR accounts met te weinig margin geforceerd tegen slechte marktprijzen. Neem zelf de regie!"
                action_code = "TIJDIG_SLUITEN"
                action_title = "Direct Zelf Sluiten Vóór 20:00 uur (IBKR Liquidatie Voorkomen)"
                recommended_action = "Plaats direct een Combo Order om de trade vóór 20:00 uur definitief te verlaten."
                execution_type = "COMBO_CLOSE"
            elif hour >= 20:
                deadline_status = "VRIJDAG_AFTER_HOURS_RISK"
                risk_level = "CRITICAL"
                triggers.append("🔴 DEADLINE VOORBIJ (> 20:00 NL tijd): Na 20:00 droogt liquiditeit op en geldt after-hours aanwijzingsrisico tot 23:30 uur.")
                consequences = "Kopers van jouw short optie hebben tot 23:30 uur de tijd om alsnog aan te wijzen na beursnieuws."
                action_code = "TIJDIG_SLUITEN"
                action_title = "Noodsluiting op Marktprijs (Direct Flat Gaan)"
                recommended_action = "Sluit de positie onmiddellijk op marktprijs om niet met aandelen het weekend in te gaan."
                execution_type = "COMBO_CLOSE"

        return {
            'risk_level': risk_level,
            'triggers': triggers,
            'consequences': consequences,
            'action_code': action_code,
            'action_title': action_title,
            'recommended_action': recommended_action,
            'execution_type': execution_type,
            'extrinsic_val': extrinsic_val,
            'intrinsic_val': intrinsic_val,
            'is_itm': is_itm,
            'is_pin_risk': is_pin,
            'is_between_strikes': is_between,
            'ex_dividend_risk': ex_div_risk,
            'deadline_status': deadline_status
        }

    @staticmethod
    def calculate_portfolio_exposure(positions, net_liquidation=0.0):
        """
        Berekent de totale potentiële aankoopverplichting (Assignment Obligation)
        en hefboom (Leverage Ratio) over alle openstaande short opties in het portfolio.
        Voorkomt dat traders overleveraged raken (zoals 12 ipv 6 contracten = $261k verplichting).
        """
        if not positions:
            positions = []
        if net_liquidation is None:
            net_liquidation = 0.0

        total_put_obligation = 0.0
        total_call_obligation = 0.0
        total_max_loss = 0.0
        breakdown = []

        for p in positions:
            sym = p.get('symbol', '')
            strat = p.get('strategy', '')
            qty = int(p.get('qty', 1))
            sold_strike = float(p.get('sold_strike', 0.0))
            bought_strike = float(p.get('bought_strike', 0.0))
            right = str(p.get('right', '')).upper()
            
            put_obl = 0.0
            call_obl = 0.0
            max_loss = 0.0

            # Short Put obligatie: Aankoopverplichting 100 aandelen @ sold_strike per contract
            if ('P' in right or 'PUT' in strat.upper() or 'CONDOR' in strat.upper()) and sold_strike > 0:
                put_obl = sold_strike * 100.0 * qty
                total_put_obligation += put_obl

            # Short Call obligatie: Leveringsverplichting 100 aandelen @ sold_strike
            if ('C' in right or 'CALL' in strat.upper() or 'CONDOR' in strat.upper()) and sold_strike > 0:
                call_obl = sold_strike * 100.0 * qty
                total_call_obligation += call_obl

            # Max verlies op spreads
            if sold_strike > 0 and bought_strike > 0:
                spread_width = abs(sold_strike - bought_strike)
                prem = float(p.get('entry_price', 0.0))
                max_loss = max(0.0, (spread_width - prem)) * 100.0 * qty
                total_max_loss += max_loss

            pct_of_account = (put_obl / net_liquidation * 100.0) if net_liquidation > 0 else 0.0

            breakdown.append({
                'symbol': sym,
                'strategy': strat,
                'qty': qty,
                'sold_strike': sold_strike,
                'bought_strike': bought_strike,
                'put_obligation': put_obl,
                'call_obligation': call_obl,
                'max_loss': max_loss,
                'pct_of_account': pct_of_account
            })

        leverage_ratio = (total_put_obligation / net_liquidation * 100.0) if net_liquidation > 0 else 0.0

        if leverage_ratio <= 100.0:
            status = "VEILIG"
            status_color = "green"
            status_icon = "🟢"
            message = "Volledig gedekt: Totale put-verplichting past binnen uw accountwaarde."
        elif leverage_ratio <= 200.0:
            status = "AANDACHT"
            status_color = "orange"
            status_icon = "🟡"
            message = f"Verhoogd margin-gebruik ({leverage_ratio:.1f}%): Vraagt actieve monitoring bij scherpe daling."
        else:
            status = "GEVAARLIJK"
            status_color = "red"
            status_icon = "🔴"
            message = f"OVERLEVERAGE ALARM ({leverage_ratio:.1f}%): Uw aankoopverplichting overstijgt het dubbele van uw accountwaarde! Kans op margin call of automatische TWS liquidatie."

        return {
            'total_put_obligation': total_put_obligation,
            'total_call_obligation': total_call_obligation,
            'total_max_loss': total_max_loss,
            'net_liquidation': net_liquidation,
            'leverage_ratio': leverage_ratio,
            'status': status,
            'status_color': status_color,
            'status_icon': status_icon,
            'message': message,
            'breakdown': breakdown
        }

    @staticmethod
    def calculate_keltner_channels(df, period=20, mult=2.0, atr_period=10):
        if df is None or df.empty or 'close' not in df.columns or len(df) < 5:
            return pd.DataFrame()
        try:
            df_copy = df.copy()
            df_copy['prev_close'] = df_copy['close'].shift(1)
            tr1 = df_copy['high'] - df_copy['low']
            tr2 = (df_copy['high'] - df_copy['prev_close']).abs()
            tr3 = (df_copy['low'] - df_copy['prev_close']).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(window=min(atr_period, len(df_copy))).mean().fillna(0)
            
            middle = df_copy['close'].ewm(span=min(period, len(df_copy)), adjust=False).mean()
            upper = middle + mult * atr
            lower = middle - mult * atr
            return pd.DataFrame({'middle': middle, 'upper': upper, 'lower': lower, 'atr': atr}, index=df.index)
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def calculate_coral_trend(df, period=15):
        if df is None or df.empty or 'close' not in df.columns or len(df) < 5:
            return pd.Series(dtype=float)
        try:
            c = df['close']
            p = min(period, len(df))
            i1 = c.ewm(span=p, adjust=False).mean()
            i2 = i1.ewm(span=p, adjust=False).mean()
            return 2 * i1 - i2
        except Exception:
            return pd.Series(dtype=float)

    @staticmethod
    def calculate_cci(df, period=20):
        if df is None or df.empty or 'close' not in df.columns or len(df) < 5:
            return pd.Series(dtype=float)
        try:
            p = min(period, len(df))
            tp = (df['high'] + df['low'] + df['close']) / 3.0
            sma_tp = tp.rolling(window=p).mean()
            mad = tp.rolling(window=p).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
            cci = (tp - sma_tp) / (0.015 * mad.replace(0, np.nan))
            return cci.fillna(0)
        except Exception:
            return pd.Series(dtype=float)

    @staticmethod
    def calculate_macd(df, fast=12, slow=26, signal_span=9):
        if df is None or df.empty or 'close' not in df.columns or len(df) < 5:
            return pd.DataFrame()
        try:
            ema_fast = df['close'].ewm(span=min(fast, len(df)), adjust=False).mean()
            ema_slow = df['close'].ewm(span=min(slow, len(df)), adjust=False).mean()
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=min(signal_span, len(df)), adjust=False).mean()
            hist = macd_line - signal_line
            return pd.DataFrame({'macd': macd_line, 'signal': signal_line, 'hist': hist}, index=df.index)
        except Exception:
            return pd.DataFrame()

    def evaluate_position_health(self, pos, hist_df=None):
        """
        Evaluates a single portfolio position (Spread or Single Leg Option)
        and returns a detailed health assessment dict.
        """
        sym = pos.get('symbol', 'N/A')
        strat = pos.get('strategy', 'Spread')
        expiry = str(pos.get('expiry', 'N/A'))
        dte = pos.get('dte', 30)
        pnl_usd = pos.get('unrealized_pnl', 0.0)
        pnl_pct = pos.get('pnl_pct', 0.0)
        mkt_price = pos.get('market_price', 0.0)
        entry_price = pos.get('entry_price', 0.0)
        sold_strike = pos.get('sold_strike', 0.0)
        bought_strike = pos.get('bought_strike', 0.0)
        underlying_p = pos.get('underlying_price', 0.0)

        # Technical Indicators on hist_df
        ema8_val, ema20_val = 0.0, 0.0
        ema_bullish = False
        keltner_lower, keltner_upper = 0.0, 0.0
        keltner_break_down = False
        coral_red = False
        stoch_k_val = 50.0
        stoch_oversold = False
        cci_val = 0.0
        cci_oversold = False

        if hist_df is not None and not hist_df.empty and len(hist_df) >= 5:
            # 1. EMA 8 vs 20
            ema8 = hist_df['close'].ewm(span=min(8, len(hist_df)), adjust=False).mean()
            ema20 = hist_df['close'].ewm(span=min(20, len(hist_df)), adjust=False).mean()
            if len(ema8) > 0 and len(ema20) > 0:
                ema8_val, ema20_val = float(ema8.iloc[-1]), float(ema20.iloc[-1])
                ema_bullish = ema8_val >= ema20_val

            # 2. Keltner
            kelt = self.calculate_keltner_channels(hist_df)
            if not kelt.empty:
                keltner_lower = float(kelt['lower'].iloc[-1])
                keltner_upper = float(kelt['upper'].iloc[-1])
                if underlying_p > 0 and keltner_lower > 0:
                    keltner_break_down = underlying_p < keltner_lower

            # 3. Coral
            coral = self.calculate_coral_trend(hist_df)
            if len(coral) >= 2:
                coral_red = float(coral.iloc[-1]) < float(coral.iloc[-2])

            # 4. StochRSI
            stoch = SpreadScanner(None).calculate_stoch_rsi(hist_df)
            if not stoch.empty and 'k' in stoch.columns:
                stoch_k_val = float(stoch['k'].iloc[-1])
                stoch_oversold = stoch_k_val < 25

            # 5. CCI
            cci = self.calculate_cci(hist_df)
            if len(cci) > 0:
                cci_val = float(cci.iloc[-1])
                cci_oversold = cci_val < -100

            # 6. MACD
            macd_df = self.calculate_macd(hist_df)

        # Build decision logic
        is_long_stock = (strat == 'Stock' and pos.get('is_long', True))
        is_short_stock = (strat == 'Stock' and not pos.get('is_long', True))
        is_bullish_strat = is_long_stock or strat in ['BullPut', 'BullCall', 'LongCall', 'ShortPut']
        is_bearish_strat = is_short_stock or strat in ['BearCall', 'BearPut', 'LongPut', 'ShortCall']

        # Calculate OmniTrader BarToBar Advanced Exit Plan (Template STPB2BADV2CT)
        omni_ref_price = entry_price if (entry_price > 0 and abs(entry_price - underlying_p) / underlying_p < 0.25) else underlying_p
        omni_res = self.calculate_omnitrader_b2b_exit_plan(
            hist_df, 
            entry_price=omni_ref_price, 
            signal_type="Long" if is_bullish_strat else "Short",
            init_mult=7.0,
            atr_periods=7,
            p_factor=0.4,
            pct_close_up=0.25,
            pct_close_down=0.25,
            use_adr=True,
            marketstate_active=True
        )

        sold_strike_threatened = False
        if sold_strike > 0 and underlying_p > 0:
            if is_bullish_strat and underlying_p <= sold_strike * 1.015:
                sold_strike_threatened = True
            elif is_bearish_strat and underlying_p >= sold_strike * 0.985:
                sold_strike_threatened = True

        action_code = "HANDHAVEN"
        action_title = "Handhaven (Positie Gezond)"
        old_to_new = f"Lopende positie ({expiry})" + " → " + "Handhaven (Geen actie vereist)"
        if strat == 'Stock':
            reasoning = f"Aandelen {sym} liggen in de veilige zone. Technische trend is stabiel (OmniStop: ${omni_res['stop_price']:.2f})."
            result_desc = "OmniTrader BarToBar dynamische stop bewaakt het kapitaal."
        else:
            reasoning = f"Positie ligt in veilige zone (DTE={dte}d). Trend is stabiel."
            result_desc = "Laat het tijdswaardeverval (Theta) in jouw voordeel werken."
        urgency = "LOW"

        # Evalueer eerst Anti-Assignment Risico conform het 21-pagina document
        anti_assign = self.evaluate_anti_assignment_routine(pos, underlying_p, dte, pnl_usd, pnl_pct)

        if anti_assign['risk_level'] in ['CRITICAL', 'HIGH']:
            urgency = anti_assign['risk_level']
            action_code = anti_assign['action_code']
            action_title = anti_assign['action_title']
            old_to_new = f"Lopende positie ({strat} {pos.get('strikes_str', '')})" + " → " + action_title
            reasoning = " | ".join(anti_assign['triggers']) if anti_assign['triggers'] else reasoning
            result_desc = anti_assign['consequences'] if anti_assign['consequences'] else result_desc

        # Check OmniTrader Exit Trigger condition
        elif omni_res['exit_signal']:
            if strat == 'Stock':
                action_code = "VERKOOP_STOPLOSS" if is_long_stock else "COVER_STOPLOSS"
                action_title = f"Aandelen {sym} Sluiten (OmniTrader BarToBar Stop)"
                old_to_new = f"Aandelen {sym} Handhaven" + " → " + f"Aandelen Sluiten op Marktprijs (Stop geraakt op ${omni_res['stop_price']:.2f})"
                reasoning = f"OmniTrader BarToBar stopniveau van ${omni_res['stop_price']:.2f} is doorbroken met Coral Trend = {omni_res['marketstate']}."
                result_desc = f"Beperk verlies of borg winst op de {pos.get('qty', 100)} aandelen {sym}."
                urgency = "HIGH"
            else:
                action_code = "TIJDIG_SLUITEN"
                action_title = "Direct Sluiten (OmniTrader BarToBar Stop)"
                old_to_new = "Openstaande Positie Handhaven" + " → " + f"Direct Sluiten op Marktprijs (OmniTrader Stop op ${omni_res['stop_price']:.2f})"
                reasoning = f"OmniTrader BarToBar stopniveau van ${omni_res['stop_price']:.2f} is doorbroken met Coral Trend = {omni_res['marketstate']}."
                result_desc = "Volgt de OmniTrader BarToBar dynamische risicobewaking."
                urgency = "CRITICAL"

        # Check standard conditions
        elif (strat != 'Stock' and (pnl_pct >= 60.0 or (pnl_pct >= 40.0 and dte <= 7))) or (strat == 'Stock' and pnl_pct >= 40.0):
            action_code = "WINST_BORGEN"
            action_title = "Winst Borgen & Positie Sluiten"
            old_to_new = f"Lopende Winstpositie ({pnl_pct:.0f}% winst)" + " → " + "Winst Borgen & Positie Sluiten op Marktprijs"
            reasoning = f"Ruim {pnl_pct:.0f}% winst behaald. Borg het behaalde rendement."
            result_desc = f"Borg de winst van ${pnl_usd:.2f} direct en maak kapitaal vrij."
            urgency = "HIGH"

        elif is_bullish_strat and (coral_red and not ema_bullish and (keltner_break_down or cci_val < -150 or pnl_pct < -35.0)):
            action_code = "TIJDIG_SLUITEN"
            action_title = "Direct Sluiten (Strikte Stop-Loss)"
            old_to_new = "Openstaande Positie Handhaven" + " → " + "Direct Sluiten op Marktprijs (Stop-Loss)"
            reasoning = f"Sterke neerwaartse trendbreuk (Coral Rood, EMA8 < 20, CCI={cci_val:.0f}). Risk op max verlies."
            result_desc = "Beperkt verlies en voorkomt 100% verlies van inleg/marge op expiratie."
            urgency = "CRITICAL"

        elif is_bearish_strat and (not coral_red and ema_bullish and (cci_val > 150 or pnl_pct < -35.0)):
            action_code = "TIJDIG_SLUITEN"
            action_title = "Direct Sluiten (Strikte Stop-Loss)"
            old_to_new = "Openstaande Positie Handhaven" + " → " + "Direct Sluiten op Marktprijs (Stop-Loss)"
            reasoning = f"Sterke opwaartse trendbreuk tegen Bear-positie in (EMA8 > 20, CCI={cci_val:.0f})."
            result_desc = "Beperkt verlies en voorkomt max verlies bij verdere uitbraak omhoog."
            urgency = "CRITICAL"

        elif strat != 'Stock' and (dte <= 21 or sold_strike_threatened or pnl_pct < -15.0):
            if stoch_oversold or cci_oversold or dte <= 21:
                action_code = "DOORROLLEN_CREDIT"
                action_title = "Doorrollen naar Volgende Maand (+30 DTE voor Credit)"
                old_to_new = f"Expiratie {expiry} ({dte} DTE)" + " → " + f"Doorrollen naar volgende maand (+30 DTE) voor Net Credit"
                reasoning = f"Positie onder druk (DTE={dte}d, StochRSI={stoch_k_val:.0f}). Oversold dip signaleert herstelkans."
                result_desc = "Wint 30 dagen extra hersteltijd en verlaagt de Break-Even uitoefenprijs door extra premie."
                urgency = "MEDIUM"
            else:
                action_code = "OMZETTEN_IRON_CONDOR"
                action_title = "Omzetten naar Iron Condor (Verkoop Onbedreigde Zijde)"
                old_to_new = f"{strat}" + " → " + "Iron Condor (Verkoop tegendraadse spread voor extra credit)"
                reasoning = "Zijwaartse markt. Verkoop de onbedreigde bovenzijde/onderzijde voor extra premie."
                result_desc = "Ontvang extra premie zonder het maximale risico op de positie te verhogen."
                urgency = "MEDIUM"

        elif strat == 'Stock' and pnl_pct < -15.0:
            action_code = "VERKOOP_STOPLOSS" if is_long_stock else "COVER_STOPLOSS"
            action_title = f"Aandelen {sym} Sluiten (Verlies > 15%)"
            old_to_new = f"Aandelen {sym} Handhaven" + " → " + "Aandelen Sluiten op Marktprijs (Stop-Loss)"
            reasoning = f"Verlies op aandelenpositie ({pnl_pct:.1f}%) overschrijdt de risicodrempel van -15%."
            result_desc = "Beperk verder koersverlies conform strikt risicobeheer."
            urgency = "HIGH"

        if strat == 'Stock':
            alternatives = [
                f"[Aanbevolen] {action_title}",
                "Verkoop Aandelen via Limit Order",
                "Verkoop Aandelen via Market Order",
                "Handhaven (Geen Actie)"
            ]
        elif anti_assign['execution_type'] == 'STOCK_CLOSE':
            alternatives = [
                f"[Aanbevolen] {action_title}",
                "Verkoop Aandelen via Market Order",
                "Handhaven (Geen Actie)"
            ]
        else:
            alternatives = [
                f"[Aanbevolen] {action_title}",
                "Direct Sluiten als Combo Order (Mid-price / Market)",
                "Doorrollen naar Volgende Maand (+30 DTE voor Credit)",
                "Omzetten naar Iron Condor (Verkoop tegendraadse zijde)",
                "Winst Borgen & Positie Sluiten",
                "Handhaven (Geen Actie)"
            ]

        return {
            'symbol': sym,
            'strategy': strat,
            'expiry': expiry,
            'dte': dte,
            'pnl_usd': pnl_usd,
            'pnl_pct': pnl_pct,
            'market_price': mkt_price,
            'entry_price': entry_price,
            'sold_strike': sold_strike,
            'bought_strike': bought_strike,
            'underlying_price': underlying_p,
            'action_code': action_code,
            'action_title': action_title,
            'old_to_new': old_to_new,
            'reasoning': reasoning,
            'result_desc': result_desc,
            'urgency': urgency,
            'alternatives': alternatives,
            'anti_assignment': anti_assign,
            'omnitrader_b2b': omni_res,
            'financials': self.calculate_position_financials(pos),
            'technical_summary': f"OmniStop=${omni_res['stop_price']:.2f}, Coral={'Bull' if omni_res['marketstate']==1 else 'Bear'}, EMA8/20={'Bull' if ema_bullish else 'Bear'}, StochRSI={stoch_k_val:.1f}"
        }

    def calculate_omnitrader_b2b_exit_plan(self, df_hist, entry_price, signal_type="Long", init_mult=7.0, atr_periods=7, p_factor=0.4, pct_close_up=0.25, pct_close_down=0.25, use_adr=True, marketstate_active=True, trade_bars_back=30):
        """
        OmniTrader BarToBarAdvanced Versie2 gecombineerd met indCORALtrend.
        Berekent de dynamische trapsgewijze stop-drempel (Drempel / ExitLevel)
        vanaf het instapmoment (trade_bars_back) van de actieve trade.
        """
        if df_hist is None or df_hist.empty or len(df_hist) < 3:
            return {
                'stop_price': round(entry_price * 0.985, 2) if signal_type == "Long" else round(entry_price * 1.015, 2),
                'exit_signal': False,
                'marketstate': 1,
                'drempel_history': [],
                'marketstate_history': [],
                'latest_close': entry_price
            }
        
        df = df_hist.copy().reset_index(drop=True)
        df.columns = [c.lower() for c in df.columns]
        
        # Calculate Volatility X (ATR vs ADR/WMA)
        if use_adr:
            hl_diff = df['high'] - df['low']
            df['x_vol'] = hl_diff.ewm(span=atr_periods, adjust=False).mean()
        else:
            tr1 = df['high'] - df['low']
            tr2 = (df['high'] - df['close'].shift(1)).abs()
            tr3 = (df['low'] - df['close'].shift(1)).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df['x_vol'] = tr.rolling(window=min(atr_periods, len(df)), min_periods=1).mean()

        # Calculate Coral Trend for MarketState
        coral = self.calculate_coral_trend(df)
        
        drempel_list = [None] * len(df)
        marketstate_list = [1] * len(df)
        
        # Bepaal start_bar van de actieve trade (SignalStartBar in OmniTrader)
        tb_back = max(5, min(len(df) - 1, trade_bars_back))
        start_bar = len(df) - tb_back

        drempel = 0.0
        for i in range(len(df)):
            # Marketstate (Coral Trend Check) voor alle bars
            mstate = 1
            if marketstate_active and len(coral) > i:
                if i > 0 and coral.iloc[i] > coral.iloc[i-1]:
                    mstate = 1
                else:
                    mstate = 0
            marketstate_list[i] = mstate

            if i < start_bar:
                continue

            c_curr = float(df['close'].iloc[i])
            c_prev = float(df['close'].iloc[i-1]) if i > 0 else c_curr
            low_curr = float(df['low'].iloc[i])
            vol = float(df['x_vol'].iloc[i])

            # 1. Initialiseer startwaarde van de stop op start_bar (Trade Entry Bar)
            if i == start_bar or drempel <= 0:
                ref_p = entry_price if (entry_price > 0 and abs(entry_price - c_curr)/c_curr < 0.3) else c_prev
                if signal_type == "Long":
                    drempel = ref_p - (init_mult * vol)
                else:
                    drempel = ref_p + (init_mult * vol)
            else:
                # 2. Bar-by-bar Trailing updates tijdens de trade
                if signal_type == "Long":
                    if c_curr >= c_prev:
                        drempel += ((c_curr - c_prev) * p_factor) + (pct_close_up / 100.0 * c_curr)
                    else:
                        drempel += (pct_close_down / 100.0 * c_curr)
                else: # Short
                    if c_curr >= c_prev:
                        drempel -= ((c_curr - c_prev) * p_factor) - (pct_close_up / 100.0 * c_curr)
                    else:
                        drempel -= (pct_close_down / 100.0 * c_curr)

            # 3. Low 3-times above Entry Price: Stop increased faster to reduce risk
            if i >= start_bar + 2 and entry_price > 0:
                low1 = float(df['low'].iloc[i-1])
                low2 = float(df['low'].iloc[i-2])
                if low_curr > entry_price and low1 > entry_price and low2 > entry_price:
                    alt_drempel = min(low_curr, low1, low2)
                    if alt_drempel > drempel:
                        drempel = max(entry_price * 0.985, drempel)

            drempel_list[i] = round(drempel, 2)

        current_drempel = drempel_list[-1]
        prev_drempel_val = drempel_list[-2] if len(drempel_list) >= 2 else current_drempel
        curr_close = float(df['close'].iloc[-1])
        curr_high = float(df['high'].iloc[-1])
        curr_mstate = marketstate_list[-1]

        exit_triggered = False
        if signal_type == "Long":
            if curr_close < prev_drempel_val and prev_drempel_val > 0 and curr_mstate == 0:
                exit_triggered = True
        else: # Short
            if curr_high > prev_drempel_val and prev_drempel_val > 0:
                exit_triggered = True

        display_stop = current_drempel
        if not exit_triggered:
            if signal_type == "Long" and current_drempel >= curr_close:
                display_stop = round(min(current_drempel, curr_close * 0.95), 2)
            elif signal_type == "Short" and current_drempel <= curr_close:
                display_stop = round(max(current_drempel, curr_close * 1.05), 2)

        return {
            'stop_price': display_stop,
            'drempel_raw': current_drempel,
            'exit_signal': exit_triggered,
            'marketstate': curr_mstate,
            'drempel_history': drempel_list,
            'marketstate_history': marketstate_list,
            'latest_close': curr_close
        }

