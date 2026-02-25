import pandas as pd
import numpy as np
import math
from py_vollib.black_scholes.greeks.analytical import delta, gamma, vega, theta
from py_vollib.black_scholes import black_scholes

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
        if T <= 0: return max(0.0, S - K if right.lower().startswith('c') else K - S)
        if right.lower().startswith('p'):
            # Put-Call Symmetry for Put
            return BjerksundStensland2002.price_american_option('c', K, S, T, q, r, sigma)
        
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
        return c1 + c2 - c3 + c4 - c5 + c6 - c7

    @staticmethod
    def black_scholes_call(S, K, T, r, q, sigma):
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return S * math.exp(-q * T) * BjerksundStensland2002.norm_cdf(d1) - K * math.exp(-r * T) * BjerksundStensland2002.norm_cdf(d2)

class SpreadScanner:
    def __init__(self, ib_client):
        self.ib_client = ib_client
        self.log_func = None

    def calculate_ema(self, df, span):
        """Calculates Exponential Moving Average."""
        return df['close'].ewm(span=span, adjust=False).mean()

    def filter_symbols_by_ema(self, symbols_data, ema_spans, direction='bull', ema_crossover=False):
        """
        Filters symbols based on EMA trend and optionally crossovers.
        direction: 'bull' (Price > EMA) or 'bear' (Price < EMA)
        ema_crossover: If True, checks if EMA 8 > EMA 50.
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
        strike_range_pct = params.get('strike_range_pct', 0.30)
        # Limit between 0.05 and 0.50 for safety
        strike_range_pct = max(0.05, min(0.50, strike_range_pct))
        
        lower_bound = component_price * (1 - strike_range_pct)
        upper_bound = component_price * (1 + strike_range_pct)
        
        valid_expirations = sorted(list(set([exp for chain in chains for exp in chain.expirations])))
        
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
            
            # 1. Handle Long Strategies
            if strategy in ['LongCall', 'LongPut']:
                right = 'C' if strategy == 'LongCall' else 'P'
                for s in strikes:
                    if s < lower_bound or s > upper_bound:
                        skips['strike_range'] += 1
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
                    if strike_p_sell in strikes and strike_p_sell <= upper_bound:
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
                width = params.get('width', 5)
                for i, long_strike in enumerate(strikes):
                    if long_strike < lower_bound or long_strike > upper_bound:
                        skips['strike_range'] += 1
                        continue
                        
                    # Target Short Strike
                    if 'Bull' in strategy:
                        target_short_strike = long_strike + width 
                    else:
                        target_short_strike = long_strike - width
                    
                    if target_short_strike not in strikes: continue
                    
                    if target_short_strike < lower_bound or target_short_strike > upper_bound:
                        skips['strike_range'] += 1
                        continue
                    
                    short_strike = target_short_strike
                    strike_buy = long_strike
                    strike_sell = short_strike
                    right = 'C' if 'Call' in strategy else 'P'
                    
                    spreads.append({
                        'symbol': params.get('symbol', ''),
                        'strategy': strategy,
                        'expiry': expiration,
                        'dte': dte,
                        'strike_buy': strike_buy,
                        'strike_sell': strike_sell,
                        'right': right,
                        'width': abs(strike_buy - strike_sell),
                        'iv': params.get('iv', 0.0)
                    })
                continue
        
        # Log Summary if 0 results
        if not spreads and log_func:
            summary = []
            if skips['dte'] > 0: summary.append(f"DTE ({skips['dte']})")
            if skips['strike'] > 0: summary.append(f"Strikes ({skips['strike']})")
            if skips['strategy'] > 0: summary.append(f"OTM/ITM ({skips['strategy']})")
            
            if summary:
                log_func(f"   ⚠️ 0 {strategy} kandidaten. Skips door: {', '.join(summary)}")
              
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
        
        if chain_data.empty or 'oi' not in chain_data.columns:
            return result
            
        strikes = sorted(chain_data['strike'].unique())
        
        # 1. Max Pain Calculation
        pain_values = {}
        for price_point in strikes:
            total_pain = 0
            
            # Pain for Calls (if price > strike)
            calls = chain_data[chain_data['right'] == 'C']
            calls = calls[calls['oi'] > 0]
            itm_calls = calls[calls['strike'] < price_point]
            if not itm_calls.empty:
                total_pain += ((price_point - itm_calls['strike']) * itm_calls['oi']).sum()
            
            # Pain for Puts (if price < strike)
            puts = chain_data[chain_data['right'] == 'P']
            puts = puts[puts['oi'] > 0]
            itm_puts = puts[puts['strike'] > price_point]
            if not itm_puts.empty:
                total_pain += ((itm_puts['strike'] - price_point) * itm_puts['oi']).sum()
            
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
        if not calls.empty and calls['oi'].max() > 0:
            result['call_wall'] = calls.loc[calls['oi'].idxmax()]['strike']
            
        # 3. Put Wall (Max Put OI)
        puts = chain_data[chain_data['right'] == 'P']
        if not puts.empty and puts['oi'].max() > 0:
            result['put_wall'] = puts.loc[puts['oi'].idxmax()]['strike']
            
        # 4. GEX Wall (Max Gamma * OI) -- simplified proxy for GEX
        # Total GEX per strike = (Gamma * OI * Spot). Spot is constant, so prioritize Gamma*OI.
        # Sum absolute gamma? Calls + Puts? Usually net gamma exposure matters.
        # For a "Magnet", absolute gamma is often used (high liquidity/hedging activity).
        if 'gamma' in chain_data.columns:
             # Calculate GEX proxy per row
             # Use absolute gamma * OI
             chain_data['gex_proxy'] = chain_data['gamma'].abs() * chain_data['oi']
             
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
        if 'gamma' not in chain_data.columns or 'oi' not in chain_data.columns:
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
            
            c_gex = (cg['gamma'] * cg['oi']).sum() if not cg.empty else 0
            p_gex = (pg['gamma'] * pg['oi']).sum() if not pg.empty else 0
            
            net_gamma_by_strike[s] = c_gex - p_gex
            
        # Find where it crosses zero (closest strike)
        if not net_gamma_by_strike: return 0.0
        
        # Simple linear approximation or just the closest strike to zero
        flip_strike = min(net_gamma_by_strike, key=lambda k: abs(net_gamma_by_strike[k]))
        return flip_strike

    def calculate_metrics(self, spreads_df, ib_client, symbol, underlying_price=None, chain_data=None, underlying_iv=0.0, hist_iv_df=None, log_func=None):
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
                model_p = row.get('model_price', 0.0)
                
                # Default to model price if available (great for weekends)
                price = model_p
                
                # Robust price selection: prioritize Midpoint (Bid+Ask)/2 if both exist
                if bid > 0 and ask > 0:
                    price = (bid + ask) / 2
                elif last > 0:
                    price = last
                elif close > 0:
                    price = close
                # Final check: if still zero, use local Bjerksund-Stensland fallback
                if float(price) == 0 and float(underlying_price) > 0:
                    try:
                        iv = row.get('iv', 0.2)
                        if iv == 0: iv = 0.2
                        
                        # Use accurate DTE from the spread data
                        dte_val = spreads_df['dte'].iloc[0] if not spreads_df.empty else 30
                        t_years = max(0.001, float(dte_val)) / 365.0 
                        q = 0.015 # Estimate div yield
                        price = BjerksundStensland2002.price_american_option(row['right'], underlying_price, float(row['strike']), t_years, 0.04, q, iv)
                        price = max(0.01, float(price))
                    except:
                        price = 0.0

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
                lookup[(float(row['strike']), r_norm)] = (float(price), greeks)
        
        # Helper for vectorizable lookup
        def get_data(strike, right):
            # Normalize right to C/P
            r_norm = 'C' if right.upper().startswith('C') else 'P'
            res = lookup.get((float(strike), r_norm))
            if res: return res, True # Found in real data
            
            # [PHANTOM FIX] Only fallback if user explicitly allows it (hidden for now) or for debugging.
            # For the scanner, we ONLY want real TWS strikes.
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
        deltas_buy = np.zeros(n)
        deltas_sell = np.zeros(n)
        
        # Tracking valid rows
        valid_mask = np.ones(n, dtype=bool)

        it = spreads_df.itertuples(index=True)
        for i, row in enumerate(it):
            # Check strategy type
            if row.right == 'STR':
                res_p = get_data(row.strike_p_buy, 'P')
                res_c = get_data(row.strike_c_buy, 'C')
                
                if not res_p[1] or not res_c[1]:
                    valid_mask[i] = False
                    continue
                    
                pb, gb = res_p[0]
                cb, cb_greeks = res_c[0]
                
                prices_buy[i] = pb + cb
                prices_sell[i] = 0.0
                net_delta[i] = gb['delta'] + cb_greeks['delta']
                net_gamma[i] = gb['gamma'] + cb_greeks['gamma']
                net_theta[i] = gb['theta'] + cb_greeks['theta']
                net_vega[i] = gb['vega'] + cb_greeks['vega']
                deltas_buy[i] = gb['delta']
            elif row.right == 'IC':
                res_pb = get_data(row.strike_p_buy, 'P')
                res_ps = get_data(row.strike_p_sell, 'P')
                res_cs = get_data(row.strike_c_sell, 'C')
                res_cb = get_data(row.strike_c_buy, 'C')
                
                if not res_pb[1] or not res_ps[1] or not res_cs[1] or not res_cb[1]:
                    valid_mask[i] = False
                    continue
                
                # Safer Unpacking
                res_pb_dat, found_pb = res_pb
                res_ps_dat, found_ps = res_ps
                res_cs_dat, found_cs = res_cs
                res_cb_dat, found_cb = res_cb
                
                pb, pg_buy = res_pb_dat
                ps, pg_sell = res_ps_dat
                cs, cg_sell = res_cs_dat
                cb, cg_buy = res_cb_dat
                prices_buy[i] = pb + cb
                prices_sell[i] = ps + cs
                net_delta[i] = pg_buy['delta'] - pg_sell['delta'] + cg_buy['delta'] - cg_sell['delta']
                net_gamma[i] = pg_buy['gamma'] - pg_sell['gamma'] + cg_buy['gamma'] - cg_sell['gamma']
                net_theta[i] = pg_buy['theta'] - pg_sell['theta'] + cg_buy['theta'] - cg_sell['theta']
                net_vega[i] =  pg_buy['vega'] - pg_sell['vega'] + cg_buy['vega'] - cg_sell['vega']
                deltas_buy[i] = pg_buy['delta']
                deltas_sell[i] = pg_sell['delta']
            else:
                # Vertical Spreads or Single Legs
                res_b = get_data(row.strike_buy, row.right)
                is_single_leg = (getattr(row, 'strike_sell', 0.0) == 0.0)
                
                if is_single_leg:
                    if not res_b[1]:
                        valid_mask[i] = False
                        continue
                    pb, gb = res_b[0]
                    prices_buy[i] = pb
                    prices_sell[i] = 0.0
                    net_delta[i] = gb['delta']
                    net_gamma[i] = gb['gamma']
                    net_theta[i] = gb['theta']
                    net_vega[i] = gb['vega']
                    deltas_buy[i] = gb['delta']
                else:
                    res_s = get_data(row.strike_sell, row.right)
                    if not res_b[1] or not res_s[1]:
                        valid_mask[i] = False
                        continue
                    
                    pb, gb = res_b[0]
                    ps, gs = res_s[0]
                    
                    prices_buy[i] = pb
                    prices_sell[i] = ps
                    net_delta[i] = gb['delta'] - gs['delta']
                    net_gamma[i] = gb['gamma'] - gs['gamma']
                    net_theta[i] = gb['theta'] - gs['theta']
                    net_vega[i] = gb['vega'] - gs['vega']
                    deltas_buy[i] = gb['delta']
                    deltas_sell[i] = gs['delta']
        
        # 4. Integrate back and FILTER
        spreads_df['price_buy'] = prices_buy
        spreads_df['price_sell'] = prices_sell
        spreads_df['net_price'] = prices_buy - prices_sell
        spreads_df['delta'] = net_delta
        spreads_df['gamma'] = net_gamma
        spreads_df['theta'] = net_theta
        spreads_df['vega'] = net_vega
        spreads_df['delta_buy'] = deltas_buy
        spreads_df['delta_sell'] = deltas_sell
        
        # Apply filter to remove phantom rows
        original_count = len(spreads_df)
        spreads_df = spreads_df[valid_mask].copy()
        removed = original_count - len(spreads_df)
        
        if removed > 0 and self.log_func:
            self.log_func(f"      🚫 {removed} fantoom-strikes verwijderd (geen Bid/Ask in TWS).")
        elif original_count > 0 and self.log_func:
            self.log_func(f"      ✅ Alle {len(spreads_df)} strikes geverifieerd in TWS.")
        
        # Financial Metrics (Use filtered DataFrame columns to avoid length mismatch)
        debits = spreads_df['net_price'].values
        widths = spreads_df['width'].values
        
        # Max Profit Calculation (Vectorized)
        is_long = (widths == 0) | (spreads_df['strategy'] == 'Strangle')
        profits = np.where(debits < 0, -debits * 100, (widths - debits) * 100)
        profits[is_long] = 10000.0 # Unlimited proxy
        
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

        # Note: Extrinsic calculation for IC is slightly different
        spread_rights = spreads_df['right'].values
        strikes_buy = spreads_df['strike_buy'].values
        # For IC, we sum extensics of all 4 legs? Simplified: sum of net buy/sell extrinsic.
        # But let's keep it simple for now as it's a proxy.
        spreads_df['extrinsic_buy'] = get_extrinsic_vec(spreads_df['price_buy'].values, strikes_buy, spread_rights, underlying_price)
        # net_extrinsic
        spreads_df['extrinsic_sell'] = 0.0 # simplified for speed, can refine if needed
        spreads_df['net_extrinsic'] = spreads_df['extrinsic_buy'] # proxy

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
        # User: "De delta van de dichst bijzijnde is ook de geschatte kans op winst"
        dist_buy = np.abs(spreads_df['strike_buy'].values - underlying_price)
        dist_sell = np.abs(spreads_df['strike_sell'].values - underlying_price)
        
        # nearest_leg_delta
        nld = np.where(dist_buy <= dist_sell, spreads_df['delta_buy'].values, spreads_df['delta_sell'].values)
        spreads_df['pop'] = np.round(np.abs(nld) * 100, 1)

        # Safety Buffer: Distance to Max Pain
        # "wil ik dan altijd 5 punten van de max pain wegblijven"
        spreads_df['max_pain_spot'] = market_structure.get('max_pain', 0.0)
        spreads_df['max_pain_selection'] = market_structure.get('max_pain_selection', 0.0)
        
        def check_buffer(row):
            b_dist = abs(row['strike_buy'] - row['max_pain_spot'])
            s_dist = abs(row['strike_sell'] - row['max_pain_spot']) if row['strike_sell'] > 0 else 999.0
            return min(b_dist, s_dist) >= 5.0
            
        spreads_df['max_pain_buffer_ok'] = spreads_df.apply(check_buffer, axis=1)

        # 5. Advanced Metrics (IV Rank, Expected Move)
        if underlying_iv > 0:
            dte_val = spreads_df['dte'].iloc[0] if not spreads_df.empty else 30
            # Expected Move (1SD) = Spot * IV * sqrt(DTE/365)
            em = underlying_price * underlying_iv * np.sqrt(dte_val / 365.0)
            spreads_df['expected_move'] = np.round(em, 2)
            spreads_df['underlying_iv'] = underlying_iv * 100 # Display as percentage
            
            # IV Rank & Percentile (only if hist_iv_df provided and not empty)
            if hist_iv_df is not None and not hist_iv_df.empty:
                ivr, ivp = self.calculate_iv_indices(underlying_iv, hist_iv_df)
                spreads_df['iv_rank'] = ivr
                spreads_df['iv_percentile'] = ivp
            else:
                spreads_df['iv_rank'] = 0.0
                spreads_df['iv_percentile'] = 0.0

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
        if high > low:
            ivr = (current_iv - low) / (high - low) * 100
        else:
            ivr = 0.0
            
        # IV Percentile: Percentage of days where IV was lower than current
        ivp = (ivs < current_iv).sum() / len(ivs) * 100
        
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
            
        if 'min_profit' in filters and filters['min_profit'] > 0:
            before = len(df)
            df = df[df['max_profit'] >= filters['min_profit']]
            dropped = before - len(df)
            if dropped > 0:
                drop_stats['Profit'] = dropped
                if log_func: log_func(f"   🔻 Filter Profit < {filters['min_profit']}: {dropped} dropped")
            
        if 'min_delta' in filters:
            # For Iron Condors and Strangles, delta_sell might be different or multiple.
            # For now, skip min_delta filter if it's a multi-leg complex strategy OR a single long.
            # Or handle it specifically.
            before = len(df)
            mask = pd.Series([True]*len(df), index=df.index)
            
            if 'delta_sell' in df.columns:
                # Vertical Spreads: Filter by Delta Sell
                vertical_mask = df['strategy'].isin(['BullCall', 'BullPut', 'BearCall', 'BearPut'])
                mask &= ~vertical_mask | (df['delta_sell'].abs() >= filters['min_delta'])
                
            df = df[mask]
            dropped = before - len(df)
            if dropped > 0:
                drop_stats['Delta'] = dropped
                if log_func: log_func(f"   🔻 Filter Delta Sell < {filters['min_delta']}: {dropped} dropped")
                    
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
            df = df[df['dte'] <= filters['max_dte']]
            dropped = before - len(df)
            if dropped > 0:
                drop_stats['Max DTE'] = dropped
                if log_func: log_func(f"   🔻 Filter DTE > {filters['max_dte']}: {dropped} dropped")
            
        if 'min_dte' in filters:
            before = len(df)
            df = df[df['dte'] >= filters['min_dte']]
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
            
        if log_func:
            if df.empty and drop_stats:
                # Provide a consolidated reason if all were dropped
                sorted_drops = sorted(drop_stats.items(), key=lambda x: x[1], reverse=True)
                drop_str = ", ".join([f"{k} ({v})" for k, v in sorted_drops])
                log_func(f"   ⚠️ Alle {initial_count} kandidaten gefilterd! Meeste dalingen door: {drop_str}")
                log_func(f"   💡 Tip: Probeer in het weekend de filters (Profit of PoP) te verlagen of de breedte te veranderen.")
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
    
    def rank_spreads(self, spreads_df, sort_criteria=None, top_n=10):
        """
        Ranks spreads based on a list of criteria.
        sort_criteria: list of strings, e.g. ['expected_move', 'gamma', 'delta', 'max_pain']
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
            if crit == 'expected_move':
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
            return spreads_df.sort_values(by=sort_cols, ascending=sort_asc).head(top_n)
        except KeyError:
            # Fallback if specific columns missing
            return spreads_df.head(top_n)
