import pandas as pd
import numpy as np
from py_vollib.black_scholes.greeks.analytical import delta, gamma, vega, theta

class SpreadScanner:
    def __init__(self, ib_client):
        self.ib_client = ib_client

    def calculate_ema(self, df, span):
        """Calculates Exponential Moving Average."""
        return df['close'].ewm(span=span, adjust=False).mean()

    def filter_symbols_by_ema(self, symbols_data, ema_spans):
        """
        Filters symbols based on EMA trend (price > EMA).
        symbols_data: dict {symbol: {'price': float, 'history': DataFrame}}
        ema_spans: list of EMA periods to check (e.g. [8, 50, 150])
        Returns: list of symbols that pass the filter
        """
        passed_symbols = []
        
        for symbol, data in symbols_data.items():
            price = data.get('price', 0)
            hist_df = data.get('history', pd.DataFrame())
            
            if hist_df.empty or price <= 0:
                continue
            
            # Check if price is above all specified EMAs
            passes_all = True
            for span in ema_spans:
                ema_value = self.calculate_ema(hist_df, span).iloc[-1]
                if price < ema_value:
                    passes_all = False
                    break
            
            if passes_all:
                passed_symbols.append(symbol)
        
        return passed_symbols

    def calculate_greeks(self, row, underlying_price, risk_free_rate=0.04):
        """
        Calculates Greeks using py_vollib.
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
            
            d = delta(flag, S, K, T, risk_free_rate, sigma)
            g = gamma(flag, S, K, T, risk_free_rate, sigma)
            v = vega(flag, S, K, T, risk_free_rate, sigma)
            t = theta(flag, S, K, T, risk_free_rate, sigma)
            
            return pd.Series([d, g, v, t], index=['delta', 'gamma', 'vega', 'theta'])
        except Exception as e:
            # print(f"Greek Calc Error: {e}")
            return pd.Series([0.0, 0.0, 0.0, 0.0], index=['delta', 'gamma', 'vega', 'theta'])

    def generate_spreads(self, chains, strategy, component_price, params):
        """
        Generates spreads based on the provided strategy and parameters.
        chains: List of SecDefOptParams objects (from TWS).
        strategy: 'BullCall', 'BullPut', 'BearCall', 'BearPut'
        component_price: Current price of the underlying.
        params: dict of parameters (width, min_dte, max_dte, etc.)
        """
        spreads = []
        # print(f"[SpreadScanner] Generating {strategy} spreads for {params.get('symbol', 'N/A')} @ ${component_price:.2f}")
        
        valid_expirations = sorted(list(set([exp for chain in chains for exp in chain.expirations])))
        # print(f"[SpreadScanner] Found {len(valid_expirations)} expirations")
        
        for expiration in valid_expirations:
            # Filter by DTE
            dte = (pd.to_datetime(expiration) - pd.Timestamp.now()).days
            if dte < params.get('min_dte', 7) or dte > params.get('max_dte', 45):
                continue
                
            # Get strikes
            strikes = sorted(list(set([strike for chain in chains for strike in chain.strikes if expiration in chain.expirations])))
            
            # Determine logic based on strategy
            # Fix linter error by converting to string explicitly if needed, though usually str is fine.
            str_strategy = str(strategy)
            is_bull = 'Bull' in str_strategy
            
            width = params.get('width', 5)
            
            for i, long_strike in enumerate(strikes):
                # Target Short Strike
                target_short_strike = long_strike + width if is_bull else long_strike - width
                
                # Check for closest strike
                # In a real scenario we might want exact width, but for now exact match:
                if target_short_strike not in strikes:
                    continue
                
                short_strike = target_short_strike
                
                # Setup legs based on strategy
                if strategy == 'BullCall': # Debit: Buy Low Call, Sell High Call
                    strike_buy = long_strike
                    strike_sell = short_strike 
                    right = 'C'
                    # Filter OTM/ITMness
                    if strike_buy > component_price * 1.1: continue 

                elif strategy == 'BullPut': # Credit: Sell High Put (Short), Buy Low Put (Long)
                    # Note: "Long Strike" in iteration is just a number. 
                    # For Bull Put, we sell the higher Strike, Buy the lower strike.
                    # We iterated `long_strike`. `short_strike` is `long_strike + width`.
                    # So `short_strike` is higher.
                    # Buying the lower (long_strike), Selling the higher (short_strike).
                    strike_buy = long_strike
                    strike_sell = short_strike
                    right = 'P'
                    if strike_sell < component_price * 0.9: continue

                elif strategy == 'BearCall': # Credit: Sell Low Call, Buy High Call
                    # Bear Call: Short Lower Call, Long Higher Call.
                    # We iterated `long_strike`. `short_strike` is `long_strike - width`.
                    # So `short_strike` is lower.
                    # Buying `long_strike` (High), Selling `short_strike` (Low).
                    strike_buy = long_strike
                    strike_sell = short_strike
                    right = 'C'
                    if strike_sell > component_price * 1.1: continue

                elif strategy == 'BearPut': # Debit: Buy High Put, Sell Low Put
                    # Bear Put: Long Higher Put, Short Lower Put.
                    # We iterated `long_strike`. `short_strike` is `long_strike - width`.
                    # `short_strike` is lower.
                    # Buying `long_strike` (High), Selling `short_strike` (Low).
                    strike_buy = long_strike
                    strike_sell = short_strike
                    right = 'P'
                    if strike_buy < component_price * 0.9: continue
                
                else: 
                    continue

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
        
        # print(f"[SpreadScanner] Generated {len(spreads)} total spread combinations")        
        return pd.DataFrame(spreads)

    def calculate_max_pain(self, chain_data):
        """
        Calculates the Max Pain price for a given option chain DataFrame.
        chain_data: DataFrame with columns [strike, right, oi]
        Returns: max_pain_price (float)
        """
        if chain_data.empty or 'oi' not in chain_data.columns:
            return 0.0
            
        strikes = sorted(chain_data['strike'].unique())
        pain_values = {}
        
        for price_point in strikes:
            total_pain = 0
            
            # Pain for Calls (if price > strike)
            calls = chain_data[chain_data['right'] == 'C']
            # Only ITM calls cause pain (Intrinsic Value)
            itm_calls = calls[calls['strike'] < price_point]
            total_pain += ((price_point - itm_calls['strike']) * itm_calls['oi']).sum()
            
            # Pain for Puts (if price < strike)
            puts = chain_data[chain_data['right'] == 'P']
            # Only ITM puts cause pain
            itm_puts = puts[puts['strike'] > price_point]
            total_pain += ((itm_puts['strike'] - price_point) * itm_puts['oi']).sum()
            
            pain_values[price_point] = total_pain
            
        if not pain_values:
            return 0.0
            
        # Max Pain is the strike with the MINIMUM total pain value
        # Explicit key function for Python safety
        max_pain_price = min(pain_values, key=lambda k: pain_values[k])
        return max_pain_price

    def calculate_metrics(self, spreads_df, ib_client, symbol, underlying_price=None, chain_data=None):
        """
        Enriches spreads with Prices, Greeks, and Max Pain data.
        If chain_data is provided (from get_chain_greeks_and_oi), use it for Greeks.
        """
        if spreads_df.empty:
            return spreads_df
        
        if underlying_price is None:
             underlying_price = 100.0 
             
        # Calculate Max Pain if chain data provided
        max_pain = 0.0
        if chain_data is not None and not chain_data.empty:
            max_pain = self.calculate_max_pain(chain_data)
             
        # Add columns
        deltas = []
        gammas = []
        thetas = []
        vegas = []
        
        # Helper to get greeks from chain_data or calc
        def get_leg_greeks(strike, right):
            if chain_data is not None and not chain_data.empty:
                # Look up in chain_data
                match = chain_data[(chain_data['strike'] == strike) & (chain_data['right'] == right)]
                if not match.empty:
                    # Return Series with Greek columns
                    return match.iloc[0] # Expects cols: delta, gamma, theta, vega
            
            # Fallback to analytical if missing or no chain_data
            # create dummy row for calculation
            row = {'strike_buy': strike, 'right': right, 'dte': spreads_df['dte'].iloc[0], 'iv': 0.2} 
            return self.calculate_greeks(row, underlying_price)

        for index, row in spreads_df.iterrows():
            # Buy Leg
            greeks_buy = get_leg_greeks(row['strike_buy'], row['right'])
            
            # Sell Leg
            greeks_sell = get_leg_greeks(row['strike_sell'], row['right'])
            
            # Net Greeks: Buy - Sell
            net_delta = greeks_buy['delta'] - greeks_sell['delta']
            net_gamma = greeks_buy['gamma'] - greeks_sell['gamma']
            net_theta = greeks_buy['theta'] - greeks_sell['theta']
            net_vega = greeks_buy['vega'] - greeks_sell['vega']
            
            deltas.append(net_delta)
            gammas.append(net_gamma)
            thetas.append(net_theta)
            vegas.append(net_vega)
            
            # Store individual leg metrics
            spreads_df.at[index, 'delta_sell'] = greeks_sell['delta']
            spreads_df.at[index, 'delta_buy'] = greeks_buy['delta']
            spreads_df.at[index, 'gamma_sell'] = greeks_sell['gamma']

        spreads_df['delta'] = deltas
        spreads_df['gamma'] = gammas
        spreads_df['theta'] = thetas
        spreads_df['vega'] = vegas
        
        # Max Pain Distance
        if max_pain > 0:
            spreads_df['max_pain'] = max_pain
            spreads_df['dist_max_pain'] = abs(underlying_price - max_pain)
            # Distance of the spread center to max pain?
            # Or just distance of underlying?
            # User wants "afstand tot maximum pain". Usually: Price vs Max Pain.
            # But maybe "Is this spread near Max Pain?"
            spread_center = (spreads_df['strike_buy'] + spreads_df['strike_sell']) / 2
            spreads_df['spread_dist_max_pain'] = abs(spread_center - max_pain)
        else:
            spreads_df['max_pain'] = 0.0
            spreads_df['dist_max_pain'] = 0.0
            spreads_df['spread_dist_max_pain'] = 0.0
        
        # Placeholder for Prob of Profit (PoP) - simplified logic
        # Real PoP requires IV and days to expiry
        # PoP ~ 1 - Delta(Short) for Credit Spreads?
        # Rough proxy:
        if 'delta_sell' in spreads_df.columns:
             spreads_df['pop'] = (1.0 - spreads_df['delta_sell'].abs()) * 100
        else:
             spreads_df['pop'] = 50.0

        # Placeholder for Max Profit
        # Real calc needs prices (Cost of spread).
        # We don't have live spread prices yet (heavy).
        # Estimate: Width * 100 * (1 - PoP/100)? No.
        # Just use width for now as max potential.
        spreads_df['max_profit'] = spreads_df['width'] * 100 
        
        return spreads_df

    def filter_spreads(self, spreads_df, filters):
        """
        Filters the generated spreads based on user criteria.
        filters: dict (min_pop, min_max_profit, etc.)
        """
        if spreads_df.empty:
            return spreads_df
            
        df = spreads_df.copy()
        
        if 'min_pop' in filters and filters['min_pop'] > 0:
            df = df[df['pop'] >= filters['min_pop']]
            
        if 'min_profit' in filters and filters['min_profit'] > 0:
            df = df[df['max_profit'] >= filters['min_profit']]
            
        if 'min_delta' in filters:
            if 'delta_sell' in df.columns:
                 df = df[df['delta_sell'].abs() >= filters['min_delta']]
        
        # New filters implementation
        if 'max_dte' in filters:
            df = df[df['dte'] <= filters['max_dte']]
            
        if 'min_dte' in filters:
            df = df[df['dte'] >= filters['min_dte']]
            
        return df
    
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
