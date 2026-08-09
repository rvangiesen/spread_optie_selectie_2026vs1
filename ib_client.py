from ib_insync import IB, Stock, Option, Index, util
from ib_insync.contract import Contract
import pandas as pd
import numpy as np
import nest_asyncio
import yfinance as yf
import time

import logging

# Apply nest_asyncio to allow nested event loops in this module too
nest_asyncio.apply()

# Suppress non-fatal IBKR warning logs (10091, 354, 200, Unknown contract, etc.)
class IBErrorFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if any(f"{code}" in msg for code in ['10091', '354', '10167', '10089', ' 200,', '200: No security definition', 'Unknown contract']):
            return False
        return True

for _logger_name in ['ib_insync.wrapper', 'ib_insync.ib', 'ib_insync.client']:
    _log = logging.getLogger(_logger_name)
    _log.addFilter(IBErrorFilter())

class IBClient:
    def __init__(self):
        self.ib = IB()
        self.host = '127.0.0.1'
        self.port = 7497  # Default paper trading port
        self.client_id = 1
        self.connected = False
        self.market_data_type = 3 # Default to Delayed
        self.last_error = ""
        self.ib.errorEvent += self._on_ib_error
        
    def _on_ib_error(self, reqId, errorCode, errorString, contract):
        """Handler for IB API error/warning events."""
        if errorCode in [354, 10091, 10167, 10089]: # Market data subscription notices
            if self.market_data_type == 1:
                sym = getattr(contract, 'symbol', '') if contract else ''
                print(f"[IBClient] Notice {errorCode} (Live data unsubscribed for '{sym}'): Auto-switching to Delayed Market Data (Type 3).")
                self.market_data_type = 3
                try:
                    self.ib.reqMarketDataType(3)
                except Exception:
                    pass

    def log_debug(self, msg):
        """Helper for logging debug information."""
        print(f"DEBUG_LOG: {msg}")
        
    def set_data_type(self, type_id):
        """Sets the market data type (1=Live, 3=Delayed, 4=Frozen)."""
        self.market_data_type = type_id
        if self.is_connected():
            self.ib.reqMarketDataType(type_id)

    def connect(self, host='127.0.0.1', port=7497, client_id=1):
        """Connects to the TWS/Gateway API."""
        try:
            if not self.ib.isConnected():
                import asyncio
                loop = util.getLoop()
                if loop.is_running():
                    async def _do_connect():
                        await asyncio.wait_for(self.ib.connectAsync(host, port, clientId=client_id, timeout=0), timeout=10.0)
                    task = loop.create_task(_do_connect())
                    util.run(task)
                else:
                    self.ib.connect(host, port, clientId=client_id, timeout=0)
                self.connected = True
                self.host = host
                self.port = port
                self.client_id = client_id
            return True, "Connected successfully"
        except Exception as e:
            self.connected = False
            return False, f"Connection failed: {str(e)}"

    def disconnect(self):
        """Disconnects from the TWS/Gateway API."""
        if self.ib.isConnected():
            self.ib.disconnect()
        self.connected = False

    def is_connected(self):
        return self.ib.isConnected()

    def safe_qualify_contract(self, contract, max_attempts=5):
        """
        Attempts to qualify a contract with a timeout.
        Returns the qualified contract or the original if it fails/times out.
        """
        if not self.is_connected():
            return contract
        
        try:
            # Try to qualify with limited attempts
            qualified = self.ib.qualifyContracts(contract)
            if qualified and len(qualified) > 0:
                return qualified[0]
        except Exception:
            pass
        
        # Return original contract if qualification fails
        return contract

    def get_market_price(self, contract):
        """Fetches the current market price (delayed or live)."""
        if not self.is_connected():
            return None
        
        data_types_to_try = [self.market_data_type]
        if self.market_data_type == 1:
            data_types_to_try = [1, 3, 4, 2]
        elif self.market_data_type == 3:
            data_types_to_try = [3, 4]
            
        for dtype in data_types_to_try:
            self.ib.reqMarketDataType(dtype) 
            ticker = self.ib.reqMktData(contract, '', False, False)
            
            for _ in range(10): 
                self.ib.sleep(0.1)
                if ticker.last == ticker.last and ticker.last > 0: 
                    self.ib.cancelMktData(contract)
                    return ticker.last
                if ticker.bid > 0 and ticker.ask > 0:
                    mid = (ticker.bid + ticker.ask) / 2
                    self.ib.cancelMktData(contract)
                    return mid
                if ticker.close == ticker.close and ticker.close > 0:
                    self.ib.cancelMktData(contract)
                    return ticker.close
            
            self.ib.cancelMktData(contract)        
            
        return None

    def qualify_contract_safe(self, contract):
        """
        Async-safe qualification with timeout.
        Uses ib.sleep() to keep the loop alive while waiting.
        Returns qualified contract or None.
        """
        if not self.is_connected():
            return None
            
        import asyncio
        import time
        
        try:
            # Use ensure_future instead of create_task for Future compatibility
            loop = asyncio.get_event_loop()
            task = asyncio.ensure_future(self.ib.qualifyContractsAsync(contract))
            
            # Wait for completion or timeout loop-pump
            start_time = time.time()
            while not task.done():
                self.ib.sleep(0.1) # KEY: Process events so task can complete!
                if time.time() - start_time > 20.0: # Increased to 20s to allow slow weekend fetches
                    print(f"DEBUG_LOG: Qualification TIMEOUT for {contract.symbol} {getattr(contract, 'strike', '')}")
                    task.cancel()
                    return None
            
            # Get result if successful
            if task.done() and not task.cancelled():
                res = task.result()
                if res and len(res) > 0:
                    return res[0]
                else:
                    print(f"DEBUG_LOG: Qualification failed (empty result) for {contract}")
                    
        except Exception as e:
            print(f"DEBUG_LOG: Error in qualify_contract_safe: {e}")
        
        return None

    def get_market_data_snapshot(self, contract, use_hist_fallback=True, use_yf=False, **kwargs):
        """
        Fetches a real-time (or delayed) snapshot of price and IV.
        Optional fallback to historical data if 'use_hist_fallback' is True.
        """
        import time
        import yfinance as yf
        price = 0.0
        iv = 0.0
        source = 'N/A'
        ticker = None

        if use_yf:
            try:
                symbol = contract.symbol
                if symbol == 'SPX': symbol = '^SPX'
                elif symbol == 'NDX': symbol = '^NDX'
                elif symbol == 'VIX': symbol = '^VIX'
                
                tk = yf.Ticker(symbol)
                df = tk.history(period="1d")
                if not df.empty:
                    price = float(df['Close'].iloc[-1])
                    return {'price': price, 'iv': 0.0, 'source': 'yfinance'}
            except Exception as e:
                print(f"[IBClient] yfinance price fetch error for {contract.symbol}: {e}")

        if not self.is_connected():
            return {'price': 0.0, 'iv': 0.0, 'source': 'Disconnected'}
        
        def log_debug(msg):
            try:
                print(f"DEBUG_LOG: {msg}")
            except:
                pass

        try:
            # 1. Qualify (Async Safe)
            try:
                 qualified_contract = self.qualify_contract_safe(contract)
                 if qualified_contract:
                     contract = qualified_contract
                     log_debug(f"Qualified: {contract.symbol} (ID: {contract.conId})")
                 else:
                     # If qualification fails as IND, try as STK for common ETFs mistakenly classified
                     if contract.secType == 'IND':
                         log_debug(f"IND qualification failed for {contract.symbol}, retrying as STK...")
                         contract.secType = 'STK'
                         qualified_contract = self.qualify_contract_safe(contract)
                         if qualified_contract:
                             contract = qualified_contract
                             log_debug(f"Qualified as STK: {contract.symbol}")
            except Exception as e:
                 log_debug(f"Qualify failed: {e}")

            # 2. Strategy: Try Standard Data first, then Delayed/Frozen if closed or unsubscribed
            # Types: 1=Live, 3=Delayed, 2=Frozen, 4=Delayed Frozen
            data_types_to_try = [self.market_data_type]
            if self.market_data_type == 1:
                data_types_to_try = [1, 3, 4, 2]
            elif self.market_data_type == 3:
                data_types_to_try = [3, 4]
            
            found = False
            ticker = None

            for dtype in data_types_to_try:
                if found: break
                
                log_debug(f"Trying Market Data Type: {dtype} for {contract.symbol}")
                self.ib.reqMarketDataType(dtype)
                self.ib.reqMktData(contract, '106', False, False)
                ticker = self.ib.ticker(contract)
                
                start_time = time.time()
                while time.time() - start_time < 2.5: # 2.5s poll per type
                    self.ib.sleep(0.1)
                    
                    p = 0.0
                    has_real_market = False
                    # Priority check for price data
                    # [FIX] For options, Bid/Ask Midpoint is much more reliable than 'Last'
                    if (contract.secType == 'OPT' or contract.secType == 'FOP') and ticker.bid > 0 and ticker.ask > 0:
                        p = (ticker.bid + ticker.ask) / 2
                        has_real_market = True
                    elif getattr(ticker, 'last', 0.0) > 0 and ticker.last == ticker.last:
                        p = ticker.last
                        has_real_market = True
                    elif getattr(ticker, 'close', 0.0) > 0 and ticker.close == ticker.close:
                        # Prefer close over bid/ask for non-options because after-hours bid/ask spreads can be massively skewed (e.g. 64 / 175)
                        p = ticker.close
                        has_real_market = True
                    elif getattr(ticker, 'bid', 0.0) > 0 and getattr(ticker, 'ask', 0.0) > 0:
                        p = (ticker.bid + ticker.ask) / 2
                        has_real_market = True
                    
                    # Only consider price valid if it comes from a real market source (last, bid/ask, or close)
                    if p > 0 and has_real_market:
                        price = p
                        found = True
                        log_debug(f"Found Price: {price} (Type {dtype})")
                        if dtype in [2, 4]:
                            source = f"Frozen/Delayed (Type {dtype})"
                        else:
                            source = "Real-time/Delayed"
                        break
                
                if not found:
                    self.ib.cancelMktData(contract)

            if found:
                 if ticker:
                    # Multi-source IV fetch
                    iv = 0.0
                    if ticker.modelGreeks and ticker.modelGreeks.impliedVol:
                        iv = ticker.modelGreeks.impliedVol
                    elif ticker.impliedVolatility and ticker.impliedVolatility > 0:
                        iv = ticker.impliedVolatility
            else:
                log_debug(f"Timeout. Last state: Last={ticker.last if ticker else '?'} Close={ticker.close if ticker else '?'}")

        except Exception as e:
            log_debug(f"Fetch error: {e}")
            pass

        # Fallback: Historical Data (Last Resort)
        if price <= 0 and use_hist_fallback:
            try:
                hist_data = self.get_historical_data(contract, duration='5 D', bar_size='1 day')
                if not hist_data.empty and 'close' in hist_data.columns:
                    price = float(hist_data['close'].iloc[-1])
                    if price > 0:
                        source = 'TWS Historical'
            except Exception:
                 pass

        if price <= 0:
             state_msg = f"Last={ticker.last if ticker else 'N/A'} Close={ticker.close if ticker else 'N/A'}"
             source = f"All sources failed ({state_msg})"
        
        # Add conId to source for better debugging of "Wrong Symbol" issues
        if contract and hasattr(contract, 'conId') and contract.conId:
            source += f" [conId: {contract.conId}]"
            
        if ticker and found: self.ib.cancelMktData(contract)
        return {'price': price, 'iv': iv, 'source': source}

    def get_market_data_batch(self, contracts):
        """
        Fetches market data for a list of contracts efficiently, chunked to respect limits.
        Returns a dictionary {symbol: price}.
        """
        if not self.is_connected() or not contracts:
            return {}
            
        data_types_to_try = [self.market_data_type]
        if self.market_data_type == 1:
            data_types_to_try = [1, 3, 4, 2]
        elif self.market_data_type == 3:
            data_types_to_try = [3, 4]
            
        results = {}
        chunk_size = 50
        for i in range(0, len(contracts), chunk_size):
            chunk = contracts[i:i + chunk_size]
            
            for dtype in data_types_to_try:
                self.ib.reqMarketDataType(dtype)
                tickers = [self.ib.reqMktData(c, '', False, False) for c in chunk]
                
                for _ in range(15):
                    self.ib.sleep(0.1)
                    pending = [t for t in tickers if (t.last != t.last and t.close != t.close and not (t.bid > 0 and t.ask > 0))]
                    if not pending:
                        break
                        
                got_data = False
                for t in tickers:
                    price = t.last if (t.last == t.last and t.last > 0) else t.close
                    if price != price or price <= 0:
                        if t.bid > 0 and t.ask > 0:
                            price = (t.bid + t.ask) / 2
                        elif t.bid > 0:
                            price = t.bid
                    if price > 0 and t.contract.symbol:
                        results[t.contract.symbol] = price
                        got_data = True
                        
                for t in tickers:
                    self.ib.cancelMktData(t.contract)
                    
                if got_data:
                    break
            
        return results

    def get_historical_data(self, contract, duration='6 M', bar_size='1 day'):
        """
        Fetches historical data for a single contract.
        Multi-tier fallback: TWS -> yfinance (for STK/IND) -> Price Snapshot.
        Returns a pandas DataFrame with OHLCV data.
        """
        import threading
        import queue
        import time
        import yfinance as yf
        from ib_insync import util

        # 1. Try TWS (Direct Async with Loop-Pumping)
        if self.is_connected():
            import asyncio
            # Qualify contract first (Crucial for speed/reliability of reqHistoricalData)
            qualified = self.qualify_contract_safe(contract)
            working_contract = qualified if qualified else contract
            
            # Use Type 3 (Delayed) for historical data on weekends/paper accounts if Type 1 fails
            self.ib.reqMarketDataType(3) 

            # Determine optimal show_type order based on day of week
            import datetime
            is_weekend = datetime.datetime.now().weekday() >= 5
            show_types = ['MIDPOINT', 'BID_ASK', 'TRADES'] if is_weekend else ['TRADES', 'MIDPOINT', 'BID_ASK']

            # Durations to try
            durations_to_try = [duration, '30 D'] if duration != '30 D' else [duration]

            for dur in durations_to_try:
                for show_type in show_types:
                    try:
                        print(f"[IBClient] TWS fetch attempt ({show_type}, {dur}) for {working_contract.symbol}")
                        coro = self.ib.reqHistoricalDataAsync(
                            working_contract,
                            endDateTime='', # 'now'
                            durationStr=dur,
                            barSizeSetting=bar_size,
                            whatToShow=show_type,
                            useRTH=True
                        )
                        task = asyncio.ensure_future(coro)
                        
                        # Shortened timeout for speed
                        start_wait = time.time()
                        while not task.done():
                            self.ib.sleep(0.1) # Faster pumping
                            if time.time() - start_wait > 10.0: # 10s limit per attempt
                                print(f"[IBClient] TWS Timeout (10s) for {working_contract.symbol} {show_type} {dur}")
                                task.cancel()
                                # Allow a small breath for cancellation to process
                                self.ib.sleep(0.05)
                                break
                        
                        if task.done() and not task.cancelled() and not task.exception():
                            bars = task.result()
                            if bars:
                                print(f"[IBClient] TWS success for {working_contract.symbol} ({show_type})")
                                return util.df(bars)
                    except Exception as e:
                        print(f"[IBClient] TWS Error for {working_contract.symbol} {show_type}: {e}")

        # 2. Try yfinance Fallback (ONLY if not in simulated future)
        if contract.secType in ['STK', 'IND', 'IDX']:
            import datetime
            symbol = contract.symbol
            if symbol == 'SPX': symbol = '^SPX'
            elif symbol == 'NDX': symbol = '^NDX'
            elif symbol == 'VIX': symbol = '^VIX'
            
            for attempt in range(1, 4):
                try:
                    # Use a slightly more generous window for yfinance
                    start_date = (datetime.datetime.now() - datetime.timedelta(days=400)).strftime('%Y-%m-%d')
                    
                    print(f"[IBClient] yf.download attempt {attempt} for {symbol} (from {start_date})...")
                    df = yf.download(
                        symbol, 
                        start=start_date,
                        interval='1d', 
                        progress=False, 
                        threads=False
                    )
                    
                    if df is not None and not df.empty:
                        # CRITICAL: yf 1.2+ often returns MultiIndex even for single symbol
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = df.columns.get_level_values(0)
                        
                        # Normalize column names to lowercase
                        df.columns = [c.lower() for c in df.columns]
                        
                        # Fix for Close vs Adj Close
                        if 'adj close' in df.columns:
                            df = df.rename(columns={'adj close': 'adj_close'})
                        
                        if 'close' in df.columns:
                            df['close'] = df['close'].astype(float)
                            df = df.sort_index()
                            print(f"[IBClient] yf SUCCESS for {symbol} ({len(df)} rows)")
                            return df
                except Exception as e:
                    print(f"[IBClient] yf attempt {attempt} error for {symbol}: {e}")
                    time.sleep(1.5)
            
            print(f"[IBClient] yf TOTAL FAILURE for {symbol}")

        # 3. Final Fallback: Single Row Snapshot
        print(f"[IBClient] Final fallback: Creating snapshot row for {contract.symbol}")
        curr_price = self.get_market_price(contract)
        if curr_price and curr_price > 0:
            return pd.DataFrame({'close': [curr_price]}, index=[pd.Timestamp.now()])
            
        return pd.DataFrame()

    def get_earnings_date(self, symbol):
        """
        Attempts to fetch the next earnings date for a symbol.
        Returns a pd.Timestamp or None.
        """
        try:
            import datetime
            current_year = datetime.datetime.now().year
            if current_year >= 2026:
                # In simulation mode, we don't have future earnings dates from yf
                return None

            # Clean symbol for yfinance
            yf_sym = symbol
            if yf_sym == 'SPX': yf_sym = '^SPX'
            elif yf_sym == 'NDX': yf_sym = '^NDX'
            
            ticker = yf.Ticker(yf_sym)
            calendar = ticker.calendar
            if calendar is not None and not calendar.empty:
                # yf usually returns a 'Earnings Date' or 'Earnings Date Low'
                # Let's try to get the first date from the calendar
                if 'Earnings Date' in calendar.index:
                    dates = calendar.loc['Earnings Date']
                    if isinstance(dates, (list, tuple, pd.Series)):
                        return pd.to_datetime(dates[0])
                    return pd.to_datetime(dates)
            
            # Additional check for 'Earnings Date' in info as backup
            info = ticker.info
            if 'nextEarningsDate' in info:
                return pd.to_datetime(info['nextEarningsDate'], unit='s')
                
        except Exception as e:
            print(f"[IBClient] Error fetching earnings for {symbol}: {e}")
            
        return None

    def get_historical_iv(self, contract, duration='1 Y', bar_size='1 day'):
        """
        Fetches historical implied volatility for a contract.
        Used for IV Rank and IV Percentile.
        """
        if not self.is_connected():
            return pd.DataFrame()
            
        import asyncio
        import time
        from ib_insync import util
        
        # Qualify first
        qualified = self.qualify_contract_safe(contract)
        working_contract = qualified if qualified else contract
        
        # Respect user configured market data type (e.g. 1 for live, 3 for delayed)
        self.ib.reqMarketDataType(self.market_data_type)
        
        try:
            print(f"[IBClient] Fetching Historical IV for {working_contract.symbol}")
            coro = self.ib.reqHistoricalDataAsync(
                working_contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow='OPTION_IMPLIED_VOLATILITY',
                useRTH=True
            )
            task = asyncio.ensure_future(coro)
            
            start_wait = time.time()
            while not task.done():
                self.ib.sleep(0.1)
                if time.time() - start_wait > 10.0:
                    print(f"[IBClient] IV History Timeout for {working_contract.symbol}")
                    task.cancel()
                    self.ib.sleep(0.05)
                    break
            
            if task.done() and not task.cancelled() and not task.exception():
                bars = task.result()
                if bars:
                    df = util.df(bars)
                    if not df.empty:
                        # Normalize
                        df.rename(columns={'close': 'iv'}, inplace=True)
                        return df
        except Exception as e:
            print(f"[IBClient] IV History Error for {working_contract.symbol}: {e}")
            
        return pd.DataFrame()

    def get_historical_data_batch(self, contracts, duration='6 M', bar_size='1 day'):
        """
        Fetches historical data for multiple contracts.
        NOTE: TWS Pacing violations are likely if we do this too fast.
        We must throttle this in a real app.
        """
        if not self.is_connected():
            return {}
            
        results = {}
        for contract in contracts:
            # Simple serial fetch for now to avoid Pacing Violation (max 50/sec but historical is stricter)
            # In a robust app, we'd use a queue/worker system.
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow='TRADES',
                useRTH=True
            )
            if bars:
                results[contract.symbol] = util.df(bars)
            else:
                 # Fallback
                 p = self.get_market_price(contract)
                 if p: results[contract.symbol] = pd.DataFrame({'close': [p]}, index=[pd.Timestamp.now()])
                 
            self.ib.sleep(0.1) # Small delay
            
        return results

    def get_atr(self, symbol, period=10):
        """
        Calculates the Average True Range (ATR) for a given symbol.
        Fetches 'period + 5' days of historical data to ensure we have enough bars.
        """
        try:
            # 1. Create stock contract
            contract = Stock(symbol=symbol, exchange='SMART', currency='USD')
            
            # 2. Fetch slightly more data than needed to have previous close for the first TR
            # Use 1 month as a safe duration for 10-day ATR
            df = self.get_historical_data(contract, duration='1 M', bar_size='1 day')
            
            if df is None or df.empty or len(df) < (period + 1):
                # print(f"DEBUG_LOG: Not enough data for ATR calculation ({symbol})")
                return 0.0
                
            # 3. Calculate True Range (TR)
            # TR = max(H-L, abs(H-Cp), abs(L-Cp))
            df['prev_close'] = df['close'].shift(1)
            df['tr1'] = df['high'] - df['low']
            df['tr2'] = (df['high'] - df['prev_close']).abs()
            df['tr3'] = (df['low'] - df['prev_close']).abs()
            df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
            
            # 4. Simple ATR (Average of last 'period' TR values)
            atr = df['tr'].tail(period).mean()
            return round(float(atr), 2)
            
        except Exception as e:
            # print(f"DEBUG_LOG: ATR calculation error for {symbol}: {e}")
            return 0.0

    def get_option_chains_params(self, symbol, sec_type='STK', exchange='SMART', currency='USD', use_yf=False):
        """
        Fetches option chain parameters (strikes, expirations) for a given underlying.
        Returns a list of SecDefOptParams objects.
        """
        if use_yf:
            import yfinance as yf
            class YFOptParams:
                def __init__(self, expirations, strikes):
                    self.expirations = expirations
                    self.strikes = strikes
            try:
                yf_sym = symbol
                if yf_sym == 'SPX': yf_sym = '^SPX'
                elif yf_sym == 'NDX': yf_sym = '^NDX'
                elif yf_sym == 'VIX': yf_sym = '^VIX'
                
                tk = yf.Ticker(yf_sym)
                expirations = tk.options
                if not expirations:
                    return []
                ib_expirations = [e.replace('-', '') for e in expirations]
                
                # Fetch strikes from the first expiration
                chain = tk.option_chain(expirations[0])
                strikes = sorted(list(set(chain.calls['strike'].tolist() + chain.puts['strike'].tolist())))
                return [YFOptParams(ib_expirations, strikes)]
            except Exception as e:
                print(f"[IBClient] YF Error fetching option chains: {e}")
                return []

        if not self.is_connected():
            return []
        
        chains = []
        try:
            # 1. First QUALIFY the underlying to get conId (Crucial for reliable lookup)
            contract = Contract(symbol=symbol, secType=sec_type, exchange=exchange, currency=currency)
            qualified_contract = self.qualify_contract_safe(contract)
            
            underlying_conId = 0
            if qualified_contract:
                 underlying_conId = qualified_contract.conId
            
            # 2. Request option parameters using conId if available, fallback to 0
            # Note: reqSecDefOptParams(underlyingSymbol, futFopExchange, underlyingSecType, underlyingConId)
            chains = self.ib.reqSecDefOptParams(
                symbol,
                '', # futFopExchange (empty for STK)
                sec_type,
                underlying_conId
            )
        except Exception as e:
            pass
            
        if not chains:
            # Fallback to yfinance during weekends or if TWS database is offline
            try:
                import yfinance as yf
                ticker = yf.Ticker(symbol)
                exps = ticker.options
                if exps:
                    ib_exps = [d.replace('-', '') for d in exps]
                    all_strikes = set()
                    # Query first 4 expirations to collect a good set of strikes
                    for exp in exps[:4]:
                        try:
                            opt_chain = ticker.option_chain(exp)
                            strikes = opt_chain.calls['strike'].tolist() + opt_chain.puts['strike'].tolist()
                            all_strikes.update(strikes)
                        except Exception:
                            continue
                    if all_strikes:
                        class MockSecDefOptParams:
                            def __init__(self, expirations, strikes):
                                self.expirations = list(expirations)
                                self.strikes = [float(s) for s in strikes]
                                self.multiplier = '100'
                                self.exchange = 'SMART'
                                self.tradingClass = ''
                        chains = [MockSecDefOptParams(ib_exps, sorted(list(all_strikes)))]
                        print(f"DEBUG_LOG: Option chains fallback to yfinance. Found {len(ib_exps)} expirations, {len(all_strikes)} strikes.")
            except Exception as e:
                print(f"DEBUG_LOG: yfinance fallback failed: {e}")
                
        return chains if chains else []

    def get_chain_greeks_and_oi(self, symbol, expiration, strikes, multiplier='100', use_yf=False):
        if use_yf:
            import yfinance as yf
            from datetime import datetime
            import math
            try:
                yf_sym = symbol
                if yf_sym == 'SPX': yf_sym = '^SPX'
                elif yf_sym == 'NDX': yf_sym = '^NDX'
                elif yf_sym == 'VIX': yf_sym = '^VIX'
                
                tk = yf.Ticker(yf_sym)
                yf_exp = f"{expiration[:4]}-{expiration[4:6]}-{expiration[6:8]}"
                chain = tk.option_chain(yf_exp)
                
                # Fetch underlying price for greeks
                und_price = tk.history(period="1d")['Close'].iloc[-1] if not tk.history(period="1d").empty else 0.0
                
                data_list = []
                
                # DTE calculation for Greeks
                exp_date = datetime.strptime(yf_exp, '%Y-%m-%d')
                dte = (exp_date - datetime.now()).days
                t_years = max(0.001, dte / 365.0)
                risk_free = 0.04 # 4% approximate risk free rate
                
                def add_to_list(df, right):
                    # Filter by strikes
                    df = df[df['strike'].isin(strikes)]
                    for _, row in df.iterrows():
                        strike = row['strike']
                        bid = row.get('bid', 0.0)
                        ask = row.get('ask', 0.0)
                        last = row.get('lastPrice', 0.0)
                        vol = row.get('volume', 0)
                        oi = row.get('openInterest', 0)
                        iv = row.get('impliedVolatility', 0.0)
                        
                        mid_p = (bid + ask) / 2 if (bid > 0 and ask > 0) else last
                        if mid_p <= 0: continue
                        
                        delta, gamma, vega, theta = 0.0, 0.0, 0.0, 0.0
                        if und_price > 0 and iv > 0:
                            try:
                                import py_vollib.black_scholes.greeks.analytical as greeks
                                flag = 'c' if right == 'C' else 'p'
                                delta = greeks.delta(flag, und_price, strike, t_years, risk_free, iv)
                                gamma = greeks.gamma(flag, und_price, strike, t_years, risk_free, iv)
                                vega = greeks.vega(flag, und_price, strike, t_years, risk_free, iv) / 100.0
                                theta = greeks.theta(flag, und_price, strike, t_years, risk_free, iv) / 365.0
                            except Exception:
                                pass # ignore calculation errors
                                
                        data_list.append({
                            'strike': strike,
                            'right': right,
                            'bid': bid,
                            'ask': ask,
                            'mid': mid_p,
                            'volume': vol or 0,
                            'openInterest': oi or 0,
                            'delta': delta,
                            'gamma': gamma,
                            'vega': vega,
                            'theta': theta,
                            'iv': iv,
                            'opt_price': mid_p,
                            'und_price': und_price
                        })
                        
                add_to_list(chain.calls, 'C')
                add_to_list(chain.puts, 'P')
                
                return pd.DataFrame(data_list)
            except Exception as e:
                print(f"[IBClient] YF Error fetching option chains: {e}")
                return pd.DataFrame()

        try:
            # 1. Create specific C/P contracts for requested strikes
            target_contracts = []
            is_index = any(idx in symbol.upper() for idx in ['SPX', 'NDX', 'RUT', 'VIX', 'DAX'])
            opt_exchange = 'CBOE' if is_index else 'SMART'
            
            for s in strikes:
                target_contracts.append(Option(symbol=symbol, lastTradeDateOrContractMonth=expiration, strike=float(s), right='C', multiplier=multiplier, exchange=opt_exchange, currency='USD'))
                target_contracts.append(Option(symbol=symbol, lastTradeDateOrContractMonth=expiration, strike=float(s), right='P', multiplier=multiplier, exchange=opt_exchange, currency='USD'))
            
            print(f"DEBUG_LOG: Qualifying {len(target_contracts)} specific contracts for {symbol} {expiration}...")
            # Qualify in bulk (this fills conId and ensures they exist)
            import asyncio
            try:
                task = asyncio.ensure_future(self.ib.qualifyContractsAsync(*target_contracts))
                start_wait = time.time()
                while not task.done():
                    self.ib.sleep(0.1)
                    if time.time() - start_wait > 3.0:
                        task.cancel()
                        break
                if task.done() and not task.cancelled() and not task.exception():
                    qualified = task.result()
                    final_valid = [c for c in qualified if c.conId > 0]
                else:
                    final_valid = []
            except Exception as e:
                print(f"DEBUG_LOG: Qualification failed: {e}")
                final_valid = []
                
            print(f"DEBUG_LOG: Successfully qualified {len(final_valid)}/{len(target_contracts)} contracts.")
            
            if not final_valid:
                print(f"DEBUG_LOG: Fallback to unqualified contracts for {symbol} {expiration}...")
                contracts = target_contracts
            else:
                contracts = final_valid
        except Exception as e:
            print(f"DEBUG_LOG: Contract discovery error: {e}")
            return pd.DataFrame()

        self.ib.reqMarketDataType(self.market_data_type)
        
        data_types_to_try = [self.market_data_type]
        if self.market_data_type == 1:
            data_types_to_try = [1, 3, 4, 2]
        elif self.market_data_type == 3:
            data_types_to_try = [3, 4]
            
        print(f"DEBUG_LOG: Requesting market data for {len(contracts)} contracts in chunks of 50...")
        all_tickers = []
        import time
        
        chunk_size = 50
        for i in range(0, len(contracts), chunk_size):
            chunk = contracts[i:i + chunk_size]
            tickers = []
            self.ib.reqMarketDataType(data_types_to_try[0])
            for c in chunk:
                t = self.ib.reqMktData(c, '106', False, False)
                tickers.append(t)
            
            for dtype in data_types_to_try:
                self.ib.reqMarketDataType(dtype)
                start_type = time.time()
                type_timeout = 2.0 if dtype == data_types_to_try[0] else 1.0
                while time.time() - start_type < type_timeout:
                    if not self.ib.isConnected(): break
                    self.ib.sleep(0.2)
                    if all((t.modelGreeks or (t.close and t.close > 0) or (t.last and t.last > 0) or (t.bid > 0 and t.ask > 0)) for t in tickers): break
                if any(t.modelGreeks or (t.bid > 0 and t.ask > 0) or (t.last > 0) or (t.close > 0) for t in tickers): break
            
            all_tickers.extend(tickers)
            # Crucial step: cancel subscriptions immediately to avoid hitting the 100 limit!
            for t in tickers:
                self.ib.cancelMktData(t.contract)
        
        found_greeks = len([t for t in all_tickers if t.modelGreeks])
        found_prices = len([t for t in all_tickers if any([t.bid>0, t.ask>0, t.last>0, t.close>0])])
        print(f"DEBUG_LOG: Polling finished. Found Greeks: {found_greeks}/{len(all_tickers)}, Found Prices: {found_prices}/{len(all_tickers)}")
        
        # Build yfinance option price fallback dictionary if TWS data is missing or it's the weekend
        yf_lookup = {}
        try:
            import yfinance as yf
            yf_exp = f"{expiration[:4]}-{expiration[4:6]}-{expiration[6:8]}"
            ticker = yf.Ticker(symbol)
            chain = ticker.option_chain(yf_exp)
            
            for _, row in chain.calls.iterrows():
                strike_val = round(float(row['strike']), 4)
                yf_lookup[(strike_val, 'C')] = {
                    'bid': float(row.get('bid', 0.0)),
                    'ask': float(row.get('ask', 0.0)),
                    'last': float(row.get('lastPrice', 0.0)),
                    'close': float(row.get('lastPrice', 0.0))
                }
            for _, row in chain.puts.iterrows():
                strike_val = round(float(row['strike']), 4)
                yf_lookup[(strike_val, 'P')] = {
                    'bid': float(row.get('bid', 0.0)),
                    'ask': float(row.get('ask', 0.0)),
                    'last': float(row.get('lastPrice', 0.0)),
                    'close': float(row.get('lastPrice', 0.0))
                }
            print(f"DEBUG_LOG: yfinance option price fallback loaded with {len(yf_lookup)} strikes for {symbol} {expiration}.")
        except Exception as e:
            print(f"DEBUG_LOG: failed to load yfinance option price fallback: {e}")

        data_list = []
        for t in all_tickers:
             strike = t.contract.strike
             right = t.contract.right
             strike_key = (round(float(strike), 4), right)
             
             bid = t.bid if (t.bid and t.bid > 0) else 0.0
             ask = t.ask if (t.ask and t.ask > 0) else 0.0
             last = t.last if (t.last and t.last > 0) else 0.0
             close = t.close if (t.close and t.close > 0) else 0.0
             
             greeks = {'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0, 'optPrice': 0.0, 'iv': 0.0, 'und_price': 0.0}
             if t.modelGreeks:
                 greeks['delta'] = t.modelGreeks.delta or 0
                 greeks['gamma'] = t.modelGreeks.gamma or 0
                 greeks['vega'] = t.modelGreeks.vega or 0
                 greeks['theta'] = t.modelGreeks.theta or 0
                 greeks['optPrice'] = t.modelGreeks.optPrice or 0.0
                 greeks['iv'] = t.modelGreeks.impliedVol or t.impliedVolatility or 0.0
                 greeks['und_price'] = t.modelGreeks.undPrice or 0.0
             
             model_p = greeks.get('optPrice', 0.0)
             und_p = greeks.get('und_price', 0.0)
             
             # Calculate intrinsic value threshold for stale price filtering
             if und_p > 0 and float(strike) > 0:
                 intr = max(0.0, und_p - float(strike)) if right == 'C' else max(0.0, float(strike) - und_p)
             else:
                 intr = 0.0
             min_valid = max(0.0, intr - 0.50)

             # Fallback to yfinance if TWS price data is missing or stale
             yf_data = yf_lookup.get(strike_key)
             if yf_data:
                 if bid <= 0 and yf_data['bid'] >= min_valid: bid = yf_data['bid']
                 if ask <= 0 and yf_data['ask'] >= min_valid: ask = yf_data['ask']
                 if last <= 0 and yf_data['last'] >= min_valid: last = yf_data['last']
                 if close <= 0 and yf_data['close'] >= min_valid: close = yf_data['close']
             
             oi = t.callOpenInterest if right == 'C' else t.putOpenInterest
             if not oi and t.futuresOpenInterest: oi = t.futuresOpenInterest

             # Filter out stale last/close values if they violate intrinsic value
             if last > 0 and last < min_valid: last = 0.0
             if close > 0 and close < min_valid: close = 0.0

             # [FIX] Robust Mid calculation: prefer (bid+ask)/2 if valid, then last, then model, then close
             if bid > 0 and ask > 0 and ((bid + ask) / 2) >= min_valid:
                 mid_p = (bid + ask) / 2
             elif last >= min_valid and last > 0:
                 mid_p = last
             elif model_p >= min_valid and model_p > 0:
                 mid_p = model_p
             elif close >= min_valid and close > 0:
                 mid_p = close
             else:
                 mid_p = 0.0

             price_for_validation = mid_p
             if price_for_validation <= 0: continue

             # Fallback voor bid/ask: als TWS helemaal geen bid/ask of model_p ('delayed data') levert, gebruik mid_p (die bv. 'close' bevat)
             if bid <= 0 and mid_p > 0: bid = mid_p
             if ask <= 0 and mid_p > 0: ask = mid_p

             data_list.append({
                 'strike': strike,
                 'right': right,
                 'bid': bid,
                 'ask': ask,
                 'mid': mid_p,
                 'volume': t.volume or 0,
                 'openInterest': oi or 0,
                 'delta': greeks['delta'],
                 'gamma': greeks['gamma'],
                 'vega': greeks['vega'],
                 'theta': greeks['theta'],
                 'iv': greeks['iv'],
                 'opt_price': price_for_validation,
                 'und_price': greeks['und_price']
             })
             
        return pd.DataFrame(data_list)
    def get_scanner_data(self, scan_code='MOST_ACTIVE', instrument='STK', location='STK.US.MAJOR', rows=50):
        """
        Fetches top symbols from TWS Scanner.
        scan_code: 'MOST_ACTIVE', 'TOP_PERC_GAIN', 'HOT_BY_VOLUME', 'OPT_VOLUME_MOST_ACTIVE', etc.
        Returns: list of symbols (str)
        """
        if not self.is_connected():
            return []
            
        from ib_insync import ScannerSubscription
        
        sub = ScannerSubscription(
            instrument=instrument, 
            locationCode=location, 
            scanCode=scan_code,
            numberOfRows=rows
        )
        
        # TagValues can be used for filters (e.g. price > 10, vol > 1M), but keep simple for now
        # tag_values = [TagValue("marketCapAbove", "1000000000")]
        
        try:
            # reqScannerData returns a list of ScannerData objects immediately if available, 
            # or waits? Actually reqScannerData is blocking in ib_insync sync mode.
            # But we want 'snapshot'.
            
            # Note: reqScannerData return list of objects with .contractDetails.contract.symbol
            scan_data = self.ib.reqScannerData(sub)
            
            symbols = []
            for item in scan_data:
                # item is ScannerData(rank=0, contractDetails=..., distance=..., benchmark=..., projection=..., comboLeg=...)
                if item.contractDetails and item.contractDetails.contract:
                    symbols.append(item.contractDetails.contract.symbol)
                    
            # Remove duplicates while preserving order
            unique_symbols = list(dict.fromkeys(symbols))
            return unique_symbols
            
        except Exception as e:
            # print(f"[IBClient] Scanner Error: {e}")
            return []
            
    def place_strategy_order(self, symbol, expiry, right, strategy, strikes_dict, action, quantity, price=None, order_type='LMT'):
        """
        Intelligently places orders for any supported strategy (single or multi-leg).
        strikes_dict: {'strike_buy': 600, 'strike_sell': 610, ...}
        action: 'BUY' or 'SELL' for the OVERALL strategy.
        """
        self.last_error = ""
        if not self.is_connected():
            self.last_error = "Niet verbonden met TWS"
            return None
        
        from ib_insync import Option, Contract, Order, ComboLeg, TagValue
        
        def make_opt(strike, r=None):
            if not strike or strike <= 0: return None
            # Use right from params if provided, else from the outer scope
            r_val = r if r else right
            
            # Intelligently routing to CBOE for index options, SMART for others
            is_index = any(idx in symbol.upper() for idx in ['SPX', 'NDX', 'RUT', 'VIX', 'DAX'])
            primary_exchange = 'CBOE' if is_index else 'SMART'
            fallback_exchange = 'SMART' if is_index else 'CBOE'
            
            # HARDENED: Use keyword args and float casting
            c = Option(
                symbol=str(symbol), 
                lastTradeDateOrContractMonth=str(expiry), 
                strike=float(strike), 
                right=str(r_val), 
                exchange=primary_exchange, 
                multiplier='100', 
                currency='USD'
            )
            qualified = self.qualify_contract_safe(c)
            if not qualified:
                print(f"DEBUG_LOG: Qualification failed for {symbol} {expiry} {r_val} {strike} on {primary_exchange}. Retrying with fallback {fallback_exchange}...")
                c.exchange = fallback_exchange
                qualified = self.qualify_contract_safe(c)
                
            if not qualified:
                err_msg = f"Optiepoot met strike {strike} ({r_val}) voor {symbol} (expiratie {expiry}) bestaat niet of kon niet worden gekwalificeerd in TWS"
                print(f"DEBUG_LOG: {err_msg}")
                self.last_error = err_msg
            return qualified

        # 1. Build Legs based on Strategy
        legs_data = [] # List of (contract, action)
        
        if strategy in ['LongCall', 'LongPut']:
            c = make_opt(strikes_dict.get('strike_buy', 0))
            if c: legs_data.append((c, 'BUY'))
            
        elif strategy in ['BullCall', 'BullPut', 'BearCall', 'BearPut']:
            c_buy = make_opt(strikes_dict.get('strike_buy', 0))
            c_sell = make_opt(strikes_dict.get('strike_sell', 0))
            if c_buy and c_sell:
                legs_data.append((c_buy, 'BUY'))
                legs_data.append((c_sell, 'SELL'))
                
        elif strategy == 'Strangle':
            c_p = make_opt(strikes_dict.get('strike_p_buy', 0), 'P')
            c_c = make_opt(strikes_dict.get('strike_c_buy', 0), 'C')
            if c_p and c_c:
                legs_data.append((c_p, 'BUY'))
                legs_data.append((c_c, 'BUY'))
                
        elif strategy == 'IronCondor':
            cpb = make_opt(strikes_dict.get('strike_p_buy', 0), 'P')
            cps = make_opt(strikes_dict.get('strike_p_sell', 0), 'P')
            ccs = make_opt(strikes_dict.get('strike_c_sell', 0), 'C')
            ccb = make_opt(strikes_dict.get('strike_c_buy', 0), 'C')
            if all([cpb, cps, ccs, ccb]):
                legs_data.append((cpb, 'BUY'))
                legs_data.append((cps, 'SELL'))
                legs_data.append((ccs, 'SELL'))
                legs_data.append((ccb, 'BUY'))

        if not legs_data:
            print(f"DEBUG_LOG: Error building legs for {strategy}")
            return None

        # 2. Construct Order
        algo_strategy = ""
        algo_params = []
        is_lmt = False
        if 'Adaptive' in order_type:
            priority = order_type.split('-')[1].strip()
            algo_strategy = 'Adaptive'
            algo_params = [TagValue('adaptivePriority', priority)]
            # Underlying order is still Limit, it just uses the algo engine. Max cap is the lmtPrice
            is_lmt = True
            order_type_str = 'LMT'
        elif order_type.startswith('LMT'):
            is_lmt = True
            order_type_str = 'LMT'
        else:
            order_type_str = order_type
            is_lmt = (order_type_str == 'LMT')
            
        if len(legs_data) == 1:
            # Single Leg
            contract, leg_action = legs_data[0]
            order = Order(
                action=action, 
                totalQuantity=quantity,
                orderType=order_type_str,
                lmtPrice=price if is_lmt else None,
                tif='DAY',
                outsideRth=True,
                transmit=True
            )
            if algo_strategy:
                order.algoStrategy = algo_strategy
                order.algoParams = algo_params
            print(f"DEBUG_LOG: Placing single leg order: {action} {quantity} {contract.localSymbol} (Algo: {algo_strategy})")
            trade = self.ib.placeOrder(contract, order)
        else:
            # Multi Leg (BAG)
            combo_legs = []
            for c, leg_act in legs_data:
                combo_legs.append(ComboLeg(conId=c.conId, ratio=1, action=leg_act, exchange='SMART'))
            
            bag = Contract(symbol=symbol, secType='BAG', currency='USD', exchange='SMART', comboLegs=combo_legs)
            order = Order(
                action=action,
                totalQuantity=quantity,
                orderType=order_type_str,
                lmtPrice=price if is_lmt else None,
                transmit=True,
                tif='DAY',
                outsideRth=True
            )
            if algo_strategy:
                order.algoStrategy = algo_strategy
                order.algoParams = algo_params
            print(f"DEBUG_LOG: Placing BAG order ({len(legs_data)} legs): {action} {quantity} combo...")
            trade = self.ib.placeOrder(bag, order)

        # 3. Wait for Submit
        import time
        start_wait = time.time()
        while trade.orderStatus.status in ('PendingSubmit', 'PreSubmitted') and not trade.isDone():
            self.ib.sleep(0.2)
            if time.time() - start_wait > 3.0: break
                
        return trade

    def get_open_orders(self):
        """
        Fetches all open orders.
        Returns a DataFrame with columns: [permId, clientId, orderId, account, symbol, secType, exchange, action, orderType, totalQuantity, cashQty, lmtPrice, auxPrice, status]
        """
        if not self.is_connected():
            return pd.DataFrame()
        
        orders = self.ib.reqOpenOrders()
        # Note: reqOpenOrders returns a list of Order objects but sometimes we need to wait for `openOrder` events?
        # ib_insync `reqOpenOrders` blocks until all orders are received in sync mode?
        # Actually in sync mode it returns list.
        
        data = []
        for o in orders:
            # o is an Order object? No, reqOpenOrders returns list of trades? or Orders?
            # ib_insync docs: reqOpenOrders() returns [Order]... wait, no.
            # It returns a list of *orders*.
            # But we usually want the Trade object which has contract + order + orderStatus.
            # `ib.openTrades()` returns a list of Trade objects for open orders.
            pass

        # Better to use ib.openTrades() or ib.reqAllOpenOrders()
        # ib.reqOpenOrders() refreshes the `ib.orders` list?
        
        # Simplest:
        self.ib.reqAllOpenOrders() 
        self.ib.sleep(0.5) # Give TWS a moment to send all orders
        trades = self.ib.openTrades()
        # returns list of Trade
        
        for t in trades:
            # t is a Trade(contract, order, orderStatus, fills, log)
            c = t.contract
            o = t.order
            s = t.orderStatus
            
            data.append({
                'symbol': c.symbol,
                'action': o.action,
                'quantity': o.totalQuantity,
                'status': s.status,
                'filled': s.filled,
                'remaining': s.remaining,
                'avgFillPrice': s.avgFillPrice,
                'lmtPrice': o.lmtPrice,
                'id': o.orderId
            })
            
        return pd.DataFrame(data)

    def get_dividend_info(self, symbol):
        import yfinance as yf
        import datetime
        
        yf_sym = symbol
        if yf_sym == 'SPX': yf_sym = '^SPX'
        if yf_sym == 'UNA': yf_sym = 'UNA.AS'
        if yf_sym == 'RDSA': yf_sym = 'SHELL.AS'
        
        info_dict = {
            'dividend_yield': 0.0,
            'dividend_rate': 0.0,
            'ex_div_date': None,
            'pay_date': None
        }
        
        try:
            ticker = yf.Ticker(yf_sym)
            info = ticker.info
            
            y_val = info.get('dividendYield', 0.0)
            info_dict['dividend_yield'] = y_val if y_val else 0.0
            
            r_val = info.get('dividendRate', 0.0)
            info_dict['dividend_rate'] = r_val if r_val else 0.0
            
            ex_div = info.get('exDividendDate')
            if ex_div:
                info_dict['ex_div_date'] = pd.to_datetime(ex_div, unit='s').date()
            else:
                # Try calendar fallback for ex-dividend
                cal = getattr(ticker, 'calendar', {})
                if isinstance(cal, dict) and 'Ex-Dividend Date' in cal:
                    info_dict['ex_div_date'] = cal['Ex-Dividend Date']
        except Exception as e:
            # print(f"[IBClient] Error fetching dividend for {symbol}: {e}")
            pass
            
        return info_dict
