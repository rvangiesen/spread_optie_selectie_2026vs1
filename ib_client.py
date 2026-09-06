import asyncio
import nest_asyncio
import logging
import yfinance as yf
import time
import pandas as pd
import numpy as np

# Python 3.14+ / Streamlit compatibility patch for asyncio.timeouts.Timeout inside nest_asyncio
try:
    from asyncio import timeouts
    _orig_aenter = timeouts.Timeout.__aenter__
    async def _patched_aenter(self):
        try:
            return await _orig_aenter(self)
        except RuntimeError as e:
            if 'Timeout should be used inside a task' in str(e):
                self._state = timeouts._State.ENTERED
                loop = asyncio.get_running_loop()
                async def _dummy(): pass
                self._task = loop.create_task(_dummy())
                self._cancelling = 0
                self.reschedule(self._when)
                return self
            raise
    timeouts.Timeout.__aenter__ = _patched_aenter
except Exception:
    pass

# Safe event loop policy for Python 3.14+
try:
    _pol = asyncio.get_event_loop_policy()
    _orig_get_loop = _pol.get_event_loop
    def _safe_get_loop():
        try:
            return _orig_get_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop
    _pol.get_event_loop = _safe_get_loop
except Exception:
    pass

# Apply nest_asyncio to allow nested event loops in this module too
nest_asyncio.apply()

from ib_insync import IB, Stock, Option, Index, util
from ib_insync.contract import Contract

# Suppress non-fatal IBKR warning logs (10091, 354, 200, 300, Unknown contract, etc.)
class IBErrorFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if any(f"{code}" in msg for code in ['10091', '354', '10167', '10089', ' 200,', '200: No security definition', 'Unknown contract', ' 300,', 'Can\'t find EId']):
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
        """Connects to the TWS/Gateway API with automatic client ID fallback and clear diagnostic messages."""
        try:
            if not self.ib.isConnected():
                import asyncio
                import random
                loop = util.getLoop()
                
                # Primary attempt
                target_cid = client_id if client_id and client_id > 0 else random.randint(1000, 9999)
                
                if loop.is_running():
                    async def _do_connect():
                        try:
                            await asyncio.wait_for(self.ib.connectAsync(host, port, clientId=target_cid, timeout=4.0), timeout=5.0)
                        except Exception:
                            # Retry with random fallback client ID if locked/busy
                            fb_cid = random.randint(10000, 99999)
                            await asyncio.wait_for(self.ib.connectAsync(host, port, clientId=fb_cid, timeout=4.0), timeout=5.0)
                    task = loop.create_task(_do_connect())
                    util.run(task)
                else:
                    try:
                        self.ib.connect(host, port, clientId=target_cid, timeout=4.0)
                    except Exception:
                        fb_cid = random.randint(10000, 99999)
                        self.ib.connect(host, port, clientId=fb_cid, timeout=4.0)
                        
                self.connected = True
                self.host = host
                self.port = port
                self.client_id = target_cid
            return True, "Connected successfully"
        except Exception as e:
            self.connected = False
            err_msg = str(e)
            if "TimeoutError" in err_msg or "timed out" in err_msg.lower() or "timeout" in err_msg.lower():
                return False, f"TWS API time-out op poort {port}. TWS reageert niet. Controleer of er een pop-up in TWS staat of herstart TWS."
            elif "ConnectionRefusedError" in err_msg or "10061" in err_msg:
                return False, f"Geen TWS API actief op poort {port}. Controleer of TWS open staat en API is ingeschakeld."
            return False, f"TWS Verbindingsfout ({host}:{port}): {err_msg}"

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
                elif symbol == 'RUT': symbol = '^RUT'
                elif symbol == 'DAX': symbol = '^GDAXI'
                elif symbol == 'DJI': symbol = '^DJI'
                
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

        # Fallback 2: yfinance (Automatic Fallback if TWS price is missing or 0)
        if price <= 0:
            try:
                symbol = contract.symbol
                if symbol == 'SPX': symbol = '^SPX'
                elif symbol == 'NDX': symbol = '^NDX'
                elif symbol == 'VIX': symbol = '^VIX'
                elif symbol == 'RUT': symbol = '^RUT'
                elif symbol == 'DAX': symbol = '^GDAXI'
                elif symbol == 'DJI': symbol = '^DJI'
                
                tk = yf.Ticker(symbol)
                df = tk.history(period="1d")
                if not df.empty:
                    price = float(df['Close'].iloc[-1])
                    if price > 0:
                        source = 'yfinance (Fallback)'
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
        Multi-tier fallback: yfinance (fastest) -> TWS -> Price Snapshot.
        Returns a pandas DataFrame with OHLCV data.
        """
        import time
        import yfinance as yf
        from ib_insync import util
        import pandas as pd

        # 1. Try yfinance FIRST for STK/IND/IDX (lightning fast ~0.1s, no TWS timeouts)
        if contract.secType in ['STK', 'IND', 'IDX']:
            import datetime
            symbol = contract.symbol
            if symbol == 'SPX': symbol = '^SPX'
            elif symbol == 'NDX': symbol = '^NDX'
            elif symbol == 'VIX': symbol = '^VIX'
            elif symbol == 'RUT': symbol = '^RUT'
            elif symbol == 'DAX': symbol = '^GDAXI'
            elif symbol == 'DJI': symbol = '^DJI'
            
            try:
                start_date = (datetime.datetime.now() - datetime.timedelta(days=400)).strftime('%Y-%m-%d')
                df = yf.download(
                    symbol, 
                    start=start_date,
                    interval='1d', 
                    progress=False, 
                    threads=False
                )
                
                if df is not None and not df.empty:
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0)
                    
                    df.columns = [c.lower() for c in df.columns]
                    
                    if 'adj close' in df.columns:
                        df = df.rename(columns={'adj close': 'adj_close'})
                    
                    if 'close' in df.columns:
                        df['close'] = df['close'].astype(float)
                        df = df.sort_index()
                        return df
            except Exception as e:
                print(f"[IBClient] yf error for {symbol}: {e}")

        # 2. Try TWS (Direct Async with Short Timeout)
        if self.is_connected():
            import asyncio
            qualified = self.qualify_contract_safe(contract)
            working_contract = qualified if qualified else contract
            
            self.ib.reqMarketDataType(3) 

            for show_type in ['TRADES', 'MIDPOINT']:
                try:
                    coro = self.ib.reqHistoricalDataAsync(
                        working_contract,
                        endDateTime='',
                        durationStr=duration,
                        barSizeSetting=bar_size,
                        whatToShow=show_type,
                        useRTH=True
                    )
                    task = asyncio.ensure_future(coro)
                    
                    start_wait = time.time()
                    while not task.done():
                        self.ib.sleep(0.05)
                        if time.time() - start_wait > 2.0: # 2s max timeout
                            task.cancel()
                            break
                    
                    if task.done() and not task.cancelled() and not task.exception():
                        bars = task.result()
                        if bars:
                            return util.df(bars)
                except Exception as e:
                    print(f"[IBClient] TWS Historical Error for {working_contract.symbol}: {e}")

        # 3. Final Fallback: Single Row Snapshot
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
                elif yf_sym == 'RUT': yf_sym = '^RUT'
                elif yf_sym == 'DAX': yf_sym = '^GDAXI'
                elif yf_sym == 'DJI': yf_sym = '^DJI'
                
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
            import pandas as pd
            from datetime import datetime
            import math
            try:
                yf_sym = symbol
                if yf_sym == 'SPX': yf_sym = '^SPX'
                elif yf_sym == 'NDX': yf_sym = '^NDX'
                elif yf_sym == 'VIX': yf_sym = '^VIX'
                elif yf_sym == 'RUT': yf_sym = '^RUT'
                elif yf_sym == 'DAX': yf_sym = '^GDAXI'
                elif yf_sym == 'DJI': yf_sym = '^DJI'
                
                tk = yf.Ticker(yf_sym)
                yf_exp = f"{expiration[:4]}-{expiration[4:6]}-{expiration[6:8]}"
                exps = tk.options
                if not exps:
                    return pd.DataFrame()
                if yf_exp not in exps:
                    yf_exp = min(exps, key=lambda d: abs((pd.to_datetime(d) - pd.to_datetime(yf_exp)).days))
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
                final_valid = []
                chunk_size = 100
                for i in range(0, len(target_contracts), chunk_size):
                    sub_chunk = target_contracts[i:i + chunk_size]
                    task = asyncio.ensure_future(self.ib.qualifyContractsAsync(*sub_chunk))
                    start_wait = time.time()
                    while not task.done():
                        self.ib.sleep(0.1)
                        if time.time() - start_wait > 10.0:
                            task.cancel()
                            break
                    if task.done() and not task.cancelled() and not task.exception():
                        res = task.result()
                        final_valid.extend([c for c in res if getattr(c, 'conId', 0) > 0])
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
            import pandas as pd
            yf_sym = symbol
            if yf_sym == 'SPX': yf_sym = '^SPX'
            elif yf_sym == 'NDX': yf_sym = '^NDX'
            elif yf_sym == 'VIX': yf_sym = '^VIX'
            elif yf_sym == 'RUT': yf_sym = '^RUT'
            elif yf_sym == 'DAX': yf_sym = '^GDAXI'
            elif yf_sym == 'DJI': yf_sym = '^DJI'
            ticker = yf.Ticker(yf_sym)
            yf_exp = f"{expiration[:4]}-{expiration[4:6]}-{expiration[6:8]}"
            exps = ticker.options
            if exps:
                if yf_exp not in exps:
                    yf_exp = min(exps, key=lambda d: abs((pd.to_datetime(d) - pd.to_datetime(yf_exp)).days))
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
            print(f"DEBUG_LOG: yfinance option price fallback unavailable for {symbol} {expiration}: {e}")

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
            
    def place_strategy_order(self, symbol, expiry, right, strategy, strikes_dict, action, quantity, price=None, order_type='LMT', enable_bracket=True, tp_pct=0.20, sl_pct=0.20, custom_tp_price=None, custom_sl_price=None):
        """
        Intelligently places orders for any supported strategy (single or multi-leg).
        Supports Take Profit and Stop Loss attached bracket orders (via % or custom $ price).
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

        # 1. Build Legs based on Strategy with explicit Option Right ('P' vs 'C')
        legs_data = [] # List of (contract, action)
        
        is_credit_strategy = strategy in ['BullPut', 'BearCall', 'IronCondor']

        if strategy == 'LongCall':
            c = make_opt(strikes_dict.get('strike_buy'), 'C')
            if c: legs_data.append((c, 'BUY'))
        elif strategy == 'LongPut':
            c = make_opt(strikes_dict.get('strike_buy'), 'P')
            if c: legs_data.append((c, 'BUY'))
        elif strategy == 'BullCall':
            c_buy = make_opt(strikes_dict.get('strike_buy'), 'C')
            c_sell = make_opt(strikes_dict.get('strike_sell'), 'C')
            if c_buy and c_sell:
                legs_data.append((c_buy, 'BUY'))
                legs_data.append((c_sell, 'SELL'))
        elif strategy == 'BearCall':
            c_sell = make_opt(strikes_dict.get('strike_sell'), 'C')
            c_buy = make_opt(strikes_dict.get('strike_buy'), 'C')
            if c_sell and c_buy:
                # Credit spread leg actions inside BAG:
                legs_data.append((c_sell, 'SELL'))
                legs_data.append((c_buy, 'BUY'))
        elif strategy == 'BullPut':
            s_sell = strikes_dict.get('strike_sell', 0)
            s_buy = strikes_dict.get('strike_buy', 0)
            if s_sell < s_buy: s_sell, s_buy = s_buy, s_sell # Ensure s_sell is higher strike
            
            p_sell = make_opt(s_sell, 'P')
            p_buy = make_opt(s_buy, 'P')
            if p_sell and p_buy:
                # Credit spread leg actions inside BAG:
                legs_data.append((p_sell, 'SELL'))
                legs_data.append((p_buy, 'BUY'))
        elif strategy == 'BearPut':
            p_buy = make_opt(strikes_dict.get('strike_buy'), 'P')
            p_sell = make_opt(strikes_dict.get('strike_sell'), 'P')
            if p_buy and p_sell:
                legs_data.append((p_buy, 'BUY'))
                legs_data.append((p_sell, 'SELL'))
        elif strategy == 'Strangle':
            p_buy = make_opt(strikes_dict.get('strike_p_buy'), 'P')
            c_buy = make_opt(strikes_dict.get('strike_c_buy'), 'C')
            if p_buy and c_buy:
                legs_data.append((p_buy, 'BUY'))
                legs_data.append((c_buy, 'BUY'))
        elif strategy == 'IronCondor':
            p_buy = make_opt(strikes_dict.get('strike_p_buy'), 'P')
            p_sell = make_opt(strikes_dict.get('strike_p_sell'), 'P')
            c_sell = make_opt(strikes_dict.get('strike_c_sell'), 'C')
            c_buy = make_opt(strikes_dict.get('strike_c_buy'), 'C')
            if p_buy and p_sell and c_sell and c_buy:
                legs_data.append((p_buy, 'BUY'))
                legs_data.append((p_sell, 'SELL'))
                legs_data.append((c_sell, 'SELL'))
                legs_data.append((c_buy, 'BUY'))

        if not legs_data:
            if not self.last_error:
                self.last_error = f"Kon optiebenen niet opbouwen voor {strategy}"
            return None

        # 2. Determine Order Action and Price Formatting
        # Standardize outer_action for TWS BAG API:
        # - Credit spreads: outer action MUST be 'SELL', limit price MUST be POSITIVE.
        # - Debit spreads & Long options: outer action MUST be 'BUY', limit price MUST be POSITIVE.
        outer_action = 'SELL' if is_credit_strategy else 'BUY'
        
        # Force limit price to positive float as required by TWS for BAG orders
        if price is not None:
            price = abs(float(price))

        can_bracket = enable_bracket and (price is not None and price > 0)
        parent_transmit = False if can_bracket else True

        # Parse Adaptive Algo parameters
        order_type_str = 'LMT'
        algo_strategy = None
        algo_params = []

        if 'Adaptive' in order_type:
            order_type_str = 'LMT'
            algo_strategy = 'Adaptive'
            priority = 'Normal'
            if 'Urgent' in order_type: priority = 'Urgent'
            elif 'Patient' in order_type: priority = 'Patient'
            algo_params = [TagValue('adaptivePriority', priority)]

        trade = None

        if len(legs_data) == 1:
            # Single Leg Option Order
            contract, leg_action = legs_data[0]
            order = Order(
                action=leg_action,
                totalQuantity=quantity,
                orderType=order_type_str,
                lmtPrice=price,
                transmit=parent_transmit,
                tif='DAY',
                outsideRth=True
            )
            if algo_strategy:
                order.algoStrategy = algo_strategy
                order.algoParams = algo_params
            print(f"DEBUG_LOG: Placing Single Leg order: {leg_action} {quantity} x {contract.symbol} {contract.strike}{contract.right} @ {price}...")
            trade = self.ib.placeOrder(contract, order)
            target_contract = contract

        else:
            # Multi-Leg Combo Order (BAG)
            combo_legs = []
            for c, leg_act in legs_data:
                combo_legs.append(ComboLeg(
                    conId=c.conId,
                    ratio=1,
                    action=leg_act,
                    exchange='SMART'
                ))
            
            bag = Contract(symbol=symbol, secType='BAG', currency='USD', exchange='SMART', comboLegs=combo_legs)
            target_contract = bag
            order = Order(
                action=outer_action,
                totalQuantity=quantity,
                orderType=order_type_str,
                lmtPrice=price,
                transmit=parent_transmit,
                tif='DAY',
                outsideRth=True
            )
            if algo_strategy:
                order.algoStrategy = algo_strategy
                order.algoParams = algo_params
            print(f"DEBUG_LOG: Placing BAG order ({len(legs_data)} legs): {outer_action} {quantity} combo @ {price}...")
            trade = self.ib.placeOrder(bag, order)

        # Attach Take Profit & Stop Loss Bracket Orders
        if can_bracket and trade and trade.order.orderId:
            parent_id = trade.order.orderId
            p_val = float(price)
            exit_action = 'BUY' if outer_action == 'SELL' else 'SELL'
            
            if custom_tp_price is not None and custom_sl_price is not None:
                tp_price = round(abs(float(custom_tp_price)), 2)
                sl_price = round(abs(float(custom_sl_price)), 2)
            else:
                if outer_action == 'BUY': # Debit / Long
                    tp_price = round(p_val * (1.0 + tp_pct), 2)
                    sl_price = round(p_val * (1.0 - sl_pct), 2)
                else: # Credit
                    tp_price = max(0.01, round(p_val * (1.0 - tp_pct), 2))
                    sl_price = round(p_val * (1.0 + sl_pct), 2)

            # 1. Take Profit Order
            tp_order = Order(
                action=exit_action,
                totalQuantity=quantity,
                orderType='LMT',
                lmtPrice=tp_price,
                parentId=parent_id,
                tif='DAY',
                outsideRth=True,
                transmit=False
            )
            print(f"DEBUG_LOG: Attaching Take Profit order: {exit_action} @ {tp_price} (parentId: {parent_id})")
            self.ib.placeOrder(target_contract, tp_order)

            # 2. Stop Loss Order - Transmits full bracket
            sl_order = Order(
                action=exit_action,
                totalQuantity=quantity,
                orderType='STP' if len(legs_data) == 1 else 'LMT',
                auxPrice=sl_price if len(legs_data) == 1 else None,
                lmtPrice=sl_price if len(legs_data) > 1 else None,
                parentId=parent_id,
                tif='DAY',
                outsideRth=True,
                transmit=True
            )
            print(f"DEBUG_LOG: Attaching Stop Loss order: {exit_action} @ {sl_price} (parentId: {parent_id})")
            self.ib.placeOrder(target_contract, sl_order)

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

    def get_account_portfolio_spreads(self):
        """
        Fetches open portfolio items from TWS, groups option contracts into
        Single Leg Options and Spreads (BullPut, BullCall, BearCall, BearPut, IronCondor),
        and returns a list of position dictionaries.
        """
        if not self.is_connected():
            return []

        import pandas as pd

        # Combine items from ib.portfolio() and ib.positions() instantly
        portfolio_items = list(self.ib.portfolio() or [])
        
        try:
            raw_positions = self.ib.positions()
            if raw_positions:
                existing_con_ids = {p.contract.conId for p in portfolio_items if p.contract and getattr(p.contract, 'conId', None)}
                for pos in raw_positions:
                    if pos.contract and pos.position != 0:
                        c_id = getattr(pos.contract, 'conId', 0)
                        if not c_id or c_id not in existing_con_ids:
                            class PosWrapper:
                                def __init__(self, p):
                                    self.contract = p.contract
                                    self.position = p.position
                                    self.marketPrice = 0.0
                                    self.marketValue = 0.0
                                    self.averageCost = getattr(p, 'avgCost', 0.0)
                                    self.unrealizedPNL = 0.0
                                    self.realizedPNL = 0.0
                                    self.account = getattr(p, 'account', '')
                            portfolio_items.append(PosWrapper(pos))
        except Exception as e:
            print(f"DEBUG_LOG: ib.positions() fetch error: {e}")

        if not portfolio_items:
            portfolio_items = []

        # Filter for option contracts (OPT and FOP) with non-zero position
        opt_items = [p for p in portfolio_items if p.contract and p.contract.secType in ['OPT', 'FOP'] and p.position != 0]

        if not opt_items:
            print(f"DEBUG_LOG: Total portfolio items: {len(portfolio_items)}, Option items: 0")
            return []

        # Group by (symbol, expiration)
        grouped = {}
        for item in opt_items:
            key = (item.contract.symbol, item.contract.lastTradeDateOrContractMonth)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(item)

        positions_list = []

        for (sym, expiry), items in grouped.items():
            # Calculate DTE
            try:
                exp_dt = pd.to_datetime(expiry)
                now_dt = pd.Timestamp.now().normalize()
                dte = (exp_dt - now_dt).days
            except Exception:
                dte = 30

            # Get underlying price
            from ib_insync import Stock
            und_price = self.get_market_price(Stock(symbol=sym, exchange='SMART', currency='USD'))
            if not und_price or und_price <= 0:
                snap = self.get_market_data_snapshot(Stock(symbol=sym, exchange='SMART', currency='USD'), use_yf=True)
                und_price = snap.get('price', 0.0)

            if len(items) == 1:
                # Single Leg Option (Long Call, Short Call, Long Put, Short Put)
                item = items[0]
                c = item.contract
                pos_qty = item.position
                right = c.right.upper()
                strike = float(c.strike)
                pnl_usd = float(item.unrealizedPNL or 0.0)
                avg_cost = float(item.averageCost or 0.0)
                mkt_price = float(item.marketPrice or 0.0)
                
                if right == 'C':
                    strat = "LongCall" if pos_qty > 0 else "ShortCall"
                else:
                    strat = "LongPut" if pos_qty > 0 else "ShortPut"
                
                total_cost = abs(avg_cost * pos_qty)
                pnl_pct = (pnl_usd / total_cost * 100.0) if total_cost > 0 else 0.0

                positions_list.append({
                    'symbol': sym,
                    'strategy': strat,
                    'expiry': expiry,
                    'dte': dte,
                    'qty': int(abs(pos_qty)),
                    'is_long': pos_qty > 0,
                    'strikes_str': f"{strike} {right}",
                    'sold_strike': strike if pos_qty < 0 else 0.0,
                    'bought_strike': strike if pos_qty > 0 else 0.0,
                    'right': right,
                    'market_price': mkt_price,
                    'entry_price': avg_cost / 100.0 if avg_cost > 0 else mkt_price,
                    'unrealized_pnl': pnl_usd,
                    'pnl_pct': pnl_pct,
                    'underlying_price': und_price,
                    'legs': [item]
                })

            elif len(items) == 2:
                # Vertical Spread (2 legs)
                p1, p2 = items[0], items[1]
                c1, c2 = p1.contract, p2.contract
                
                rights = {c1.right.upper(), c2.right.upper()}
                pnl_usd = float(p1.unrealizedPNL or 0.0) + float(p2.unrealizedPNL or 0.0)
                
                total_cost = abs(float(p1.averageCost or 0.0) * float(p1.position)) + abs(float(p2.averageCost or 0.0) * float(p2.position))
                pnl_pct = (pnl_usd / (total_cost / 2.0) * 100.0) if total_cost > 0 else 0.0
                
                qty = int(abs(p1.position))

                if len(rights) == 1 and 'P' in rights:
                    sell_item = p1 if p1.position < 0 else (p2 if p2.position < 0 else None)
                    buy_item = p1 if p1.position > 0 else (p2 if p2.position > 0 else None)
                    
                    sold_strike = float(sell_item.contract.strike) if sell_item else 0.0
                    bought_strike = float(buy_item.contract.strike) if buy_item else 0.0
                    
                    strat = "BullPut" if sold_strike > bought_strike else "BearPut"
                    strikes_str = f"{sold_strike}/{bought_strike} P"
                elif len(rights) == 1 and 'C' in rights:
                    sell_item = p1 if p1.position < 0 else (p2 if p2.position < 0 else None)
                    buy_item = p1 if p1.position > 0 else (p2 if p2.position > 0 else None)
                    
                    sold_strike = float(sell_item.contract.strike) if sell_item else 0.0
                    bought_strike = float(buy_item.contract.strike) if buy_item else 0.0
                    
                    strat = "BearCall" if sold_strike < bought_strike else "BullCall"
                    strikes_str = f"{sold_strike}/{bought_strike} C"
                else:
                    strat = "Strangle"
                    sold_strike = float(c1.strike)
                    bought_strike = float(c2.strike)
                    strikes_str = f"{c1.strike}/{c2.strike}"

                positions_list.append({
                    'symbol': sym,
                    'strategy': strat,
                    'expiry': expiry,
                    'dte': dte,
                    'qty': qty,
                    'is_long': False,
                    'strikes_str': strikes_str,
                    'sold_strike': sold_strike,
                    'bought_strike': bought_strike,
                    'right': list(rights)[0] if len(rights) == 1 else 'COMBO',
                    'market_price': (float(p1.marketPrice or 0) + float(p2.marketPrice or 0)) / 2.0,
                    'entry_price': (float(p1.averageCost or 0) + float(p2.averageCost or 0)) / 200.0,
                    'unrealized_pnl': pnl_usd,
                    'pnl_pct': pnl_pct,
                    'underlying_price': und_price,
                    'legs': [p1, p2]
                })

            else:
                pnl_usd = sum([float(i.unrealizedPNL or 0.0) for i in items])
                qty = int(abs(items[0].position))
                positions_list.append({
                    'symbol': sym,
                    'strategy': 'IronCondor',
                    'expiry': expiry,
                    'dte': dte,
                    'qty': qty,
                    'is_long': False,
                    'strikes_str': f"Multi-Leg ({len(items)} legs)",
                    'sold_strike': 0.0,
                    'bought_strike': 0.0,
                    'right': 'COMBO',
                    'market_price': 0.0,
                    'entry_price': 0.0,
                    'unrealized_pnl': pnl_usd,
                    'pnl_pct': 0.0,
                    'underlying_price': und_price,
                    'legs': items
                })

        return positions_list

    def execute_portfolio_adjustments(self, approved_actions):
        """
        Executes approved exit/management actions in TWS for portfolio positions.
        Generates and places real orders in TWS for closing, rolling, or iron condor expansion.
        """
        if not self.is_connected() or not approved_actions:
            return []

        from ib_insync import Option, MarketOrder, LimitOrder, Contract, ComboLeg
        import pandas as pd

        results = []

        for action in approved_actions:
            sym = action.get('symbol')
            code = action.get('selected_action', action.get('action_code', ''))
            legs = action.get('legs', [])
            qty = action.get('qty', 1)

            if not legs:
                results.append({'symbol': sym, 'status': 'SKIPPED', 'message': f"Geen optiebenen gevonden voor {sym}."})
                continue

            # --- CASE 1: SLUITEN / WINST BORGEN / STOP LOSS ---
            if any(k in code for k in ['TIJDIG_SLUITEN', 'WINST_BORGEN', 'Direct Sluiten', 'Winst Borgen', 'Stop-Loss']):
                closed_count = 0
                for item in legs:
                    c = item.contract
                    pos_qty = item.position
                    if pos_qty == 0:
                        continue
                    
                    q_contract = self.qualify_contract_safe(c) or c
                    close_action = 'BUY' if pos_qty < 0 else 'SELL'
                    close_qty = int(abs(pos_qty))
                    
                    order = MarketOrder(action=close_action, totalQuantity=close_qty)
                    trade = self.ib.placeOrder(q_contract, order)
                    closed_count += 1
                
                results.append({
                    'symbol': sym,
                    'status': 'SUCCESS',
                    'message': f"Sluitingsorder verstuurd naar TWS ({closed_count} leg(s) gesloten)."
                })

            # --- CASE 2: DOORROLLEN (ROLLING) ---
            elif any(k in code for k in ['Doorrollen', 'DOORROLLEN', 'roll']):
                placed_orders = 0
                
                # 1. Close current legs
                for item in legs:
                    c = item.contract
                    pos_qty = item.position
                    if pos_qty == 0: continue
                    q_c = self.qualify_contract_safe(c) or c
                    close_act = 'BUY' if pos_qty < 0 else 'SELL'
                    order = MarketOrder(action=close_act, totalQuantity=int(abs(pos_qty)))
                    self.ib.placeOrder(q_c, order)
                    placed_orders += 1

                # 2. Open new legs in next month (+30 days DTE)
                curr_exp = legs[0].contract.lastTradeDateOrContractMonth
                try:
                    exp_dt = pd.to_datetime(curr_exp)
                    next_exp_target = (exp_dt + pd.Timedelta(days=30)).strftime("%Y%m%d")
                except Exception:
                    next_exp_target = (pd.Timestamp.now() + pd.Timedelta(days=60)).strftime("%Y%m%d")

                # Get available expirations for symbol
                chains = self.get_option_chains_params(sym)
                next_exp = None
                if chains and hasattr(chains, 'expirations') and chains.expirations:
                    exps = sorted([e for e in chains.expirations if e > curr_exp])
                    if exps:
                        next_exp = exps[0]
                
                if not next_exp:
                    next_exp = next_exp_target

                # Open new legs for next expiration
                for item in legs:
                    c = item.contract
                    pos_qty = item.position
                    if pos_qty == 0: continue
                    
                    new_opt = Option(
                        symbol=sym,
                        lastTradeDateOrContractMonth=str(next_exp),
                        strike=float(c.strike),
                        right=str(c.right),
                        exchange=c.exchange or 'SMART',
                        currency=c.currency or 'USD',
                        multiplier=c.multiplier or '100'
                    )
                    q_new_opt = self.qualify_contract_safe(new_opt) or new_opt
                    open_act = 'SELL' if pos_qty < 0 else 'BUY'
                    order = MarketOrder(action=open_act, totalQuantity=int(abs(pos_qty)))
                    self.ib.placeOrder(q_new_opt, order)
                    placed_orders += 1

                results.append({
                    'symbol': sym,
                    'status': 'SUCCESS',
                    'message': f"Doorrol-orders verstuurd naar TWS ({placed_orders} benen: oude gesloten, nieuwe geopend op {next_exp})."
                })

            # --- CASE 3: OMZETTEN NAAR IRON CONDOR ---
            elif any(k in code for k in ['Iron Condor', 'OMZETTEN']):
                placed_orders = 0
                curr_exp = legs[0].contract.lastTradeDateOrContractMonth
                
                rights = {item.contract.right.upper() for item in legs}
                
                if 'P' in rights:
                    sold_put = max([float(i.contract.strike) for i in legs])
                    call_sold_strike = round(sold_put * 1.05, 1)
                    call_bought_strike = round(sold_put * 1.075, 1)
                    
                    c_short = Option(symbol=sym, lastTradeDateOrContractMonth=str(curr_exp), strike=call_sold_strike, right='C', exchange='SMART', currency='USD', multiplier='100')
                    c_long = Option(symbol=sym, lastTradeDateOrContractMonth=str(curr_exp), strike=call_bought_strike, right='C', exchange='SMART', currency='USD', multiplier='100')
                    
                    q_short = self.qualify_contract_safe(c_short) or c_short
                    q_long = self.qualify_contract_safe(c_long) or c_long
                    
                    self.ib.placeOrder(q_short, MarketOrder(action='SELL', totalQuantity=qty))
                    self.ib.placeOrder(q_long, MarketOrder(action='BUY', totalQuantity=qty))
                    placed_orders += 2

                results.append({
                    'symbol': sym,
                    'status': 'SUCCESS' if placed_orders > 0 else 'WARNING',
                    'message': f"Iron Condor uitbreidingsorders ({placed_orders} benen) verstuurd naar TWS."
                })

            else:
                results.append({
                    'symbol': sym,
                    'status': 'SKIPPED',
                    'message': f"Positie gehandhaafd (Geen actie vereist)."
                })

        # Ensure event loop flushes all placed orders over the socket to TWS
        if self.is_connected():
            self.ib.sleep(1.5)

        return results


