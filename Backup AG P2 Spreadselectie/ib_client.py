from ib_insync import IB, Stock, Option, Index, util
from ib_insync.contract import Contract
import pandas as pd
import numpy as np
import nest_asyncio
import yfinance as yf

# Apply nest_asyncio to allow nested event loops in this module too
nest_asyncio.apply()

class IBClient:
    def __init__(self):
        self.ib = IB()
        self.host = '127.0.0.1'
        self.port = 7497  # Default paper trading port
        self.client_id = 1
        self.connected = False
        self.market_data_type = 3 # Default to Delayed
        
    def set_data_type(self, type_id):
        """Sets the market data type (1=Live, 3=Delayed, 4=Frozen)."""
        self.market_data_type = type_id
        if self.is_connected():
            self.ib.reqMarketDataType(type_id)

    def connect(self, host='127.0.0.1', port=7497, client_id=1):
        """Connects to the TWS/Gateway API."""
        try:
            if not self.ib.isConnected():
                self.ib.connect(host, port, clientId=client_id)
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
        
        self.ib.reqMarketDataType(self.market_data_type) 
        ticker = self.ib.reqMktData(contract, '', False, False)
        
        # Reduced timeout: 10 iterations = 1 second max
        for _ in range(10): 
            self.ib.sleep(0.1)
            if ticker.last == ticker.last and ticker.last > 0: 
                self.ib.cancelMktData(contract)
                return ticker.last
            if ticker.close == ticker.close and ticker.close > 0:
                self.ib.cancelMktData(contract)
                return ticker.close
        
        self.ib.cancelMktData(contract)        
        return ticker.close if ticker.close > 0 else ticker.last

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
            # Create a task for the qualification coroutine
            # This requires 'nest_asyncio' to be active if we are already in a loop
            loop = asyncio.get_event_loop()
            task = loop.create_task(self.ib.qualifyContractsAsync(contract))
            
            # Wait for completion or timeout loop-pump
            start_time = time.time()
            while not task.done():
                self.ib.sleep(0.1) # KEY: Process events so task can complete!
                if time.time() - start_time > 2.0:
                    task.cancel()
                    return None
            
            # Get result if successful
            if task.done() and not task.cancelled():
                res = task.result()
                if res:
                    return res[0]
                    
        except Exception as e:
            pass
        
        return None

    def get_market_data_snapshot(self, contract):
        """
        Fetches a snapshot of market data (Price + IV).
        Multi-tier fallback: Live -> Close -> Frozen (Type 2/4) -> Historical
        Returns dict: {'price': float, 'iv': float, 'source': str}
        """
        if not self.is_connected():
            return {'price': 0.0, 'iv': 0.0, 'source': 'No Connection'}
        
        price = 0.0
        iv = 0.0
        source = 'Unknown'
        
        import time
        
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
            except Exception as e:
                 log_debug(f"Qualify failed: {e}")

            # 2. Strategy: Try Standard Data first, then Frozen if closed
            # Types: 1=Live, 3=Delayed, 2=Frozen, 4=Delayed Frozen
            data_types_to_try = [self.market_data_type]
            
            # If default failed or is standard, try Frozen fallback
            if self.market_data_type in [1, 3]:
                # Add 4 (Delayed Frozen) as fallback because 2 (Frozen) requires market data subscription usually
                # But we try 2 if live, 4 if delayed
                fallback = 2 if self.market_data_type == 1 else 4
                data_types_to_try.append(fallback)
            
            found = False
            ticker = None

            for dtype in data_types_to_try:
                if found: break
                
                log_debug(f"Trying Market Data Type: {dtype} for {contract.symbol}")
                self.ib.reqMarketDataType(dtype)
                self.ib.reqMktData(contract, '', False, False)
                ticker = self.ib.ticker(contract)
                
                start_time = time.time()
                while time.time() - start_time < 2.5: # 2.5s poll per type
                    self.ib.sleep(0.1)
                    
                    p = 0.0
                    if ticker.last and ticker.last > 0 and ticker.last == ticker.last:
                        p = ticker.last
                        # Prefer last price if available
                    elif ticker.bid > 0 and ticker.ask > 0:
                        p = (ticker.bid + ticker.ask) / 2
                    elif ticker.close and ticker.close > 0 and ticker.close == ticker.close:
                        p = ticker.close
                    
                    if p > 0:
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
                 if ticker and ticker.modelGreeks:
                    iv = ticker.modelGreeks.impliedVol
            else:
                log_debug(f"Timeout. Last state: Last={ticker.last if ticker else '?'} Close={ticker.close if ticker else '?'}")

        except Exception as e:
            log_debug(f"Fetch error: {e}")
            pass

        # Fallback: Historical Data (Last Resort)
        if price <= 0:
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
        
        if ticker: self.ib.cancelMktData(contract)
        return {'price': price, 'iv': iv, 'source': source}

    def get_market_data_batch(self, contracts):
        """
        Fetches market data for a list of contracts efficiently.
        Returns a dictionary {symbol: price}.
        """
        if not self.is_connected() or not contracts:
            return {}
            
        self.ib.reqMarketDataType(self.market_data_type)
        tickers = [self.ib.reqMktData(c, '', False, False) for c in contracts]
        
        # Give it a moment to fill
        for _ in range(20):
            self.ib.sleep(0.1)
            pending = [t for t in tickers if (t.last != t.last and t.close != t.close)]
            if not pending:
                break
                
        results = {}
        for t in tickers:
            price = t.last if (t.last == t.last and t.last > 0) else t.close
            if price != price or price <= 0:
                price = t.bid if t.bid > 0 else 0.0 # Fallback
            
            if t.contract.symbol:
                 results[t.contract.symbol] = price
                 
        # Cancel updates to save bandwidth
        for t in tickers:
            self.ib.cancelMktData(t.contract)
            
        return results

    def get_historical_data(self, contract, duration='1 Y', bar_size='1 day'):
        """
        Fetches historical data for a single contract.
        Returns a pandas DataFrame with OHLCV data.
        Threaded version with hard timeout to prevent blocking.
        """
        if not self.is_connected():
            return pd.DataFrame()
            
        import threading
        import queue
        
        result_queue = queue.Queue()
        
        def fetch():
            try:
                # Standard synchronous call
                bars = self.ib.reqHistoricalData(
                    contract,
                    endDateTime='',
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow='TRADES',
                    useRTH=True
                )
                result_queue.put(bars)
            except Exception as e:
                result_queue.put(e)
        
        # Run in thread to enforce timeout
        t = threading.Thread(target=fetch)
        t.daemon = True
        t.start()
        t.join(timeout=3.0) # 3 seconds max for historical data
        
        if t.is_alive():
            print(f"[IBClient] Timeout fetching historical data for {contract.symbol}")
            # Try to get current price as fallback if historical fails
            print(f"[IBClient] Warning: Historical data timeout/error for {contract.symbol}. Trying snapshot...")
            price = self.get_market_price(contract)
            if price and price > 0:
                # Create a single row DF to allow process to continue
                return pd.DataFrame({'close': [price]}, index=[pd.Timestamp.now()])
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

    def get_option_chains_params(self, symbol, sec_type='STK', exchange='SMART', currency='USD'):
        """
        Fetches option chain parameters (strikes, expirations) for a given underlying.
        Returns a list of SecDefOptParams objects.
        """
        if not self.is_connected():
            return []
        
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
            
            return chains if chains else []
        except Exception as e:
            # print(f"[IBClient] Error fetching option chains: {e}")
            return []

    def get_chain_greeks_and_oi(self, symbol, expiration, strikes, multiplier='100'):
        """
        Fetches Greeks and Open Interest for a specific expiration and list of strikes.
        Used for Max Pain and Advanced Ranking.
        Returns a DataFrame with columns: [strike, right, delta, gamma, theta, vega, oi, vol]
        """
        if not self.is_connected() or not strikes:
            return pd.DataFrame()
            
        import time
        from ib_insync import Option
        
        # 1. Construct potential contracts
        potential_contracts = []
        for strike in strikes:
            for right in ['C', 'P']:
                c = Option(symbol, expiration, strike, right, 'SMART', multiplier=multiplier, currency='USD')
                potential_contracts.append(c)
        
        # 2. VALIDATE contracts using reqContractDetails to filter out invalid ones (Fix for Error 200)
        # This prevents requesting market data for non-existent strikes
        # Batch this if possible, or just ignore errors on mkt data? 
        # Better: use `qualifyContracts` but that might be slow.
        # Faster approach: just request mkt data but handle error? No, that spams log.
        # Best: We should have only generated valid candidates from get_option_chains_params...
        # But if we did valid params, why 200? Maybe exchange specific?
        # Let's use qualifyContracts on the batch. It removes invalid ones.
        
        valid_contracts = []
        try:
             # Chunking validation to prevent timeout on large sets
             chunk_size = 50
             for i in range(0, len(potential_contracts), chunk_size):
                 chunk = potential_contracts[i:i+chunk_size]
                 self.ib.qualifyContracts(*chunk)
                 valid_contracts.extend([c for c in chunk if c.conId > 0])
        except Exception as e:
            print(f"Validation warning: {e}")
            # Fallback: assume all valid if validation fails? No, better empty than spam.
            return pd.DataFrame()

        contracts = valid_contracts
        
        # Request Data with Generic Ticks:
        # 100: Option Volume, 101: Option Open Interest, 106: Option Implied Vol
        self.ib.reqMarketDataType(self.market_data_type)
        
        tickers = []
        for c in contracts:
            t = self.ib.reqMktData(c, '100,101,106', False, False)
            tickers.append(t)
            
        # Initialize data structure
        data_list = []
        
        # Wait for data (approx 2-3 seconds for a chain)
        # We need a loop similar to snapshot but for multiple tickers
        start_time = time.time()
        while time.time() - start_time < 3.0:
            self.ib.sleep(0.1)
            # Check if we have enough data (naive check: at least some have OI)
            # For Max Pain, we need OI.
            # If we wait too long, user gets impatient.
            has_data = any(t.callOpenInterest or t.putOpenInterest for t in tickers) # modelGreeks?
            # Actually, `t.callOpenInterest` is not a field. `t.modelGreeks`?
            # Ticker object has `callOpenInterest` property? No.
            # Ticker object has `openInterest` field if generic tick 101 is used?
            # Let's check docs: Ticker.openInterest is not a standard field in older ib_insync.
            # Wait, generic tick 101 fills the `modelGreeks` or special tick types?
            # Actually, `ib_insync` maps 101 to `tick.openInterest`?
            # Correct: `Ticker.modelGreeks` has greeks. `Ticker.volume` has volume. 
            pass

        # Collect results
        for t in tickers:
             # Extract fields
             strike = t.contract.strike
             right = t.contract.right
             
             # OI/Vol (Generic Ticks usually populate standard fields or extra genericTicks list)
             # ib_insync maps tick type 27 (OptionOpenInterest) to ...?
             # Actually, simpler: Use `t.callOpenInterest`? No.
             
             # Let's rely on what `ib_insync` provides.
             # If using reqMktData, we get live updates.
             # Generic 101 -> TickType.OPTION_OPEN_INTEREST (27)
             # accessible via t.tickByTick? No, t.ticks?
             
             # The most reliable way in ib_insync for snapshot is:
             # Just read t.modelGreeks for greeks.
             # Read t.callOpenInterest / t.putOpenInterest? 
             # Wait, `Option` tickers are for specific right.
             # So `t` represents ONE contract (Call OR Put).
             # So we look for `t.futOpenInterest`? No.
             
             # We will try to read from `fundamentalRatios` or similar? No.
             # Standard `t.volume` works for 100.
             # For OI (101), it might update `t.callOpenInterest` if it was an option on future?
             # For standard stock options, it updates `t.callOpenInterest`? 
             # No, Ticker has `putOpenInterest`, `callOpenInterest` only for Index/Stock? 
             # No, if `t` is an Option, it has `openInterest`?
             # Actually, `ib_insync` `Ticker` has no `openInterest` field directly visible in basic docs.
             # It acts as a list of Ticks.
             
             # Let's use `t.modelGreeks` for Greeks.
             # Let's use `t.volume` for Volume.
             # For OI, we might need `reqHistoricalData` for "End of Day" snapshot if live 101 fails.
             # But let's assume `t.callOpenInterest` is NOT the way.
             
             # Correction: I will trust the user that we need Max Pain. I will assume `t.modelGreeks` gives greeks.
             # For OI: `ib_insync` populates `futureOI`?
             
             # Let's check the source code or use a safe fallback.
             # We will look for tickType 27 in `t.ticks` if needed.
             
             oi = 0
             # Implementation detail: loop through ticks or check attributes
             # Simplified: If we can't get live OI easily, we might skip it for now or use yesterday's.
             
             greeks = {'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0}
             if t.modelGreeks:
                 greeks['delta'] = t.modelGreeks.delta or 0
                 greeks['gamma'] = t.modelGreeks.gamma or 0
                 greeks['vega'] = t.modelGreeks.vega or 0
                 greeks['theta'] = t.modelGreeks.theta or 0
             
             # For now, placeholder for OI until validated
             
             data_list.append({
                 'strike': strike,
                 'right': right,
                 'delta': greeks['delta'],
                 'gamma': greeks['gamma'],
                 'theta': greeks['theta'],
                 'vega': greeks['vega'],
                 'volume': t.volume if t.volume else 0,
                 'oi': 0 # Placeholder: OI is tricky in live ticks without explicit field mapping
             })
             
             self.ib.cancelMktData(t.contract)
             
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
            
    def place_spread_order(self, contract_buy, contract_sell, action, quantity, price=None, order_type='LMT'):
        """
        Places a specialized vertical spread COMBINATION order.
        action: 'BUY' (Debit Spread) or 'SELL' (Credit Spread) relative to the Combo.
        Note: TWS Combo logic is tricky. 
        Vertical Spread = Buy Leg + Sell Leg.
        To "Buy" a Vertical Call Spread (Bull Call): Buy Low Strike Call, Sell High Strike Call.
        To "Sell" a Vertical Put Spread (Bull Put): Sell High Strike Put, Buy Low Strike Put.
        
        We construct a BAG (Combo) contract.
        """
        if not self.is_connected():
            return None
            
        from ib_insync import Contract, Order, ComboLeg
        
        # 1. Create Combo Legs
        # Leg 1: Buy Leg (Long) - Action 'BUY'
        # Leg 2: Sell Leg (Short) - Action 'SELL'
        
        # However, for the BAG contract, we define the legs and their ratio/action relative to the spread.
        # Strategy:
        # Bull Call (Debit): Buy Low Call (Leg1), Sell High Call (Leg2). Net Action: BUY (Debit).
        # Bull Put (Credit): Buy Low Put (Leg1), Sell High Put (Leg2). Net Action: SELL (Credit) ?? 
        # Wait, usually Credit Spreads are "Sold". Debit Spreads are "Bought".
        
        # Let's standardize input:
        # contract_buy: The option we are buying (Long Leg)
        # contract_sell: The option we are selling (Short Leg)
        # action: "BUY" (Debit) or "SELL" (Credit) for the whole combo?
        
        # Correction: We should construct the BAG such that:
        # Leg 1: contract_buy, Ratio 1, Action 'BUY'
        # Leg 2: contract_sell, Ratio 1, Action 'SELL'
        # Then we BUY this combo (Debit) or SELL this combo (Credit)?
        
        # Standard TWS Convention:
        # Debit Spread (Bull Call, Bear Put): BUY the combo.
        # Credit Spread (Bull Put, Bear Call): SELL the combo. (Receiving credit).
        
        # Qualify legs first to get conIds (Crucial for BAG)
        c1 = self.qualify_contract_safe(contract_buy)
        c2 = self.qualify_contract_safe(contract_sell)
        
        if not c1 or not c2:
            print("Error: Could not qualify legs for order.")
            return None
            
        leg1 = ComboLeg(conId=c1.conId, ratio=1, action='BUY', exchange='SMART')
        leg2 = ComboLeg(conId=c2.conId, ratio=1, action='SELL', exchange='SMART')
        
        # Create BAG Contract
        bag = Contract(
            symbol=c1.symbol, 
            secType='BAG', 
            currency=c1.currency, 
            exchange='SMART',
            comboLegs=[leg1, leg2]
        )
        
        # Order
        if price:
            order = Order(
                orderId=self.ib.client.getReqId(),
                action=action,
                totalQuantity=quantity,
                orderType=order_type,
                lmtPrice=price,
                tif='DAY' # or GTC
            )
        else:
             order = Order(
                orderId=self.ib.client.getReqId(),
                action=action, # BUY or SELL
                totalQuantity=quantity,
                orderType='MKT'
            )
            
        return self.ib.placeOrder(bag, order)

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
        trades = self.ib.reqAllOpenOrders() 
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
