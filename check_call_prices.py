from ib_insync import IB, Option
import nest_asyncio
nest_asyncio.apply()

ib = IB()
try:
    ib.connect('127.0.0.1', 7497, clientId=99)
    print("Connected")
    
    strikes = [537.5, 542.5, 552.5]
    for s in strikes:
        c = Option(symbol='SMH', lastTradeDateOrContractMonth='20260710', strike=s, right='C', exchange='SMART', currency='USD')
        qualified = ib.qualifyContracts(c)
        if not qualified:
            print(f"Failed to qualify SMH 20260710 C {s}")
            continue
        c = qualified[0]
        # Request market data snapshot
        ib.reqMarketDataType(3)  # Delayed
        ticker = ib.reqMktData(c, '', False, False)
        ib.sleep(2.0)
        print(f"\nStrike {s} C:")
        print(f"  Bid: {ticker.bid}, Ask: {ticker.ask}, Last: {ticker.last}, Close: {ticker.close}")
        mid = (ticker.bid + ticker.ask) / 2 if (ticker.bid > 0 and ticker.ask > 0) else getattr(ticker, 'close', 0.0)
        print(f"  Calculated Mid: {mid}")
        
finally:
    ib.disconnect()
