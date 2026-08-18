import asyncio
import time
import datetime
import pandas as pd
from ib_insync import IB, Stock, util
import yfinance as yf

async def run_diag():
    ib = IB()
    try:
        print("\n--- YFINANCE DIAGNOSTICS ---")
        sym = 'SPY'
        try:
            print(f"Testing yf.download('{sym}', period='1mo')...")
            df = yf.download(sym, period='1mo', progress=False)
            print(f"Result: {len(df)} rows" if not df.empty else "Result: EMPTY")
            if not df.empty:
                print(df.tail(3))
        except Exception as e:
            print(f"yf.download Error: {e}")

        print("\n--- TWS DIAGNOSTICS ---")
        import random
        cid = random.randint(1000, 9999)
        await ib.connectAsync('127.0.0.1', 7497, clientId=cid)
        print(f"Connected to TWS (cid: {cid})")
        
        contract = Stock('SPY', 'SMART', 'USD')
        print(f"Qualifying contract: {contract}")
        qualified = await ib.qualifyContractsAsync(contract)
        if qualified:
            print(f"Qualified: {qualified[0]}")
            contract = qualified[0]
        else:
            print("Qualification FAILED")

        for dtype in [3, 4, 1]:
            print(f"\n--- Testing with MarketDataType: {dtype} ---")
            ib.reqMarketDataType(dtype)
            for show in ['TRADES', 'MIDPOINT', 'BID_ASK']:
                print(f"Testing TWS {show} (dtype {dtype})...")
                # Friday Feb 20, 2026 16:00:00
                end_time = '20260220 16:00:00 US/Eastern'
                try:
                    bars = await asyncio.wait_for(
                        ib.reqHistoricalDataAsync(
                            contract, endDateTime=end_time, durationStr='2 D',
                            barSizeSetting='1 day', whatToShow=show, useRTH=True
                        ),
                        timeout=30.0
                    )
                    if bars:
                        print(f"SUCCESS (dtype {dtype}): {len(bars)} bars found for {show}")
                        print(util.df(bars))
                        return # Stop if we find something
                    else:
                        print(f"EMPTY (dtype {dtype}) for {show}")
                except Exception as e:
                    print(f"ERROR (dtype {dtype}) for {show}: {e}")

        print("\n--- YFINANCE DIAGNOSTICS ---")
        sym = 'SPY'
        try:
            print(f"Testing yf.download('{sym}', period='1mo')...")
            df = yf.download(sym, period='1mo', progress=False)
            print(f"Result: {len(df)} rows" if not df.empty else "Result: EMPTY")
        except Exception as e:
            print(f"yf.download Error: {e}")

        try:
            print(f"Testing Ticker('{sym}').history(period='1mo')...")
            t = yf.Ticker(sym)
            df2 = t.history(period='1mo')
            print(f"Result: {len(df2)} rows" if not df2.empty else "Result: EMPTY")
        except Exception as e:
            print(f"Ticker.history Error: {e}")

    except Exception as e:
        print(f"Global Error: {e}")
    finally:
        ib.disconnect()

if __name__ == "__main__":
    asyncio.run(run_diag())
