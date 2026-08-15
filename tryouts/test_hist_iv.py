import asyncio
from ib_insync import *
import pandas as pd

async def main():
    ib = IB()
    try:
        await ib.connectAsync('127.0.0.1', 7497, clientId=999)
    except Exception as e:
        print("Could not connect:", e)
        return

    contract = Stock('SPY', 'SMART', 'USD')
    ib.qualifyContracts(contract)

    ib.reqMarketDataType(3)

    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime='',
        durationStr='1 Y',
        barSizeSetting='1 day',
        whatToShow='OPTION_IMPLIED_VOLATILITY',
        useRTH=True
    )
    
    if bars:
        df = util.df(bars)
        print("Historical IV (first 5):")
        print(df.head())
        print("\nHistorical IV (last 5):")
        print(df.tail())
        print(f"\nMin IV: {df['close'].min()}, Max IV: {df['close'].max()}")
    else:
        print("No historical IV returned.")

    ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
