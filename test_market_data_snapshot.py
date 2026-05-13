import asyncio
from ib_client import IBClient
from ib_insync import Stock

async def test_snapshot():
    client = IBClient()
    connected, msg = client.connect('127.0.0.1', 7497, client_id=1234)
    if not connected:
        print("Failed to connect")
        return
        
    contract = Stock('CRL', 'SMART', 'USD')
    
    # Try fetching market data
    data = client.get_market_data_snapshot(contract, use_hist_fallback=False)
    print(f"Result for CRL: {data}")
    
    client.disconnect()

if __name__ == '__main__':
    asyncio.run(test_snapshot())
