"""
Test script to diagnose market data retrieval issues
"""
from ib_insync import *
import nest_asyncio
import time

nest_asyncio.apply()

# Connect to TWS
ib = IB()
try:
    ib.connect('127.0.0.1', 7497, clientId=9999)
    print("[OK] Connected to TWS")
    
    # Create SPY contract
    contract = Stock('SPY', 'SMART', 'USD')
    print(f"[INFO] Contract created: {contract}")
    
    # Qualification check (Optional but informative)
    try:
        ib.qualifyContracts(contract)
        print(f"[INFO] Contract qualified: {contract} (ConId: {contract.conId})")
    except Exception as e:
        print(f"[WARN] Qualification failed: {e}")

    # Test 1: Real-time market data (Live)
    print("\n[TEST 1] Real-time market data (Type 1)")
    ib.reqMarketDataType(1)  
    ticker = ib.reqMktData(contract, '', False, False)
    
    start = time.time()
    while time.time() - start < 2:
        ib.sleep(0.1)
        if ticker.last > 0 or ticker.close > 0 or ticker.bid > 0:
            print(f"  [SUCCESS] Data received: Last={ticker.last} Close={ticker.close} Bid={ticker.bid} Ask={ticker.ask}")
            break
    else:
        print(f"  [FAIL] No data received. Final state: Last={ticker.last} Close={ticker.close} Bid={ticker.bid}")
    
    ib.cancelMktData(contract)

    # Test 2: Delayed market data (Type 3)
    print("\n[TEST 2] Delayed market data (Type 3)")
    ib.reqMarketDataType(3) 
    ticker2 = ib.reqMktData(contract, '', False, False)
    
    start = time.time()
    while time.time() - start < 2:
        ib.sleep(0.1)
        if ticker2.last > 0 or ticker2.close > 0 or ticker2.bid > 0:
            print(f"  [SUCCESS] Data received: Last={ticker2.last} Close={ticker2.close} Bid={ticker2.bid} Ask={ticker2.ask}")
            break
    else:
        print(f"  [FAIL] No data received. Final state: Last={ticker2.last} Close={ticker2.close} Bid={ticker2.bid}")

    ib.cancelMktData(contract)
    
    ib.disconnect()
    print("\n[INFO] Test completed")
    
except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()
