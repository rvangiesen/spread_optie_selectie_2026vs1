from ib_insync import *
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=9998)
ib.reqMarketDataType(3)

contract = Stock('SPY', 'SMART', 'USD')
ib.qualifyContracts(contract)

chains = ib.reqSecDefOptParams(contract.symbol, '', contract.secType, contract.conId)

if chains:
    chain = chains[0]
    print(f"Expirations: {list(chain.expirations)[:3]} ...")
    print(f"Strikes: {list(chain.strikes)[:3]} ...")
    
    # Try an option
    exp = chain.expirations[10]
    strike = chain.strikes[len(chain.strikes)//2]
    opt = Option('SPY', exp, strike, 'C', 'SMART')
    ib.qualifyContracts(opt)
    
    ib.reqMarketDataType(3)
    ticker3 = ib.reqMktData(opt, '106', False, False)
    ib.sleep(2)
    print(f"Type 3 (strike {strike}): bid={ticker3.bid} ask={ticker3.ask} close={ticker3.close}")
    if ticker3.modelGreeks:
        print(f"Type 3 IV: {ticker3.modelGreeks.impliedVol}")
    ib.cancelMktData(opt)

    ib.reqMarketDataType(4)
    ticker4 = ib.reqMktData(opt, '106', False, False)
    ib.sleep(2)
    print(f"Type 4 (strike {strike}): bid={ticker4.bid} ask={ticker4.ask} close={ticker4.close}")
    if ticker4.modelGreeks:
        print(f"Type 4 IV: {ticker4.modelGreeks.impliedVol}")
else:
    print("No chains found")

ib.disconnect()
