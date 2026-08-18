from ib_insync import *
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=9998)
contract = Option('SPY', '20260619', 730, 'C', 'SMART')
ib.qualifyContracts(contract)

print("--- TYPE 3 ---")
ib.reqMarketDataType(3)
ticker1 = ib.reqMktData(contract, '', False, False)
ib.sleep(2)
print('Type 3:', ticker1.bid, ticker1.ask, ticker1.close, ticker1.modelGreeks)

ib.cancelMktData(contract)

print("--- TYPE 4 ---")
ib.reqMarketDataType(4)
ticker2 = ib.reqMktData(contract, '', False, False)
ib.sleep(2)
print('Type 4:', ticker2.bid, ticker2.ask, ticker2.close, ticker2.modelGreeks)

ib.disconnect()
