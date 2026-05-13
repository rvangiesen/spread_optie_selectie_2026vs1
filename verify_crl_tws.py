from ib_insync import IB, Stock
import sys

def check_crl():
    ib = IB()
    try:
        ib.connect('127.0.0.1', 7497, clientId=999)
    except Exception as e:
        print(f"Connection error: {e}")
        return
        
    contract = Stock('CRL', 'SMART', 'USD')
    details = ib.reqContractDetails(contract)
    print("Contract Details:")
    for d in details:
        print(f" - {d.contract.symbol} [{d.contract.conId}] : {d.longName} ({d.contract.primaryExchange})")
        
    ib.reqMarketDataType(3) # Delayed
    ticker = ib.reqMktData(contract, '', False, False)
    ib.sleep(2)
    print(f"Market Data:")
    print(f" - Last: {ticker.last}")
    print(f" - Close: {ticker.close}")
    print(f" - Bid: {ticker.bid}, Ask: {ticker.ask}")
    
    ib.disconnect()

if __name__ == '__main__':
    check_crl()
