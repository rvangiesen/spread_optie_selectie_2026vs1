from ib_insync import IB, Option
import nest_asyncio
nest_asyncio.apply()

ib = IB()
try:
    ib.connect('127.0.0.1', 7497, clientId=99)
    print("Connected")
    
    # 1. Let's try to qualify 557.5 C on 20260626 (June 26, 2026)
    c_weekly = Option(symbol='SMH', lastTradeDateOrContractMonth='20260626', strike=557.5, right='C', exchange='SMART', multiplier='100', currency='USD')
    res_weekly = ib.qualifyContracts(c_weekly)
    print("Weekly 557.5 C Result:", res_weekly)
    
    # 2. Let's try to qualify 557.5 C on 20260717 (July 17, 2026)
    c_monthly = Option(symbol='SMH', lastTradeDateOrContractMonth='20260717', strike=557.5, right='C', exchange='SMART', multiplier='100', currency='USD')
    res_monthly = ib.qualifyContracts(c_monthly)
    print("Monthly 557.5 C Result:", res_monthly)
    
    # 3. Let's list all strikes for July 17, 2026 explicitly for calls
    c_search = Option(symbol='SMH', lastTradeDateOrContractMonth='20260717', right='C', exchange='SMART')
    details = ib.reqContractDetails(c_search)
    strikes = sorted(list(set(d.contract.strike for d in details)))
    print("All strikes for July 17, 2026:", strikes)
    
finally:
    ib.disconnect()
