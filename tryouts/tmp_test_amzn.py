import pandas as pd
import datetime
from ib_client import IBClient
from ib_insync import Stock

tws_host = "127.0.0.1"
tws_port = 7497

if __name__ == "__main__":
    ib = IBClient()
    success, msg = ib.connect(tws_host, tws_port, 9999)
    if not success:
        print("CONNECT FAILED", msg)
        exit(1)
        
    sym = "AMZN"
    ib.set_data_type(3) # delayed
    contract = Stock(sym, 'SMART', 'USD')
    price_data = ib.get_market_data_snapshot(contract, use_hist_fallback=False)
    price = price_data.get('price', 0.0)
    print(f"Price for {sym}: {price}")
    
    chains = ib.get_option_chains_params(sym, sec_type='STK')
    if not chains:
        print("No chains found")
        exit(1)
        
    exp_targets = sorted(chains[0].expirations)
    today = datetime.date.today()
    chosen_exp = None
    for exp in exp_targets:
        exp_date = datetime.datetime.strptime(exp, "%Y%m%d").date()
        if (exp_date - today).days >= 7:
            chosen_exp = exp
            break
            
    print(f"Chosen Exp: {chosen_exp}")
    
    valid_strikes = []
    for chain in chains:
        valid_strikes.extend(chain.strikes)
    valid_strikes = sorted(list(set(valid_strikes)))
    
    def find_closest_strikes(val, count=5):
        return sorted(valid_strikes, key=lambda x: abs(x - val))[:count]
        
    atm_strike_target = price
    target_strikes_set = set()
    target_strikes_set.update(find_closest_strikes(atm_strike_target))
    target_strikes = sorted(list(target_strikes_set))
    
    print(f"Target strikes for {atm_strike_target}: {target_strikes}")
    
    chain_data = ib.get_chain_greeks_and_oi(sym, chosen_exp, target_strikes)
    print("CHAIN DATA RETURNED:")
    print(chain_data)
    
    ib.disconnect()
