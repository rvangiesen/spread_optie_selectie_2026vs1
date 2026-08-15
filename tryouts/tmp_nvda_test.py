import sys, os
from ib_client import IBClient
import pandas as pd

client = IBClient()
price = 184.65
atm_strike = 185.0
target_strikes = [182.5, 185.0, 187.5] # test a few strikes

data = client.get_chain_greeks_and_oi('NVDA', '20260417', target_strikes)
print("--- 20260417 ---")
print(data)

data2 = client.get_chain_greeks_and_oi('NVDA', '20260424', target_strikes)
print("--- 20260424 ---")
print(data2)
