import sys
from ib_client import IBClient
client = IBClient()
if not client.connect():
    print("Cannot connect!")
    sys.exit(1)
chains = client.get_option_chains_params('NVDA')
for i, c in enumerate(chains):
    print(f"Chain {i}: Exchange {c.exchange}, TradingClass {c.tradingClass}, Expirations {len(c.expirations)} items")
    print(c.expirations[:10])
client.disconnect()
