import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logic import SpreadScanner

scanner = SpreadScanner(ib_client=None)

def safe_log(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', errors='replace').decode('ascii'))

# Test 1: Barchart Stock Screener CSV (e.g. top-100-stocks-to-buy)
stock_df = pd.DataFrame({
    'Symbol': ['MRNA', 'PRAX', 'PLSE'],
    'Latest': [145.13, 375.94, 50.42],
    'Volume': [87316000, 413300, 453100]
})

res1 = scanner.parse_barchart_flow(stock_df, min_size=100, log_func=safe_log)
print("Test 1 Result (Stock List CSV):", len(res1), "rows parsed.")

# Test 2: Barchart Option Flow CSV with UPPERCASE column names (as loaded in pandas)
flow_df = pd.DataFrame({
    'SYMBOL': ['AAPL', 'NVDA'],
    'TYPE': ['CALL', 'PUT'],
    'STRIKE': [170.0, 120.0],
    'PRICE~': [180.0, 115.0],
    'DTE': [45, 30],
    'SIZE': [500, 1000],
    'DELTA': [0.50, -0.40],
    'CODE': ['MLCT', 'SLCN'],
    'EXPIRES': ['2026-10-16T16:00:00-04:00', '2026-09-18'],
    'IV': ['35.5%', '42.1%'],
    'PREMIUM': [125000, 85000]
})

res2 = scanner.parse_barchart_flow(flow_df, min_size=100, log_func=safe_log)
print("Test 2 Result (Option Flow CSV UPPERCASE):", len(res2), "rows parsed.")

if len(res2) == 2:
    print("SUCCESS: Both Option Flow rows parsed cleanly with uppercase column names!")
else:
    print("WARNING: Expected 2 rows parsed, got", len(res2))
