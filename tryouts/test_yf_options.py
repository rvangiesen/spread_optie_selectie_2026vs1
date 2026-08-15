import yfinance as yf
import pandas as pd

tk = yf.Ticker("SPY")
expirations = tk.options
print("Expirations:", expirations[:3])
chain = tk.option_chain(expirations[0])

calls = chain.calls
puts = chain.puts
print("Calls columns:", calls.columns)
print(calls[['strike', 'lastPrice', 'bid', 'ask', 'impliedVolatility']].head())
