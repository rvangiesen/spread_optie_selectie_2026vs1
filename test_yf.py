import yfinance as yf
import pandas as pd
import datetime
import urllib.request
import io

print("S&P 500 symbolen ophalen via Wikipedia...")
req = urllib.request.Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})
html = urllib.request.urlopen(req).read()
df_sp500 = pd.read_html(html)[0]
symbols = df_sp500['Symbol'].tolist()
symbols = [s.replace('.', '-') for s in symbols]

print(f"Haal {len(symbols)} symbolen op. We testen de eerste 3...")
symbols = symbols[:3]

results_data = []

for sym in symbols:
    print(f"Testing {sym}...")
    tk = yf.Ticker(sym)
    hist = tk.history(period="1d")
    if hist.empty:
        print("No hist")
        continue
    
    price = float(hist['Close'].iloc[-1])
    atm_strike = round(price)
    itm_call_strike = round(price * 0.85)
    itm_put_strike = round(price * 1.15)
    
    opts = tk.options
    if not opts:
        print("No opts")
        continue

    today = datetime.date.today()
    chosen_exp = opts[0]
    for exp in opts:
        exp_date = datetime.datetime.strptime(exp, "%Y-%m-%d").date()
        if (exp_date - today).days >= 7:
            chosen_exp = exp
            break
            
    chain = tk.option_chain(chosen_exp)
    calls = chain.calls
    puts = chain.puts
    
    def get_spread(df, target_strike):
        if df is None or df.empty: return None, None, None
        closest_idx = (df['strike'] - target_strike).abs().idxmin()
        row = df.loc[closest_idx]
        bid = row.get('bid', 0.0)
        ask = row.get('ask', 0.0)
        if pd.isna(bid): bid = 0.0
        if pd.isna(ask): ask = 0.0
        return float(bid), float(ask), max(0.0, float(ask) - float(bid))

    c_b, c_a, c_s = get_spread(calls, atm_strike)
    p_b, p_a, p_s = get_spread(puts, atm_strike)
    
    results_data.append({
        'Symbol': sym, 'Price': price, 'ATM_C_Spread': c_s, 'ATM_P_Spread': p_s
    })

print("Success! Data:")
for r in results_data:
    print(r)
