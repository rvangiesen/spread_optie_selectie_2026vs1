import sys
import os
import pandas as pd
import numpy as np

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logic import PortfolioAnalyzer

def test_portfolio_analyzer():
    print("--- Test 1: Technical Indicators Calculation ---")
    dates = pd.date_range(end=pd.Timestamp.now(), periods=50)
    prices = [100.0 + math.sin(i / 5.0) * 10 - i * 0.2 for i in range(50)]
    
    df_hist = pd.DataFrame({
        'high': [p + 1.0 for p in prices],
        'low': [p - 1.0 for p in prices],
        'close': prices
    }, index=dates)

    analyzer = PortfolioAnalyzer()
    
    kelt = analyzer.calculate_keltner_channels(df_hist)
    coral = analyzer.calculate_coral_trend(df_hist)
    cci = analyzer.calculate_cci(df_hist)
    macd = analyzer.calculate_macd(df_hist)
    
    assert not kelt.empty, "Keltner calculation failed!"
    assert len(coral) > 0, "Coral calculation failed!"
    assert len(cci) > 0, "CCI calculation failed!"
    assert not macd.empty, "MACD calculation failed!"
    
    print("[OK] Alle technische indicatoren succesvol berekend.")

    print("\n--- Test 2: Single Leg Option (Long Call) Evaluation ---")
    pos_long_call = {
        'symbol': 'NVDA',
        'strategy': 'LongCall',
        'expiry': '20260918',
        'dte': 18,
        'qty': 1,
        'is_long': True,
        'strikes_str': '130.0 C',
        'sold_strike': 0.0,
        'bought_strike': 130.0,
        'right': 'C',
        'market_price': 4.50,
        'entry_price': 3.00,
        'unrealized_pnl': 150.0,
        'pnl_pct': 50.0,
        'underlying_price': 132.50
    }
    
    res_call = analyzer.evaluate_position_health(pos_long_call, df_hist)
    print(f"Long Call Resultaat: {res_call['action_title']} ({res_call['urgency']})")
    assert 'action_code' in res_call
    assert len(res_call['alternatives']) == 6

    print("\n--- Test 3: Vertical Spread (Bull Put) Under Pressure Evaluation ---")
    pos_bull_put = {
        'symbol': 'AAPL',
        'strategy': 'BullPut',
        'expiry': '20260918',
        'dte': 15,
        'qty': 2,
        'is_long': False,
        'strikes_str': '220.0/215.0 P',
        'sold_strike': 220.0,
        'bought_strike': 215.0,
        'right': 'P',
        'market_price': 2.80,
        'entry_price': 1.20,
        'unrealized_pnl': -320.0,
        'pnl_pct': -40.0,
        'underlying_price': 218.50 # Under sold strike!
    }

    res_bp = analyzer.evaluate_position_health(pos_bull_put, df_hist)
    print(f"Bull Put Resultaat: {res_bp['action_title']} ({res_bp['urgency']})")
    print(f"Oude -> Nieuwe situatie: {res_bp['old_to_new'].encode('ascii', 'ignore').decode()}")
    print(f"Onderbouwing: {res_bp['reasoning']}")
    assert res_bp['action_code'] in ['TIJDIG_SLUITEN', 'DOORROLLEN_CREDIT']
    print("[OK] Position Evaluation Test geslaagd.")


if __name__ == '__main__':
    import math
    test_portfolio_analyzer()
