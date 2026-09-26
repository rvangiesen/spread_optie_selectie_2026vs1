import sys
import os
import pandas as pd
import numpy as np

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from risk_model import EarlyAssignmentRiskEngine
from logic import SpreadScanner

def test_ex_div_assignment_engine():
    print("=== 1. Test: EarlyAssignmentRiskEngine Ex-Dividend Detection ===")
    
    # Scenario A: Short Call ITM with Dividend >= Extrinsic Value (ARBITRAGE)
    # Spot 105, Strike 100, Call price 5.03 -> Intrinsic = 5.0, Extrinsic = 0.03
    # Dividend = $0.80 per share, Ex-Div in 10 days (DTE = 20)
    risk_arb = EarlyAssignmentRiskEngine.evaluate_assignment_risk(
        spot=105.0,
        strike_sell=100.0,
        short_option_price=5.03,
        right='C',
        dte=20,
        dividend_amount=0.80,
        days_to_ex_div=10
    )
    print(f"Scenario A (Arbitrage): risk_level={risk_arb['risk_level']}, prob={risk_arb['probability_assignment_pct']}%, desc={risk_arb['status_desc']}")
    assert risk_arb['risk_level'] == 'CRITICAL'
    assert risk_arb['is_ex_div_risk'] == True
    assert risk_arb['probability_assignment_pct'] >= 90.0
    assert risk_arb['action_code'] == 'DIRECT_SLUITEN'

    # Scenario B: Short Call OTM but within 3% of spot, Ex-Div in 12 days
    # Spot 98, Strike 100 (< 3% OTM), Call price 1.50
    risk_near = EarlyAssignmentRiskEngine.evaluate_assignment_risk(
        spot=98.0,
        strike_sell=100.0,
        short_option_price=1.50,
        right='C',
        dte=25,
        dividend_amount=0.75,
        days_to_ex_div=12
    )
    print(f"Scenario B (Near Money): risk_level={risk_near['risk_level']}, is_ex_div={risk_near['is_ex_div_risk']}, desc={risk_near['status_desc']}")
    assert risk_near['risk_level'] == 'WARNING'
    assert risk_near['is_ex_div_risk'] == True

    # Scenario C: Short Put (Puts are NOT subject to dividend exercise risk)
    # Spot 95, Strike 100 (ITM Put), Dividend = $1.00
    risk_put = EarlyAssignmentRiskEngine.evaluate_assignment_risk(
        spot=95.0,
        strike_sell=100.0,
        short_option_price=5.50,
        right='P',
        dte=20,
        dividend_amount=1.00,
        days_to_ex_div=10
    )
    print(f"Scenario C (Put): is_ex_div={risk_put['is_ex_div_risk']}")
    assert risk_put['is_ex_div_risk'] == False

    # Scenario D: Ex-Div date is AFTER expiration (e.g. ex-div in 35 days, DTE is 20)
    risk_after = EarlyAssignmentRiskEngine.evaluate_assignment_risk(
        spot=105.0,
        strike_sell=100.0,
        short_option_price=5.03,
        right='C',
        dte=20,
        dividend_amount=0.80,
        days_to_ex_div=35
    )
    print(f"Scenario D (After Expiry): is_ex_div={risk_after['is_ex_div_risk']}")
    assert risk_after['is_ex_div_risk'] == False

    print("✅ EarlyAssignmentRiskEngine testen geslaagd!")

def test_filter_spreads_ex_div():
    print("\n=== 2. Test: Filter Spreads Ex-Dividend Filter ===")
    scanner = SpreadScanner()
    
    mock_df = pd.DataFrame([
        {
            'symbol': 'AAPL',
            'strategy': 'BearCall',
            'strike_buy': 240.0,
            'strike_sell': 230.0,
            'right': 'C',
            'dte': 30,
            'assignment_risk_badge': '🚨 EX-DIV ARBITRAGE (Div $1.08 >= Tijdswaarde)',
            'capital_covered': True,
            'max_profit': 150.0,
            'pop': 70.0
        },
        {
            'symbol': 'MSFT',
            'strategy': 'BearCall',
            'strike_buy': 460.0,
            'strike_sell': 450.0,
            'right': 'C',
            'dte': 30,
            'assignment_risk_badge': '🟢 VEILIG (Volledig Gedekt)',
            'capital_covered': True,
            'max_profit': 140.0,
            'pop': 72.0
        },
        {
            'symbol': 'SPY',
            'strategy': 'BullPut',
            'strike_buy': 570.0,
            'strike_sell': 580.0,
            'right': 'P',
            'dte': 30,
            'assignment_risk_badge': '🟢 VEILIG (Volledig Gedekt)',
            'capital_covered': True,
            'max_profit': 160.0,
            'pop': 75.0
        }
    ])
    
    # Filter with filter_ex_div_risk = True
    filtered = scanner.filter_spreads(mock_df, {'filter_ex_div_risk': True})
    print(f"Overgebleven na ex-div filter: {len(filtered)} rijen (AAPL met EX-DIV moet gefilterd zijn)")
    assert len(filtered) == 2
    assert 'AAPL' not in filtered['symbol'].values
    assert 'MSFT' in filtered['symbol'].values
    assert 'SPY' in filtered['symbol'].values
    
    print("✅ filter_spreads Ex-Div toewijzingsfilter succesvol geverifieerd!")

if __name__ == '__main__':
    test_ex_div_assignment_engine()
    test_filter_spreads_ex_div()
    print("\n🎉 Alle Ex-Dividend toewijzingstesten succesvol doorstaan!")
