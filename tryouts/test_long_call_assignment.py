import sys
import os
import pandas as pd
import numpy as np

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logic import PortfolioAnalyzer
from risk_model import EarlyAssignmentRiskEngine

def test_bns_long_call():
    print("=== Test: BNS Long Call Early Assignment Evaluation ===")
    
    pos_bns = {
        'symbol': 'BNS',
        'strategy': 'LongCall',
        'expiry': '20261016',
        'dte': 25,
        'qty': 1,
        'is_long': True,
        'strikes_str': '90.0 C',
        'sold_strike': 0.0,
        'bought_strike': 90.0,
        'right': 'C',
        'market_price': 5.80,
        'entry_price': 6.01,
        'unrealized_pnl': -21.28,
        'pnl_pct': -3.5,
        'underlying_price': 95.47
    }

    analyzer = PortfolioAnalyzer()

    # 1. Direct check of evaluate_anti_assignment_routine
    anti_assign = analyzer.evaluate_anti_assignment_routine(
        pos=pos_bns,
        underlying_p=95.47,
        dte=25,
        pnl_usd=-21.28,
        pnl_pct=-3.5
    )

    print(f"Risk Level: {anti_assign['risk_level']}")
    print(f"Triggers: {anti_assign['triggers']}")
    print(f"Consequences: {anti_assign['consequences']}")
    print(f"Assignment Prob: {anti_assign['assignment_probability_pct']}%")
    print(f"Extrinsic Val: ${anti_assign['extrinsic_val']:.2f}")
    print(f"Intrinsic Val: ${anti_assign['intrinsic_val']:.2f}")

    assert anti_assign['risk_level'] == 'SAFE', f"Expected SAFE, got {anti_assign['risk_level']}"
    assert len(anti_assign['triggers']) == 0, f"Expected no triggers, got {anti_assign['triggers']}"
    assert anti_assign['assignment_probability_pct'] == 0.0, f"Expected 0.0%, got {anti_assign['assignment_probability_pct']}"
    assert abs(anti_assign['intrinsic_val'] - 5.47) < 0.01, f"Expected 5.47 intrinsic, got {anti_assign['intrinsic_val']}"
    assert abs(anti_assign['extrinsic_val'] - 0.33) < 0.01, f"Expected 0.33 extrinsic, got {anti_assign['extrinsic_val']}"

    # 2. Check EarlyAssignmentRiskEngine guard when strike_sell <= 0
    engine_risk = EarlyAssignmentRiskEngine.evaluate_assignment_risk(
        spot=95.47,
        strike_sell=0.0,
        short_option_price=5.80,
        right='C',
        dte=25
    )
    assert engine_risk['risk_level'] == 'VEILIG', f"Expected VEILIG, got {engine_risk['risk_level']}"
    assert engine_risk['probability_assignment_pct'] == 0.0

    # 3. Check Financials BEP formatting (no raw $ LaTeX breakage)
    fin = analyzer.calculate_position_financials(pos_bns)
    print(f"BEP Status string: {fin['bep_status'].encode('ascii', 'ignore').decode()}")
    assert "\\" in fin['bep_status'], "Expected escaped dollar signs in bep_status"

    print("\n[SUCCESS] Alle Long Call assignment tests succesvol geslaagd!")

if __name__ == '__main__':
    test_bns_long_call()
