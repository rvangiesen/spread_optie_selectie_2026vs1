import os
import sys
import datetime
import pandas as pd

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from stock_profile_manager import StockProfileManager
from logic import PortfolioAnalyzer

def run_integration_test():
    print("🧪 Start Integratietest Auto-Optimalisatie (Stap 1 t/m 4)...")
    mgr = StockProfileManager()

    # 1. Stel een strak profiel in voor SPY (1.10x EM -> 50% target)
    spy_prof = {
        'best_em_multiplier': 1.10,
        'best_width': 5.0,
        'best_min_dte': 14,
        'best_max_dte': 25,
        'last_sweep_date': datetime.date.today().strftime('%Y-%m-%d')
    }
    mgr.save_profile('SPY', spy_prof)
    assert mgr.get_profit_target_pct('SPY') == 50.0

    # 2. Stel een diep profiel in voor NVDA (1.65x EM -> 75% target)
    nvda_prof = {
        'best_em_multiplier': 1.65,
        'best_width': 15.0,
        'best_min_dte': 21,
        'best_max_dte': 35,
        'last_sweep_date': datetime.date.today().strftime('%Y-%m-%d')
    }
    mgr.save_profile('NVDA', nvda_prof)
    assert mgr.get_profit_target_pct('NVDA') == 75.0

    # 3. Test PortfolioAnalyzer voor SPY bij 52% winst (moet WINST_BORGEN triggeren want >= 50%)
    analyzer = PortfolioAnalyzer(None)
    pos_spy = {
        'symbol': 'SPY',
        'strategy': 'BullPut',
        'right': 'P',
        'expiry': '2026-10-16',
        'dte': 20,
        'unrealized_pnl': 260.0,
        'pnl_pct': 52.0,
        'market_price': 0.40,
        'short_leg_price': 0.40,
        'entry_price': 1.00,
        'sold_strike': 580.0,
        'bought_strike': 575.0,
        'underlying_price': 590.0,
        'legs': []
    }
    res_spy = analyzer.evaluate_position_health(pos_spy, None)
    print(f"SPY Evaluatie: Action={res_spy['action_code']}, Target={res_spy.get('profit_target_pct')}%, Reason={res_spy.get('reasoning')}")
    assert res_spy['action_code'] == 'WINST_BORGEN', f"Verwacht WINST_BORGEN voor SPY bij 52% winst, kreeg {res_spy['action_code']}"
    assert res_spy['profit_target_pct'] == 50.0
    print("✅ SPY (1.10x EM): Winst borgen correct getriggerd op 50% target!")

    # 4. Test PortfolioAnalyzer voor NVDA bij 55% winst (mag NOG GEEN WINST_BORGEN triggeren want target is 75%)
    pos_nvda = {
        'symbol': 'NVDA',
        'strategy': 'BullPut',
        'right': 'P',
        'expiry': '2026-10-16',
        'dte': 28,
        'unrealized_pnl': 550.0,
        'pnl_pct': 55.0,
        'market_price': 1.50,
        'short_leg_price': 1.50,
        'entry_price': 3.30,
        'sold_strike': 120.0,
        'bought_strike': 105.0,
        'underlying_price': 135.0,
        'legs': []
    }
    res_nvda = analyzer.evaluate_position_health(pos_nvda, None)
    print(f"NVDA Evaluatie: Action={res_nvda['action_code']}, Target={res_nvda.get('profit_target_pct')}%")
    assert res_nvda['action_code'] == 'HANDHAVEN', f"NVDA bij 55% winst mag nog niet sluiten (target 75%), kreeg {res_nvda['action_code']}"
    assert res_nvda['profit_target_pct'] == 75.0
    print("✅ NVDA (1.65x EM): Positie correct gehandhaafd (target 75% nog niet bereikt)!")

    # 5. Test NVDA bij 76% winst (moet NU WEL WINST_BORGEN triggeren)
    pos_nvda['pnl_pct'] = 76.0
    res_nvda_76 = analyzer.evaluate_position_health(pos_nvda, None)
    assert res_nvda_76['action_code'] == 'WINST_BORGEN'
    print("✅ NVDA (1.65x EM): Winst borgen correct getriggerd bij 76% winst!")

    print("🎉 Alle integratietesten voor Auto-Optimalisatie & Portfolio Dynamiek succesvol doorstaan!")

if __name__ == '__main__':
    run_integration_test()
