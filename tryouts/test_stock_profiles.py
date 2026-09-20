import os
import sys
import datetime

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from stock_profile_manager import StockProfileManager

def run_tests():
    print("🧪 Testen van StockProfileManager...")
    mgr = StockProfileManager()

    # 1. Test opslaan van een profiel
    test_spy = {
        'best_em_multiplier': 1.20,
        'best_width': 10.0,
        'best_min_dte': 14,
        'best_max_dte': 25,
        'best_min_bep_dist': 6.0,
        'hit_rate': 88.0,
        'avg_pnl': 125.50,
        'total_trades': 20,
        'last_sweep_date': datetime.date.today().strftime('%Y-%m-%d')
    }
    assert mgr.save_profile('SPY', test_spy), "Opslaan SPY mislukt"
    print("✅ 1. Opslaan profiel gelukt")

    # 2. Test ophalen en profit target voor strakke EM (1.20x -> 50%)
    prof_spy = mgr.get_profile('SPY')
    assert prof_spy is not None, "SPY profiel niet gevonden"
    assert prof_spy['best_em_multiplier'] == 1.20
    assert prof_spy['profit_target_pct'] == 50.0, f"Verwacht 50.0% winstdoel voor 1.20x EM, kreeg {prof_spy['profit_target_pct']}"
    assert mgr.get_profit_target_pct('SPY') == 50.0
    print("✅ 2. Ophalen en dynamisch winstdoel (50% voor strakke EM) gelukt")

    # 3. Test opslaan en profit target voor diepe EM (1.65x -> 75%)
    test_nvda = {
        'best_em_multiplier': 1.65,
        'best_width': 15.0,
        'best_min_dte': 21,
        'best_max_dte': 35,
        'best_min_bep_dist': 8.0,
        'hit_rate': 92.0,
        'avg_pnl': 210.00,
        'total_trades': 25,
        'last_sweep_date': datetime.date.today().strftime('%Y-%m-%d')
    }
    assert mgr.save_profile('NVDA', test_nvda), "Opslaan NVDA mislukt"
    prof_nvda = mgr.get_profile('NVDA')
    assert prof_nvda['profit_target_pct'] == 75.0, f"Verwacht 75.0% winstdoel voor 1.65x EM, kreeg {prof_nvda['profit_target_pct']}"
    assert mgr.get_profit_target_pct('NVDA') == 75.0
    print("✅ 3. Ophalen en dynamisch winstdoel (75% voor diepe EM) gelukt")

    # 4. Test expiry
    assert not mgr.is_expired('SPY', max_age_days=30), "Vandaag opgeslagen profiel zou niet expired moeten zijn"
    
    # Maak een oud profiel aan van 45 dagen geleden
    old_date = (datetime.date.today() - datetime.timedelta(days=45)).strftime('%Y-%m-%d')
    test_old = {
        'best_em_multiplier': 1.44,
        'best_width': 10.0,
        'last_sweep_date': old_date
    }
    mgr.save_profile('OLD', test_old)
    assert mgr.is_expired('OLD', max_age_days=30), "Profiel van 45 dagen oud zou expired moeten zijn"
    assert mgr.is_expired('NONEXISTENT'), "Niet-bestaand aandeel moet als expired gelden"
    print("✅ 4. Expiry verificatie (>30 dagen) gelukt")

    # 5. Test DataFrame export
    df = mgr.to_dataframe()
    assert not df.empty, "DataFrame mag niet leeg zijn"
    assert 'SPY' in df.index
    assert 'NVDA' in df.index
    print("✅ 5. DataFrame export gelukt:\n", df[['best_em_multiplier', 'best_width', 'profit_target_pct', 'last_sweep_date']])

    # Clean up test symbol
    mgr.delete_profile('OLD')

    # 6. Test quick_optimize_stock via SpreadHitRateTester
    print("⚡ 6. Testen van quick_optimize_stock op SPY...")
    from hitrate_backtester import SpreadHitRateTester
    tester = SpreadHitRateTester()
    spy_opt = tester.quick_optimize_stock('SPY', trades_per_symbol=2)
    assert spy_opt['symbol'] == 'SPY'
    assert 'best_em_multiplier' in spy_opt
    assert 'profit_target_pct' in spy_opt
    mgr.save_profile('SPY', spy_opt)
    print(f"✅ 6. quick_optimize_stock geslaagd voor SPY: {spy_opt['best_em_multiplier']}x EM, target {spy_opt['profit_target_pct']}%")

    print("🎉 Alle StockProfileManager & Sweep testen succesvol doorstaan!")

if __name__ == '__main__':
    run_tests()

