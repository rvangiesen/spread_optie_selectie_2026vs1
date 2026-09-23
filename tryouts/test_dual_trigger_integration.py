import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import numpy as np
import yfinance as yf

from logic import (
    analyze_dual_trigger_spreads,
    calculate_profit_stop_levels,
    SpreadScanner
)
from hitrate_backtester import SpreadHitRateTester

def test_analyze_dual_trigger_spreads():
    # Test on synthetic data
    dates = pd.date_range('2025-01-01', periods=50, freq='D')
    prices = np.linspace(100, 120, 50) + np.random.normal(0, 1, 50)
    df = pd.DataFrame({
        'Open': prices,
        'High': prices + 1.0,
        'Low': prices - 1.0,
        'Close': prices,
        'Volume': 1000000
    }, index=dates)
    
    res = analyze_dual_trigger_spreads(df)
    assert not res.empty
    assert 'Squeeze_On' in res.columns
    assert 'Squeeze_Fire_Up' in res.columns
    assert 'Pullback_Entry' in res.columns
    assert 'Long_Signal' in res.columns
    assert 'Exit_Signal' in res.columns
    assert 'Signal_Type' in res.columns
    print("test_analyze_dual_trigger_spreads: PASSED")

def test_calculate_profit_stop_levels():
    # BullPut credit spread
    ps_put = calculate_profit_stop_levels('BullPut', entry_price_or_credit=1.50, width=5.0, target_profit_pct=70.0)
    assert ps_put['target_profit_pct'] == 70.0
    assert ps_put['exit_spread_price'] == 0.45  # 1.50 * 0.30
    assert ps_put['target_profit_usd'] == 105.0 # 150 * 0.70

    # BullCall debit spread
    ps_call = calculate_profit_stop_levels('BullCall', entry_price_or_credit=2.00, width=5.0, target_profit_pct=70.0)
    assert ps_call['target_profit_pct'] == 70.0
    assert ps_call['target_profit_usd'] == 210.0 # (5 - 2) * 100 * 0.70

    # LongCall single leg
    ps_lc = calculate_profit_stop_levels('LongCall', entry_price_or_credit=3.00, target_profit_pct=70.0)
    assert ps_lc['target_profit_pct'] == 70.0
    assert ps_lc['exit_spread_price'] == 5.10  # 3.00 * 1.70
    print("test_calculate_profit_stop_levels: PASSED")

def test_scanner_dual_trigger_status():
    scanner = SpreadScanner()
    ticker = yf.Ticker('SPY')
    hist = ticker.history(period='3mo')
    status = scanner.get_dual_trigger_status(hist, max_lookback=5)
    assert 'has_signal' in status
    assert 'signal_type' in status
    assert 'badge' in status
    print(f"test_scanner_dual_trigger_status: PASSED (SPY status={status['badge']})")

def test_backtester_10_trades_dual_trigger():
    tester = SpreadHitRateTester()
    res = tester.run_backtest(
        symbols=['SPY'],
        trades_per_symbol=10,
        use_dual_trigger=True,
        profit_target_pct=70.0,
        target_strategy='BullCall'
    )
    summary = res['summary']
    details = res['details_df']
    assert not details.empty
    assert len(details) <= 10
    assert 'entry_signal' in details.columns
    assert 'exit_reason' in details.columns
    assert 'days_held' in details.columns
    print(f"test_backtester_10_trades_dual_trigger: PASSED (Trades={summary['total_trades']}, WinRate={summary['hit_rate']}%, AvgHeld={summary['avg_days_held']}d)")

if __name__ == '__main__':
    test_analyze_dual_trigger_spreads()
    test_calculate_profit_stop_levels()
    test_scanner_dual_trigger_status()
    test_backtester_10_trades_dual_trigger()
    print("\nALL DUAL-TRIGGER INTEGRATION TESTS PASSED!")
