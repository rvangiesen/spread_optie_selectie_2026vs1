import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import pandas as pd
from risk_model import AntiGravityGammaThetaEngine

def test_ultimate_ag_score_bounds():
    # 1. Test Best-Case Scenario
    best = AntiGravityGammaThetaEngine.calculate_ultimate_ag_score(
        pop_adj=95.0,
        max_profit=150.0,
        max_loss=350.0,
        expected_value=85.0,
        tei_score=1.8,
        ttp_days=5.0,
        dte=30.0,
        bep_dist_pct=10.0,
        ds_be=5.0,
        atr_10=2.0,
        gamma_cliff=False,
        cue="CONFIDENT_UPTREND",
        max_pain_ok=True,
        koopadvies_ok=True,
        strategy_type="BullPut"
    )
    assert 85.0 <= best['ultimate_ag_score'] <= 100.0, f"Expected elite score >= 85, got {best['ultimate_ag_score']}"
    assert best['score_pop'] <= 25.0
    assert best['score_roc'] <= 20.0
    assert best['score_ttp'] <= 15.0
    assert best['score_safety'] <= 20.0
    assert best['score_flow'] <= 20.0
    print(f"[OK] Best-case test geslaagd: Score = {best['ultimate_ag_score']}/100")
    print(f"     Pijlers: P1={best['score_pop']}, P2={best['score_roc']}, P3={best['score_ttp']}, P4={best['score_safety']}, P5={best['score_flow']}")

    # 2. Test Worst-Case Scenario / Gamma Cliff
    worst = AntiGravityGammaThetaEngine.calculate_ultimate_ag_score(
        pop_adj=45.0,
        max_profit=10.0,
        max_loss=990.0,
        expected_value=-50.0,
        tei_score=0.2,
        ttp_days=25.0,
        dte=2.0,
        bep_dist_pct=0.5,
        ds_be=0.2,
        atr_10=3.0,
        gamma_cliff=True,
        cue="CONFIDENT_DOWNTREND",
        max_pain_ok=False,
        koopadvies_ok=False,
        strategy_type="BullPut"
    )
    assert worst['ultimate_ag_score'] < 40.0, f"Expected low score < 40, got {worst['ultimate_ag_score']}"
    assert worst['score_safety'] == 0.0, f"Expected safety 0 on cliff, got {worst['score_safety']}"
    print(f"[OK] Worst-case / Gamma Cliff test geslaagd: Score = {worst['ultimate_ag_score']}/100")

    # 3. Test Intermediate Scenario
    mid = AntiGravityGammaThetaEngine.calculate_ultimate_ag_score(
        pop_adj=75.0,
        max_profit=100.0,
        max_loss=400.0,
        expected_value=25.0,
        tei_score=1.1,
        ttp_days=14.0,
        dte=30.0,
        bep_dist_pct=6.5,
        ds_be=3.2,
        atr_10=2.0,
        gamma_cliff=False,
        cue="FLAT_PINNING",
        max_pain_ok=True,
        koopadvies_ok=True,
        strategy_type="BullPut"
    )
    assert 60.0 <= mid['ultimate_ag_score'] <= 85.0, f"Expected mid score between 60 and 85, got {mid['ultimate_ag_score']}"
    print(f"[OK] Intermediate test geslaagd: Score = {mid['ultimate_ag_score']}/100")

if __name__ == "__main__":
    test_ultimate_ag_score_bounds()
    print("ALL TESTS PASSED!")
