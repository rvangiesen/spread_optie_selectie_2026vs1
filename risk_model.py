import math
import numpy as np
from scipy.stats import norm

def get_bs_risk_metrics(S, K, T, r, q, sigma, atr_10, target_profit_usd, technical_multiplier=1.0):
    """
    S: Current Price, K: Strike, T: Time to Expiry (years), r: Risk-free rate, 
    q: Dividend yield, sigma: IV, atr_10: 10-day ATR, 
    target_profit_usd: Profit goal (e.g., 5.0), technical_multiplier: Adjustment factor.
    """
    # --- Part A: Bjerksund-Stensland Early Exercise Boundary (I) ---
    beta = (0.5 - (r - q) / sigma**2) + math.sqrt(((r - q) / sigma**2 - 0.5)**2 + 2 * r / sigma**2)
    B_inf = (beta / (beta - 1)) * K
    B_0 = max(K, (r / (r - q)) * K) if r > q else K
    
    # Boundary approximation (h-function)
    h = -( (r - q) * T + 2 * sigma * math.sqrt(T) ) * (B_0 / (B_inf - B_0))
    I = B_0 + (B_inf - B_0) * (1 - math.exp(h))
    
    # --- Part B: Greeks Approximation (Taylor Expansion) ---
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    delta = math.exp(-q * T) * norm.cdf(d1)
    gamma = (math.exp(-q * T) * norm.pdf(d1)) / (S * sigma * math.sqrt(T))
    theta = -(S * sigma * math.exp(-q * T) * norm.pdf(d1)) / (2 * math.sqrt(T)) # Simplified
    
    # --- Part C: Time-to-Profit (TTP) Calculation ---
    # User clarification: target_profit_usd is the SHARE PRICE move (koerswinst)
    delta_s = float(target_profit_usd)
    s_target = S + delta_s
    
    # --- Part D: Risk Factors ---
    effective_velocity = atr_10 * technical_multiplier
    ttp_days = abs(delta_s) / effective_velocity if effective_velocity > 0 else float('inf')
    
    # Efficiency Index: ratio of target room vs boundary room
    tei = (I - s_target) / (I - S) if (I - S) != 0 else 0
    
    return {
        "target_price": round(s_target, 2),
        "exercise_boundary": round(I, 2),
        "days_to_profit": round(ttp_days, 1),
        "tei_score": round(tei, 3),
        "is_efficient": tei > 0 and ttp_days < (T * 365 * 0.5)
    }

if __name__ == "__main__":
    # Test example 
    res = get_bs_risk_metrics(
        S=100.0,
        K=105.0,
        T=30/365,
        r=0.05,
        q=0.01,
        sigma=0.20,
        atr_10=2.5,
        target_profit_usd=5.0
    )
    print("Risk metrics test:", res)
