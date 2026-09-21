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
        "is_efficient": tei > 1.2 and ttp_days < (T * 365 * 0.5)
    }


class AntiGravityGammaThetaEngine:
    """
    Quantifies the Gamma/Theta relationship, Breakeven Move (dS_BE),
    Bayesian PoP adjustments based on option chain barriers, and Expected Value (EV).
    Derived from BSM differential equation and Gopinathan market maker dynamics:
    Theta + 0.5 * sigma^2 * S^2 * Gamma ~ 0  =>  dS_BE = sqrt(2 * |Theta_daily| / |Gamma|)
    """
    def __init__(self, risk_free_rate: float = 0.04):
        self.r = risk_free_rate

    @staticmethod
    def calculate_breakeven_move(net_gamma: float, net_theta_daily: float) -> float:
        """
        Calculates the required daily underlying price move (dS) to offset daily theta decay:
        dS_BE = sqrt(2 * |Theta_daily| / |Gamma|)
        For sellers (short gamma, long theta): profit if |dS_daily| < dS_BE.
        For buyers (long gamma, short theta): profit if |dS_daily| > dS_BE.
        """
        gamma_abs = abs(float(net_gamma))
        theta_abs = abs(float(net_theta_daily))
        if gamma_abs < 1e-7:
            return float('inf')
        return float(np.sqrt(2.0 * theta_abs / gamma_abs))

    @staticmethod
    def calculate_gamma_theta_ratio(net_gamma: float, net_theta_daily: float) -> float:
        """
        R_gamma_theta = |Gamma| / max(|Theta_daily|, 1e-5)
        """
        return round(abs(float(net_gamma)) / max(abs(float(net_theta_daily)), 1e-5), 4)

    @staticmethod
    def check_gamma_cliff(dte: float, ds_be: float, atr_daily: float) -> bool:
        """
        Gamma Cliff Alert: When DTE <= 3 and dS_BE < 0.35 * ATR_daily,
        short gamma positions accelerate risk uncontrollably.
        """
        if dte <= 3.0 and atr_daily > 0 and ds_be < (0.35 * atr_daily):
            return True
        return False

    def evaluate_strategy_ev_pop(
        self,
        spot: float,
        strategy_type: str,
        net_delta: float,
        net_gamma: float,
        net_theta_daily: float,
        max_profit: float,
        max_loss: float,
        chain_regime: dict = None,
        sigma_imp: float = 0.20,
        dte: float = 30.0,
        strikes: list = None,
        atr_daily: float = 0.0,
        commission: float = 1.50
    ) -> dict:
        """
        Full evaluation of strategy PoP (BSM + Bayesian OI/Greek adjustments),
        dS Breakeven, Expected Value (EV), and execution verdict.
        """
        if chain_regime is None:
            chain_regime = {}

        t_years = max(float(dte), 0.5) / 365.0
        sigma_val = max(0.01, float(sigma_imp))
        spot_val = max(0.01, float(spot))
        profit_val = max(0.0, float(max_profit))
        loss_val = max(1.0, float(max_loss))

        ds_be = self.calculate_breakeven_move(net_gamma, net_theta_daily)
        gt_ratio = self.calculate_gamma_theta_ratio(net_gamma, net_theta_daily)

        # 1. Base BSM Probability of Profit (PoP_BSM)
        strat_upper = str(strategy_type).upper()
        if strat_upper == "IRONCONDOR" and strikes and len(strikes) >= 4:
            # strikes: [p_buy, p_sell, c_sell, c_call]
            k_put = float(strikes[1])
            k_call = float(strikes[2])
            denom = sigma_val * np.sqrt(t_years)
            d2_put = (np.log(spot_val / k_put) + (self.r - 0.5 * sigma_val**2) * t_years) / denom
            d2_call = (np.log(spot_val / k_call) + (self.r - 0.5 * sigma_val**2) * t_years) / denom
            pop_bsm = float(norm.cdf(d2_put) - norm.cdf(d2_call))
        elif "CREDIT" in strat_upper or strat_upper in ["BULLPUT", "BEARCALL"]:
            # Credit spread base PoP via delta or max loss / (max profit + max loss)
            pop_bsm = float(loss_val / (profit_val + loss_val))
        elif strat_upper in ["STRANGLE", "SHORTSTRANGLE"]:
            if strikes and len(strikes) >= 2:
                kp, kc = min(strikes), max(strikes)
                denom = sigma_val * np.sqrt(t_years)
                d2_p = (np.log(spot_val / kp) + (self.r - 0.5 * sigma_val**2) * t_years) / denom
                d2_c = (np.log(spot_val / kc) + (self.r - 0.5 * sigma_val**2) * t_years) / denom
                pop_bsm = float(norm.cdf(d2_p) - norm.cdf(d2_c))
            else:
                pop_bsm = 0.65
        elif strat_upper in ["STRADDLE", "LONGSTRADDLE"]:
            # Long straddle has lower PoP (~35-45%) but uncapped upside
            pop_bsm = 0.40
        else:
            # Debit Spreads / Long options
            pop_bsm = float(profit_val / (profit_val + loss_val))

        pop_bsm = float(np.clip(pop_bsm, 0.05, 0.95))

        # 2. Bayesian Adjustments on PoP
        pop_adj = pop_bsm
        cue = str(chain_regime.get("cue", "NEUTRAL_RANGE")).upper()

        # A. Barrier Component (Delta PoP_OI)
        is_credit_type = ("CREDIT" in strat_upper) or (strat_upper in ["IRONCONDOR", "BULLPUT", "BEARCALL", "SHORTSTRANGLE"])
        if is_credit_type:
            # Flat/Pinning bonus
            if cue == "FLAT_PINNING":
                pop_adj += 0.08
            # Wall protection
            if chain_regime.get("support_protected", False):
                pop_adj += 0.04
            if chain_regime.get("resistance_protected", False):
                pop_adj += 0.04
            if chain_regime.get("vested_wall_protected", False):
                pop_adj += 0.05

        # B. Gamma/Theta Headwind / Tailwind Component
        if net_theta_daily > 0:
            # Net seller (Theta in favor)
            expected_daily_move = spot_val * (sigma_val / np.sqrt(252.0))
            if expected_daily_move < ds_be and ds_be != float('inf'):
                pop_adj += 0.05
        else:
            # Net buyer (seeking Gamma / directional breakout)
            if cue in ["CONFIDENT_UPTREND", "CONFIDENT_DOWNTREND"]:
                pop_adj += 0.07
            elif cue == "FAR_VOLATILITY_EXPANSION":
                pop_adj += 0.05

        pop_adj = float(np.clip(pop_adj, 0.05, 0.95))

        # 3. Expected Value (EV)
        # EV = (PoP_adj * Max Profit) - ((1 - PoP_adj) * Max Loss) - Commission
        ev = (pop_adj * profit_val) - ((1.0 - pop_adj) * loss_val) - commission

        # 4. Gamma Cliff Warning
        gamma_cliff = self.check_gamma_cliff(dte, ds_be, atr_daily)

        # 5. Execution Verdict
        if gamma_cliff:
            verdict = "GAMMA_CLIFF_RISK"
        elif ev > 0 and pop_adj >= 0.65:
            verdict = "EXECUTE"
        elif ev > 0 and pop_adj >= 0.50:
            verdict = "SPECULATIVE"
        else:
            verdict = "REJECT"

        return {
            "strategy": strategy_type,
            "dS_breakeven_daily": round(ds_be, 2) if ds_be != float('inf') else 999.0,
            "gamma_theta_ratio": gt_ratio,
            "pop_bsm": round(pop_bsm * 100.0, 1),
            "pop_adjusted": round(pop_adj * 100.0, 1),
            "expected_value": round(ev, 2),
            "gamma_cliff_warning": gamma_cliff,
            "trade_verdict": verdict
        }

    @staticmethod
    def calculate_ultimate_ag_score(
        pop_adj: float,
        max_profit: float,
        max_loss: float,
        expected_value: float,
        tei_score: float,
        ttp_days: float,
        dte: float,
        bep_dist_pct: float,
        ds_be: float,
        atr_10: float,
        gamma_cliff: bool,
        cue: str,
        max_pain_ok: bool,
        koopadvies_ok: bool,
        strategy_type: str = "BullPut",
        symbol_hitrate_factor: float = 1.0
    ) -> dict:
        """
        Calculates the normalized Ultimate AG-Score (0.0 to 100.0) across 5 core pillars:
        - Pillar 1: Slaagkans & Statistiek (max 25 pts)
        - Pillar 2: Rendement op Investering & EV (max 20 pts)
        - Pillar 3: Tijdswaarde & Verzilveringssnelheid (max 15 pts)
        - Pillar 4: Koersbuffer & Gamma/Theta Veiligheid (max 20 pts)
        - Pillar 5: Institutionele Flow & Sentiment (max 20 pts)
        """
        # --- Pillar 1: Slaagkans & Statistiek (Max 25 pts) ---
        pop_val = float(pop_adj) / 100.0 if float(pop_adj) > 1.0 else float(pop_adj)
        pop_val = float(np.clip(pop_val, 0.0, 1.0))
        hit_factor = float(np.clip(symbol_hitrate_factor, 0.80, 1.20))
        s1 = 25.0 * pop_val * hit_factor
        s1 = float(np.clip(s1, 0.0, 25.0))

        # --- Pillar 2: Rendement op Investering & EV (Max 20 pts) ---
        profit_val = max(0.0, float(max_profit))
        loss_val = max(1.0, float(max_loss))
        roc = profit_val / loss_val # Return on Capital / Risk
        s2_roc = 10.0 * min(1.0, roc / 0.25)
        ev_val = float(expected_value)
        s2_ev = 10.0 * max(0.0, min(1.0, (ev_val + 10.0) / 40.0)) if ev_val > 0 else 0.0
        s2 = float(np.clip(s2_roc + s2_ev, 0.0, 20.0))

        # --- Pillar 3: Tijdswaarde & Verzilveringssnelheid (Max 15 pts) ---
        tei = max(0.1, float(tei_score))
        s3_tei = 7.5 * min(1.0, tei / 1.5)
        dte_safe = max(1.0, float(dte))
        ttp_safe = max(0.0, float(ttp_days))
        speed_ratio = max(0.0, 1.0 - (ttp_safe / (dte_safe * 1.5)))
        s3_speed = 7.5 * min(1.0, speed_ratio)
        s3 = float(np.clip(s3_tei + s3_speed, 0.0, 15.0))

        # --- Pillar 4: Koersbuffer & Gamma/Theta Veiligheid (Max 20 pts) ---
        if gamma_cliff:
            s4 = 0.0
        else:
            bep_dist = max(0.0, float(bep_dist_pct))
            s4_bep = 10.0 * min(1.0, bep_dist / 8.0)
            atr_safe = max(0.1, float(atr_10))
            ds_val = float(ds_be) if ds_be != float('inf') else 999.0
            coverage = ds_val / (atr_safe * 1.5)
            s4_gt = 10.0 * min(1.0, coverage)
            s4 = float(np.clip(s4_bep + s4_gt, 0.0, 20.0))

        # --- Pillar 5: Institutionele Flow & Sentiment (Max 20 pts) ---
        cue_upper = str(cue).upper()
        strat_upper = str(strategy_type).upper()
        is_bull = any(x in strat_upper for x in ['BULL', 'LONG'])
        is_bear = any(x in strat_upper for x in ['BEAR', 'SHORT'])
        
        if 'PINNING' in cue_upper or 'FLAT' in cue_upper:
            s5_cue = 7.0 if 'CONDOR' in strat_upper else 5.5
        elif 'UPTREND' in cue_upper:
            s5_cue = 8.0 if is_bull else 2.0
        elif 'DOWNTREND' in cue_upper:
            s5_cue = 8.0 if is_bear else 2.0
        else:
            s5_cue = 5.0
            
        s5_mp = 6.0 if max_pain_ok else 2.0
        s5_koop = 6.0 if koopadvies_ok else 3.0
        s5 = float(np.clip(s5_cue + s5_mp + s5_koop, 0.0, 20.0))

        total_score = round(float(np.clip(s1 + s2 + s3 + s4 + s5, 0.0, 100.0)), 1)

        return {
            'ultimate_ag_score': total_score,
            'score_pop': round(s1, 1),
            'score_roc': round(s2, 1),
            'score_ttp': round(s3, 1),
            'score_safety': round(s4, 1),
            'score_flow': round(s5, 1)
        }


class EarlyAssignmentRiskEngine:
    """
    Quantifies Early Assignment Risk for Short Option Legs (especially Short Puts in Bull Put Spreads).
    
    Principles:
    1. Extrinsic Value Protection:
       An option holder acts economically rational. Exercising an option with remaining extrinsic value
       destroys that time value. Therefore, as long as Extrinsic Value > $0.10, assignment probability is < 1%.
    2. The Danger Zone:
       When a short put is ITM (Spot < Strike_sell) and Extrinsic Value <= $0.10 (and critically <= $0.05),
       the option holder loses nothing by exercising. The probability of early assignment surges (> 50-80%).
    3. Capital Asymmetry (Notional Exposure):
       Upon assignment of 1 contract, the trader must purchase 100 shares at Strike_sell:
       Notional Capital Required = Strike_sell * 100 * Quantity.
       If account cash < Notional Capital, assignment causes immediate severe margin calls.
    """
    @staticmethod
    def calculate_extrinsic_value(spot: float, strike: float, option_price: float, right: str = 'P') -> float:
        """
        Calculates remaining extrinsic (time) value:
        Extrinsic = Option_Price - Intrinsic_Value
        """
        s = float(spot)
        k = float(strike)
        p = float(option_price)
        r = str(right).upper()
        if r.startswith('P'):
            intrinsic = max(0.0, k - s)
        else:
            intrinsic = max(0.0, s - k)
        return max(0.0, p - intrinsic)

    @staticmethod
    def evaluate_assignment_risk(
        spot: float,
        strike_sell: float,
        short_option_price: float,
        right: str = 'P',
        dte: float = 30.0,
        short_delta: float = 0.0,
        account_cash: float = None,
        quantity: int = 1
    ) -> dict:
        """
        Evaluates early assignment risk, probability, notional capital required,
        and provides an action recommendation.
        """
        s = float(spot)
        k_sell = float(strike_sell)
        p_short = float(short_option_price)
        r = str(right).upper()
        qty = max(1, int(quantity))
        
        # Guard: Indien er geen verkochte (short) optiepoot is (k_sell <= 0), is er 0% aanwijzingsrisico
        if k_sell <= 0:
            return {
                'risk_level': 'VEILIG',
                'probability_assignment_pct': 0.0,
                'status_desc': 'Geen geschreven (short) optiepoot aanwezig. Aanwijzingsrisico is niet van toepassing.',
                'action_code': 'HANDHAVEN',
                'extrinsic_value': 0.0,
                'intrinsic_value': 0.0,
                'notional_capital': 0.0,
                'capital_covered': True,
                'capital_warning': '',
                'should_close_now': False
            }
        
        is_put = r.startswith('P')
        is_itm = (s < k_sell) if is_put else (s > k_sell)
        itm_amount = max(0.0, (k_sell - s) if is_put else (s - k_sell))
        
        # Extrinsic value calculation
        extrinsic_val = EarlyAssignmentRiskEngine.calculate_extrinsic_value(s, k_sell, p_short, r)
        
        # Notional assignment capital (cash required to buy 100 shares per contract)
        notional_capital = k_sell * 100.0 * qty
        
        # Assignment probability calculation
        if not is_itm:
            # OTM options have near-zero assignment risk
            prob_assign = 0.05 if (abs(s - k_sell) / max(0.01, k_sell)) < 0.015 and dte <= 2 else 0.01
            risk_level = "VEILIG"
            status_desc = "OTM: Geen toewijzingsrisico (koper heeft geen baat bij uitoefening)."
            action_code = "HANDHAVEN"
        else:
            # ITM: Risk depends heavily on remaining Extrinsic Value
            if extrinsic_val <= 0.05:
                prob_assign = 85.0 if dte <= 5 else 65.0
                risk_level = "CRITICAL"
                status_desc = f"🚨 TIJDWAARDE VERDAMPT (${extrinsic_val:.2f} <= $0.05): Koper verliest niets bij uitoefening. Zeer hoge kans op vervroegde aanwijzing!"
                action_code = "DIRECT_SLUITEN"
            elif extrinsic_val <= 0.10:
                prob_assign = 45.0 if dte <= 7 else 25.0
                risk_level = "WARNING"
                status_desc = f"⚠️ GEVARENZONE TIJDWAARDE (${extrinsic_val:.2f} <= $0.10): Tijdswaarde is minimaal. Sluit de spread om toewijzing voor te zijn."
                action_code = "TIJDIG_SLUITEN"
            elif extrinsic_val <= 0.25:
                prob_assign = 10.0
                risk_level = "ELEVATED"
                status_desc = f"ITM met matige tijdswaarde (${extrinsic_val:.2f}). Toewijzing onwaarschijnlijk maar volg de positie."
                action_code = "MONITOR"
            else:
                prob_assign = 1.0
                risk_level = "LOW_ITM"
                status_desc = f"ITM maar beschermd door ruime tijdswaarde (${extrinsic_val:.2f} > $0.25)."
                action_code = "MONITOR"

        # Delta acceleration
        if abs(float(short_delta)) >= 0.85:
            prob_assign = max(prob_assign, 75.0)
            if risk_level not in ["CRITICAL"]:
                risk_level = "WARNING"

        # Capital coverage check
        capital_covered = True
        capital_warning = ""
        if account_cash is not None and account_cash > 0:
            if account_cash < notional_capital:
                capital_covered = False
                shortfall = notional_capital - account_cash
                capital_warning = f"⚠️ ONVOLDOENDE TOOIWIJZINGSDEKKING: Vereist ${notional_capital:,.0f} cash, beschikbaar is ${account_cash:,.0f} (tekort: ${shortfall:,.0f}). Bij toewijzing dreigt margin call. Kies bij voorkeur Bull Call!"

        return {
            "extrinsic_value": round(extrinsic_val, 2),
            "itm_amount": round(itm_amount, 2),
            "is_itm": is_itm,
            "notional_capital": round(notional_capital, 2),
            "probability_assignment_pct": round(prob_assign, 1),
            "risk_level": risk_level,
            "status_desc": status_desc,
            "action_code": action_code,
            "capital_covered": capital_covered,
            "capital_warning": capital_warning,
            "should_close_now": action_code in ["DIRECT_SLUITEN", "TIJDIG_SLUITEN"]
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

    gt_engine = AntiGravityGammaThetaEngine()
    test_eval = gt_engine.evaluate_strategy_ev_pop(
        spot=100.0,
        strategy_type="IronCondor",
        net_delta=0.02,
        net_gamma=-0.04,
        net_theta_daily=1.20,
        max_profit=150.0,
        max_loss=350.0,
        chain_regime={"cue": "FLAT_PINNING", "support_protected": True, "resistance_protected": True},
        sigma_imp=0.22,
        dte=28.0,
        strikes=[90.0, 95.0, 105.0, 110.0],
        atr_daily=2.0
    )
    print("Gamma/Theta Engine test:", test_eval)

    # Test Early Assignment Risk Engine
    assign_eval = EarlyAssignmentRiskEngine.evaluate_assignment_risk(
        spot=145.0,
        strike_sell=150.0,
        short_option_price=5.08, # intrinsic = 5.0, extrinsic = 0.08 (< 0.10)
        right='P',
        dte=3.0,
        short_delta=-0.88,
        account_cash=5000.0,
        quantity=1
    )
    print("Assignment Risk test (ITM low extrinsic): risk_level=", assign_eval['risk_level'], "prob=", assign_eval['probability_assignment_pct'], "extrinsic=", assign_eval['extrinsic_value'], "covered=", assign_eval['capital_covered'])



