import os
import json
import datetime
import pandas as pd
from typing import Dict, Any, Optional

class StockProfileManager:
    """
    Beheert de aandeel-specifieke optimalisatieprofielen (persistent in JSON).
    Slaat per aandeel de optimale EM-multiplier, spread-breedte, DTE-range,
    winstdoelen (profit targets) en validatiedata op.
    """
    DEFAULT_FILENAME = "stock_profiles.json"

    def __init__(self, filepath: Optional[str] = None):
        if filepath:
            self.filepath = filepath
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.join(base_dir, "data")
            os.makedirs(data_dir, exist_ok=True)
            self.filepath = os.path.join(data_dir, self.DEFAULT_FILENAME)

    def load_profiles(self) -> Dict[str, Any]:
        """Laadt alle opgeslagen profielen in uit het JSON-bestand."""
        if not os.path.exists(self.filepath):
            return {}
        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Fout bij het laden van {self.filepath}: {e}")
            return {}

    def save_profiles(self, profiles: Dict[str, Any]) -> bool:
        """Slaat het volledige profielen-woordenboek op naar het JSON-bestand."""
        try:
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(profiles, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"⚠️ Fout bij het opslaan van {self.filepath}: {e}")
            return False

    def get_profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Haalt het specifieke profiel op voor een aandeel/symbool."""
        if not symbol:
            return None
        profiles = self.load_profiles()
        return profiles.get(symbol.upper().strip())

    def save_profile(self, symbol: str, profile_data: Dict[str, Any]) -> bool:
        """Slaat of werkt het profiel voor één specifiek symbool bij."""
        if not symbol:
            return False
        sym = symbol.upper().strip()
        profiles = self.load_profiles()

        # Zorg voor basisdefaults
        today_str = datetime.date.today().strftime('%Y-%m-%d')
        em_mult = profile_data.get('best_em_multiplier', 1.44)

        # Bereken automatisch profit target indien niet opgegeven
        profit_target = profile_data.get('profit_target_pct')
        if profit_target is None:
            if em_mult <= 1.20:
                profit_target = 50.0  # Strakke EM: sneller winst borgen
            elif em_mult >= 1.60:
                profit_target = 75.0  # Diepe EM: maximale premie-uitfasering
            else:
                profit_target = 65.0  # Gebalanceerd

        clean_data = {
            'symbol': sym,
            'best_em_multiplier': float(em_mult),
            'best_width': float(profile_data.get('best_width', 10.0)),
            'best_min_dte': int(profile_data.get('best_min_dte', 14)),
            'best_max_dte': int(profile_data.get('best_max_dte', 25)),
            'best_min_bep_dist': float(profile_data.get('best_min_bep_dist', 6.0)),
            'profit_target_pct': float(profit_target),
            'last_sweep_date': profile_data.get('last_sweep_date', today_str),
            'hit_rate': float(profile_data.get('hit_rate', 0.0)),
            'avg_pnl': float(profile_data.get('avg_pnl', 0.0)),
            'total_trades': int(profile_data.get('total_trades', 0)),
            'status': profile_data.get('status', 'VALID'),
            'notes': profile_data.get('notes', 'Geoptimaliseerd via Auto-Optimalisatie')
        }

        profiles[sym] = clean_data
        return self.save_profiles(profiles)

    def is_expired(self, symbol: str, max_age_days: int = 30) -> bool:
        """
        Controleert of een profiel ontbreekt of ouder is dan het opgegeven aantal dagen.
        Geeft True als een her-kalibratie (refresh) nodig is.
        """
        if not symbol:
            return True
        prof = self.get_profile(symbol)
        if not prof:
            return True
        
        date_str = prof.get('last_sweep_date')
        if not date_str:
            return True
        
        try:
            sweep_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            today = datetime.date.today()
            age_days = (today - sweep_date).days
            return age_days > max_age_days
        except Exception:
            return True

    def get_profit_target_pct(self, symbol: str) -> float:
        """
        Geeft het dynamische winstdoelpercentage terug voor een specifiek aandeel.
        Standaard 60.0% indien geen profiel aanwezig.
        """
        prof = self.get_profile(symbol)
        if not prof:
            return 60.0
        return float(prof.get('profit_target_pct', 60.0))

    def get_all_profiles(self) -> Dict[str, Any]:
        """Geeft alle opgeslagen profielen terug."""
        return self.load_profiles()

    def delete_profile(self, symbol: str) -> bool:
        """Verwijdert een profiel."""
        if not symbol:
            return False
        sym = symbol.upper().strip()
        profiles = self.load_profiles()
        if sym in profiles:
            del profiles[sym]
            return self.save_profiles(profiles)
        return False

    def to_dataframe(self) -> pd.DataFrame:
        """Exporteert alle profielen als een overzichtelijke DataFrame voor Streamlit."""
        profiles = self.load_profiles()
        if not profiles:
            return pd.DataFrame()
        df = pd.DataFrame.from_dict(profiles, orient='index')
        return df
