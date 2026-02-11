from datetime import datetime
from src.config import settings

def calculate_smart_price_logic(
    current_db_price: float,
    current_volatility: float,
    last_update_date: datetime | None,
    new_price: float
) -> tuple[float, float]:
    """
    Logica pura di Smart Pricing.
    Non esegue query DB. Riceve lo stato attuale e calcola il nuovo stato.
    
    Returns:
        (final_price, new_volatility)
    """
    
    # 1. Calcolo Giorni di differenza (Obsolescenza)
    days_diff = 0
    if last_update_date:
        days_diff = (datetime.now() - last_update_date).days

    # 2. Calcolo Deviazione (Shock)
    deviation = 0.0
    if current_db_price > 0:
        deviation = abs(new_price - current_db_price) / current_db_price

    final_price = new_price
    new_volatility = current_volatility

    # 3. Trigger Analysis (Using constants from settings)
    is_shock = deviation > settings.DEVIATION_THRESHOLD
    is_stale = days_diff > settings.STALENESS_DAYS
    
    # 4. Ponderazione (Logica Identica all'originale)
    if is_shock or is_stale:
        # Peso 90% al nuovo dato
        w_old, w_new = 0.1, 0.9
        
        if is_shock: 
            new_volatility += settings.VOLATILITY_INCREMENT
            print(f"      ⚡ PRICE SHOCK (+{deviation*100:.1f}%) -> Volatilità aumentata.")
        if is_stale:
            print(f"      🕰️  DATA STALE ({days_diff}gg) -> Aggiornamento prioritario.")
    else:
        # Situazione stabile: media bilanciata
        w_old, w_new = 0.5, 0.5
    
    # 5. Calcolo Finale
    if current_db_price > 0:
        final_price = (current_db_price * w_old) + (new_price * w_new)
    
    return final_price, new_volatility