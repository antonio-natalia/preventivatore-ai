import sqlite3
import pandas as pd

DB_FILE = "../db/preventivatore_v2.db"

def run_audit():
    print("🕵️‍♂️  AVVIO AUDIT DATA QUALITY...")
    conn = sqlite3.connect(DB_FILE)
    
    # --- 1. SMOKE TEST (Controlli di base) ---
    print("\n--- 1. SMOKE TEST ---")
    
    n_recipes = conn.execute("SELECT COUNT(*) FROM recipes").fetchone()[0]
    n_components = conn.execute("SELECT COUNT(*) FROM components").fetchone()[0]
    
    print(f"✅ Ricette Totali: {n_recipes}")
    print(f"✅ Componenti Totali: {n_components}")
    
    if n_recipes == 0:
        print("❌ CRITICO: Nessuna ricetta importata. Controlla il parser.")
        return

    # Cerchiamo "Ricette Fantasma" (Prezzo 0 o Descrizione Vuota)
    ghosts = conn.execute("""
        SELECT COUNT(*) FROM recipes 
        WHERE unit_material_price = 0 AND unit_manpower_price = 0
    """).fetchone()[0]
    
    if ghosts > 0:
        print(f"⚠️  WARNING: {ghosts} ricette hanno prezzo materiale E manodopera a zero.")
    else:
        print("✅ Nessuna ricetta a prezzo zero trovata.")

    # --- 2. CONSISTENCY CHECK (Padre vs Figli) ---
    print("\n--- 2. CONSISTENCY CHECK (Padre vs Somma Figli) ---")
    # Verifichiamo se: Prezzo Materiale Padre ~ Somma(Prezzo Comp * Coeff) dei figli tipo MAT
    
    sql_check = """
    SELECT 
        r.id, r.code, r.description,
        r.unit_material_price as P_PADRE,
        SUM(CASE WHEN c.type = 'MAT' THEN c.unit_price * c.qty_coefficient ELSE 0 END) as P_CALCOLATO,
        r.unit_material_price - SUM(CASE WHEN c.type = 'MAT' THEN c.unit_price * c.qty_coefficient ELSE 0 END) as DELTA
    FROM recipes r
    LEFT JOIN components c ON r.id = c.recipe_id
    GROUP BY r.id
    HAVING abs(DELTA) > 0.1  -- Tolleranza di 10 centesimi
    ORDER BY abs(DELTA) DESC
    LIMIT 5
    """
    
    anomalies = pd.read_sql_query(sql_check, conn)
    
    if anomalies.empty:
        print("✅ INTEGRITÀ PERFETTA: I prezzi dei padri corrispondono alla somma dei componenti.")
    else:
        print(f"❌ TROVATE {len(anomalies)} INCONGRUENZE DI CALCOLO.")
        print("Ecco le 5 peggiori (Il parser potrebbe aver letto la colonna sbagliata o mancano figli):")
        print(anomalies.to_string())
        print("\nNOTE: Se P_CALCOLATO è 0, significa che non abbiamo catturato i figli per quella ricetta.")

    # --- 3. MANPOWER CHECK ---
    print("\n--- 3. MANPOWER CHECK ---")
    # Verifichiamo che la manodopera sia stata catturata
    no_labor = conn.execute("SELECT COUNT(*) FROM recipes WHERE unit_manpower_price = 0").fetchone()[0]
    pct_no_labor = (no_labor / n_recipes) * 100
    
    print(f"Info: {no_labor} ricette ({pct_no_labor:.1f}%) non hanno costi di manodopera espliciti nel footer.")
    if pct_no_labor > 50:
        print("⚠️  WARNING: Alta percentuale senza manodopera. Verifica se la colonna P. MAN UNITARIO è corretta.")

    conn.close()

if __name__ == "__main__":
    run_audit()