import sqlite3
import sys

DB_FILE = "../db/preventivatore_v2.db"

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Per accedere alle colonne per nome
    return conn

def print_recipe_card(conn, recipe_id):
    """Stampa una scheda dettagliata della ricetta e dei suoi componenti"""
    r = conn.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchone()
    comps = conn.execute("SELECT * FROM components WHERE recipe_id = ?", (recipe_id,)).fetchall()
    
    print("\n" + "="*70)
    print(f"🆔 ID DB: {r['id']} | CODICE: {r['code']}")
    print(f"📝 DESCRIZIONE: {r['description']}")
    print("-" * 70)
    print(f"📊 DATI UNITARI (Dal Footer):")
    print(f"   • Costo Materiale:  € {r['unit_material_price']:.2f}")
    print(f"   • Costo Manodopera: € {r['unit_manpower_price']:.2f}")
    print(f"   • Ore/Qta Manod.:   {r['unit_manpower_qty']:.2f}")
    print("-" * 70)
    print(f"🔩 COMPONENTI (BOM - {len(comps)} elementi):")
    
    if not comps:
        print("   (Nessun componente registrato)")
    
    for c in comps:
        # Calcolo costo riga componente (se qty e price esistono)
        tot_row = (c['qty_coefficient'] or 0) * (c['unit_price'] or 0)
        tipo = "[MAN]" if c['type'] == 'MAN' else "[MAT]"
        print(f"   {tipo} {c['description'][:50]:<50} | Qty: {c['qty_coefficient']:<5} | Prz: € {c['unit_price']:<7} | Tot: € {tot_row:.2f}")
    print("="*70)

def interactive_check(conn, query, title):
    """Cicla sui risultati della query e chiede conferma all'utente"""
    ids = conn.execute(query).fetchall()
    count = len(ids)
    
    if count == 0:
        return

    print(f"\n⚠️  TROVATI {count} CASI: {title}")
    choice = input("   Vuoi esaminarli riga per riga? [s/n]: ").lower().strip()
    
    if choice != 's':
        return

    print("\n💡 ISTRUZIONI: Premi [Invio] per il prossimo, [q] per interrompere questa lista.")
    
    for i, row in enumerate(ids):
        print(f"\n--- Caso {i+1} di {count} ---")
        print_recipe_card(conn, row['id'])
        
        user_input = input("👉 [Invio] Prossimo | [q] Esci dalla lista: ").lower().strip()
        if user_input == 'q':
            print("   Interruzione ispezione.")
            break

def run_audit_interactive():
    conn = get_db()
    
    print("🕵️‍♂️  AVVIO AUDIT INTERATTIVO")
    
    # 1. Statistiche Rapide
    n_recipes = conn.execute("SELECT COUNT(*) FROM recipes").fetchone()[0]
    print(f"✅ Database caricato: {n_recipes} ricette totali.")
    
    # 2. Ispezione Ricette a Zero (Ghost Recipes)
    # Query: Materiale 0 AND Manodopera 0
    sql_zeros = "SELECT id FROM recipes WHERE unit_material_price = 0 AND unit_manpower_price = 0"
    interactive_check(conn, sql_zeros, "RICETTE CON PREZZO ZERO (Possibili Note o Titoli)")

    # 3. Ispezione Senza Manodopera
    # Query: Manodopera 0 (Ma Materiale > 0 per non duplicare i casi sopra)
    sql_no_labor = "SELECT id FROM recipes WHERE unit_manpower_price = 0 AND unit_material_price > 0"
    interactive_check(conn, sql_no_labor, "RICETTE SOLA FORNITURA (No Manodopera)")

    print("\n🏁 Audit completato.")
    conn.close()

if __name__ == "__main__":
    run_audit_interactive()