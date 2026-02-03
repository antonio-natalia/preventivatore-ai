import sqlite3
import sys
import os

DB_FILE = "../db/preventivatore_v2_bulk.db"

def get_db():
    if not os.path.exists(DB_FILE):
        print(f"❌ Errore: Database '{DB_FILE}' non trovato.")
        sys.exit(1)
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Per accedere alle colonne per nome
    return conn

def inspect_recipe_by_code(target_id):
    conn = get_db()
    
    print(f"\n🔎 ISPEZIONE RICETTA: '{target_id}'")
    
    # 1. Cerchiamo il Padre (Recipe)
    # Nota: Potrebbero esserci più ricette con lo stesso codice provenienti da file diversi
    recipes = conn.execute("SELECT * FROM recipes WHERE id = ?", (target_id,)).fetchall()
    
    if not recipes:
        print("❌ Nessuna ricetta trovata con questo codice.")
        conn.close()
        return

    print(f"   Trovate {len(recipes)} varianti nel database.\n")

    for i, r in enumerate(recipes):
        r_id = r["id"]
        
        # 2. Cerchiamo i Figli (Components)
        comps = conn.execute("SELECT * FROM components WHERE recipe_id = ?", (r_id,)).fetchall()
        
        print("="*80)
        print(f"VARIANTE #{i+1} | ID DB: {r['id']}")
        print(f"📄 Fonte:      {r['source_file']}")
        print(f"📝 Descrizione: {r['description']}")
        print("-" * 80)
        print(f"💰 DATI ECONOMICI (Header/Footer):")
        print(f"   • Prezzo Materiale Unitario:  € {r['unit_material_price']:,.2f}" if r['unit_material_price'] else "   • Prezzo Materiale Unitario:  N/A")
        print(f"   • Prezzo Manodopera Unitario: € {r['unit_manpower_price']:,.2f}" if r['unit_manpower_price'] else "   • Prezzo Manodopera Unitario: N/A")
        print(f"   • Ore Manodopera:             {r['unit_manpower_qty']:.2f} h" if r['unit_manpower_qty'] else "   • Ore Manodopera:             N/A")
        
        print(f"\n🔩 DISTINTA BASE (BOM) - {len(comps)} Componenti:")
        print(f"   {'TIPO':<5} | {'QTY':<8} | {'PREZZO UN.':<12} | {'TOTALE RIGA':<12} | {'DESCRIZIONE'}")
        print("   " + "-"*75)
        
        tot_calc_mat = 0.0
        tot_calc_man = 0.0
        
        for c in comps:
            qty = c['qty_coefficient'] or 0
            price = c['unit_price'] or 0
            row_tot = qty * price
            
            tipo = "MAN" if c['type'] == 'MAN' else "MAT"
            if tipo == "MAT": tot_calc_mat += row_tot
            
            print(f"   {tipo:<5} | {qty:<8.3f} | € {price:<10.2f} | € {row_tot:<10.2f} | {c['description']}")

        print("-" * 80)
        
        # 3. Data Quality Check (Consistency)
        # Verifichiamo se la somma dei componenti corrisponde al prezzo del padre
        delta = abs(r['unit_material_price'] - tot_calc_mat)
        status = "✅ OK" if delta < 0.05 else f"⚠️  DISCREPANZA (€ {delta:.2f})"
        
        print(f"⚖️  CONSISTENCY CHECK (Materiali):")
        print(f"   Prezzo Padre: € {r['unit_material_price']:.2f}")
        print(f"   Somma Figli:  € {tot_calc_mat:.2f}")
        print(f"   Stato:        {status}")
        print("="*80 + "\n")

    conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Se passo l'id come argomento: python debug_inspect_recipe.py 10.05.001
        id = sys.argv[1]
        inspect_recipe_by_code(id)
    else:
        # Modalità interattiva
        while True:
            user_input = input("Inserisci Codice Articolo (o 'q' per uscire): ").strip()
            if user_input.lower() in ['q', 'exit']:
                break
            if user_input:
                inspect_recipe_by_code(user_input)