import sqlite3
import struct
import json
import os
import sys
import sqlite_vec
from openai import OpenAI
from dotenv import load_dotenv, find_dotenv

# --- PATH SETUP INTELLIGENTE ---
dotenv_path = find_dotenv()

if not dotenv_path:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
else:
    load_dotenv(dotenv_path)
    PROJECT_ROOT = os.path.dirname(dotenv_path)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# CONFIGURAZIONE DEFAULT
DB_FILE = os.path.join(PROJECT_ROOT, "db", "preventivatore_v2_bulk.db")
DEFAULT_THRESHOLD = 0.72

def serialize_f32(vector):
    return struct.pack(f"<{len(vector)}f", *vector)

def get_embedding(text):
    text = str(text).replace("\n", " ")
    return client.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    conn.row_factory = sqlite3.Row # Importante per Pandas
    return conn

def sonar_ping(query, threshold):
    conn = get_db()
    
    print(f"\n📡 SONAR PING: '{query}' (Soglia: {threshold})")
    print("   Calcolo embedding...", end="", flush=True)
    
    try:
        query_vec = get_embedding(query)
        query_bin = serialize_f32(query_vec)
        print(" Fatto.")
    except Exception as e:
        print(f"\n❌ Errore API OpenAI: {e}")
        return []

    # Recuperiamo SEMPRE i top 5
    sql = """
        SELECT r.id, r.code, r.description, r.unit_material_price, r.unit_manpower_price, v.distance
        FROM vec_recipes v
        JOIN recipes r ON v.rowid = r.id
        WHERE v.embedding MATCH ? AND k = 5
        ORDER BY v.distance ASC
    """
    
    rows = conn.execute(sql, (query_bin,)).fetchall()
    conn.close()
    
    print(f"\n   Analisi vettoriale (Top 5 vicini):")
    print("-" * 120)
    print(f"   {'SCORE':<8} | {'DIST':<8} | {'ID':<6} | {'P.MAT.':<10} | {'P.MAN.':<10} | {'DESCRIZIONE'}")
    print("-" * 120)
    
    candidates_over_threshold = []

    for row in rows:
        dist = row['distance']
        sim = 1 / (1 + dist)
        
        is_valid = sim >= threshold
        color = "\033[92m" if is_valid else "\033[90m" # Verde / Grigio
        reset = "\033[0m"
        marker = "✅" if is_valid else "❌"
        
        desc_short = (row['description'][:60] + '..') if len(row['description']) > 60 else row['description']
        
        print(f"   {color}{sim:.4f}   | {dist:.4f}   | {row['id']:<6} | {row['unit_material_price']:<10.2f} | {row['unit_manpower_price']:<10.2f} | {desc_short} {marker}{reset}")
        
        if is_valid:
            candidates_over_threshold.append({
                "id": row['id'], 
                "code": row['code'], 
                "desc": row['description'], 
                "score": sim
            })
            
    return candidates_over_threshold

def get_recipe_details(recipe_id):
    conn = get_db()
    # Query per la ricetta (Padre)
    recipe = conn.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchall()
    # Query per i componenti (Figli)
    components = conn.execute("SELECT * FROM components WHERE recipe_id = ?", (recipe_id,)).fetchall()
    conn.close()
    return recipe, components

def check_gpt(query, candidates):
    if not candidates:
        print("\n⚠️  Nessun candidato sopra la soglia. GPT non può essere invocato.")
        return None

    print("\n🤖 GPT VALIDATION REQUEST...")
    
    options_text = ""
    for i, c in enumerate(candidates):
        options_text += f"Opzione {i+1}: {c['desc']} (Score: {c['score']:.2f})\n"

    prompt = f"""
    RDO: "{query}"
    CANDIDATI:
    {options_text}
    
    Seleziona il match tecnico migliore.
    Rispondi JSON: {{ "selected_index": 1, "status": "OK" (o "DIFFERENT"), "reason": "..." }}
    """
    
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0
        )
        content = json.loads(res.choices[0].message.content)
        
        idx = content.get("selected_index", -1)
        reason = content.get("reason", "")
        
        if idx > 0 and idx <= len(candidates):
            selected = candidates[idx-1]
            print(f"   GPT ha scelto: \033[1m{selected['code']}\033[0m")
            print(f"   Motivo: \033[93m{reason}\033[0m")
            return selected
        else:
            print(f"   GPT non ha selezionato nulla di valido. Motivo: {reason}")
            return None

    except Exception as e:
        print(f"❌ Errore GPT: {e}")
        return None

def main():
    print("╔════════════════════════════════════════════════════╗")
    print("║      SONAR DEBUGGER - PREVENTIVATORE AI            ║")
    print("╚════════════════════════════════════════════════════╝")
    
    while True:
        query = input("\n📝 Inserisci descrizione RDO (o 'q' per uscire): ").strip()
        if query.lower() in ['exit', 'quit', 'q']:
            break
        if not query: continue

        thr_input = input(f"🎚️  Soglia (Default {DEFAULT_THRESHOLD}): ").strip()
        try:
            threshold = float(thr_input) if thr_input else DEFAULT_THRESHOLD
        except:
            threshold = DEFAULT_THRESHOLD

        candidates = sonar_ping(query, threshold)
        
        if candidates:
            gpt_choice = input(f"\n🧠 Validare {len(candidates)} candidati con GPT? [y/n]: ").lower().strip()
            
            match = None
            if gpt_choice == 'y':
                match = check_gpt(query, candidates)
            elif len(candidates) == 1:
                print("   (Singolo candidato valido, lo seleziono automaticamente)")
                match = candidates[0]
            
            if match:
                print(f"\n🔎 DETTAGLIO RICETTA SELEZIONATA (ID: {match['id']})")
                
                recipe_rows, comp_rows = get_recipe_details(match['id'])

                # --- VISUALIZZAZIONE RICETTA (Layout a Scheda) ---
                if recipe_rows:
                    r = recipe_rows[0] # Prendo il record
                    print("\n" + "═"*100)
                    print(f"📄  RECIPE MASTER DATA | CODE: \033[1m{r['code']}\033[0m")
                    print("═"*100)
                    print(f"🆔  DB ID:       {r['id']}")
                    print(f"📁  SOURCE:      {r['source_file']}")
                    print(f"💰  PREZZI UNIT: Materiali: € {r['unit_material_price']:.2f}  |  Manodopera: € {r['unit_manpower_price']:.2f}")
                    print("─" * 100)
                    print(f"📝  DESCRIZIONE COMPLETA:\n\033[36m{r['description']}\033[0m") # Ciano per la desc
                    print("═"*100)
                
                # --- VISUALIZZAZIONE COMPONENTI (Layout a Lista Verticale) ---
                if comp_rows:
                    print(f"\n🔩  COMPONENTS LIST ({len(comp_rows)} elementi)")
                    print("─" * 100)
                    
                    calc_tot_mat = 0.0

                    for i, row in enumerate(comp_rows):
                        c = dict(row) # Converto in dict per sicurezza
                        
                        # Dati e Calcoli
                        c_type = c.get('type', 'N/A')
                        c_code = c.get('code', 'N/A')
                        p_unit = c.get('unit_price', 0) or 0
                        qty = c.get('qty_coefficient', 0) or 0
                        subtot = p_unit * qty
                        desc = c.get('description', '')

                        # Accumulo totale (Escludendo manodopera esplicita se necessario)
                        if c_type != 'MAN':
                            calc_tot_mat += subtot

                        # Stampa Blocco Componente
                        print(f"   🔹 ITEM #{i+1} [{c_type}] code: \033[1m{c_code}\033[0m")
                        print(f"       ├─ Quantità: {qty}")
                        print(f"       ├─ Prezzo Unitario: € {p_unit:.2f}")
                        print(f"       ├─ Subtotale:       € {subtot:.2f}")
                        print(f"       └─ Descrizione:     {desc}")
                        print("   " + "."*60)
                    
                    print(f"\n   📊 VERIFICA SOMMA COMPONENTI (Materiali): \033[1m€ {calc_tot_mat:.2f}\033[0m")
                    print("─" * 100)

        else:
            print("   (Nessun candidato valido per GPT)")

if __name__ == "__main__":
    main()