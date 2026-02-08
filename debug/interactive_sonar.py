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

# CONFIGURAZIONE
DB_FILE = os.path.join(PROJECT_ROOT, "db", "preventivatore_v3_smart.db")
DEFAULT_THRESHOLD = 0.72

# --- PROMPT CENTRALIZZATO ---
PROMPT_VALIDATION_TEXT = """
Sei un preventivista edile esperto.
Confronta la richiesta dell'utente (INPUT) con i candidati trovati nel database (DB).

INPUT: "{user_query}"

CANDIDATI DAL DB:
{cand_str}

COMPITI:
1. Identifica il candidato tecnicamente compatibile.
2. Se la similarità è alta ma c'è ambiguità, marca come "CHECK".
3. Se nessuno è compatibile, marca come "NOMATCH".

OUTPUT JSON:
{{
    "selected_index": <int o -1 se nessuno>,
    "status": "<MATCH | CHECK | NOMATCH>",
    "reason": "<spiegazione breve>"
}}
"""

def serialize_f32(vector):
    return struct.pack(f"<{len(vector)}f", *vector)

def get_embedding(text):
    text = str(text).replace("\n", " ")
    return client.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding

def get_db():
    conn = sqlite3.connect(DB_FILE)
    try:
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
    except Exception as e:
        print(f"❌ Errore caricamento sqlite_vec: {e}")
        # sys.exit(1) # Commentato per debug se manca libreria locale su alcuni OS
    conn.row_factory = sqlite3.Row 
    return conn

def sonar_ping(query, threshold):
    conn = get_db()
    
    print(f"\n📡 SONAR PING: '{query}' (Soglia visualizzazione: {threshold})")
    print("   Calcolo embedding...", end="", flush=True)
    
    try:
        query_vec = get_embedding(query)
        query_bin = serialize_f32(query_vec)
        print(" Fatto.")
    except Exception as e:
        print(f"\n❌ Errore API OpenAI: {e}")
        return []

    # Recuperiamo i top 5 con TUTTI i campi V3 utili
    sql = """
        SELECT 
            r.id, 
            r.code, 
            r.description, 
            r.unit_material_price, 
            r.unit_manpower_price, 
            r.source_file,
            r.volatility_index,
            r.is_complex_assembly,
            v.distance
        FROM vec_recipes v
        JOIN recipes r ON v.rowid = r.id
        WHERE v.embedding MATCH ? AND k = 5
        ORDER BY v.distance ASC
    """
    
    try:
        rows = conn.execute(sql, (query_bin,)).fetchall()
    except Exception as e:
        print(f"\n❌ Errore Query Vettoriale: {e}")
        conn.close()
        return []
        
    conn.close()
    
    print(f"\n   Analisi vettoriale (Top 5 vicini):")
    print("-" * 140)
    # Header Tabella
    print(f"   {'SCORE':<8} | {'ID':<6} | {'P.MAT.':<9} | {'P.MAN.':<9} | {'VOLAT.':<6} | {'SOURCE':<15} | {'DESCRIZIONE'}")
    print("-" * 140)
    
    candidates = []

    for row in rows:
        dist = row['distance']
        sim = 1 / (1 + dist)
        
        if sim < threshold:
            continue
        
        # Color Coding
        color = "\033[92m" if sim > 0.85 else "\033[90m" # Verde / Grigio scuro
        reset = "\033[0m"
        
        # --- FIX: SANITIZZAZIONE DATI (Gestione NULL) ---
        p_mat = row['unit_material_price'] or 0.0
        p_man = row['unit_manpower_price'] or 0.0
        vol_idx = row['volatility_index'] or 0.0
        is_complex = row['is_complex_assembly']
        source_file = row['source_file'] or "N/A"
        description = row['description'] or "[No Description]"
        row_id = row['id']
        row_code = row['code'] or ""

        # Formattazione stringhe sicura
        vol_str = f"{vol_idx:.2f}"
        if is_complex: vol_str = f"\033[91m{vol_str}!\033[0m" # Rosso se complesso
        
        desc_short = (description[:50] + '..') if len(description) > 50 else description   
        src_short = str(source_file)[:15]

        # Stampa sicura con variabili pulite
        print(f"   {color}{sim:.4f}   | {row_id:<6} | {p_mat:<9.2f} | {p_man:<9.2f} | {vol_str:<6} | {src_short:<15} | {desc_short}{reset}")
        
        candidates.append({
            "id": row_id, 
            "code": row_code, 
            "desc": description, 
            "score": sim,
            "source": source_file,
            "p_mat": p_mat,
            "p_man": p_man
        })
            
    return candidates

def get_recipe_details(recipe_id):
    conn = get_db()
    # Query per la ricetta (Padre)
    recipe = conn.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchall()
    # Query per i componenti (Figli) con check tabella
    try:
        components = conn.execute("SELECT * FROM components WHERE recipe_id = ?", (recipe_id,)).fetchall()
    except:
        components = []
    conn.close()
    return recipe, components

def check_gpt(query, candidates):
    if not candidates:
        print("\n⚠️  Nessun candidato trovato.")
        return None

    print("\n🤖 GPT VALIDATION REQUEST (Prompt Centralizzato V3)...")
    
    cand_str = ""
    for i, c in enumerate(candidates):
        cand_str += f"[{i}] {c['desc']} (Score: {c['score']:.2f} | Src: {c['source']})\n"

    try:
        formatted_prompt = PROMPT_VALIDATION_TEXT.format(user_query=query, cand_str=cand_str)
        
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": formatted_prompt}],
            response_format={"type": "json_object"},
            temperature=0
        )
        content = json.loads(res.choices[0].message.content)
        
        idx = content.get("selected_index", -1)
        status = content.get("status", "UNKNOWN")
        reason = content.get("reason", "")
        
        print(f"   GPT Response: Status=\033[1m{status}\033[0m | Index={idx}")
        print(f"   Reason: \033[93m{reason}\033[0m")

        if idx is not None and isinstance(idx, int) and 0 <= idx < len(candidates):
            return candidates[idx]
        else:
            return None

    except Exception as e:
        print(f"❌ Errore GPT: {e}")
        return None

def main():
    print("╔════════════════════════════════════════════════════╗")
    print("║      SONAR V3 - PREVENTIVATORE INTELLIGENTE        ║")
    print("╚════════════════════════════════════════════════════╝")
    print(f"💾 DB Target: {os.path.basename(DB_FILE)}")
    
    while True:
        query = input("\n📝 Inserisci descrizione RDO (o 'q' per uscire): ").strip()
        if query.lower() in ['exit', 'quit', 'q']:
            break
        if not query: continue

        thr_input = input(f"🎚️  Soglia visualizzazione (Default {DEFAULT_THRESHOLD}): ").strip()
        try:
            threshold = float(thr_input) if thr_input else DEFAULT_THRESHOLD
        except:
            threshold = DEFAULT_THRESHOLD

        candidates = sonar_ping(query, threshold)
        
        if candidates:
            action = input(f"\n[G]PT Validate | [M]anual Select ID | [ENTER] Skip: ").lower().strip()
            
            match = None
            if action == 'g':
                match = check_gpt(query, candidates)
            elif action == 'm':
                sel_id = input("Inserisci ID da esplorare: ")
                if sel_id.isdigit():
                    match = next((c for c in candidates if str(c['id']) == sel_id), None)
                    if not match: match = {'id': int(sel_id)} 
            
            if match:
                r_id = match['id']
                print(f"\n🔎 DRILL-DOWN RICETTA (ID: {r_id})")
                
                recipe_rows, comp_rows = get_recipe_details(r_id)

                if recipe_rows:
                    r = recipe_rows[0]
                    # Sanitizzazione Dati Drill-Down
                    p_mat = r['unit_material_price'] or 0.0
                    p_man = r['unit_manpower_price'] or 0.0
                    vol_idx = r['volatility_index'] or 0.0
                    last_date = r['last_price_date'] or "N/A"
                    source_f = r['source_file'] or "N/A"
                    desc_full = r['description'] or ""
                    code_full = r['code'] or "NO-CODE"
                    
                    vol_alert = "⚠️ HIGH" if vol_idx > 0.5 else "LOW"
                    
                    print("\n" + "═"*120)
                    print(f"📄  RECIPE MASTER DATA | CODE: \033[1m{code_full}\033[0m")
                    print("═"*120)
                    print(f"📁  SOURCE FILE: {source_f}")
                    print(f"📅  LAST UPDATE: {last_date}")
                    print(f"📊  VOLATILITY:  {vol_idx:.4f} ({vol_alert}) | COMPLEX: {r['is_complex_assembly']}")
                    print("─" * 120)
                    print(f"💰  PREZZI UNITARI AGGREGATI:")
                    print(f"    🧱 Materiali:  € {p_mat:.2f}")
                    print(f"    👷 Manodopera: € {p_man:.2f}")
                    print("─" * 120)
                    print(f"📝  DESCRIZIONE:\n\033[36m{desc_full}\033[0m")
                    print("═"*120)
                
                if comp_rows:
                    print(f"\n🔩  BOM (Bill of Materials) - {len(comp_rows)} Elementi")
                    print("─" * 120)
                    print(f"   {'TYPE':<5} | {'QTY':<8} | {'UNIT PRICE':<12} | {'SUBTOTAL':<12} | {'DESCRIPTION'}")
                    print("─" * 120)
                    
                    calc_tot = 0.0

                    for row in comp_rows:
                        c = dict(row)
                        c_type = c.get('type', 'N/A') or 'N/A'
                        p_unit = c.get('unit_price', 0) or 0
                        qty = c.get('qty_coefficient', 0) or 0
                        subtot = p_unit * qty
                        desc = c.get('description', '') or ""
                        
                        calc_tot += subtot
                        
                        print(f"   {c_type:<5} | {qty:<8.2f} | € {p_unit:<10.2f} | € {subtot:<10.2f} | {desc}")

                    print("─" * 120)
                    print(f"   ∑ CHECK SOMMA (Tutti i componenti): \033[1m€ {calc_tot:.2f}\033[0m")
                    print("─" * 120)

if __name__ == "__main__":
    main()