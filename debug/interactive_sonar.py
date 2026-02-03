import sqlite3
import struct
import json
import os
import sys
import sqlite_vec
from openai import OpenAI
from dotenv import load_dotenv

# Carica variabili d'ambiente
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# CONFIGURAZIONE DEFAULT
DB_FILE = "../db/preventivatore_v2_bulk.db"
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

    # Recuperiamo SEMPRE i top 5, indipendentemente dalla soglia, per vedere cosa c'è vicino
    sql = """
        SELECT r.id, r.code, r.description, r.unit_material_price, r.unit_manpower_price, v.distance
        FROM vec_recipes v
        JOIN recipes r ON v.rowid = r.id
        WHERE v.embedding MATCH ? AND k = 5
        ORDER BY v.distance ASC
    """
    
    rows = conn.execute(sql, (query_bin,)).fetchall()
    conn.close()
    
    results = []
    print(f"   Analisi vettoriale (Top 5 vicini):")
    print("-" * 120)
    print(f"   {'SCORE':<8} | {'DIST':<8} | {'ID':<10} | {'P.MAT.':<10} | {'P.MAN.':<10} | {'DESCRIZIONE'}")
    print("-" * 120)
    
    candidates_over_threshold = []

    for row in rows:
        dist = row[5]
        sim = 1 / (1 + dist)
        
        # Visualizzazione Console
        is_valid = sim >= threshold
        color = "\033[92m" if is_valid else "\033[90m" # Verde se valido, Grigio scuro se scartato
        reset = "\033[0m"
        marker = "✅" if is_valid else "❌"
        
        print(f"   {color}{sim:.4f}   | {dist:.4f}   | {row[0]:<10} | {row[3]:<10} | {row[4]:<10} | {row[2][:50]}... {marker}{reset}")
        
        if is_valid:
            candidates_over_threshold.append({
                "id": row[0], "code": row[1], "desc": row[2], "score": sim
            })
            
    return candidates_over_threshold

def get_components(recipe_id):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    comps = conn.execute("SELECT * FROM components WHERE recipe_id = ?", (recipe_id,)).fetchall()
    conn.close()
    return comps

def get_recipes(recipe_id):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    recipes = conn.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchall()
    conn.close()
    return recipes

def check_gpt(query, candidates):
    if not candidates:
        print("\n⚠️  Nessun candidato sopra la soglia. GPT non può essere invocato.")
        return

    print("\n🤖 GPT VALIDATION REQUEST")
    
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
        status = content.get("status", "UNKNOWN")
        reason = content.get("reason", "")
        
        print(f"   GPT Response: Index {idx} | Status: {status}")
        print(f"   Reason: \033[93m{reason}\033[0m") # Giallo per la motivazione
        return candidates[idx-1] if idx > 0 and idx <= len(candidates) else None

    except Exception as e:
        print(f"❌ Errore GPT: {e}")

def main():
    print("╔════════════════════════════════════════════════════╗")
    print("║      SONAR DEBUGGER - PREVENTIVATORE AI            ║")
    print("╚════════════════════════════════════════════════════╝")
    print("Digita 'exit' per uscire.")
    
    while True:
        # 1. INPUT QUERY
        query = input("\n📝 Inserisci descrizione RDO: ").strip()
        if query.lower() in ['exit', 'quit', 'q']:
            print("👋 Bye.")
            break
        if not query: continue

        # 2. INPUT SOGLIA
        thr_input = input(f"🎚️  Soglia (Default {DEFAULT_THRESHOLD}): ").strip()
        try:
            threshold = float(thr_input) if thr_input else DEFAULT_THRESHOLD
        except:
            print("⚠️ Valore non valido, uso default.")
            threshold = DEFAULT_THRESHOLD

        # 3. ESECUZIONE VETTORIALE
        candidates = sonar_ping(query, threshold)
        
        # 4. INPUT GPT
        if candidates:
            gpt_choice = input(f"🧠 Vuoi validare i {len(candidates)} candidati validi con GPT? [y/n]: ").lower().strip()
            if gpt_choice == 'y':
                match = check_gpt(query, candidates)
                recipes = get_recipes(match['id'])
                if recipes:
                    print("\n🔍 Recupero ricetta id: ", match['id'])
                    for r in recipes:
                        print(f"\n✅ MATCH TROVATO: {r['code']} - {r['description']}")
                        print(dict(r))
                        print(f"   Prezzo Articolo: {r['unit_material_price']} - Prezzo Manodopera{r['unit_manpower_price']}")
                else:
                    print("   (Nessuna ricetta trovata per questo match.)")
                components = get_components(match['id'])
                if components:
                    print("\n🔍 Recupero componenti")
                    for c in components:
                        print(f"\n✅ MATCH TROVATO: {c['code']} - {c['description']}")
                        print(f"   Prezzo Articolo: {c['unit_price']}")
                else:
                    print("   (Nessun componente trovato per questo match.)")
        else:
            print("   (Nessun candidato valido per GPT)")

if __name__ == "__main__":
    main()