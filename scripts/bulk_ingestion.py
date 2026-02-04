import pandas as pd
import sqlite3
import os
import glob
import struct
import time
import json
import numpy as np
import argparse
import sqlite_vec
from datetime import datetime, timedelta
from openai import OpenAI
from dotenv import load_dotenv, find_dotenv

# --- SETUP ---
dotenv_path = find_dotenv()
if not dotenv_path:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
else:
    load_dotenv(dotenv_path)
    PROJECT_ROOT = os.path.dirname(dotenv_path)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# CONFIGURAZIONE
INPUT_FOLDER = os.path.join(PROJECT_ROOT, "data")
DB_FILE = os.path.join(PROJECT_ROOT, "db", "preventivatore_v2_bulk.db")
VECTOR_BATCH_SIZE = 200

# SOGLIE SMART PRICING ADATTIVO
SIMILARITY_MERGE = 0.98  
SIMILARITY_JUDGE = 0.92  
VOLATILITY_THRESHOLD = 0.5
DEVIATION_THRESHOLD = 0.20 # 20% di variazione fa scattare il trigger
STALENESS_DAYS = 180       # 6 mesi di buco fanno scattare il trigger

# GLOBALS (Configurabili da args)
PRICING_MODE = "SMART_ADAPTIVE" # Options: SMART_ADAPTIVE, MAX, LATEST, SMART_1Y

# MAPPATURA V5 STRICT (O Formato Cliente)
IDX = {
    "ARTICOLO": 0, "DESCRIZIONE": 1, "UM": 2, "Q_COMP": 3,
    "Q_ART": 4, "Q_MAN": 5, "P_COMP": 8, "P_ART": 9,
    "P_MAN": 10, "IMPORTO_TOT": 14
}

def serialize_f32(vector):
    return struct.pack(f"<{len(vector)}f", *vector)

def get_embedding_single(text):
    text = str(text).replace("\n", " ").strip()
    return client.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    try:
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
    except: pass
    return conn

def judge_similarity(new_desc, existing_desc):
    """LLM Judge per decidere Merge vs Branch."""
    if new_desc.lower() == existing_desc.lower():
        return True, "Identical String"
    
    prompt = f"""
    Sei un Senior Engineer MEP.
    Voce A (Database): "{existing_desc}"
    Voce B (Nuova): "{new_desc}"
    
    La Voce B è funzionalmente equivalente alla Voce A (es. stessa funzione, installazione simile) 
    tanto da poter unire i loro storici prezzi? 
    Se cambia solo la marca o un dettaglio minore, rispondi TRUE.
    Se cambia la natura tecnica o la complessità, rispondi FALSE.
    
    Rispondi JSON: {{ "is_merge": true/false, "reason": "..." }}
    """
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0
        )
        data = json.loads(res.choices[0].message.content)
        return data.get("is_merge", False), data.get("reason", "")
    except:
        return False, "Error"

def find_semantic_match(desc, conn):
    vec = get_embedding_single(desc)
    bin_vec = serialize_f32(vec)
    row = conn.execute("""
        SELECT r.id, r.description, v.distance
        FROM vec_recipes v
        JOIN recipes r ON v.rowid = r.id
        WHERE v.embedding MATCH ? AND k = 1
        ORDER BY v.distance ASC
    """, (bin_vec,)).fetchone()
    if row:
        return row[0], row[1], 1 / (1 + row[2])
    return None, None, 0.0

# --- CORE PRICING ENGINE ---

def calculate_smart_adaptive_price(history, now):
    """
    Implementa le regole di Smart Pricing Adattivo:
    1. Deviazione Significativa -> Peso alto all'ultimo prezzo.
    2. Dato Vecchio -> Peso alto all'ultimo prezzo.
    3. Altrimenti -> Media pesata temporale standard.
    """
    if not history: return 0.0
    
    # Ordina per data decrescente (più recente prima)
    # history item: (price, date_obj)
    sorted_hist = sorted(history, key=lambda x: x[1], reverse=True)
    
    latest_price, latest_date = sorted_hist[0]
    
    if len(sorted_hist) == 1:
        return latest_price

    # Calcolo media storica "Reference" (escluso l'ultimo dato)
    # Usiamo pesi temporali standard per il reference
    ref_w_sum = 0.0
    ref_p_sum = 0.0
    
    rest_hist = sorted_hist[1:]
    latest_prev_date = rest_hist[0][1] # Data del penultimo aggiornamento
    
    for price, date_obj in rest_hist:
        days = (now - date_obj).days
        w = 1.0 if days <= 365 else (0.5 if days <= 730 else 0.1)
        ref_p_sum += price * w
        ref_w_sum += w
        
    ref_avg = ref_p_sum / ref_w_sum if ref_w_sum > 0 else 0.0
    
    # --- CHECK TRIGGERS ---
    
    # Trigger 1: Deviazione Significativa
    deviation = abs(latest_price - ref_avg) / ref_avg if ref_avg > 0 else 0.0
    is_significant_deviation = deviation > DEVIATION_THRESHOLD
    
    # Trigger 2: Staleness (Tempo passato dall'ultimo aggiornamento)
    gap_days = (latest_date - latest_prev_date).days
    is_stale = gap_days > STALENESS_DAYS
    
    if is_significant_deviation or is_stale:
        # "Vicino a quest'ultimo" -> Peso dominante (es. 90%)
        # Formula: 0.9 * Latest + 0.1 * Reference
        final_price = (0.9 * latest_price) + (0.1 * ref_avg)
    else:
        # "Mantenere media nell'intorno ultimo aggiornamento" -> Standard Time Weighted
        # Ricalcoliamo includendo latest con il suo peso naturale (che sarà alto essendo recente)
        all_w_sum = 0.0
        all_p_sum = 0.0
        for price, date_obj in sorted_hist:
            days = (now - date_obj).days
            w = 1.0 if days <= 365 else (0.5 if days <= 730 else 0.1)
            all_p_sum += price * w
            all_w_sum += w
        final_price = all_p_sum / all_w_sum
        
    return final_price

def recalc_recipe_stats(recipe_id, conn):
    """
    Ricalcola i prezzi in base alla PRICING_MODE selezionata.
    """
    comps = conn.execute("SELECT id, qty_coefficient, type FROM components WHERE recipe_id=?", (recipe_id,)).fetchall()
    recipe_total = 0.0
    all_prices_for_volatility = []

    for cid, qty, ctype in comps:
        # Fetch history raw
        rows = conn.execute("SELECT raw_price, date FROM price_history WHERE component_id=?", (cid,)).fetchall()
        if not rows: continue
        
        # Parse Dates
        history = []
        now = datetime.now()
        for r_price, r_date_str in rows:
            try: d = datetime.strptime(str(r_date_str), "%Y-%m-%d %H:%M:%S")
            except: d = now
            history.append((r_price, d))
            
        # --- APPLICAZIONE STRATEGIA ---
        new_unit_price = 0.0
        
        if PRICING_MODE == "MAX":
            new_unit_price = max([h[0] for h in history])
            
        elif PRICING_MODE == "LATEST":
            # Sort by date desc, take first
            history.sort(key=lambda x: x[1], reverse=True)
            new_unit_price = history[0][0]
            
        elif PRICING_MODE == "SMART_1Y":
            # Filter last 1 year, then weighted avg
            one_year_ago = now - timedelta(days=365)
            filtered = [h for h in history if h[1] >= one_year_ago]
            if not filtered: # Fallback a latest se vuoto
                filtered = sorted(history, key=lambda x: x[1], reverse=True)[:1]
            
            # Simple average of filtered (or weighted, but sticking to simple for "SMART_1Y" usually implies focus on recency)
            # Let's use weighted standard on the subset
            w_sum = 0; p_sum = 0
            for p, d in filtered:
                w = 1.0
                p_sum += p * w; w_sum += w
            new_unit_price = p_sum / w_sum
            
        else: # DEFAULT: SMART_ADAPTIVE
            new_unit_price = calculate_smart_adaptive_price(history, now)

        # Update Cache
        conn.execute("UPDATE components SET unit_price=?, last_calculated_at=CURRENT_TIMESTAMP WHERE id=?", (new_unit_price, cid))
        
        if ctype != 'MAN':
            recipe_total += new_unit_price * qty
            # Per volatilità usiamo tutto lo storico raw
            all_prices_for_volatility.extend([h[0] * qty for h in history])

    # 2. Volatilità (Sempre calcolata su tutto lo storico per sicurezza)
    if len(all_prices_for_volatility) > 1:
        cv = np.std(all_prices_for_volatility) / np.mean(all_prices_for_volatility) if np.mean(all_prices_for_volatility) > 0 else 0.0
    else:
        cv = 0.0
        
    is_complex = 1 if cv > VOLATILITY_THRESHOLD else 0
    conn.execute("UPDATE recipes SET unit_material_price=?, volatility_index=?, is_complex_assembly=?, last_price_date=CURRENT_TIMESTAMP WHERE id=?", 
                 (recipe_total, cv, is_complex, recipe_id))

# --- INGESTION FLOW ---

def insert_new_recipe(conn, data, filename):
    cur = conn.execute("INSERT INTO recipes (code, description, source_file) VALUES (?,?,?)",
                       (data["code"], data["desc"], filename))
    rid = cur.lastrowid
    for c in data["components"]:
        cur_c = conn.execute("INSERT INTO components (recipe_id, description, type, qty_coefficient, unit_price) VALUES (?,?,?,?,0)",
                             (rid, c['desc'], c['type'], c['qty']))
        conn.execute("INSERT INTO price_history (component_id, raw_price, source_file) VALUES (?,?,?)",
                     (cur_c.lastrowid, c['price'], filename))
    return rid

def merge_into_recipe(conn, rid, data, filename):
    existing_comps = conn.execute("SELECT id, description FROM components WHERE recipe_id=?", (rid,)).fetchall()
    for new_c in data["components"]:
        target_cid = None
        for ecid, edesc in existing_comps:
            if new_c['desc'] in edesc or edesc in new_c['desc']:
                target_cid = ecid
                break
        if not target_cid:
            cur_c = conn.execute("INSERT INTO components (recipe_id, description, type, qty_coefficient, unit_price) VALUES (?,?,?,?,0)",
                                 (rid, new_c['desc'], new_c['type'], new_c['qty']))
            target_cid = cur_c.lastrowid
        conn.execute("INSERT INTO price_history (component_id, raw_price, source_file) VALUES (?,?,?)",
                     (target_cid, new_c['price'], filename))

def process_file(filepath):
    df = pd.read_excel(filepath, header=None, dtype=str)
    filename = os.path.basename(filepath)
    conn = get_db_connection()
    
    curr = None
    foot_hits = 0
    stats = {"branch": 0, "merge": 0}

    def clean(val):
        if pd.isna(val): return None
        s = str(val).strip().replace('€','').replace('.','').replace(',','.')
        try: return float(s)
        except: return None

    for _, row in df.iterrows():
        raw_desc = row[IDX["DESCRIZIONE"]]
        tot = clean(row[IDX["IMPORTO_TOT"]])

        if curr:
            if tot is not None: foot_hits += 1
            if foot_hits >= 2:
                rid, rdesc, sim = find_semantic_match(curr["desc"], conn)
                action = "BRANCH"
                
                if rid:
                    if sim >= SIMILARITY_MERGE: action = "MERGE"
                    elif sim >= SIMILARITY_JUDGE:
                        is_merge, _ = judge_similarity(curr["desc"], rdesc)
                        if is_merge: action = "MERGE"
                
                if action == "BRANCH":
                    rid = insert_new_recipe(conn, curr, filename)
                    stats["branch"] += 1
                else:
                    merge_into_recipe(conn, rid, curr, filename)
                    stats["merge"] += 1
                
                # RECALC with SELECTED STRATEGY
                recalc_recipe_stats(rid, conn)
                
                curr = None; foot_hits = 0; continue

        if not curr and pd.notna(row[IDX["ARTICOLO"]]) and pd.notna(raw_desc) and tot is None:
            curr = {"code": str(row[IDX["ARTICOLO"]]), "desc": str(raw_desc), "components": []}
            foot_hits = 0; continue

        if curr and pd.notna(raw_desc) and tot is None:
            p = clean(row[IDX["P_COMP"]])
            q = clean(row[IDX["Q_COMP"]])
            if p is not None or q is not None:
                is_man = "operaio" in str(raw_desc).lower()
                curr["components"].append({"desc": str(raw_desc), "type": "MAN" if is_man else "MAT", "qty": q or 0, "price": p or 0})

    conn.commit()
    conn.close()
    return stats

def sync_vectors():
    conn = get_db_connection()
    cursor = conn.execute("SELECT r.id, r.description FROM recipes r LEFT JOIN vec_recipes v ON r.id = v.rowid WHERE v.rowid IS NULL")
    while True:
        batch = cursor.fetchmany(VECTOR_BATCH_SIZE)
        if not batch: break
        texts = [str(r[1]).replace("\n", " ").strip() for r in batch]
        try:
            resp = client.embeddings.create(input=texts, model="text-embedding-3-small")
            vec_data = [(batch[i][0], serialize_f32(d.embedding)) for i, d in enumerate(resp.data)]
            conn.executemany("INSERT INTO vec_recipes(rowid, embedding) VALUES(?, ?)", vec_data)
            conn.commit()
            print(f"   -> Synced {len(batch)} vectors.")
        except Exception as e:
            print(f"Error: {e}"); break
    conn.close()

# --- ENTRY POINT ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk Ingestion & Pricing Update")
    parser.add_argument("--override", type=str, choices=["MAX", "LATEST", "SMART_1Y"], 
                        help="Forza una strategia di prezzo specifica (Default: SMART_ADAPTIVE)")
    args = parser.parse_args()
    
    if args.override:
        PRICING_MODE = args.override
        print(f"⚠️  OVERRIDE ATTIVO: Strategia Prezzi impostata su '{PRICING_MODE}'")
    else:
        print(f"ℹ️  Strategia Prezzi Standard: SMART_ADAPTIVE")

    files = glob.glob(os.path.join(INPUT_FOLDER, "*.xlsx"))
    print(f"📦 SMART INGESTION: {len(files)} file.")
    for f in files:
        print(f"Processing {os.path.basename(f)}...")
        s = process_file(f)
        print(f"   -> BRANCH: {s['branch']} | MERGE: {s['merge']}")
    sync_vectors()