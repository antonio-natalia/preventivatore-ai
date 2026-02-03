import pandas as pd
import sqlite3
import os
import glob
import hashlib
import struct
import time
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

# --- CONFIGURAZIONE ---
INPUT_FOLDER = os.path.join(PROJECT_ROOT, "data")
DB_FILE = os.path.join(PROJECT_ROOT, "db", "preventivatore_v3.db")
VECTOR_BATCH_SIZE = 200 # Quante ricette processare per volta

# MAPPATURA V5 (STRICT)
IDX = {
    "ARTICOLO": 0, "DESCRIZIONE": 1, "UM": 2,
    "Q_COMP": 3, "Q_ART": 4, "Q_MAN": 5,
    "P_COMP": 8, "P_ART": 9, "P_MAN": 10,
    "IMPORTO_TOT": 14
}

def serialize_f32(vector):
    return struct.pack(f"<{len(vector)}f", *vector)

# --- UTILS DATABASE ---

def init_db():  
    print(f"🔧 SYSTEM CHECK: Verifica integrità schema su {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Tabelle Dati
    c.execute('''CREATE TABLE IF NOT EXISTS recipes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT, description TEXT,
        unit_material_price REAL, unit_manpower_price REAL,
        source_file TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS components (
        id INTEGER PRIMARY KEY AUTOINCREMENT, recipe_id INTEGER,
        code TEXT, description TEXT, type TEXT, qty_coefficient REAL, unit_price REAL,
        FOREIGN KEY(recipe_id) REFERENCES recipes(id)
    )''')

    # Tabella Tracking File (Idempotenza)
    c.execute('''CREATE TABLE IF NOT EXISTS ingested_files (
        filename TEXT PRIMARY KEY,
        file_hash TEXT,
        import_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT, -- 'SUCCESS', 'ERROR', 'SKIPPED'
        recipes_count INTEGER
    )''')
    
    # Caricamento Estensione e Tabella Vettori
    try:
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        c.execute("CREATE VIRTUAL TABLE IF NOT EXISTS vec_recipes USING vec0(embedding float[1536])")
    except Exception as e:
        print(f"❌ ERRORE CRITICO sqlite-vec: {e}")
        exit()
        
    conn.commit()
    conn.close()
    print("✅ Schema integro. Pronto per l'ingestion.")

def get_file_hash(filepath):
    hasher = hashlib.md5()
    try:
        with open(filepath, 'rb') as f:
            # Leggiamo a chunk anche il file per non saturare la RAM su file enormi
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return None

def check_file_status(filename, file_hash):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.execute("SELECT file_hash, status FROM ingested_files WHERE filename = ?", (filename,))
    row = cur.fetchone()
    conn.close()
    
    if row:
        stored_hash, status = row
        if stored_hash == file_hash and status == 'SUCCESS':
            return "SKIP" # Già fatto e non cambiato
        if stored_hash != file_hash:
            return "UPDATE" # Esiste ma cambiato
    return "NEW"

def log_ingestion_result(filename, file_hash, status, count=0):
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        INSERT INTO ingested_files (filename, file_hash, status, recipes_count, import_date)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(filename) DO UPDATE SET
            file_hash=excluded.file_hash,
            status=excluded.status,
            recipes_count=excluded.recipes_count,
            import_date=CURRENT_TIMESTAMP
    """, (filename, file_hash, status, count))
    conn.commit()
    conn.close()

# --- PARSING ---
def clean_float(val):
    if pd.isna(val) or str(val).strip() == "": return None
    s = str(val).strip().replace('€', '').strip()
    if ',' in s and '.' in s: s = s.replace('.', '').replace(',', '.')
    elif ',' in s: s = s.replace(',', '.')
    try: return float(s)
    except: return None

def is_populated(val):
    return pd.notna(val) and str(val).strip() != ""

def process_excel_file(filepath):
    """
    Parsing V5 Strict.
    Restituisce il numero di ricette salvate o solleva eccezione.
    """
    df = pd.read_excel(filepath, header=None, dtype=str)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    filename_short = os.path.basename(filepath)
    current_recipe = None
    footer_hits = 0 # Contatore per la regola "almeno due volte importo totale"
    saved_count = 0
    
    # Per sicurezza cancelliamo vecchie ricette di questo file se era un UPDATE
    c.execute("DELETE FROM recipes WHERE source_file = ?", (filename_short,))
    # Nota: SQLite con FK ON DELETE CASCADE pulirebbe i componenti, ma per sicurezza...
    # (Qui assumiamo un DB semplice. In prod useremmo una transazione esplicita)

    for i, row in df.iterrows():
        raw_art = row[IDX["ARTICOLO"]]
        raw_desc = row[IDX["DESCRIZIONE"]]
        val_tot = clean_float(row[IDX["IMPORTO_TOT"]])
        
        # 1. Chiusura
        if current_recipe:
            if val_tot is not None: footer_hits += 1
            if footer_hits >= 2:
                u_art = clean_float(row[IDX["P_ART"]]) or 0.0
                u_man = clean_float(row[IDX["P_MAN"]]) or 0.0
                # Insert Ricetta
                c.execute("INSERT INTO recipes (code, description, unit_article_price, unit_manpower_price, source_file) VALUES (?,?,?,?,?)",
                          (current_recipe["code"], current_recipe["desc"], u_art, u_man, filename_short))
                rid = c.lastrowid
                # Insert Componenti - Non inseriamo codice articolo per i componenti, in quanto non a disposizione
                for comp in current_recipe["components"]:
                    c.execute("INSERT INTO components (recipe_id, description, type, qty_coefficient, unit_price) VALUES (?,?,?,?,?,?)",
                              (rid, comp['desc'], comp['type'], comp['qty'], comp['price']))
                
                saved_count += 1
                current_recipe = None
                footer_hits = 0
                continue
        
        # 2. Apertura
        if not current_recipe:
            if is_populated(raw_art) and is_populated(raw_desc) and val_tot is None:
                 current_recipe = {"code": str(raw_art).strip(), "desc": str(raw_desc).strip(), "components": []}
                 footer_hits = 0
                 continue
                 
        # 3. Body
        if current_recipe and is_populated(raw_desc) and val_tot is None:
            p_comp = clean_float(row[IDX["P_COMP"]])
            q_comp = clean_float(row[IDX["Q_COMP"]])
            if p_comp is not None or q_comp is not None:
                is_labor = "operaio" in str(raw_desc).lower()
                current_recipe["components"].append({
                    "code": str(raw_art) if is_populated(raw_art) else "",
                    "desc": str(raw_desc), "type": "MAN" if is_labor else "MAT",
                    "qty": q_comp or 0.0, "price": p_comp or 0.0
                })

    conn.commit() # Commit unico per file
    conn.close()
    return saved_count

# --- VETTORIZZAZIONE RESILIENTE (CHUNKED) ---

def sync_vectors_incremental():
    """
    Processa i vettori mancanti.
    """
    print("\n🧠 VECTOR SYNC ENGINE: Avvio...")
    conn = sqlite3.connect(DB_FILE)
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    
    # 1. Contiamo quanto lavoro c'è da fare (Opzionale, solo per progress bar)
    count_cursor = conn.execute("""
        SELECT COUNT(*) FROM recipes r 
        LEFT JOIN vec_recipes v ON r.id = v.rowid 
        WHERE v.rowid IS NULL
    """)
    total_missing = count_cursor.fetchone()[0]
    
    if total_missing == 0:
        print("   ✅ Tutti i vettori sono sincronizzati.")
        conn.close()
        return

    print(f"   ⚠️  Target: {total_missing} nuove ricette da indicizzare.")
    print(f"   ⚙️  Batch Size: {VECTOR_BATCH_SIZE}")

    # 2. Cursore per processare a blocchi (Stream)
    cursor = conn.execute("""
        SELECT r.id, r.description 
        FROM recipes r
        LEFT JOIN vec_recipes v ON r.id = v.rowid
        WHERE v.rowid IS NULL
    """)

    processed_so_far = 0
    
    while True:
        # FETCHMANY: Legge solo N righe in RAM
        batch = cursor.fetchmany(VECTOR_BATCH_SIZE)
        
        if not batch:
            break # Finito tutto
            
        ids = [r[0] for r in batch]
        texts = [str(r[1]).replace("\n", " ").strip() for r in batch]
        
        try:
            # Chiamata API
            t0 = time.time()
            resp = client.embeddings.create(input=texts, model="text-embedding-3-small")
            
            # Preparazione dati binari
            vec_data = []
            for j, data_obj in enumerate(resp.data):
                vec_bin = serialize_f32(data_obj.embedding)
                vec_data.append((ids[j], vec_bin))
            
            # Scrittura e COMMIT immediato (Checkpointing)
            conn.executemany("INSERT INTO vec_recipes(rowid, embedding) VALUES(?, ?)", vec_data)
            conn.commit() 
            
            processed_so_far += len(batch)
            elapsed = time.time() - t0
            print(f"   saved batch: {processed_so_far}/{total_missing} ({elapsed:.2f}s)")
            
        except Exception as e:
            print(f"   ❌ CRITICAL ERROR nel batch (ID {ids[0]}-{ids[-1]}): {e}")
            print("   ⏹️  Arresto di sicurezza. Rilancia lo script per riprendere da qui.")
            break # Usciamo per evitare loop infiniti su errori API

    conn.close()
    print("   ✅ Sync Session Terminata.")

# --- MAIN LOOP ---

def run_ingestion():
    if not os.path.exists(INPUT_FOLDER):
        os.makedirs(INPUT_FOLDER)
        print(f"📁 Creata cartella '{INPUT_FOLDER}'.")
        return

    init_db()
    files = glob.glob(os.path.join(INPUT_FOLDER, "*.xlsx"))
    print(f"📦 BULK MANAGER: {len(files)} file rilevati.")
    
    new_data_flag = False
    
    # FASE 1: Parsing Files (Commit per file)
    for filepath in files:
        filename = os.path.basename(filepath)
        f_hash = get_file_hash(filepath)
        
        if not f_hash:
            print(f"❌ Impossibile leggere {filename}")
            continue

        status = check_file_status(filename, f_hash)
        
        if status == "SKIP":
            continue # Silenzioso per non intasare log
            
        print(f"📄 Processing: {filename}...", end="")
        try:
            cnt = process_excel_file(filepath)
            if cnt > 0:
                print(f" OK ({cnt} ricette)")
                log_ingestion_result(filename, f_hash, "SUCCESS", cnt)
                new_data_flag = True
            else:
                print(" ZERO DATI (Skipped)")
                log_ingestion_result(filename, f_hash, "WARNING_ZERO", 0)
        except Exception as e:
            print(f" ERROR: {str(e)[:50]}...")
            log_ingestion_result(filename, f_hash, "ERROR", 0)

    # FASE 2: Vector Sync (Batch & Checkpoint)
    # Lo lanciamo solo se abbiamo caricato nuovi dati o se ci sono residui precedenti
    # (Per sicurezza lo lanciamo sempre, tanto controlla lui se c'è lavoro)
    sync_vectors_incremental()
    
    print("\n🏁 Sistema pronto.")

if __name__ == "__main__":
    run_ingestion()