import sqlite3
import os
import sys
import time
from dotenv import load_dotenv, find_dotenv

# Importiamo il motore di ingestion esistente come libreria
# Assicurati che bulk_ingestion.py sia nella stessa cartella
import scripts.bulk_ingestion as engine

# --- PATH SETUP ---
dotenv_path = find_dotenv()
if not dotenv_path:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
else:
    load_dotenv(dotenv_path)
    PROJECT_ROOT = os.path.dirname(dotenv_path)

# CONFIGURAZIONE FILE
OLD_DB_FILE = os.path.join(PROJECT_ROOT, "db", "preventivatore_v2_bulk.db")
TARGET_DB_FILE = os.path.join(PROJECT_ROOT, "db", "preventivatore_v3_smart.db")

def setup_target_db_schema():
    """
    Inizializza il DB Target con lo schema V3 completo (Smart Pricing).
    """
    if os.path.exists(TARGET_DB_FILE):
        print(f"⚠️  ATTENZIONE: Il DB target esiste già: {TARGET_DB_FILE}")
        confirm = input("    Vuoi sovrascriverlo e perdere i dati contenuti? (y/n): ")
        if confirm.lower() != 'y':
            print("    Migrazione annullata.")
            sys.exit()
        os.remove(TARGET_DB_FILE)
    
    print("🔨 Inizializzazione Schema V3 (Smart Pricing)...")
    conn = sqlite3.connect(TARGET_DB_FILE)
    c = conn.cursor()
    
    # 1. Recipes (con colonne volatilità)
    c.execute('''CREATE TABLE recipes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT, description TEXT,
        unit_material_price REAL, unit_manpower_price REAL,
        source_file TEXT,
        volatility_index REAL DEFAULT 0.0,
        is_complex_assembly BOOLEAN DEFAULT 0,
        confidence_score REAL DEFAULT 0.0,
        last_price_date DATETIME
    )''')
    
    # 2. Components (con cache prezzi)
    c.execute('''CREATE TABLE components (
        id INTEGER PRIMARY KEY AUTOINCREMENT, recipe_id INTEGER,
        code TEXT, description TEXT, type TEXT, qty_coefficient REAL, 
        unit_price REAL, last_calculated_at DATETIME,
        FOREIGN KEY(recipe_id) REFERENCES recipes(id)
    )''')
    
    # 3. Price History (fondamentale per Smart Pricing)
    c.execute('''CREATE TABLE price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        component_id INTEGER,
        raw_price REAL,
        date DATETIME DEFAULT CURRENT_TIMESTAMP,
        source_file TEXT,
        context_tags TEXT,
        reliability_score REAL DEFAULT 1.0,
        FOREIGN KEY(component_id) REFERENCES components(id)
    )''')

    # 4. Ingested Files (Tracking)
    c.execute('''CREATE TABLE IF NOT EXISTS ingested_files (
        filename TEXT PRIMARY KEY,
        file_hash TEXT,
        import_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT,
        recipes_count INTEGER
    )''')

    # 5. Vector Table
    try:
        conn.enable_load_extension(True)
        import sqlite_vec
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        c.execute("CREATE VIRTUAL TABLE vec_recipes USING vec0(embedding float[1536])")
    except Exception as e:
        print(f"⚠️  Warning Estensioni Vettoriali: {e}")
        
    conn.commit()
    conn.close()

def migrate_loop():
    print(f"\n🚀 AVVIO MIGRAZIONE LEGACY")
    print(f"    Sorgente: {OLD_DB_FILE}")
    print(f"    Destinazione: {TARGET_DB_FILE}")
    
    # 1. Preparazione
    setup_target_db_schema()
    
    conn_src = sqlite3.connect(OLD_DB_FILE)
    conn_tgt = sqlite3.connect(TARGET_DB_FILE)
    
    # Carichiamo estensioni sul target per le funzioni di engine
    try:
        conn_tgt.enable_load_extension(True)
        import sqlite_vec
        sqlite_vec.load(conn_tgt)
        conn_tgt.enable_load_extension(False)
    except: pass

    # OVERRIDE GLOBALE: Forziamo il modulo engine a usare il nostro nuovo DB
    engine.DB_FILE = TARGET_DB_FILE 
    
    # 2. Lettura Dati Vecchi
    print("📦 Lettura dati legacy...", end="")
    try:
        # Preleviamo tutto
        src_recipes = conn_src.execute("SELECT id, code, description, source_file FROM recipes").fetchall()
    except Exception as e:
        print(f"\n❌ Errore lettura DB sorgente: {e}")
        return

    print(f" {len(src_recipes)} ricette trovate.")
    
    stats = {"migrated": 0, "merged": 0, "errors": 0}
    
    # 3. Loop Migrazione
    # Usiamo una cache locale per deduplicare stringhe identiche senza chiamare GPT/Vector Search
    # (Ottimizzazione Massiva)
    start_time = time.time()
    
    for idx, r_old in enumerate(src_recipes):
        rid_old, r_code, r_desc, r_source = r_old
        
        # Recupera componenti
        comps_old = conn_src.execute("""
            SELECT description, type, qty_coefficient, unit_price 
            FROM components WHERE recipe_id = ?
        """, (rid_old,)).fetchall()
        
        # Preparazione Dati
        recipe_data = {
            "code": r_code, 
            "desc": r_desc,
            "components": []
        }
        
        for c in comps_old:
            # Nota: unit_price vecchio diventa il primo prezzo dello storico
            recipe_data["components"].append({
                "desc": c[0],
                "type": c[1],
                "qty": c[2],
                "price": c[3] if c[3] is not None else 0.0
            })

        # --- LOGICA DEDUPLICA OTTIMIZZATA ---
        # Invece di usare i vettori (che non ci sono ancora sul DB vuoto),
        # facciamo un check SQL esatto sulla descrizione per trovare duplicati.
        
        # Gestione Source File Dinamico
        dynamic_source = f"migration_{r_source}" if r_source else "migration_legacy_unknown"

        try:
            # Check esistenza stringa esatta (Case Insensitive)
            existing = conn_tgt.execute(
                "SELECT id FROM recipes WHERE description LIKE ?", 
                (r_desc,)
            ).fetchone()
            
            if existing:
                # MERGE (Trovato duplicato testuale)
                rid_match = existing[0]
                engine.merge_into_recipe(conn_tgt, rid_match, recipe_data, dynamic_source)
                engine.recalc_recipe_stats(rid_match, conn_tgt) # Aggiorna prezzi medi e volatilità
                stats["merged"] += 1
            else:
                # BRANCH (Nuova ricetta)
                engine.insert_new_recipe(conn_tgt, recipe_data, dynamic_source)
                stats["migrated"] += 1
                
            if idx % 50 == 0:
                print(f"\r⏳ Progress: {idx}/{len(src_recipes)} | New: {stats['migrated']} | Merged: {stats['merged']}", end="")
                
        except Exception as e:
            print(f"\n❌ Errore record {rid_old}: {e}")
            stats["errors"] += 1

    conn_src.close()
    conn_tgt.close() # Chiudiamo per flushare
    
    print(f"\n\n🏁 FASE 1 COMPLETATA ({time.time() - start_time:.1f}s).")
    
    # 4. Generazione Vettori (Batch)
    # Ora che i dati sono dentro, generiamo i vettori per abilitare la ricerca futura
    print("🧠 Generazione Embedding (Batch Async)...")
    
    # Riapriamo connessione tramite engine per usare la sua funzione di sync
    engine.sync_vectors()
    
    print("\n✅ MIGRAZIONE SUCCESSFUL.")
    print(f"    Database pronto: {TARGET_DB_FILE}")

if __name__ == "__main__":
    migrate_loop()