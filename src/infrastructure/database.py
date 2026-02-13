import sqlite3
import os
import sqlite_vec
from src.config import settings

def get_db_connection(db_path: str = None) -> sqlite3.Connection:
    """
    Stabilisce una connessione al database SQLite.
    
    1. Directory Auto-Creation: Gestisce automaticamente la creazione del path (utile per volumi Docker).
    2. Vector Extension Loading: Carica 'sqlite-vec' ad ogni connessione.
    3. Row Factory: Restituisce oggetti accessibili per nome colonna.
    
    Args:
        db_path (str, optional): Override del percorso DB. Se None, usa settings.DB_FILE.

    Returns:
        sqlite3.Connection: Connessione attiva e configurata.
    """
    target_db = db_path or settings.DB_FILE
    
    # 1. Gestione robusta del Path (Local & Cloud Friendly)
    try:
        db_dir = os.path.dirname(target_db)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            print(f"📁 [INFRA] Creata directory database: {db_dir}")
    except OSError as e:
        print(f"❌ [INFRA] Errore critico creazione directory DB: {e}")
        raise e

    # 2. Connessione
    try:
        conn = sqlite3.connect(target_db)
    except sqlite3.Error as e:
        print(f"❌ [INFRA] Errore connessione SQLite: {e}")
        raise e
    
    # 3. Caricamento Estensione Vettoriale (CRITICO)
    # Senza questo blocco, qualsiasi query su 'vec_recipes' fallirà.
    try:
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
    except Exception as e:
        print(f"❌ [INFRA] Impossibile caricare estensione sqlite-vec: {e}")
        conn.close()
        raise e

    # 4. Configurazione Performance & Usabilità
    conn.row_factory = sqlite3.Row
    # WAL Mode migliora la concorrenza (utile se TUI e Ingestion girano insieme)
    conn.execute("PRAGMA journal_mode=WAL;") 
    
    return conn