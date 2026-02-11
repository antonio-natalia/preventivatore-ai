import sqlite3
import sqlite_vec
from src.config import settings

def get_db_connection():
    """Restituisce una connessione configurata al DB SQLite."""
    conn = sqlite3.connect(settings.DB_FILE)
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    return conn