import sqlite3
import os
from dotenv import load_dotenv, find_dotenv

# PATH SETUP
dotenv_path = find_dotenv()
if not dotenv_path:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
else:
    load_dotenv(dotenv_path)
    PROJECT_ROOT = os.path.dirname(dotenv_path)

DB_FILE = os.path.join(PROJECT_ROOT, "db", "preventivatore_v2_bulk.db")

def migrate_v2():
    print(f"🔧 MIGRATION V2: Updating Schema on {DB_FILE}...")
    if not os.path.exists(DB_FILE):
        print("❌ Database non trovato. Esegui prima l'inizializzazione base.")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    try:
        # 1. Tabella PRICE_HISTORY
        c.execute('''CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            component_id INTEGER,
            raw_price REAL,
            date DATETIME DEFAULT CURRENT_TIMESTAMP,
            source_file TEXT,
            context_tags TEXT,
            reliability_score REAL DEFAULT 1.0,
            FOREIGN KEY(component_id) REFERENCES components(id)
        )''')
        print("   -> Table 'price_history' ready.")

        # 2. Colonne Statistiche RECIPES
        current_cols = [row[1] for row in c.execute("PRAGMA table_info(recipes)")]
        new_cols = [
            ("volatility_index", "REAL DEFAULT 0.0"),
            ("is_complex_assembly", "BOOLEAN DEFAULT 0"),
            ("confidence_score", "REAL DEFAULT 0.0"),
            ("last_price_date", "DATETIME")
        ]
        for name, dtype in new_cols:
            if name not in current_cols:
                c.execute(f"ALTER TABLE recipes ADD COLUMN {name} {dtype}")
                print(f"   -> Column recipes.{name} added.")

        # 3. Colonne Cache COMPONENTS
        comp_cols = [row[1] for row in c.execute("PRAGMA table_info(components)")]
        if "last_calculated_at" not in comp_cols:
            c.execute("ALTER TABLE components ADD COLUMN last_calculated_at DATETIME")
            print("   -> Column components.last_calculated_at added.")

        conn.commit()
        print("✅ MIGRATION SUCCESSFUL.")

    except Exception as e:
        print(f"❌ MIGRATION FAILED: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_v2()