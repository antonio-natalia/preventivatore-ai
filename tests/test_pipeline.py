import unittest
import os
import sys
import sqlite3
import pandas as pd
import shutil
import json
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# --- GESTIONE PATH ---
# Risaliamo alla root del progetto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, 'scripts')

# Aggiungiamo i percorsi per importare i moduli come "top-level"
sys.path.append(BASE_DIR)
sys.path.append(SCRIPTS_DIR)

try:
    import bulk_ingestion
    import generate_quote
except ImportError as e:
    raise ImportError(f"Errore import moduli. Verifica che bulk_ingestion.py sia in /scripts e generate_quote.py in root. Dettagli: {e}")

# --- CONFIGURAZIONE TEST ---
TEST_DIR = "test_env_temp"
TEST_DB = os.path.join(TEST_DIR, "test_db.db")
TEST_INPUT_DIR = os.path.join(TEST_DIR, "data")

class TestEndToEndPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup iniziale cartelle."""
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)
        os.makedirs(TEST_INPUT_DIR)

    @classmethod
    def tearDownClass(cls):
        """Pulizia finale."""
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)

    def setUp(self):
        """Setup per OGNI test."""
        # Rimuoviamo il DB fisico per partire puliti
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

        # Override delle variabili globali nei moduli
        # Importante: usiamo i moduli importati (top-level)
        bulk_ingestion.DB_FILE = TEST_DB
        bulk_ingestion.INPUT_FOLDER = TEST_INPUT_DIR
        generate_quote.DB_FILE = TEST_DB
        
        # Reset Pricing Mode default
        bulk_ingestion.PRICING_MODE = "SMART_ADAPTIVE"
        
        self._init_db_schema()

    def _init_db_schema(self):
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        # Schema Tabelle Standard
        c.execute('''CREATE TABLE IF NOT EXISTS recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, description TEXT,
            unit_material_price REAL, unit_manpower_price REAL, source_file TEXT,
            volatility_index REAL DEFAULT 0.0, is_complex_assembly BOOLEAN DEFAULT 0,
            confidence_score REAL DEFAULT 0.0, last_price_date DATETIME
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS components (
            id INTEGER PRIMARY KEY AUTOINCREMENT, recipe_id INTEGER,
            code TEXT, description TEXT, type TEXT, qty_coefficient REAL, 
            unit_price REAL, last_calculated_at DATETIME,
            FOREIGN KEY(recipe_id) REFERENCES recipes(id)
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, component_id INTEGER, raw_price REAL,
            date DATETIME DEFAULT CURRENT_TIMESTAMP, source_file TEXT, context_tags TEXT, reliability_score REAL,
            FOREIGN KEY(component_id) REFERENCES components(id)
        )''')
        
        # FIX VEC0: Creiamo SEMPRE una tabella standard di fallback.
        # Poiché i test MOCKANO le chiamate di ricerca, questa tabella serve solo per gli INSERT di setup.
        # Non useremo mai 'MATCH' o 'k' su questa tabella nei test (perché le query sono mockate).
        c.execute("DROP TABLE IF EXISTS vec_recipes")
        c.execute("CREATE TABLE vec_recipes (rowid INTEGER PRIMARY KEY, embedding BLOB, distance REAL)")

        conn.commit()
        conn.close()

    def _create_excel_input(self, filename, items):
        rows = []
        for desc, price in items:
            row_head = [None]*20
            row_head[0] = "ART_TEST"; row_head[1] = desc
            rows.append(row_head)
            
            row_comp = [None]*20
            row_comp[1] = desc; row_comp[3] = 1.0; row_comp[8] = price
            rows.append(row_comp)
            
            row_foot = [None]*20
            row_foot[14] = price
            rows.append(row_foot); rows.append(row_foot)

        df = pd.DataFrame(rows)
        path = os.path.join(TEST_INPUT_DIR, filename)
        df.to_excel(path, index=False, header=False)
        return path

    # --- TEST CASES ---

    # NOTA SUI PATCH: Usiamo 'bulk_ingestion' (nome modulo importato) non 'scripts.bulk_ingestion'
    @patch('bulk_ingestion.get_embedding_single')
    @patch('bulk_ingestion.find_semantic_match')
    def test_smart_pricing_adaptive_logic(self, mock_find, mock_embed):
        """Verifica logica Adaptive (Shock Prezzi)."""
        print("\n🧪 TEST: Smart Pricing Adaptive Logic")
        
        # Setup Mock
        mock_embed.return_value = [0.1]*1536
        # Simula sequenza: 1. Nessun match (Nuovo) -> 2. Match trovato (Update)
        mock_find.side_effect = [(None, None, 0.0), (1, "Presa Test", 0.99)]

        # 1. Ingestion Base (100€)
        self._create_excel_input("base.xlsx", [("Presa Test", 100.0)])
        bulk_ingestion.process_file(os.path.join(TEST_INPUT_DIR, "base.xlsx"))
        
        # Manipolazione Temporale: Retrodatare il primo prezzo a 6 mesi fa
        conn = sqlite3.connect(TEST_DB)
        old_date = (datetime.now() - timedelta(days=181)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE price_history SET date = ? WHERE id = 1", (old_date,))
        conn.commit()
        conn.close()
        
        # 2. Ingestion Shock (150€) - Oggi
        self._create_excel_input("shock.xlsx", [("Presa Test", 150.0)])
        bulk_ingestion.process_file(os.path.join(TEST_INPUT_DIR, "shock.xlsx"))
        
        # Verifica Prezzo
        conn = sqlite3.connect(TEST_DB)
        price = conn.execute("SELECT unit_price FROM components WHERE id=1").fetchone()[0]
        conn.close()
        
        print(f"   -> Base (6 mesi fa): 100€ | Shock (Oggi): 150€")
        print(f"   -> Prezzo Calcolato: {price}€")
        
        # Logica Adaptive: (0.9 * 150) + (0.1 * 100) = 145.0
        self.assertAlmostEqual(price, 145.0, delta=1.0)

    @patch('bulk_ingestion.get_embedding_single')
    @patch('bulk_ingestion.find_semantic_match')
    def test_pricing_override_max(self, mock_find, mock_embed):
        """Verifica Override MAX."""
        print("\n🧪 TEST: Pricing Override MAX")
        
        mock_embed.return_value = [0.1]*1536
        # Sequenza: 1. Nuovo, 2. Match, 3. Match
        mock_find.side_effect = [(None, None, 0), (1, "Cavo", 0.99), (1, "Cavo", 0.99)]
        
        # Patch della variabile globale PRICING_MODE dentro il modulo bulk_ingestion
        with patch.object(bulk_ingestion, 'PRICING_MODE', 'MAX'):
            
            # Ingestion sequenza: 10 -> 15 -> 12
            self._create_excel_input("f1.xlsx", [("Cavo", 10.0)])
            bulk_ingestion.process_file(os.path.join(TEST_INPUT_DIR, "f1.xlsx"))
            
            self._create_excel_input("f2.xlsx", [("Cavo", 15.0)])
            bulk_ingestion.process_file(os.path.join(TEST_INPUT_DIR, "f2.xlsx"))
            
            self._create_excel_input("f3.xlsx", [("Cavo", 12.0)])
            bulk_ingestion.process_file(os.path.join(TEST_INPUT_DIR, "f3.xlsx"))
        
        conn = sqlite3.connect(TEST_DB)
        price = conn.execute("SELECT unit_price FROM components WHERE id=1").fetchone()[0]
        conn.close()
        
        print(f"   -> Result MAX: {price}")
        self.assertEqual(price, 15.0)

    @patch('bulk_ingestion.get_embedding_single')
    @patch('generate_quote.get_embedding')
    @patch('generate_quote.validate_match_with_gpt')
    def test_volatility_safety_net(self, mock_gpt, mock_embed_q, mock_embed_i):
        """Verifica Safety Net Volatilità."""
        print("\n🧪 TEST: Volatility Safety Net")
        mock_embed_i.return_value = [0.1]*1536
        mock_embed_q.return_value = [0.1]*1536
        
        # Setup DB Manuale (bypassiamo ingestion)
        conn = sqlite3.connect(TEST_DB)
        conn.execute("INSERT INTO recipes (id, description, volatility_index, is_complex_assembly) VALUES (99, 'Quadro Complex', 0.8, 1)")
        # Insert nel Vector Mock (Tabella standard, quindi INSERT normale funziona)
        conn.execute("INSERT INTO vec_recipes(rowid, embedding, distance) VALUES(99, NULL, 0.0)")
        conn.commit()
        conn.close()

        mock_gpt.return_value = {"selected_index": 1, "status": "OK", "reason": "Test Match"}

        # Patchiamo la funzione di ricerca di generate_quote per evitare SQL reale
        with patch('generate_quote.search_similar_candidates') as mock_search:
            # Simuliamo che il DB restituisca il record volatile
            mock_search.return_value = [{
                "id": 99, "desc": "Quadro Complex", "price_mat": 500.0,
                "is_complex": 1, "volatility": 0.8, "similarity": 0.99
            }]
            
            candidates = generate_quote.search_similar_candidates("Quadro")
            best_match = candidates[0]
            is_complex = best_match.get('is_complex', 0)
            
            print(f"   -> Is Complex: {is_complex}")
            self.assertEqual(is_complex, 1)

if __name__ == '__main__':
    unittest.main()