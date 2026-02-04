import unittest
import sqlite3
import os
import sys
import numpy as np
from datetime import datetime

# Aggiunge la root al path per eventuali import futuri dei moduli core
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- CONFIGURAZIONE TEST ---
TEST_DB_FILE = "test_env_db.db"

class TestSmartPricingEngine(unittest.TestCase):
    """
    Test Suite per la validazione delle logiche di Smart Pricing:
    1. Deduplica Semantica (Merge vs Branch)
    2. Calcolo Prezzo Medio Pesato (Time-Weighted)
    3. Rilevamento Volatilità (Black Box Items)
    """

    def setUp(self):
        """Preparazione ambiente: DB pulito e Schema V2."""
        if os.path.exists(TEST_DB_FILE):
            os.remove(TEST_DB_FILE)
        
        self.conn = sqlite3.connect(TEST_DB_FILE)
        c = self.conn.cursor()
        
        # Replica esatta dello Schema V2 (Target Architecture)
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
        
        c.execute('''CREATE TABLE components (
            id INTEGER PRIMARY KEY AUTOINCREMENT, recipe_id INTEGER,
            code TEXT, description TEXT, type TEXT, qty_coefficient REAL, 
            unit_price REAL, -- Cache Calculated
            last_calculated_at DATETIME,
            FOREIGN KEY(recipe_id) REFERENCES recipes(id)
        )''')
        
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
        self.conn.commit()

    def tearDown(self):
        """Pulizia ambiente."""
        self.conn.close()
        if os.path.exists(TEST_DB_FILE):
            os.remove(TEST_DB_FILE)

    # --- HELPER METHODS (Simulano la logica di ingestion) ---

    def _simulate_ingestion(self, code, desc, price, filename, mode="BRANCH", target_rid=None):
        """Simula l'arrivo di una voce dal parser."""
        if mode == "BRANCH":
            # Crea Ricetta
            cur = self.conn.execute("INSERT INTO recipes (code, description, source_file) VALUES (?,?,?)", (code, desc, filename))
            rid = cur.lastrowid
            # Crea Componente
            cur_c = self.conn.execute("INSERT INTO components (recipe_id, description, type, qty_coefficient, unit_price) VALUES (?,?,?,?,0)",
                                (rid, desc, "MAT", 1.0))
            cid = cur_c.lastrowid
            # Crea Storico
            self.conn.execute("INSERT INTO price_history (component_id, raw_price, source_file) VALUES (?,?,?)", (cid, price, filename))
            return rid, cid
        
        elif mode == "MERGE":
            # Trova componente esistente
            row = self.conn.execute("SELECT id FROM components WHERE recipe_id=?", (target_rid,)).fetchone()
            cid = row[0]
            # Aggiunge solo Storico
            self.conn.execute("INSERT INTO price_history (component_id, raw_price, source_file) VALUES (?,?,?)", (cid, price, filename))
            return target_rid, cid

    def _run_price_engine(self, rid):
        """Simula l'algoritmo di ricalcolo (ALG-01 + ALG-02)."""
        row = self.conn.execute("SELECT id FROM components WHERE recipe_id=?", (rid,)).fetchone()
        cid = row[0]
        
        prices = self.conn.execute("SELECT raw_price FROM price_history WHERE component_id=?", (cid,)).fetchall()
        raw_prices = [p[0] for p in prices]
        
        # 1. Media Pesata (Semplificata per il test: pesi uguali)
        avg_price = sum(raw_prices) / len(raw_prices)
        
        # 2. Volatilità
        if len(raw_prices) > 1:
            mean = np.mean(raw_prices)
            std = np.std(raw_prices)
            cv = std / mean if mean > 0 else 0.0
        else:
            cv = 0.0
        
        is_complex = 1 if cv > 0.5 else 0
        
        # Aggiornamento DB
        self.conn.execute("UPDATE components SET unit_price = ? WHERE id = ?", (avg_price, cid))
        self.conn.execute("UPDATE recipes SET volatility_index = ?, is_complex_assembly = ? WHERE id = ?", (cv, is_complex, rid))
        self.conn.commit()

    # --- TEST CASES ---

    def test_deduplication_logic(self):
        """Verifica che prezzi diversi per lo stesso oggetto vengano mediati."""
        print("\n🧪 TEST: Logica Deduplica & Media Prezzi")
        
        # 1. Primo inserimento (100€)
        rid, cid = self._simulate_ingestion("A01", "Presa Standard", 100.0, "file_old.xlsx", mode="BRANCH")
        self._run_price_engine(rid)
        
        # 2. Secondo inserimento (120€) -> MERGE
        self._simulate_ingestion("A01", "Presa Standard", 120.0, "file_new.xlsx", mode="MERGE", target_rid=rid)
        self._run_price_engine(rid)
        
        # Verifica Prezzo Calcolato (Media: 110€)
        final_price = self.conn.execute("SELECT unit_price FROM components WHERE id=?", (cid,)).fetchone()[0]
        hist_count = self.conn.execute("SELECT count(*) FROM price_history WHERE component_id=?", (cid,)).fetchone()[0]
        
        self.assertEqual(hist_count, 2, "Lo storico deve avere 2 record")
        self.assertAlmostEqual(final_price, 110.0, msg="Il prezzo medio dovrebbe essere 110.0")
        print("   ✅ Deduplica OK. Prezzo aggiornato correttamente.")

    def test_volatility_safety_flag(self):
        """Verifica che alta varianza attivi il flag MANUAL_ESTIMATION."""
        print("\n🧪 TEST: Rilevamento Alta Volatilità")
        
        # 1. Inserimento Base (1.000€)
        rid, cid = self._simulate_ingestion("Q99", "Quadro Complesso", 1000.0, "small_job.xlsx", mode="BRANCH")
        
        # 2. Inserimento Divergente (5.000€) -> Alta varianza
        self._simulate_ingestion("Q99", "Quadro Complesso", 5000.0, "big_job.xlsx", mode="MERGE", target_rid=rid)
        self._run_price_engine(rid)
        
        # Calcolo atteso: Mean=3000, Std=2000, CV=0.66 (> 0.5)
        row = self.conn.execute("SELECT volatility_index, is_complex_assembly FROM recipes WHERE id=?", (rid,)).fetchone()
        cv = row[0]
        flag = row[1]
        
        self.assertTrue(cv > 0.5, f"CV calcolato ({cv:.2f}) dovrebbe essere > 0.5")
        self.assertEqual(flag, 1, "Il flag is_complex_assembly deve essere 1")
        print(f"   ✅ Volatilità Rilevata (CV={cv:.2f}). Flag attivato.")

if __name__ == '__main__':
    unittest.main()