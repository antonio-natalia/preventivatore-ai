from datetime import datetime
from typing import Optional, List, Tuple

class RecipeRepository:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()

    # --- LETTURA ---

    def search_pure_vector(self, query_vec: bytes, limit: int = 5):
        """
        Cerca nella tabella RECIPES usando lo schema SMART.
        Ritorna: list of tuples (id, distance, description, p_mat, p_man, source)
        """
        # Join tra vec_recipes e recipes (che ora ha i prezzi divisi)
        sql = """
            SELECT 
                r.id, 
                v.distance, 
                r.description, 
                r.unit_material_price, 
                r.unit_manpower_price,
                r.source_file
            FROM vec_recipes v
            JOIN recipes r ON v.rowid = r.id
            WHERE v.embedding MATCH ? AND k = ?
            ORDER BY v.distance
        """
        self.cursor.execute(sql, (query_vec, limit))
        return self.cursor.fetchall()

    def get_components(self, recipe_id: int):
        """
        Recupera i componenti dalla tabella COMPONENTS.
        Mapping: qty_coefficient -> unit_quantity
        """
        sql = """
            SELECT 
                description, 
                qty_coefficient as unit_quantity, 
                unit_price, 
                type
            FROM components 
            WHERE recipe_id = ?
        """
        self.cursor.execute(sql, (recipe_id,))
        # Convertiamo in lista di dizionari
        cols = [column[0] for column in self.cursor.description]
        results = []
        for row in self.cursor.fetchall():
            results.append(dict(zip(cols, row)))
        return results

    def get_metadata(self, recipe_id: int) -> Tuple[float, float, str]:
        """Recupera metadati ricetta: (price, volatility, description)."""
        # NOTA: price qui potrebbe essere 0 se non persistito, dipende dalla logica precedente.
        # Manteniamo la query originale.
        self.cursor.execute("SELECT price, volatility_index, description FROM recipes WHERE id=?", (recipe_id,))
        row = self.cursor.fetchone()
        if row:
            return (row[0] or 0.0, row[1] or 0.0, row[2])
        return (0.0, 0.0, "")

    def get_latest_price_history(self, recipe_id: int) -> Tuple[float, Optional[str]]:
        """Recupera l'ultimo prezzo storico per calcolo obsolescenza."""
        self.cursor.execute("""
            SELECT raw_price, date 
            FROM price_history 
            WHERE recipe_id = ? 
            ORDER BY date DESC LIMIT 1
        """, (recipe_id,))
        row = self.cursor.fetchone()
        if row:
            return (row[0], row[1])
        return (0.0, None)

    def find_similar_vectors(self, query_vec: bytes, limit: int = 1) -> Optional[Tuple[int, float]]:
        """Cerca vettori simili usando sqlite_vec."""
        self.cursor.execute(f"""
            SELECT rowid, distance FROM vec_recipes 
            WHERE embedding MATCH ? AND k=? 
            ORDER BY distance
        """, (query_vec, limit))
        return self.cursor.fetchone()

    # --- SCRITTURA ---

    def insert_recipe(self, description: str, volatility: float) -> int:
        self.cursor.execute("INSERT INTO recipes (description, volatility_index) VALUES (?, ?)", 
                            (description, volatility))
        return self.cursor.lastrowid

    def update_recipe_volatility(self, recipe_id: int, volatility: float):
        self.cursor.execute("UPDATE recipes SET volatility_index = ? WHERE id = ?", 
                            (volatility, recipe_id))

    def insert_price_history(self, recipe_id: int, price: float, source_file: str):
        self.cursor.execute("""
            INSERT INTO price_history (recipe_id, raw_price, date, source_file)
            VALUES (?, ?, ?, ?)
        """, (recipe_id, price, datetime.now(), source_file))

    def insert_vector(self, recipe_id: int, embedding: bytes):
        self.conn.execute("INSERT INTO vec_recipes(rowid, embedding) VALUES(?, ?)", 
                          (recipe_id, embedding))

    def insert_component(self, recipe_id: int, desc: str, qty: float, price: float, c_type: str):
        self.cursor.execute("""
            INSERT INTO components (recipe_id, description, quantity, price, type)
            VALUES (?, ?, ?, ?, ?)
        """, (recipe_id, desc, qty, price, c_type))

    def update_component_price(self, component_id: int, price: float):
        self.cursor.execute("UPDATE components SET unit_price = ? WHERE id = ?", 
                            (price, component_id))

    def commit(self):
        self.conn.commit()