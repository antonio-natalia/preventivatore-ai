import sqlite3
from typing import List, Tuple, Dict, Optional, Any

class CatalogRepository:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.cursor = connection.cursor()

    def commit(self):
        """Esegue il commit della transazione corrente."""
        self.connection.commit()

    # =========================================================================
    # WRITERS (Usati da IngestionService)
    # =========================================================================

    def upsert_catalog_item(self, sku: str, external_id: str, desc_short: str, desc_long: str, 
                          uom: str, category: str, pricing_strategy: str, 
                          material_cost: float, labor_cost: float, 
                          source_file: str, embedding: bytes = None):
        """
        Inserisce o aggiorna un articolo nel catalogo.
        """
        sql_insert = """
            INSERT INTO catalog_items (
                sku, external_ref_id, description_short, description_long, 
                unit_of_measure, category_tag, pricing_strategy, 
                current_material_cost, current_labor_cost, cost_integrity_status, source_file_origin
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'DIRTY', ?)
            ON CONFLICT(sku) DO UPDATE SET
                external_ref_id = excluded.external_ref_id,
                description_short = excluded.description_short,
                description_long = excluded.description_long,
                unit_of_measure = excluded.unit_of_measure,
                category_tag = excluded.category_tag,
                pricing_strategy = excluded.pricing_strategy,
                
                -- Aggiorna i prezzi solo se è una risorsa (USE_DECLARED_PRICE)
                current_material_cost = CASE 
                    WHEN excluded.pricing_strategy = 'USE_DECLARED_PRICE' THEN excluded.current_material_cost
                    ELSE catalog_items.current_material_cost END,
                current_labor_cost = CASE 
                    WHEN excluded.pricing_strategy = 'USE_DECLARED_PRICE' THEN excluded.current_labor_cost
                    ELSE catalog_items.current_labor_cost END,
                
                cost_integrity_status = 'DIRTY', -- Forza ricalcolo
                last_update_timestamp = CURRENT_TIMESTAMP,
                source_file_origin = excluded.source_file_origin
            RETURNING id
        """
        params = (
            sku, external_id, desc_short, desc_long, 
            uom, category, pricing_strategy, 
            material_cost, labor_cost, source_file
        )
        self.cursor.execute(sql_insert, params)
        row_id = self.cursor.fetchone()[0]

        if embedding:
            self.cursor.execute("DELETE FROM vec_catalog_items WHERE rowid = ?", (row_id,))
            self.cursor.execute("INSERT INTO vec_catalog_items(rowid, embedding) VALUES (?, ?)", (row_id, embedding))

    def archive_bom_snapshot(self, parent_sku: str, replaced_by_source: str):
        """
        Copia la distinta base corrente di un articolo nella tabella di history
        prima che venga sovrascritta.
        """
        sql_archive = """
            INSERT INTO bom_history_log (parent_sku, child_sku, usage_quantity, replaced_by_source_file)
            SELECT parent_sku, child_sku, usage_quantity, ?
            FROM bill_of_materials
            WHERE parent_sku = ?
        """
        self.cursor.execute(sql_archive, (replaced_by_source, parent_sku))

    def replace_bom_links(self, parent_sku: str, children_links: List[Tuple[str, float]], source_file: str = "UNKNOWN"):
        """
        Riscrive completamente la distinta base (BOM) per un padre.
        Prima di cancellare, archivia la versione precedente nel log storico.
        """
        # 1. Archiviazione Snapshot
        self.archive_bom_snapshot(parent_sku, source_file)

        # 2. Cancellazione Vecchia BOM
        self.cursor.execute("DELETE FROM bill_of_materials WHERE parent_sku = ?", (parent_sku,))
        
        # 3. Inserimento Nuova BOM
        if children_links:
            data_to_insert = [(parent_sku, child_sku, quantity) for child_sku, quantity in children_links]
            self.cursor.executemany(
                "INSERT INTO bill_of_materials (parent_sku, child_sku, usage_quantity) VALUES (?, ?, ?)",
                data_to_insert
            )

    def log_integrity_error(self, parent_sku: str, missing_child_sku: str, source_file: str):
        """Registra un errore di integrità (Orfano)."""
        self.cursor.execute(
            "INSERT INTO bom_integrity_errors (parent_sku, missing_child_sku, source_file_origin) VALUES (?, ?, ?)",
            (parent_sku, missing_child_sku, source_file)
        )
        self.cursor.execute("UPDATE catalog_items SET cost_integrity_status = 'BROKEN' WHERE sku = ?", (parent_sku,))

    def log_price_change_history(self, sku: str, material_cost: float, labor_cost: float, event_type: str, source_context: str):
        """Audit trail delle variazioni prezzo."""
        self.cursor.execute("""
            INSERT INTO cost_history_log (sku, recorded_material_cost, recorded_labor_cost, event_type, source_context)
            VALUES (?, ?, ?, ?, ?)
        """, (sku, material_cost, labor_cost, event_type, source_context))

    # =========================================================================
    # ENGINE & CALCULATION (Quelli che mancavano e causavano l'errore)
    # =========================================================================

    def mark_leaves_as_valid(self):
        """
        Marca automaticamente come 'VALID' tutte le foglie (strategia USE_DECLARED_PRICE).
        Questo è il punto di innesco della catena di validazione Bottom-Up.
        """
        self.cursor.execute("UPDATE catalog_items SET cost_integrity_status = 'VALID' WHERE pricing_strategy = 'USE_DECLARED_PRICE'")

    def get_dirty_nodes_ready_for_calculation(self) -> List[str]:
        """
        Trova i nodi 'DIRTY' che hanno TUTTI i figli 'VALID'.
        """
        sql_query = """
            SELECT p.sku 
            FROM catalog_items p
            WHERE p.cost_integrity_status = 'DIRTY'
              AND p.pricing_strategy = 'SUM_CHILDREN'
              AND NOT EXISTS (
                  -- Verifica se esiste almeno un figlio NON VALID
                  SELECT 1 
                  FROM bill_of_materials bom
                  JOIN catalog_items c ON bom.child_sku = c.sku
                  WHERE bom.parent_sku = p.sku
                    AND c.cost_integrity_status != 'VALID'
              )
        """
        self.cursor.execute(sql_query)
        return [row[0] for row in self.cursor.fetchall()]

    def calculate_node_cost(self, parent_sku: str) -> Tuple[float, float]:
        """
        Esegue la somma matematica pesata dei costi dei figli.
        Returns: (total_material, total_labor)
        """
        sql_calculation = """
            SELECT 
                SUM(child.current_material_cost * bom.usage_quantity),
                SUM(child.current_labor_cost * bom.usage_quantity)
            FROM bill_of_materials bom
            JOIN catalog_items child ON bom.child_sku = child.sku
            WHERE bom.parent_sku = ?
        """
        self.cursor.execute(sql_calculation, (parent_sku,))
        result = self.cursor.fetchone()
        
        # Gestione caso NULL
        mat = result[0] if result[0] is not None else 0.0
        man = result[1] if result[1] is not None else 0.0
        return (mat, man)

    def update_computed_cost_and_validate(self, sku: str, material_cost: float, labor_cost: float):
        """Aggiorna il costo calcolato di un nodo e lo imposta a VALID."""
        self.cursor.execute("""
            UPDATE catalog_items 
            SET current_material_cost = ?, 
                current_labor_cost = ?, 
                cost_integrity_status = 'VALID', 
                last_update_timestamp = CURRENT_TIMESTAMP
            WHERE sku = ?
        """, (material_cost, labor_cost, sku))

    # =========================================================================
    # READERS (Usati da Sonar e QuoteService)
    # =========================================================================

    def search_pure_vector(self, query_vec: bytes, limit: int = 5):
        """
        Cerca nella tabella CATALOG_ITEMS usando vettori.
        Usato da: QuoteService.
        Ritorna: list of tuples (id, distance, desc, p_mat, p_man, source, sku, strategy, status)
        """
        # Join tra vec_catalog_items e catalog_items
        # Nota: description_full viene costruita a runtime o letta se salvata. 
        # Nel DDL abbiamo salvato description_long e description_short.
        # Costruiamo una descrizione combinata per l'output.
        sql = """
            SELECT 
                c.id, 
                v.distance, 
                COALESCE(c.description_long, c.description_short) as description, 
                c.current_material_cost, 
                c.current_labor_cost,
                c.source_file_origin,
                c.sku,
                c.pricing_strategy,
                c.cost_integrity_status
            FROM vec_catalog_items v
            JOIN catalog_items c ON v.rowid = c.id
            WHERE v.embedding MATCH ? AND k = ?
            ORDER BY v.distance
        """
        self.cursor.execute(sql, (query_vec, limit))
        return self.cursor.fetchall()

    def search_sonar_vectors(self, query_vec: bytes, limit: int = 5):
        """
        Query specifica per Sonar TUI.
        Recupera campi per visualizzazione deterministica.
        """
        sql = """
            SELECT 
                c.id, 
                c.sku, 
                COALESCE(c.description_short, c.sku) as description, 
                c.current_material_cost, 
                c.current_labor_cost, 
                c.source_file_origin,
                c.pricing_strategy,        -- Al posto di volatility
                c.cost_integrity_status,   -- Al posto di is_complex
                v.distance,
                c.last_update_timestamp
            FROM vec_catalog_items v
            JOIN catalog_items c ON v.rowid = c.id
            WHERE v.embedding MATCH ? AND k = ?
            ORDER BY v.distance ASC
        """
        self.cursor.execute(sql, (query_vec, limit))
        
        results = []
        # Mapping colonne -> dict per TUI
        cols = ["id", "sku", "description", "current_material_cost", "current_labor_cost", 
                "source_file", "pricing_strategy", "cost_integrity_status", "distance", "last_update_timestamp"]
        
        for row in self.cursor.fetchall():
            results.append(dict(zip(cols, row)))
        return results

    def get_components(self, parent_item_id: int):
        """
        Recupera i figli esplodendo la BOM.
        Usato da: QuoteService (Drill-down).
        """
        # 1. Recupera SKU del padre dall'ID
        self.cursor.execute("SELECT sku FROM catalog_items WHERE id = ?", (parent_item_id,))
        row = self.cursor.fetchone()
        if not row: return []
        parent_sku = row[0]

        # 2. Join BOM -> Catalog Items (Figli)
        sql = """
            SELECT 
                child.sku,
                COALESCE(child.description_short, child.sku) as description, 
                bom.usage_quantity as unit_quantity, 
                (child.current_material_cost + child.current_labor_cost) as unit_price, 
                CASE 
                    WHEN child.current_labor_cost > child.current_material_cost THEN 'MAN' 
                    ELSE 'MAT' 
                END as type
            FROM bill_of_materials bom
            JOIN catalog_items child ON bom.child_sku = child.sku
            WHERE bom.parent_sku = ?
        """
        self.cursor.execute(sql, (parent_sku,))
        
        cols = [column[0] for column in self.cursor.description]
        results = []
        for row in self.cursor.fetchall():
            results.append(dict(zip(cols, row)))
        return results

    def get_item_by_id(self, item_id: int):
        """Recupera l'intera riga catalog_item per ID."""
        sql = "SELECT * FROM catalog_items WHERE id = ?"
        self.cursor.execute(sql, (item_id,))
        cols = [column[0] for column in self.cursor.description]
        row = self.cursor.fetchone()
        return dict(zip(cols, row)) if row else None
    
    # Metodo alias per compatibilità con vecchio codice (se esiste)
    find_similar_vectors = search_pure_vector