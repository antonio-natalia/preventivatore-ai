import os
import struct
import glob
import pandas as pd
from tqdm import tqdm
from typing import List, Optional, Tuple, Callable

from src.infrastructure.parsers import parse_six_xml_topology, parse_complex_excel_topology, parse_item_based_topology
from src.infrastructure.ai_client import get_embeddings_batch

def serialize_float32(vector: List[float]) -> bytes:
    return struct.pack(f"{len(vector)}f", *vector)

class IngestionService:
    def __init__(self, repository):
        self.repository = repository
        self.BATCH_SIZE = 50
        self.SUPPORTED_EXTENSIONS = {'.xml', '.xlsx', '.xls'}

    def process_path(self, path: str):
        """
        Gestisce sia un singolo file che una intera cartella in modo ricorsivo.
        """
        if os.path.isfile(path):
            self.process_file(path)
        elif os.path.isdir(path):
            print(f"📂 Rilevata cartella: {path}")
            files_to_process = []
            for root, _, files in os.walk(path):
                for f in files:
                    _, ext = os.path.splitext(f)
                    if ext.lower() in self.SUPPORTED_EXTENSIONS:
                        files_to_process.append(os.path.join(root, f))
            
            if not files_to_process:
                print("❌ Nessun file supportato (.xml, .xlsx, .xls) trovato nella cartella.")
                return

            print(f"📦 Trovati {len(files_to_process)} file da processare in batch.")
            
            for i, file_path in enumerate(files_to_process, 1):
                print(f"\n--- FILE {i}/{len(files_to_process)} ---")
                try:
                    self.process_file(file_path)
                except Exception as e:
                    print(f"❌ Errore critico processando {os.path.basename(file_path)}: {e}")
        else:
            print(f"❌ Percorso non valido: {path}")

    def _detect_excel_parser(self, file_path: str) -> Tuple[Optional[Callable], Optional[int]]:
        """
        Scansiona le prime righe del file Excel per identificare quale parser usare.
        Ritorna: (funzione_parser, indice_riga_header)
        """
        MAX_SCAN_ROWS = 20  
        SIGNATURE_ITEM_BASED = {"ITEMN", "CODICE", "DESCRIZIONE", "QCOMP"}
        
        try:
            df_preview = pd.read_excel(file_path, header=None, nrows=MAX_SCAN_ROWS, dtype=str)
        except Exception as e:
            print(f"⚠️ Errore lettura anteprima Excel: {e}")
            return None, None

        for row_idx, row in df_preview.iterrows():
            row_values = {str(val).strip().upper().replace('.', '').replace(' ', '').replace('_', '') for val in row if pd.notna(val)}
            
            if SIGNATURE_ITEM_BASED.issubset(row_values):
                print(f"   ✅ Rilevato formato 'Item-Based' alla riga {row_idx}")
                return parse_item_based_topology, row_idx

        print(f"   ℹ️  Nessun header 'Item-Based' trovato. Tentativo con parser 'Posizionale Legacy'.")
        return parse_complex_excel_topology, None

    def process_file(self, file_path: str):
        filename = os.path.basename(file_path)
        print(f"🚀 Avvio Ingestion Deterministica per: {filename}")
        
        data = None
        
        # 1. SELEZIONE PARSER
        if file_path.lower().endswith('.xml'):
            data = parse_six_xml_topology(file_path)
            
        elif file_path.lower().endswith(('.xlsx', '.xls')):
            parser_func, header_row = self._detect_excel_parser(file_path)
            
            if parser_func:
                if parser_func == parse_item_based_topology:
                    data = parser_func(file_path, header_row=header_row)
                else:
                    data = parser_func(file_path)
            else:
                print(f"⚠️  Formato Excel non riconosciuto.")
                return
        else:
            print(f"⚠️  Skipping {filename}: Formato non supportato")
            return

        if not data:
            return

        items = data['items']
        raw_relations = data['relations']
        
        if not items:
            print("⚠️  Nessun item trovato nel file (o errore di parsing).")
            return

        # COSTRUZIONE MAPPA ID -> SKU
        internal_id_to_sku_map = {}
        for item in items:
            if item.get('external_ref_id'):
                internal_id_to_sku_map[item['external_ref_id']] = item['sku']
            else:
                internal_id_to_sku_map[item['sku']] = item['sku']

        # ----------------------------------------------------
        # FASE 1: STAGING (Upsert Catalogo)
        # ----------------------------------------------------
        print("📥 Fase 1: Upsert Catalogo...")
        
        upserted = 0
        skipped = 0
        
        for i in range(0, len(items), self.BATCH_SIZE):
            raw_batch = items[i : i + self.BATCH_SIZE]
            valid_batch = []
            
            for item in raw_batch:
                if not item['description_full']:
                    skipped += 1
                    continue
                valid_batch.append(item)
            
            if not valid_batch: continue

            # Embeddings (Opzionale, silenziamo errori API)
            texts = [item['description_full'] for item in valid_batch]
            embeddings = []
            try:
                embeddings = get_embeddings_batch(texts)
            except Exception:
                embeddings = [None] * len(valid_batch)

            # Scrittura
            for idx, item in enumerate(valid_batch):
                mat_cost = item['declared_price'] if item['cost_type'] == 'MAT' else 0.0
                man_cost = item['declared_price'] if item['cost_type'] == 'MAN' else 0.0
                
                vec_blob = None
                if idx < len(embeddings) and embeddings[idx]:
                    vec_blob = serialize_float32(embeddings[idx])

                self.repository.upsert_catalog_item(
                    sku=item['sku'],
                    external_id=item['external_ref_id'],
                    desc_short=item['description_short'],
                    desc_long=item['description_long'],
                    uom=item['unit_of_measure'],
                    category=item['category_tag'],
                    pricing_strategy=item['pricing_strategy'],
                    material_cost=mat_cost,
                    labor_cost=man_cost,
                    source_file=filename,
                    embedding=vec_blob
                )
                upserted += 1
            
            self.repository.commit()
            
        print(f"   -> Importati: {upserted}, Scartati: {skipped}")

        # ----------------------------------------------------
        # FASE 2: WIRING (Costruzione Grafo)
        # ----------------------------------------------------
        if raw_relations:
            print("🔗 Fase 2: Costruzione Grafo...")
            
            # --- LOGICA DI DEDUPLICAZIONE (Last Write Wins) ---
            # Utilizziamo un dizionario nidificato per garantire che ogni coppia
            # (Padre, Figlio) sia unica prima di inviarla al database.
            # Se il listino contiene duplicati (es. Item 1 e Item 4 sono lo stesso SKU),
            # l'ultima riga letta sovrascriverà la precedente.
            
            bom_deduplication_map = {}
            # Struttura: { parent_sku: { child_sku: quantity } }
            
            for rel in raw_relations:
                p_sku = rel['parent_sku']
                c_int_id = rel['child_internal_id']
                qty = rel['quantity']
                
                c_sku = internal_id_to_sku_map.get(c_int_id)
                
                if c_sku:
                    if p_sku not in bom_deduplication_map:
                        bom_deduplication_map[p_sku] = {}
                    
                    # Qui avviene la deduplicazione:
                    # Se c_sku esisteva già per questo padre, il valore viene sovrascritto (=)
                    # Non usiamo +=, perché assumeremmo una somma di parti.
                    # Usiamo =, assumendo una ridefinizione dello standard.
                    bom_deduplication_map[p_sku][c_sku] = qty
                else:
                    self.repository.log_integrity_error(p_sku, f"REF_{c_int_id}", filename)

            # Trasformazione finale nel formato richiesto dal Repository (Lista di tuple)
            bom_map_final = {}
            for p_sku, children_dict in bom_deduplication_map.items():
                bom_map_final[p_sku] = [(c_sku, q) for c_sku, q in children_dict.items()]

            for p_sku, children in tqdm(bom_map_final.items(), desc="Linking BOM", leave=False):
                try: 
                    # replace_bom_links gestisce la cancellazione dei vecchi record
                    # e l'inserimento dei nuovi. Grazie alla deduplicazione sopra,
                    # non violeremo il vincolo UNIQUE(parent, child).
                    self.repository.replace_bom_links(p_sku, children, source_file=filename)
                except Exception as e:
                    print(f"❌ ERRORE SCRITTURA BOM per {p_sku}: {e}")
            
            self.repository.commit()
        else:
            print("🔗 Fase 2: Nessuna relazione gerarchica trovata (Skipped)")

        # ----------------------------------------------------
        # FASE 3: COST ROLLUP
        # ----------------------------------------------------
        print("🧮 Fase 3: Calcolo Costi...")
        self.repository.mark_leaves_as_valid()
        self.repository.commit()
        
        calcs = 0
        while True:
            ready = self.repository.get_dirty_nodes_ready_for_calculation()
            if not ready: break
            
            for p_sku in ready:
                costs = self.repository.calculate_node_cost(p_sku)
                self.repository.update_computed_cost_and_validate(p_sku, costs[0], costs[1])
                self.repository.log_price_change_history(p_sku, costs[0], costs[1], 'CALC', filename)
                calcs += 1
            self.repository.commit()

        print(f"✅ Ingestion {filename} completata. Ricalcolati: {calcs}")