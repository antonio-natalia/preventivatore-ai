import os
import struct
import json
from datetime import datetime
from tqdm import tqdm

from src.config import settings
from src.core.pricing import calculate_smart_price_logic
from src.infrastructure.repositories import RecipeRepository
from src.infrastructure.ai_client import get_embedding, get_chat_completion_json
from src.infrastructure.parsers import parse_excel, parse_six_xml
from src.infrastructure.database import get_db_connection # Helper per orphan logging se necessario

# Helper per orfani (portato qui per semplicità o spostabile in infrastructure)
import csv
def log_orphan(source_file, recipe_name, comp):
    file_exists = os.path.isfile(settings.ORPHANS_FILE)
    with open(settings.ORPHANS_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Source", "Recipe_DB", "Desc", "Price", "Qty", "Type", "Status"])
        writer.writerow([source_file, recipe_name, comp['description'], 
                         comp['unit_price'], comp['qty_coefficient'], comp['type'], "PENDING"])

def serialize_f32(vector):
    return struct.pack(f"{len(vector)}f", *vector)

class IngestionService:
    def __init__(self, repo: RecipeRepository):
        self.repo = repo

    def process_file(self, file_path: str, file_type: str = "auto", pricing_mode: str = "SMART_ADAPTIVE"):
        filename = os.path.basename(file_path)
        print(f"🚀 Avvio Ingestion Service per: {filename}")
        
        # 1. Parsing
        if file_type == "xml" or file_path.endswith(".xml"):
            items = parse_six_xml(file_path)
        else:
            items = parse_excel(file_path)
        
        print(f"📦 Trovati {len(items)} articoli.")

        # 2. Processing Loop
        for item in tqdm(items, desc="Syncing DB"):
            self._process_single_item(item, filename, pricing_mode)
        
        # 3. Commit finale
        self.repo.commit()
        print("✅ Ingestion conclusa con successo.")

    def _process_single_item(self, item, source_file, pricing_mode):
        raw_desc = item['description']
        raw_price = item['price']
        components = item['components']

        # A. Vettorizzazione
        vec = get_embedding(raw_desc)
        query_vec = serialize_f32(vec)

        # B. Ricerca Vettoriale
        match = self.repo.find_similar_vectors(query_vec, limit=1)
        match_id = None
        
        if match:
            rowid, dist = match
            similarity = 1 - dist
            
            # C. Logica di Matching (Core Logic preservata)
            if similarity >= settings.SIMILARITY_MERGE:
                # print(f"   🟢 AUTO-MERGE ({similarity:.3f})")
                match_id = rowid
            
            elif similarity >= settings.SIMILARITY_JUDGE:
                # Judge AI
                db_price, _, db_desc = self.repo.get_metadata(rowid)
                prompt = f"Confronta: 1. DB: '{db_desc}' 2. INPUT: '{raw_desc}'. Stesso articolo tecnico? Rispondi JSON {{'decision': 'MERGE'|'BRANCH'}}."
                try:
                    res_json = get_chat_completion_json(prompt)
                    res = json.loads(res_json)
                    if res.get("decision") == "MERGE":
                        # print(f"   🟡 AI-MERGE")
                        match_id = rowid
                except: pass

        # D. Esecuzione (Merge o Insert)
        if match_id:
            self._merge_recipe(match_id, raw_price, source_file, components, pricing_mode)
        else:
            self._create_recipe(raw_desc, raw_price, source_file, components, query_vec)

    def _merge_recipe(self, recipe_id, new_price, source_file, incoming_components, pricing_mode):
        # 1. Recupero dati per Smart Pricing
        curr_price, last_date_str = self.repo.get_latest_price_history(recipe_id)
        _, curr_vol, recipe_name = self.repo.get_metadata(recipe_id)
        
        last_date = None
        if last_date_str:
            try: last_date = datetime.strptime(str(last_date_str).split('.')[0], "%Y-%m-%d %H:%M:%S")
            except: pass

        # 2. Calcolo Logica Pura
        final_price, new_volatility = calculate_smart_price_logic(
            current_db_price=curr_price,
            current_volatility=curr_vol,
            last_update_date=last_date,
            new_price=new_price
        )

        # 3. Aggiornamento DB
        self.repo.insert_price_history(recipe_id, new_price, source_file)
        self.repo.update_recipe_volatility(recipe_id, new_volatility)

        # 4. Gestione Componenti
        self._reconcile_components(recipe_id, recipe_name, incoming_components, source_file)

    def _create_recipe(self, desc, price, source_file, incoming_components, vector_blob):
        new_id = self.repo.insert_recipe(desc, volatility=0.0)
        self.repo.insert_vector(new_id, vector_blob)
        self.repo.insert_price_history(new_id, price, source_file)
        
        # Inserimento componenti massivo
        for c in incoming_components:
            self.repo.insert_component(new_id, c['description'], c['qty_coefficient'], c['unit_price'], c['type'])

    def _reconcile_components(self, recipe_id, recipe_name, incoming, source_file):
        if not incoming: return
        existing = self.repo.get_components(recipe_id)
        db_map = {row[1].lower().strip(): row[0] for row in existing} # desc -> id

        if not existing:
            # Arricchimento
            for c in incoming:
                self.repo.insert_component(recipe_id, c['description'], c['qty_coefficient'], c['unit_price'], c['type'])
        else:
            # Merge / Orfani
            for c in incoming:
                key = c['description'].lower().strip()
                if key in db_map:
                    self.repo.update_component_price(db_map[key], c['unit_price'])
                else:
                    log_orphan(source_file, recipe_name, c)