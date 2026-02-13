import os
import sys
import struct
import json

# Setup Path per import moduli
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.infrastructure.database import get_db_connection
from src.infrastructure.repositories import CatalogRepository
from src.infrastructure.ai_client import get_embedding, get_chat_completion_json
from src.config import settings

# --- COSTANTI ---
DEFAULT_THRESHOLD = 0.72

PROMPT_VALIDATION_TEXT = """
Sei un preventivista edile esperto (Senior Quantity Surveyor).
Il tuo compito è identificare se tra i CANDIDATI del database esiste una voce tecnicamente equivalente alla RICHIESTA dell'utente.

RICHIESTA UTENTE: "{user_query}"

CANDIDATI ESTRATTI (Database):
{cand_str}

ISTRUZIONI:
1. Analizza le specifiche tecniche (dimensioni, materiali, unità di misura).
2. Ignora differenze sintattiche minori (es. "3x1.5" = "3G1,5").
3. Se esiste un match tecnico valido, selezionalo.
4. Se nessuno è compatibile o mancano dati critici, scarta.

OUTPUT JSON (Rigoroso):
{{
    "selected_index": <int: indice del candidato migliore, oppure -1 se nessuno va bene>,
    "status": "<string: MATCH | WARNING | NOMATCH>",
    "reason": "<string: Spiegazione tecnica sintetica (max 20 parole)>"
}}
"""

def serialize_f32(vector):
    return struct.pack(f"<{len(vector)}f", *vector)

class SonarTUI:
    def __init__(self):
        self.conn = get_db_connection()
        self.repo = CatalogRepository(self.conn)
        self.current_threshold = DEFAULT_THRESHOLD
        print(f"💾 DB Connected (Deterministic Graph)")
        print(f"🧠 AI Module: Ready (GPT-4o)")

    def run(self):
        print("╔════════════════════════════════════════════════════╗")
        print("║      SONAR V4 - DETERMINISTIC PRICING ENGINE       ║")
        print("╚════════════════════════════════════════════════════╝")
        
        try:
            while True:
                # 1. Input Descrizione
                query = input(f"\n📝 RDO [t={self.current_threshold}] (q=exit): ").strip()
                if query.lower() in ['q', 'exit']: break
                
                # Cambio threshold rapido
                if query.startswith("t="):
                    try:
                        self.current_threshold = float(query.split("=")[1])
                        print(f"✅ Threshold aggiornato a: {self.current_threshold}")
                    except: pass
                    continue

                if not query: continue

                # 2. Input Soglia (Opzionale, premi enter per default)
                t_in = input(f"   Soglia [{self.current_threshold}]: ").strip()
                if t_in: 
                    try: self.current_threshold = float(t_in)
                    except: pass

                # 3. Esecuzione
                self.sonar_ping(query, self.current_threshold)
        finally:
            self.conn.close()

    def sonar_ping(self, query, threshold):
        print(f"📡 Vector Search: '{query}'...")
        try:
            query_vec = serialize_f32(get_embedding(query))
        except Exception as e:
            print(f"❌ Embedding Error: {e}")
            return

        # Recupero candidati
        rows = self.repo.search_sonar_vectors(query_vec, limit=10)
        
        candidates = []
        visible_idx = 0
        
        print("-" * 140)
        print(f"   {'SCORE':<6} | {'SKU':<12} | {'P.MAT':<8} | {'P.MAN':<8} | {'STRAT.':<8} | {'STATUS':<8} | {'DESCRIZIONE'}")
        print("-" * 140)
        
        for row in rows:
            dist = row['distance']
            sim = 1 / (1 + dist)
            
            if sim < threshold: continue
            
            color = "\033[92m" if sim > 0.85 else "\033[90m"
            reset = "\033[0m"
            
            sku = row['sku']
            p_mat = row['current_material_cost']
            p_man = row['current_labor_cost']
            strat = "∑" if row['pricing_strategy'] == "SUM_CHILDREN" else "€"
            status = row['cost_integrity_status']
            
            # Formattazione Stato
            if status == 'BROKEN': status = f"\033[91mBROKEN\033[0m"
            elif status == 'DIRTY': status = f"\033[93mDIRTY\033[0m"
            else: status = f"\033[92mVALID\033[0m"
            
            desc = (row['description'][:60] + '..') if row['description'] else "No Desc"
            
            print(f"[{visible_idx}] {color}{sim:.3f} | {sku:<12} | {p_mat:<8.2f} | {p_man:<8.2f} | {strat:<8} | {status:<8} | {desc}{reset}")
            
            candidates.append(row)
            visible_idx += 1
            
        if not candidates:
            print("   (Nessun risultato sopra la soglia)")
            return

        # --- MENU INTERATTIVO ---
        print("-" * 140)
        while True:
            choice = input(f"👉 [0-{len(candidates)-1}] Drill-down | [A] Chiedi ad AI | [ENTER] Nuova ricerca: ").strip().lower()
            
            if not choice:
                break # Nuova ricerca
            
            if choice == 'a':
                self.ask_ai_judge(query, candidates)
                continue # Rimane nel loop per permettere drill-down dopo AI
                
            if choice.isdigit():
                idx = int(choice)
                if 0 <= idx < len(candidates):
                    self.drill_down(candidates[idx]['id'])
                    continue

    def ask_ai_judge(self, query, candidates):
        """Invoca GPT-4o per arbitrare i risultati."""
        print(f"\n🤖 Chiedo all'AI di analizzare i {len(candidates)} candidati...")
        
        # 1. Costruzione contesto per il prompt
        cand_str = ""
        for i, c in enumerate(candidates):
            desc = c.get('description', 'N/A')
            # Includiamo prezzo e strategia per dare contesto all'AI
            cand_str += f"[{i}] SKU:{c['sku']} | DESC:{desc} | PREZZO:{c['current_material_cost'] + c['current_labor_cost']:.2f}\n"

        full_prompt = PROMPT_VALIDATION_TEXT.format(user_query=query, cand_str=cand_str)
        
        try:
            # 2. Chiamata API
            json_str = get_chat_completion_json(full_prompt, model="gpt-4o")
            resp = json.loads(json_str)
            
            # 3. Visualizzazione Risultato
            idx = resp.get("selected_index", -1)
            status = resp.get("status", "UNKNOWN")
            reason = resp.get("reason", "No reason provided")
            
            print("\n" + "═" * 60)
            print(f"🧠 AI VERDICT: {status}")
            print("═" * 60)
            print(f"MOTIVAZIONE: {reason}")
            
            if idx != -1 and 0 <= idx < len(candidates):
                best = candidates[idx]
                print(f"CANDIDATO SCELTO [{idx}]: \033[1m{best['sku']}\033[0m - {best['description'][:80]}...")
            else:
                print("NESSUN CANDIDATO VALIDO SECONDO L'AI.")
            print("═" * 60 + "\n")
            
        except Exception as e:
            print(f"❌ Errore AI: {e}")

    def drill_down(self, item_id):
        item = self.repo.get_item_by_id(item_id)
        comps = self.repo.get_components(item_id)
        
        if not item: return

        print(f"\n🔎 DETTAGLIO: \033[1m{item['sku']}\033[0m")
        desc = item.get('description_long') or item.get('description_short')
        print(f"   {desc}")
        print(f"   Stato: {item['cost_integrity_status']} | Strategia: {item['pricing_strategy']}")
        print(f"   Costo: MAT € {item['current_material_cost']:.2f} + MAN € {item['current_labor_cost']:.2f}")
        
        if comps:
            print(f"\n   🔩 BOM ({len(comps)} componenti):")
            print(f"     {'QTY':<6} | {'SKU':<15} | {'UNIT €':<10} | {'DESC'}")
            print("     " + "-"*60)
            for c in comps:
                print(f"     {c['unit_quantity']:<6.2f} | {c['sku']:<15} | {c['unit_price']:<10.2f} | {c['description'][:30]}")
        else:
             print("\n   🍃 (Foglia)")
        print("")