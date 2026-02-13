import struct
import json
import logging
from tqdm import tqdm
from src.core.entities import QuoteLineItem, QuoteComponentItem, QuoteResult
from src.infrastructure.repositories import CatalogRepository
from src.infrastructure.ai_client import get_embedding, get_chat_completion_json

# Configurazione Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- COSTANTI & SOGLIE ---
THRESHOLD_AUTO = 0.96
THRESHOLD_GREEN = 0.85
THRESHOLD_YELLOW = 0.60

# --- PROMPT VALIDATION ---
PROMPT_VALIDATION_TEXT = """
    Sei un Senior Quantity Surveyor ed esperto in computi metrici MEP.
    OBIETTIVO: Identificare la voce del database tecnicamente equivalente alla RDO.
    
    INPUT RDO: "{user_query}"
    
    CANDIDATI DATABASE:
    {cand_str}
    
    CRITERI:
    1. Dimensioni e Unità devono coincidere (es. 120mm = 12cm).
    2. Funzionalità tecnica equivalente.
    
    OUTPUT JSON:
    {{
      "selected_index": <int o -1>,
      "status": "<MATCH | CHECK | NOMATCH>",
      "reason": "Spiegazione sintetica"
    }}
"""

def serialize_f32(vector):
    return struct.pack(f"{len(vector)}f", *vector)

class QuoteService:
    def __init__(self, repo: CatalogRepository):
        self.repo = repo

    def generate_quote(self, data_input: list, solo_manodopera: bool = False) -> QuoteResult:
        stats = {"processed": 0, "match": 0, "warning": 0, "nomatch": 0}
        output_items = []
        
        logger.info(f"⚙️  Avvio Quote Service (Deterministic Logic). Righe: {len(data_input)}")
        
        for idx, item in enumerate(tqdm(data_input, desc="Computing Quote")):
            stats["processed"] += 1
            
            # Parsing Input
            desc = item.get("descrizione_completa", "") or item.get("description", "")
            if not desc: continue
            
            try: qta = float(item.get("quantita", 0) or 0.0)
            except: qta = 0.0
            
            # Prezzi Input opzionali
            try: p_orig = float(item.get("prezzo_unitario", 0) or 0.0)
            except: p_orig = 0.0
            try: p_man = float(item.get("prezzo_manodopera", 0) or 0.0)
            except: p_man = 0.0

            line = QuoteLineItem(
                row_index=idx,
                codice_input=item.get("codice_originale", ""),
                description_input=desc,
                quantity_input=qta,
                um_input=item.get("unita_misura", ""),
                p_mat_rdo=p_orig,
                p_man_rdo=p_man
            )

            # 1. Ricerca Vettoriale
            query_vec = get_embedding(desc)
            # candidates: (id, distance, desc, p_mat, p_man, source, sku, strategy, status)
            candidates = self.repo.search_pure_vector(serialize_f32(query_vec), limit=5)

            # 2. Validazione Match
            best_idx, status, reason = self._validate_match_with_gpt(desc, candidates)
            
            line.status = status
            line.reasoning = reason

            # 3. Popolamento Dati
            if best_idx >= 0:
                match = candidates[best_idx]
                # Unpacking tuple sicura
                match_id = match[0]
                # match[1] è distance
                match_desc = match[2]
                raw_p_mat = float(match[3] or 0.0)
                raw_p_man = float(match[4] or 0.0)
                src_file = match[5] or ""
                sku = match[6]
                strategy = match[7]
                integrity = match[8]
                
                line.match_id = match_id
                line.match_sku = sku
                line.match_description = match_desc
                line.source_file = src_file
                line.pricing_strategy = strategy
                line.integrity_status = integrity
                
                if solo_manodopera:
                    line.p_unit_mat_db = 0.0
                    line.p_unit_man_db = raw_p_man
                else:
                    line.p_unit_mat_db = raw_p_mat
                    line.p_unit_man_db = raw_p_man
                    
                    # Esplosione Figli (BOM)
                    # Se l'item è un NODO (SUM_CHILDREN), recuperiamo la distinta base
                    comps_data = self.repo.get_components(match_id)
                    for c in comps_data:
                        child = QuoteComponentItem(
                            sku=c.get('sku', ''),
                            description=c.get('description', ''),
                            unit_quantity=float(c.get('unit_quantity', 0)),
                            unit_price=float(c.get('unit_price', 0)),
                            type=c.get('type', 'MAT')
                        )
                        child.total_price = child.unit_price * child.unit_quantity * line.quantity_input
                        line.children.append(child)

                if status in ["MATCH", "AUTO_MATCH"]: 
                    stats["match"] += 1
                elif status == "WARNING":
                    stats["warning"] += 1
            else:
                stats["nomatch"] += 1

            output_items.append(line)

        return QuoteResult(items=output_items, stats=stats)

    def _validate_match_with_gpt(self, user_query, candidates):
        """Logica di Validazione Match (Invariata nella struttura, aggiornata nel parsing candidato)."""
        if not candidates:
            return -1, "NOMATCH", "Nessun candidato nel DB"

        top_candidate = candidates[0]
        similarity = 1 - top_candidate[1]

        if similarity >= THRESHOLD_AUTO:
            return 0, "MATCH", f"Auto-Match ({similarity:.2f})"

        if similarity < THRESHOLD_YELLOW:
            return -1, "NOMATCH", f"Low Similarity ({similarity:.2f})"

        # Costruzione Prompt con dettagli deterministici
        cand_str = ""
        for i, c in enumerate(candidates):
            # c: (id, dist, desc, p_mat, p_man, source, sku, strategy, status)
            sim = 1 - c[1]
            cand_str += f"[{i}] {c[6]} - {c[2]} (Sim: {sim:.2f}) | Strat: {c[7]} | Sts: {c[8]}\n"

        try:
            formatted_prompt = PROMPT_VALIDATION_TEXT.format(user_query=user_query, cand_str=cand_str)
            resp_str = get_chat_completion_json(formatted_prompt, model="gpt-4o-mini")
            ai_resp = json.loads(resp_str)
            
            idx = ai_resp.get("selected_index", -1)
            status = ai_resp.get("status", "CHECK").upper()
            reason = ai_resp.get("reason", "GPT Decision")
            
            if not isinstance(idx, int) or idx < 0 or idx >= len(candidates):
                return -1, "NOMATCH", "GPT Rejected"
                
            return idx, status, reason

        except Exception as e:
            if similarity >= THRESHOLD_GREEN:
                return 0, "WARNING", f"GPT Error, fallback Top1"
            return -1, "NOMATCH", f"GPT Error: {e}"