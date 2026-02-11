import struct
import json
import logging
from tqdm import tqdm
from src.core.entities import QuoteLineItem, QuoteComponentItem, QuoteResult
from src.infrastructure.repositories import RecipeRepository
from src.infrastructure.ai_client import get_embedding, get_chat_completion_json

# Configurazione Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- COSTANTI & SOGLIE (Originali) ---
THRESHOLD_AUTO = 0.96
THRESHOLD_GREEN = 0.85
THRESHOLD_YELLOW = 0.60

# --- PROMPT ORIGINALE (NON MODIFICARE) ---
PROMPT_VALIDATION_TEXT = """
    Sei un Senior Quantity Surveyor ed esperto in computi metrici MEP.
    
    OBIETTIVO: Identificare la voce del database tecnicamente equivalente alla RDO.
    
    INPUT:
    Voce RDO: "{user_query}"
    Opzioni DATABASE:
    {cand_str}
    
    ISTRUZIONI CRITICHE (NORMALIZZAZIONE & LOGICA):
    1. NORMALIZZAZIONE UNITÀ: Converti sempre mentalmente le unità (es. 120mm = 12cm = 0.12m). Se le dimensioni fisiche coincidono, È UN MATCH.
    2. TOLLERANZA SINTATTICA: "3x1.5" equivale a "3G1,5" (G = Giallo/Verde).
    3. ANALISI FUNZIONALE: Chiediti "Posso installare l'articolo del DB al posto di quello richiesto senza varianti sostanziali?".
    
    OUTPUT JSON:
    Rispondi esclusivamente con questo formato JSON:
    {{
      "selected_index": <int o -1 se nessuno>,
    "status": "<MATCH | CHECK | NOMATCH>",
      "reason": "Spiegazione sintetica. DEVI esplicitare le conversioni fatte (es. 'Trovato 120mm che corrisponde ai 12cm richiesti')."
    }}
"""

def serialize_f32(vector):
    return struct.pack(f"{len(vector)}f", *vector)

class QuoteService:
    def __init__(self, repo: RecipeRepository):
        self.repo = repo

    def generate_quote(self, data_input: list, solo_manodopera: bool = False) -> QuoteResult:
        stats = {"processed": 0, "match": 0, "warning": 0, "nomatch": 0}
        output_items = []
        
        logger.info(f"⚙️  Avvio Quote Service (Legacy Logic 1:1). Righe: {len(data_input)}")
        
        for idx, item in enumerate(tqdm(data_input, desc="Computing Quote")):
            stats["processed"] += 1
            
            # Parsing Input
            desc = item.get("descrizione_completa", "") or item.get("description", "")
            if not desc: continue
            
            try: qta = float(item.get("quantita", 0) or 0.0)
            except: qta = 0.0
            
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
            # La funzione search_pure_vector ritorna esattamente la struttura attesa:
            # (id, distance, desc, p_mat, p_man, source)
            candidates = self.repo.search_pure_vector(serialize_f32(query_vec), limit=5)

            # 2. Validazione Match (Logica Originale Ripristinata)
            best_idx, status, reason = self._validate_match_with_gpt(desc, candidates)
            
            line.status = status
            line.reasoning = reason

            # 3. Popolamento Dati
            if best_idx >= 0:
                match = candidates[best_idx]
                # match: (id, distance, desc, p_mat, p_man, source)
                match_id = match[0]
                line.match_id = match_id
                line.match_description = match[2]
                
                raw_p_mat = float(match[3] or 0.0)
                raw_p_man = float(match[4] or 0.0)
                line.source_file = match[5] or ""
                
                if solo_manodopera:
                    line.p_unit_mat_db = 0.0
                    line.p_unit_man_db = raw_p_man
                else:
                    line.p_unit_mat_db = raw_p_mat
                    line.p_unit_man_db = raw_p_man
                    
                    # Esplosione Figli
                    comps_data = self.repo.get_components(match_id)
                    for c in comps_data:
                        try: c_qty = float(c.get('unit_quantity', 0))
                        except: c_qty = 0.0
                        try: c_price = float(c.get('unit_price', 0))
                        except: c_price = 0.0
                        
                        child = QuoteComponentItem(
                            description=c.get('description', ''),
                            unit_quantity=c_qty,
                            unit_price=c_price,
                            type=c.get('type', 'MAT')
                        )
                        child.total_price = child.unit_price * child.unit_quantity * line.quantity_input
                        line.children.append(child)

                if status == "MATCH" or status == "AUTO_MATCH": 
                    stats["match"] += 1
                elif status == "WARNING":
                    stats["warning"] += 1
            else:
                stats["nomatch"] += 1

            output_items.append(line)

        return QuoteResult(items=output_items, stats=stats)

    def _validate_match_with_gpt(self, user_query, candidates):
        """
        Logica Ibrida: Thresholds + GPT.
        Ritorna: best_idx, status, reason
        COPIA 1:1 DAL CODICE ORIGINALE
        """
        if not candidates:
            return -1, "NOMATCH", "Nessun candidato nel DB"

        top_candidate = candidates[0]
        # candidates[0] = (id, distance, desc, p_mat, p_man, source)
        # distance è all'indice 1
        similarity = 1 - top_candidate[1]  # cosine distance to similarity

        # 1. FAST PATH: Auto-Accept (Risparmio API)
        if similarity >= THRESHOLD_AUTO:
            return 0, "MATCH", f"Auto-Match per alta similarità ({similarity:.2f})"

        # 2. FAST PATH: Rejection immediata (Troppo diversi)
        if similarity < THRESHOLD_YELLOW:
            return -1, "NOMATCH", f"Similarità troppo bassa ({similarity:.2f})"

        # 3. GPT JUDGE (Zona Grigia o Verde Bassa)
        cand_str = ""
        for i, c in enumerate(candidates):
            # c: (id, distance, desc, p_mat, p_man, source)
            sim = 1 - c[1]
            # Mapping indici: 2=desc, 5=source, 3=p_mat, 4=p_man
            cand_str += f"[{i}] {c[2]} (Sim: {sim:.2f}) | Src: {c[5]} | p_mat: {c[3]} | p_man: {c[4]}\n"

        try:
            formatted_prompt = PROMPT_VALIDATION_TEXT.format(user_query=user_query, cand_str=cand_str)
            
            # Utilizziamo il wrapper infrastrutturale ma passiamo il modello richiesto 'gpt-4o-mini'
            resp_str = get_chat_completion_json(formatted_prompt, model="gpt-4o-mini")
            
            ai_resp = json.loads(resp_str)
            idx = ai_resp.get("selected_index", -1)
            status = ai_resp.get("status", "CHECK").upper()
            reason = ai_resp.get("reason", "GPT Decision")
            
            # Validazione indici
            if not isinstance(idx, int) or idx < 0 or idx >= len(candidates):
                return -1, "NOMATCH", "GPT ha scartato tutti i candidati"
                
            return idx, status, reason

        except Exception as e:
            # Fallback euristico se GPT fallisce
            if similarity >= THRESHOLD_GREEN:
                return 0, "WARNING", f"GPT Error, fallback su Top1 ({similarity:.2f})"
            return -1, "NOMATCH", f"GPT Error: {str(e)}"