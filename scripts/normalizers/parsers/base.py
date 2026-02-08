from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd
import re
# Importiamo il modello dati dal livello superiore per evitare duplicazioni
from ..base import VoceComputoMetric 

class BaseParser(ABC):
    # Fallback per sicurezza
    FALLBACK_KEYWORDS = ["sommano", "totale", "riporto"]

    def __init__(self, col_map: Dict, ai_config: Dict):
        self.col_map = col_map
        self.config = ai_config
        
        # Mapping Indici Standard
        self.idx_cod = col_map.get("CODICE")
        self.idx_desc = col_map.get("DESCRIZIONE")
        self.idx_qta = col_map.get("QUANTITA")
        self.idx_um = col_map.get("UM")
        self.idx_price = col_map.get("PREZZO_UNITARIO")
        self.idx_total = col_map.get("PREZZO_TOTALE") 
        self.idx_man = col_map.get("PREZZO_MANODOPERA")
        
        # Estrazione Regole
        self.extraction_rules = ai_config.get("row_extraction_rules", {})
        self.marker_config = self.extraction_rules.get("target_row_marker", {})
        self.desc_config = self.extraction_rules.get("description_composition", {})
        self.cleaning_config = ai_config.get("cleaning", {})
        self.exclude_keywords = [k.lower() for k in self.cleaning_config.get("exclude_rows_containing", [])]

    # --- UTILS COMUNI ---
    def _safe_float(self, v):
        if pd.isna(v) or v == "": return 0.0
        s = str(v).strip().lower().replace('€', '').replace(' ', '')
        if ',' in s and '.' in s: s = s.replace('.', '').replace(',', '.')
        elif ',' in s: s = s.replace(',', '.')
        try: return float(s)
        except: return 0.0

    def _is_semantic_noise(self, row_vals: list) -> bool:
        check_text = " ".join([str(x) for x in row_vals]).lower()
        return any(k in check_text for k in self.exclude_keywords)

    def _is_target_row(self, row_vals: list, qta: float, price: float) -> bool:
        """
        Determina se la riga è valida per l'estrazione.
        CORREZIONE: Introduce logica STRICT per 'must_have_price'.
        """
        # 1. Configurazione: Prezzo Obbligatorio?
        must_have_price = self.marker_config.get("must_have_price", False)
        
        # --- LOGICA DI VALIDAZIONE VALORI ---
        if must_have_price:
            # STRICT MODE: Il prezzo > 0 è obbligatorio.
            # La quantità da sola NON rende la riga un target (es. righe di misurazione parziale).
            # Questo evita che "Campo 1" (qta=47, price=0) venga scambiato per una voce finita.
            if price <= 0:
                return False
        else:
            # PERMISSIVE MODE: Basta che ci sia Quantità O Prezzo.
            # Utile per pattern semplici o liste materiali senza importi.
            if price <= 0 and qta <= 0:
                return False

        # 2. Check Keywords (Se richieste dalla config AI)
        marker_col_idx = self.marker_config.get("column_index", -1)
        keywords = [k.lower() for k in self.marker_config.get("keywords", [])]

        # Se non ci sono keywords specifiche richieste, la validazione numerica (passo 1) è sufficiente.
        if not keywords:
            return True 

        # Se ci sono keywords, dobbiamo trovarle nella colonna indicata o ovunque
        check_val = ""
        # Caso A: Colonna specifica definita e valida
        if marker_col_idx is not None and marker_col_idx >= 0 and marker_col_idx < len(row_vals):
            check_val = str(row_vals[marker_col_idx]).strip().lower()
        # Caso B: Cerca nella Descrizione (Fallback comune)
        elif self.idx_desc is not None and self.idx_desc < len(row_vals):
            check_val = str(row_vals[self.idx_desc]).strip().lower()
        # Caso C: Cerca nell'intera riga (Fallback estremo)
        else:
            check_val = " ".join([str(x) for x in row_vals]).lower()

        # Verifica presenza keyword (es. "sommano", "totale")
        if any(k in check_val for k in keywords):
            return True
        
        # Fallback Keywords Hardcoded (Sicurezza per evitare blocchi se l'AI dimentica "Totale")
        if any(k in check_val for k in self.FALLBACK_KEYWORDS):
             return True

        return False

    @abstractmethod
    def parse(self, df: pd.DataFrame, h_idx: int) -> List[VoceComputoMetric]:
        pass