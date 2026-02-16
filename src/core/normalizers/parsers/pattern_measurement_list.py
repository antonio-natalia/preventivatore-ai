import pandas as pd
from typing import List
from .base import BaseParser
from ..base import VoceComputoMetric # Import corretto del modello

class MeasurementListParser(BaseParser):
    def parse(self, df: pd.DataFrame, h_idx: int) -> List[VoceComputoMetric]:
        print(f"⚡ [Parser] PATTERN_MEASUREMENT_LIST Active")
        items = []
        df_work = df.iloc[h_idx+1:].copy()
        
        # Config Strategia
        desc_strategy = self.desc_config.get("strategy", "MERGE_UPWARDS_UNTIL_CODE")
        desc_separator = self.desc_config.get("separator", " ")

        # Stato
        current_code = ""
        current_desc_buffer = [] 
        current_um = ""
        
        for _, row in df_work.iterrows():
            vals = row.values
            if self._is_semantic_noise(vals): continue

            def get_val(idx): return str(vals[idx]).strip() if idx is not None and idx < len(vals) and pd.notna(vals[idx]) else ""
            
            raw_code = get_val(self.idx_cod)
            raw_desc = get_val(self.idx_desc)
            qta = self._safe_float(vals[self.idx_qta]) if self.idx_qta is not None else 0.0
            price = self._safe_float(vals[self.idx_price]) if self.idx_price is not None else 0.0
            
            # Recupero totale se necessario
            if self.idx_total is not None and price == 0:
                total_val = self._safe_float(vals[self.idx_total])
                if total_val > 0 and qta > 0: price = round(total_val / qta, 5)

            # 1. Update Stato (Nuovo Codice)
            if len(raw_code) > 1 and raw_code.lower() != "nan":
                if raw_code != current_code:
                    current_code = raw_code
                    current_desc_buffer = [] 
                    current_um = get_val(self.idx_um)

            # 2. Check Target (Riga di Chiusura)
            is_target = self._is_target_row(vals, qta, price)

            if is_target and current_code:
                # Se la riga target ha testo utile, lo aggiungiamo al buffer prima del merge
                if raw_desc and not any(k in raw_desc.lower() for k in self.exclude_keywords):
                     current_desc_buffer.append(raw_desc)

                final_desc = ""
                if desc_strategy == "CURRENT_ROW_ONLY":
                    final_desc = raw_desc
                else: 
                    # MERGE_UPWARDS_UNTIL_CODE
                    final_desc = desc_separator.join(current_desc_buffer)

                item = {
                    "codice_originale": current_code,
                    "descrizione_completa": final_desc.strip(),
                    "quantita": qta,
                    "unita_misura": current_um if current_um else get_val(self.idx_um),
                    "prezzo_unitario": price,
                    "prezzo_manodopera": self._safe_float(vals[self.idx_man]) if self.idx_man else 0.0,
                    "metadata": {"strategy": desc_strategy},
                    "reasoning": "MeasurementList Target Hit"
                }
                items.append(item)
                current_desc_buffer = [] # Reset buffer descrizioni

            # 3. Accumulo Buffer (Se non è target)
            else:
                if raw_desc:
                    if not current_desc_buffer or current_desc_buffer[-1] != raw_desc:
                        current_desc_buffer.append(raw_desc)

        return [VoceComputoMetric(**i) for i in items]