import pandas as pd
from .base import BaseParser
from ..base import VoceComputoMetric # Import corretto del modello

class BlockTotalParser(BaseParser):
    def parse(self, df: pd.DataFrame, h_idx: int) -> List[VoceComputoMetric]:
        print(f"⚡ [Parser] PATTERN_BLOCK_TOTAL Active")
        items = []
        df_work = df.iloc[h_idx+1:].copy()
        
        last_valid_code = ""
        
        for _, row in df_work.iterrows():
            vals = row.values
            if self._is_semantic_noise(vals): continue

            def get_val(idx): return str(vals[idx]).strip() if idx is not None and idx < len(vals) and pd.notna(vals[idx]) else ""
            
            raw_code = get_val(self.idx_cod)
            raw_desc = get_val(self.idx_desc)
            qta = self._safe_float(vals[self.idx_qta]) if self.idx_qta is not None else 0.0
            price = self._safe_float(vals[self.idx_price]) if self.idx_price is not None else 0.0
            
            # Gestione Prezzo Totale come unitario se serve
            if self.idx_total is not None and price == 0:
                total_val = self._safe_float(vals[self.idx_total])
                if total_val > 0 and qta > 0: price = round(total_val / qta, 5)
            
            # Check Target Row (Usa le regole AI strict)
            is_target = self._is_target_row(vals, qta, price)
            
            # Update Contesto
            if len(raw_code) > 1 and raw_code.lower() != "nan":
                last_valid_code = raw_code
            
            if is_target:
                # Fallback codice
                code_to_use = raw_code if (len(raw_code) > 1 and raw_code.lower() != "nan") else last_valid_code
                
                if code_to_use:
                    item = {
                        "codice_originale": code_to_use,
                        "descrizione_completa": raw_desc,
                        "quantita": qta,
                        "unita_misura": get_val(self.idx_um),
                        "prezzo_unitario": price,
                        "prezzo_manodopera": self._safe_float(vals[self.idx_man]) if self.idx_man else 0.0,
                        "metadata": {},
                        "reasoning": "BlockTotal Target Hit"
                    }
                    items.append(item)

        return [VoceComputoMetric(**i) for i in items]