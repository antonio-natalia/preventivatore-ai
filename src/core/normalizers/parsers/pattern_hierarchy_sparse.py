import pandas as pd
from .base import BaseParser
from ..base import VoceComputoMetric # Import corretto del modello

class HierarchySparseParser(BaseParser):
    def parse(self, df: pd.DataFrame, h_idx: int) -> List[VoceComputoMetric]:
        print(f"⚡ [Parser] PATTERN_HIERARCHY_SPARSE Active")
        items = []
        df_work = df.iloc[h_idx+1:].copy()
        
        # Stack di descrizioni gerarchiche
        parent_levels = [] 
        last_code = ""

        for _, row in df_work.iterrows():
            vals = row.values
            if self._is_semantic_noise(vals): continue
            
            def get_val(idx): return str(vals[idx]).strip() if idx is not None and idx < len(vals) and pd.notna(vals[idx]) else ""
            
            raw_code = get_val(self.idx_cod)
            raw_desc = get_val(self.idx_desc)
            qta = self._safe_float(vals[self.idx_qta]) if self.idx_qta is not None else 0.0
            price = self._safe_float(vals[self.idx_price]) if self.idx_price is not None else 0.0
            
            if self.idx_total is not None and price == 0:
                total_val = self._safe_float(vals[self.idx_total])
                if total_val > 0 and qta > 0: price = round(total_val / qta, 5)

            is_target = self._is_target_row(vals, qta, price)

            if len(raw_code) > 1: last_code = raw_code

            if is_target:
                # È un figlio (Foglia) -> Scrivi voce
                full_desc = " ".join(parent_levels + [raw_desc])
                
                item = {
                    "codice_originale": last_code,
                    "descrizione_completa": full_desc.strip(),
                    "quantita": qta,
                    "unita_misura": get_val(self.idx_um),
                    "prezzo_unitario": price,
                    "prezzo_manodopera": self._safe_float(vals[self.idx_man]) if self.idx_man else 0.0,
                    "metadata": {"depth": len(parent_levels)},
                    "reasoning": "Hierarchy Sparse Merge"
                }
                items.append(item)
            else:
                # È un padre (Nodo intermedio) -> Accumula descrizione
                if raw_desc:
                    parent_levels.append(raw_desc)
                    # Safety: limita profondità stack
                    if len(parent_levels) > 4: parent_levels.pop(0)

        return [VoceComputoMetric(**i) for i in items]