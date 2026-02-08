import pandas as pd
import json
import os
from typing import List, Dict
from openai import OpenAI
from dotenv import load_dotenv, find_dotenv

from .base import BaseNormalizer, VoceComputoMetric
from .parsers.pattern_block_total import BlockTotalParser
from .parsers.pattern_measurement_list import MeasurementListParser
from .parsers.pattern_hierarchy_sparse import HierarchySparseParser
from .prompts.pattern_recognition import PROMPT_PATTERN_RECOGNITION_V3

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

class SemanticNormalizerV3(BaseNormalizer):
    def __init__(self):
        self.model = "gpt-4o"
        self.parsers_map = {
            "PATTERN_BLOCK_TOTAL": BlockTotalParser,
            "PATTERN_MEASUREMENT_LIST": MeasurementListParser,
            "PATTERN_HIERARCHY_SPARSE": HierarchySparseParser
        }

    def _prepare_ai_payload(self, df: pd.DataFrame, scan_mode: str, sample_rows: int) -> str:
        """
        Prepara il contenuto CSV per l'AI in base alla modalità richiesta.
        """
        total_rows = len(df)
        
        # MODALITÀ DEEP SCAN: Tutto il contenuto
        if scan_mode == "deep_scan":
            print(f"🔬 [Deep Scan] Preparazione payload completo ({total_rows} righe)...")
            # Converte tutto in stringa CSV
            return df.to_csv(index=False, header=False, sep="|")

        # MODALITÀ FAST PEEK: Testa + Coda
        print(f"👀 [Fast Peek] Preparazione payload parziale (Head/Tail: {sample_rows})...")
        
        if total_rows <= (sample_rows * 2):
            return df.to_csv(index=False, header=False, sep="|")
        
        head_csv = df.head(sample_rows).to_csv(index=False, header=False, sep="|")
        tail_csv = df.tail(sample_rows).to_csv(index=False, header=False, sep="|")
        
        return f"{head_csv}\n... [OMISSIS: {total_rows - (sample_rows*2)} ROWS SKIPPED] ...\n{tail_csv}"

    def _analyze_pattern_with_ai(self, df: pd.DataFrame, scan_mode: str, sample_rows: int) -> Dict:
        # Prepara i dati in base al flag CLI
        csv_content = self._prepare_ai_payload(df, scan_mode, sample_rows)
        
        # Costruisce il prompt (PROMPT INVARIATO)
        full_prompt = PROMPT_PATTERN_RECOGNITION_V3 + f"\n\nDATI GREZZI DA ANALIZZARE:\n{csv_content}"
        
        try:
            response = client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": full_prompt}],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"❌ [AI Error] {e}")
            return {}

    def _resolve_column_indices(self, df: pd.DataFrame, h_idx: int, name_mapping: Dict) -> Dict:
        if h_idx >= len(df): return {}
        
        header_row = [str(x).strip().lower() for x in df.iloc[h_idx].tolist()]
        header_map = {val: idx for idx, val in enumerate(header_row) if val != "nan"}
        
        resolved = {}
        target_keys = {
            "item_code": "CODICE", "description": "DESCRIZIONE",
            "quantity": "QUANTITA", "unit_measure": "UM",
            "unit_price": "PREZZO_UNITARIO", "total_price": "PREZZO_TOTALE"
        }

        print(f"🔎 Header rilevato: {list(header_map.keys())}")

        for ai_key, parser_key in target_keys.items():
            ai_name = name_mapping.get(ai_key)
            if ai_name:
                clean_name = str(ai_name).strip().lower()
                # 1. Match Esatto
                if clean_name in header_map:
                    resolved[parser_key] = header_map[clean_name]
                # 2. Match Parziale
                else:
                    for h_name, h_idx in header_map.items():
                        if clean_name in h_name or h_name in clean_name:
                            resolved[parser_key] = h_idx
                            break
        return resolved

    def _update_marker_index(self, ai_config: Dict, df: pd.DataFrame, h_idx: int):
        marker = ai_config.get("row_extraction_rules", {}).get("target_row_marker", {})
        col_name = marker.get("column_name")
        if col_name and isinstance(col_name, str):
            header_row = [str(x).strip().lower() for x in df.iloc[h_idx].tolist()]
            clean_target = col_name.strip().lower()
            for idx, val in enumerate(header_row):
                if val == clean_target:
                    marker["column_index"] = idx
                    break

    def normalize(self, file_path: str, **kwargs) -> List[VoceComputoMetric]:
        # 1. Gestione Parametri CLI
        scan_mode = kwargs.get("scan_mode", "fast_peek")
        sample_rows = kwargs.get("sample_rows", 50)
        
        abs_path = os.path.abspath(file_path)
        print(f"\n🚀 [V3 Semantic] Avvio su: {abs_path}")
        print(f"🔧 Config: Mode={scan_mode.upper()} | SampleRows={sample_rows}")

        # 2. Caricamento
        try:
            df = pd.read_excel(abs_path, header=None)
        except:
            try: df = pd.read_csv(abs_path, header=None, sep=None, engine='python')
            except Exception as e:
                print(f"❌ Errore file: {e}"); return []

        # 3. Analisi (Senza Retry)
        ai_config = self._analyze_pattern_with_ai(df, scan_mode, sample_rows)
        
        pattern_type = ai_config.get("pattern_type")
        if not pattern_type or pattern_type not in self.parsers_map:
            print(f"🛑 Configurazione AI Invalida o Pattern Sconosciuto: {pattern_type}")
            print(json.dumps(ai_config, indent=2))
            return []

        # 4. Parsing
        h_idx = ai_config.get("header_row_index", 0)
        col_map = self._resolve_column_indices(df, h_idx, ai_config.get("column_mapping", {}))
        self._update_marker_index(ai_config, df, h_idx)
        
        print(f"⚙️ Pattern: {pattern_type}")
        print(f"🗺️ Mapping: {col_map}")
        
        parser = self.parsers_map[pattern_type](col_map, ai_config)
        try:
            return parser.parse(df, h_idx)
        except Exception as e:
            print(f"❌ Errore Parser: {e}")
            return []