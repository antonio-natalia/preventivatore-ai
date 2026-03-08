import os
import pandas as pd
from src.config import settings

from src.infrastructure.vision_client import run_digitizer_task

# 2. IMPORTA IL NORMALIZZATORE 
from src.core.normalizers.v3_semantic import SemanticNormalizerV3
from src.core.normalizers.base import NormalizationConfig


class DigitizationService:
    def __init__(self):
        # Il costruttore ora è vuoto o può contenere inizializzazioni
        # che non dipendono dal file system.
        pass

    def _ensure_xlsx_format(self, input_file: str) -> str:
        """
        Converte CSV/XLS legacy in XLSX standard per openpyxl.
        """
        base, ext = os.path.splitext(input_file)
        if ext.lower() == '.xlsx':
            return input_file
        
        print(f"🔄 Conversione formato: {ext} -> .xlsx")
        try:
            if ext.lower() == '.csv':
                df = pd.read_csv(input_file, sep=None, engine='python')
            elif ext.lower() == '.xls':
                df = pd.read_excel(input_file)
            else:
                return input_file
            
            target = f"{base}_converted.xlsx"
            df.to_excel(target, index=False)
            return target
        except Exception as e:
            print(f"⚠️ Errore conversione: {e}. Uso originale.")
            return input_file

    def process_document(self, input_file_path: str, output_json_path: str, deep_scan: bool = False, sample_rows: int = 500):
        """
        Orchestra il flusso: Input -> (Digitizer Infra) -> Normalizer Core -> JSON
        """
        if not os.path.exists(input_file_path):
            print(f"❌ File non trovato: {input_file_path}")
            return None # Restituiamo None per indicare fallimento

        ext = os.path.splitext(input_file_path)[1].lower()
        target_xlsx = input_file_path

        # --- FASE 1: DIGITIZER (Se PDF/IMG) ---
        # Se l'input è un PDF/immagine, dobbiamo creare un Excel temporaneo.
        # Lo creiamo nella stessa directory del file JSON di output.
        
        structural_metadata = None # Metadata opzionale da Vision AI

        if ext in ['.pdf', '.png', '.jpg', '.jpeg', '.tiff']:
            output_dir = os.path.dirname(output_json_path)
            temp_raw_excel = os.path.join(output_dir, "raw_vision_output.xlsx")
            
            # run_digitizer_task ora ritorna (success, metadata)
            success, meta_dict = run_digitizer_task(input_file_path, temp_raw_excel)
            
            if not success:
                print("❌ Fase Digitizer fallita. Interruzione.")
                return None
            target_xlsx = temp_raw_excel
            
            # Convertiamo il dict in oggetto Config se presente
            if meta_dict:
                try:
                    structural_metadata = NormalizationConfig(**meta_dict)
                    print(f"🧠 Metadati Strutturali acquisiti da Vision: {structural_metadata.pattern_type}")
                except Exception as e:
                    print(f"⚠️ Errore parsing metadati Vision: {e}. Procedo con fallback.")
            
        elif ext in ['.csv', '.xls']:
            target_xlsx = self._ensure_xlsx_format(input_file_path)

        # --- FASE 2: NORMALIZZAZIONE ---
        # Il percorso del file JSON di output è ora passato come argomento.
        final_json_output = output_json_path
        
        try:
            scan_mode = "deep_scan" if deep_scan else "fast_peek"
            
            normalizer = SemanticNormalizerV3()
            results = normalizer.normalize(
                target_xlsx, 
                precomputed_config=structural_metadata,
                scan_mode=scan_mode, 
                sample_rows=sample_rows
            )
        
        except Exception as e:
            print(f"❌ Errore durante la normalizzazione: {e}")
            import traceback
            traceback.print_exc()
            return None

        # --- FASE 3: PERSISTENZA ---
        if results and len(results) > 0:
            print(f"\n💾 [Orchestrator] Salvataggio {len(results)} voci...")
            # Assumendo che results sia lista di oggetti Pydantic
            try:
                output_data = [v.model_dump() for v in results]
            except AttributeError:
                # Fallback se restituisce dict
                output_data = results
            
            import json
            with open(final_json_output, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            
            print(f"✅ File JSON pronto: {final_json_output}")
            return final_json_output # Restituisci il percorso del file generato
        else:
            print("⚠️ Nessun dato estratto.")
            return None # Restituisci None se non ci sono dati
