import os
import asyncio
import base64
import logging
import json
import fitz  # PyMuPDF
import pandas as pd
from typing import List, Dict, Optional, Tuple, Any
# from openai import AsyncOpenAI # Rimosso, useremo il wrapper di ai_client
from src.config import settings
from src.infrastructure.telemetry import track_phase, log_metric
from src.infrastructure.ai_client import get_async_chat_completion_json # Importiamo il nuovo wrapper

# Recupera logger esistente
logger = logging.getLogger("preventivatore_ai")
# aclient = AsyncOpenAI(api_key=settings.OPENAI_API_KEY) # Rimosso, usa aclient da ai_client

# --- PROMPT DIGITIZER MASTER ---
# Questo prompt è usato per il chunk iniziale (es. prime 5 pagine) per capire la struttura E estrarre i primi dati.
# Allineato con PROMPT_PATTERN_RECOGNITION_V3 per i nomi delle chiavi di output.
PROMPT_MASTER_STRUCTURE = """
Sei un Senior Data Scientist esperto in parsing di documenti tecnici (Computi Metrici, Listini, BOM).
Il tuo compito è analizzare la struttura di un file PDF/Immagine grezzo e generare una configurazione JSON per estrarre le voci di costo in modo deterministico, oltre ad estrarre i dati tabellari.

# OBIETTIVO
Devi creare una strategia di estrazione che funzioni per il file specifico fornito, identificando:
1. Le colonne corrette (evitando falsi positivi).
2. La logica per identificare le righe valide (quelle che portano il costo).
3. Estrai TUTTI i dati tabellari visibili.

OUTPUT FORMAT (JSON ONLY):
{
  "structure": {
    "pattern_type": "PATTERN_BLOCK_TOTAL" | "PATTERN_MEASUREMENT_LIST" | "PATTERN_HIERARCHY_SPARSE",
    "raw_headers": List[str], // Nomi esatti delle intestazioni di colonna identificate visivamente, in ordine.
    "header_row_index": int, // Indice 0-based della riga di intestazione nella matrice 'table_data'.
    "column_mapping": {
        "item_code": "Nome Esatto Colonna Codice",      // Deve essere ESATTAMENTE uno dei nomi in raw_headers
        "description": "Nome Esatto Colonna Descrizione", // Deve essere ESATTAMENTE uno dei nomi in raw_headers
        "unit_measure": "Nome Esatto Colonna UM",       // Deve essere ESATTAMENTE uno dei nomi in raw_headers
        "quantity": "Nome Esatto Colonna Quantità",     // Deve essere ESATTAMENTE uno dei nomi in raw_headers
        "unit_price": "Nome Esatto Colonna Prezzo Unitario", // Deve essere ESATTAMENTE uno dei nomi in raw_headers
        "total_price": "Nome Esatto Colonna Totale"     // Deve essere ESATTAMENTE uno dei nomi in raw_headers
    },
    "row_extraction_rules": {
      "target_row_marker": {
        "column_name": "Nome Esatto Colonna Descrizione", // Colonna dove cercare le keyword
        "keywords": [], // Es. ["SOMMANO", "TOTALE"]
        "must_have_price": true
      },
      "description_composition": {
        "strategy": "CURRENT_ROW_ONLY" | "MERGE_UPWARDS_UNTIL_CODE" | "MERGE_PREVIOUS_ROWS",
        "separator": " "
      }
    },
    "cleaning": {
        "exclude_rows_containing": ["Pagina", "Riporto", "TOTALE GENERALE"]
    },
    "analysis_reasoning": "Spiegazione del perché hai scelto queste colonne e questo pattern."
  },
  "table_data": [
    ["Header1", "Header2", "Header3"],
    ["Val1", "Val2", "Val3"]
  ]
}

# REGOLE ESTRAZIONE DATI
- Estrai TUTTE le righe visibili nelle tabelle.
- Includi l'intestazione della tabella come prima riga di 'table_data'.
- Sii fedele al contenuto visivo.
- **MASSIMA RIGOROSITÀ NELLA SEGMENTAZIONE DELLE COLONNE**: NON unire mai il contenuto di celle che logicamente appartengono a colonne diverse. Anche se visivamente il testo di una colonna sembra sovrapporsi o invadere lo spazio della colonna adiacente nel PDF/immagine, devi mantenere i confini logici e generare celle distinte nella 'table_data'. Ogni contenuto individuato visivamente in una 'colonna' deve rimanere nella sua 'colonna' e non fondersi con altre. Se una colonna appare vuota, restituisci una stringa vuota `""`.
"""

# --- PROMPT DIGITIZER WORKER ---
# Usato per le pagine successive, estrae solo i dati allineandoli alla struttura nota.
# Questo prompt verrà costruito dinamicamente con il contesto dei raw_headers.
PROMPT_WORKER_TEMPLATE = """
Sei un estrattore meccanico di dati tabellari.
Il tuo compito è estrarre i dati tabellari dalle immagini delle pagine fornite, mantenendo una segmentazione delle colonne estremamente rigorosa.

# CONTESTO STRUTTURALE (Identificato dalla fase Master)
Le colonne attese e il loro ordine sono: {raw_headers_context}
Le intestazioni semantiche mappate sono: {column_mapping_context}

# REGOLE ESTRAZIONE DATI
- Estrai TUTTE le righe visibili nelle tabelle.
- NON includere nuovamente l'intestazione della tabella se è una continuazione.
- **MASSIMA RIGOROSITÀ NELLA SEGMENTAZIONE DELLE COLONNE**: Segui la segmentazione delle colonne basata sul contesto fornito ({raw_headers_context}). NON unire mai il contenuto di celle che logicamente appartengono a colonne diverse. Anche se visivamente il testo di una colonna sembra sovrapporsi o invadere lo spazio della colonna adiacente nel PDF/immagine, devi mantenere i confini logici e generare celle distinte. Ogni contenuto individuato visivamente in una 'colonna' deve rimanere nella sua 'colonna' e non fondersi con altre. Se una colonna appare vuota, restituisci una stringa vuota `""`.
- Allinea i dati estratti rigorosamente a queste colonne.

OUTPUT JSON: {{"table_data": [["cell1", "cell2"], ...]}}
"""

# MODEL_DIGITIZER = "gpt-4o" # Ora da settings

async def _process_page_chunk(chunk_images: List[Dict], prompt: str, is_master: bool = False) -> Dict:
    """
    Processa un blocco di immagini con GPT-4o Vision usando il wrapper di ai_client.
    """
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                *chunk_images
            ],
        }
    ]
    
    try:
        # Usiamo il wrapper astratto per le chiamate AI
        result = await get_async_chat_completion_json(
            messages=messages, 
            model=settings.VISION_MODEL, 
            temperature=0.0
        )
        # get_async_chat_completion_json gestisce già retry e backoff.
        # Se fallisce dopo i retry, solleva un'eccezione, che verrà catturata da _run_digitizer_task_async
        return result if result is not None else {"table_data": []} if not is_master else {}
    except Exception as e:
        logger.error(f"Errore nel processare il chunk di pagine (vision_client): {e}")
        # Rilanciamo l'eccezione per farla gestire al chiamante, come _run_digitizer_task_async
        raise


@track_phase(phase_name="vision_digitization_task")
def run_digitizer_task(pdf_path: str, output_excel_path: str) -> Tuple[bool, Optional[Dict]]:
    """
    Esegue il task di digitalizzazione usando OpenAI Vision API.
    Ritorna (success, metadata_strutturale).
    """
    return asyncio.run(_run_digitizer_task_async(pdf_path, output_excel_path))

async def _run_digitizer_task_async(pdf_path: str, output_excel_path: str) -> Tuple[bool, Optional[Dict]]:
    if not os.path.exists(pdf_path):
        logger.error(f"File non trovato: {pdf_path}")
        return False, None

    try:
        # 1. Conversione PDF in immagini
        logger.info(f"Converting PDF {os.path.basename(pdf_path)} to images...")
        doc = fitz.open(pdf_path)
        all_images = []
        
        for page in doc:
            pix = page.get_pixmap()
            img_bytes = pix.pil_tobytes(format="PNG")
            b64 = base64.b64encode(img_bytes).decode("utf-8")
            all_images.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "high"}
            })
        doc.close()
        
        total_pages = len(all_images)
        logger.info(f"Converted {total_pages} pages.")

        # 2. Strategia Chunking
        MASTER_CHUNK_SIZE = settings.VISION_MASTER_CHUNK_SIZE
        WORKER_CHUNK_SIZE = settings.VISION_WORKER_CHUNK_SIZE
        
        full_table_data = []
        structural_metadata = None
        
        # --- FASE 1: MASTER (Prime N pagine) ---
        master_images = all_images[:MASTER_CHUNK_SIZE]
        logger.info(f"Processing MASTER chunk (pages 1-{len(master_images)})...")
        
        master_result = await _process_page_chunk(master_images, PROMPT_MASTER_STRUCTURE, is_master=True)
        
        if not master_result or "table_data" not in master_result:
            logger.error("Master chunk failed extraction.")
            return False, None
            
        full_table_data.extend(master_result["table_data"])
        structural_metadata = master_result.get("structure")
        
        if not structural_metadata:
             logger.warning("Master chunk produced data but NO structural metadata.")
        else:
             logger.info(f"Structure identified: {structural_metadata.get('pattern_type')}")

        # --- FASE 2: WORKERS (Resto delle pagine in parallelo) ---
        remaining_images = all_images[MASTER_CHUNK_SIZE:]
        if remaining_images:
            logger.info(f"Processing remaining {len(remaining_images)} pages in parallel chunks...")
            
            tasks = []
            
            # Prepariamo il prompt Worker con il contesto dalla fase Master
            worker_prompt = PROMPT_WORKER_TEMPLATE
            if structural_metadata:
                raw_headers_context = structural_metadata.get("raw_headers", [])
                column_mapping_context = structural_metadata.get("column_mapping", {})
                worker_prompt = PROMPT_WORKER_TEMPLATE.format(
                    raw_headers_context=json.dumps(raw_headers_context), # Assicuriamo il formato stringa per il prompt
                    column_mapping_context=json.dumps(column_mapping_context) # Assicuriamo il formato stringa per il prompt
                )
            else:
                logger.warning("Nessun metadato strutturale dal Master, worker userà prompt generico.")
            
            for i in range(0, len(remaining_images), WORKER_CHUNK_SIZE):
                chunk = remaining_images[i : i + WORKER_CHUNK_SIZE]
                tasks.append(_process_page_chunk(chunk, worker_prompt)) # Passa il prompt dinamico
            
            worker_results = await asyncio.gather(*tasks)
            
            for res in worker_results:
                chunk_data = res.get("table_data", [])
                full_table_data.extend(chunk_data)

        # 3. Salvataggio Excel
        if not full_table_data:
            return False, None
            
        df = pd.DataFrame(full_table_data)
        df.to_excel(output_excel_path, index=False, header=False)
        
        logger.info(f"Excel saved: {output_excel_path} (Rows: {len(df)})")
        log_metric("vision_pages_processed", total_pages, {"model": settings.VISION_MODEL})
        
        return True, structural_metadata

    except Exception as e:
        logger.exception(f"Critical error in Digitizer Task: {e}")
        return False, None
