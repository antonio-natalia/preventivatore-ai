import os
import time
import base64
import logging
from openai import OpenAI
from src.config import settings
from src.infrastructure.telemetry import track_phase, log_metric

# Recupera logger esistente
logger = logging.getLogger("preventivatore_ai")
client = OpenAI(api_key=settings.OPENAI_API_KEY)

# --- PROMPT DIGITIZER ---
PROMPT_DIGITIZER = """
Sei un estrattore meccanico di dati. NON SEI un analista.

OBIETTIVO:
Estrai TUTTO il testo dalle tabelle del PDF e salvalo in un file Excel "raw_input.xlsx" IMMEDIATAMENTE.

REGOLE ASSOLUTE (VIETATO PENSARE):
1.  **NESSUNA PULIZIA:** Non filtrare righe, non rimuovere intestazioni, non correggere errori, non unire righe spezzate. Se il PDF ha spazzatura, voglio la spazzatura nell'Excel.
2.  **NESSUNA ANALISI:** Non cercare di capire cosa sono i dati. Copia il contenuto visivo delle tabelle cella per cella.
3.  **USO DI PDFPLUMBER:** Usa questo codice Python specifico:
    - Importa `pdfplumber`
    - Itera su tutte le pagine.
    - Usa `page.extract_table()` con tolleranza standard.
    - Accumula TUTTE le liste di liste risultanti in un unico DataFrame pandas.
    - Salva il DataFrame in Excel.
4.  **STOP IMMEDIATO:** Appena hai il DataFrame grezzo, salva il file e fermati. Non fare passaggi successivi di "verifica" o "affinamento".

OUTPUT RICHIESTO:
Soltanto il file "raw_input.xlsx".
"""

MODEL_DIGITIZER = "gpt-4o-mini"

@track_phase(phase_name="vision_digitization_task")
def run_digitizer_task(pdf_path: str, output_excel_path: str) -> bool:
    """
    Esegue il task di digitalizzazione usando OpenAI Assistants API.
    Monitorato da telemetria (durata totale e tentativi di polling).
    """
    if not os.path.exists(pdf_path):
        logger.error(f"File non trovato: {pdf_path}")
        return False

    try:
        # 1. Upload File
        logger.info(f"Uploading file to OpenAI: {os.path.basename(pdf_path)}")
        file_obj = client.files.create(
            file=open(pdf_path, "rb"),
            purpose='assistants'
        )
        file_id = file_obj.id

        # 2. Creazione Assistant Temporaneo
        assistant = client.beta.assistants.create(
            name="PDF_Digitizer_Worker",
            instructions=PROMPT_DIGITIZER,
            model=MODEL_DIGITIZER,
            tools=[{"type": "code_interpreter"}]
        )

        # 3. Creazione Thread e Run
        thread = client.beta.threads.create(
            messages=[
                {
                    "role": "user",
                    "content": "Esegui l'estrazione delle tabelle da questo file.",
                    "attachments": [{"file_id": file_id, "tools": [{"type": "code_interpreter"}]}]
                }
            ]
        )
        
        run = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=assistant.id
        )

        # 4. Polling Loop
        attempts = 0
        while run.status not in ["completed", "failed", "cancelled"]:
            attempts += 1
            if attempts % 5 == 0:
                logger.info(f"Waiting for Assistant... (Attempt {attempts})")
            
            time.sleep(2)
            run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
            
            if run.status == "failed":
                logger.error(f"Assistant Run Failed: {run.last_error}")
                # Cleanup parziale
                client.files.delete(file_id)
                client.beta.assistants.delete(assistant.id)
                return False
        
        # Metrica: Quanti cicli di attesa?
        log_metric("vision_polling_attempts", attempts, {"model": "gpt-4o"})

        # 5. Recupero File Generato
        messages = client.beta.threads.messages.list(thread_id=thread.id)
        file_generated_id = None
        
        for msg in messages.data:
            if msg.role == "assistant" and msg.content:
                for content_block in msg.content:
                    if content_block.type == 'text' and content_block.text.annotations:
                        for ann in content_block.text.annotations:
                            if ann.type == 'file_path':
                                file_generated_id = ann.file_path.file_id
                                break
                if file_generated_id: break
        
        result = False
        if file_generated_id:
            logger.info("Downloading generated Excel file...")
            file_content = client.files.content(file_generated_id)
            with open(output_excel_path, "wb") as f:
                f.write(file_content.read())
            logger.info(f"Excel salvato in: {output_excel_path}")
            result = True
        else:
            logger.warning("Nessun file Excel generato dall'Assistant.")

        # 6. Cleanup
        try:
            client.files.delete(file_id)
            if file_generated_id:
                client.files.delete(file_generated_id)
            client.beta.assistants.delete(assistant.id)
            client.beta.threads.delete(thread.id)
        except Exception as cleanup_err:
            logger.warning(f"Errore minore durante cleanup: {cleanup_err}")

        return result

    except Exception as e:
        logger.exception(f"Critical Error in Digitizer Task: {e}")
        return False