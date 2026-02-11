import os
import time
import base64
import pandas as pd
from io import BytesIO
from openai import OpenAI
from src.config import settings

# Inizializza client (Singleton da settings/infra)
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

def run_digitizer_task(file_path: str, output_excel_path: str):
    """
    Refactoring di 'run_assistant_task' mantenendo la logica 1:1.
    Gestisce upload file, creazione assistant e download risultato.
    """
    print(f"👁️  Avvio Digitizer (Vision) su: {os.path.basename(file_path)}")
    
    # 1. Caricamento File (Logic 1:1)
    try:
        with open(file_path, "rb") as f:
            uploaded_file = client.files.create(
                file=f,
                purpose='assistants'
            )
        file_id = uploaded_file.id
    except Exception as e:
        print(f"❌ Errore upload file: {e}")
        return False

    # 2. Creazione Assistant (Logic 1:1)
    assistant = client.beta.assistants.create(
        name="Excel Digitizer Worker",
        instructions=PROMPT_DIGITIZER,
        model=MODEL_DIGITIZER,
        tools=[{"type": "code_interpreter"}],
        tool_resources={
            "code_interpreter": {
                "file_ids": [file_id]
            }
        }
    )

    # 3. Thread & Run (Logic 1:1)
    thread = client.beta.threads.create()
    
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant.id
    )

    # 4. Polling Loop (Logic 1:1)
    print("⏳ Waiting for Digitizer...")
    while True:
        run_status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run_status.status == 'completed':
            break
        elif run_status.status in ['failed', 'cancelled', 'expired']:
            print(f"❌ Run fallita: {run_status.status}")
            # Cleanup
            client.files.delete(file_id)
            client.beta.assistants.delete(assistant.id)
            return False
        time.sleep(2) # Polling interval originale? Assumo 2s standard

    # 5. Recupero File Generato (Logic 1:1)
    messages = client.beta.threads.messages.list(thread_id=thread.id)
    file_generated_id = None
    
    # Cerca l'ultimo file annotato nei messaggi
    for msg in messages.data:
        if msg.role == "assistant" and msg.content:
            for content_block in msg.content:
                if content_block.type == 'text' and content_block.text.annotations:
                    for ann in content_block.text.annotations:
                        if ann.type == 'file_path':
                            file_generated_id = ann.file_path.file_id
                            break
            if file_generated_id: break
    
    if file_generated_id:
        # Download
        file_content = client.files.content(file_generated_id)
        with open(output_excel_path, "wb") as f:
            f.write(file_content.read())
        print(f"✅ Excel Grezzo generato: {output_excel_path}")
        result = True
    else:
        print("❌ Nessun file Excel generato dall'Assistant.")
        result = False

    # 6. Cleanup (Logic 1:1)
    try:
        client.files.delete(file_id)
        if file_generated_id: client.files.delete(file_generated_id)
        client.beta.assistants.delete(assistant.id)
    except: pass

    return result