import os
import time
import sys
import re
import pandas as pd
import warnings
from openai import OpenAI, RateLimitError
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings("ignore", category=DeprecationWarning)

# --- PATH SETUP INTELLIGENTE ---
dotenv_path = find_dotenv()

if not dotenv_path:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
else:
    load_dotenv(dotenv_path)
    PROJECT_ROOT = os.path.dirname(dotenv_path)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- CONFIGURAZIONE ---
INPUT_FILENAME = "input_cliente.pdf" 

# Costruiamo i percorsi partendo dalla ROOT reale del progetto
INPUT_FILE = os.path.join(PROJECT_ROOT, "richieste_ordine", INPUT_FILENAME)
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "richieste_ordine", "input_cliente_clean.xlsx")
TEMP_RAW_EXCEL = os.path.join(PROJECT_ROOT, "richieste_ordine", "temp_raw_extraction.xlsx")

# --- 1. PROMPT DIGITIZER (Solo per PDF -> Raw Excel) ---
# Usiamo gpt-4o-mini qui perché è più veloce ed economico per task meccanici
MODEL_DIGITIZER = "gpt-4o-mini" 

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

# --- 2. PROMPT NORMALIZER (Raw Excel -> Clean Flat Excel) ---
# Usiamo gpt-4o qui perché serve ragionamento logico complesso
MODEL_NORMALIZER = "gpt-4o"

PROMPT_NORMALIZER = """
    Sei un Senior Quantity Surveyor ed esperto Data Engineer.
    Il tuo obiettivo è normalizzare un Computo Metrico (RDO) disordinato in un formato standard "FLAT" (Piatto) estraendo le voci di computo.

    OUTPUT RICHIESTO (Excel):
    Genera un file con queste colonne esatte:
    - CODICE: Identificativo univoco (del figlio, se presente).
    - DESCRIZIONE (Testo): Deve contenere tutti i requisiti tecnici ed i dettagli necessari per l'individuazione della giusta voce di prezzo.
    --esempio 1: "Cavo multipolare flessibile resistente al fuoco, non propagante l'incendio, senza alogeni, conforme ai requisiti previsti dalla Normativa Europea Regolamento UE 305/2011 - Prodotti ... UNEL 35024/ 1, CEI UNEL 35026, UNI EN 13501-6; sigla di designazione FTG18(0)M16, tensione nominale 0,6/1 kV:- 7Gx1,5 mm²"
    --esempio 2: "Fornitura in opera di Quadro Elettrico di reparto superficie media con degenze e/o ambulatori" 
    - QUANTITA: Numero float. identifica la quantità richiesta per ogni voce
    - UNITA_DI_MISURA: ad esempio "m", "mq", "cad", "kg", "lt", "h", ecc.
    - PREZZO_UNITARIO: Se presente, estrai il prezzo unitario indicato nella RDO. Se non è presente, lascia vuoto.
    - PREZZO_MANODOPERA: Se presente, estrai il prezzo della manodopera indicato nella RDO. Se non è presente, lascia vuoto.
    - METADATI: Info di posizione o dettagli non tecnici.

    ISTRUZIONI DI DIAGNOSI (PYTHON):
    Analizza la struttura del file. Identifica quale dei 3 pattern logici viene usato e applica la logica corrispondente:

    PATTERN A: STRUTTURA "PIATTA" (Riga Singola)
    - Riconoscimento: Ogni riga ha Codice, Descrizione, Quantità e importi popolati.
    - Azione: Estrai i dati direttamente.

    PATTERN B: STRUTTURA "A MISURAZIONI" (Stesso Codice ripetuto)
    - Riconoscimento: Lo stesso Codice Articolo si ripete su più righe. Una di esse è solitamente la principale e contiene la descrizione con le specifiche tecniche, le altre possono avere delle misure(es. "lunghezza 5.00") oppure indicare un totale ("Sommano", "Totale").
    - Azione: Raggruppa per Codice. Descrizione = la descrizione della riga principale oppure l'unione delle descrizione se quelle secondarie contengono specifiche tecniche. Quantità = la riga che esprime il totale oppure somma delle parziali. Prezzo Unitario e Manodopera = quelli della riga che esprime i totali oppure somma delle parziali.

    PATTERN C: STRUTTURA "GERARCHICA / VARIANTI" (Padre-Figlio)
    - Riconoscimento:
    1. C'è una riga PADRE con Descrizione generica (es. "Cavo multipolare...") ma SENZA Quantità (o qta=0).
    2. Seguono righe FIGLIE con descrizioni brevi (es. "sez. 3x1.5", "sez. 4x2.5") e con Quantità > 0.
    3. I codici possono mostrare gerarchia (es. Padre "1.01", Figlio "1.01.a"), oppure essere sequenziali (es. Padre "A3.1.15", Figlio "A3.1.16", "A3.1.17").
    - Azione (Ereditarietà):
    -- Per ogni FIGLIO, crea una voce di computo. il codice è quello del figlio, la Descrizione Finale deve essere: "DESCRIZIONE PADRE + DESCRIZIONE FIGLIO".
    -- Esempio Output: "Cavo multipolare... - sez. 3x1.5".
    -- La Quantità, l'Unità di Misura ed i prezzi sono quelli del Figlio.

    REGOLE GENERALI CRITICHE:
    1. **Unicità della Riga:** L'output deve avere una riga per ogni voce PREZZABILE (cioè con Quantità > 0).
    2. **Descrizione Tecnica:** Mantieni "Fornitura e posa". Rimuovi riferimenti puramente logistici (Piani, Stanze) spostandoli nei METADATI.
    3. **Pulizia Numeri:** Gestisci formato italiano (1.000,00 -> 1000.0 e 16,00 -> 16.0).

    PROCEDURA OPERATIVA (PYTHON):
    1. Carica il dataframe.
    2. Identifica header e colonne.
    3. Identifica il pattern logico.
    4. Itera sulle righe mantenendo una variabile 'parent' che indica il dataframe su cui si sta lavorando.
    - Se trovi una riga con codice indentico vuol dire che è lo stesso articolo -> capisci se la descrizione è rilevante o no e nel caso mergia la descrizione con 'parent', capisci se si tratta di una riga totale o di misura e aggiorna le quantità e gli importi di 'parent'.
    - Se trovi una riga con codice diverso -> capisci se sei in presenza di un figlio (codice che inizia con il padre, oppure codice immediatamente successivo (es: padre A3.1.15, figli A3.1.16 e A3.1.17) con descrizione breve che aggiunge dettagli tecnici) o di un nuovo articolo.
    -- Se è un figlio -> capisce se la descrizione è autonoma o deve ereditare dal 'parent', prendi quantità, unità di misura e prezzi del figlio.
    -- Se è un nuovo articolo -> scrivi 'parent' su output e inizia il nuovo dataframe.
    5. Genera il file output 'normalized_quote.xlsx' e rendilo disponibile per il download.
    """

def upload_file_to_openai(filepath):
    """Carica un file su OpenAI con gestione retry."""
    print(f"   ⬆️  Upload {os.path.basename(filepath)}...", end="")
    try:
        file_obj = client.files.create(
            file=open(filepath, "rb"),
            purpose='assistants'
        )
        print(f" Fatto ({file_obj.id})")
        return file_obj
    except Exception as e:
        print(f"\n❌ Errore Upload: {e}")
        return None

def extract_wait_time(error_message):
    """Estrae i secondi da attendere dal messaggio di errore di OpenAI."""
    try:
        match = re.search(r"try again in (\d+(\.\d+)?)s", str(error_message))
        if match:
            return float(match.group(1))
    except: pass
    return 10.0 # Default fallback

def run_assistant_task(task_name, file_obj, instructions, model_name, output_filename=None):
    """
    Esegue un task con gestione automatica del RATE LIMIT.
    """
    print(f"   🤖 Avvio Agente: {task_name} (Model: {model_name})...")
    
    assistant = client.beta.assistants.create(
        name=f"Worker_{task_name}",
        instructions=instructions,
        model=model_name,
        tools=[{"type": "code_interpreter"}]
    )

    thread = client.beta.threads.create(
        messages=[{
            "role": "user",
            "content": "Esegui il task sul file allegato. Genera il file richiesto.",
            "attachments": [{"file_id": file_obj.id, "tools": [{"type": "code_interpreter"}]}]
        }]
    )

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        # Avvio la run
        run = client.beta.threads.runs.create(thread_id=thread.id, assistant_id=assistant.id)
        
        # Polling
        spinner = ['|', '/', '-', '\\']
        idx = 0
        start_time = time.time()
        
        while True:
            run_status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
            elapsed = int(time.time() - start_time)
            
            sys.stdout.write(f"\r   ⏳ Status: {run_status.status.upper()}... {spinner[idx % 4]} ({elapsed}s)")
            sys.stdout.flush()
            idx += 1

            if run_status.status == 'completed':
                sys.stdout.write("\n")
                print(f"   ✅ Task completato in {elapsed}s.")
                # Successo -> Usciamo dal loop di retry e dal loop di polling
                retry_count = 999 
                break
                
            elif run_status.status == 'failed':
                sys.stdout.write("\n")
                err_code = run_status.last_error.code
                err_msg = run_status.last_error.message
                
                if err_code == 'rate_limit_exceeded':
                    wait_s = extract_wait_time(err_msg) + 2.0 # +2s di buffer
                    print(f"⚠️  RATE LIMIT RAGGIUNTO. Pausa di raffreddamento: {wait_s:.1f}s...")
                    time.sleep(wait_s)
                    print("   🔄 Riprovo l'esecuzione...")
                    retry_count += 1
                    break # Usciamo dal loop di polling per ricreare la Run nel loop esterno
                else:
                    print(f"❌ ERRORE CRITICO AI ({task_name}): {err_code} - {err_msg}")
                    client.beta.assistants.delete(assistant.id)
                    return None
            
            elif run_status.status in ['cancelled', 'expired']:
                sys.stdout.write("\n")
                print(f"❌ Task {run_status.status}.")
                client.beta.assistants.delete(assistant.id)
                return None
            
            time.sleep(0.5)
        
        # Se abbiamo completato (retry_count 999), procediamo al download
        if retry_count == 999:
            break

    # --- RECUPERO OUTPUT ---
    if retry_count < 999: # Se siamo usciti per max retries
        print(f"\n❌ Troppi tentativi falliti per Rate Limit.")
        client.beta.assistants.delete(assistant.id)
        return None

    print(f"   📥 Analisi output ({task_name})...")
    messages = client.beta.threads.messages.list(thread_id=thread.id)
    file_id_out = None
    
    for msg in messages.data:
        if msg.role == "assistant":
            if msg.attachments:
                file_id_out = msg.attachments[0].file_id
                break
            for content in msg.content:
                if content.type == 'text':
                    for ann in content.text.annotations:
                        if ann.type == 'file_path':
                            file_id_out = ann.file_path.file_id
                            break
        if file_id_out: break

    saved_path = None
    if file_id_out:
        data = client.files.content(file_id_out)
        target_path = output_filename if output_filename else f"temp_{task_name}.xlsx"
        with open(target_path, "wb") as f:
            f.write(data.read())
        saved_path = target_path
        print(f"   💾 FILE SALVATO: {saved_path}")
        client.files.delete(file_id_out)
    else:
        print(f"\n⚠️  Nessun file generato da {task_name}.")
        print("   Ultimo messaggio AI:")
        for msg in messages.data:
            if msg.role == "assistant":
                print(f"   > {msg.content[0].text.value}")
                break

    client.beta.assistants.delete(assistant.id)
    return saved_path

def convert_legacy_excel(filepath):
    filename, ext = os.path.splitext(filepath)
    ext = ext.lower()
    if ext == '.xlsx': return filepath, False
    
    print(f"   🔄 Conversione legacy {ext} -> .xlsx...", end="")
    try:
        if ext == '.xls':
            df = pd.read_excel(filepath, header=None)
        elif ext == '.csv':
            df = pd.read_csv(filepath, header=None)
        else:
            return filepath, False
        
        temp_path = filename + "_temp.xlsx"
        df.to_excel(temp_path, index=False, header=False)
        print(" Fatto.")
        return temp_path, True
    except Exception as e:
        print(f"\n❌ Errore conversione locale: {e}")
        return filepath, False

def main_pipeline():
    print(f"🚀 AVVIO PIPELINE DI NORMALIZZAZIONE")
    print(f"   Input: {INPUT_FILE}")

    if not os.path.exists(INPUT_FILE):
        print("❌ File non trovato.")
        return

    # --- FASE 0: Preparazione File ---
    current_file_path = INPUT_FILE
    is_temp_file = False
    filename, ext = os.path.splitext(INPUT_FILE)
    ext = ext.lower()

    # --- FASE 1: DIGITIZER (Solo se PDF) ---
    if ext == '.pdf':
        print("\n📄 Rilevato PDF: Avvio Fase 1 (Estrazione Geometrica)...")
        pdf_obj = upload_file_to_openai(current_file_path)
        if not pdf_obj: return

        # Qui usiamo MODEL_DIGITIZER (gpt-4o-mini) per risparmiare token/rate limit
        raw_excel_path = run_assistant_task(
            "Digitizer", 
            pdf_obj, 
            PROMPT_DIGITIZER, 
            model_name=MODEL_DIGITIZER, # <--- SWITCH MODELLO
            output_filename=TEMP_RAW_EXCEL
        )
        
        client.files.delete(pdf_obj.id)
        
        if not raw_excel_path:
            print("❌ Fase 1 fallita. Impossibile procedere.")
            return
            
        current_file_path = raw_excel_path
        is_temp_file = True 

    elif ext in ['.xls', '.csv']:
        current_file_path, is_temp_file = convert_legacy_excel(current_file_path)

    # --- FASE 2: NORMALIZER (Logica Semantica) ---
    print("\n🧠 Avvio Fase 2 (Analisi Logica e Normalizzazione)...")
    
    excel_obj = upload_file_to_openai(current_file_path)
    if not excel_obj: return

    # Qui usiamo MODEL_NORMALIZER (gpt-4o) perché serve intelligenza
    final_result_path = run_assistant_task(
        "Normalizer", 
        excel_obj, 
        PROMPT_NORMALIZER, 
        model_name=MODEL_NORMALIZER,
        output_filename=OUTPUT_FILE
    )

    # --- CLEANUP ---
    print("\n🧹 Pulizia risorse temporanee...")
    client.files.delete(excel_obj.id)
    if is_temp_file and os.path.exists(current_file_path):
        os.remove(current_file_path)
        print(f"   Rimosso file temporaneo: {current_file_path}")

    if final_result_path:
        print(f"\n🏆 SUCCESSO! File pronto: {final_result_path}")
    else:
        print("\n❌ Pipeline fallita.")

if __name__ == "__main__":
    main_pipeline()