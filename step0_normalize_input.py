import os
import time
import pandas as pd
import warnings
from openai import OpenAI
from dotenv import load_dotenv

# Ignora i warning di deprecazione di OpenAI per pulizia log
warnings.filterwarnings("ignore", category=DeprecationWarning)

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- CONFIGURAZIONE ---
INPUT_FILE = "./richieste_ordine/LTE Impianti Srl Sacco Computo CF03.xls" 
OUTPUT_FILE = "./richieste_ordine/input_cliente_clean.xlsx"

def convert_to_modern_excel(filepath):
    """
    Se il file è .xls o .csv, lo converte in .xlsx standard 
    per renderlo leggibile al Code Interpreter di OpenAI.
    """
    filename, ext = os.path.splitext(filepath)
    ext = ext.lower()
    
    # Se è già xlsx, va bene così
    if ext == '.xlsx':
        return filepath, False

    print(f"   🔄 Conversione locale da {ext} a .xlsx...", end="")
    temp_file = filename + "_temp.xlsx"
    
    try:
        if ext == '.xls':
            # Richiede pip install xlrd
            df = pd.read_excel(filepath, header=None)
        elif ext == '.csv':
            df = pd.read_csv(filepath, header=None)
        else:
            return filepath, False
            
        # Salviamo in formato moderno
        df.to_excel(temp_file, index=False, header=False)
        print(f" Fatto ({temp_file})")
        return temp_file, True
        
    except Exception as e:
        print(f"\n❌ Errore conversione locale: {e}")
        print("   Assicurati di avere installato: pip install xlrd openpyxl")
        return filepath, False

def process_file_with_assistant():
    print(f"🚀 AVVIO ASSISTANT NORMALIZER V3 (Auto-Convert)")
    print(f"   File Input: {INPUT_FILE}")

    # 0. PRE-PROCESSING LOCALE
    # Convertiamo .xls -> .xlsx QUI, dove abbiamo il controllo delle librerie
    upload_path, is_temp = convert_to_modern_excel(INPUT_FILE)

    # 1. UPLOAD DEL FILE
    print(f"   ⬆️  Caricamento file su OpenAI ({os.path.basename(upload_path)})...", end="")
    try:
        file_obj = client.files.create(
            file=open(upload_path, "rb"),
            purpose='assistants'
        )
        print(f" Fatto ({file_obj.id})")
    except Exception as e:
        print(f"\n❌ Errore Upload API: {e}")
        if is_temp and os.path.exists(upload_path): os.remove(upload_path)
        return

    # 2. CREAZIONE ASSISTENTE
    instructions = """
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
    - METADATI: Info di posizione o dettagli non tecnici.

    ISTRUZIONI DI DIAGNOSI (PYTHON):
    Analizza la struttura del file. Identifica quale dei 3 pattern logici viene usato e applica la logica corrispondente:

    PATTERN A: STRUTTURA "PIATTA" (Riga Singola)
    - Riconoscimento: Ogni riga ha Codice, Descrizione e Quantità popolate.
    - Azione: Estrai i dati direttamente.

    PATTERN B: STRUTTURA "A MISURAZIONI" (Stesso Codice ripetuto)
    - Riconoscimento: Lo stesso Codice Articolo si ripete su più righe. Una di esse è solitamente la principale e contiene la descrizione con le specifiche tecniche, le altre possono avere delle misure(es. "lunghezza 5.00") oppure indicare un totale ("Sommano", "Totale").
    - Azione: Raggruppa per Codice. Descrizione = la descrizione della riga principale oppure l'unione delle descrizione se quelle secondarie contengono specifiche tecniche. Quantità = la riga che esprime il totale oppure somma delle parziali.

    PATTERN C: STRUTTURA "GERARCHICA / VARIANTI" (Padre-Figlio)
    - Riconoscimento:
    1. C'è una riga PADRE con Descrizione generica (es. "Cavo multipolare...") ma SENZA Quantità (o qta=0).
    2. Seguono righe FIGLIE con descrizioni brevi (es. "sez. 3x1.5", "sez. 4x2.5") e con Quantità > 0.
    3. Spesso i codici mostrano gerarchia (es. Padre "1.01", Figlio "1.01.a").
    - Azione (Ereditarietà):
    - Per ogni FIGLIO, crea una voce di computo. il codice è quello del figlio, la Descrizione Finale deve essere: "DESCRIZIONE PADRE + DESCRIZIONE FIGLIO".
    - Esempio Output: "Cavo multipolare... - sez. 3x1.5".
    - La Quantità e l'Unità di Misura sono quelle del Figlio.

    REGOLE GENERALI CRITICHE:
    1. **Unicità della Riga:** L'output deve avere una riga per ogni voce PREZZABILE (cioè con Quantità > 0).
    2. **Descrizione Tecnica:** Mantieni "Fornitura e posa". Rimuovi riferimenti puramente logistici (Piani, Stanze) spostandoli nei METADATI.
    3. **Pulizia Numeri:** Gestisci formato italiano (1.000,00 -> 1000.0).

    PROCEDURA OPERATIVA (PYTHON):
    1. Carica il dataframe.
    2. Identifica header e colonne.
    3. Identifica il pattern logico.
    4. Itera sulle righe mantenendo una variabile che indica il dataframe su cui si sta lavorando.
    - Se trovi una riga con codice indentico vuol dire che è lo stesso articolo -> capisci se la descrizione è rilevante o no e nel caso mergia la descrizione, capisci se si tratta di una riga totale o di misura e aggiorna le quantità.
    - Se trovi una riga con codice diverso -> capisci se sei in presenza di un figlio (codice che inizia con il padre, descrizione breve che aggiunge dettagli tecnici) o di un nuovo articolo.
    -- Se è un figlio -> capisce se la descrizione è autonoma o deve ereditare dal padre, prendi quantità e unità di misura del figlio.
    -- Se è un nuovo articolo -> inizia il nuovo dataframe.
    5. Genera il file output 'normalized_quote.xlsx' e rendilo disponibile per il download.
    """

    print("   🤖 Setup Assistente...", end="")
    assistant = client.beta.assistants.create(
        name="Normalizer Bot",
        instructions=instructions,
        model="gpt-4o",
        tools=[{"type": "code_interpreter"}]
    )
    print(" Fatto.")

    # 3. ESECUZIONE
    thread = client.beta.threads.create(
        messages=[
            {
                "role": "user",
                "content": "Esegui la normalizzazione sul file allegato. Crea il file Excel di output.",
                "attachments": [{"file_id": file_obj.id, "tools": [{"type": "code_interpreter"}]}]
            }
        ]
    )

    print("   🏃 Esecuzione remota...", end="")
    run = client.beta.threads.runs.create(thread_id=thread.id, assistant_id=assistant.id)

    # 4. POLLING
    while True:
        run_status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        
        if run_status.status == 'completed':
            print(" ✅")
            break
        elif run_status.status in ['failed', 'cancelled', 'expired']:
            print(f"\n❌ Errore AI: {run_status.last_error}")
            break
        
        print(".", end="", flush=True)
        time.sleep(1.5)

    # 5. DOWNLOAD
    print("   📥 Controllo risultati...")
    messages = client.beta.threads.messages.list(thread_id=thread.id)
    file_id_output = None
    
    for msg in messages.data:
        if msg.role == "assistant":
            # Controllo attachments
            if msg.attachments:
                file_id_output = msg.attachments[0].file_id
                break
            # Controllo annotazioni nel testo
            for content in msg.content:
                if content.type == 'text':
                    for annotation in content.text.annotations:
                        if annotation.type == 'file_path':
                            file_id_output = annotation.file_path.file_id
                            break
        if file_id_output: break

    if file_id_output:
        data = client.files.content(file_id_output)
        with open(OUTPUT_FILE, "wb") as f:
            f.write(data.read())
        print(f"   💾 SALVATO: {OUTPUT_FILE}")
    else:
        print("   ⚠️  Nessun file generato. Risposta dell'assistente:")
        for msg in messages.data:
            if msg.role == "assistant":
                print(f"   > {msg.content[0].text.value}")

    # 6. CLEANUP
    print("   🧹 Pulizia...", end="")
    try:
        client.files.delete(file_obj.id)
        client.beta.assistants.delete(assistant.id)
        if is_temp and os.path.exists(upload_path): os.remove(upload_path)
    except: pass
    print(" Fatto.")

if __name__ == "__main__":
    process_file_with_assistant()