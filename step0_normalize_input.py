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
    Sei un Senior Quantity Surveyor.
    
    OBIETTIVO:
    Analizza il file Excel RDO caricato. Estrai le voci di computo e genera un NUOVO file Excel pulito.
    
    COLONNE OUTPUT RICHIESTE:
    - CODICE (Testo): identificativo articolo, se presente. 
    - DESCRIZIONE (Testo): Deve contenere tutti i requisiti tecnici ed i dettagli necessari per l'individuazione della giusta voce di prezzo.
    --esempio 1: "Cavo multipolare flessibile resistente al fuoco, non propagante l'incendio, senza alogeni, conforme ai requisiti previsti dalla Normativa Europea Regolamento UE 305/2011 - Prodotti ... UNEL 35024/ 1, CEI UNEL 35026, UNI EN 13501-6; sigla di designazione FTG18(0)M16, tensione nominale 0,6/1 kV:- 7Gx1,5 mm²"
    --esempio 2: "Fornitura in opera di Quadro Elettrico di reparto superficie media con degenze e/o ambulatori" 
    - QUANTITA (Numero): identifica la quantità richiesta per ogni voce.
    - UNITA_DI_MISURA (Testo): ad esempio "m", "mq", "cad", "kg", "lt", "h", ecc.
    - METADATI (Testo): Info di ulteriori, ad esempio riguardo la posizione (Piano, Scala, ecc.) o la tipologia (Materiale, Lavorazione, Quadri, Emergenze, ecc.), se presenti.

    CONTENUTO OUTPUT RICHIESTO:
    - Una riga per ogni voce di computo, con descrizione completa, quantità ed unità di misura associate.
    - La descrizione deve:
        - essere completa e dettagliata,
        - contenere tutti i requisiti tecnici e le specifiche necessarie in modo da permettere l'individuazione della giusta voce di prezzo al preventivatore AI basato su ricerca vettoriale
        - escludere la quantità richiesta per l'articolo, non necessaria all'individuazione della voce di prezzo nel database.
    
    INDICAZIONI DI CONTESTO SUGLI RDO IN INPUT:
    - Generalmente contengono un codice articolo, alfanumerico, utile a capire dove inizia e finisce ogni voce.
    -- Se il codice è ripetuto in più righe, queste fanno parte della stessa voce di computo e probabilmente contengono una voce principale con descrizione tecnica estesa e delle sottovoci che possono non aggiungere informazioni rilevanti e si possono ignorare, a meno che non si tratti dell'indicazione della quantità richiesta per la voce. In tal caso il dato non va tenuto nella descrizione ma nell'apposito colonna di output. stesso discorso per l'unità di misura.
    -- Se esiste una gerarchia di codici (es. 1, 1.1, 1.2, 2, 2.1, ecc.) generalmente le righe con codice principale (1, 2) contengono la descrizione tecnica comune, mentre le sottovoci (1.1, 1.2, 2.1) contengono dettagli tecnici aggiuntivi. in tal caso va creata una singola voce di computo con la descrizione completa (unendo le righe) per ogni sottovoce con la rispettiva quantità ed unità di misura.
    
    ISTRUZIONI TECNICHE:
    1. Usa Python/Pandas per leggere il file.
    2. Il file è un Excel senza intestazioni standard. Cerca la riga che contiene "Descrizione" o "Designazione" per capire l'header.
    3. Pulisci i numeri (gestisci formattazione italiana 1.000,00).
    4. Genera il file output 'normalized_quote.xlsx' e rendilo disponibile per il download.
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