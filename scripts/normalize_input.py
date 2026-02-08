import os
import sys
import json
import argparse
import time
import re
import warnings
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv, find_dotenv

# Filtra warning
warnings.filterwarnings("ignore")

# --- PATH SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Importa il Normalizzatore Decoupled
from normalizers.v3_semantic import SemanticNormalizerV3

dotenv_path = find_dotenv()
if not dotenv_path:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
else:
    load_dotenv(dotenv_path)
    PROJECT_ROOT = os.path.dirname(dotenv_path)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- CONFIGURAZIONE ---
WORK_DIR = os.path.join(PROJECT_ROOT, "richieste_ordine")
MODEL_DIGITIZER = "gpt-4o-mini" # Modello veloce per OCR/Trascrizione

# --- PROMPT DIGITIZER (RIPRISTINATO) ---
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

# --- HELPER FUNCTIONS: DIGITIZER AGENT ---

def upload_file_to_openai(filepath):
    print(f"📤 Uploading: {os.path.basename(filepath)}...")
    with open(filepath, "rb") as f:
        file_obj = client.files.create(file=f, purpose="assistants")
    return file_obj

def run_assistant_task(agent_name, file_obj, prompt, model_name, output_filename):
    """Esegue un task Assistant (Digitizer) con logica di Retry robusta"""
    print(f"🤖 [{agent_name}] Avvio Task con modello {model_name}...")
    
    assistant = client.beta.assistants.create(
        name=f"MEP_{agent_name}",
        instructions=prompt,
        model=model_name,
        tools=[{"type": "code_interpreter"}]
    )

    thread = client.beta.threads.create(
        messages=[{
            "role": "user",
            "content": "Esegui l'analisi sul file allegato e genera l'output richiesto.",
            "attachments": [{ "file_id": file_obj.id, "tools": [{"type": "code_interpreter"}] }]
        }]
    )

    max_retries = 3
    run_succeeded = False
    
    for attempt in range(max_retries + 1):
        if attempt > 0: print(f"🔄 [{agent_name}] Tentativo {attempt}/{max_retries}...")

        run = client.beta.threads.runs.create(thread_id=thread.id, assistant_id=assistant.id)
        print(f"⏳ [{agent_name}] Elaborazione...", end="", flush=True)
        
        while True:
            run_status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
            
            if run_status.status == 'completed':
                print(" Fatto!")
                run_succeeded = True
                break
            
            elif run_status.status in ['failed', 'cancelled', 'expired']:
                error_msg = run_status.last_error.message if run_status.last_error else "unknown"
                print(f"\n❌ [{agent_name}] Errore Run: {error_msg}")
                
                if 'rate_limit' in error_msg.lower() and attempt < max_retries:
                    match = re.search(r"try again in (\d+\.?\d*)s", error_msg)
                    wait = float(match.group(1)) + 5.0 if match else 20.0
                    print(f"🛑 Rate Limit. Attendo {wait:.1f}s...")
                    time.sleep(wait)
                    break 
                else:
                    client.beta.assistants.delete(assistant.id)
                    return None
            
            time.sleep(2)
            print(".", end="", flush=True)
        
        if run_succeeded: break
    
    if not run_succeeded: return None

    # Recupero Output
    messages = client.beta.threads.messages.list(thread_id=thread.id)
    last_msg = messages.data[0]
    file_id_output = None
    
    if last_msg.content:
        for cb in last_msg.content:
            if cb.type == 'text' and cb.text.annotations:
                for ann in cb.text.annotations:
                    if ann.type == 'file_path':
                        file_id_output = ann.file_path.file_id
                        break
    
    if not file_id_output:
        print(f"\n⚠️ [{agent_name}] Nessun file generato.")
        client.beta.assistants.delete(assistant.id)
        return None

    print(f"📥 [{agent_name}] Download output...")
    file_content = client.files.content(file_id_output)
    with open(output_filename, "wb") as f:
        f.write(file_content.read())
    
    print(f"💾 Salvato intermedio: {os.path.basename(output_filename)}")
    client.beta.assistants.delete(assistant.id)
    return output_filename

# --- HELPER: PREPARAZIONE INPUT LOCALE ---

def ensure_xlsx_format(input_path: str) -> str:
    """Gestisce conversioni locali per CSV/XLS legacy"""
    base, ext = os.path.splitext(input_path)
    ext = ext.lower()
    
    if ext == '.xlsx': return input_path
    
    output_path = f"{base}_converted.xlsx"
    print(f"🔄 [Orchestrator] Conversione locale {ext} -> xlsx...")
    
    try:
        if ext == '.csv':
            try: df = pd.read_csv(input_path, sep=None, engine='python', encoding='utf-8')
            except: df = pd.read_csv(input_path, sep=None, engine='python', encoding='latin1')
        elif ext == '.xls':
            try: df = pd.read_excel(input_path, engine='xlrd')
            except ImportError:
                print("❌ Manca 'xlrd'. Esegui 'pip install xlrd>=2.0.1'")
                return input_path
        else:
            return input_path # Passa oltre se non gestito qui (es. PDF)

        df.to_excel(output_path, index=False)
        return output_path
    except Exception as e:
        print(f"❌ Errore conversione locale: {e}")
        return input_path

# --- MAIN ORCHESTRATOR ---

def main():
    parser = argparse.ArgumentParser(description="AI MEP Normalizer Orchestrator")
    parser.add_argument("--file", type=str, required=True, help="Percorso del file da processare (PDF, CSV, XLS, XLSX, PNG, JPG)")
    parser.add_argument("--deep-scan", action="store_true", help="Abilita scansione completa contenuto (V3)")
    parser.add_argument("--sample-rows", type=int, default=50, help="Righe sampling Fast Peek (V3)")
    
    args = parser.parse_args()

    input_file = os.path.abspath(args.file)
    if not os.path.exists(input_file):
        print(f"❌ Errore: File non trovato: {input_file}")
        return

    base_name = os.path.splitext(os.path.basename(input_file))[0]
    ext = os.path.splitext(input_file)[1].lower()
    
    target_xlsx = input_file
    
    # --- FASE 1: PREPARAZIONE DATO (Pipeline Ibrida) ---
    
    # CASO A: PDF/IMMAGINI -> Digitizer Agent
    if ext in ['.pdf', '.png', '.jpg', '.jpeg']:
        print(f"\n📄 [Orchestrator] Rilevato formato non strutturato ({ext}). Avvio Digitizer...")
        temp_raw_excel = os.path.join(WORK_DIR, f"{base_name}_raw_extraction.xlsx")
        
        file_obj = upload_file_to_openai(input_file)
        res = run_assistant_task("Digitizer", file_obj, PROMPT_DIGITIZER, MODEL_DIGITIZER, temp_raw_excel)
        
        if not res:
            print("❌ Fase Digitizer fallita. Interruzione.")
            return
        target_xlsx = temp_raw_excel
        
    # CASO B: CSV/XLS Legacy -> Conversione Locale
    elif ext in ['.csv', '.xls']:
        target_xlsx = ensure_xlsx_format(input_file)

    # --- FASE 2: NORMALIZZAZIONE (Decoupled Strategy) ---
    
    final_json_output = os.path.join(WORK_DIR, f"{base_name}_clean.json")
    
    try:
        scan_mode = "deep_scan" if args.deep_scan else "fast_peek"

        normalizer = SemanticNormalizerV3()
        results = normalizer.normalize(
            target_xlsx, 
            scan_mode=scan_mode, 
            sample_rows=args.sample_rows
        )
    
    except Exception as e:
        print(f"❌ Errore durante la normalizzazione: {e}")
        import traceback
        traceback.print_exc()
        return

    # --- FASE 3: PERSISTENZA ---
    if results and len(results) > 0:
        print(f"\n💾 [Orchestrator] Salvataggio {len(results)} voci...")
        output_data = [v.model_dump() for v in results]
        
        with open(final_json_output, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
            
        print(f"✅ Output Finale: {os.path.basename(final_json_output)}")
    else:
        print("⚠️ Nessuna voce estratta.")

if __name__ == "__main__":
    main()