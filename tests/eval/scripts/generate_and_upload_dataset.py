import fitz # PyMuPDF
import base64
from openai import OpenAI
import json
import os
import yaml
import argparse
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def load_config(config_path: str) -> dict:
    """Carica la configurazione dal file YAML."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def estrai_pagine_base64(percorso_pdf: str) -> list:
    """
    Legge un PDF e restituisce una lista di stringhe Base64, 
    una per ogni pagina del documento.
    """
    pagine_base64 = []
    
    try:
        doc = fitz.open(percorso_pdf)
        for pagina_num, pagina in enumerate(doc):
            # Renderizza la pagina come pixmap (immagine), aumentando la risoluzione
            pix = pagina.get_pixmap(matrix=fitz.Matrix(2, 2))
            
            # Converte il pixmap in bytes (formato JPEG)
            img_bytes = pix.tobytes("jpeg")
            
            # Codifica in Base64
            b64_string = base64.b64encode(img_bytes).decode('utf-8')
            pagine_base64.append(b64_string)
        doc.close()
    except Exception as e:
        print(f"Errore durante l'estrazione delle pagine Base64 dal PDF {percorso_pdf}: {e}")
        raise # Rilancia l'eccezione per interrompere il processo se l'estrazione fallisce

    return pagine_base64

def generate_dataset(pdf_ids: dict, config: dict) -> str:
    """
    Genera il file JSONL del dataset di valutazione con ground truth.
    :param pdf_ids: Dizionario con nomi file PDF (le chiavi sono i nomi file) e i loro ID OpenAI (non più usati direttamente per il content).
    :param config: Configurazione del test.
    :return: Percorso del file JSONL generato localmente.
    """
    output_dataset_path = config["GROUND_TRUTH_TEMPLATE_PATH"]
    target_model_prompt_path = config["TARGET_MODEL_PROMPT_PATH"]
    pdf_input_dir = config["PDF_INPUT_DIR"]
    ground_truths_dir = config["GROUND_TRUTHS_DIR"] # Recupera la nuova directory

    if not os.path.exists(target_model_prompt_path):
        raise FileNotFoundError(f"Prompt del modello target non trovato: {target_model_prompt_path}")

    with open(target_model_prompt_path, "r", encoding="utf-8") as f:
        target_model_prompt_content = f.read()
    
    dataset_entries = []

    for pdf_name, _ in pdf_ids.items():
        local_pdf_path = os.path.join(pdf_input_dir, pdf_name)
        if not os.path.exists(local_pdf_path):
            print(f"ATTENZIONE: PDF locale non trovato '{local_pdf_path}'. Saltando questo PDF.")
            continue

        # Costruisce il percorso del file di ground truth JSON
        # Assumiamo che il nome del file JSON sia lo stesso del PDF ma con estensione .json
        gt_file_name = os.path.splitext(pdf_name)[0] + ".json"
        local_gt_path = os.path.join(ground_truths_dir, gt_file_name)

        if not os.path.exists(local_gt_path):
            print(f"ATTENZIONE: Ground truth JSON non trovato '{local_gt_path}' per il PDF '{pdf_name}'. Saltando questo PDF.")
            continue

        # Carica la ground truth specifica per questo PDF
        with open(local_gt_path, "r", encoding="utf-8") as f:
            current_ideal_structure = json.load(f)

        lista_base64 = estrai_pagine_base64(local_pdf_path)
        
        content_array = [
            {
                "type": "text",
                "text": target_model_prompt_content
            }
        ]
        
        for b64_img in lista_base64:
            content_array.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{b64_img}"
                }
            })
            
        messages_payload = [
            {
                "role": "user",
                "content": content_array
            }
        ]
        entry = {
            "item": {
                "input": messages_payload,
                "ideal": current_ideal_structure # Usiamo la ground truth caricata dinamicamente
            }
        }
        dataset_entries.append(entry)

    with open(output_dataset_path, "w", encoding="utf-8") as f:
        for entry in dataset_entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    
    print(f"Dataset generato localmente: {output_dataset_path}")
    return output_dataset_path

def upload_dataset(dataset_path: str) -> str:
    """
    Carica il file JSONL del dataset di valutazione su OpenAI e restituisce il suo ID.
    """
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Il dataset file non è stato trovato: {dataset_path}")

    print(f"Caricamento del dataset {dataset_path} su OpenAI...")
    with open(dataset_path, "rb") as f:
        dataset_file = client.files.create(
            file=f,
            purpose="evals"
        )
    print(f"Dataset caricato. ID: {dataset_file.id}")
    return dataset_file.id

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Genera e carica il dataset di valutazione JSONL su OpenAI.")
    parser.add_argument("--config", type=str, default="tests/eval/configs/eval_gpt_5_mini.yaml",
                        help="Percorso al file di configurazione YAML.")
    parser.add_argument("--pdf-ids", type=str, dest="pdf_ids",
                        help="JSON stringa di nomi file PDF e i loro ID OpenAI (es. '{\"test.pdf\": \"file-abc\"}')")
    
    args = parser.parse_args()

    config = load_config(args.config)

    if args.pdf_ids:
        try:
            pdf_ids_dict = json.loads(args.pdf_ids)
        except json.JSONDecodeError:
            print("Errore: la stringa --pdf-ids non è un JSON valido.")
            exit(1)
    else:
        print("Errore: è necessario fornire --pdf-ids con un JSON valido di nomi file PDF e i loro ID.")
        exit(1)

    try:
        local_dataset_path = generate_dataset(pdf_ids_dict, config)
        dataset_file_id = upload_dataset(local_dataset_path)
        print(f"Dataset File ID generato: {dataset_file_id}")
    except Exception as e:
        print(f"Errore durante la generazione o l'upload del dataset: {e}")
