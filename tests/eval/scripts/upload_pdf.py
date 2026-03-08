from openai import OpenAI
import os
import argparse
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def upload_pdf(pdf_path: str) -> str:
    """
    Carica un file PDF su OpenAI e restituisce il suo ID.
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"Il file PDF non è stato trovato: {pdf_path}")

    print(f"Caricamento del file {pdf_path} su OpenAI...")
    with open(pdf_path, "rb") as f:
        file_obj = client.files.create(
            file=f,
            purpose="user_data" # Il purpose "user_data" è appropriato per file usati in un task del modello.
        )
    print(f"File caricato. ID: {file_obj.id}")
    return file_obj.id

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carica un file PDF su OpenAI.")
    parser.add_argument("pdf_path", type=str, help="Percorso del file PDF da caricare.")
    args = parser.parse_args()

    try:
        file_id = upload_pdf(args.pdf_path)
        print(f"File ID generato: {file_id}")
    except Exception as e:
        print(f"Errore durante l'upload del PDF: {e}")
