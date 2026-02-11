import argparse
import sys
import os

# 1. Setup Path per trovare i moduli 'src'
# Aggiunge la root del progetto al PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 2. Imports Infrastrutturali
from src.infrastructure.database import get_db_connection
from src.infrastructure.repositories import RecipeRepository
from src.infrastructure.parsers import load_json_input
from src.infrastructure.excel_writer import write_quote_dto_to_excel 

# 3. Imports Servizi
from src.services.ingestion_service import IngestionService
from src.services.quote_service import QuoteService
from src.services.digitization_service import DigitizationService

def main():
    parser = argparse.ArgumentParser(description="Preventivatore AI - CLI Manager")
    subparsers = parser.add_subparsers(dest="command", help="Comando da eseguire")
    
    # --- COMMAND: INGEST (Listini) ---
    ingest_parser = subparsers.add_parser("ingest", help="Importa file Excel o XML")
    ingest_parser.add_argument("--file", required=True, help="Percorso del file")
    ingest_parser.add_argument("--type", choices=["excel", "xml", "auto"], default="auto", help="Tipo di file")
    ingest_parser.add_argument("--strategy", choices=["SMART_ADAPTIVE", "MAX", "LATEST"], default="SMART_ADAPTIVE", help="Strategia prezzi")

    # --- COMMAND: DIGITIZE (OCR/Vision) ---
    digitize_parser = subparsers.add_parser("digitize", help="Estrai dati da PDF/IMG e normalizza")
    digitize_parser.add_argument("--file", required=True, help="File input (PDF, IMG, XLS, CSV)")
    digitize_parser.add_argument("--deep-scan", action="store_true", help="Analisi semantica approfondita")
    digitize_parser.add_argument("--sample", type=int, default=0, help="Test su N righe")

    # --- COMMAND: QUOTE (Preventivi) ---
    quote_parser = subparsers.add_parser("quote", help="Genera preventivo da JSON normalizzato")
    quote_parser.add_argument("--file", required=True, help="File JSON (output di normalize_input)")
    quote_parser.add_argument("--output", default="preventivo_output.xlsx", help="File Excel output")
    quote_parser.add_argument("--solo-manodopera", action="store_true", help="Calcola solo costo installazione")

    args = parser.parse_args()

    # --- ROUTING DEI COMANDI ---

    if args.command == "ingest":
        if not os.path.exists(args.file):
            print(f"❌ File non trovato: {args.file}")
            return
        try:
            conn = get_db_connection()
            repo = RecipeRepository(conn)
            service = IngestionService(repo)
            
            service.process_file(
                file_path=args.file, 
                file_type=args.type, 
                pricing_mode=args.strategy
            )
            conn.close()
        except Exception as e:
            print(f"❌ Errore Ingestion: {e}")

    elif args.command == "digitize":
        if DigitizationService is None:
            print("❌ Errore: Modulo DigitizationService mancante.")
            return
            
        try:
            service = DigitizationService()
            service.process_document(
                input_file=args.file,
                deep_scan=args.deep_scan,
                sample_rows=args.sample
            )
        except Exception as e:
            print(f"❌ Errore Digitizer: {e}")
            import traceback
            traceback.print_exc()

    elif args.command == "quote":
        # 1. Caricamento Input (JSON Diretto)
        try:
            print(f"📂 Lettura input JSON: {args.file}")
            normalized_data = load_json_input(args.file)
        except Exception as e:
            print(f"❌ Errore lettura file: {e}")
            return

        try:
            conn = get_db_connection()
            repo = RecipeRepository(conn)
            service = QuoteService(repo)

            # 2. Esecuzione Service (Logica 1:1)
            result_dto = service.generate_quote(
                data_input=normalized_data, 
                solo_manodopera=args.solo_manodopera
            )

            # 3. Scrittura Output (Funzione Rinominata Correttamente)
            write_quote_dto_to_excel(result_dto, args.output)
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Errore Generazione Preventivo: {e}")
            import traceback
            traceback.print_exc()

    else:
        parser.print_help()

if __name__ == "__main__":
    main()