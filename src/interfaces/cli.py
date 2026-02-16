import argparse
import sys
import os
import logging
import sqlite3

# 1. Setup Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 2. Imports Infrastrutturali
from src.infrastructure.database import get_db_connection
from src.infrastructure.schema import init_domain_schema
from src.infrastructure.repositories import CatalogRepository
from src.infrastructure.parsers import load_json_input
from src.infrastructure.excel_writer import write_quote_dto_to_excel

# 3. Imports Servizi & Interfacce
from src.services.ingestion_service import IngestionService
from src.services.quote_service import QuoteService
from src.services.digitization_service import DigitizationService
from src.interfaces.sonar_tui import SonarTUI
from src.interfaces.diagnostics import run_diagnostics

# Configurazione Logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

def main():
    parser = argparse.ArgumentParser(description="Preventivatore AI - CLI Manager")
    subparsers = parser.add_subparsers(dest="command", help="Comando da eseguire")
    
    # --- COMMAND: INGEST ---
    ingest_parser = subparsers.add_parser("ingest", help="Importa file (XML/Excel) o intere cartelle")
    ingest_parser.add_argument("file", help="Percorso del file singolo o della cartella da processare")
    
    # --- COMMAND: QUOTE ---
    quote_parser = subparsers.add_parser("quote", help="Genera preventivo da JSON")
    quote_parser.add_argument("input", help="File JSON input")
    quote_parser.add_argument("output", help="File Excel output")
    quote_parser.add_argument("--solo-manodopera", action="store_true", help="Calcola solo ore lavoro")

    # --- COMMAND: SONAR ---
    subparsers.add_parser("sonar", help="Interfaccia TUI per esplorazione vettoriale")

    # --- COMMAND: INIT-DB ---
    subparsers.add_parser("init-db", help="Inizializza il Database vuoto")
    
    # --- COMMAND: CHECK ---
    subparsers.add_parser("check", help="Esegue diagnostica sistema")

    args = parser.parse_args()

    if args.command == "ingest":
        print(f"🚀 Avvio Ingestion... (File: {args.file})")
        conn = get_db_connection()
        
        # --- FIX: Inizializzazione Automatica Schema ---
        # Se il DB è nuovo/vuoto, crea le tabelle prima di procedere
        init_domain_schema(conn)
        # -----------------------------------------------
        
        repo = CatalogRepository(conn)
        service = IngestionService(repo)
        try:
            service.process_path(args.file)
        except KeyboardInterrupt:
            print("\n🛑 Ingestion interrotta dall'utente.")
        except Exception as e:
            # Per debugging approfondito se serve
            # import traceback
            # traceback.print_exc()
            print(f"❌ Errore Ingestion: {e}")
        finally:
            conn.close()

    elif args.command == "quote":
        print(f"💰 Calcolo Preventivo... (Input: {args.input})")
        try:
            raw_data = load_json_input(args.input)
            if not raw_data:
                print("❌ Errore: File input vuoto o non valido.")
                return

            conn = get_db_connection()
            
            # --- FIX: Inizializzazione Automatica Schema ---
            init_domain_schema(conn)
            # -----------------------------------------------
            
            repo = CatalogRepository(conn)
            service = QuoteService(repo)
            
            result_dto = service.generate_quote(raw_data, args.solo_manodopera)
            
            write_quote_dto_to_excel(result_dto, args.output)
            conn.close()
            print(f"✅ Preventivo generato: {args.output}")
        except Exception as e:
            print(f"❌ Errore Preventivo: {e}")
            import traceback
            traceback.print_exc()

    elif args.command == "sonar":
        try:
            # Nota: SonarTUI gestisce la connessione internamente, 
            # ma è buona norma assicurarsi che il DB esista.
            conn = get_db_connection()
            init_domain_schema(conn)
            conn.close()
            
            app = SonarTUI()
            app.run()
        except KeyboardInterrupt:
            print("\n👋 Chiusura Sonar.")
        except Exception as e:
            print(f"❌ Errore Sonar: {e}")

    elif args.command == "init-db":
        print("🚀 Avvio procedura di inizializzazione Database...")
        try:
            conn = get_db_connection()
            init_domain_schema(conn)
            conn.close()
            print("🏁 Procedura completata con successo.")
        except Exception as e:
            print(f"❌ Errore critico durante init-db: {e}")
            sys.exit(1)
    
    elif args.command == "check":
            try:
                run_diagnostics()
            except Exception as e:
                print(f"❌ Errore Diagnostica: {e}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()