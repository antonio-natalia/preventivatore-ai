import argparse
import sys
import os
import logging
import sqlite3

# Setup Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Imports Infrastrutturali
from src.infrastructure.database import get_db_connection
from src.infrastructure.schema import init_domain_schema
from src.infrastructure.telemetry import setup_telemetry, track_phase, log_metric
from src.infrastructure.excel_writer import write_quote_dto_to_excel

# Imports Servizi
from src.services.ingestion_service import IngestionService
from src.services.quote_service import QuoteService
from src.services.digitization_service import DigitizationService
from src.interfaces.sonar_tui import SonarTUI
from src.interfaces.diagnostics import run_diagnostics

# Configurazione Logger Iniziale (sarà sovrascritta da setup_telemetry)
logger = logging.getLogger("preventivatore_ai")

def main():
    parser = argparse.ArgumentParser(description="Preventivatore AI - CLI Manager")
    
    # Argomento Globale per Tracing Distribuito
    parser.add_argument("--trace-id", help="ID di tracciamento per collegare processi distribuiti (es. da Power Automate)", default=None)
    
    subparsers = parser.add_subparsers(dest="command", help="Comando da eseguire")
    
    # --- COMMAND: INGEST ---
    ingest_parser = subparsers.add_parser("ingest", help="Importa file (XML/Excel)")
    ingest_parser.add_argument("file", help="Percorso del file o cartella")
    
    # --- COMMAND: QUOTE ---
    quote_parser = subparsers.add_parser("quote", help="Genera preventivo da JSON")
    quote_parser.add_argument("input_json", help="File JSON (output del digitizer)")
    quote_parser.add_argument("output", help="File Excel di output")
    quote_parser.add_argument("--solo-manodopera", action="store_true", help="Calcola solo ore uomo")
    
    # --- COMMAND: DIGITIZE ---
    digi_parser = subparsers.add_parser("digitize", help="Converte PDF/IMG in JSON")
    digi_parser.add_argument("--input", required=True, help="File input (PDF, PNG, JPG, XLS)")
    digi_parser.add_argument("--output", required=True, help="File output (JSON)")
    digi_parser.add_argument("--deep", action="store_true", help="Analisi profonda")

    # --- COMMANDS: UTILS ---
    subparsers.add_parser("sonar", help="Interfaccia TUI esplorativa")
    subparsers.add_parser("init-db", help="Inizializza Database vuoto")
    subparsers.add_parser("check", help="Diagnostica sistema")

    args = parser.parse_args()

    # 1. INIZIALIZZAZIONE TELEMETRIA (Prima di tutto)
    # Se args.trace_id è presente (passato da Power Automate), lo usiamo.
    # Altrimenti ne generiamo uno nuovo.
    setup_telemetry(args.trace_id)
    
    if not args.command:
        parser.print_help()
        return

    logger.info(f"CLI Command Started: {args.command}", extra={"command_args": vars(args)})

    try:
        if args.command == "ingest":
            conn = get_db_connection()
            # Import qui per evitare cicli se repository dipendesse da altro
            from src.infrastructure.repositories import CatalogRepository
            repo = CatalogRepository(conn) 
            repo = CatalogRepository(conn)
            service = IngestionService(repo)
            
            # Decoratore manuale o implicito nel service
            service.process_path(args.file)
            conn.close()

        elif args.command == "digitize":
            service = DigitizationService()
            success = service.process_document(args.input, args.output, args.deep)
            if not success:
                sys.exit(1)

        elif args.command == "quote":
            # Lazy import repository
            from src.infrastructure.repositories import CatalogRepository
            conn = get_db_connection()
            repo = CatalogRepository(conn)
            service = QuoteService(repo)
            
            # Load JSON input
            import json
            with open(args.input_json, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            result_dto = service.generate_quote(raw_data, args.solo_manodopera)
            write_quote_dto_to_excel(result_dto, args.output)
            conn.close()
            logger.info(f"Quote generated successfully: {args.output}")

        elif args.command == "sonar":
            conn = get_db_connection()
            init_domain_schema(conn)
            conn.close()
            app = SonarTUI()
            app.run()

        elif args.command == "init-db":
            conn = get_db_connection()
            init_domain_schema(conn)
            conn.close()
            logger.info("Database initialized successfully.")
        
        elif args.command == "check":
            run_diagnostics()

    except Exception as e:
        logger.critical(f"Unhandled Exception in CLI: {e}", exc_info=True)
        sys.exit(1)
    
    logger.info("CLI Command Completed Successfully")

if __name__ == "__main__":
    main()