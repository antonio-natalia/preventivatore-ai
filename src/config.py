import os
from pydantic_settings import BaseSettings
from pydantic import SecretStr
from dotenv import load_dotenv, find_dotenv

# Carica .env se presente
load_dotenv(find_dotenv())

# --- MODIFICA STANDARD: DEFINIZIONE DEFAULT ---
# Calcoliamo il percorso di default (Locale) fuori dalla classe.
# Questo garantisce che se non viene passata una Env Var, usiamo questo.
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DB_PATH = os.path.join(_BASE_DIR, "db", "preventivatore_v4.db")

class Settings(BaseSettings):
    # --- PROJECT PATHS ---
    PROJECT_ROOT: str = _BASE_DIR
    DATA_FOLDER: str = os.path.join(PROJECT_ROOT, "data")
    
    # -------------------------------------------------------------------------
    # CONFIGURAZIONE IBRIDA (LOCALE vs CLOUD)
    # Pydantic cercherà automaticamente una variabile d'ambiente chiamata "DB_FILE".
    # 1. Su Cloud imposteremo DB_FILE="/mnt/data/preventivatore_v4.db" -> Pydantic userà quella.
    # 2. Su Local non imposteremo nulla -> Pydantic userà _DEFAULT_DB_PATH.
    # -------------------------------------------------------------------------
    DB_FILE: str = _DEFAULT_DB_PATH
    
    ORPHANS_FILE: str = os.path.join(PROJECT_ROOT, "data", "orphaned_components.csv")

    # --- API KEYS ---
    OPENAI_API_KEY: str

    # --- SHAREPOINT SETTINGS ---
    SHAREPOINT_TENANT_ID: str
    SHAREPOINT_CLIENT_ID: str
    SHAREPOINT_CLIENT_SECRET: SecretStr
    SHAREPOINT_SITE_NAME: str # es. "NomeDelSito"

    # --- AI VISION SETTINGS ---
    VISION_MODEL: str = "gpt-5-mini" # Modello per l'elaborazione Vision
    VISION_IMAGE_DPI: int = 600 # Risoluzione DPI per la conversione PDF -> immagine
    VISION_MASTER_CHUNK_SIZE: int = 5 # Numero di pagine per il chunk iniziale Master
    VISION_WORKER_CHUNK_SIZE: int = 5 # Numero di pagine per i chunk paralleli Worker

    # --- NORMALIZER SETTINGS ---
    NORMALIZER_AI_MODEL: str = "gpt-5-nano" # Modello per l'AI interna del Normalizer

    # --- AI JUDGE SETTINGS ---
    AI_JUDGE_MODEL: str = "gpt-4o" # Modello per l'AI Judge nel QuoteService (allineato con tests/eval/configs/eval_gpt_5_mini.yaml)

    # --- VECTOR SETTINGS ---
    VECTOR_BATCH_SIZE: int = 200

    # --- BUSINESS LOGIC THRESHOLDS (Preserved from bulk_ingestion.py) ---
    SIMILARITY_MERGE: float = 0.98
    SIMILARITY_JUDGE: float = 0.92
    DEVIATION_THRESHOLD: float = 0.20  # 20% shock
    STALENESS_DAYS: int = 180          # 6 mesi obsolescenza
    VOLATILITY_INCREMENT: float = 0.1  # Quanto aumenta il rischio su shock

    class Config:
        env_file = ".env"
        extra = "ignore" # Ignora variabili extra nel .env

# Istanza singleton
settings = Settings()
