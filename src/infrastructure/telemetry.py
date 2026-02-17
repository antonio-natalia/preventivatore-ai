import os
import time
import uuid
import logging
import functools
from contextvars import ContextVar
from typing import Optional, Dict, Any
from pythonjsonlogger import jsonlogger

# --- CONTESTO DI TRACCIAMENTO ---
# Mantiene il Trace ID corrente in modo thread-safe/async-safe
_trace_context: ContextVar[str] = ContextVar("trace_id", default="UNKNOWN")

def get_current_trace_id() -> str:
    return _trace_context.get()

def set_trace_id(trace_id: str):
    _trace_context.set(trace_id)

# --- LOGGING CONFIGURATION ---
class ContextAwareJsonFormatter(jsonlogger.JsonFormatter):
    """
    Arricchisce ogni log con il Trace ID corrente automaticamente.
    """
    def add_fields(self, log_record, record, message_dict):
        super(ContextAwareJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['trace_id'] = get_current_trace_id()
        log_record['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

def setup_telemetry(trace_id: Optional[str] = None) -> logging.Logger:
    """
    Inizializza il sistema di logging e imposta il Trace ID globale.
    """
    # 1. Gestione ID
    if not trace_id:
        trace_id = str(uuid.uuid4())
    set_trace_id(trace_id)

    # 2. Configurazione Logger
    logger = logging.getLogger("preventivatore_ai")
    logger.setLevel(logging.INFO)
    
    # Pulizia handler precedenti (per evitare log doppi nei test/rilanci)
    if logger.handlers:
        logger.handlers.clear()

    handler = logging.StreamHandler()
    
    # Switch Formattazione in base all'ambiente
    env = os.getenv("APP_ENV", "LOCAL").upper()
    
    if env == "CLOUD":
        # Formato JSON per Azure Log Analytics
        formatter = ContextAwareJsonFormatter(
            '%(timestamp)s %(levelname)s %(name)s %(message)s'
        )
    else:
        # Formato Testo per Sviluppo Locale (più leggibile)
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [TRACE:%(trace_id)s] %(message)s',
            datefmt='%H:%M:%S'
        )
        # Hack per iniettare trace_id nel LogRecord standard
        old_factory = logging.getLogRecordFactory()
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            record.trace_id = get_current_trace_id()
            return record
        logging.setLogRecordFactory(record_factory)

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False  # Evita propagazione al root logger (librerie esterne)

    return logger

# --- METRICS & DECORATORS ---

def log_metric(name: str, value: float, tags: Dict[str, str] = None):
    """
    Emette un log strutturato specifico per essere ingerito come metrica.
    In futuro qui collegheremo OpenTelemetry Meter.
    """
    logger = logging.getLogger("preventivatore_ai.metrics")
    payload = {
        "event": "METRIC",
        "metric_name": name,
        "value": value,
        "tags": tags or {}
    }
    logger.info("Metric Recorded", extra=payload)

def track_phase(phase_name: str):
    """
    Decoratore per misurare la durata di una funzione (Fase).
    Emette log di Start/End e la metrica di durata.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger("preventivatore_ai.tracer")
            start_time = time.time()
            
            # Log Inizio
            logger.info(f"PHASE_START: {phase_name}", extra={
                "event": "PHASE_START", 
                "phase": phase_name
            })
            
            try:
                result = func(*args, **kwargs)
                status = "SUCCESS"
                return result
            except Exception as e:
                status = "ERROR"
                raise e
            finally:
                duration = time.time() - start_time
                
                # Log Fine con Durata
                logger.info(f"PHASE_END: {phase_name}", extra={
                    "event": "PHASE_END", 
                    "phase": phase_name,
                    "duration_seconds": round(duration, 3),
                    "status": status
                })
                
                # Registrazione Metrica
                log_metric("phase_duration_seconds", duration, {"phase": phase_name, "status": status})
        
        return wrapper
    return decorator