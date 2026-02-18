# 03. Convenzioni del Codice

Questo documento descrive le regole e le convenzioni seguite nel codebase per garantire coerenza e manutenibilità.

## Analisi Statica

### Stile di Naming
-   **Variabili e Funzioni:** `snake_case` (es. `get_db_connection`).
-   **Classi:** `PascalCase` (es. `IngestionService`).
-   **Costanti:** `UPPER_SNAKE_CASE` (es. `VOLATILITY_INCREMENT`).
-   Lo stile segue le convenzioni standard di Python (PEP 8).

### Type Hints
-   I Type Hints sono utilizzati in modo estensivo nelle firme di funzioni e metodi, migliorando la leggibilità e permettendo l'analisi statica del codice.

### Gestione degli Errori
-   L'entry point principale (`src/interfaces/cli.py`) implementa un blocco `try...except Exception as e` globale.
-   Questo pattern garantisce che qualsiasi eccezione non gestita venga catturata, loggata con il suo traceback completo, e che il processo termini con un codice di uscita non nullo (`sys.exit(1)`).

### Logging e Telemetria
-   Il sistema di logging è centralizzato e configurato in `src/infrastructure/telemetry.py`.
-   **Logging Strutturato e Adattivo:** I log vengono emessi in formato JSON quando la variabile d'ambiente `APP_ENV` è impostata su `"CLOUD"`. In ambiente locale, viene utilizzato un formato testuale più leggibile per facilitare il debug.
-   **Trace ID:** Il sistema implementa un `trace_id` per correlare tutte le operazioni di una singola esecuzione. L'ID viene gestito in modo context-aware tramite `ContextVar` e può essere passato come argomento `--trace-id` alla CLI.
-   **Tracciamento delle Performance:** Il decoratore `@track_phase` viene utilizzato per misurare la durata di funzioni critiche, emettendo log `PHASE_START` e `PHASE_END` e una metrica di durata.

## Struttura delle Directory

La struttura della directory `src/` segue una logica basata sull'architettura a strati.

-   `src/interfaces/`
    -   **Scopo:** Punti di ingresso dell'applicazione (es. `cli.py`).
-   `src/services/`
    -   **Scopo:** Contiene la logica di business principale e l'orchestrazione dei flussi.
-   `src/core/`
    -   **Scopo:** Definisce le entità di business (`entities.py`), le regole e le astrazioni.
-   `src/infrastructure/`
    -   **Scopo:** Implementazioni concrete per l'interazione con sistemi esterni (database, API, file system).
-   `src/config.py`
    -   **Scopo:** Gestione centralizzata della configurazione tramite la classe `Settings` (`pydantic-settings`). Carica le variabili da un file `.env` e dall'ambiente.
