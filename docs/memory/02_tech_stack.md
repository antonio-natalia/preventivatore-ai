# 02. Stack Tecnologico e Architettura

Questo documento descrive i componenti tecnologici, il pattern architetturale e l'ambiente di runtime del sistema.

## Core Tecnologico
-   **Linguaggio:** Python 3.10 (come specificato nel `dockerfile`).
-   **Framework CLI:** `argparse` dalla libreria standard di Python. Non vengono usati framework esterni come Click o Typer (Vedi `src/interfaces/cli.py`).
-   **Librerie Chiave:**
    -   `pandas`: Utilizzata pesantemente per la manipolazione di dati tabellari durante l'analisi dei file Excel.
    -   **Modellazione Dati**: Vengono usate sia le `dataclasses` della libreria standard (per i DTO del preventivo in `src/core/entities.py`) sia `pydantic` (per la validazione della configurazione in `src/config.py` e per i modelli di normalizzazione).
    -   `openpyxl`: Per la lettura e scrittura di file Excel.

## Database
-   **Sistema:** SQLite (Vedi `sqlite3` import).
-   **Interazione:** L'applicazione non utilizza un ORM (Object-Relational Mapper). Le query sono scritte in SQL raw ed eseguite tramite il driver `sqlite3` (Vedi `src/infrastructure/repositories.py`).
-   **Schema:** Lo schema del database è definito programmaticamente in `src/infrastructure/schema.py`.
-   **Ricerca Semantica:** Lo schema include una tabella virtuale `vec_catalog_items` che utilizza l'estensione `vec0` di SQLite, confermando l'implementazione di una ricerca vettoriale per similarità semantica.
-   **Tabelle Principali:**
    -   `catalog_items`: Anagrafica centrale di tutti gli articoli. Contiene i costi calcolati, la strategia di prezzo e lo stato di integrità.
    -   `bill_of_materials`: Tabella di giunzione che definisce le relazioni padre-figlio e le quantità di impiego, formando il grafo per il calcolo dei costi.
    -   `cost_history_log`: Traccia uno storico di tutte le variazioni di prezzo per ogni articolo, garantendo l'auditabilità.
    -   `bom_integrity_errors`: Log degli "orfani", ovvero componenti referenziati in una distinta base ma non presenti in `catalog_items`.
    -   `bom_history_log`: Archivia le versioni precedenti delle distinte base quando vengono aggiornate.

## Architettura
-   **Pattern:** Il codice è strutturato secondo un' **Architettura a Strati (Layered Architecture)**.
    -   **Strato di Interfaccia (`src/interfaces/`):** Gestisce l'interazione con l'esterno (la CLI). È il punto di ingresso dell'applicazione.
    -   **Strato di Servizio (`src/services/`):** Contiene la logica di business e orchestra le operazioni.
    -   **Strato di Infrastruttura (`src/infrastructure/`):** Contiene implementazioni concrete per l'accesso a risorse esterne (database, file system, API).
    -   **Strato Core/Dominio (`src/core/`):** Definisce le entità di business e le astrazioni principali.
-   **Gestione Dipendenze:** La Dependency Inversion è applicata manualmente. Le dipendenze (es. `CatalogRepository`) vengono create nello strato di interfaccia (`cli.py`) e iniettate nei costruttori dei servizi.
-   **Tipo:** L'applicazione è un **Monolite**.

## Ambiente di Runtime
-   **Containerizzazione:** L'applicazione è progettata per essere eseguita all'interno di un container Docker (Vedi `dockerfile`).
-   **Immagine Base:** `python:3.10-slim`.
-   **Variabili d'Ambiente Chiave:**
    -   `APP_ENV`: Determina il formato dei log. Impostato su `"CLOUD"` abilita il logging JSON strutturato, altrimenti usa un formato testuale.
    -   `DB_FILE`: Percorso completo al file del database SQLite.
    -   `OPENAI_API_KEY`: Chiave API per i servizi OpenAI.
    -   `PYTHONDONTWRITEBYTECODE=1`: Evita la creazione di file `.pyc`.
    -   `PYTHONUNBUFFERED=1`: Forza lo stream di output/error a non avere buffer, essenziale per un logging corretto in ambienti containerizzati.
-   **Sicurezza:** Il container viene eseguito con un utente non-root (`appuser`).
-   **Esecuzione:** Il `dockerfile` non definisce un `ENTRYPOINT` o `CMD` fissi; il comando viene passato dinamicamente all'avvio del container.
