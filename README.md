# AI Preventivatore MEP
Questo repository ospita il motore di preventivazione intelligente per impianti MEP (Meccanici, Elettrici, Idraulici).
Il sistema utilizza l'Intelligenza Artificiale (OpenAI GPT-4o + Vector Search) per digitalizzare richieste, trovare corrispondenze nel listino prezzi e generare preventivi dettagliati.

---

## 🏗 Architettura Software

Il progetto adotta il pattern architetturale **Modular Monolith**.
Questo approccio garantisce una separazione netta delle responsabilità, mantenendo tutto il codice in un unico repository distribuibile.

### I Layer del Sistema

Il codice è organizzato in 4 layer concentrici (da esterno a interno):

**1. Interfaces (`src/interfaces`)**
Punto di ingresso verso il mondo esterno. Attualmente ospita la CLI (`cli.py`).
* *Responsabilità:* Parsing degli argomenti, avvio dei Servizi.

**2. Services (`src/services`)**
Orchestratori della logica di business ("Application Layer"). Coordinano il flusso dei dati tra Infrastructure e Core.
* *Esempi:* `QuoteService`, `IngestionService`, `DigitizationService`.
* *Responsabilità:* Eseguire un caso d'uso (es. "Genera Preventivo", "Ingerisci Listino").

**3. Core (`src/core`)**
Il cuore del dominio ("Domain Layer"). Contiene la logica pura e le strutture dati. Non ha dipendenze esterne (DB, API).
* *Esempi:* `QuoteLineItem` (DTO), Logica di Normalizzazione Semantica.
* *Responsabilità:* Definire *cosa* è un preventivo e le regole di validazione.

**4. Infrastructure (`src/infrastructure`)**
Implementazione tecnica dei dettagli ("Adapter Layer").
* *Esempi:* `RecipeRepository` (SQLite), `OpenAIClient`, `ExcelWriter`, `VisionClient`.
* *Responsabilità:* Parlare con il Database, scrivere file Excel, chiamare API esterne.

---

## 🧠 Funzionalità Intelligenti (Core Features)

### 1. Smart Pricing & Adaptive Learning
Il sistema non si limita a copiare i prezzi di listino, ma calcola il prezzo "giusto" basandosi sulla storia.
* **Logica:** `Weighted Average` (Media Ponderata) tra Prezzo Storico e Prezzo Nuovo.
* **Shock Detection:** Se il nuovo prezzo devia >20% (`DEVIATION_THRESHOLD`), il sistema segnala un'anomalia (Shock) e aumenta l'indice di volatilità.
* **Staleness Check:** Se un prezzo nel DB è più vecchio di 6 mesi (`STALENESS_DAYS`), il nuovo dato ha priorità massima (peso 90%).

### 2. Hybrid Search (Vector + AI Validation)
La ricerca dei componenti non è una semplice query SQL `LIKE %...%`.
1.  **Vettorizzazione:** Ogni descrizione viene trasformata in un vettore a 1536 dimensioni (`text-embedding-3-small`).
2.  **Ricerca Semantica:** `sqlite-vec` trova i candidati più vicini per significato (es. "Interruttore" ≈ "Commutatore").
3.  **AI Judge (GPT-4o):** Se la similarità è nella "zona grigia" (0.85 - 0.96), un modello LLM analizza tecnicamente se i due articoli sono intercambiabili.

### 3. Vision Digitizer
Trasforma file non strutturati (PDF scansionati, Immagini) in dati strutturati.
* **Tecnologia:** OpenAI GPT-4o Vision + Code Interpreter.
* **Processo:** L'AI "guarda" il PDF, estrae le tabelle visivamente e scrive un file Excel grezzo, che poi viene normalizzato semanticamente.

---

## 📂 Struttura del Progetto

    /preventivatore-ai
    │
    ├── /data                   # Cartella dati (Input temporanei, Listini)
    ├── /db                     # Database SQLite (preventivatore_v3_smart.db)
    ├── /richieste_ordine       # Output del processo di digitalizzazione (JSON)
    │
    ├── /src                    # CODICE SORGENTE
    │   ├── config.py           # Gestione Variabili d'Ambiente (Settings)
    │   │
    │   ├── /core               # DOMAIN LAYER (Logica Pura & DTO)
    │   │   ├── entities.py     # Definizioni Dati (QuoteLineItem, QuoteResult)
    │   │   └── /normalizers    # Logica semantica per pulizia dati
    │   │
    │   ├── /infrastructure     # INFRA LAYER (Tecnologia)
    │   │   ├── database.py     # Connessione DB & sqlite_vec
    │   │   ├── repositories.py # Query SQL incapsulate
    │   │   ├── ai_client.py    # Wrapper OpenAI (Chat & Embedding)
    │   │   ├── vision_client.py# Wrapper GPT-4o Vision (OCR)
    │   │   ├── excel_writer.py # Generazione Excel Output
    │   │   └── parsers.py      # Lettura input (JSON, Excel, XML)
    │   │
    │   ├── /services           # APPLICATION LAYER (Orchestratori)
    │   │   ├── ingestion_service.py    # Caricamento listini nel DB
    │   │   ├── digitization_service.py # PDF -> JSON
    │   │   └── quote_service.py        # JSON -> Logica Matching -> DTO
    │   │
    │   └── /interfaces         # ENTRY POINTS
    │       └── cli.py          # Command Line Interface unificata
    │
    ├── .env                    # Chiavi API e Configurazione locale
    └── requirements.txt        # Dipendenze Python

---

## 🚀 Guida Operativa (CLI)

Tutte le operazioni passano attraverso un unico punto di ingresso: `src/interfaces/cli.py`.

### 1. Ingestion Listini (Aggiornamento Database)
Carica nuovi listini (Excel o XML SIX/STR) nel database vettoriale.

    # Per file XML (SIX/STR)
    python src/interfaces/cli.py ingest --file data/listino_2024.xml --type xml

    # Per file Excel standard
    python src/interfaces/cli.py ingest --file data/listino_privato.xlsx --type excel

### 2. Digitalizzazione Input (PDF -> JSON)
Trasforma una Richiesta di Offerta (PDF, Immagine o Excel grezzo) in un formato JSON strutturato e normalizzato.

    # Estrazione da PDF (OCR + Normalizzazione)
    python src/interfaces/cli.py digitize --file data/richiesta_cliente.pdf --deep-scan

*Output:* Genera un file JSON in `/richieste_ordine/richiesta_cliente_clean.json`.

### 3. Generazione Preventivo (JSON -> Excel)
Il cuore del sistema. Prende il JSON generato al passo 2, cerca i match nel DB, valida con l'AI e genera l'Excel finale.

    # Generazione Standard
    python src/interfaces/cli.py quote --file richieste_ordine/richiesta_cliente_clean.json --output preventivi/offerta_finale.xlsx

    # Generazione "Solo Manodopera" (Modalità Installazione)
    python src/interfaces/cli.py quote --file richieste_ordine/richiesta_clean.json --output preventivi/offerta_manodopera.xlsx --solo-manodopera

---

## 🛠 Guida allo Sviluppo

### Regole d'Oro per la Manutenzione

1.  **No Logic in IO:** Il `Service` non deve mai scrivere su disco o stampare a video direttamente (tranne log). Restituisce DTO. L'`Infrastructure` si occupa di salvare.
2.  **No SQL in Service:** Il `Service` non deve mai contenere stringhe SQL. Chiama i metodi di `RecipeRepository`.

### Setup Ambiente di Sviluppo

**1. Virtual Env:**

    python -m venv venv
    # Windows:
    venv\Scripts\activate
    # Mac/Linux:
    source venv/bin/activate

**2. Dipendenze:**

    pip install -r requirements.txt

**3. Variabili d'Ambiente:**
Crea un file `.env` nella root:

    OPENAI_API_KEY=sk-proj-....
    DB_FILE=db/preventivatore_v3_smart.db