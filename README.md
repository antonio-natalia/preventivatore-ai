# AI Preventivatore MEP
Questo repository ospita il motore di preventivazione intelligente per impianti MEP (Meccanici, Elettrici, Idraulici).
Il sistema è stato evoluto da un modello statistico a un **Modello Ingegneristico Deterministico**, basato su Distinta Base (BOM) Relazionale. Utilizza l'Intelligenza Artificiale (OpenAI GPT-4o + Vector Search) per la ricerca semantica e la digitalizzazione, ma si affida a calcoli matematici rigorosi per la determinazione dei costi.

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
* *Esempi:* `IngestionService` (Motore di Calcolo), `DigitizationService`.
* *Responsabilità:* Eseguire un caso d'uso (es. "Ricalcola Costi", "Ingerisci Listino").

**3. Core (`src/core`)**
Il cuore del dominio ("Domain Layer"). Contiene la logica pura e le regole di validazione business.
* *Responsabilità:* Definire le regole di integrità del grafo BOM e le strategie di pricing.

**4. Infrastructure (`src/infrastructure`)**
Implementazione tecnica dei dettagli ("Adapter Layer").
* *Esempi:* `CatalogRepository` (SQLite Graph), `OpenAIClient`, `ExcelWriter`, `Parsers`.
* *Responsabilità:* Parlare con il Database, scrivere file Excel, chiamare API esterne, parsing XML/Excel.

---

## 🧠 Funzionalità Core (Motore Deterministico)

### 1. Motore di Calcolo Bottom-Up
Il sistema abbandona le stime probabilistiche per un approccio rigoroso basato sulla struttura del prodotto.
La regola fondamentale è: **"Il prezzo di un Padre è ESCLUSIVAMENTE la somma matematica dei prezzi dei suoi Figli"**.

* **Identificazione Univoca (SKU):** Ogni articolo è identificato da uno SKU (Stock Keeping Unit) univoco. Non sono ammessi duplicati.
* **Strategia di Pricing (BUY vs MAKE):** Ogni articolo ha un attributo `pricing_strategy` che pilota l'algoritmo:
    * **`USE_DECLARED_PRICE` (BUY):** Risorse elementari (Foglie). Il prezzo viene letto direttamente dal listino fornitore o fattura.
    * **`SUM_CHILDREN` (MAKE):** Articoli composti (Nodi). Il prezzo del file viene ignorato. Il costo è calcolato sommando `(Costo Figlio * Quantità)` ricorsivamente.
* **Propagazione:** Una variazione di prezzo su una materia prima (es. Rame) innesca un ricalcolo a catena su tutti i semilavorati e prodotti finiti che la contengono.

### 2. Hybrid Search (Vector + AI Validation)
La ricerca dei componenti per la preventivazione utilizza tecniche avanzate di NLP.
1.  **Vettorizzazione:** Ogni descrizione valida viene trasformata in un vettore a 1536 dimensioni (`text-embedding-3-small`).
2.  **Ricerca Semantica:** `sqlite-vec` trova i candidati più vicini per significato.
3.  **AI Judge (GPT-4o):** Se la similarità è ambigua, un LLM valuta l'intercambiabilità tecnica.
4.  **Strict Validation:** Durante l'ingestion, articoli privi di descrizione vengono scartati alla fonte per mantenere alta la qualità del database vettoriale.

### 3. Vision Digitizer
Trasforma file non strutturati (PDF scansionati, Immagini) in dati strutturati.
* **Tecnologia:** OpenAI GPT-4o Vision + Code Interpreter.
* **Processo:** L'AI estrae le tabelle visivamente e normalizza i dati per il matching con il catalogo.

---

## 🗄 Architettura dei Dati (Schema Domain-Driven)

Il database è stato rifattorizzato da una tabella piatta a un **Grafo Relazionale** per supportare la logica BOM.

**TABELLA: `catalog_items` (Master Data)**
Rappresenta i Nodi e le Foglie del grafo.
* `sku` (PK): Codice univoco di business.
* `pricing_strategy`: Il "semaforo" (`USE_DECLARED_PRICE` o `SUM_CHILDREN`).
* `cost_integrity_status`: Stato di validità del calcolo.
    * `VALID`: Prezzo allineato.
    * `DIRTY`: In attesa di ricalcolo (dipendenza modificata).
    * `BROKEN`: Calcolo impossibile (componenti mancanti).
* `current_material_cost` / `current_labor_cost`: Costi separati per natura.

**TABELLA: `bill_of_materials` (Topologia)**
Rappresenta gli Archi del grafo.
* `parent_sku`: Chi viene assemblato.
* `child_sku`: Chi viene usato.
* `usage_quantity`: Coefficiente tecnico.
* *Nota:* Non contiene prezzi, solo relazioni strutturali.

**TABELLA: `cost_history_log` (Audit Trail)**
Traccia l'evoluzione temporale dei costi.
* Registra se una variazione è dovuta a un nuovo listino (`IMPORT_UPDATE`) o a un ricalcolo automatico (`CALCULATION_CHANGE`).

**TABELLA: `bom_integrity_errors` (Error Log)**
Registra gli "Orfani": casi in cui un padre richiede uno SKU che non esiste nel database.

---

## ⚙️ Workflow di Ingestion (3 Fasi)

Il caricamento di un nuovo listino (es. XML SIX/STR) non è lineare ma segue un processo a tre stadi per garantire la consistenza matematica.

**FASE 1: STAGING (Upsert Anagrafiche)**
* Il sistema carica tutti gli articoli dal file.
* Filtra gli articoli senza descrizione (Quality Gate).
* Genera/Aggiorna gli Embeddings vettoriali.
* Se l'articolo è una Foglia (`USE_DECLARED`), aggiorna il prezzo.
* Imposta lo stato di tutti gli item toccati a `DIRTY`.

**FASE 2: WIRING (Cablaggio Struttura)**
* Estrae la topologia (Analisi) dal file.
* Cancella i vecchi legami per i padri coinvolti.
* Scrive i nuovi legami nella tabella `bill_of_materials`.
* Rileva errori di integrità: se un figlio manca, il padre viene marcato `BROKEN` e l'errore registrato in `bom_integrity_errors`.

**FASE 3: COST ROLLUP (Calcolo Iterativo)**
* Il motore identifica i nodi `DIRTY` che hanno tutte le dipendenze `VALID`.
* Esegue il calcolo: `Totale = Somma(Prezzo_Figlio * Quantità)`.
* Aggiorna il padre e lo promuove a `VALID`.
* Ripete il ciclo risalendo l'albero (Bottom-Up) fino ai prodotti finiti.
* Storicizza ogni variazione di prezzo.

---

## 📂 Struttura del Progetto

    /preventivatore-ai
    │
    ├── /data                   # Input temporanei
    ├── /db                     # Database SQLite (preventivatore_v3_smart.db)
    │
    ├── /src                    # CODICE SORGENTE
    │   ├── config.py           # Settings
    │   │
    │   ├── /core               # DOMAIN LAYER
    │   │   └── entities.py     # Definizioni Dati
    │   │
    │   ├── /infrastructure     # INFRA LAYER
    │   │   ├── schema.py       # DDL Database (Schema aggiornato)
    │   │   ├── repositories.py # Query Graph & Rollup Logic
    │   │   ├── parsers.py      # XML Topology Extraction
    │   │   └── ai_client.py    # OpenAI Client
    │   │
    │   ├── /services           # APPLICATION LAYER
    │   │   ├── ingestion_service.py    # Orchestratore Ingestion 3-Fasi
    │   │   └── digitization_service.py # PDF -> JSON
    │   │
    │   └── /interfaces         # ENTRY POINTS
    │       └── cli.py          # CLI Unificata
    │
    └── requirements.txt        # Dipendenze

---

## 🚀 Guida Operativa (CLI)

### 1. Ingestion Listini - Supporto Multi-Formato (XML/Excel)
Il sistema di Ingestion è in grado di normalizzare input eterogenei mantenendo la struttura gerarchica:
* **XML (SIX/STR):** Supporto nativo per lo standard di interscambio edilizio. Estrae codici, descrizioni estese e l'intera struttura BOM nidificata.
* **Excel Gerarchico (.xlsx):** Supporto avanzato per file di computo (ex formato legacy). Riconosce automaticamente la struttura "Padre -> Componenti" basandosi sulla posizione delle righe e delle colonne (Articolo, Descrizione, P_COMP, Q_COMP).
    * *SKU Sintetici:* Per i componenti Excel privi di codice, il sistema genera automaticamente un ID univoco (Hash MD5) per permetterne il riutilizzo e la tracciabilità nel database.

### 2. Digitalizzazione Input
Trasforma PDF/Immagini in JSON strutturato.

    python src/interfaces/cli.py digitize --file data/richiesta.pdf --deep-scan

### 3. Generazione Preventivo
Crea il file Excel finale basandosi sui costi calcolati nel DB.

    python src/interfaces/cli.py quote --file richieste/clean.json --output preventivi/offerta.xlsx

---

## 🛠 Gestione Errori e Conflitti

* **Orfani:** Se un componente richiesto non esiste nel DB o nel file corrente, il padre viene marcato come `BROKEN`. Il sistema non "inventa" prezzi per riempire i buchi.
* **Descrizioni Mancanti:** Gli item senza descrizione nel listino sorgente vengono scartati silenziosamente in fase di Staging (con conteggio nel report finale) per evitare errori nelle API AI.
* **Cicli Infiniti:** Il calcolo a livelli (dependency resolution) previene loop infiniti (A contiene B contiene A), interrompendo il calcolo per quei rami.