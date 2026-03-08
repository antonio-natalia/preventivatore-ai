# Preventivatore AI - Motore di Costificazione Deterministico (MEP)

Il **Preventivatore AI** è un sistema enterprise per la generazione di preventivi nel settore Impiantistico (MEP: Mechanical, Electrical, Plumbing).

A differenza dei "Chatbot" generici che allucinano i prezzi, questo sistema è un **Motore Ibrido**:
1.  **AI (Probabilistica):** Usa LLM e Vettori solo per *capire* la richiesta e *trovare* l'articolo giusto nel catalogo.
2.  **Grafo (Deterministico):** Calcola il prezzo usando rigorose formule matematiche su Distinte Base (BOM) reali, garantendo precisione al centesimo.

---

## 🏗️ Architettura & Tecnologie (Per Sviluppatori)

Il progetto segue i principi della **Clean Architecture** e del **Domain-Driven Design (DDD)**.

### Tech Stack
* **Linguaggio:** Python 3.10+
* **Database:** SQLite 3 (scelto per portabilità e velocità in-process).
* **Vector Engine:** `sqlite-vec` (estensione per ricerca vettoriale locale ad alte prestazioni).
* **AI Models:** OpenAI `text-embedding-3-small` (Embedding), `gpt-5.1` (Vision/Reasoning per Digitizer e AI Judge), `gpt-5-nano` (Ragionamento per Normalizer fallback).
* **Data Processing:** Pandas (ETL Excel), Pydantic (Validazione Dati).
* **Interface:** CLI (Command Line Interface) nativa.

### Osservabilità & Telemetria (Cloud Native)
Il sistema implementa uno stack di telemetria pronto per Azure Container Apps:
* **Structured Logging (JSON):** Se `APP_ENV=CLOUD`, i log vengono emessi in JSON con campi contestuali (`trace_id`, `timestamp`, `level`) per l'ingestion automatica in Azure Log Analytics.
* **Distributed Tracing:** Ogni esecuzione CLI accetta un `--trace-id`. Questo permette di correlare processi disgiunti (es. Digitization -> Quotation) orchestrati da Power Automate come un'unica transazione di business.
* **Metrics:** Decoratori Python (`@track_phase`) misurano automaticamente la durata delle fasi critiche e il consumo di token OpenAI, esponendo metriche chiave per i KPI.

### Struttura del Progetto
* `src/core`: Entità di dominio e interfacce astratte.
* `src/infrastructure`: Implementazioni concrete (SQLite, OpenAI, Excel Parsers, Telemetry).
* `src/services`: Logica applicativa (Orchestrazione flussi).
* `src/interfaces`: Punti di ingresso (CLI, TUI Sonar).

---

## 📘 Funzionalità & Flussi Logici (Per Utilizzatori)

Il sistema offre 4 strumenti principali. Ecco come funzionano "sotto il cofano".

### 1. Ingestion Engine (`ingest`)
*Importa i listini e crea il "Cervello" del sistema.*

**Input:** File Excel (formato TeamSystem/Export) o XML (SIX).
**Il Processo:**
1.  **Parsing Intelligente:** Il sistema normalizza le colonne (es. gestisce sia `Q_COMP.` che `QCOMP`, formatta numeri italiani `1.000,00` e inglesi `1000.00`).
2.  **Separazione Ruoli:**
    * Se un articolo ha dei componenti figli, viene marcato come **NODO** (`SUM_CHILDREN`). Il suo prezzo di listino viene ignorato e ricalcolato dai figli.
    * Se un articolo è semplice (es. Vite, Operaio), è una **FOGLIA** (`USE_DECLARED_PRICE`).
3.  **Deduplicazione Logica:** Se il file Excel contiene lo stesso articolo definito più volte, il sistema tiene solo l'ultima definizione letta (logica "Listino Aggiornato").
4.  **BOM Wiring:** Collega automaticamente Padri e Figli nel database relazionale.

### 2. Digitizer & Normalizer (`digitize`)
*Trasforma la carta in dati e la interpreta semanticamente.*

**Input:** PDF (anche scansionati), Immagini (PNG/JPG) di computi metrici o file Excel/CSV (anche non standard).
**Il Processo (Ottimizzato):**
1.  **Digitalizzazione Visiva Potenziata (GPT-5.1 Vision):**
    *   Converte il PDF/immagine in input in una serie di immagini con **DPI configurabile** (tramite `VISION_IMAGE_DPI` in `src/config.py`), garantendo una migliore qualità visiva per l'AI.
    *   Utilizza la **Vision AI (`gpt-5.1`)** con un processo a due fasi:
        *   **Fase Master:** Analizza un chunk iniziale di pagine per **identificare la struttura del documento**: `raw_headers` (intestazioni visive), `header_row_index` (riga dell'header), `column_mapping` (mappatura semantica delle colonne), `pattern_type` (tipo di layout del computo metrico), e regole di estrazione/pulizia. Questo output di metadati è garantito conforme a uno schema Pydantic (`NormalizationConfig`) grazie a `structured_output` dell'API.
        *   **Fase Worker:** Processa le pagine rimanenti in parallelo per estrarre i dati tabellari, sfruttando la struttura identificata dalla fase Master per una **segmentazione rigorosa e accurata delle colonne**.
    *   Produce un file **XLSX temporaneo** contenente i dati tabellari grezzi estratti e i metadati strutturali.
2.  **Normalizzazione Intelligente:**
    *   Il `Normalizer` (`SemanticNormalizerV3`) riceve il file XLSX temporaneo.
    *   **Efficienza AI:** Se i metadati strutturali sono stati pre-calcolati dalla Vision AI (nella fase precedente per PDF/immagini), il `Normalizer` li utilizza direttamente per istanziare il parser corretto e processare il DataFrame. **Questo bypassa la sua analisi AI interna**, riducendo costi e latenza.
    *   **Fallback Robustezza:** Se l'input è un Excel nativo (e quindi non ci sono metadati pre-calcolati dal Digitizer), il `Normalizer` esegue la sua analisi AI interna (`gpt-5-nano`) per inferire il `pattern_type` e la `column_mapping` dal campione di dati.
    *   Converte i dati in un formato JSON standardizzato (`quote_request.json`) di oggetti `VoceComputoMetric`.
-   **Benefici Chiave del Nuovo Approccio:**
    *   **Qualità e Determinismo Superiori:** L'uso di Vision AI (`gpt-5.1`) con input ad alta risoluzione (DPI configurabile) e `structured_output` garantisce un'estrazione dei dati più precisa e meno soggetta a errori di interpretazione o slittamento delle colonne.
    *   **Ottimizzazione dei Costi AI:** L'eliminazione delle chiamate AI ridondanti nel `Normalizer` per i flussi digitalizzati da PDF/immagini, e l'uso di modelli più economici come `gpt-5-nano` per il fallback, riduce i costi operativi complessivi.
    *   **Semplificazione del Debugging:** L'output strutturato e i log dettagliati per ogni fase facilitano l'identificazione e la risoluzione dei problemi, in linea con i principi della Clean Architecture.

### 3. Quote Engine (`quote`)
*Il cuore del preventivatore.*

**Input:** Il file JSON generato dal Digitizer.
**Il Processo (Ibrido):**
1.  **Analisi Semantica:** Per ogni riga del preventivo (es. "Fornitura Posa Cavo 3x1.5"), l'AI cerca nel database i concetti simili, non le parole esatte (capisce che "Cordina" è simile a "Cavo").
2.  **Matching:** Identifica lo SKU (Codice Articolo) migliore.
3.  **Calcolo Deterministico:** Una volta trovato lo SKU, **abbandona l'AI**. Interroga il Grafo per sapere:
    * Quanto costa oggi il rame? (Foglia)
    * Quanto costa l'operaio? (Foglia)
    * Quanti metri/ore servono per questo articolo? (BOM)
4.  **Output:** Genera un Excel con il prezzo finale analitico, separando Materiali e Manodopera.

### 4. Sonar (`sonar`)
*Lo strumento di controllo (Raggi X).*

Un'interfaccia grafica da terminale che permette di:
* **Navigare il Grafo:** Vedere esattamente da quali componenti è formato un prezzo (es. "Perché questo quadro costa 500€? Ah, vedo 3 interruttori e 2 ore di lavoro").
* **Testare l'AI:** Provare a scrivere frasi di ricerca per vedere cosa il sistema "pesca" dal database.
* **Verificare Integrità:** Controllare se ci sono articoli con prezzo a zero o collegamenti mancanti (Orfani).

---

## 🛠️ Guida Rapida all'Installazione

1.  **Prerequisiti:**
    * Python 3.10 o superiore.
    * Chiave API OpenAI (inserita nel file `.env`).

2.  **Setup Iniziale:**
    ```bash
    # 1. Clona repo e entra nella cartella
    # 2. Crea ambiente virtuale
    python -m venv venv
    source venv/bin/activate  # (su Windows: venv\Scripts\activate)
    
    # 3. Installa dipendenze
    pip install -r requirements.txt
    
    # 4. Inizializza il Database (Crea tabelle vuote)
    python src/interfaces/cli.py init-db
    ```

3.  **Comandi Principali:**

    * **Carica Listino:**
        `python src/interfaces/cli.py ingest "docs/listino_2025.xlsx"`
    
    * **Digitalizza PDF (con Tracing):**
        `python src/interfaces/cli.py digitize --input "rdo.pdf" --output "rdo.json" --trace-id "REQ-001"`
    
    * **Fai Preventivo (collega al processo precedente):**
        `python src/interfaces/cli.py quote "rdo.json" "prev.xlsx" --trace-id "REQ-001"`
        
    * **Apri Sonar:**
        `python src/interfaces/cli.py sonar`
