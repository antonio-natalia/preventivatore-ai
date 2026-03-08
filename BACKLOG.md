# Backlog di Progetto

Questo documento consolida le attività pianificate, tracciando lo stato di avanzamento tra le funzionalità Core, AI e Interfaccia.

## ✅ Completato (Done)

### Core Architecture & Database
- [x] **[ARCH-01] Refactoring Clean Architecture:** Separazione in `core`, `infrastructure`, `services`.
- [x] **[DB-01] Schema V3 Domain-Driven:** Migrazione a Grafo Relazionale (`catalog_items`, `bill_of_materials`) per supporto calcolo deterministico.
- [x] **[CLI] Unified Interface:** Centralizzazione comandi in `cli.py` con auto-init del DB.

### Ingestion & Pricing Engine
- [x] **[ING-03] Ingestion Resiliente:** Gestione file multipli e formati misti (XML/XLS).
- [x] **[NEW] Item-Based Parser:** Supporto specifico per export TeamSystem (colonne dinamiche `Q_COMP`, `Q_MAN`, gestione formati numerici).
- [x] **[FEAT-01] Struttura Gerarchica:** Implementazione logica Bottom-Up (`SUM_CHILDREN` vs `USE_DECLARED_PRICE`).
- [x] **[NEW] Deduplicazione & Integrità:** Strategia *Last Write Wins* per i duplicati e vincoli `UNIQUE` DB ripristinati.

### AI & Digitization
- [x] **[ING-03] Normalizer & Digitizer:** Pipeline `PDF -> GPT-4o Vision -> JSON` per standardizzare le RDO (implementato in `DigitizationService`).
- [x] **[PMT-01] Chain of Thought:** Prompt engineering per il matching semantico con spiegazione logica (`reasoning`).

### User Interface
- [x] **[ARCH-07] Sonar TUI:** Interfaccia terminale per navigazione grafo, ricerca vettoriale e debug BOM.

---

## ☁️ Cloud V1 Release (SharePoint + Azure CA Jobs)

Questa sezione contiene tutte le attività necessarie per la messa in produzione su Azure con architettura Event-Driven.

### 1. Application Readiness (Containerization)

- [x] **[OPS-01] Dockerfile & Build Optimization**
    * **Obiettivo:** Creare un'immagine Docker leggera e sicura per l'esecuzione dei Job.
    * **Specifiche:**
        1.  Base Image: `python:3.10-slim` (per ridurre dimensioni e superficie d'attacco).
        2.  Workdir: `/app`.
        3.  Dependencies: Copiare `requirements.txt` ed eseguire `pip install --no-cache-dir`.
        4.  Source: Copiare cartella `src`.
        5.  Entrypoint: Non definire un CMD fisso (sarà sovrascritto dal Job), ma assicurarsi che `src/interfaces/cli.py` sia eseguibile.
        6.  User: Eseguire come utente non-root (security best practice).

- [x] **[OPS-02] Cloud-Native Logging (JSON)**
    * **Obiettivo:** Rendere i log leggibili da Azure Log Analytics quando in produzione.
    * **Specifiche:**
        1.  Modificare `src/interfaces/cli.py` o creare `src/infrastructure/logger.py`.
        2.  Leggere variabile d'ambiente `APP_ENV`.
        3.  Se `APP_ENV == 'CLOUD'`, configurare il logger per emettere output in formato JSON (usando libreria `python-json-logger` o formatter custom).
        4.  Campi richiesti: `timestamp`, `level`, `message`, `module`, `correlation_id` (se disponibile).
        5.  Se `APP_ENV` è vuoto o 'LOCAL', mantenere output colorato su console.

### 2. Infrastructure as Code (Azure Setup)

- [x] **[INFRA-01] Setup Script (Azure CLI)**
    * **Obiettivo:** Script Bash/PowerShell per creare l'intera infrastruttura con un comando.
    * **Specifiche:**
        1.  Creare Resource Group: `rg-preventivatore-prod`.
        2.  Creare Azure Container Registry (ACR): `acrpreventivatore`.
        3.  Creare Storage Account + File Share: `stpreventivatore` / share `data`.
        4.  Creare Container Apps Environment: `cae-preventivatore` (profilo 'Consumption').
        5.  **Output:** Lo script deve stampare le credenziali ACR e la Connection String dello Storage (da passare al cliente per il setup iniziale).

- [x] **[INFRA-02] Container App Job Definition**
    * **Obiettivo:** Definire il template YAML per il Job di Azure Container Apps.
    * **Specifiche:**
        1.  Nome: `job-preventivatore`.
        2.  Trigger Type: `Manual` (sarà scatenato via HTTP da Power Automate).
        3.  Volumes: Montare la Azure File Share `data` sul path `/mnt/data`.
        4.  Secrets: Definire `openai-api-key` (da KeyVault o input manuale).
        5.  Env Vars: `APP_ENV=CLOUD`, `OPENAI_API_KEY=secretref:openai-api-key`.
        6.  Resources: CPU 1.0, Memory 2.0Gi.

### 3. Orchestration and User Workflow (MVP)

- [x] **[MVP-01] Implementazione Pipeline di Automazione con Report Analitico**
    * **User Story:**
        > Come Specialista Preventivi, voglio depositare un file di Computo Metrico in una cartella SharePoint e ricevere automaticamente un report analitico dettagliato in una cartella di output, così da poter vedere come l'AI ha processato il documento senza eseguire codice manualmente.
    * **Criteri di Accettazione:**
        1.  Due cartelle SharePoint sono configurate: `_1_INPUT` e `_2_OUTPUT`.
        2.  Un flusso Power Automate viene creato, attivato dall'aggiunta di un nuovo file nella cartella `_1_INPUT`.
        3.  Il flusso invoca un servizio di backend che esegue i comandi esistenti `digitize` e `quote` sul file di input.
        4.  Il servizio di backend genera il report Excel analitico standard usando l'attuale `excel_writer.py`.
        5.  Il file Excel risultante viene salvato nella cartella `_2_OUTPUT`.
        6.  Una notifica di successo viene inviata via Microsoft Teams all'utente che ha caricato il file, contenente un link al report di output.
        7.  Una notifica di errore viene inviata se il processo fallisce.
    * **Note di Implementazione:**
        -   Il servizio di backend dovrebbe essere un semplice wrapper stateless attivato via HTTP (es. Azure Function, Azure Container App) attorno all'applicazione Python containerizzata.
        -   Il flusso Power Automate deve passare il contenuto del file o un link sicuro al servizio di backend.
        -   L'identificazione dell'utente per le notifiche Teams deve essere gestita da Power Automate, che cattura la proprietà "File Creato Da".
        -   Questa attività **non** implica la modifica della logica di generazione Excel esistente.

---

## 🚧 In Corso (WIP)
- [ ] **[BUG] Migliorare Qualità Estrazione PDF (Digitizer):** La conversione del PDF in XLSX temporaneo produce risultati troppo scadenti (dall'Assistant API) per consentire al Normalizer di individuare l'header di partenza per il parsing e il parser corretto.
    *   **Obiettivo:** Il Digitizer deve restituire un output XLSX congruo per la successiva analisi da parte del Normalizer.
    *   **Note:** Valutare se affinare il `PROMPT_DIGITIZER`, esplorare opzioni avanzate di `pdfplumber` nel prompt o considerare l'uso di un modello GPT più performante (es. `gpt-4o`) per questo specifico task.

---

## 📅 Pianificato (To Do)
- [ ] **[MVP-02] Implementazione Esportazione Excel Compatibile con CPM**
    * **User Story:**
        > Come Specialista Preventivi, dopo aver revisionato il report analitico, voglio un secondo file Excel formattato specificamente per Teamsystem CPM, così da poter fare copia-incolla dei dati direttamente in CPM per accelerare la creazione del preventivo ufficiale.
    * **Criteri di Accettazione:**
        1.  Un nuovo modulo writer Excel viene creato (es. `src/infrastructure/cpm_excel_writer.py`).
        2.  Il nuovo writer prende un oggetto `QuoteResult` e genera un file Excel con una struttura compatibile con l'importazione copia-incolla di CPM.
        3.  Il file di output deve usare una struttura gerarchica rappresentata da una colonna "Livello" (es. `1` per le voci padre, `2` per i componenti figli).
        4.  Le intestazioni di colonna nel file generato devono **corrispondere esattamente** ai nomi richiesti da CPM.
        5.  Per le voci `NO MATCH`, la riga di output deve contenere i dati originali, con le colonne relative ai costi lasciate vuote.
        6.  Il flusso principale dell'applicazione viene aggiornato per generare **due file** al termine: il report analitico e il nuovo file di importazione per CPM.
    * **Note di Implementazione:**
        -   **Prerequisito:** I nomi esatti delle colonne e il loro ordine per l'importazione in CPM devono essere ottenuti dagli stakeholder e definiti come costanti.
        -   Il nuovo writer dovrà iterare su `QuoteResult.items` per scrivere le righe di "Livello 1" e le righe di "Livello 2" per ogni figlio.
        -   Il file prodotto sarà posizionato nella cartella `_2_OUTPUT` insieme al report analitico, con un nome distinto (es. `[NomeOriginale]_PER_CPM.xlsx`).

- [ ] **[KPI-02] Qualità del Prezzo (Accuracy Analysis):**
    * **Obiettivo:** Misurare quanto il preventivo AI si discosta da quello finale corretto dall'uomo.
    * **Specifiche:** Script offline che prende due Excel (AI Output vs Human Final), calcola il delta sui totali e sulle singole righe, e genera un report di accuratezza (%).

## Backlog
### Product KPIs


### Testing
- [ ] **[TEST-01] Integration Testing:** Creazione suite di test su DB in-memory per validare scenari di duplicazione righe e calcolo prezzi.

### AI Intelligence
- [ ] **[NEW] AI Agent "Feedback Loop":** Implementazione feedback loop per autoapprendimento di errori o preferenze dell'utente nella redazione dei preventivi.
- [ ] **[NEW] AI Agent "Spaccato Tecnico":** Agente multimodale per leggere schemi unifilari (PDF) allegati alle RDO e generare BOM dinamiche per le voci a corpo (Strategia per i quadri elettrici).
- [ ] **[DATA-01] Gestione "Prezzi a Corpo":** Identificazione e gestione articoli con prezzo forfettario (es. Quadri Elettrici) tramite flag `is_variable_price` o `warning` nel preventivo.

### Business Features
- [ ] **[BK-02] Progressive Enrichment (Semantic Memory):** Apprendimento automatico sinonimi (Merge) per migliorare i vettori nel tempo.
- [ ] **[BK-03] Families & Variants (CPQ):** Gestione avanzata varianti (es. Serie Civile: Vimar vs Bticino) come filtro nel preventivo.
