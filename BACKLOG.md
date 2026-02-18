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

### 3. Orchestration (Power Automate)

- [ ] **[ORCH-01] Power Automate Logic Definition**
    * **Obiettivo:** Documentare (o esportare JSON) il flusso logico per l'implementazione sul tenant del cliente.
    * **Specifiche:**
        1.  **Trigger:** SharePoint "When a file is created (properties only)" -> Cartella `Input`.
        2.  **Action 1:** "Get file content" (SharePoint).
        3.  **Action 2:** "Create file" (Azure File Storage) -> Path `/input/{filename}`.
        4.  **Action 3 (HTTP Premium):** POST verso Azure Container Apps Job execution endpoint.
            * Auth: Managed Identity o Token.
            * Body: `{"args": ["digitize", "--input", "/mnt/data/input/{filename}", "--output", "/mnt/data/temp/{filename}.json"]}`.
        5.  **Action 4 (HTTP Premium):** POST (secondo job per 'quote') o catena di comandi.
        6.  **Action 5:** Polling/Delay loop in attesa del file in `/output`.
        7.  **Action 6:** "Create file" (SharePoint) -> Cartella `Output`.
        8.  **Action 7:** Teams Notification.

---

## 🚧 In Corso (WIP)

---

## 📅 Pianificato (To Do)
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