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

## 🚧 In Corso (WIP)

---

## 📅 Pianificato (To Do)

### AI Intelligence (Priorità Alta)
- [ ] **[BK-02] Progressive Enrichment (Semantic Memory):** Apprendimento automatico sinonimi (Merge) per migliorare i vettori nel tempo.
- [ ] **[NEW] AI Agent "Spaccato Tecnico":** Agente multimodale per leggere schemi unifilari (PDF) allegati alle RDO e generare BOM dinamiche per le voci a corpo (Strategia per i quadri elettrici).

### Business Features (Priorità Media)
- [ ] **[BK-03] Families & Variants (CPQ):** Gestione avanzata varianti (es. Serie Civile: Vimar vs Bticino) come filtro nel preventivo.
- [ ] **[BK-04] Confidence Score Visualization:** Esporre nel file Excel di output una barra o colore (Verde/Giallo/Rosso) che indichi la sicurezza del matching AI.
- [ ] **[BK-01] Feedback Loop:** Script per ri-addestrare (o aggiornare pesi) basandosi sulle correzioni manuali fatte dall'utente sul file Excel finale.

### Infrastructure (Priorità Bassa)
- [ ] **[WEB] Dashboard Web:** Porting della UI da CLI (Sonar) a interfaccia Web (Streamlit o React).
- [ ] **[EXP] Advanced Excel Export:** Generazione Excel con formule attive (non solo valori statici) per permettere ricalcoli post-export da parte dell'utente.

- [ ] **[TEST-01] Integration Testing:** Creazione suite di test su DB in-memory per validare scenari di duplicazione righe e calcolo prezzi (evitare regressioni su logica aggregazione vs deduplicazione).
- [ ] **[DATA-01] Gestione "Prezzi a Corpo":** Identificazione e gestione articoli con prezzo forfettario (es. Quadri Elettrici) tramite flag `is_variable_price` o `warning` nel preventivo.