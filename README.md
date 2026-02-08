# 🏗️ AI MEP Estimator (Preventivatore Elettrico) - MVP v3.1

Sistema intelligente end-to-end per la digitalizzazione, normalizzazione e preventivazione di computi metrici MEP (Meccanici, Elettrici, Idraulici).
Il sistema utilizza una pipeline **RAG Agentica** (Retrieval-Augmented Generation) combinata con algoritmi di **Smart Pricing** e modelli LLM (GPT-4o) per validare le scelte tecniche.

## 🌟 What's New in v3.1

* **👷 Labor-Only Mode:** Nuova funzionalità per generare preventivi di sola manodopera. Utilizzando il flag `--solo-manodopera`, il sistema azzera automaticamente i costi dei materiali mantenendo inalterati i costi e le ore di installazione.
* **📄 Digitizer & Normalizer Unificato:** Nuova CLI (`normalize_input.py`) che accetta PDF, Immagini o Excel. Usa Agenti OpenAI per OCR e pulizia semantica.
* **💾 Database V3 Smart:** Struttura database che supporta tracciamento file sorgente, indici di volatilità e relazioni Padre/Figlio.
* **🌳 Quotation Engine Gerarchico:** Esplosione delle voci complesse (Padre + Figli/Componenti).
* **🛡️ Crash Recovery:** Salvataggio stream CSV in tempo reale per non perdere dati.

## 📂 Struttura del Progetto

    /preventivatore-ai
    │
    ├── db/                     # Database SQLite (preventivatore_v3_smart.db)
    ├── richieste_ordine/       # Input (PDF/XLSX) e Output Intermedi (JSON)
    ├── preventivi/             # Output Finale: Preventivi Excel (.xlsx)
    ├── tmp/                    # File temporanei e Recovery Stream
    │
    ├── scripts/                # Script di Manutenzione
    │   ├── bulk_ingestion.py   # Ingestion Motore Prezzi (Smart Adaptive)
    │   ├── recalc_recipe...    # Utility ricalcolo totali padre/figlio
    │   └── step17_migrate...   # Script di migrazione da V2 a V3
    │
    ├── normalize_input.py      # ENTRY POINT 1: Digitizer & Normalizer
    ├── generate_quote.py       # ENTRY POINT 2: Generatore Preventivi
    ├── interactive_sonar.py    # ENTRY POINT 3: Esplorazione DB
    │
    ├── requirements.txt        # Dipendenze Python
    └── README.md               # Documentazione Progetto

## 🚀 Workflow Operativo

Il processo si divide in tre fasi distinte: **Input**, **Ingestion (Knowledge Base)** e **Output**.

### FASE 1: Digitalizzazione e Normalizzazione
Trasforma input del cliente (PDF, Scansione, Excel) in JSON strutturato.

**Sintassi:**
    python normalize_input.py v3 --file "richieste_ordine/computo_cliente.pdf"

**Opzioni:**
* `--deep-scan`: Analisi profonda del contesto (più lento, più preciso).
* `--sample-rows 100`: Limita l'analisi a N righe per test rapidi.

### FASE 2: Aggiornamento Knowledge Base
Ingestion listini e calcolo prezzi intelligenti.

    python scripts/bulk_ingestion.py

### FASE 3: Generazione Preventivo
Genera il file Excel finale a partire dal JSON normalizzato.

    python generate_quote.py [OPZIONI]

**Opzioni Chiave:**
* `--solo-manodopera`: **(Nuovo)** Genera il preventivo azzerando i costi materiali (P.MAT = 0) e quotando esclusivamente la manodopera. Utile per appalti di sola posa.

**Funzionalità:**
* **Auto-Detection:** Trova automaticamente l'ultimo file JSON.
* **Matching Ibrido:** Vettoriale + GPT Judge.
* **Esplosione:** Scrive riga PADRE e righe FIGLIO (Componenti).
* **Recovery:** Scrive su `tmp/preventivo_recovery_stream.csv`.

### FASE 4: Analisi e Debug (Sonar)
Interrogazione manuale del DB vettoriale.

    python interactive_sonar.py

## 🧠 Logica Tecnica (Deep Dive)

### 1. Smart Pricing Adaptive
Il prezzo nel DB v3 non è statico:
* **Trigger Shock:** Variazione >20% -> Peso maggiore all'ultimo prezzo.
* **Obsolescenza:** Dato >180 gg -> Peso maggiore al nuovo prezzo.
* **Volatilità:** Se CV > 0.5, la voce è marcata come complessa.

### 2. Matching Gerarchico
* **DB:** Una "Ricetta" è collegata a N "Componenti".
* **Output:** `generate_quote.py` calcola il totale riga sommando i componenti. Con il flag `--solo-manodopera`, filtra solo i componenti di tipo `MAN`.

---

**Stato Progetto:** Produzione (MVP v3.1)