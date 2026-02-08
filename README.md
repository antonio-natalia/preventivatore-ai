# 🏗️ AI MEP Estimator (Preventivatore Elettrico) - MVP v3.0

Sistema intelligente end-to-end per la digitalizzazione, normalizzazione e preventivazione di computi metrici MEP (Meccanici, Elettrici, Idraulici).
Il sistema utilizza una pipeline **RAG Agentica** (Retrieval-Augmented Generation) combinata con algoritmi di **Smart Pricing** e modelli LLM (GPT-4o) per validare le scelte tecniche.

---

## 🌟 What's New in v3.0 (Full Pipeline)

* **📄 Digitizer & Normalizer Unificato:** Nuova CLI (`normalize_input.py`) che accetta PDF, Immagini o Excel. Usa Agenti OpenAI per OCR e pulizia semantica, producendo un output JSON strutturato (`_clean.json`) invece di file Excel intermedi.
* **💾 Database V3 Smart:** Nuova struttura database (`preventivatore_v3_smart.db`) che supporta:
    * Tracciamento file sorgente per ogni prezzo.
    * Indici di volatilità e complessità.
    * Relazioni Padre/Figlio (Ricette e Componenti).
* **🌳 Quotation Engine Gerarchico:** Il generatore di preventivi (`generate_quote.py`) ora esplode le voci complesse. Nel preventivo finale vedrai la voce "Padre" (es. Punto Luce) e sotto i "Figli" (Scatola, Frutto, Placca) con i relativi fabbisogni.
* **🛡️ Crash Recovery:** Il preventivatore scrive un flusso CSV in tempo reale (`tmp/stream...`). Se il processo si interrompe, non perdi il lavoro svolto fino a quel momento.
* **📡 Interactive Sonar V3:** Tool da riga di comando per esplorare il database vettoriale, analizzare la volatilità dei prezzi e verificare i match con GPT in tempo reale.

---

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
    │   └── step17_migrate...   # Script di migrazione da V2 a V3
    │
    ├── normalize_input.py      # ENTRY POINT 1: Digitizer & Normalizer
    ├── generate_quote.py       # ENTRY POINT 2: Generatore Preventivi
    ├── interactive_sonar.py    # ENTRY POINT 3: Esplorazione DB
    │
    ├── requirements.txt        # Dipendenze Python
    └── README.md               # Documentazione Progetto

---

## 🚀 Workflow Operativo

Il processo si divide in tre fasi distinte: **Input**, **Ingestion (Knowledge Base)** e **Output**.

### FASE 1: Digitalizzazione e Normalizzazione
Il primo passo è trasformare qualsiasi input del cliente (PDF, Scansione, Excel sporco) in un formato JSON pulito e comprensibile dalla macchina.

**Sintassi:**

    python normalize_input.py v3 --file "richieste_ordine/computo_cliente.pdf"

**Cosa succede:**
1.  **Rilevamento:** Se è un PDF/IMG, attiva l'Agente Digitizer (OCR con GPT-4o Vision).
2.  **Estrazione:** Crea un Excel grezzo (`raw_input.xlsx`).
3.  **Normalizzazione:** L'AI analizza semanticamente le colonne, identifica Descrizioni e Quantità.
4.  **Output:** Genera un file `richieste_ordine/[nome_file]_clean.json`.

**Opzioni Avanzate:**
* `--deep-scan`: Invia tutto il contenuto del file all'AI per capire contesti complessi (più lento, più preciso).
* `--sample-rows 100`: Aumenta il numero di righe analizzate per file molto lunghi.

### FASE 2: Aggiornamento Knowledge Base (Opzionale)
Se hai nuovi listini fornitori o storici preventivi da imparare.

    python scripts/bulk_ingestion.py

**Logica di Ingestion:**
* **Smart Merge:** Se una voce esiste già, aggiorna lo storico prezzi.
* **Branching:** Se una voce è tecnicamente diversa, crea una nuova "Ricetta".
* **Pricing:** Calcola volatilità e prezzi medi pesati (vedi sezione Smart Pricing).

### FASE 3: Generazione Preventivo
Prende l'ultimo file JSON generato e crea il preventivo Excel.

    python generate_quote.py

**Funzionalità Chiave:**
* **Auto-Detection:** Trova automaticamente l'ultimo file `_clean.json` nella cartella `richieste_ordine`.
* **Matching Ibrido:** Vettoriale (Ricerca) + GPT Judge (Validazione tecnica).
* **Esplosione:** Scrive nel file Excel sia la voce aggregata che i componenti analitici.
* **Recovery:** Scrive su `tmp/preventivo_recovery_stream.csv` riga per riga.
* **Output:** Salva in `preventivi/[PREVENTIVO - Data] nome_file.xlsx`.

### FASE 4: Analisi e Debug (Sonar)
Per interrogare il DB manualmente e capire perché una voce non viene trovata o ha un prezzo specifico.

    python interactive_sonar.py

---

## 🧠 Logica Tecnica (Deep Dive)

### 1. Smart Pricing Adaptive
Il prezzo nel DB v3 non è statico. `bulk_ingestion.py` calcola il prezzo unitario seguendo queste regole:

* **Trigger Shock:** Se il nuovo prezzo devia >20% dalla media storica -> Peso 90% all'ultimo dato (il mercato è cambiato).
* **Trigger Obsolescenza:** Se il dato è vecchio (>180 gg) -> Peso dominante al nuovo dato.
* **Volatilità:** Calcola il CV (Coefficiente Variazione). Se > 0.5, la voce viene marcata come `is_complex` e richiede attenzione.

### 2. Matching Gerarchico (Padre/Figlio)
A differenza della v2, il sistema v3 gestisce la composizione:

* **DB:** Una "Ricetta" (es. Punto Luce) è collegata a N "Componenti" (Scatola, Cavo, Frutto).
* **Output:** `generate_quote.py` scrive la riga PADRE (con il prezzo totale matchato) e subito sotto le righe FIGLIO (indentate) con i fabbisogni calcolati (`Quantità RDO * Coefficiente Componente`).

### 3. Stati del Preventivo (Color Coding)
Il file Excel finale usa codici colore per guidare l'utente:

* **VERDE (MATCH):** Corrispondenza tecnica verificata da GPT o similarità vettoriale > 96%.
* **GIALLO (WARNING):** Corrispondenza probabile ma con dubbi (es. marca diversa, descrizione ambigua).
* **ROSSO (NO MATCH):** Nessuna corrispondenza trovata nel DB. Prezzo basato su stima RDO o zero.

---

## 🛠️ Manutenzione

### Migrazione da v2 a v3
Se hai un database legacy `preventivatore_v2_bulk.db`, usa lo script di migrazione per generare la struttura v3 e ricalcolare i vettori:

    python scripts/step17_migrate_legacy.py

*Nota: Questo resetta il DB target v3.*

### Reset Recovery
Se `generate_quote.py` si interrompe, puoi consultare `tmp/preventivo_recovery_stream.csv` per i dati parziali. Questo file viene sovrascritto ad ogni nuova esecuzione completa.

---

**Stato Progetto:** Produzione (MVP v3.0)