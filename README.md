# 🏗️ AI MEP Estimator (Preventivatore Elettrico) - MVP v2.0

Sistema intelligente per la generazione di computi metrici e preventivi elettrici (MEP), basato su RAG (Retrieval-Augmented Generation) e algoritmi di Pricing Adattivo.

---

## 🌟 What's New in v2.0 (Smart Pricing)

* **🧠 Adaptive Pricing Logic:** Il sistema non si limita a una media statica. Rileva automaticamente **shock di prezzo** (>20% di variazione) e dati obsoleti (>6 mesi), spostando dinamicamente il peso statistico verso i dati di mercato più recenti.
* **🛡️ Volatility Safety Net:** Identifica articoli ad alto rischio (es. Quadri Elettrici complessi) calcolando il **Coefficiente di Variazione (CV)**. Se la volatilità supera la soglia di sicurezza, il sistema blocca il prezzo automatico e richiede una stima manuale ("MANUAL ESTIMATION").
* **🤖 Agentic Deduplication:** Un Agente AI interviene durante l'ingestion per decidere se un nuovo articolo è una variante di uno esistente (**Merge**) o un prodotto tecnicamente diverso (**Branch**), mantenendo il database pulito.
* **📊 Pricing Strategies:** Supporto per strategie di prezzo forzate via riga di comando (`MAX`, `LATEST`, `SMART_ADAPTIVE`) per scenari di mercato incerti.

---

## 📂 Project Structure

    /preventivatore-ai
    │
    ├── db/                     # Database SQLite (Relazionale + Vettoriale)
    ├── richieste_ordine/       # Input: File RDO del cliente (.xlsx)
    ├── preventivi/             # Output: Preventivi generati con analisi prezzi
    ├── tests/                  # Suite di Test End-to-End e Regressione
    │   └── test_pipeline.py
    │
    ├── scripts/                # Script di Ingestion e Manutenzione
    │   ├── bulk_ingestion.py   # Core Ingestion Engine (Adaptive Logic)
    │   ├── step17_migrate...   # Script di migrazione dati Legacy -> Smart
    │   └── normalize_input.py  # Utility di pre-processing
    │
    ├── generate_quote.py       # Core Quotation Engine (Script Principale)
    ├── requirements.txt        # Dipendenze Python
    └── README.md               # Documentazione Progetto

---

## 🚀 Quick Start

### 1. Installazione

    pip install -r requirements.txt
    # Nota: Assicurarsi che l'estensione sqlite-vec sia configurata se si usa vector search avanzata

### 2. Ingestion Dati (Popolamento DB)
Carica listini o storici preventivi nel "Cervello" del sistema. Lo script si trova ora nella cartella `scripts/`.

    # Modalità Standard (Smart Adaptive - Consigliata)
    python scripts/bulk_ingestion.py

    # Modalità Override (es. Forza prezzi massimi per prudenza)
    python scripts/bulk_ingestion.py --override MAX
    # Opzioni: MAX, LATEST, SMART_1Y, SMART_ADAPTIVE

### 3. Generazione Preventivo
Processa una richiesta cliente (RDO). Il sistema cercherà match semantici e applicherà la logica di pricing.

    # Assicurarsi che il file input sia in richieste_ordine/input_cliente_clean.xlsx
    python generate_quote.py

*L'output verrà salvato in `preventivi/` con evidenziazione automatica delle voci a rischio (Giallo/Arancione).*

### 4. Esecuzione Test
Per verificare che la logica finanziaria e di sicurezza funzioni correttamente:

    python -m unittest tests/test_pipeline.py

---

## 🧠 Logica di Smart Pricing (Technical Deep Dive)

Il calcolo del prezzo unitario (`unit_price`) segue questo albero decisionale rigoroso:

1.  **Safety Check (Volatilità):**
    * Viene calcolato il CV (Deviazione Standard / Media) su tutto lo storico.
    * Se `CV > 0.5` (alta instabilità), la voce è marcata `is_complex`.
    * **Output:** Prezzo `0.00 €` + Stato `MANUAL_ESTIMATION`.

2.  **Adaptive Trigger (Reattività):**
    * Il sistema confronta l'ultimo prezzo inserito con la media storica.
    * **Trigger 1 (Shock):** Variazione prezzo > 20%.
    * **Trigger 2 (Obsolescenza):** Ultimo aggiornamento > 180 giorni fa.
    * Se uno dei trigger scatta: **Peso 90%** all'ultimo prezzo, **10%** allo storico.

3.  **Standard Fallback (Stabilità):**
    * Se il mercato è stabile, applica una media pesata temporale classica.
    * **Pesi:** 1.0 (Anno corrente), 0.5 (Anno precedente), 0.1 (Storico antico).

---

## 🛠️ Manutenzione & Migrazione

### Migrazione da Legacy (v1)
Se provieni dalla versione 1 del database, esegui questo script per inizializzare le strutture dati "Smart" e calcolare le metriche iniziali:

    python scripts/step17_migrate_legacy.py

*Attenzione: Questo script resetta il DB target `preventivatore_v3_smart.db`.*

---

## 🏷️ Versionamento (Git Flow)

### Creazione Tag MVP v2.0

    git add .
    git commit -m "Release MVP v2.0: Smart Pricing & Adaptive Logic"
    git push origin main
    git tag -a v2.0 -m "MVP v2: Smart Pricing, Safety Nets & Unit Tests"
    git push origin v2.0

### Ripristino MVP v1.0 (Legacy)
Se necessario tornare alla versione base (solo RAG, senza logica prezzi complessa), fare checkout del tag `v1.0` (se precedentemente creato).

---
**Project Status:** Production Ready (MVP v2.0)