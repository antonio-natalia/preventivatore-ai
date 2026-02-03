# Preventivatore AI (RAG-Based)

Tool automatizzato per la generazione di computi metrici estimativi basato su AI e Ricerca Vettoriale.

Il sistema permette di trasformare richieste di offerta (RDO) disordinate (PDF, Excel) in preventivi strutturati, utilizzando un database vettoriale di storici e la validazione semantica di GPT-4o.

## 🚀 Funzionalità Chiave

### 1. Normalizzazione Intelligente (AI-Driven)
* **Supporto Multi-Formato:** Accetta in input **PDF** (tramite estrazione OCR/geometrica), **XLS** e **XLSX**.
* **Riconoscimento Pattern:** Un Agente AI identifica automaticamente la struttura del computo:
    * *Pattern A (Piatto):* Voci semplici riga per riga.
    * *Pattern B (A Misurazioni):* Voci con righe di dettaglio misure e totali separati.
    * *Pattern C (Gerarchico):* Voci Padre (descrizione tecnica) + Figli (varianti/misure).
* **Strategia "Decoupled":** Pipeline a due stadi (Digitizer -> Normalizer) per massimizzare la qualità su input complessi.

### 2. Motore di Ricerca Ibrido
* **Vector Search:** Ricerca semantica ad alta precisione su database SQLite locale (estensione `sqlite-vec`).
* **AI Validation:** GPT-4o ("The Judge") analizza i candidati vettoriali e scarta i falsi positivi basandosi su specifiche tecniche sottili.

### 3. Generazione Preventivo
* **Output Excel:** Genera file `.xlsx` pronti per il cliente, completi di:
    * Prezzi Materiali e Manodopera (ereditati dallo storico).
    * Analisi dei costi (Voci Padre e Sottocomponenti).
    * Metriche di confidenza (Match / Warning / No Match).

---

## 🛠 Installazione

1.  **Clona la repository:**
    ```bash
    git clone [https://github.com/tuo-user/preventivatore-ai.git](https://github.com/tuo-user/preventivatore-ai.git)
    cd preventivatore-ai
    ```

2.  **Configura l'ambiente:**
    Crea un file `.env` nella root del progetto con la tua chiave API:
    ```text
    OPENAI_API_KEY=sk-proj-....
    ```

3.  **Installa le dipendenze:**
    ```bash
    pip install -r requirements.txt
    ```
    *Nota: Assicurati di avere `python-dotenv`, `openai`, `pandas`, `sqlite-vec`, `xlsxwriter`, `openpyxl`, `pdfplumber`.*

---

## 📂 Struttura del Progetto

### Core Scripts
* **`step0_normalize_input.py`**
    * *Input:* `richieste_ordine/input_cliente.[pdf|xls|xlsx]`
    * *Output:* `richieste_ordine/input_cliente_clean.xlsx`
    * *Funzione:* Pipeline AI che digitalizza i PDF e normalizza le voci in un formato standard "Flat".
* **`generate_quote.py`**
    * *Input:* `richieste_ordine/input_cliente_clean.xlsx` + DB Vettoriale.
    * *Output:* `preventivi/[PREVENTIVO - TIMESTAMP] NomeCliente.xlsx`
    * *Funzione:* Il motore principale. Cerca i match nel DB, valida con GPT e produce il preventivo finale.

### Utility & Debug
* **`interactive_sonar.py`**
    * Tool interattivo da terminale per testare "al volo" la ricerca vettoriale su singole frasi. Utile per capire perché una voce non viene trovata o per calibrare le soglie di similarità.
* **`step16_incremental_master.py`** (o script di ingestion equivalente)
    * Script per caricare nuovi preventivi storici nel database vettoriale `preventivatore_v2_bulk.db`.

---

##  Workflow Tipico

1.  Metti il file del cliente (es. `Computo_Ospedale.pdf`) nella cartella `richieste_ordine`.
2.  Aggiorna la variabile `INPUT_FILENAME` in `step0_normalize_input.py`.
3.  Esegui il normalizzatore:
    ```bash
    python step0_normalize_input.py
    ```
4.  Verifica il file generato `input_cliente_clean.xlsx`.
5.  Esegui il generatore di preventivi:
    ```bash
    python generate_quote.py
    ```
6.  Troverai il preventivo completo nella cartella `preventivi`.