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
* **AI Models:** OpenAI `text-embedding-3-small` (Embedding) e `gpt-4o` (Vision/Reasoning).
* **Data Processing:** Pandas (ETL Excel), Pydantic (Validazione Dati).
* **Interface:** CLI (Command Line Interface) nativa.

### Design Patterns & Cavilli Implementativi
* **Repository Pattern:** L'accesso ai dati è astratto in `src/infrastructure/repositories.py`. Il codice di business non tocca mai l'SQL direttamente.
* **Recursive CTE (Common Table Expressions):** Il calcolo dei costi (`calculate_node_cost`) utilizza query SQL ricorsive per attraversare l'albero della BOM a profondità arbitraria in millisecondi.
* **Idempotenza & Last-Write-Wins:** L'ingestion è progettata per essere eseguita `N` volte senza corrompere i dati. Se un listino ridefinisce un articolo, l'ultima versione sovrascrive la precedente (Deduplicazione a Dizionario prima del DB).
* **Unique Constraints:** Il DB impone vincoli stretti (`UNIQUE(parent_sku, child_sku)`) nella tabella `bill_of_materials` per impedire duplicati fisici e garantire l'integrità del grafo.

### Struttura del Progetto
* `src/core`: Entità di dominio e interfacce astratte.
* `src/infrastructure`: Implementazioni concrete (SQLite, OpenAI, Excel Parsers).
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
*Trasforma la carta in dati.*

**Input:** PDF (anche scansionati), Immagini (PNG/JPG) di computi metrici.
**Il Processo:**
1.  **Vision AI:** Usa GPT-4o Vision per "vedere" il documento come un umano, riconoscendo tabelle, descrizioni tecniche e quantità anche in layout complessi.
2.  **Normalizzazione:** Converte tutto in un formato JSON standardizzato (`quote_request.json`) che separa chiaramente le righe di computo dalle note legali.

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
    
    * **Digitalizza PDF:**
        `python src/interfaces/cli.py digitize --input "richiesta.pdf" --output "richiesta.json"`
    
    * **Fai Preventivo:**
        `python src/interfaces/cli.py quote "richiesta.json" "preventivo_output.xlsx"`
        
    * **Apri Sonar:**
        `python src/interfaces/cli.py sonar`