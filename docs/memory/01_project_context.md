# 01. Contesto di Progetto

Questo documento descrive lo scopo di business, i flussi utente principali e un glossario dei termini di dominio dedotti dall'analisi statica del codice.

## Scopo

Il sistema, denominato "Preventivatore AI", ha lo scopo di automatizzare la creazione di preventivi (offerte economiche). L'analisi del codice suggerisce che il sistema è in grado di:
1.  **Ingerire Dati:** Processare file strutturati (come Excel o XML) che contengono elenchi di materiali o lavorazioni, noti come "Computo Metrico", e popolarne un catalogo interno.
2.  **Digitalizzare Documenti:** Estrarre dati strutturati da documenti non strutturati come PDF o immagini, trasformandoli in un formato JSON intermedio, pronto per essere preventivato.
3.  **Generare Preventivi:** A partire dai dati estratti, generare un file Excel finale che rappresenta il preventivo, applicando logiche di costing complesse basate sul catalogo e sulle distinte base (Bill of Materials).

## Flussi Principali

Le funzionalità utente sono esposte tramite un'interfaccia a riga di comando (CLI) definita in `src/interfaces/cli.py`.

### 1. Ingestione Dati (`ingest`)
-   **Comando:** `python -m src.interfaces.cli ingest <file_or_folder_path>`
-   **Scopo:** Caricare nel catalogo interno i dati di un computo metrico da un file (es. Excel) o da una cartella.
-   **Flusso:**
    1.  Il comando riceve un percorso a un file o una directory.
    2.  Invoca `IngestionService`.
    3.  Il servizio analizza il file, ne normalizza il contenuto e lo utilizza per popolare o aggiornare le tabelle `catalog_items` e `bill_of_materials` tramite il `CatalogRepository`.
-   **Logiche di Business dell'Ingestione:**
    -   **Parsing Adattivo:** Riconosce automaticamente diversi formati di file Excel (es. "Item-Based", posizionale) ispezionando le prime righe del file.
    -   **Filtro Qualità:** Scarta le voci del catalogo che non hanno una descrizione completa.
    -   **Deduplicazione Distinte Base:** Se una relazione padre-figlio è definita più volte, viene considerata valida solo l'ultima occorrenza ("Last Write Wins").
    -   **Cost Roll-up Iterativo:** Calcola i costi partendo dagli articoli "foglia" (materiali base) e risalendo iterativamente il grafo per calcolare i costi degli assiemi.

### 2. Digitalizzazione Documento (`digitize`)
-   **Comando:** `python -m src.interfaces.cli digitize --input <input_file> --output <output_json>`
-   **Scopo:** Convertire un documento (PDF, immagine, o anche Excel non standard) in un formato JSON strutturato, pronto per la fase di `quote`.
-   **Flusso:**
    1.  Il comando riceve un file di input e un percorso di output.
    2.  Invoca `DigitizationService`.
    3.  Il servizio processa il file (potenzialmente usando servizi di Vision AI) e scrive il risultato strutturato nel file JSON di output.
    4.  Il flag `--deep` suggerisce l'esistenza di una modalità di analisi più intensiva.

### 3. Generazione Preventivo (`quote`)
-   **Comando:** `python -m src.interfaces.cli quote <input_json> <output_excel>`
-   **Scopo:** Generare il file Excel del preventivo finale partendo da un file JSON (tipicamente l'output del comando `digitize`).
-   **Flusso:**
    1.  Il comando riceve il JSON di input e il percorso Excel di output.
    2.  Viene invocato `QuoteService`, che contiene la logica di business principale per il "pricing".
    3.  Il servizio interagisce con il `CatalogRepository` per trovare corrispondenze nel catalogo, recuperare i costi e esplodere le distinte base.
    4.  Il risultato (`QuoteResult`) viene passato a `write_quote_dto_to_excel` per generare il file finale.
    5.  Il flag `--solo-manodopera` permette di generare un preventivo calcolando solo i costi di manodopera.
-   **Logiche di Business del Pricing:**
    -   **Matching a 3 Livelli:**
        1.  **Auto-Match:** Se la similarità semantica è > 96%, il match è automatico.
        2.  **Rifiuto Automatico:** Se la similarità è < 60%, la voce è scartata.
        3.  **Giudizio AI:** Per valori intermedi, un modello GPT valuta i candidati e seleziona il migliore, agendo come un esperto di dominio.
    -   **Fallback su Errore AI:** In caso di errore del servizio AI, il sistema accetta il candidato migliore con un `WARNING` se la similarità è alta, altrimenti lo scarta.
    -   **Esplosione Distinta Base:** Per gli articoli "assieme", il servizio recupera ed elenca tutti i sotto-componenti (figli) nel preventivo.
-   **Formato di Output (Excel):**
    -   Il file Excel generato contiene due fogli: `Preventivo` e `Metriche`.
    -   Nel foglio `Preventivo`, la gerarchia è rappresentata da una colonna `"TIPO"` che distingue le righe `"PADRE"` (voci principali) dalle righe `"FIGLIO"` (componenti della distinta base).

### 4. Utility
-   **`init-db`**: Inizializza lo schema del database da zero, creando tutte le tabelle.
-   **`sonar`**: Avvia un'interfaccia testuale (TUI) per l'esplorazione interattiva dei dati nel catalogo.
-   **`check`**: Esegue una diagnostica del sistema.

## Glossario di Dominio

-   **Voce di Catalogo (Catalog Item):** L'entità centrale del sistema, rappresentata nella tabella `catalog_items`. Può essere un materiale base o un assieme complesso (prodotto finito).
-   **Riga di Preventivo (QuoteLineItem):** Oggetto dati che rappresenta una riga principale del preventivo, arricchita con i dati di input, i dati di match dal database e lo stato del calcolo (Vedi `src/core/entities.py`).
-   **Componente di Preventivo (QuoteComponentItem):** Oggetto dati che rappresenta un sotto-componente di una riga di preventivo, derivato dalla distinta base (Bill of Materials) dell'articolo principale (Vedi `src/core/entities.py`).
-   **Distinta Base (Bill of Materials):** La relazione gerarchica tra Voci di Catalogo, definita nella tabella `bill_of_materials`. Specifica quali componenti e in che quantità (`usage_quantity`) sono necessari per produrre un assieme.
-   **Strategia di Prezzo (Pricing Strategy):** Una regola di business associata a un articolo nel catalogo (`catalog_items`).
    -   `USE_DECLARED_PRICE`: Il costo è quello dichiarato (es. da un listino fornitore). L'articolo è una "foglia" del grafo.
    -   `SUM_CHILDREN`: Il costo è calcolato ricorsivamente sommando i costi dei suoi componenti (definiti in `bill_of_materials`). L'articolo è un "nodo" del grafo.
-   **Stato di Integrità del Costo (Cost Integrity Status):** Un "semaforo" che indica lo stato del costo di un articolo.
    -   `VALID`: Il costo è aggiornato e corretto.
    -   `DIRTY`: L'articolo o uno dei suoi componenti è cambiato; il costo necessita di un ricalcolo.
    -   `BROKEN`: Uno dei componenti necessari per il calcolo è mancante ("orfano"), rendendo il calcolo impossibile.
