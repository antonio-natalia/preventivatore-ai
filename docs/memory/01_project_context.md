# 01. Contesto di Progetto

Questo documento descrive lo scopo di business, i flussi utente principali e un glossario dei termini di dominio dedotti dall'analisi statica del codice.

## Scopo

Il sistema, denominato "Preventivatore AI", ha lo scopo di automatizzare la creazione di preventivi (offerte economiche). L'analisi del codice suggerisce che il sistema è in grado di:
1.  **Ingerire Dati:** Processare file strutturati (come Excel o XML) che contengono elenchi di materiali o lavorazioni, noti come "Computo Metrico", e popolarne un catalogo interno.
2.  **Digitalizzare Documenti:** Estrarre dati strutturati da documenti non strutturati come PDF o immagini, trasformandoli in un formato JSON intermedio, pronto per essere preventivato.
3.  **Generare Preventivi:** A partire dai dati estratti, generare un file Excel finale che rappresenta il preventivo, applicando logiche di costing complesse basate sul catalogo e sulle distinte base (Bill of Materials).

## Flusso Utente MVP (SharePoint - Pull Strategy)

Il flusso utente definito per il Minimum Viable Product (MVP) implementa un approccio "Magic Folder" basato su SharePoint, utilizzando una strategia di **Pull** per superare i limiti di licenza dei connettori Power Automate.

1.  **Input:** L'utente deposita un file (es. Computo Metrico in Excel) in una cartella SharePoint dedicata (es. `_1_INPUT`) all'interno della libreria documenti del sito di preventivazione.
2.  **Trigger (Power Automate):**
    -   Un flusso "When a file is created (properties only)" rileva il nuovo file.
    -   Il flusso invia una richiesta HTTP POST autenticata al Container App Job su Azure.
    -   **Payload:** La richiesta include il **percorso completo** (`file_path`) del file appena creato e il nome del file.
3.  **Esecuzione (Container App - Pull):**
    -   Il Job si avvia ed esegue il comando `process-sharepoint-file`.
    -   **Autenticazione:** Il servizio si autentica a Microsoft Graph API utilizzando un **Service Principal** (App-Only Auth) configurato in Azure AD, con permessi `Sites.ReadWrite.All`.
    -   **Download:** Il servizio scarica il file dal percorso specificato in una cartella temporanea locale al container.
    -   **Elaborazione:** Vengono eseguiti i servizi di `Digitization` e `Quote` sul file locale.
    -   **Upload:** Il report Excel finale viene caricato su SharePoint. Il percorso di destinazione viene derivato dinamicamente sostituendo `INPUT` con `OUTPUT` nel percorso originale.
4.  **Output:** Il sistema genera il file Excel nella cartella `_2_OUTPUT`.
5.  **Notifica:**
    -   Se il job termina con successo, l'utente riceve una notifica su Microsoft Teams con il link al file.
    -   Se il job fallisce, l'utente riceve una notifica di errore con i dettagli.

## Configurazione Service Principal per SharePoint

L'applicazione necessita di un Service Principal (App Registration) in Azure Active Directory per autenticarsi a Microsoft Graph API e accedere ai file su SharePoint in modo non interattivo. Di seguito i passaggi per la configurazione.

### 1. Creazione della Registrazione App
1.  **Accesso al Portale Azure:** Accedere a `portal.azure.com`.
2.  **Azure Active Directory:** Navigare in "Azure Active Directory".
3.  **Registrazione App:**
    *   Andare su "App registrations" -> "New registration".
    *   **Name:** `PreventivatoreAI-SharePoint-Service` (o un nome descrittivo).
    *   **Supported account types:** Lasciare "Accounts in this organizational directory only".
    *   **Redirect URI:** Può essere lasciato vuoto.
    *   Cliccare su "Register".

### 2. Recupero Credenziali
Una volta creata l'app, dalla sua pagina di overview, copiare i seguenti valori. Saranno le variabili d'ambiente necessarie per l'applicazione Python:
-   `Application (client) ID` -> `SHAREPOINT_CLIENT_ID`
-   `Directory (tenant) ID` -> `SHAREPOINT_TENANT_ID`

In aggiunta, è necessario definire il nome del sito SharePoint target:
-   **Nome Sito SharePoint** -> `SHAREPOINT_SITE_NAME` (es. `LTE DIREZIONE - LTE Preventivazione`)

### 3. Creazione del Client Secret
1.  Nel menu a sinistra dell'app, navigare in "Certificates & secrets".
2.  Cliccare su "New client secret".
3.  Aggiungere una descrizione (es. `sharepoint_app_secret`) e scegliere una scadenza.
4.  **IMPORTANTE:** Copiare immediatamente il valore del segreto (colonna "Value"). Non sarà più visibile in seguito. Questo valore è il `SHAREPOINT_CLIENT_SECRET`.

### 4. Assegnazione Permessi API
1.  Nel menu a sinistra, navigare in "API permissions".
2.  Cliccare su "Add a permission" e selezionare "Microsoft Graph".
3.  Selezionare **"Application permissions"** (non "Delegated").
4.  Nella barra di ricerca, digitare `Sites` e, dall'elenco, selezionare `Sites.ReadWrite.All`.
5.  Cliccare su "Add permissions".

### 5. Concessione del Consenso Amministrativo (Admin Consent)
I permessi di tipo "Application" richiedono il consenso di un amministratore del tenant per essere attivati.
1.  Nella stessa pagina "API permissions", cliccare sul pulsante **"Grant admin consent for [Nome del Tuo Tenant]"**.
2.  Verificare che la colonna "Status" per il permesso `Sites.ReadWrite.All` mostri un'icona verde con la dicitura "Granted for...".

A questo punto, le quattro impostazioni (`SHAREPOINT_TENANT_ID`, `SHAREPOINT_CLIENT_ID`, `SHAREPOINT_CLIENT_SECRET`, `SHAREPOINT_SITE_NAME`) sono pronte per essere inserite nel file `.env` dell'applicazione o nei segreti dell'ambiente di produzione (es. Azure Container Apps).

## Manuale di Configurazione Power Automate

Di seguito sono riportate le istruzioni passo-passo per configurare il flusso Power Automate necessario per orchestrare la pipeline.

### Prerequisiti
-   Accesso a Power Automate.
-   Connettore HTTP (Premium) disponibile.
-   Credenziali (Client ID e Secret) per autenticare la chiamata HTTP verso Azure (Gestione API o autenticazione diretta su endpoint Container App).

### Step 1: Trigger SharePoint
-   **Connettore:** SharePoint
-   **Azione:** *When a file is created (properties only)*
-   **Configurazione:**
    -   **Site Address:** Selezionare il sito `LTE DIREZIONE - LTE Preventivazione`.
    -   **Library Name:** Selezionare `Documenti`.
    -   **Folder:** Selezionare la cartella `_1_INPUT` (navigando il percorso `LTE Preventivazione/Test Preventivatore/INPUT`).

### Step 2: Trigger Azure Job (HTTP)
-   **Connettore:** HTTP
-   **Azione:** *HTTP*
-   **Configurazione:**
    -   **Method:** `POST`
    -   **URI:** Inserire l'endpoint di gestione Azure per avviare il job.
        -   Formato: `https://management.azure.com/subscriptions/{SUB_ID}/resourceGroups/{RG_NAME}/providers/Microsoft.App/jobs/{JOB_NAME}/start?api-version=2024-03-01`
    -   **Authentication:** Selezionare *Active Directory OAuth*.
        -   *Tenant:* Il tuo Tenant ID.
        -   *Audience:* `https://management.azure.com/`
        -   *Client ID / Secret:* Le credenziali di un Service Principal che ha il ruolo "Contributor" (o "Container Apps Job Operator") sul Resource Group.
    -   **Body (JSON):**
        ```json
        {
          "template": {
            "containers": [
              {
                "name": "job-preventivatore",
                "command": [
                  "python",
                  "-m",
                  "src.interfaces.cli",
                  "process-sharepoint-file",
                  "--file-path",
                  "@{triggerBody()?['{Path}']}" 
                ]
              }
            ]
          }
        }
        ```
        *Nota:* `@{triggerBody()?['{Path}']}` è il contenuto dinamico "Full Path" (Percorso completo) fornito dal trigger SharePoint.

### Step 3: Loop di Monitoraggio (Opzionale ma Raccomandato)
Poiché l'azione HTTP avvia il job in modo asincrono (Fire & Forget), per inviare la notifica di completamento è necessario interrogare lo stato.
-   **Azione:** *Do until*
    -   Loop fino a che lo stato dell'esecuzione del job è `Succeeded` o `Failed`.
    -   All'interno del loop: `Delay` (30 secondi) -> `HTTP GET` (Status dell'esecuzione).

### Step 4: Notifica Teams
-   **Connettore:** Microsoft Teams
-   **Azione:** *Post message in a chat or channel*
-   **Configurazione:**
    -   **Recipient:** `@{triggerBody()?['Editor']?['Email']}` (L'email dell'utente che ha caricato il file).
    -   **Message:** "Il tuo preventivo per il file `@{triggerBody()?['{Name}']}` è pronto. Trovi il report nella cartella OUTPUT."

## Flussi Principali (CLI)

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
    -   Il sistema genera due file Excel distinti: un **Report Analitico** e un **File di Import per CPM**.
    -   **Report Analitico:** Contiene due fogli, `Preventivo` e `Metriche`. Nel foglio `Preventivo`, la gerarchia è rappresentata da una colonna `"TIPO"` (`"PADRE"`/`"FIGLIO"`). Questo file è pensato per la revisione umana.
    -   **File di Import per CPM:** Un file a singolo foglio con una struttura gerarchica basata su una colonna "Livello" (`1` per padre, `2` per figlio) e con intestazioni di colonna che corrispondono esattamente a quelle richieste da Teamsystem CPM per l'importazione.

### 4. Integrazione SharePoint (`process-sharepoint-file`)
-   **Comando:** `python -m src.interfaces.cli process-sharepoint-file --file-path <path_to_file>`
-   **Scopo:** Entry point per l'automazione cloud. Gestisce il ciclo di vita completo di un file attivato da remoto.
-   **Flusso:**
    1.  Riceve il percorso relativo del file su SharePoint (es. `/Shared Documents/Project/INPUT/file.xlsx`).
    2.  Crea un ambiente temporaneo isolato (temp directory).
    3.  Scarica il file tramite Microsoft Graph API.
    4.  Esegue in sequenza `digitize` e `quote`.
    5.  Calcola il percorso di output sostituendo "INPUT" con "OUTPUT" nel percorso originale.
    6.  Carica il report finale su SharePoint.

### 5. Utility
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
