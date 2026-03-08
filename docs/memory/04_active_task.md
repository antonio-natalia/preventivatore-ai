- [x] Generazione iniziale della documentazione tramite reverse engineering della codebase.
- [x] Definizione del flusso di avvio del preventivatore da utente.
- [x] Implementare la pipeline MVP con report analitico (`[MVP-01]` da `BACKLOG.md`).
- [x] Configurazione del flusso Power Automate per l'avvio del preventivatore all'upload di un nuovo file in una specifica cartella SharePoint.
- [ ] Implementare l'esportazione Excel compatibile con CPM (`[MVP-02]` da `BACKLOG.md`).
- [ ] **[BUG] Migliorare Qualità Estrazione PDF (Digitizer):** La conversione del PDF in XLSX temporaneo produce risultati troppo scadenti (dall'Assistant API con `code_interpreter`) per consentire al Normalizer di individuare l'header di partenza per il parsing e il parser corretto.
    *   **Obiettivo di Prodotto:** Il Digitizer deve restituire un output XLSX di alta qualità, strutturato e congruo per la successiva analisi da parte del Normalizer, garantendo che l'header e il pattern di parsing possano essere individuati correttamente.
    *   **Soluzione Tecnica Scelta:** Implementare un processo di digitalizzazione basato sulle API di Visione native di `gpt-4o` (Completion API), combinando un'estrazione iniziale del "master header" con un'elaborazione asincrona pagina per pagina dei dati.

    *   **Dettagli Tecnici Implementativi:**
        1.  **Conversione PDF-Immagini:** Utilizzare la libreria `PyMuPDF` (installata tramite `requirements.txt` e con le dipendenze di sistema necessarie nel `dockerfile`) per convertire ogni pagina del PDF di input in un'immagine (formato PNG).
        2.  **Fase 1: Identificazione del Master Header:**
            *   Effettuare una prima chiamata sincrona all'API di Visione di `gpt-4o` inviando un **chunk iniziale di pagine** del PDF (es. le prime 1-5 pagine, entro il limite di 20 immagini dell'API).
            *   Il prompt per questa chiamata sarà estremamente focalizzato sull'identificazione dell'**unico e completo header della tabella principale** del documento. L'output atteso sarà un JSON contenente solo questo master header.
            *   Questo "master header" sarà la guida per l'allineamento dei dati di tutte le pagine successive.
        3.  **Fase 2: Estrazione Dati Pagina per Pagina (Asincrona):**
            *   Una volta individuato il "master header", processare le pagine rimanenti del PDF **individualmente**, in modo **asincrono**, utilizzando `asyncio` per inviare multiple richieste API in parallelo.
            *   Per ogni pagina, invocare l'API di Visione di `gpt-4o` fornendo il "master header" nel prompt.
            *   Il prompt istruirà l'AI a estrarre *soltanto le righe di dati* dalla pagina corrente, allineandole a quell'header e ignorando qualsiasi ripetizione di header o altro testo non pertinente. Se l'AI rileva un nuovo header concettualmente diverso, può essere istruita a segnalarlo separatamente (funzionalità da valutare e aggiungere in futuro se necessario, per ora ci si concentra sull'header principale).
            *   Ogni risposta da GPT-4o sarà un JSON contenente i dati estratti dalla singola pagina.
        4.  **Consolidamento e Output:**
            *   Consolidare in ordine i dati estratti da tutte le singole pagine (fase 2) in un'unica struttura (es. `list[list[str]]`). La coerenza garantita dal "master header" semplifica notevolmente questa fase, che diventa una semplice concatenazione.
            *   Costruire un `pandas.DataFrame` utilizzando il "master header" come intestazione e i dati consolidati.
            *   Salvare il `DataFrame` risultante in un nuovo file XLSX temporaneo (es. `raw_input.xlsx`) che il Normalizer potrà processare in modo affidabile, trovando l'header e il pattern corretto.
        5.  **Miglioramenti al Logging per Debugging e Trasparenza:**
            *   Registrare l'**intera stringa raw** (`response.choices[0].message.content`) ricevuta da GPT-4o *prima* di qualsiasi pulizia o parsing, in caso di errori di parsing JSON, per facilitare l'analisi del malformato.
            *   Salvare l'output raw completo da GPT-4o in un file di testo temporaneo (`vision_output_raw_[timestamp].json`) per ogni chiamata API, come artefatto tangibile per il debugging.
            *   Aggiungere un messaggio di log `INFO` dopo aver ricevuto la risposta dall'API di Visione ma prima del parsing JSON, ad esempio: `Ricevuta risposta dall'API Visione, elaborazione dell'output...`

    *   **Giustificazione della Scelta:**
        *   Questa strategia combina il vantaggio di stabilire uno schema dati coerente fin dall'inizio (Master Header) con l'efficienza e la scalabilità dell'elaborazione pagina per pagina asincrona, superando efficacemente il limite di 20 immagini delle API di Visione.
        *   Riduce drasticamente la complessità del post-processing rispetto ad altri approcci di chunking, in quanto l'AI è già guidata a produrre dati conformi allo schema dell'header.
        *   Sfrutta la capacità nativa di `gpt-4o` di comprendere layout visivi complessi direttamente dalle immagini, superando i limiti di `pdfplumber` nel rilevare strutture ambigue.

    *   **Impatto:**
        *   **Costi:** Aumento stimato dei costi di 20-50x rispetto alla soluzione precedente (dovuto all'utilizzo di `gpt-4o` e Vision API), ma ottimizzato rispetto a un'elaborazione completamente sequenziale.
        *   **Latenza:** Aumento della latenza totale per documenti lunghi rispetto all'Assistant API, ma mitigato in modo significativo dall'elaborazione asincrona e parallela delle pagine.
        *   **Dipendenze:** Necessità di gestire la dipendenza `PyMuPDF` (e le sue eventuali dipendenze di sistema come `libgl1`) nel `dockerfile`.
        *   **Robustezza:** Maggiore robustezza nell'estrazione dei dati e nella gestione di documenti multi-pagina.
        *   **Compatibilità:** La logica del Normalizer non richiederà modifiche, in quanto riceverà un input XLSX di qualità superiore.
