# Preventivatore AI (RAG-Based)

Tool automatizzato per la generazione di computi metrici estimativi basato su AI e Ricerca Vettoriale.

## 🚀 Funzionalità
* **Bulk Ingestion:** Indicizzazione automatica di storici preventivi Excel.
* **Vector Search:** Ricerca semantica degli articoli (Embedding OpenAI).
* **AI Validation:** GPT-4o valida tecnicamente i match vettoriali.
* **RDO Normalizer:** Agente AI per convertire input clienti disordinati in formati standard.

## 🛠 Installazione

1.  Clona la repo.
2.  Crea un file `.env` con la tua `OPENAI_API_KEY`.
3.  Installa le dipendenze:
    ```bash
    pip install -r requirements.txt
    ```

## 📂 Struttura
* `step0_assistant_normalizer_v3.py`: Normalizzatore input cliente (XLS/PDF -> XLSX Clean).
* `step16_incremental_master.py`: Ingestion dei preventivi storici nel DB.
* `generate_quote.py`: Motore di generazione preventivi.