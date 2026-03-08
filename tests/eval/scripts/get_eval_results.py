from openai import OpenAI
import argparse
import time
import json
import os
import yaml
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def load_config(config_path: str) -> dict:
    """Carica la configurazione dal file YAML."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_eval_results(eval_id: str, run_id: str, results_dir: str):
    """
    Recupera e stampa i risultati di una specifica esecuzione (run) di valutazione.
    Salva i risultati completi in un file JSON.
    :param eval_id: L'ID della valutazione a cui appartiene il run.
    :param run_id: L'ID dell'esecuzione (run) di cui recuperare i risultati.
    :param results_dir: Directory dove salvare i risultati JSON.
    """
    print(f"Recupero dei risultati per Run ID: {run_id} (Eval ID: {eval_id})...")

    # ------------------------------------------------------------------------------
    # FASE 1: POLLING DELLO STATO DEL RUN
    # ------------------------------------------------------------------------------
    while True:
        run = client.evals.runs.retrieve(run_id=run_id, eval_id=eval_id)

        if run.status in ("completed", "failed"):
            break

        time.sleep(5)

    if run.status == "failed":
        raise RuntimeError(f"Il Run Evals {run_id} è fallito a livello server.")


    # ------------------------------------------------------------------------------
    # FASE 2: ESTRAZIONE DEGLI OUTPUT ITEMS (PUNTO CRITICO)
    # ------------------------------------------------------------------------------
    risultati_grezzi = client.evals.runs.output_items.list(
        run_id=run_id,
        eval_id=eval_id
    )

    report_finale = []

    # ------------------------------------------------------------------------------
    # FASE 3: PARSING DEI CAMPI UFFICIALI (EVITARE ERRORI DI CHIAVE)
    # ------------------------------------------------------------------------------
    for item in risultati_grezzi:
        risposta_modello_raw = "N/A"
        if hasattr(item.sample, 'output') and len(item.sample.output) > 0:
             risposta_modello_raw = item.sample.output[0].content

        # Tentativo di leggere la risposta del modello come JSON strutturato
        risposta_modello_parsed = None
        try:
            risposta_modello_parsed = json.loads(risposta_modello_raw)
        except json.JSONDecodeError:
            # Se la decodifica fallisce, manteniamo la stringa originale
            risposta_modello_parsed = risposta_modello_raw
        
        voto_finale = None
        if item.results and len(item.results) > 0:
            grader_result = item.results[0]
            voto_finale = getattr(grader_result, 'score', getattr(grader_result, 'value', None))

        report_finale.append({
            "id_riga": getattr(item, 'datasource_item_id', 'Sconosciuto'),
            "risposta_modello": risposta_modello_parsed,
            "valutazione_grader": voto_finale
        })

    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
        print(f"Creata directory risultati: {results_dir}")

    output_file_path = os.path.join(results_dir, f"eval_run_results_{run_id}.json")

    with open(output_file_path, "w", encoding="utf-8") as f:
        json.dump(report_finale, f, indent=2, ensure_ascii=False)
    
    print(f"Risultati completi salvati in: {output_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recupera i risultati di un'esecuzione (run) di valutazione da OpenAI.")
    parser.add_argument("eval_id", type=str, help="L'ID della valutazione a cui appartiene il run.")
    parser.add_argument("run_id", type=str, help="L'ID dell'esecuzione (run) di cui recuperare i risultati.")
    parser.add_argument("--config", type=str, default="tests/eval/configs/eval_gpt_5_mini.yaml",
                        help="Percorso al file di configurazione YAML.")
    args = parser.parse_args()

    config = load_config(args.config)
    results_dir = config["EVAL_RESULTS_DIR"]

    try:
        get_eval_results(args.eval_id, args.run_id, results_dir)
    except Exception as e:
        print(f"Errore durante il recupero dei risultati dell'eval: {e}")
