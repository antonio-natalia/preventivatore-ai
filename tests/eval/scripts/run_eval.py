from openai import OpenAI
import argparse
import yaml
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# Inizializza il client OpenAI. Assicurati che OPENAI_API_KEY sia impostata come variabile d'ambiente.
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def load_config(config_path: str) -> dict:
    """Carica la configurazione dal file YAML."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def run_openai_eval(eval_id: str, config: dict, dataset_file_id: str) -> str:
    """
    Avvia un'esecuzione (run) per una valutazione (eval) specifica, configurando il modello target e il prompt.
    :param eval_id: L'ID della valutazione da eseguire.
    :param config: Dizionario di configurazione caricato da YAML.
    :param dataset_file_id: L'ID del file JSONL del dataset caricato su OpenAI.
    :return: L'ID dell'esecuzione (run) creata.
    """
    target_model = config["TARGET_MODEL"]
    
    # Parametri di sampling, se presenti nella config. Default vuoto.
    sampling_params = config.get("SAMPLING_PARAMS", {})

    print(f"Avvio dell'esecuzione per l'eval ID: {eval_id} con modello target {target_model}...")
    run = client.evals.runs.create(
        eval_id=eval_id,
        data_source={
            "type": "completions",
            "source": {
                "type": "file_id",
                "id": dataset_file_id
            },
            "model": target_model,
            "input_messages": {
                "type": "item_reference",
                "item_reference": "item.input"
            }
        }
    )
    print(f"Esecuzione avviata. Run ID: {run.id}")
    return run.id

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Avvia un'esecuzione (run) per una valutazione (eval) su OpenAI.")
    parser.add_argument("eval_id", type=str, help="L'ID della valutazione da eseguire.")
    parser.add_argument("--config", type=str, default="tests/eval/configs/eval_gpt_5_mini.yaml",
                        help="Percorso al file di configurazione YAML.")
    parser.add_argument("--dataset_file_id", type=str, required=True,
                        help="L'ID del file JSONL del dataset caricato su OpenAI.")
    args = parser.parse_args()

    config = load_config(args.config)

    try:
        run_id = run_openai_eval(args.eval_id, config, args.dataset_file_id)
        print(f"Run ID generato: {run_id}")
    except Exception as e:
        print(f"Errore durante l'avvio dell'esecuzione: {e}")
