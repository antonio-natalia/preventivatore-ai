from openai import OpenAI
import yaml
import os
import argparse
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def load_config(config_path: str) -> dict:
    """Carica la configurazione dal file YAML."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def create_openai_eval(config: dict) -> str:
    """
    Crea una nuova valutazione (eval) su OpenAI.
    :param config: Dizionario di configurazione caricato da YAML.
    :return: L'ID della valutazione creata.
    """
    judge_model = config["JUDGE_MODEL"]
    
    grader_prompt_path = config["JUDGE_PROMPT_PATH"]

    if not os.path.exists(grader_prompt_path):
        raise FileNotFoundError(f"Prompt del grader non trovato: {grader_prompt_path}")

    with open(grader_prompt_path, "r", encoding="utf-8") as f:
        grader_prompt_content = f.read()

    print(f"Creazione dell'eval su OpenAI...")
    eval_obj = client.evals.create(
        name="boq_pdf_extraction_eval", # Nome fisso come da esempio
        data_source_config={
            "type": "custom",
            "item_schema": {
                "type": "object",
                "properties": {
                    "input": { # 'input' ora è un array di messaggi
                        "type": "array"
                    },
                    "ideal": { # Aggiunto 'ideal' come oggetto, dato che contiene la struttura attesa
                        "type": "object"
                    }
                },
                "required": ["input", "ideal"] # I campi richiesti sono 'input' e 'ideal'
            },
            "include_sample_schema": True 
        },
        testing_criteria=[
            {
                "type": "label_model",
                "name": "boq_extraction_quality",
                "model": judge_model,
                "input": [
                    {
                        "role": "developer",
                        "content": grader_prompt_content
                    },
                    {
                        "role": "user",
                        # Riferimento a item.ideal e sample.output_text
                        "content": "Output del modello: {{sample.output_text}}\nGround truth: {{item.ideal}}"
                    }
                ],
                "labels": config.get("GRADER_LABELS"),
                "passing_labels": config.get("GRADER_PASSING_LABELS")
            }
        ]
    )

    # Validazione che i campi obbligatori del grader siano stati forniti
    if not eval_obj.testing_criteria[0].labels:
        raise ValueError("Manca il parametro 'GRADER_LABELS' nel file di configurazione. Si prega di aggiungerlo.")
    if not eval_obj.testing_criteria[0].passing_labels:
        raise ValueError("Manca il parametro 'GRADER_PASSING_LABELS' nel file di configurazione. Si prega di aggiungerlo.")

    print(f"Eval creata. ID: {eval_obj.id}")
    return eval_obj.id

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crea una valutazione (eval) su OpenAI.")
    parser.add_argument("--config", type=str, default="tests/eval/configs/eval_gpt_5_mini.yaml",
                        help="Percorso al file di configurazione YAML.")
    # L'argomento --dataset_file_id è stato rimosso in quanto non necessario per la creazione dell'eval.
    args = parser.parse_args()

    config = load_config(args.config)

    try:
        eval_id = create_openai_eval(config) # Chiamata aggiornata senza dataset_file_id
        print(f"Eval ID generato: {eval_id}")
    except Exception as e:
        print(f"Errore durante la creazione dell'eval: {e}")
