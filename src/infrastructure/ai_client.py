import time
from typing import List
from openai import OpenAI
from src.config import settings
from src.infrastructure.telemetry import log_metric, setup_telemetry

# Inizializza client (Singleton)
client = OpenAI(api_key=settings.OPENAI_API_KEY)
logger = setup_telemetry() # Recupera il logger configurato

def get_embedding(text: str) -> list[float]:
    """
    Wrapper per ottenere embedding con telemetria.
    """
    start = time.time()
    text = text.replace("\n", " ")
    
    try:
        response = client.embeddings.create(input=[text], model="text-embedding-3-small")
        
        # Metriche
        duration = time.time() - start
        tokens = response.usage.total_tokens
        log_metric("openai_api_duration", duration, {"operation": "embedding", "model": "text-embedding-3-small"})
        log_metric("openai_tokens_total", tokens, {"operation": "embedding"})
        
        return response.data[0].embedding
    except Exception as e:
        logger.error(f"OpenAI Embedding Error: {e}")
        raise

def get_chat_completion_json(prompt: str, model: str = "gpt-4o") -> str:
    """
    Wrapper per chat completion (JSON Mode) con telemetria.
    """
    start = time.time()
    
    try:
        # Log Pre-Call (Utile per debuggare timeout)
        logger.debug("Calling OpenAI ChatCompletion...", extra={"model": model, "prompt_len": len(prompt)})
        
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        
        # Metriche
        duration = time.time() - start
        usage = response.usage
        
        log_metric("openai_api_duration", duration, {"operation": "chat_completion", "model": model})
        log_metric("openai_tokens_input", usage.prompt_tokens, {"operation": "chat_completion"})
        log_metric("openai_tokens_output", usage.completion_tokens, {"operation": "chat_completion"})
        
        return response.choices[0].message.content

    except Exception as e:
        logger.error(f"OpenAI Chat Error: {e}")
        raise

def get_embeddings_batch(texts: List[str], model: str = "text-embedding-3-small") -> List[List[float]]:
    """
    Batch embedding con telemetria.
    """
    start = time.time()
    clean_texts = [text.replace("\n", " ") for text in texts]
    
    try:
        response = client.embeddings.create(input=clean_texts, model=model)
        
        duration = time.time() - start
        tokens = response.usage.total_tokens
        
        log_metric("openai_api_duration", duration, {"operation": "batch_embedding", "batch_size": str(len(texts))})
        log_metric("openai_tokens_total", tokens, {"operation": "batch_embedding"})
        
        # Riordina i risultati in base all'indice
        data = sorted(response.data, key=lambda x: x.index)
        return [item.embedding for item in data]
        
    except Exception as e:
        logger.error(f"OpenAI Batch Embedding Error: {e}")
        raise