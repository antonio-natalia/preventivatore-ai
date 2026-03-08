import time
import asyncio
import json
from typing import List, Dict, Optional, Type
from pydantic import BaseModel # Importa BaseModel per i tipi di schema
from openai import OpenAI, AsyncOpenAI
from src.config import settings
from src.infrastructure.telemetry import log_metric, setup_telemetry

# Inizializza client (Singleton)
client = OpenAI(api_key=settings.OPENAI_API_KEY)
aclient = AsyncOpenAI(api_key=settings.OPENAI_API_KEY) # Client asincrono
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

async def get_async_chat_completion_json(messages: List[Dict], model: str = "gpt-4o", temperature: float = 0.0, retries: int = 3, response_model: Optional[Type[BaseModel]] = None) -> Optional[Dict]:
    """
    Wrapper asincrono per chat completion (JSON Mode o Structured Output) con telemetria e retry/backoff.
    Accetta un elenco di messaggi per supportare la Vision API e un response_model per output strutturato.
    """
    start = time.time()
    for attempt in range(retries):
        try:
            logger.debug(f"Calling OpenAI ChatCompletion (attempt {attempt+1}/{retries})...", extra={"model": model, "messages_len": len(messages)})
            
            response_format = {"type": "json_object"}
            if response_model:
                # Costruisce lo schema JSON per structured_output
                response_format["schema"] = response_model.model_json_schema()

            response = await aclient.chat.completions.create( # Usa aclient asincrono
                model=model,
                messages=messages,
                response_format=response_format,
                #temperature=temperature
            )
            
            duration = time.time() - start
            usage = response.usage
            
            log_metric("openai_api_duration", duration, {"operation": "async_chat_completion", "model": model})
            log_metric("openai_tokens_input", usage.prompt_tokens, {"operation": "async_chat_completion"})
            log_metric("openai_tokens_output", usage.completion_tokens, {"operation": "async_chat_completion"})
            
            content = response.choices[0].message.content
            # Pulizia markdown json
            if content.startswith("```json"): content = content[7:]
            if content.endswith("```"): content = content[:-3]
            
            json_data = json.loads(content)
            
            # Se è stato fornito un response_model, proviamo a validare e istanziare
            if response_model:
                try:
                    return response_model(**json_data).model_dump() # Restituisce dict
                except Exception as pydantic_err:
                    logger.error(f"Pydantic validation failed for Structured Output: {pydantic_err}. Data: {content[:500]}...")
                    # Rilancia l'errore di validazione per una gestione specifica
                    raise ValueError(f"Structured Output Validation Error: {pydantic_err}") from pydantic_err
            
            return json_data

        except Exception as e:
            logger.error(f"OpenAI Async Chat Error (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt) # Backoff esponenziale
            else:
                raise # Rilancia l'eccezione dopo l'ultimo tentativo
    return None # Non dovrebbe accadere con raise finale

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
