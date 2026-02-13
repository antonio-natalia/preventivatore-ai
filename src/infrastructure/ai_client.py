from openai import OpenAI
from src.config import settings
from typing import List

# Inizializza client una volta sola
client = OpenAI(api_key=settings.OPENAI_API_KEY)

def get_embedding(text: str) -> list[float]:
    """
    Wrapper per ottenere embedding.
    """
    text = text.replace("\n", " ")
    return client.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding

def get_chat_completion_json(prompt: str, model: str = "gpt-4o") -> str:
    """
    Wrapper per chat completion che forza output JSON.
    Restituisce il contenuto grezzo della risposta (stringa JSON).
    """
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.0
    )
    return response.choices[0].message.content

def get_embeddings_batch(texts: List[str], model: str = "text-embedding-3-small") -> List[List[float]]:
    """
    Richiede embeddings per una lista di testi in una sola chiamata HTTP.
    Ottimizza costi e latenza.
    """
    # Pulizia base come nel metodo singolo
    clean_texts = [text.replace("\n", " ") for text in texts]
    
    # OpenAI permette input come array di stringhe
    response = client.embeddings.create(input=clean_texts, model=model)
    
    # Garantiamo che l'ordine sia preservato (OpenAI solitamente lo fa, ma siamo espliciti)
    # response.data è una lista di oggetti Embedding(embedding=[...], index=0, ...)
    sorted_data = sorted(response.data, key=lambda x: x.index)
    
    return [item.embedding for item in sorted_data]