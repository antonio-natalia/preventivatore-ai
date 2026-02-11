from openai import OpenAI
from src.config import settings

# Inizializza client una volta sola
client = OpenAI(api_key=settings.OPENAI_API_KEY)

def get_embedding(text: str) -> list[float]:
    """
    Wrapper per ottenere embedding.
    Rimuove newline come da best practice originale.
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