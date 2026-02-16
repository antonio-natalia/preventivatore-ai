# Usa un'immagine base leggera di Python 3.10
FROM python:3.10-slim

# Imposta variabili d'ambiente per ottimizzare Python in Docker
# PYTHONDONTWRITEBYTECODE: Evita la creazione di file .pyc
# PYTHONUNBUFFERED: Forza lo stdout a essere inviato direttamente ai log (fondamentale per Azure Logs)
# PYTHONPATH: Aggiunge /app al path per facilitare gli import dei moduli
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Imposta la directory di lavoro
WORKDIR /app

# Installa dipendenze di sistema minime necessarie per compilare alcune librerie Python
# (es. gcc per estensioni C, se necessario). Pulisce la cache apt per ridurre le dimensioni.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia prima solo il requirements.txt per sfruttare la cache dei layer di Docker
COPY requirements.txt .

# Installa le dipendenze Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente
COPY src ./src

# Crea un utente non-root per la sicurezza e cambia i permessi
RUN useradd -m appuser && chown -R appuser /app

# Passa all'utente non-root
USER appuser

# Nessun CMD o ENTRYPOINT fisso.
# Il comando verrà passato dinamicamente dal Job di Azure Container Apps.
# Esempio di comando che verrà lanciato: python src/interfaces/cli.py ingest ...