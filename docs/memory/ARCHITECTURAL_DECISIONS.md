# Architectural Decisions

## Tech Stack & Versions

*   **Python:** 3.10 (as defined in the Dockerfile)
*   **SQLite:** Version bundled with the base image
*   **sqlite-vec:** 0.1.6 (for vector embeddings)
*   **OpenAI API:** Utilized via the `openai` library (version 2.14.0)
*   **pandas:** 2.3.3
*   **xlsxwriter:** 3.2.9
*   **pdfplumber:** 0.11.8

## Configuration Strategy

The application employs a hybrid configuration strategy:

*   **Environment Variables:** Take precedence over default values. This is crucial for cloud deployments where configurations are injected at runtime.
*   **Local Defaults:** Defined within `src/config.py` to provide sensible defaults for local development and testing.

This approach allows for flexibility across different environments while ensuring a functional application out-of-the-box.

## Database Strategy

*   **SQLite:** Chosen for its simplicity and file-based nature, suitable for a modular monolith architecture.
*   **`sqlite-vec` Extension:** Enables vector similarity searches for semantic matching of catalog items.
*   **Directory Auto-Creation:** The `get_db_connection` function in `src/infrastructure/database.py` automatically creates the database directory if it doesn't exist, simplifying deployment and setup.
*   **WAL Mode:** Enabled to improve concurrency, especially important if the TUI and ingestion processes run concurrently.

## Deployment Constraints

*   **Azure Container Apps:** The application is designed to be deployed as an Azure Container App.
*   **Dynamic Command Injection:** The `Dockerfile` does not specify a fixed `CMD` or `ENTRYPOINT`. Instead, the command is passed dynamically by the Azure Container Apps job.
*   **Volume Mount:** Persistent data (e.g., the SQLite database) should be stored on a mounted volume (e.g., `/mnt/data`) to survive container restarts.
*   **Environment Variables:** Azure Container Apps should be configured with the necessary environment variables (e.g., `OPENAI_API_KEY`, `DB_FILE`).
*   **Working Directory:** The application expects to be run from the `/app` working directory.
*   **User Context:** The application runs as a non-root user (`appuser`) for security best practices.
*   **Logging:** Relies on `stdout` for logging, which is captured by Azure Container Apps. The `PYTHONUNBUFFERED=1` environment variable ensures that logs are flushed immediately.
````
docs/memory/BUSINESS_FLOWS.md
````
<<<<<<< SEARCH
