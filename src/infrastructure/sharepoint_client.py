import os
import httpx
import msal
import logging
from src.config import settings
from src.infrastructure.telemetry import track_phase

logger = logging.getLogger(__name__)

class SharePointClient:
    """
    Gestisce l'interazione con Microsoft Graph API per SharePoint Online.
    Utilizza un Service Principal (App-Only Auth) per accedere ai file.
    """
    
    GRAPH_API_BASE_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self):
        self.tenant_id = settings.SHAREPOINT_TENANT_ID
        self.client_id = settings.SHAREPOINT_CLIENT_ID
        # Retrieve secret value from SecretStr
        self.client_secret = settings.SHAREPOINT_CLIENT_SECRET.get_secret_value()
        self.site_name = settings.SHAREPOINT_SITE_NAME
        
        self.site_id = None # Cache for site ID
        self._token = None  # Cache for access token

        # MSAL Application
        self.app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )

    def _get_access_token(self) -> str:
        """Ottiene un token di accesso valido per MS Graph."""
        if self._token:
            # TODO: Implementare controllo scadenza token se necessario per long-running jobs
            return self._token

        logger.info("Acquisizione token OAuth2 per SharePoint...")
        result = self.app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

        if "access_token" in result:
            self._token = result["access_token"]
            return self._token
        else:
            error_desc = result.get("error_description", "Unknown Error")
            logger.error(f"Errore acquisizione token: {result.get('error')}")
            raise Exception(f"Failed to acquire token: {error_desc}")

    def _get_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json"
        }

    def _get_site_id(self) -> str:
        """Risolve il nome del sito nel Site ID univoco di Graph."""
        if self.site_id:
            return self.site_id

        # Graph API per cercare siti per nome (hostname:/sites/sitename)
        # Nota: Questo assume una struttura URL standard. Se fallisce, potrebbe servire una ricerca.
        # Strategia alternativa robusta: Cerca per keyword
        logger.info(f"Risoluzione Site ID per '{self.site_name}'...")
        
        # Opzione 1: Search (più flessibile)
        search_url = f"{self.GRAPH_API_BASE_URL}/sites?search={self.site_name}"
        
        with httpx.Client() as client:
            resp = client.get(search_url, headers=self._get_headers())
            resp.raise_for_status()
            data = resp.json()
            
            if not data.get("value"):
                raise Exception(f"Site '{self.site_name}' not found via Graph API.")
            
            # Prende il primo risultato (assumendo sia quello corretto)
            self.site_id = data["value"][0]["id"]
            logger.info(f"Site ID risolto: {self.site_id}")
            return self.site_id

    @track_phase(phase_name="sharepoint_download")
    def download_file_by_path(self, sharepoint_path: str, local_dest_path: str):
        """
        Scarica un file dato il suo percorso relativo al sito/document library.
        
        :param sharepoint_path: Percorso completo es. "/sites/NomeSito/Shared Documents/Cartella/File.xlsx"
                                o percorso relativo alla root del drive.
        :param local_dest_path: Dove salvare il file localmente.
        """
        site_id = self._get_site_id()
        
        # Logica per gestire il path. Graph API accetta percorsi relativi al drive di default o percorsi assoluti.
        # Se il path inizia con /sites/..., proviamo a estrarre la parte relativa.
        # Approccio più sicuro: Usare l'endpoint getByPath sul drive di default (Documenti)
        
        # Pulizia path: Rimuovi prefisso sito se presente, o gestisci come path relativo alla library "Documenti"
        # Esempio Path Input: "/Shared Documents/LTE Preventivazione/..."
        # Endpoint: /sites/{site-id}/drive/root:/{path-relative-to-root}:/content
        
        # Rimuoviamo "/Shared Documents/" o simile se presente all'inizio per ottenere il path relativo alla root del drive
        # Nota: Questo è un punto fragile. Assumiamo che i file siano nella Document Library di default ("Documents").
        
        clean_path = sharepoint_path
        if sharepoint_path.startswith("/Shared Documents/"):
             clean_path = sharepoint_path.replace("/Shared Documents/", "", 1)
        
        # Codifica URL del path
        from urllib.parse import quote
        encoded_path = quote(clean_path)
        
        # Costruzione URL Download
        # Sintassi: GET /sites/{site-id}/drive/root:/{path}:/content
        download_url = f"{self.GRAPH_API_BASE_URL}/sites/{site_id}/drive/root:/{encoded_path}:/content"
        
        logger.info(f"Scaricando da: {clean_path} ...")
        
        with httpx.Client(follow_redirects=True) as client:
            with client.stream("GET", download_url, headers=self._get_headers()) as resp:
                if resp.status_code == 404:
                    raise FileNotFoundError(f"File non trovato su SharePoint: {clean_path}")
                resp.raise_for_status()
                
                with open(local_dest_path, "wb") as f:
                    for chunk in resp.iter_bytes():
                        f.write(chunk)
                        
        logger.info(f"Download completato: {local_dest_path}")

    @track_phase(phase_name="sharepoint_upload")
    def upload_file(self, local_file_path: str, remote_folder_path: str, remote_file_name: str):
        """
        Carica un file su SharePoint.
        
        :param local_file_path: Path del file locale da caricare.
        :param remote_folder_path: Cartella di destinazione su SharePoint (es. "/LTE Preventivazione/OUTPUT").
        :param remote_file_name: Nome del file da salvare su SharePoint.
        """
        site_id = self._get_site_id()
        
        # Pulizia path remoto (simile al download)
        clean_folder_path = remote_folder_path
        if remote_folder_path.startswith("/Shared Documents/"):
             clean_folder_path = remote_folder_path.replace("/Shared Documents/", "", 1)
        
        # Assicuriamoci che non inizi con /
        if clean_folder_path.startswith("/"):
            clean_folder_path = clean_folder_path[1:]

        from urllib.parse import quote
        encoded_path = quote(f"{clean_folder_path}/{remote_file_name}")
        
        # Endpoint: PUT /sites/{site-id}/drive/root:/{parent-path}/{filename}:/content
        upload_url = f"{self.GRAPH_API_BASE_URL}/sites/{site_id}/drive/root:/{encoded_path}:/content"
        
        logger.info(f"Caricamento in corso su: {clean_folder_path}/{remote_file_name} ...")
        
        with open(local_file_path, "rb") as f:
            file_content = f.read()
            
        with httpx.Client() as client:
            resp = client.put(upload_url, headers=self._get_headers(), content=file_content)
            resp.raise_for_status()
            
        logger.info("Upload completato con successo.")
