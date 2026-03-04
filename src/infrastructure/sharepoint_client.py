import os
import httpx
import msal
import logging
import codecs # Importa il modulo codecs
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
        self.main_drive_id = None # Cache for the main document library drive ID
        self._token = None  # Cache for access token

        # MSAL Application
        self.app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )
        # Pre-resolve site ID and main drive ID during initialization
        self._get_site_id() # Assicurati che il site_id sia risolto e memorizzato nella cache
        self._get_drive_id("Documenti") # Risolvi e memorizza nella cache l'ID del drive principale per la libreria predefinita

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

    def _get_drive_id(self, drive_name: str) -> str:
        """Risolve il nome del drive (libreria documenti) nel suo ID univoco per il sito corrente."""
        if self.main_drive_id:
            return self.main_drive_id

        site_id = self._get_site_id() # Assicurati che il site_id sia risolto
        logger.info(f"Risoluzione Drive ID per '{drive_name}' sul sito '{site_id}'...")

        drives_url = f"{self.GRAPH_API_BASE_URL}/sites/{site_id}/drives"

        with httpx.Client() as client:
            resp = client.get(drives_url, headers=self._get_headers())
            resp.raise_for_status()
            data = resp.json()

            for drive in data.get("value", []):
                if drive.get("name") == drive_name:
                    self.main_drive_id = drive["id"]
                    logger.info(f"Drive ID risolto per '{drive_name}': {self.main_drive_id}")
                    return self.main_drive_id
            
            raise Exception(f"Drive (document library) '{drive_name}' not found for site '{self.site_name}'.")

    @track_phase(phase_name="sharepoint_download")
    def download_file_by_path(self, sharepoint_path: str, local_dest_path: str):
        """
        Scarica un file dato il suo percorso relativo al sito/document library.
        
        :param sharepoint_path: Percorso completo es. "/sites/NomeSito/Shared Documents/Cartella/File.xlsx"
                                o percorso relativo alla root del drive.
        :param local_dest_path: Dove salvare il file localmente.
        """
        # Il site_id e il drive_id principale ("Documenti condivisi") vengono pre-risolti nel costruttore.
        drive_id = self.main_drive_id
        if not drive_id:
            raise Exception("Main SharePoint drive ID not resolved. Ensure SharePointClient is initialized correctly.")

        # Pulizia path: La `sharepoint_path` dal trigger Power Automate è una URL server-relative completa,
        # es. "/sites/NOME_SITO/Documenti condivisi/Cartella/File.xlsx".
        # L'API Graph `drives/{drive-id}/root:/{path}:/content` si aspetta un path relativo alla root della libreria documenti.
        
        # Elenco dei nomi comuni per le librerie documenti di default
        # Si attende che la `sharepoint_path` possa iniziare direttamente con il nome della libreria,
        # senza una barra iniziale, o con una barra.
        doc_library_markers = ["Documenti condivisi/", "Shared Documents/"]
        
        # Inizializza il percorso pulito con il path originale
        clean_path = sharepoint_path
        
        # Rimuove una eventuale barra iniziale dal percorso per uniformare la ricerca dei marcatori
        if clean_path.startswith('/'):
            clean_path = clean_path[1:]

        # Cerca e rimuovi il prefisso della libreria documenti nel percorso
        for marker in doc_library_markers:
            if clean_path.startswith(marker):
                # Rimuovi il marker dall'inizio del path
                clean_path = clean_path[len(marker):]
                break # Esci una volta che il marker è stato trovato e rimosso
        else:
            # Se nessun marker conosciuto è stato trovato, registra un avviso.
            # Il path potrebbe essere già nella forma corretta o usare un nome libreria non standard.
            logger.warning(f"Nessun nome di libreria documenti di default riconosciuto nel path: '{clean_path}'. Il path verrà usato 'così com'è'.")
            
        
        # Codifica URL del path
        # Decodifica le sequenze di escape Unicode letterali (es. "\u2013" diventa "–")
        # Questo è necessario perché l'argomento "--file-path" sembra passare la stringa con escape letterali.
        try:
            clean_path = codecs.decode(clean_path, 'unicode_escape')
        except UnicodeDecodeError as e:
            logger.warning(f"Errore durante la decodifica unicode_escape del path '{clean_path}': {e}. Il path verrà usato così com'è.")

        from urllib.parse import quote
        encoded_path = quote(clean_path)
        
        # Costruzione URL Download
        # Sintassi: GET /drives/{drive-id}/root:/{path}:/content
        download_url = f"{self.GRAPH_API_BASE_URL}/drives/{drive_id}/root:/{encoded_path}:/content"
        
        logger.info(f"Scaricando da: {clean_path} (Drive ID: {drive_id}) ...")
        
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
        # Il drive_id principale ("Documenti condivisi") viene pre-risolto nel costruttore.
        drive_id = self.main_drive_id
        if not drive_id:
            raise Exception("Main SharePoint drive ID not resolved. Ensure SharePointClient is initialized correctly.")
        
        clean_folder_path = remote_folder_path
        
        # Rimuoviamo il prefisso della libreria documenti se presente, come per il download.
        doc_library_markers = ["Documenti condivisi/", "Shared Documents/"]
        
        # Rimuove una eventuale barra iniziale dal percorso per uniformare la ricerca dei marcatori
        if clean_folder_path.startswith('/'):
            clean_folder_path = clean_folder_path[1:]

        for marker in doc_library_markers:
            if clean_folder_path.startswith(marker):
                clean_folder_path = clean_folder_path[len(marker):]
                break
        
        # Assicuriamoci che non inizi con / dopo la pulizia del marker
        if clean_folder_path.startswith("/"):
            clean_folder_path = clean_folder_path[1:]

        # Combina e decodifica le sequenze di escape Unicode letterali per il path completo
        full_remote_path = f"{clean_folder_path}/{remote_file_name}"
        try:
            full_remote_path = codecs.decode(full_remote_path, 'unicode_escape')
        except UnicodeDecodeError as e:
            logger.warning(f"Errore durante la decodifica unicode_escape del path di upload '{full_remote_path}': {e}. Il path verrà usato così com'è.")

        from urllib.parse import quote
        encoded_path = quote(full_remote_path)
        
        # Endpoint: PUT /drives/{drive-id}/root:/{parent-path}/{filename}:/content
        upload_url = f"{self.GRAPH_API_BASE_URL}/drives/{drive_id}/root:/{encoded_path}:/content"
        
        logger.info(f"Caricamento in corso su: {clean_folder_path}/{remote_file_name} (Drive ID: {drive_id}) ...")
        
        with open(local_file_path, "rb") as f:
            file_content = f.read()
            
        with httpx.Client() as client:
            resp = client.put(upload_url, headers=self._get_headers(), content=file_content)
            resp.raise_for_status()
            
        logger.info("Upload completato con successo.")
