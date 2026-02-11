from abc import ABC, abstractmethod
from typing import List, Dict, Any
from pydantic import BaseModel, Field

class VoceComputoMetric(BaseModel):
    codice_originale: str = Field(..., description="Codice identificativo voce")
    descrizione_completa: str = Field(..., description="Descrizione tecnica completa")
    quantita: float = Field(..., description="Quantità numerica")
    unita_misura: str = Field(..., description="Unità di misura")
    prezzo_unitario: float = Field(0.0, description="Prezzo unitario")
    prezzo_manodopera: float = Field(0.0, description="Prezzo manodopera")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    reasoning: str = Field(..., description="Logica applicata")

class BaseNormalizer(ABC):
    @abstractmethod
    def normalize(self, file_path: str, **kwargs) -> List[VoceComputoMetric]:
        """
        :param file_path: Percorso file.
        :param kwargs: Opzioni extra (es. scan_mode, max_sample_rows).
        """
        pass