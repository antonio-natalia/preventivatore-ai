from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class QuoteComponentItem:
    """Rappresenta una riga 'FIGLIO' nel preventivo."""
    description: str
    unit_quantity: float # qty_coefficient nel DB
    unit_price: float
    type: str # MAT o MAN
    
    # Calcolati
    total_price: float = 0.0 # unit_price * unit_quantity * parent_qty
    
@dataclass
class QuoteLineItem:
    """Rappresenta una riga 'PADRE' nel preventivo."""
    # Input
    row_index: int
    codice_input: str
    description_input: str
    quantity_input: float
    um_input: str
    
    # Prezzi Input (RDO)
    p_mat_rdo: float = 0.0
    p_man_rdo: float = 0.0
    
    # Match DB
    match_id: Optional[int] = None
    match_description: str = ""
    source_file: str = ""
    
    # Prezzi DB (Unitari)
    p_unit_mat_db: float = 0.0
    p_unit_man_db: float = 0.0
    
    # Logica AI
    status: str = "NOMATCH" # MATCH, WARNING, NOMATCH
    reasoning: str = ""
    
    # Gerarchia
    children: List[QuoteComponentItem] = field(default_factory=list)
    
    @property
    def p_unit_tot_db(self) -> float:
        return self.p_unit_mat_db + self.p_unit_man_db

    @property
    def p_unit_tot_rdo(self) -> float:
        return self.p_mat_rdo + self.p_man_rdo

@dataclass
class QuoteResult:
    items: List[QuoteLineItem]
    stats: dict