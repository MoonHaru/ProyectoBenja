from dataclasses import dataclass, asdict
from typing import Dict, Optional
from datetime import datetime

@dataclass
class Medicamento:
    """Modelo unificado para medicamentos de todas las instituciones"""
    
    # Identificadores universales
    institution: str                     # IMSS, ISSSTE, SEDENA, etc.
    code: str                           # Código original de la institución
    normalized_code: str = ""           # Código normalizado
    
    # Información básica universal
    description: str = ""
    generic_name: str = ""
    active_ingredient: str = ""
    presentation: str = ""
    concentration: str = ""
    
    # Clasificación universal
    therapeutic_group: str = ""
    atc_code: str = ""                  # Código ATC internacional
    
    # Campos específicos por institución (JSON)
    institution_specific: str = ""      # JSON con campos únicos
    
    # Metadatos
    source: str = ""
    last_updated: str = ""
    status: str = "active"
    
    def __post_init__(self):
        if not self.last_updated:
            self.last_updated = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        return asdict(self)