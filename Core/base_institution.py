from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Optional
from core.models import Medicamento

class BaseInstitution(ABC):
    """Clase base para todas las instituciones de salud mexicanas"""
    
    def __init__(self, institution_name: str, db_prefix: str):
        self.institution_name = institution_name
        self.db_prefix = db_prefix
        self.last_error = ""
        self.last_stats = {}
    
    @abstractmethod
    def get_data_sources(self) -> Dict[str, str]:
        """Retorna URLs o fuentes de datos específicas de la institución"""
        pass
    
    @abstractmethod
    def parse_institution_data(self) -> List[Medicamento]:
        """Parsea datos específicos de la institución"""
        pass
    
    @abstractmethod
    def normalize_medication_code(self, code: str) -> str:
        """Normaliza códigos específicos de la institución"""
        pass
    
    # Métodos comunes (implementación base)
    def initialize(self) -> Tuple[bool, str]:
        """Inicialización común"""
        try:
            self._init_database()
            return True, f"{self.institution_name}_INIT_SUCCESS"
        except Exception as e:
            self.last_error = str(e)
            return False, f"{self.institution_name}_INIT_ERROR"
    
    def sync_data(self) -> Tuple[bool, str]:
        """Sincronización común"""
        try:
            medications = self.parse_institution_data()
            if not medications:
                return False, f"{self.institution_name}_NO_DATA"
            
            success_count = self._store_medications(medications)
            self.last_stats = {
                'total_processed': len(medications),
                'successfully_stored': success_count
            }
            
            return True, f"{self.institution_name}_SYNC_SUCCESS"
        except Exception as e:
            self.last_error = str(e)
            return False, f"{self.institution_name}_SYNC_ERROR"
    
    def is_ready(self) -> Tuple[bool, str]:
        """Verificación común de estado"""
        # Implementación base común
        pass
    
    def search(self, term: str) -> List[Dict]:
        """Búsqueda común"""
        # Implementación base común
        pass