"""
Módulo IMSS Refactorizado - Limpio y Simple
Retorna solo True/False con códigos de error
"""

import requests
import PyPDF2
import re
import json
import csv
import sqlite3
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import io
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

@dataclass
class MedicamentoIMSS:
    """Estructura de datos para un medicamento del IMSS"""
    # Identificadores
    clave: str
    clave_normalizada: str = ""
    
    # Información básica
    descripcion: str = ""
    nombre_generico: str = ""
    principio_activo_normalizado: str = ""
    presentacion: str = ""
    concentracion: str = ""
    
    # Clasificación
    grupo_terapeutico: str = ""
    subgrupo_terapeutico: str = ""
    categoria_medicamento: str = ""
    
    # Información clínica
    indicaciones: str = ""
    via_administracion: str = ""
    dosis_adultos: str = ""
    dosis_pediatrica: str = ""
    dosis_maxima: str = ""
    
    # Información de seguridad
    riesgo_embarazo: str = ""
    efectos_adversos: str = ""
    contraindicaciones: str = ""
    precauciones: str = ""
    interacciones: str = ""
    
    # Información farmacológica
    generalidades: str = ""
    mecanismo_accion: str = ""
    farmacocinetica: str = ""
    
    # Metadatos
    fuente: str = "IMSS"
    fecha_actualizacion: str = ""
    estado: str = "activo"
    
    def __post_init__(self):
        self.clave_normalizada = self.clave.replace(".", "").replace("-", "").upper()
        if not self.fecha_actualizacion:
            self.fecha_actualizacion = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        return asdict(self)

class IMSSModule:
    """Módulo IMSS simplificado con interfaz limpia"""
    
    def __init__(self, db_path: str = "imss_medicamentos.db"):
        self.db_path = Path(db_path)
        self.urls = {
            'catalogo_principal': 'https://www.imss.gob.mx/sites/all/statics/pdf/cuadros-basicos/CBM.pdf',
            'catalogo_ii': 'https://www.imss.gob.mx/sites/all/statics/pdf/cuadros-basicos/Listado-medicamentos-Catalogo-II.pdf'
        }
        
        # Obtener estructura de campos
        sample_med = MedicamentoIMSS(clave="000.000.0000.00")
        self.field_names = list(sample_med.to_dict().keys())
        self.field_count = len(self.field_names)
        
        self.last_error = ""
        self.last_stats = {}
    
    def initialize(self) -> Tuple[bool, str]:
        """
        Inicializa el módulo IMSS
        Returns:
            (success: bool, error_code: str)
        """
        try:
            self._init_database()
            self._init_optimizer()
            return True, "IMSS_INIT_SUCCESS"
        except Exception as e:
            self.last_error = str(e)
            return False, "IMSS_INIT_ERROR"
    
    def sync_data(self) -> Tuple[bool, str]:
        """
        Sincroniza datos de todas las fuentes IMSS
        Returns:
            (success: bool, error_code: str)
        """
        try:
            logger.info("Iniciando sincronización IMSS...")
            
            # Procesar PDFs
            medicamentos = self._process_all_catalogs()
            if not medicamentos:
                return False, "IMSS_NO_DATA_FOUND"
            
            # Almacenar en base de datos
            success_count = 0
            for medicamento in medicamentos:
                if self._add_medication(medicamento):
                    success_count += 1
            
            # Configurar optimizaciones
            opt_success = self._setup_optimizations()
            
            # Actualizar estadísticas
            self.last_stats = {
                'total_processed': len(medicamentos),
                'successfully_added': success_count,
                'errors': len(medicamentos) - success_count,
                'optimization_enabled': opt_success
            }
            
            if success_count == 0:
                return False, "IMSS_NO_MEDICATIONS_SAVED"
            
            logger.info(f"IMSS sincronización completada: {success_count}/{len(medicamentos)}")
            return True, "IMSS_SYNC_SUCCESS"
            
        except Exception as e:
            self.last_error = str(e)
            return False, "IMSS_SYNC_ERROR"
    
    def is_ready(self) -> Tuple[bool, str]:
        """
        Verifica si el módulo está listo para usar
        Returns:
            (ready: bool, status_code: str)
        """
        try:
            if not self.db_path.exists():
                return False, "IMSS_NO_DATABASE"
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Verificar que tiene datos
            cursor.execute("SELECT COUNT(*) FROM medicamentos")
            total_meds = cursor.fetchone()[0]
            
            if total_meds == 0:
                conn.close()
                return False, "IMSS_EMPTY_DATABASE"
            
            # Verificar normalización
            cursor.execute("SELECT name FROM pragma_table_info('medicamentos') WHERE name = 'principio_activo_normalizado'")
            has_normalization = cursor.fetchone() is not None
            
            if has_normalization:
                cursor.execute("SELECT COUNT(*) FROM medicamentos WHERE principio_activo_normalizado IS NOT NULL AND principio_activo_normalizado != ''")
                normalized_count = cursor.fetchone()[0]
                normalization_percent = (normalized_count / total_meds * 100) if total_meds > 0 else 0
            else:
                normalization_percent = 0
            
            conn.close()
            
            if normalization_percent > 50:
                return True, "IMSS_READY"
            else:
                return False, "IMSS_NEEDS_OPTIMIZATION"
                
        except Exception as e:
            self.last_error = str(e)
            return False, "IMSS_CHECK_ERROR"
    
    def get_stats(self) -> Dict:
        """Obtiene estadísticas del módulo"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM medicamentos")
            total = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT grupo_terapeutico, COUNT(*) 
                FROM medicamentos 
                WHERE grupo_terapeutico IS NOT NULL AND grupo_terapeutico != ''
                GROUP BY grupo_terapeutico 
                ORDER BY COUNT(*) DESC LIMIT 5
            ''')
            top_groups = dict(cursor.fetchall())
            
            conn.close()
            
            return {
                'institution': 'IMSS',
                'total_medications': total,
                'top_therapeutic_groups': top_groups,
                'database_file': str(self.db_path),
                'last_sync_stats': self.last_stats
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def search(self, term: str) -> List[Dict]:
        """Búsqueda simple de medicamentos"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT clave, descripcion, grupo_terapeutico 
                FROM medicamentos WHERE 
                descripcion LIKE ? OR nombre_generico LIKE ? 
                LIMIT 10
            ''', (f'%{term}%', f'%{term}%'))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'clave': row[0],
                    'descripcion': row[1],
                    'grupo': row[2]
                })
            
            conn.close()
            return results
            
        except Exception as e:
            return []
    
    def export_data(self, format: str = 'json') -> Tuple[bool, str]:
        """
        Exporta datos del módulo
        Returns:
            (success: bool, filename_or_error: str)
        """
        try:
            filename = f"imss_medicamentos.{format}"
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM medicamentos")
            datos = cursor.fetchall()
            conn.close()
            
            if format.lower() == 'json':
                medicamentos_json = [dict(zip(self.field_names, row)) for row in datos]
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(medicamentos_json, f, ensure_ascii=False, indent=2)
            
            elif format.lower() == 'csv':
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(self.field_names)
                    writer.writerows(datos)
            
            return True, filename
            
        except Exception as e:
            return False, f"EXPORT_ERROR: {str(e)}"
    
    def get_last_error(self) -> str:
        """Obtiene el último error ocurrido"""
        return self.last_error
    
    # Métodos privados (implementación interna)
    def _init_database(self):
        """Inicializa base de datos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Crear tabla
        campos_sql = []
        for field_name in self.field_names:
            if field_name == 'clave':
                campos_sql.append(f"{field_name} TEXT PRIMARY KEY")
            else:
                campos_sql.append(f"{field_name} TEXT")
        
        create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS medicamentos (
                {', '.join(campos_sql)}
            )
        '''
        
        cursor.execute(create_table_sql)
        
        # Índices básicos
        indices = [
            'CREATE INDEX IF NOT EXISTS idx_clave_norm ON medicamentos(clave_normalizada)',
            'CREATE INDEX IF NOT EXISTS idx_descripcion ON medicamentos(descripcion)',
            'CREATE INDEX IF NOT EXISTS idx_grupo ON medicamentos(grupo_terapeutico)'
        ]
        
        for indice in indices:
            cursor.execute(indice)
        
        conn.commit()
        conn.close()
    
    def _init_optimizer(self):
        """Inicializa módulo de optimización"""
        try:
            from core.database_inspector_module import check_normalization
            from core.optimization_module import initialize_optimization_module
            self.optimizer = initialize_optimization_module(str(self.db_path))
        except ImportError:
            self.optimizer = None
    
    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Descarga PDF"""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.content
        except:
            return None
    
    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extrae texto de PDF"""
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            return "\n".join(page.extract_text() for page in pdf_reader.pages)
        except:
            return ""
    
    def _parse_main_catalog(self, text: str) -> List[MedicamentoIMSS]:
        """Parsea catálogo principal"""
        medicamentos = []
        patron_clave = r'(\d{3}\.\d{3}\.\d{4}\.\d{2})'
        secciones = re.split(patron_clave, text)
        
        for i in range(1, len(secciones), 2):
            if i + 1 < len(secciones):
                clave = secciones[i]
                contenido = secciones[i + 1]
                medicamento = self._parse_detailed_medication(clave, contenido)
                if medicamento:
                    medicamentos.append(medicamento)
        
        return medicamentos
    
    def _parse_catalog_ii(self, text: str) -> List[MedicamentoIMSS]:
        """Parsea catálogo II"""
        medicamentos = []
        patron = r'(\d{3}\.\d{3}\.\d{4}\.\d{2})\s+([A-Z][A-Z\s\-]+)'
        
        for clave, nombre in re.findall(patron, text):
            medicamento = MedicamentoIMSS(
                clave=clave.strip(),
                descripcion=nombre.strip(),
                nombre_generico=nombre.strip(),
                grupo_terapeutico="Especializado",
                categoria_medicamento="Catálogo II"
            )
            medicamentos.append(medicamento)
        
        return medicamentos
    
    def _parse_detailed_medication(self, clave: str, contenido: str) -> Optional[MedicamentoIMSS]:
        """Parsea medicamento individual"""
        try:
            contenido = re.sub(r'\s+', ' ', contenido).strip()
            
            # Extracciones básicas
            descripcion_match = re.search(r'^([^\.]+(?:mg|g|ml|UI|mcg|μg)[^\.]*)', contenido)
            descripcion = descripcion_match.group(1) if descripcion_match else ""
            
            return MedicamentoIMSS(
                clave=clave,
                descripcion=descripcion,
                nombre_generico=self._extract_generic_name(descripcion),
                presentacion=self._extract_presentation(descripcion),
                grupo_terapeutico=self._determine_therapeutic_group(clave),
                categoria_medicamento="Básico"
            )
        except:
            return None
    
    def _extract_generic_name(self, descripcion: str) -> str:
        """Extrae nombre genérico"""
        if "contiene:" in descripcion.lower():
            partes = descripcion.split("contiene:")
            if len(partes) > 1:
                return partes[1].split(".")[0].strip()
        return ""
    
    def _extract_presentation(self, descripcion: str) -> str:
        """Extrae presentación"""
        presentaciones = ['tableta', 'ampolleta', 'cápsula', 'solución']
        for presentacion in presentaciones:
            if presentacion in descripcion.lower():
                return presentacion.title()
        return ""
    
    def _determine_therapeutic_group(self, clave: str) -> str:
        """Determina grupo terapéutico"""
        grupos = {'010': 'Analgesia', '040': 'Anestesia', '020': 'Cardiología'}
        return grupos.get(clave[:3], 'No clasificado')
    
    def _process_all_catalogs(self) -> List[MedicamentoIMSS]:
        """Procesa todos los catálogos"""
        todos_medicamentos = []
        
        # Catálogo principal
        pdf_principal = self._download_pdf(self.urls['catalogo_principal'])
        if pdf_principal:
            texto = self._extract_pdf_text(pdf_principal)
            medicamentos = self._parse_main_catalog(texto)
            todos_medicamentos.extend(medicamentos)
        
        # Catálogo II
        pdf_ii = self._download_pdf(self.urls['catalogo_ii'])
        if pdf_ii:
            texto = self._extract_pdf_text(pdf_ii)
            medicamentos = self._parse_catalog_ii(texto)
            todos_medicamentos.extend(medicamentos)
        
        return todos_medicamentos
    
    def _add_medication(self, medicamento: MedicamentoIMSS) -> bool:
        """Agrega medicamento a la base"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            placeholders = ', '.join(['?' for _ in range(self.field_count)])
            valores = tuple(medicamento.to_dict().values())
            
            cursor.execute(f'INSERT OR REPLACE INTO medicamentos VALUES ({placeholders})', valores)
            conn.commit()
            conn.close()
            return True
        except:
            return False
    
    def _setup_optimizations(self) -> bool:
        """Configura optimizaciones"""
        if self.optimizer:
            try:
                result = self.optimizer.normalize_database()
                return result.get('estado') in ['completada', 'ya_completada']
            except:
                return False
        return False

# Función de conveniencia
def create_imss_module(db_path: str = "imss_medicamentos.db") -> IMSSModule:
    """Crea una instancia del módulo IMSS"""
    return IMSSModule(db_path)