"""
Módulo Inspector de Base de Datos IMSS
Permite recorrer y verificar la normalización de la base de datos
"""

import sqlite3
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class IMSSDatabaseInspector:
    """Inspector para verificar normalización y estado de la base de datos"""
    
    def __init__(self, db_path: str):
        """
        Inicializa el inspector
        
        Args:
            db_path: Ruta a la base de datos SQLite
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Base de datos no encontrada: {db_path}")
    
    def _get_connection(self):
        """Obtiene conexión a la base de datos"""
        return sqlite3.connect(self.db_path)
    
    def get_database_structure(self) -> Dict:
        """
        Obtiene información sobre la estructura de la base de datos
        
        Returns:
            Diccionario con información de tablas y columnas
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        structure = {
            'tables': {},
            'indexes': [],
            'total_size': self.db_path.stat().st_size if self.db_path.exists() else 0
        }
        
        # Obtener todas las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for (table_name,) in tables:
            # Información de columnas
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # Contar registros
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            
            structure['tables'][table_name] = {
                'columns': [
                    {
                        'name': col[1],
                        'type': col[2],
                        'not_null': bool(col[3]),
                        'default': col[4],
                        'primary_key': bool(col[5])
                    }
                    for col in columns
                ],
                'row_count': count
            }
        
        # Obtener índices
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'")
        indexes = cursor.fetchall()
        structure['indexes'] = [idx[0] for idx in indexes]
        
        conn.close()
        return structure
    
    def check_normalization_status(self) -> Dict:
        """
        Verifica el estado de normalización de la base de datos
        
        Returns:
            Diccionario con estado de normalización
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        status = {
            'normalization_tables_exist': False,
            'normalization_completed': False,
            'normalization_date': None,
            'medicamentos_normalized': 0,
            'medicamentos_without_normalization': 0,
            'active_ingredients_found': 0,
            'optimization_indexes_exist': False
        }
        
        try:
            # Verificar si existen las tablas de normalización
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('principios_activos', 'metadatos_sistema')")
            normalization_tables = [row[0] for row in cursor.fetchall()]
            status['normalization_tables_exist'] = len(normalization_tables) == 2
            
            # Verificar si la normalización está completada
            if 'metadatos_sistema' in normalization_tables:
                cursor.execute("SELECT valor, fecha_actualizacion FROM metadatos_sistema WHERE clave = 'normalizacion_completa'")
                result = cursor.fetchone()
                if result:
                    status['normalization_completed'] = result[0] == 'true'
                    status['normalization_date'] = result[1]
            
            # Verificar medicamentos normalizados
            cursor.execute("SELECT name FROM pragma_table_info('medicamentos') WHERE name = 'principio_activo_normalizado'")
            if cursor.fetchone():
                cursor.execute("SELECT COUNT(*) FROM medicamentos WHERE principio_activo_normalizado IS NOT NULL AND principio_activo_normalizado != ''")
                status['medicamentos_normalized'] = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM medicamentos WHERE principio_activo_normalizado IS NULL OR principio_activo_normalizado = ''")
                status['medicamentos_without_normalization'] = cursor.fetchone()[0]
            
            # Verificar principios activos
            if 'principios_activos' in normalization_tables:
                cursor.execute("SELECT COUNT(*) FROM principios_activos")
                status['active_ingredients_found'] = cursor.fetchone()[0]
            
            # Verificar índices de optimización
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE '%principio_activo%'")
            status['optimization_indexes_exist'] = len(cursor.fetchall()) > 0
            
        except Exception as e:
            logger.error(f"Error verificando normalización: {e}")
        
        conn.close()
        return status
    
    def sample_normalized_data(self, limit: int = 10) -> List[Dict]:
        """
        Obtiene una muestra de datos normalizados
        
        Args:
            limit: Número máximo de registros a obtener
            
        Returns:
            Lista de medicamentos con información de normalización
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        sample_data = []
        
        try:
            # Verificar si existe la columna de normalización
            cursor.execute("SELECT name FROM pragma_table_info('medicamentos') WHERE name = 'principio_activo_normalizado'")
            if cursor.fetchone():
                cursor.execute('''
                    SELECT 
                        clave, 
                        descripcion, 
                        nombre_generico, 
                        principio_activo_normalizado,
                        grupo_terapeutico
                    FROM medicamentos 
                    WHERE principio_activo_normalizado IS NOT NULL 
                    AND principio_activo_normalizado != ''
                    LIMIT ?
                ''', (limit,))
                
                results = cursor.fetchall()
                for row in results:
                    sample_data.append({
                        'clave': row[0],
                        'descripcion': row[1],
                        'nombre_generico': row[2],
                        'principio_activo_normalizado': row[3],
                        'grupo_terapeutico': row[4]
                    })
        
        except Exception as e:
            logger.error(f"Error obteniendo muestra: {e}")
        
        conn.close()
        return sample_data
    
    def analyze_active_ingredients(self) -> Dict:
        """
        Analiza los principios activos normalizados
        
        Returns:
            Análisis de principios activos
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        analysis = {
            'top_ingredients': [],
            'ingredients_with_multiple_products': [],
            'total_unique_ingredients': 0,
            'normalization_quality': {}
        }
        
        try:
            # Verificar si existe la tabla de principios activos
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = 'principios_activos'")
            if cursor.fetchone():
                # Top principios activos
                cursor.execute('''
                    SELECT 
                        principio_activo_normalizado, 
                        total_medicamentos,
                        grupos_terapeuticos
                    FROM principios_activos 
                    ORDER BY total_medicamentos DESC 
                    LIMIT 10
                ''')
                
                for row in cursor.fetchall():
                    analysis['top_ingredients'].append({
                        'ingredient': row[0],
                        'medication_count': row[1],
                        'therapeutic_groups': row[2].split(',') if row[2] else []
                    })
                
                # Total de ingredientes únicos
                cursor.execute("SELECT COUNT(*) FROM principios_activos")
                analysis['total_unique_ingredients'] = cursor.fetchone()[0]
                
                # Ingredientes con múltiples productos
                cursor.execute('''
                    SELECT 
                        principio_activo_normalizado, 
                        total_medicamentos
                    FROM principios_activos 
                    WHERE total_medicamentos > 1
                    ORDER BY total_medicamentos DESC
                    LIMIT 20
                ''')
                
                for row in cursor.fetchall():
                    analysis['ingredients_with_multiple_products'].append({
                        'ingredient': row[0],
                        'product_count': row[1]
                    })
            
            # Análisis de calidad de normalización
            cursor.execute("SELECT name FROM pragma_table_info('medicamentos') WHERE name = 'principio_activo_normalizado'")
            if cursor.fetchone():
                # Medicamentos sin normalizar
                cursor.execute("SELECT COUNT(*) FROM medicamentos WHERE principio_activo_normalizado IS NULL OR principio_activo_normalizado = ''")
                unnormalized = cursor.fetchone()[0]
                
                # Total de medicamentos
                cursor.execute("SELECT COUNT(*) FROM medicamentos")
                total = cursor.fetchone()[0]
                
                if total > 0:
                    normalization_percentage = ((total - unnormalized) / total) * 100
                    analysis['normalization_quality'] = {
                        'total_medications': total,
                        'normalized_medications': total - unnormalized,
                        'unnormalized_medications': unnormalized,
                        'normalization_percentage': round(normalization_percentage, 2)
                    }
        
        except Exception as e:
            logger.error(f"Error analizando principios activos: {e}")
        
        conn.close()
        return analysis
    
    def find_normalization_examples(self, search_term: str = "") -> List[Dict]:
        """
        Encuentra ejemplos de normalización para un término específico
        
        Args:
            search_term: Término a buscar (opcional)
            
        Returns:
            Lista de ejemplos de normalización
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        examples = []
        
        try:
            cursor.execute("SELECT name FROM pragma_table_info('medicamentos') WHERE name = 'principio_activo_normalizado'")
            if cursor.fetchone():
                if search_term:
                    # Buscar ejemplos que contengan el término
                    cursor.execute('''
                        SELECT 
                            clave,
                            descripcion,
                            nombre_generico,
                            principio_activo_normalizado
                        FROM medicamentos 
                        WHERE (descripcion LIKE ? OR nombre_generico LIKE ? OR principio_activo_normalizado LIKE ?)
                        AND principio_activo_normalizado IS NOT NULL 
                        AND principio_activo_normalizado != ''
                        ORDER BY principio_activo_normalizado
                        LIMIT 20
                    ''', (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%'))
                else:
                    # Obtener ejemplos aleatorios
                    cursor.execute('''
                        SELECT 
                            clave,
                            descripcion,
                            nombre_generico,
                            principio_activo_normalizado
                        FROM medicamentos 
                        WHERE principio_activo_normalizado IS NOT NULL 
                        AND principio_activo_normalizado != ''
                        ORDER BY RANDOM()
                        LIMIT 15
                    ''')
                
                for row in cursor.fetchall():
                    examples.append({
                        'clave': row[0],
                        'original_description': row[1],
                        'generic_name': row[2],
                        'normalized_ingredient': row[3],
                        'normalization_applied': self._show_normalization_process(row[1], row[2], row[3])
                    })
        
        except Exception as e:
            logger.error(f"Error buscando ejemplos: {e}")
        
        conn.close()
        return examples
    
    def _show_normalization_process(self, description: str, generic_name: str, normalized: str) -> Dict:
        """Muestra cómo se aplicó el proceso de normalización"""
        original = generic_name or description
        return {
            'original': original,
            'normalized': normalized,
            'process_applied': original != normalized
        }
    
    def get_inspection_report(self) -> Dict:
        """
        Genera un reporte completo de inspección
        
        Returns:
            Reporte completo del estado de la base de datos
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'database_file': str(self.db_path),
            'file_size_mb': round(self.db_path.stat().st_size / (1024 * 1024), 2),
            'structure': self.get_database_structure(),
            'normalization_status': self.check_normalization_status(),
            'active_ingredients_analysis': self.analyze_active_ingredients(),
            'sample_data': self.sample_normalized_data(5),
            'recommendations': []
        }
        
        # Generar recomendaciones
        norm_status = report['normalization_status']
        
        if not norm_status['normalization_tables_exist']:
            report['recommendations'].append("Ejecutar módulo de optimización para crear tablas de normalización")
        
        if not norm_status['normalization_completed']:
            report['recommendations'].append("Ejecutar proceso de normalización completa")
        
        if norm_status['medicamentos_without_normalization'] > 0:
            report['recommendations'].append(f"Normalizar {norm_status['medicamentos_without_normalization']} medicamentos restantes")
        
        if not norm_status['optimization_indexes_exist']:
            report['recommendations'].append("Crear índices de optimización para búsquedas rápidas")
        
        if norm_status['active_ingredients_found'] == 0:
            report['recommendations'].append("Generar tabla de principios activos agrupados")
        
        return report

# Funciones de utilidad para usar el módulo
def inspect_database(db_path: str) -> Dict:
    """
    Función de conveniencia para inspeccionar una base de datos
    
    Args:
        db_path: Ruta a la base de datos
        
    Returns:
        Reporte de inspección
    """
    inspector = IMSSDatabaseInspector(db_path)
    return inspector.get_inspection_report()

def check_normalization(db_path: str) -> Dict:
    """
    Función de conveniencia para verificar solo la normalización
    
    Args:
        db_path: Ruta a la base de datos
        
    Returns:
        Estado de normalización
    """
    inspector = IMSSDatabaseInspector(db_path)
    return inspector.check_normalization_status()

def show_normalization_examples(db_path: str, search_term: str = "", limit: int = 10) -> List[Dict]:
    """
    Función de conveniencia para mostrar ejemplos de normalización
    
    Args:
        db_path: Ruta a la base de datos
        search_term: Término a buscar
        limit: Límite de resultados
        
    Returns:
        Lista de ejemplos
    """
    inspector = IMSSDatabaseInspector(db_path)
    return inspector.find_normalization_examples(search_term)

# Ejemplo de uso del módulo
if __name__ == "__main__":
    print("=== INSPECTOR DE BASE DE DATOS IMSS ===")
    
    # Usar con la base de datos del sistema
    db_file = "imss_medicamentos.db"
    
    if Path(db_file).exists():
        print(f"Inspeccionando base de datos: {db_file}")
        
        # Reporte completo
        inspector = IMSSDatabaseInspector(db_file)
        report = inspector.get_inspection_report()
        
        print(f"\n📊 ESTRUCTURA DE BASE DE DATOS:")
        print(f"   Tamaño: {report['file_size_mb']} MB")
        print(f"   Tablas: {len(report['structure']['tables'])}")
        print(f"   Índices: {len(report['structure']['indexes'])}")
        
        print(f"\n🔧 ESTADO DE NORMALIZACIÓN:")
        norm = report['normalization_status']
        print(f"   Tablas de normalización: {'✅' if norm['normalization_tables_exist'] else '❌'}")
        print(f"   Normalización completada: {'✅' if norm['normalization_completed'] else '❌'}")
        print(f"   Medicamentos normalizados: {norm['medicamentos_normalized']}")
        print(f"   Sin normalizar: {norm['medicamentos_without_normalization']}")
        print(f"   Principios activos únicos: {norm['active_ingredients_found']}")
        
        print(f"\n🧪 ANÁLISIS DE PRINCIPIOS ACTIVOS:")
        analysis = report['active_ingredients_analysis']
        if analysis['normalization_quality']:
            quality = analysis['normalization_quality']
            print(f"   Porcentaje normalizado: {quality['normalization_percentage']}%")
            print(f"   Total medicamentos: {quality['total_medications']}")
        
        print(f"   Ingredientes únicos: {analysis['total_unique_ingredients']}")
        
        if analysis['top_ingredients']:
            print(f"\n🔝 TOP 5 PRINCIPIOS ACTIVOS:")
            for i, ingredient in enumerate(analysis['top_ingredients'][:5], 1):
                print(f"   {i}. {ingredient['ingredient']}: {ingredient['medication_count']} medicamentos")
        
        print(f"\n💡 RECOMENDACIONES:")
        for rec in report['recommendations']:
            print(f"   • {rec}")
        
        # Ejemplos de normalización
        print(f"\n📋 EJEMPLOS DE NORMALIZACIÓN:")
        examples = inspector.find_normalization_examples("", limit=3)
        for example in examples:
            print(f"   {example['clave']}: {example['original_description']}")
            print(f"   → Normalizado como: {example['normalized_ingredient']}")
            print()
        
    else:
        print(f"❌ Base de datos no encontrada: {db_file}")
        print("Ejecuta primero el código principal para crear la base de datos")