"""
Módulo de Optimización para el Sistema IMSS
Agrega capacidades de normalización, agrupación y búsqueda rápida
"""

import re
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class IMSSOptimizationModule:
    """Módulo de optimización para el sistema IMSS existente"""
    
    def __init__(self, db_connection):
        """
        Inicializa el módulo con una conexión a la base de datos existente
        
        Args:
            db_connection: Conexión SQLite existente o path a la base de datos
        """
        self.db_path = db_connection if isinstance(db_connection, str) else None
        self.conn = db_connection if not isinstance(db_connection, str) else None
    
    def _get_connection(self):
        """Obtiene conexión a la base de datos"""
        if self.conn:
            return self.conn
        else:
            return sqlite3.connect(str(self.db_path))
    
    def setup_optimization_tables(self):
        """Configura las tablas necesarias para optimización"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Agregar columna de principio activo normalizado si no existe
        try:
            cursor.execute('ALTER TABLE medicamentos ADD COLUMN principio_activo_normalizado TEXT')
        except sqlite3.OperationalError:
            pass  # La columna ya existe
        
        # Tabla para agrupar medicamentos por principio activo
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS principios_activos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                principio_activo_normalizado TEXT UNIQUE,
                nombres_comerciales TEXT,
                total_medicamentos INTEGER,
                grupos_terapeuticos TEXT,
                fecha_normalizacion TEXT
            )
        ''')
        
        # Tabla de metadatos para evitar procesamientos repetidos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadatos_sistema (
                clave TEXT PRIMARY KEY,
                valor TEXT,
                fecha_actualizacion TEXT
            )
        ''')
        
        # Crear índices optimizados
        indices = [
            'CREATE INDEX IF NOT EXISTS idx_principio_activo ON medicamentos(principio_activo_normalizado)',
            'CREATE INDEX IF NOT EXISTS idx_optimized_search ON medicamentos(principio_activo_normalizado, grupo_terapeutico)',
            'CREATE INDEX IF NOT EXISTS idx_categoria_estado ON medicamentos(categoria_medicamento, estado)'
        ]
        
        for indice in indices:
            cursor.execute(indice)
        
        conn.commit()
        if self.db_path:  # Solo cerrar si abrimos la conexión aquí
            conn.close()
        
        logger.info("Tablas de optimización configuradas")
    
    def normalize_active_ingredient(self, descripcion: str, nombre_generico: str = "") -> str:
        """
        Normaliza el principio activo para detectar medicamentos similares
        
        Args:
            descripcion: Descripción del medicamento
            nombre_generico: Nombre genérico si está disponible
            
        Returns:
            Principio activo normalizado
        """
        texto_base = nombre_generico or descripcion
        
        # Remover prefijos/sufijos comunes de sales
        texto = re.sub(r'\b(clorhidrato|sulfato|besilato|maleato|tartrato|citrato|acetato|bromuro)\s+(de\s+)?', '', texto_base.lower())
        
        # Remover concentraciones
        texto = re.sub(r'\d+(\.\d+)?\s*(mg|g|ml|mcg|μg|ui|%|mEq)', '', texto)
        
        # Remover formas farmacéuticas
        texto = re.sub(r'\b(tableta|capsula|ampolleta|solucion|crema|gel|jarabe|supositorio|parche)\b', '', texto)
        
        # Remover palabras irrelevantes
        texto = re.sub(r'\b(cada|contiene|envase|con)\b', '', texto)
        
        # Normalizar espacios y caracteres especiales
        texto = re.sub(r'[^\w\s]', '', texto)
        texto = re.sub(r'\s+', ' ', texto).strip()
        
        return texto.upper() if texto else ""
    
    def normalize_database(self) -> Dict[str, int]:
        """
        Normaliza toda la base de datos una sola vez
        
        Returns:
            Diccionario con estadísticas del proceso
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Verificar si ya fue normalizada
        cursor.execute("SELECT valor FROM metadatos_sistema WHERE clave = 'normalizacion_completa'")
        resultado = cursor.fetchone()
        
        if resultado and resultado[0] == 'true':
            logger.info("Base de datos ya normalizada")
            if self.db_path:
                conn.close()
            return self._get_normalization_stats()
        
        logger.info("Iniciando normalización de base de datos...")
        
        # 1. Actualizar principios activos normalizados
        cursor.execute("SELECT clave, descripcion, nombre_generico FROM medicamentos")
        medicamentos = cursor.fetchall()
        
        actualizaciones = 0
        for clave, descripcion, nombre_generico in medicamentos:
            principio_normalizado = self.normalize_active_ingredient(descripcion, nombre_generico or "")
            cursor.execute(
                "UPDATE medicamentos SET principio_activo_normalizado = ? WHERE clave = ?",
                (principio_normalizado, clave)
            )
            actualizaciones += 1
        
        # 2. Generar tabla de principios activos agrupados
        cursor.execute("DELETE FROM principios_activos")
        
        cursor.execute('''
            SELECT 
                principio_activo_normalizado,
                GROUP_CONCAT(descripcion) as nombres_comerciales,
                COUNT(*) as total_medicamentos,
                GROUP_CONCAT(grupo_terapeutico) as grupos_terapeuticos
            FROM medicamentos 
            WHERE principio_activo_normalizado != '' AND principio_activo_normalizado IS NOT NULL
            GROUP BY principio_activo_normalizado
            ORDER BY COUNT(*) DESC
        ''')
        
        grupos_principios = cursor.fetchall()
        principios_insertados = 0
        
        for principio, nombres, total, grupos in grupos_principios:
            cursor.execute('''
                INSERT INTO principios_activos 
                (principio_activo_normalizado, nombres_comerciales, total_medicamentos, grupos_terapeuticos, fecha_normalizacion)
                VALUES (?, ?, ?, ?, ?)
            ''', (principio, nombres, total, grupos, datetime.now().isoformat()))
            principios_insertados += 1
        
        # 3. Marcar como normalizada
        cursor.execute('''
            INSERT OR REPLACE INTO metadatos_sistema (clave, valor, fecha_actualizacion)
            VALUES ('normalizacion_completa', 'true', ?)
        ''', (datetime.now().isoformat(),))
        
        conn.commit()
        if self.db_path:
            conn.close()
        
        logger.info(f"Normalización completada: {actualizaciones} medicamentos, {principios_insertados} principios activos")
        
        return {
            'medicamentos_actualizados': actualizaciones,
            'principios_activos_encontrados': principios_insertados,
            'estado': 'completada'
        }
    
    def _get_normalization_stats(self) -> Dict[str, int]:
        """Obtiene estadísticas de normalización existente"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM medicamentos WHERE principio_activo_normalizado != ''")
        medicamentos_normalizados = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM principios_activos")
        principios_unicos = cursor.fetchone()[0]
        
        if self.db_path:
            conn.close()
        
        return {
            'medicamentos_actualizados': medicamentos_normalizados,
            'principios_activos_encontrados': principios_unicos,
            'estado': 'ya_completada'
        }
    
    def find_similar_medications(self, search_term: str) -> List[Dict]:
        """
        Busca medicamentos similares por principio activo (RÁPIDO)
        
        Args:
            search_term: Término de búsqueda
            
        Returns:
            Lista de grupos de medicamentos similares
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Normalizar término de búsqueda
        search_normalized = self.normalize_active_ingredient("", search_term)
        
        # Buscar en tabla optimizada
        cursor.execute('''
            SELECT 
                pa.principio_activo_normalizado,
                pa.total_medicamentos,
                pa.grupos_terapeuticos,
                GROUP_CONCAT(m.clave, '|') as claves,
                GROUP_CONCAT(m.descripcion) as descripciones
            FROM principios_activos pa
            JOIN medicamentos m ON pa.principio_activo_normalizado = m.principio_activo_normalizado
            WHERE pa.principio_activo_normalizado LIKE ? COLLATE NOCASE
            GROUP BY pa.principio_activo_normalizado
            ORDER BY pa.total_medicamentos DESC
        ''', (f"%{search_normalized}%",))
        
        resultados = []
        for row in cursor.fetchall():
            principio, total, grupos, claves, descripciones = row
            resultados.append({
                'principio_activo': principio,
                'total_medicamentos': total,
                'grupos_terapeuticos': grupos.split(', ') if grupos else [],
                'claves': claves.split('|') if claves else [],
                'descripciones': descripciones.split(' | ') if descripciones else []
            })
        
        if self.db_path:
            conn.close()
        
        return resultados
    
    def get_optimized_exploration(self) -> Dict:
        """
        Explora la base de datos de forma optimizada usando índices
        
        Returns:
            Diccionario con datos de exploración pre-calculados
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        exploration = {}
        
        # 1. Principios activos más comunes (ya calculados)
        cursor.execute('''
            SELECT principio_activo_normalizado, total_medicamentos 
            FROM principios_activos 
            ORDER BY total_medicamentos DESC 
            LIMIT 10
        ''')
        exploration['top_active_ingredients'] = [
            {'ingredient': row[0], 'count': row[1]} 
            for row in cursor.fetchall()
        ]
        
        # 2. Grupos terapéuticos (con índice)
        cursor.execute('''
            SELECT grupo_terapeutico, COUNT(*) as cantidad
            FROM medicamentos 
            WHERE grupo_terapeutico != '' 
            GROUP BY grupo_terapeutico 
            ORDER BY cantidad DESC
        ''')
        exploration['therapeutic_groups'] = [
            {'group': row[0], 'count': row[1]} 
            for row in cursor.fetchall()
        ]
        
        # 3. Distribución por categoría
        cursor.execute('''
            SELECT categoria_medicamento, COUNT(*) as cantidad
            FROM medicamentos 
            GROUP BY categoria_medicamento
        ''')
        exploration['by_category'] = [
            {'category': row[0], 'count': row[1]} 
            for row in cursor.fetchall()
        ]
        
        # 4. Metadatos del sistema
        cursor.execute("SELECT clave, valor, fecha_actualizacion FROM metadatos_sistema")
        exploration['system_metadata'] = {
            row[0]: {'value': row[1], 'date': row[2]} 
            for row in cursor.fetchall()
        }
        
        if self.db_path:
            conn.close()
        
        return exploration
    
    def get_optimization_status(self) -> Dict:
        """
        Verifica el estado de optimización del sistema
        
        Returns:
            Estado actual de las optimizaciones
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        status = {
            'tables_created': False,
            'database_normalized': False,
            'indexes_created': False,
            'ready_for_fast_search': False
        }
        
        # Verificar si existen las tablas de optimización
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='principios_activos'")
        status['tables_created'] = cursor.fetchone() is not None
        
        # Verificar si la base está normalizada
        try:
            cursor.execute("SELECT valor FROM metadatos_sistema WHERE clave = 'normalizacion_completa'")
            resultado = cursor.fetchone()
            status['database_normalized'] = resultado and resultado[0] == 'true'
        except sqlite3.OperationalError:
            status['database_normalized'] = False
        
        # Verificar índices
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_principio_activo'")
        status['indexes_created'] = cursor.fetchone() is not None
        
        # Sistema listo para búsqueda rápida
        status['ready_for_fast_search'] = all([
            status['tables_created'],
            status['database_normalized'],
            status['indexes_created']
        ])
        
        if self.db_path:
            conn.close()
        
        return status

def initialize_optimization_module(db_connection) -> IMSSOptimizationModule:
    """
    Inicializa el módulo de optimización
    
    Args:
        db_connection: Conexión a la base de datos existente
        
    Returns:
        Instancia del módulo de optimización configurada
    """
    module = IMSSOptimizationModule(db_connection)
    module.setup_optimization_tables()
    return module

# Ejemplo de uso como módulo independiente
if __name__ == "__main__":
    # Este código solo se ejecuta si el archivo se ejecuta directamente
    print("Módulo de Optimización IMSS")
    print("Para usar este módulo, impórtalo en tu código base:")
    print()
    print("from optimization_module import initialize_optimization_module")
    print("optimizer = initialize_optimization_module('tu_base_datos.db')")
    print("optimizer.normalize_database()")
    print("results = optimizer.find_similar_medications('paracetamol')")