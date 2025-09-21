"""
Módulo de Verificación Rápida IMSS
Solo verifica el estado sin reconstruir datos
"""

import sqlite3
from pathlib import Path
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class IMSSQuickChecker:
    """Verificador rápido del estado de la base de datos IMSS"""
    
    def __init__(self, db_path: str = "imss_medicamentos.db"):
        self.db_path = Path(db_path)
        self.exists = self.db_path.exists()
    
    def quick_status(self) -> Dict:
        """Verificación súper rápida del estado general"""
        if not self.exists:
            return {'status': 'no_database', 'message': 'Base de datos no existe'}
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Verificaciones básicas rápidas
            cursor.execute("SELECT COUNT(*) FROM medicamentos")
            total_meds = cursor.fetchone()[0]
            
            # Verificar si tiene normalización
            cursor.execute("SELECT name FROM pragma_table_info('medicamentos') WHERE name = 'principio_activo_normalizado'")
            has_normalization_column = cursor.fetchone() is not None
            
            if has_normalization_column:
                cursor.execute("SELECT COUNT(*) FROM medicamentos WHERE principio_activo_normalizado IS NOT NULL AND principio_activo_normalizado != ''")
                normalized_count = cursor.fetchone()[0]
                normalization_percent = (normalized_count / total_meds * 100) if total_meds > 0 else 0
            else:
                normalized_count = 0
                normalization_percent = 0
            
            # Verificar tabla de optimización
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = 'principios_activos'")
            has_optimization_table = cursor.fetchone() is not None
            
            if has_optimization_table:
                cursor.execute("SELECT COUNT(*) FROM principios_activos")
                unique_ingredients = cursor.fetchone()[0]
            else:
                unique_ingredients = 0
            
            conn.close()
            
            return {
                'status': 'ready' if normalization_percent > 90 else 'needs_optimization',
                'total_medications': total_meds,
                'normalized_medications': normalized_count,
                'normalization_percentage': round(normalization_percent, 1),
                'unique_ingredients': unique_ingredients,
                'optimization_table_exists': has_optimization_table,
                'ready_for_use': normalization_percent > 50 and has_optimization_table
            }
            
        except Exception as e:
            conn.close()
            return {'status': 'error', 'message': str(e)}
    
    def test_search_performance(self, test_term: str = "paracetamol") -> Dict:
        """Prueba rápida de rendimiento de búsqueda"""
        if not self.exists:
            return {'error': 'No database'}
        
        import time
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Búsqueda tradicional
            start_time = time.time()
            cursor.execute("SELECT COUNT(*) FROM medicamentos WHERE descripcion LIKE ?", (f'%{test_term}%',))
            traditional_count = cursor.fetchone()[0]
            traditional_time = time.time() - start_time
            
            # Búsqueda optimizada (si existe)
            optimized_time = None
            optimized_count = 0
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = 'principios_activos'")
            if cursor.fetchone():
                start_time = time.time()
                cursor.execute("""
                    SELECT COUNT(*) FROM principios_activos 
                    WHERE principio_activo_normalizado LIKE ?
                """, (f'%{test_term.upper()}%',))
                optimized_count = cursor.fetchone()[0]
                optimized_time = time.time() - start_time
            
            conn.close()
            
            return {
                'test_term': test_term,
                'traditional_search': {
                    'time_seconds': round(traditional_time, 4),
                    'results_count': traditional_count
                },
                'optimized_search': {
                    'time_seconds': round(optimized_time, 4) if optimized_time else None,
                    'results_count': optimized_count,
                    'available': optimized_time is not None
                }
            }
            
        except Exception as e:
            conn.close()
            return {'error': str(e)}
    
    def sample_normalization(self, limit: int = 5) -> List[Dict]:
        """Muestra una pequeña muestra de normalización"""
        if not self.exists:
            return []
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT clave, descripcion, principio_activo_normalizado 
                FROM medicamentos 
                WHERE principio_activo_normalizado IS NOT NULL 
                AND principio_activo_normalizado != ''
                LIMIT ?
            """, (limit,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'clave': row[0],
                    'descripcion': row[1][:50] + '...' if len(row[1]) > 50 else row[1],
                    'principio_normalizado': row[2]
                })
            
            conn.close()
            return results
            
        except Exception as e:
            conn.close()
            return []
    
    def suggest_next_steps(self, status: Dict) -> List[str]:
        """Sugiere los próximos pasos basado en el estado"""
        suggestions = []
        
        if status.get('status') == 'no_database':
            suggestions.append("Ejecutar sincronización inicial para crear base de datos")
            return suggestions
        
        if status.get('status') == 'error':
            suggestions.append("Verificar integridad de la base de datos")
            return suggestions
        
        total_meds = status.get('total_medications', 0)
        normalized_percent = status.get('normalization_percentage', 0)
        has_optimization = status.get('optimization_table_exists', False)
        
        if total_meds == 0:
            suggestions.append("Base de datos vacía - ejecutar sincronización de PDFs")
        elif total_meds < 1000:
            suggestions.append("Pocos medicamentos - verificar sincronización completa")
        
        if normalized_percent < 10:
            suggestions.append("Ejecutar normalización inicial del módulo de optimización")
        elif normalized_percent < 90:
            suggestions.append("Completar normalización de medicamentos restantes")
        
        if not has_optimization:
            suggestions.append("Crear tabla de principios activos para búsquedas rápidas")
        
        if normalized_percent > 90 and has_optimization:
            suggestions.append("Sistema listo - puedes usar búsquedas optimizadas")
        
        return suggestions

def quick_check(db_path: str = "imss_medicamentos.db") -> None:
    """Función de conveniencia para verificación rápida con output formateado"""
    
    print("=== VERIFICACIÓN RÁPIDA IMSS ===")
    
    checker = IMSSQuickChecker(db_path)
    status = checker.quick_status()
    
    print(f"\nEstado: {status.get('status', 'unknown').upper()}")
    
    if status.get('status') == 'no_database':
        print("❌ Base de datos no existe")
        print("\nSolución: Ejecutar código principal para crear la base")
        return
    
    if status.get('status') == 'error':
        print(f"❌ Error: {status.get('message', 'Unknown')}")
        return
    
    # Mostrar estadísticas
    total = status.get('total_medications', 0)
    normalized = status.get('normalized_medications', 0)
    percent = status.get('normalization_percentage', 0)
    ingredients = status.get('unique_ingredients', 0)
    
    print(f"\n📊 ESTADÍSTICAS:")
    print(f"   Total medicamentos: {total:,}")
    print(f"   Medicamentos normalizados: {normalized:,} ({percent}%)")
    print(f"   Principios activos únicos: {ingredients:,}")
    print(f"   Optimización disponible: {'✅' if status.get('optimization_table_exists') else '❌'}")
    print(f"   Listo para uso: {'✅' if status.get('ready_for_use') else '❌'}")
    
    # Mostrar muestra de normalización
    if normalized > 0:
        print(f"\n🧪 MUESTRA DE NORMALIZACIÓN:")
        sample = checker.sample_normalization(3)
        for item in sample:
            print(f"   {item['clave']}: {item['descripcion']}")
            print(f"   → {item['principio_normalizado']}")
    
    # Probar rendimiento
    print(f"\n⚡ PRUEBA DE RENDIMIENTO:")
    perf = checker.test_search_performance()
    if 'error' not in perf:
        trad = perf['traditional_search']
        opt = perf['optimized_search']
        print(f"   Búsqueda tradicional: {trad['time_seconds']}s ({trad['results_count']} resultados)")
        if opt['available']:
            speedup = trad['time_seconds'] / opt['time_seconds'] if opt['time_seconds'] > 0 else 0
            print(f"   Búsqueda optimizada: {opt['time_seconds']}s ({opt['results_count']} grupos)")
            print(f"   Mejora de velocidad: {speedup:.1f}x más rápido" if speedup > 1 else "   Similar velocidad")
        else:
            print("   Búsqueda optimizada: No disponible")
    
    # Sugerencias
    suggestions = checker.suggest_next_steps(status)
    if suggestions:
        print(f"\n💡 PRÓXIMOS PASOS:")
        for suggestion in suggestions:
            print(f"   • {suggestion}")
    
    print(f"\n{'='*40}")

# Función súper rápida para integrar en otros códigos
def is_ready(db_path: str = "imss_medicamentos.db") -> bool:
    """Verifica si el sistema está listo para usar (súper rápido)"""
    checker = IMSSQuickChecker(db_path)
    status = checker.quick_status()
    return status.get('ready_for_use', False)

# Ejemplo de uso
if __name__ == "__main__":
    quick_check()