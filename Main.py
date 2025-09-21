"""
Main Súper Limpia - Solo Llamadas a Módulos
Interfaz simple con códigos de éxito/error
"""

import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_modules():
    """Inicializa todos los módulos disponibles"""
    modules = {}
    
    # IMSS Module
    try:
        from modules.imss_clean_module import create_imss_module
        from modules.issste_module import ISSSTEDataManager
        imss = create_imss_module()
        success, code = imss.initialize()
        modules['IMSS'] = {
            'module': imss,
            'initialized': success,
            'code': code,
            'error': imss.get_last_error() if not success else None
        }
    except ImportError:
        modules['IMSS'] = {'initialized': False, 'code': 'MODULE_NOT_FOUND', 'error': 'IMSS module not available'}
    
    # ISSSTE Module
    try:
        from issste_module import ISSSTEDataManager
        issste = ISSSTEDataManager()
        # ISSSTE no tiene initialize(), asumimos que es exitoso si se crea
        modules['ISSSTE'] = {
            'module': issste,
            'initialized': True,
            'code': 'ISSSTE_INIT_SUCCESS',
            'error': None
        }
    except ImportError:
        modules['ISSSTE'] = {'initialized': False, 'code': 'MODULE_NOT_FOUND', 'error': 'ISSSTE module not available'}
    except Exception as e:
        modules['ISSSTE'] = {'initialized': False, 'code': 'ISSSTE_INIT_ERROR', 'error': str(e)}
    
    return modules

def check_modules_ready(modules):
    """Verifica qué módulos están listos para usar"""
    status = {}
    
    for name, module_info in modules.items():
        if not module_info['initialized']:
            status[name] = {'ready': False, 'code': module_info['code']}
            continue
        
        module = module_info['module']
        
        if name == 'IMSS':
            ready, code = module.is_ready()
            status[name] = {'ready': ready, 'code': code}
        
        elif name == 'ISSSTE':
            # Para ISSSTE, verificamos si tiene datos
            try:
                stats = module.obtener_estadisticas()
                ready = stats.get('total_medicamentos', 0) > 0
                code = 'ISSSTE_READY' if ready else 'ISSSTE_EMPTY'
                status[name] = {'ready': ready, 'code': code}
            except:
                status[name] = {'ready': False, 'code': 'ISSSTE_CHECK_ERROR'}
    
    return status

def sync_modules_data(modules):
    """Sincroniza datos de todos los módulos"""
    results = {}
    
    for name, module_info in modules.items():
        if not module_info['initialized']:
            results[name] = {'success': False, 'code': 'MODULE_NOT_INITIALIZED'}
            continue
        
        module = module_info['module']
        
        try:
            if name == 'IMSS':
                success, code = module.sync_data()
                results[name] = {'success': success, 'code': code}
                if not success:
                    results[name]['error'] = module.get_last_error()
            
            elif name == 'ISSSTE':
                sync_result = module.sincronizar_datos()
                success = sync_result['agregados_exitosos'] > 0
                code = 'ISSSTE_SYNC_SUCCESS' if success else 'ISSSTE_SYNC_NO_DATA'
                results[name] = {'success': success, 'code': code, 'stats': sync_result}
        
        except Exception as e:
            results[name] = {'success': False, 'code': f'{name}_SYNC_ERROR', 'error': str(e)}
    
    return results

def show_modules_stats(modules):
    """Muestra estadísticas de todos los módulos"""
    print("\n" + "="*60)
    print("ESTADÍSTICAS DE MÓDULOS")
    print("="*60)
    
    for name, module_info in modules.items():
        if not module_info['initialized']:
            print(f"{name}: No inicializado ({module_info['code']})")
            continue
        
        module = module_info['module']
        
        try:
            if name == 'IMSS':
                stats = module.get_stats()
            elif name == 'ISSSTE':
                stats = module.obtener_estadisticas()
            else:
                continue
            
            print(f"\n{name}:")
            print(f"  Total medicamentos: {stats.get('total_medicamentos', 0):,}")
            
            if 'top_therapeutic_groups' in stats:
                print("  Top grupos terapéuticos:")
                for grupo, cantidad in list(stats['top_therapeutic_groups'].items())[:3]:
                    print(f"    - {grupo}: {cantidad}")
            
            if 'por_nivel_atencion' in stats:
                print("  Por nivel de atención:")
                for nivel, cantidad in stats['por_nivel_atencion'].items():
                    print(f"    - {nivel}: {cantidad}")
        
        except Exception as e:
            print(f"{name}: Error obteniendo estadísticas - {e}")

def demonstrate_searches(modules):
    """Demuestra búsquedas en todos los módulos"""
    print("\n" + "="*60)
    print("DEMOSTRACIÓN DE BÚSQUEDAS")
    print("="*60)
    
    search_terms = ["paracetamol", "omeprazol"]
    
    for term in search_terms:
        print(f"\nBúsqueda: '{term}'")
        
        for name, module_info in modules.items():
            if not module_info['initialized']:
                continue
            
            module = module_info['module']
            
            try:
                if name == 'IMSS':
                    results = module.search(term)
                elif name == 'ISSSTE':
                    results = module.buscar_medicamentos(term)
                    # Convertir a formato simple
                    results = [{'clave': m.clave, 'descripcion': m.descripcion} for m in results[:5]]
                else:
                    continue
                
                print(f"  {name}: {len(results)} resultados")
                for i, result in enumerate(results[:2], 1):
                    desc = result.get('descripcion', '')[:50]
                    print(f"    {i}. {result.get('clave', '')}: {desc}...")
            
            except Exception as e:
                print(f"  {name}: Error en búsqueda - {e}")

def export_all_data(modules):
    """Exporta datos de todos los módulos"""
    print("\n" + "="*60)
    print("EXPORTANDO DATOS")
    print("="*60)
    
    for name, module_info in modules.items():
        if not module_info['initialized']:
            continue
        
        module = module_info['module']
        
        try:
            if name == 'IMSS':
                success, filename = module.export_data('json')
                if success:
                    print(f"{name}: Exportado a {filename}")
                else:
                    print(f"{name}: Error - {filename}")
            
            elif name == 'ISSSTE':
                filename = module.exportar_datos('json')
                print(f"{name}: Exportado a {filename}")
        
        except Exception as e:
            print(f"{name}: Error exportando - {e}")

def main():
    """Main súper limpia - solo llamadas a módulos"""
    print("=== SISTEMA MULTI-INSTITUCIONAL MEXICANO ===")
    
    # 1. INICIALIZAR MÓDULOS
    print("\n1. Inicializando módulos...")
    modules = initialize_modules()
    
    for name, info in modules.items():
        status = "✓" if info['initialized'] else "✗"
        print(f"   {status} {name}: {info['code']}")
        if info.get('error'):
            print(f"     Error: {info['error']}")
    
    # Verificar si al menos un módulo está disponible
    available_modules = [name for name, info in modules.items() if info['initialized']]
    if not available_modules:
        print("\n❌ No hay módulos disponibles. Verifica las importaciones.")
        return
    
    # 2. VERIFICAR ESTADO
    print("\n2. Verificando estado de módulos...")
    status = check_modules_ready(modules)
    
    needs_sync = []
    ready_modules = []
    
    for name, info in status.items():
        status_icon = "✓" if info['ready'] else "⚠"
        print(f"   {status_icon} {name}: {info['code']}")
        
        if info['ready']:
            ready_modules.append(name)
        else:
            needs_sync.append(name)
    
    # 3. SINCRONIZAR SI ES NECESARIO
    if needs_sync:
        print(f"\n3. Módulos necesitan sincronización: {', '.join(needs_sync)}")
        user_input = input("¿Continuar con sincronización? (s/n): ").lower()
        
        if user_input in ['s', 'si', 'y', 'yes']:
            print("Sincronizando datos...")
            
            # Filtrar solo módulos que necesitan sync
            modules_to_sync = {name: modules[name] for name in needs_sync if name in modules}
            sync_results = sync_modules_data(modules_to_sync)
            
            for name, result in sync_results.items():
                status_icon = "✓" if result['success'] else "✗"
                print(f"   {status_icon} {name}: {result['code']}")
                
                if result['success'] and 'stats' in result:
                    stats = result['stats']
                    total = stats.get('agregados_exitosos', stats.get('total_processed', 0))
                    print(f"     Medicamentos: {total:,}")
        else:
            print("Sincronización cancelada. Trabajando solo con módulos listos.")
    else:
        print("\n3. Todos los módulos están listos ✓")
    
    # 4. MOSTRAR ESTADÍSTICAS
    show_modules_stats(modules)
    
    # 5. DEMOSTRAR BÚSQUEDAS
    demonstrate_searches(modules)
    
    # 6. EXPORTAR DATOS
    export_all_data(modules)
    
    print("\n" + "="*60)
    print("RESUMEN FINAL")
    print("="*60)
    print(f"Módulos disponibles: {len(available_modules)}")
    print(f"Módulos listos: {len(ready_modules)}")
    print("Sistema operativo para consultas y análisis")
    
    print("\n=== SISTEMA COMPLETADO ===")

if __name__ == "__main__":
    main()