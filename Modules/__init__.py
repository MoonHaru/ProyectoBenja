"""
Institution-specific modules
"""
try:
    from .imss_clean_module import create_imss_module
    IMSS_AVAILABLE = True
except ImportError:
    IMSS_AVAILABLE = False

try:
    from .issste_module import ISSSTEDataManager
    ISSSTE_AVAILABLE = True
except ImportError:
    ISSSTE_AVAILABLE = False

__all__ = ['create_imss_module', 'ISSSTEDataManager']