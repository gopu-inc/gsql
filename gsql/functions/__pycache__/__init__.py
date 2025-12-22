"""
GSQL Functions Module
"""

import sys
from pathlib import Path

# Version
__version__ = "1.0.0"
__all__ = []

# Ajout au path
FUNCTIONS_DIR = Path(__file__).parent
if str(FUNCTIONS_DIR) not in sys.path:
    sys.path.insert(0, str(FUNCTIONS_DIR))

# Import de FunctionManager
try:
    from .function_manager import FunctionManager, FunctionError
    __all__.extend(['FunctionManager', 'FunctionError'])
    FUNCTION_MANAGER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import FunctionManager: {e}")
    FunctionManager = None
    FunctionError = None
    FUNCTION_MANAGER_AVAILABLE = False

# Import des fonctions utilisateur
try:
    from .user_functions import *
    # Ajouter toutes les fonctions de user_functions à __all__
    import importlib
    user_functions_module = importlib.import_module('.user_functions', 'gsql.functions')
    if hasattr(user_functions_module, '__all__'):
        __all__.extend(user_functions_module.__all__)
    else:
        # Ajouter toutes les fonctions publiques
        for name in dir(user_functions_module):
            if not name.startswith('_'):
                __all__.append(name)
except ImportError as e:
    print(f"Warning: Could not import user_functions: {e}")

# Export des flags
__all__.extend(['FUNCTION_MANAGER_AVAILABLE'])

# Message d'initialisation
if __name__ != "__main__":
    pass  # Initialisation silencieuse
else:
    print(f"GSQL Functions Module v{__version__}")
    print(f"FunctionManager available: {FUNCTION_MANAGER_AVAILABLE}")
