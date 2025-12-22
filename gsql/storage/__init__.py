"""
GSQL Storage Module - Initialization file
"""

import os
import sys
from pathlib import Path

# Version du module
__version__ = "3.0.0"
__author__ = "GSQL Team"
__license__ = "MIT"

# Configuration des chemins d'import
STORAGE_DIR = Path(__file__).parent
PROJECT_ROOT = STORAGE_DIR.parent

# Ajout au path Python pour les imports relatifs
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import des exceptions du stockage
try:
    # Essayer d'importer depuis storage.exceptions d'abord
    from .exceptions import (
        StorageError,
        BufferPoolError,
        TransactionError,
        ConstraintViolationError,
        SQLExecutionError,
        SQLSyntaxError,
        ConnectionError,
        QueryError,
        TimeoutError
    )
except ImportError:
    # Fallback: définir les exceptions basiques si le fichier n'existe pas
    class StorageError(Exception):
        """Erreur générale du stockage"""
        pass
    
    class BufferPoolError(StorageError):
        """Erreur du buffer pool"""
        pass
    
    class TransactionError(StorageError):
        """Erreur de transaction"""
        pass
    
    class ConstraintViolationError(StorageError):
        """Violation de contrainte"""
        pass
    
    class SQLExecutionError(StorageError):
        """Erreur d'exécution SQL"""
        pass
    
    class SQLSyntaxError(SQLExecutionError):
        """Erreur de syntaxe SQL"""
        pass
    
    class ConnectionError(StorageError):
        """Erreur de connexion"""
        pass
    
    class QueryError(StorageError):
        """Erreur de requête"""
        pass
    
    class TimeoutError(StorageError):
        """Timeout d'opération"""
        pass

# Import du stockage SQLite principal
try:
    from .sqlite_storage import SQLiteStorage, create_storage, quick_query
    from .sqlite_storage import BufferPool, TransactionManager
    SQLITE_AVAILABLE = True
except ImportError as e:
    SQLITE_AVAILABLE = False
    SQLiteStorage = None
    create_storage = None
    quick_query = None
    BufferPool = None
    TransactionManager = None
    print(f"Warning: SQLite storage not available: {e}")

# Import du backend JSON
try:
    from .json_storage import JSONStorage
    JSON_AVAILABLE = True
except ImportError:
    JSON_AVAILABLE = False
    JSONStorage = None

# Définition des alias pour compatibilité
Storage = SQLiteStorage  # Alias principal

# Factory function pour créer des instances de stockage
def get_storage(storage_type="sqlite", **kwargs):
    """
    Factory pour créer des instances de stockage
    
    Args:
        storage_type: Type de stockage ("sqlite" ou "json")
        **kwargs: Arguments à passer au constructeur
    
    Returns:
        Instance du stockage demandé
    
    Raises:
        ValueError: Si le type de stockage n'est pas supporté
    """
    if storage_type == "sqlite":
        if not SQLITE_AVAILABLE:
            raise ImportError("SQLite storage is not available")
        return create_storage(**kwargs) if create_storage else SQLiteStorage(**kwargs)
    
    elif storage_type == "json":
        if not JSON_AVAILABLE:
            raise ImportError("JSON storage is not available")
        return JSONStorage(**kwargs)
    
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")

# Fonction utilitaire pour vérifier la disponibilité des backends
def check_storage_backends():
    """Vérifie quels backends de stockage sont disponibles"""
    return {
        "sqlite": SQLITE_AVAILABLE,
        "json": JSON_AVAILABLE,
        "version": __version__
    }

# Fonction pour obtenir des statistiques système
def get_storage_stats():
    """Retourne des statistiques sur le module de stockage"""
    stats = {
        "version": __version__,
        "backends_available": check_storage_backends(),
        "storage_dir": str(STORAGE_DIR),
        "files": []
    }
    
    # Lister les fichiers du dossier storage
    try:
        for file in STORAGE_DIR.iterdir():
            if file.is_file() and file.suffix == ".py" and file.name != "__init__.py":
                stats["files"].append(file.name)
    except Exception as e:
        stats["error"] = str(e)
    
    return stats

# Initialisation du logging
def setup_logging(level="INFO"):
    """Configure le logging pour le module storage"""
    import logging
    
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper()))
    
    return logger

# Variables exportées
__all__ = [
    # Classes principales
    "SQLiteStorage",
    "Storage",  # Alias
    "BufferPool",
    "TransactionManager",
    
    # Backend JSON
    "JSONStorage",
    
    # Exceptions
    "StorageError",
    "BufferPoolError",
    "TransactionError",
    "ConstraintViolationError",
    "SQLExecutionError",
    "SQLSyntaxError",
    "ConnectionError",
    "QueryError",
    "TimeoutError",
    
    # Fonctions
    "create_storage",
    "quick_query",
    "get_storage",
    "check_storage_backends",
    "get_storage_stats",
    "setup_logging",
    
    # Constantes
    "__version__",
    "__author__",
    "__license__",
    
    # Flags de disponibilité
    "SQLITE_AVAILABLE",
    "JSON_AVAILABLE"
]

# Message d'initialisation
if __name__ != "__main__":
    # Initialisation silencieuse pour les imports normaux
    pass
else:
    # Mode standalone: afficher les infos
    print(f"GSQL Storage Module v{__version__}")
    print("=" * 40)
    
    stats = check_storage_backends()
    for backend, available in stats.items():
        if backend != "version":
            status = "✓" if available else "✗"
            print(f"{status} {backend.upper():10} {'Available' if available else 'Not available'}")
    
    print(f"\nStorage directory: {STORAGE_DIR}")
    
    # Tester les imports
    if SQLITE_AVAILABLE:
        print("\n✓ SQLite storage ready")
    if JSON_AVAILABLE:
        print("✓ JSON storage ready")
