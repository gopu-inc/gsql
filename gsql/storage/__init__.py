# gsql/storage/__init__.py
"""
GSQL Storage Module - SQLite Backend Only
Version: 3.0 - Sans YAML
"""

# N'importez PAS yaml_storage !
# Importez uniquement les modules SQLite

from .sqlite_storage import (
    SQLiteStorage, 
    BufferPool, 
    TransactionManager, 
    create_storage, 
    get_storage_stats
)

# Déclarer explicitement ce qui est exporté
__all__ = [
    'SQLiteStorage',
    'BufferPool', 
    'TransactionManager',
    'create_storage',
    'get_storage_stats'
]

# Optionnel : vous pouvez aussi SUPPRIMER le fichier yaml_storage.py
# rm gsql/storage/yaml_storage.py
