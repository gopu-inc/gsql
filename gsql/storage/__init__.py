# gsql/storage/__init__.py - CORRIGÉ
"""
GSQL Storage Module - SQLite Backend Only
"""

# Import depuis le fichier sqlite_storage.py DANS LE MÊME DOSSIER
from .sqlite_storage import (
    SQLiteStorage, 
    BufferPool, 
    TransactionManager, 
    create_storage, 
    get_storage_stats
)

__all__ = [
    'SQLiteStorage',
    'BufferPool', 
    'TransactionManager',
    'create_storage',
    'get_storage_stats'
]
