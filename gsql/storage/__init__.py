"""
Package storage pour GSQL
"""

from .exceptions import *
from .sqlite_storage import SqliteStorage, get_storage_stats

# Pas de YamlStorage
__all__ = ['SqliteStorage', 'get_storage_stats']