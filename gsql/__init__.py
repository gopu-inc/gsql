# gsql/gsql/__init__.py
"""
GSQL - Simple yet powerful SQL database in Python
"""

__version__ = "0.1.0"
__author__ = "GSQL Contributors"

# Importations différées pour éviter les dépendances circulaires
def __getattr__(name):
    if name == "GSQL":
        from .database import GSQL
        return GSQL
    elif name == "TableProxy":
        from .database import TableProxy
        return TableProxy
    elif name == "GSQLSyntaxError":
        from .exceptions import GQLSyntaxError
        return GQLSyntaxError
    elif name == "GSQLExecutionError":
        from .exceptions import GQLExecutionError
        return GQLExecutionError
    raise AttributeError(f"module 'gsql' has no attribute '{name}'")

# Pour permettre: from gsql import *
__all__ = ['GSQL', 'TableProxy', 'GSQLSyntaxError', 'GSQLExecutionError']
