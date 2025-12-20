# gsql/gsql/__init__.py
"""
GSQL - Pure Python SQL Database
"""

__version__ = "1.0.0"
__author__ = "GSQL Team"

# Lazy imports to avoid circular dependencies
def __getattr__(name):
    if name == "GSQL":
        from .database import GSQL
        return GSQL
    elif name == "GSQLSyntaxError":
        from .exceptions import GSQLSyntaxError
        return GSQLSyntaxError
    elif name == "GSQLExecutionError":
        from .exceptions import GSQLExecutionError
        return GSQLExecutionError
    raise AttributeError(f"module 'gsql' has no attribute '{name}'")

__all__ = ['GSQL', 'GSQLSyntaxError', 'GSQLExecutionError']
