# gsql/gsql/__init__.py
"""
GSQL - Pure Python SQL Database
"""

__version__ = "1.0.0"
__author__ = "GSQL Team"

# Export main classes
def __getattr__(name):
    if name == "GSQL":
        from .database import GSQL
        return GSQL
    raise AttributeError(f"module 'gsql' has no attribute '{name}'")

__all__ = ['GSQL']
