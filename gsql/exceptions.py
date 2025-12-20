# gsql/gsql/exceptions.py
"""
Custom exceptions for GSQL
"""

class GQLError(Exception):
    """Base exception"""
    pass

class GSQLSyntaxError(GQLError):
    """SQL syntax error"""
    pass

class GSQLExecutionError(GQLError):
    """Query execution error"""
    pass

class GSQLTableError(GQLError):
    """Table error"""
    pass

class GSQLColumnError(GQLError):
    """Column error"""
    pass

# Pour la rétrocompatibilité
GQLSyntaxError = GSQLSyntaxError
GQLExecutionError = GSQLExecutionError
GQLTableError = GSQLTableError
GQLColumnError = GSQLColumnError
