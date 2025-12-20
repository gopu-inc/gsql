# gsql/gsql/exceptions.py
"""
Custom exceptions for GSQL
"""

class GQLError(Exception):
    """Base exception"""
    pass

class GQLSyntaxError(GQLError):
    """SQL syntax error"""
    pass

class GQLExecutionError(GQLError):
    """Query execution error"""
    pass

class GQLTableError(GQLError):
    """Table error"""
    pass

class GQLColumnError(GQLError):
    """Column error"""
    pass
