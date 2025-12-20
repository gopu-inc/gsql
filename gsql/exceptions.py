# gsql/gsql/exceptions.py
"""
Exceptions personnalisées pour GSQL
"""

class GQLError(Exception):
    """Base exception for GSQL errors"""
    pass


class GQLSyntaxError(GQLError):
    """Syntax error in SQL query"""
    pass


class GQLExecutionError(GQLError):
    """Query execution error"""
    pass


class GQLTableError(GQLError):
    """Table-related error"""
    pass


class GQLColumnError(GQLError):
    """Column-related error"""
    pass


class GQLTypeError(GQLError):
    """Type error"""
    pass


class GQLConstraintError(GQLError):
    """Constraint violation error"""
    pass
