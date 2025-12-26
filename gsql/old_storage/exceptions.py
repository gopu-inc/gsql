"""
Exceptions pour le module de stockage GSQL
"""

class StorageError(Exception):
    """Erreur générale du stockage"""
    def __init__(self, message="Storage error", error_code=None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

    def __str__(self):
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class BufferPoolError(StorageError):
    """Erreur du buffer pool"""
    def __init__(self, message="Buffer pool error", page_id=None):
        self.page_id = page_id
        full_message = f"{message} (page: {page_id})" if page_id else message
        super().__init__(full_message, "BUFFER_POOL_ERROR")


class TransactionError(StorageError):
    """Erreur de transaction"""
    def __init__(self, message="Transaction error", tid=None):
        self.tid = tid
        full_message = f"{message} (TID: {tid})" if tid else message
        super().__init__(full_message, "TRANSACTION_ERROR")


class ConstraintViolationError(StorageError):
    """Violation de contrainte"""
    def __init__(self, message="Constraint violation", constraint=None, table=None):
        self.constraint = constraint
        self.table = table
        full_message = message
        if constraint:
            full_message += f" (constraint: {constraint})"
        if table:
            full_message += f" (table: {table})"
        super().__init__(full_message, "CONSTRAINT_ERROR")


class SQLExecutionError(StorageError):
    """Erreur d'exécution SQL"""
    def __init__(self, message="SQL execution error", sql=None):
        self.sql = sql
        full_message = f"{message}" + (f" - Query: {sql[:100]}..." if sql else "")
        super().__init__(full_message, "SQL_EXECUTION_ERROR")


class SQLSyntaxError(SQLExecutionError):
    """Erreur de syntaxe SQL"""
    def __init__(self, message="SQL syntax error", position=None, token=None):
        self.position = position
        self.token = token
        full_message = message
        if position:
            full_message += f" at position {position}"
        if token:
            full_message += f" near '{token}'"
        super().__init__(full_message, "SQL_SYNTAX_ERROR")


# Exceptions supplémentaires pour la base de données
class ConnectionError(StorageError):
    """Erreur de connexion à la base de données"""
    def __init__(self, message="Database connection error", db_path=None):
        self.db_path = db_path
        full_message = f"{message}" + (f" (db: {db_path})" if db_path else "")
        super().__init__(full_message, "CONNECTION_ERROR")


class QueryError(StorageError):
    """Erreur de requête"""
    def __init__(self, message="Query error", query_type=None):
        self.query_type = query_type
        full_message = f"{message}" + (f" (type: {query_type})" if query_type else "")
        super().__init__(full_message, "QUERY_ERROR")


class TimeoutError(StorageError):
    """Timeout d'opération"""
    def __init__(self, message="Operation timeout", operation=None, timeout_sec=None):
        self.operation = operation
        self.timeout_sec = timeout_sec
        full_message = f"{message}"
        if operation:
            full_message += f" (operation: {operation})"
        if timeout_sec:
            full_message += f" after {timeout_sec}s"
        super().__init__(full_message, "TIMEOUT_ERROR")


# Registre des exceptions pour l'export
__all__ = [
    'StorageError',
    'BufferPoolError',
    'TransactionError', 
    'ConstraintViolationError',
    'SQLExecutionError',
    'SQLSyntaxError',
    'ConnectionError',
    'QueryError',
    'TimeoutError'
]
