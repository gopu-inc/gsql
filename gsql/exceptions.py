class GSQLBaseException(Exception):
    """Exception de base pour GSQL"""
    pass

class SQLSyntaxError(GSQLBaseException):
    """Erreur de syntaxe SQL"""
    pass

class SQLExecutionError(GSQLBaseException):
    """Erreur d'exécution SQL"""
    pass

class ConstraintViolationError(GSQLBaseException):
    """Violation de contrainte"""
    pass

class TransactionError(GSQLBaseException):
    """Erreur de transaction"""
    pass

class FunctionError(GSQLBaseException):
    """Erreur dans les fonctions"""
    pass

class NLError(GSQLBaseException):
    """Erreur de traitement du langage naturel"""
    pass

class BufferPoolError(GSQLBaseException):
    """Erreur du buffer pool"""
    pass
