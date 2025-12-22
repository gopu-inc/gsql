"""
GSQL - A lightweight SQL database engine with natural language interface
"""


"""
GSQL - A lightweight SQL database management system
"""

__version__ = "1.0.0"
__author__ = "Gopu Inc."
__description__ = "GSQL Database Management System"
# Import Database first to avoid circular imports
try:
    from .database import Database, create_database, get_default_database, set_default_database
except ImportError as e:
    print(f"Warning: Could not import Database: {e}")
    Database = None
    create_database = None
    get_default_database = None
    set_default_database = None

# Import exceptions
from .exceptions import (
    GSQLBaseException,
    SQLSyntaxError,
    SQLExecutionError,
    ConstraintViolationError,
    TransactionError,
    FunctionError,
    NLError,
    BufferPoolError
)

# Import other components conditionally
try:
    from .nlp.translator import NLToSQLTranslator, nl_to_sql
    NLP_AVAILABLE = True
except ImportError:
    NLToSQLTranslator = None
    nl_to_sql = None
    NLP_AVAILABLE = False

try:
    from .functions.user_functions import FunctionManager
    FUNCTIONS_AVAILABLE = True
except ImportError:
    FunctionManager = None
    FUNCTIONS_AVAILABLE = False

# Shortcut function
def connect(database_path: str, use_nlp: bool = True, buffer_pool_size: int = 100):
    """
    Connect to a GSQL database
    
    Args:
        database_path (str): Path to database file
        use_nlp (bool): Enable natural language processing
        buffer_pool_size (int): Size of buffer pool in pages
        
    Returns:
        Database: Connected database instance
    """
    if Database is None:
        raise ImportError("Database module not available")
    
    return Database(database_path, use_nlp=use_nlp, buffer_pool_size=buffer_pool_size)

# Export all public members
__all__ = [
    # Main classes
    'Database',
    'FunctionManager',
    'NLToSQLTranslator',
    
    # Factory functions
    'connect',
    'create_database',
    'get_default_database',
    'set_default_database',
    
    # Helper functions
    'nl_to_sql',
    
    # Exceptions
    'GSQLBaseException',
    'SQLSyntaxError',
    'SQLExecutionError',
    'ConstraintViolationError',
    'TransactionError',
    'FunctionError',
    'NLError',
    'BufferPoolError',
    
    # Constants
    'NLP_AVAILABLE',
    'FUNCTIONS_AVAILABLE',
    '__version__',
    '__author__',
    '__license__',
]
