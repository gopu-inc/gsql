#!/usr/bin/env python3
"""
Database module for GSQL - Main database engine class
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union

from .exceptions import (
    GSQLBaseException, SQLSyntaxError, SQLExecutionError,
    ConstraintViolationError, TransactionError
)
from .parser import SQLParser
from .executor import QueryExecutor
from .storage import StorageEngine
from .functions.user_functions import FunctionManager
from .nlp.translator import NLToSQLTranslator

logger = logging.getLogger(__name__)

class Database:
    """Main database class for GSQL"""
    
    def __init__(self, db_path: str, use_nlp: bool = True, buffer_pool_size: int = 100):
        """
        Initialize a GSQL database
        
        Args:
            db_path (str): Path to database file
            use_nlp (bool): Enable natural language processing
            buffer_pool_size (int): Size of buffer pool
        """
        self.db_path = db_path
        self.use_nlp = use_nlp
        
        # Initialize components
        self.storage = StorageEngine(db_path, buffer_pool_size=buffer_pool_size)
        self.parser = SQLParser()
        self.function_manager = FunctionManager()
        
        # Initialize NLP translator if enabled
        self.nlp_translator = None
        if use_nlp:
            try:
                self.nlp_translator = NLToSQLTranslator()
            except Exception as e:
                logger.warning(f"NLP translator initialization failed: {e}")
                self.nlp_translator = None
        
        # Initialize executor with all components
        self.executor = QueryExecutor(
            database=self,
            function_manager=self.function_manager,
            nlp_translator=self.nlp_translator
        )
        
        # Inject function manager into parser
        self.parser.function_manager = self.function_manager
        
        logger.info(f"Database initialized: {db_path}")
    
    def execute(self, sql: str, params: Dict = None, use_cache: bool = True) -> Any:
        """
        Execute a SQL query or natural language command
        
        Args:
            sql (str): SQL query or natural language text
            params (Dict): Parameters for prepared statements
            use_cache (bool): Use query cache
            
        Returns:
            Query results
            
        Raises:
            SQLExecutionError: If execution fails
        """
        try:
            return self.executor.execute(sql, params=params, use_cache=use_cache)
        except Exception as e:
            if isinstance(e, GSQLBaseException):
                raise e
            raise SQLExecutionError(f"Execution failed: {str(e)}")
    
    def execute_nl(self, nl_query: str) -> Any:
        """
        Execute a natural language query
        
        Args:
            nl_query (str): Query in natural language
            
        Returns:
            Query results
            
        Raises:
            SQLExecutionError: If execution fails
        """
        if not self.use_nlp or not self.nlp_translator:
            raise SQLExecutionError("NLP is not enabled")
        
        return self.execute(nl_query)
    
    def create_function(self, name: str, params: List[str], body: str, return_type: str = "TEXT") -> str:
        """
        Create a user-defined function
        
        Args:
            name (str): Function name
            params (List[str]): Parameter names
            body (str): Function body (Python code)
            return_type (str): Return type
            
        Returns:
            str: Success message
        """
        return self.function_manager.create_function(name, params, body, return_type)
    
    def create_function_from_sql(self, sql: str) -> str:
        """
        Create a function from SQL CREATE FUNCTION statement
        
        Args:
            sql (str): CREATE FUNCTION SQL statement
            
        Returns:
            str: Success message
        """
        parsed = self.parser.parse_create_function(sql)
        return self.create_function(
            name=parsed['name'],
            params=parsed['params'],
            body=parsed['body'],
            return_type=parsed['return_type']
        )
    
    def list_functions(self) -> List[Dict]:
        """
        List all available functions
        
        Returns:
            List[Dict]: Function information
        """
        return self.function_manager.list_functions()
    
    def drop_function(self, name: str) -> str:
        """
        Drop a user-defined function
        
        Args:
            name (str): Function name
            
        Returns:
            str: Success message
        """
        return self.function_manager.drop_function(name)
    
    def begin_transaction(self) -> int:
        """
        Begin a new transaction
        
        Returns:
            int: Transaction ID
        """
        return self.storage.transaction_manager.begin()
    
    def commit_transaction(self, tid: int) -> str:
        """
        Commit a transaction
        
        Args:
            tid (int): Transaction ID
            
        Returns:
            str: Success message
        """
        self.storage.transaction_manager.commit(tid)
        return f"Transaction {tid} committed"
    
    def rollback_transaction(self, tid: int) -> str:
        """
        Rollback a transaction
        
        Args:
            tid (int): Transaction ID
            
        Returns:
            str: Success message
        """
        self.storage.transaction_manager.rollback(tid)
        return f"Transaction {tid} rolled back"
    
    def get_cache_stats(self) -> Dict:
        """
        Get buffer pool cache statistics
        
        Returns:
            Dict: Cache statistics
        """
        return self.storage.buffer_pool.stats()
    
    def clear_cache(self) -> str:
        """
        Clear query cache
        
        Returns:
            str: Success message
        """
        self.executor.clear_cache()
        return "Cache cleared"
    
    def import_csv(self, csv_path: str, table_name: str = None, delimiter: str = ',') -> str:
        """
        Import data from CSV file
        
        Args:
            csv_path (str): Path to CSV file
            table_name (str): Table name (defaults to filename)
            delimiter (str): CSV delimiter
            
        Returns:
            str: Success message
        """
        import csv
        from pathlib import Path
        
        filepath = Path(csv_path)
        if not filepath.exists():
            raise SQLExecutionError(f"File not found: {csv_path}")
        
        if table_name is None:
            table_name = filepath.stem
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            headers = reader.fieldnames
            
            if not headers:
                raise SQLExecutionError("CSV file has no headers")
            
            # Create table
            create_sql = f"CREATE TABLE {table_name} ({', '.join([f'{h} TEXT' for h in headers])})"
            self.execute(create_sql)
            
            # Insert data
            count = 0
            for row in reader:
                values = []
                for h in headers:
                    if row[h] is None:
                        values.append('NULL')
                    else:
                        # Échapper les apostrophes simplement
                        value = str(row[h])
                        # Remplacer une apostrophe par deux apostrophes
                        if "'" in value:
                            value = value.replace("'", "''")
                        values.append(f"'{value}'")
                
                insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(values)})"
                self.execute(insert_sql)
                count += 1
            
            return f"Imported {count} rows into table '{table_name}'"
    
    def export_csv(self, table_name: str, csv_path: str, delimiter: str = ',') -> str:
        """
        Export table to CSV file
        
        Args:
            table_name (str): Table name
            csv_path (str): Output CSV path
            delimiter (str): CSV delimiter
            
        Returns:
            str: Success message
        """
        import csv
        
        result = self.execute(f"SELECT * FROM {table_name}")
        
        if not result or not isinstance(result, dict) or 'rows' not in result or not result['rows']:
            raise SQLExecutionError(f"No data in table '{table_name}'")
        
        rows = result['rows']
        headers = list(rows[0].keys())
        
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers, delimiter=delimiter)
            writer.writeheader()
            writer.writerows(rows)
        
        return f"Exported {len(rows)} rows to '{csv_path}'"
    
    def get_tables(self) -> List[str]:
        """
        Get list of all tables in the database
        
        Returns:
            List[str]: Table names
        """
        try:
            # This is a simplified version - you might need to adjust based on your storage
            result = self.execute("SELECT name FROM sqlite_master WHERE type='table'")
            if isinstance(result, dict) and 'rows' in result:
                return [row['name'] for row in result['rows']]
            return []
        except:
            return []
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """
        Get schema of a table
        
        Args:
            table_name (str): Table name
            
        Returns:
            List[Dict]: Column information
        """
        try:
            # Simplified schema query
            result = self.execute(f"PRAGMA table_info({table_name})")
            if isinstance(result, dict) and 'rows' in result:
                return result['rows']
            return []
        except:
            return []
    
    def close(self):
        """Close the database connection"""
        # Cleanup resources
        if hasattr(self.executor, 'clear_cache'):
            self.executor.clear_cache()
        logger.info(f"Database closed: {self.db_path}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

# Factory function for easy database creation
def create_database(db_path: str, **kwargs) -> Database:
    """
    Create a new database
    
    Args:
        db_path (str): Path to database file
        **kwargs: Additional arguments for Database constructor
        
    Returns:
        Database: New database instance
    """
    # Ensure directory exists
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Remove existing file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
    
    return Database(db_path, **kwargs)

# Singleton for default database
_default_db = None

def get_default_database() -> Optional[Database]:
    """Get the default database instance"""
    return _default_db

def set_default_database(db: Database):
    """Set the default database instance"""
    global _default_db
    _default_db = db
