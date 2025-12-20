# gsql/gsql/database.py
"""
Main GSQL database class
"""

import json
import os
from .exceptions import GSQLSyntaxError, GSQLExecutionError  # Changé ici

class GSQL:
    """GSQL Database"""
    
    def __init__(self, path=None):
        self.path = path or "gsql.db"
        
        # Dynamic imports to avoid circular dependencies
        from .parser import SQLParser
        from .storage import StorageEngine
        from .executor import QueryExecutor
        
        self.parser = SQLParser()
        self.storage = StorageEngine(self.path)
        self.executor = QueryExecutor(self.storage)
        
        # Metadata
        self.tables = {}
        self._load_metadata()
    
    def execute(self, sql):
        """Execute SQL query"""
        try:
            # Parse
            ast = self.parser.parse(sql)
            
            # Execute
            result = self.executor.execute(ast)
            
            # Update metadata if needed
            if ast['type'] in ['CREATE_TABLE', 'INSERT', 'DELETE']:
                self._save_metadata()
            
            return result
            
        except Exception as e:
            if isinstance(e, (GSQLSyntaxError, GSQLExecutionError)):
                raise
            raise GSQLExecutionError(f"Execution error: {str(e)}")
    
    def query(self, sql):
        """Execute SELECT query and return data"""
        result = self.execute(sql)
        return result.get('data', [])
    
    def create_table(self, name, columns):
        """Create table via Python API"""
        self.storage.create_table(name, columns)
        self.tables[name] = {
            'columns': columns,
            'row_count': 0
        }
        self._save_metadata()
    
    def insert(self, table, data):
        """Insert data via Python API"""
        return self.storage.insert(table, data)
    
    def select(self, table, where=None, columns=None):
        """Select data via Python API"""
        return self.storage.select(table, where, columns)
    
    def create_index(self, table, column):
        """Create index on column"""
        return self.storage.create_index(table, column)
    
    def _load_metadata(self):
        """Load metadata from disk"""
        meta_file = f"{self.path}.meta"
        if os.path.exists(meta_file):
            with open(meta_file, 'r') as f:
                self.tables = json.load(f)
    
    def _save_metadata(self):
        """Save metadata to disk"""
        meta_file = f"{self.path}.meta"
        with open(meta_file, 'w') as f:
            json.dump(self.tables, f, indent=2)
    
    def close(self):
        """Close database"""
        self._save_metadata()
        self.storage.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
