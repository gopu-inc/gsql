# gsql/gsql/database.py
"""
Main GSQL database class with persistent storage
"""

import os
from .exceptions import GSQLSyntaxError, GSQLExecutionError

class GSQL:
    """GSQL Database with TOML configuration"""
    
    def __init__(self, path=None, config_file="GSQL.toml"):
        self.path = path or "gsql.db"
        self.config_file = config_file
        
        # Dynamic imports
        from .parser import SQLParser
        from .storage import PersistentStorage
        from .executor import QueryExecutor
        
        # Initialize components
        self.parser = SQLParser()
        self.storage = PersistentStorage(config_file)
        self.executor = QueryExecutor(self.storage)
        
        # Load metadata
        self.tables = {}
        self._load_metadata()
        
        print(f"✅ GSQL initialized with config: {config_file}")
    
    def execute(self, sql: str) -> dict:
        """Execute SQL query with persistence"""
        try:
            # Parse
            ast = self.parser.parse(sql)
            
            # Execute
            result = self.executor.execute(ast)
            
            # Update metadata
            self._update_metadata(ast, result)
            
            return result
            
        except Exception as e:
            if isinstance(e, (GSQLSyntaxError, GSQLExecutionError)):
                raise
            raise GSQLExecutionError(f"Execution error: {str(e)}")
    
    def query(self, sql: str) -> list:
        """Execute SELECT query"""
        result = self.execute(sql)
        return result.get('data', [])
    
    def create_table(self, name: str, columns: list) -> None:
        """Create table via Python API"""
        self.storage.create_table(name, columns)
        self.tables[name] = {
            'columns': columns,
            'created_at': self._current_timestamp()
        }
        self._save_metadata()
    
    def insert(self, table: str, data: dict) -> int:
        """Insert data via Python API"""
        return self.storage.insert(table, data)
    
    def select(self, table: str, where: dict = None, 
               columns: list = None, limit: int = None) -> list:
        """Select data via Python API"""
        return self.storage.select(table, where, columns, limit)
    
    def update(self, table: str, set_data: dict, where: dict) -> int:
        """Update data via Python API"""
        return self.storage.update(table, set_data, where)
    
    def delete(self, table: str, where: dict) -> int:
        """Delete data via Python API"""
        return self.storage.delete(table, where)
    
    def create_index(self, table: str, column: str) -> None:
        """Create index on column"""
        return self.storage.create_index(table, column)
    
    def list_tables(self) -> list:
        """List all tables"""
        return self.storage.list_tables()
    
    def get_table_info(self, table: str) -> dict:
        """Get detailed table information"""
        return self.storage.get_table_info(table)
    
    def backup(self, name: str = None) -> str:
        """Create backup"""
        return self.storage.backup(name)
    
    def restore(self, backup_name: str) -> None:
        """Restore from backup"""
        return self.storage.restore(backup_name)
    
    def stats(self) -> dict:
        """Get database statistics"""
        return {
            'tables': len(self.tables),
            'total_rows': sum(
                t.get('row_count', 0) for t in self.tables.values()
            ),
            'storage_path': str(self.storage.base_path)
        }
    
    def _load_metadata(self) -> None:
        """Load metadata from storage"""
        tables = self.storage.list_tables()
        for table in tables:
            info = self.storage.get_table_info(table)
            if info:
                self.tables[table] = info
    
    def _save_metadata(self) -> None:
        """Save metadata is now handled by storage engine"""
        pass
    
    def _update_metadata(self, ast: dict, result: dict) -> None:
        """Update metadata after operation"""
        if ast.get('type') == 'CREATE_TABLE':
            table = ast.get('table')
            if table and table not in self.tables:
                self.tables[table] = {
                    'columns': ast.get('columns', []),
                    'created_at': self._current_timestamp()
                }
    
    def _current_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def close(self) -> None:
        """Close database"""
        self.storage.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
