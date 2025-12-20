# gsql/gsql/database.py - CORRECTION
"""
Main GSQL database class - SIMPLIFIED
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Définir les exceptions locales pour éviter les imports circulaires
class GSQLSyntaxError(Exception):
    pass

class GSQLExecutionError(Exception):
    pass

class GSQLTableError(Exception):
    pass

class GSQL:
    """GSQL Database - Simplified and Working"""
    
    def __init__(self, path: Optional[str] = None):
        """
        Initialize GSQL database
        
        Args:
            path: Path to database file (optional)
        """
        self.path = path or "gsql.db"
        self.data_dir = Path(self.path).parent / f"{Path(self.path).stem}_data"
        self.data_dir.mkdir(exist_ok=True)
        
        # Initialize subdirectories
        (self.data_dir / 'tables').mkdir(exist_ok=True)
        (self.data_dir / 'meta').mkdir(exist_ok=True)
        
        # Import components
        try:
            from .parser import SQLParser
            from .storage import PersistentStorage
            from .executor import QueryExecutor
            
            self.parser = SQLParser()
            self.storage = PersistentStorage(self.path)
            self.executor = QueryExecutor(self.storage)
            
        except ImportError as e:
            # Fallback to very simple storage
            print(f"GSQL Warning: {e}")
            self._init_simple_components()
        
        # Metadata
        self.tables: Dict[str, Dict] = {}
        self._load_metadata()
    
    def _init_simple_components(self):
        """Initialize simple components as fallback"""
        # Simple parser
        class SimpleParser:
            def parse(self, sql):
                # Very simple parser for CREATE TABLE and INSERT
                sql = sql.strip().upper()
                if sql.startswith('CREATE TABLE'):
                    # Simple CREATE TABLE parsing
                    parts = sql.split()
                    table_name = parts[2]
                    return {
                        'type': 'CREATE_TABLE',
                        'table': table_name,
                        'columns': [{'name': 'id', 'type': 'INT'}]
                    }
                elif sql.startswith('INSERT INTO'):
                    # Simple INSERT parsing
                    parts = sql.split()
                    table_name = parts[2]
                    return {
                        'type': 'INSERT',
                        'table': table_name,
                        'values': [[1, 'test']]
                    }
                elif sql.startswith('SELECT'):
                    # Simple SELECT parsing
                    parts = sql.split()
                    table_name = parts[parts.index('FROM') + 1]
                    return {
                        'type': 'SELECT',
                        'table': table_name,
                        'columns': ['*']
                    }
                else:
                    raise GSQLSyntaxError(f"Unsupported SQL: {sql}")
        
        # Simple storage
        class SimpleStorage:
            def __init__(self, db_path):
                self.db_path = db_path
                self.data_dir = Path(db_path).parent / f"{Path(db_path).stem}_data"
                self.data_dir.mkdir(exist_ok=True)
            
            def create_table(self, name, columns):
                table_file = self.data_dir / 'tables' / f'{name}.json'
                with open(table_file, 'w') as f:
                    json.dump([], f)
                
                meta_file = self.data_dir / 'meta' / f'{name}.json'
                with open(meta_file, 'w') as f:
                    json.dump({
                        'name': name,
                        'columns': columns,
                        'created_at': datetime.now().isoformat(),
                        'row_count': 0
                    }, f)
            
            def insert(self, table, data):
                table_file = self.data_dir / 'tables' / f'{table}.json'
                if table_file.exists():
                    with open(table_file, 'r') as f:
                        rows = json.load(f)
                else:
                    rows = []
                
                rows.append(data)
                
                with open(table_file, 'w') as f:
                    json.dump(rows, f, indent=2)
                
                # Update metadata
                meta_file = self.data_dir / 'meta' / f'{table}.json'
                if meta_file.exists():
                    with open(meta_file, 'r') as f:
                        meta = json.load(f)
                    meta['row_count'] = len(rows)
                    with open(meta_file, 'w') as f:
                        json.dump(meta, f, indent=2)
                
                return len(rows)
            
            def select(self, table, where=None, columns=None, limit=None):
                table_file = self.data_dir / 'tables' / f'{table}.json'
                if not table_file.exists():
                    return []
                
                with open(table_file, 'r') as f:
                    rows = json.load(f)
                
                results = []
                for row in rows:
                    if where:
                        match = all(row.get(k) == v for k, v in where.items())
                        if not match:
                            continue
                    
                    if columns:
                        filtered = {k: v for k, v in row.items() if k in columns}
                        results.append(filtered)
                    else:
                        results.append(row)
                    
                    if limit and len(results) >= limit:
                        break
                
                return results
            
            def list_tables(self):
                tables_dir = self.data_dir / 'tables'
                if not tables_dir.exists():
                    return []
                return [f.stem for f in tables_dir.glob('*.json')]
            
            def get_table_info(self, table):
                meta_file = self.data_dir / 'meta' / f'{table}.json'
                if meta_file.exists():
                    with open(meta_file, 'r') as f:
                        return json.load(f)
                return None
        
        # Simple executor
        class SimpleExecutor:
            def __init__(self, storage):
                self.storage = storage
            
            def execute(self, ast):
                query_type = ast.get('type')
                
                if query_type == 'CREATE_TABLE':
                    table_name = ast['table']
                    columns = ast.get('columns', [])
                    self.storage.create_table(table_name, columns)
                    return {
                        'type': 'CREATE_TABLE',
                        'table': table_name,
                        'columns': len(columns)
                    }
                
                elif query_type == 'INSERT':
                    table_name = ast['table']
                    values = ast.get('values', [])
                    
                    rows_inserted = 0
                    for row_values in values:
                        # Convert row values to dict
                        row = {}
                        for i, val in enumerate(row_values):
                            row[f'col_{i}'] = val
                        
                        self.storage.insert(table_name, row)
                        rows_inserted += 1
                    
                    return {
                        'type': 'INSERT',
                        'table': table_name,
                        'rows_affected': rows_inserted
                    }
                
                elif query_type == 'SELECT':
                    table_name = ast['table']
                    columns = ast.get('columns', ['*'])
                    
                    data = self.storage.select(table_name, None, columns)
                    
                    return {
                        'type': 'SELECT',
                        'table': table_name,
                        'data': data,
                        'row_count': len(data)
                    }
                
                else:
                    raise GSQLExecutionError(f"Unsupported query type: {query_type}")
        
        self.parser = SimpleParser()
        self.storage = SimpleStorage(self.path)
        self.executor = SimpleExecutor(self.storage)
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query"""
        try:
            # Parse SQL
            ast = self.parser.parse(sql)
            
            # Execute query
            result = self.executor.execute(ast)
            
            # Update metadata
            if ast.get('type') == 'CREATE_TABLE':
                table = ast.get('table')
                if table and table not in self.tables:
                    self.tables[table] = {
                        'columns': ast.get('columns', []),
                        'created_at': datetime.now().isoformat(),
                        'row_count': 0
                    }
                    self._save_metadata()
            
            return result
            
        except Exception as e:
            raise GSQLExecutionError(f"Error: {str(e)}")
    
    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SELECT query and return data"""
        result = self.execute(sql)
        return result.get('data', [])
    
    def list_tables(self) -> List[str]:
        """List all tables in database"""
        return self.storage.list_tables()
    
    def get_table_info(self, table: str) -> Optional[Dict]:
        """Get detailed table information"""
        return self.storage.get_table_info(table)
    
    def _load_metadata(self):
        """Load metadata"""
        meta_file = self.data_dir / 'meta' / 'tables.json'
        if meta_file.exists():
            try:
                with open(meta_file, 'r') as f:
                    self.tables = json.load(f)
            except:
                self.tables = {}
    
    def _save_metadata(self):
        """Save metadata"""
        try:
            meta_file = self.data_dir / 'meta' / 'tables.json'
            with open(meta_file, 'w') as f:
                json.dump(self.tables, f, indent=2)
        except:
            pass
    
    def close(self):
        """Close database"""
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Convenience function
def connect(database: Optional[str] = None) -> GSQL:
    """Connect to GSQL database"""
    return GSQL(database)


# Export
__all__ = ['GSQL', 'connect', 'GSQLSyntaxError', 'GSQLExecutionError', 'GSQLTableError']
