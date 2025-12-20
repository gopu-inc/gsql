# gsql/gsql/database.py - Version améliorée
"""
GSQL Database - Improved Version
"""

import json
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

class GSQLSyntaxError(Exception):
    pass

class GSQLExecutionError(Exception):
    pass

class GSQL:
    """GSQL Database - Complete Implementation"""
    
    def __init__(self, path: Optional[str] = None):
        self.path = path or "gsql.db"
        self.data_dir = Path(self.path).parent / f"{self.path}_data"
        self.data_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (self.data_dir / 'tables').mkdir(exist_ok=True)
        (self.data_dir / 'meta').mkdir(exist_ok=True)
        
        # Initialize parser and storage
        self._init_components()
        
        # Metadata cache
        self.tables_meta = {}
        self._load_metadata()
    
    def _init_components(self):
        """Initialize parser and storage"""
        # Improved Parser
        class SQLParser:
            def parse(self, sql: str) -> Dict:
                sql = sql.strip()
                sql_upper = sql.upper()
                
                if sql_upper.startswith('CREATE TABLE'):
                    return self._parse_create_table(sql)
                elif sql_upper.startswith('INSERT INTO'):
                    return self._parse_insert(sql)
                elif sql_upper.startswith('SELECT'):
                    return self._parse_select(sql)
                elif sql_upper.startswith('DELETE FROM'):
                    return self._parse_delete(sql)
                else:
                    raise GSQLSyntaxError(f"Unsupported SQL: {sql}")
            
            def _parse_create_table(self, sql: str) -> Dict:
                """Parse CREATE TABLE statement"""
                # Pattern: CREATE TABLE table_name (col1 type1, col2 type2, ...)
                pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*)\)'
                match = re.match(pattern, sql, re.IGNORECASE)
                
                if not match:
                    raise GSQLSyntaxError("Invalid CREATE TABLE syntax")
                
                table_name = match.group(1)
                columns_str = match.group(2)
                
                # Parse columns
                columns = []
                for col_def in self._split_by_comma(columns_str):
                    col_def = col_def.strip()
                    parts = col_def.split()
                    
                    if len(parts) < 2:
                        raise GSQLSyntaxError(f"Invalid column definition: {col_def}")
                    
                    col_name = parts[0]
                    col_type = parts[1].upper()
                    
                    # Parse constraints
                    constraints = []
                    for part in parts[2:]:
                        if part.upper() == 'PRIMARY' and 'KEY' in parts:
                            constraints.append('PRIMARY_KEY')
                        elif part.upper() == 'NOT' and 'NULL' in parts:
                            constraints.append('NOT_NULL')
                        elif part.upper() == 'UNIQUE':
                            constraints.append('UNIQUE')
                    
                    columns.append({
                        'name': col_name,
                        'type': self._normalize_type(col_type),
                        'constraints': constraints
                    })
                
                return {
                    'type': 'CREATE_TABLE',
                    'table': table_name,
                    'columns': columns
                }
            
            def _parse_insert(self, sql: str) -> Dict:
                """Parse INSERT INTO statement"""
                # Pattern: INSERT INTO table (col1, col2) VALUES (val1, val2)
                pattern = r'INSERT\s+INTO\s+(\w+)\s*(?:\(([^)]+)\))?\s*VALUES\s*(.+)'
                match = re.match(pattern, sql, re.IGNORECASE)
                
                if not match:
                    raise GSQLSyntaxError("Invalid INSERT syntax")
                
                table_name = match.group(1)
                columns_str = match.group(2)
                values_str = match.group(3)
                
                # Parse columns
                columns = []
                if columns_str:
                    columns = [col.strip() for col in columns_str.split(',')]
                
                # Parse values
                values = self._parse_values(values_str)
                
                return {
                    'type': 'INSERT',
                    'table': table_name,
                    'columns': columns,
                    'values': values
                }
            
            def _parse_select(self, sql: str) -> Dict:
                """Parse SELECT statement"""
                # Pattern: SELECT columns FROM table WHERE conditions
                pattern = r'SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
                match = re.match(pattern, sql, re.IGNORECASE)
                
                if not match:
                    raise GSQLSyntaxError("Invalid SELECT syntax")
                
                columns_str = match.group(1)
                table_name = match.group(2)
                where_str = match.group(3)
                
                # Parse columns
                if columns_str.strip() == '*':
                    columns = ['*']
                else:
                    columns = [col.strip() for col in columns_str.split(',')]
                
                # Parse WHERE (simplified)
                where = None
                if where_str:
                    where = self._parse_where(where_str)
                
                return {
                    'type': 'SELECT',
                    'table': table_name,
                    'columns': columns,
                    'where': where
                }
            
            def _parse_delete(self, sql: str) -> Dict:
                """Parse DELETE statement"""
                pattern = r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
                match = re.match(pattern, sql, re.IGNORECASE)
                
                if not match:
                    raise GSQLSyntaxError("Invalid DELETE syntax")
                
                table_name = match.group(1)
                where_str = match.group(2)
                
                where = None
                if where_str:
                    where = self._parse_where(where_str)
                
                return {
                    'type': 'DELETE',
                    'table': table_name,
                    'where': where
                }
            
            def _parse_values(self, values_str: str) -> List[List[Any]]:
                """Parse VALUES clause"""
                rows = []
                
                # Find all value tuples
                tuples = re.findall(r'\(([^)]+)\)', values_str)
                
                for tup in tuples:
                    values = []
                    for val in self._split_by_comma(tup):
                        values.append(self._parse_value(val.strip()))
                    rows.append(values)
                
                return rows
            
            def _parse_where(self, where_str: str) -> Dict:
                """Parse WHERE clause (simplified)"""
                # Simple equality for now
                conditions = []
                
                # Split by AND
                and_parts = where_str.split(' AND ')
                
                for part in and_parts:
                    part = part.strip()
                    # Pattern: column = value
                    match = re.match(r'(\w+)\s*=\s*(.+)', part, re.IGNORECASE)
                    if match:
                        col = match.group(1)
                        value = self._parse_value(match.group(2).strip())
                        conditions.append({
                            'column': col,
                            'operator': '=',
                            'value': value
                        })
                
                return {'conditions': conditions} if conditions else None
            
            def _parse_value(self, val: str) -> Any:
                """Parse a SQL value"""
                # String
                if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                    return val[1:-1]
                # Number
                elif val.replace('.', '').replace('-', '').isdigit():
                    if '.' in val:
                        return float(val)
                    else:
                        return int(val)
                # Boolean
                elif val.upper() in ('TRUE', 'FALSE'):
                    return val.upper() == 'TRUE'
                # NULL
                elif val.upper() == 'NULL':
                    return None
                else:
                    # Try as number
                    try:
                        return float(val) if '.' in val else int(val)
                    except ValueError:
                        return val
            
            def _split_by_comma(self, s: str) -> List[str]:
                """Split by comma, ignoring commas in strings"""
                parts = []
                current = []
                in_string = False
                string_char = None
                
                for char in s:
                    if char in ("'", '"'):
                        if not in_string:
                            in_string = True
                            string_char = char
                        elif char == string_char:
                            in_string = False
                            string_char = None
                        current.append(char)
                    elif char == ',' and not in_string:
                        parts.append(''.join(current).strip())
                        current = []
                    else:
                        current.append(char)
                
                if current:
                    parts.append(''.join(current).strip())
                
                return parts
            
            def _normalize_type(self, type_str: str) -> str:
                """Normalize data type"""
                type_map = {
                    'INT': 'integer',
                    'INTEGER': 'integer',
                    'BIGINT': 'bigint',
                    'FLOAT': 'float',
                    'DOUBLE': 'double',
                    'TEXT': 'text',
                    'VARCHAR': 'text',
                    'STRING': 'text',
                    'BOOLEAN': 'boolean',
                    'BOOL': 'boolean'
                }
                return type_map.get(type_str.upper(), 'text')
        
        # Improved Storage
        class Storage:
            def __init__(self, data_dir):
                self.data_dir = data_dir
            
            def create_table(self, name: str, columns: List[Dict]) -> None:
                """Create table with metadata"""
                # Create data file
                table_file = self.data_dir / 'tables' / f'{name}.json'
                with open(table_file, 'w') as f:
                    json.dump([], f, indent=2)
                
                # Create metadata file
                meta_file = self.data_dir / 'meta' / f'{name}.json'
                with open(meta_file, 'w') as f:
                    json.dump({
                        'name': name,
                        'columns': columns,
                        'created_at': datetime.now().isoformat(),
                        'modified_at': datetime.now().isoformat(),
                        'row_count': 0,
                        'next_id': 1
                    }, f, indent=2)
            
            def insert(self, table: str, data: Dict) -> int:
                """Insert data with proper column mapping"""
                table_file = self.data_dir / 'tables' / f'{table}.json'
                meta_file = self.data_dir / 'meta' / f'{table}.json'
                
                if not meta_file.exists():
                    raise GSQLExecutionError(f"Table '{table}' doesn't exist")
                
                # Load metadata
                with open(meta_file, 'r') as f:
                    meta = json.load(f)
                
                # Load data
                if table_file.exists():
                    with open(table_file, 'r') as f:
                        rows = json.load(f)
                else:
                    rows = []
                
                # Assign ID
                row_id = meta['next_id']
                data['_id'] = row_id
                data['_created'] = datetime.now().isoformat()
                
                rows.append(data)
                
                # Save data
                with open(table_file, 'w') as f:
                    json.dump(rows, f, indent=2)
                
                # Update metadata
                meta['next_id'] += 1
                meta['row_count'] = len(rows)
                meta['modified_at'] = datetime.now().isoformat()
                
                with open(meta_file, 'w') as f:
                    json.dump(meta, f, indent=2)
                
                return row_id
            
            def select(self, table: str, where: Dict = None, 
                      columns: List[str] = None, limit: int = None) -> List[Dict]:
                """Select data with filtering"""
                table_file = self.data_dir / 'tables' / f'{table}.json'
                
                if not table_file.exists():
                    return []
                
                with open(table_file, 'r') as f:
                    rows = json.load(f)
                
                results = []
                for row in rows:
                    # Apply WHERE filter
                    if where and not self._matches_where(row, where):
                        continue
                    
                    # Project columns
                    if columns and columns != ['*']:
                        projected = {col: row.get(col) for col in columns if col in row}
                        results.append(projected)
                    else:
                        # Remove internal fields
                        filtered = {k: v for k, v in row.items() if not k.startswith('_')}
                        results.append(filtered)
                    
                    # Apply limit
                    if limit and len(results) >= limit:
                        break
                
                return results
            
            def _matches_where(self, row: Dict, where: Dict) -> bool:
                """Check if row matches WHERE conditions"""
                for col, val in where.items():
                    if col not in row or row[col] != val:
                        return False
                return True
            
            def list_tables(self) -> List[str]:
                """List all tables"""
                tables_dir = self.data_dir / 'tables'
                if not tables_dir.exists():
                    return []
                return [f.stem for f in tables_dir.glob('*.json')]
            
            def get_table_info(self, table: str) -> Dict:
                """Get table metadata"""
                meta_file = self.data_dir / 'meta' / f'{table}.json'
                if meta_file.exists():
                    with open(meta_file, 'r') as f:
                        return json.load(f)
                return None
        
        # Executor
        class Executor:
            def __init__(self, storage):
                self.storage = storage
            
            def execute(self, ast: Dict) -> Dict:
                """Execute AST"""
                query_type = ast.get('type')
                
                if query_type == 'CREATE_TABLE':
                    table = ast['table']
                    columns = ast['columns']
                    self.storage.create_table(table, columns)
                    return {
                        'type': 'CREATE_TABLE',
                        'table': table,
                        'columns': len(columns)
                    }
                
                elif query_type == 'INSERT':
                    table = ast['table']
                    columns = ast.get('columns', [])
                    values_list = ast['values']
                    
                    rows_inserted = 0
                    for values in values_list:
                        # Map values to columns
                        if columns:
                            row_data = {}
                            for i, col in enumerate(columns):
                                if i < len(values):
                                    row_data[col] = values[i]
                        else:
                            # No columns specified, use positional mapping
                            # Need table metadata to know column names
                            meta = self.storage.get_table_info(table)
                            if meta and 'columns' in meta:
                                table_columns = meta['columns']
                                row_data = {}
                                for i, col_def in enumerate(table_columns):
                                    if i < len(values):
                                        row_data[col_def['name']] = values[i]
                            else:
                                # Fallback: use generic names
                                row_data = {f'col_{i}': val for i, val in enumerate(values)}
                        
                        self.storage.insert(table, row_data)
                        rows_inserted += 1
                    
                    return {
                        'type': 'INSERT',
                        'table': table,
                        'rows_affected': rows_inserted
                    }
                
                elif query_type == 'SELECT':
                    table = ast['table']
                    columns = ast['columns']
                    where_ast = ast.get('where')
                    
                    # Convert WHERE AST to simple dict
                    where = None
                    if where_ast and where_ast.get('conditions'):
                        where = {}
                        for cond in where_ast['conditions']:
                            if cond['operator'] == '=':
                                where[cond['column']] = cond['value']
                    
                    data = self.storage.select(table, where, columns)
                    
                    return {
                        'type': 'SELECT',
                        'table': table,
                        'data': data,
                        'row_count': len(data)
                    }
                
                elif query_type == 'DELETE':
                    table = ast['table']
                    where_ast = ast.get('where')
                    
                    # For now, just return placeholder
                    return {
                        'type': 'DELETE',
                        'table': table,
                        'rows_affected': 0
                    }
                
                else:
                    raise GSQLExecutionError(f"Unsupported query type: {query_type}")
        
        self.parser = SQLParser()
        self.storage = Storage(self.data_dir)
        self.executor = Executor(self.storage)
    
    def _load_metadata(self):
        """Load table metadata"""
        meta_dir = self.data_dir / 'meta'
        if meta_dir.exists():
            for meta_file in meta_dir.glob('*.json'):
                with open(meta_file, 'r') as f:
                    meta = json.load(f)
                    self.tables_meta[meta['name']] = meta
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query"""
        try:
            # Parse
            ast = self.parser.parse(sql)
            
            # Execute
            result = self.executor.execute(ast)
            
            # Update metadata cache
            if ast.get('type') == 'CREATE_TABLE':
                table = ast['table']
                if table not in self.tables_meta:
                    self.tables_meta[table] = {
                        'name': table,
                        'columns': ast['columns'],
                        'created_at': datetime.now().isoformat(),
                        'row_count': 0
                    }
            
            return result
            
        except GSQLSyntaxError as e:
            raise e
        except Exception as e:
            raise GSQLExecutionError(f"Execution error: {str(e)}")
    
    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SELECT query"""
        result = self.execute(sql)
        return result.get('data', [])
    
    def list_tables(self) -> List[str]:
        """List all tables"""
        return self.storage.list_tables()
    
    def get_table_info(self, table: str) -> Dict:
        """Get table information"""
        return self.storage.get_table_info(table)
    
    def close(self):
        """Close database"""
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def connect(path: Optional[str] = None) -> GSQL:
    """Connect to GSQL database"""
    return GSQL(path)

__all__ = ['GSQL', 'connect', 'GSQLSyntaxError', 'GSQLExecutionError']
