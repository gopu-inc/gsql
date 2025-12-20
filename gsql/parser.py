# gsql/gsql/parser.py
"""
SQL Parser in pure Python
"""

import re

class SQLParser:
    """Parse SQL queries"""
    
    def __init__(self):
        self.keywords = {
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
            'CREATE', 'TABLE', 'DELETE', 'UPDATE', 'SET',
            'INT', 'TEXT', 'FLOAT', 'BOOLEAN', 'PRIMARY', 'KEY',
            'NOT', 'NULL', 'AND', 'OR', 'LIKE', 'ORDER', 'BY',
            'GROUP', 'HAVING', 'LIMIT', 'OFFSET'
        }
    
    def parse(self, sql):
        """Parse SQL query"""
        sql = sql.strip()
        
        # Remove multiple spaces
        sql = re.sub(r'\s+', ' ', sql)
        
        # Convert to uppercase for parsing (but preserve strings)
        parts = []
        in_string = False
        string_char = None
        current = []
        
        for char in sql:
            if char in ("'", '"') and not in_string:
                in_string = True
                string_char = char
                current.append(char)
            elif char == string_char and in_string:
                in_string = False
                string_char = None
                current.append(char)
            elif not in_string and char == ' ':
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)
        
        if current:
            parts.append(''.join(current))
        
        # Reconstruct with keywords in uppercase
        result_parts = []
        for part in parts:
            if not in_string and part.upper() in self.keywords:
                result_parts.append(part.upper())
            else:
                result_parts.append(part)
        
        sql_upper = ' '.join(result_parts)
        
        # Parse based on first keyword
        if sql_upper.startswith('CREATE TABLE'):
            return self._parse_create_table(sql)
        elif sql_upper.startswith('INSERT INTO'):
            return self._parse_insert(sql)
        elif sql_upper.startswith('SELECT'):
            return self._parse_select(sql)
        elif sql_upper.startswith('DELETE FROM'):
            return self._parse_delete(sql)
        elif sql_upper.startswith('UPDATE'):
            return self._parse_update(sql)
        else:
            from .exceptions import GQLSyntaxError
            raise GQLSyntaxError(f"Unsupported SQL: {sql}")
    
    def _parse_create_table(self, sql):
        """Parse CREATE TABLE"""
        # Simple regex for MVP
        pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*)\)'
        match = re.match(pattern, sql, re.IGNORECASE)
        
        if not match:
            from .exceptions import GQLSyntaxError
            raise GQLSyntaxError("Invalid CREATE TABLE syntax")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        
        # Parse columns
        columns = []
        col_defs = self._split_by_comma(columns_str)
        
        for col_def in col_defs:
            col_def = col_def.strip()
            parts = col_def.split()
            
            if len(parts) < 2:
                from .exceptions import GQLSyntaxError
                raise GQLSyntaxError(f"Invalid column: {col_def}")
            
            col_name = parts[0]
            col_type = parts[1].upper()
            
            # Parse constraints
            constraints = []
            i = 2
            while i < len(parts):
                if parts[i].upper() == 'PRIMARY' and i+1 < len(parts) and parts[i+1].upper() == 'KEY':
                    constraints.append('PRIMARY_KEY')
                    i += 2
                elif parts[i].upper() == 'NOT' and i+1 < len(parts) and parts[i+1].upper() == 'NULL':
                    constraints.append('NOT_NULL')
                    i += 2
                elif parts[i].upper() == 'UNIQUE':
                    constraints.append('UNIQUE')
                    i += 1
                else:
                    i += 1
            
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
    
    def _parse_insert(self, sql):
        """Parse INSERT INTO"""
        # Pattern: INSERT INTO table (col1, col2) VALUES (val1, val2), (val3, val4)
        pattern = r'INSERT\s+INTO\s+(\w+)\s*(?:\(([^)]+)\))?\s*VALUES\s*(.+)'
        match = re.match(pattern, sql, re.IGNORECASE)
        
        if not match:
            from .exceptions import GQLSyntaxError
            raise GQLSyntaxError("Invalid INSERT syntax")
        
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
    
    def _parse_select(self, sql):
        """Parse SELECT"""
        # Pattern: SELECT columns FROM table WHERE conditions
        pattern = r'SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
        match = re.match(pattern, sql, re.IGNORECASE)
        
        if not match:
            from .exceptions import GQLSyntaxError
            raise GQLSyntaxError("Invalid SELECT syntax")
        
        columns_str = match.group(1)
        table_name = match.group(2)
        where_str = match.group(3)
        
        # Parse columns
        if columns_str.strip() == '*':
            columns = ['*']
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parse WHERE
        where = None
        if where_str:
            where = self._parse_where(where_str)
        
        return {
            'type': 'SELECT',
            'table': table_name,
            'columns': columns,
            'where': where
        }
    
    def _parse_delete(self, sql):
        """Parse DELETE"""
        pattern = r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
        match = re.match(pattern, sql, re.IGNORECASE)
        
        if not match:
            from .exceptions import GQLSyntaxError
            raise GQLSyntaxError("Invalid DELETE syntax")
        
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
    
    def _parse_update(self, sql):
        """Parse UPDATE"""
        # Not implemented in MVP
        from .exceptions import GQLSyntaxError
        raise GQLSyntaxError("UPDATE not implemented")
    
    def _parse_where(self, where_str):
        """Parse WHERE clause"""
        conditions = []
        
        # Split by AND/OR
        tokens = re.split(r'\s+(AND|OR)\s+', where_str, flags=re.IGNORECASE)
        
        i = 0
        while i < len(tokens):
            condition = tokens[i].strip()
            
            # Parse condition
            cond_match = re.match(r'(\w+)\s*([=<>!]+|LIKE)\s*(.+)', condition)
            if cond_match:
                col = cond_match.group(1)
                op = cond_match.group(2)
                value = self._parse_value(cond_match.group(3).strip())
                
                connector = None
                if i + 1 < len(tokens):
                    connector = tokens[i + 1].upper()
                    i += 1
                
                conditions.append({
                    'column': col,
                    'operator': op,
                    'value': value,
                    'connector': connector
                })
            
            i += 1
        
        return {'conditions': conditions}
    
    def _parse_values(self, values_str):
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
    
    def _parse_value(self, val):
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
            # Try to parse as number
            try:
                return float(val) if '.' in val else int(val)
            except ValueError:
                return val
    
    def _split_by_comma(self, s):
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
    
    def _normalize_type(self, type_str):
        """Normalize data type"""
        type_map = {
            'INT': 'integer',
            'INTEGER': 'integer',
            'BIGINT': 'bigint',
            'FLOAT': 'float',
            'DOUBLE': 'double',
            'REAL': 'real',
            'TEXT': 'text',
            'VARCHAR': 'text',
            'STRING': 'text',
            'BOOLEAN': 'boolean',
            'BOOL': 'boolean',
            'JSON': 'json'
        }
        return type_map.get(type_str.upper(), 'text')
