# gsql/parser.py
import re
from typing import Dict, List, Any, Optional, Tuple
from .exceptions import GSQLSyntaxError

class SQLParser:
    """Parser SQL simple mais puissant"""
    
    def __init__(self):
        # Regex pour les commandes SQL
        self.patterns = {
            'create_table': r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)',
            'insert': r'INSERT\s+INTO\s+(\w+)\s*(?:\(([^)]+)\))?\s*VALUES\s*(.*)',
            'select': r'SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?',
            'delete': r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?',
            'update': r'UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?',
            'create_index': r'CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\((.*?)\)',
        }
        
        # Types de données supportés
        self.data_types = {
            'INT': 'integer',
            'INTEGER': 'integer',
            'BIGINT': 'bigint',
            'FLOAT': 'float',
            'DOUBLE': 'double',
            'TEXT': 'text',
            'VARCHAR': 'varchar',
            'BOOLEAN': 'boolean',
            'JSON': 'json',
            'DATETIME': 'datetime',
        }
    
    def parse(self, sql: str) -> Dict[str, Any]:
        """Parser une requête SQL en AST"""
        sql = sql.strip().upper()
        
        if sql.startswith('CREATE TABLE'):
            return self._parse_create_table(sql)
        elif sql.startswith('INSERT INTO'):
            return self._parse_insert(sql)
        elif sql.startswith('SELECT'):
            return self._parse_select(sql)
        elif sql.startswith('DELETE FROM'):
            return self._parse_delete(sql)
        elif sql.startswith('UPDATE'):
            return self._parse_update(sql)
        elif sql.startswith('CREATE INDEX'):
            return self._parse_create_index(sql)
        else:
            raise GSQLSyntaxError(f"Commande non supportée: {sql}")
    
    def _parse_create_table(self, sql: str) -> Dict[str, Any]:
        """Parser CREATE TABLE"""
        match = re.search(self.patterns['create_table'], sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise GSQLSyntaxError("Syntaxe CREATE TABLE invalide")
        
        table_name = match.group(1)
        columns_def = match.group(2).strip()
        
        # Parser les colonnes
        columns = []
        for col_def in self._split_by_comma(columns_def):
            col_parts = col_def.strip().split()
            if len(col_parts) < 2:
                raise GSQLSyntaxError(f"Définition de colonne invalide: {col_def}")
            
            col_name = col_parts[0]
            col_type = col_parts[1].upper()
            
            # Extraire les contraintes
            constraints = []
            for part in col_parts[2:]:
                if part.upper() == 'PRIMARY KEY':
                    constraints.append('PRIMARY_KEY')
                elif part.upper() == 'NOT NULL':
                    constraints.append('NOT_NULL')
                elif part.upper() == 'UNIQUE':
                    constraints.append('UNIQUE')
            
            columns.append({
                'name': col_name,
                'type': self.data_types.get(col_type, 'text'),
                'original_type': col_type,
                'constraints': constraints
            })
        
        return {
            'type': 'CREATE_TABLE',
            'table': table_name,
            'columns': columns
        }
    
    def _parse_insert(self, sql: str) -> Dict[str, Any]:
        """Parser INSERT INTO"""
        match = re.search(self.patterns['insert'], sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise GSQLSyntaxError("Syntaxe INSERT invalide")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        values_str = match.group(3)
        
        # Parser les colonnes (optionnel)
        columns = []
        if columns_str:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parser les valeurs
        values = self._parse_values(values_str)
        
        return {
            'type': 'INSERT',
            'table': table_name,
            'columns': columns,
            'values': values
        }
    
    def _parse_select(self, sql: str) -> Dict[str, Any]:
        """Parser SELECT"""
        match = re.search(self.patterns['select'], sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise GSQLSyntaxError("Syntaxe SELECT invalide")
        
        columns_str = match.group(1)
        table_name = match.group(2)
        where_str = match.group(3)
        
        # Parser les colonnes
        columns = []
        if columns_str.strip() == '*':
            columns = ['*']
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parser WHERE
        where = None
        if where_str:
            where = self._parse_where(where_str)
        
        return {
            'type': 'SELECT',
            'table': table_name,
            'columns': columns,
            'where': where
        }
    
    def _parse_where(self, where_str: str) -> Dict[str, Any]:
        """Parser la clause WHERE"""
        # Support pour AND, OR, =, !=, >, <, >=, <=, LIKE
        conditions = []
        
        # Séparer par AND/OR
        parts = re.split(r'\s+(AND|OR)\s+', where_str, flags=re.IGNORECASE)
        
        for i in range(0, len(parts), 2):
            condition = parts[i].strip()
            operator = parts[i+1].upper() if i+1 < len(parts) else None
            
            # Parser chaque condition
            cond_match = re.match(r'(\w+)\s*([=!<>]+|LIKE|IN)\s*(.+)', condition)
            if cond_match:
                col = cond_match.group(1)
                op = cond_match.group(2)
                value = self._parse_value(cond_match.group(3).strip())
                
                conditions.append({
                    'column': col,
                    'operator': op,
                    'value': value,
                    'connector': operator
                })
        
        return {'conditions': conditions} if conditions else None
    
    def _parse_values(self, values_str: str) -> List[List[Any]]:
        """Parser les valeurs INSERT"""
        # Support pour: (1, 'Alice'), (2, 'Bob')
        rows = []
        
        # Trouver toutes les tuples de valeurs
        tuples = re.findall(r'\(([^)]+)\)', values_str)
        
        for tup in tuples:
            values = []
            for val in self._split_by_comma(tup):
                values.append(self._parse_value(val.strip()))
            rows.append(values)
        
        return rows
    
    def _parse_value(self, val: str) -> Any:
        """Parser une valeur SQL"""
        # Chaîne
        if val.startswith("'") and val.endswith("'"):
            return val[1:-1]
        # Nombre
        elif val.replace('.', '').isdigit():
            if '.' in val:
                return float(val)
            else:
                return int(val)
        # Booléen
        elif val.upper() in ['TRUE', 'FALSE']:
            return val.upper() == 'TRUE'
        # NULL
        elif val.upper() == 'NULL':
            return None
        # JSON
        elif val.startswith('{') or val.startswith('['):
            try:
                return json.loads(val)
            except:
                return val
        else:
            return val
    
    def _split_by_comma(self, s: str) -> List[str]:
        """Diviser par virgule, en ignorant les virgules dans les strings"""
        parts = []
        current = []
        in_string = False
        string_char = None
        
        for char in s:
            if char in ["'", '"']:
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
