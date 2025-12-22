import re
from typing import Dict, Any, List, Tuple, Optional
from .exceptions import SQLSyntaxError, FunctionError

class SQLParser:
    """Parser SQL étendu avec support des fonctions"""
    
    def __init__(self, function_manager=None):
        self.function_manager = function_manager
    
    def parse_create_function(self, sql: str) -> Dict[str, Any]:
        """
        Parse CREATE FUNCTION statement
        
        Syntaxe:
        CREATE FUNCTION nom_fonction(param1 TYPE, param2 TYPE)
        RETURNS TYPE
        AS $$
        code_python
        $$ LANGUAGE plpython;
        """
        pattern = r"""
            CREATE\s+FUNCTION\s+
            (\w+)                           # nom fonction
            \s*\((.*?)\)                    # paramètres
            \s*RETURNS\s+(\w+)              # type retour
            \s*AS\s+\$\$(.*?)\$\$          # corps fonction
            \s*LANGUAGE\s+(\w+)             # langage
            ;?
        """
        
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL | re.VERBOSE)
        if not match:
            raise SQLSyntaxError("Invalid CREATE FUNCTION syntax")
        
        name = match.group(1)
        params_str = match.group(2).strip()
        return_type = match.group(3).upper()
        body = match.group(4).strip()
        language = match.group(5).lower()
        
        # Validation du langage
        if language not in ['plpython', 'python']:
            raise SQLSyntaxError(f"Unsupported language: {language}")
        
        # Extraction des paramètres
        params = []
        if params_str:
            for param in params_str.split(','):
                param = param.strip()
                if not param:
                    continue
                
                # Format: nom TYPE ou juste nom
                parts = param.split()
                if len(parts) == 2:
                    param_name = parts[0]
                    param_type = parts[1].upper()
                elif len(parts) == 1:
                    param_name = parts[0]
                    param_type = 'ANY'
                else:
                    raise SQLSyntaxError(f"Invalid parameter: {param}")
                
                params.append({
                    'name': param_name,
                    'type': param_type
                })
        
        return {
            'type': 'create_function',
            'name': name,
            'params': [p['name'] for p in params],
            'param_types': [p['type'] for p in params],
            'return_type': return_type,
            'body': body,
            'language': language
        }
    
    def parse_select_with_functions(self, sql: str) -> Dict[str, Any]:
        """Parse SELECT avec support des fonctions"""
        # Détection des appels de fonction
        func_pattern = r'(\w+)\s*\((.*?)\)'
        
        # Analyse de la clause SELECT
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            
            # Recherche des fonctions
            functions = []
            columns = []
            
            for item in select_clause.split(','):
                item = item.strip()
                func_match = re.match(r'(\w+)\s*\((.*)\)', item)
                
                if func_match:
                    func_name = func_match.group(1)
                    func_args = func_match.group(2)
                    
                    # Extraction des arguments
                    args = []
                    for arg in self._split_arguments(func_args):
                        args.append(arg.strip())
                    
                    functions.append({
                        'name': func_name,
                        'args': args,
                        'alias': self._extract_alias(item)
                    })
                else:
                    columns.append({
                        'name': item,
                        'alias': self._extract_alias(item)
                    })
            
            return {
                'type': 'select',
                'columns': columns,
                'functions': functions,
                # ... reste de l'analyse ...
            }
        
        return self.parse_select(sql) if hasattr(self, 'parse_select') else {'type': 'select'}
    
    def _split_arguments(self, args_str: str) -> List[str]:
        """Sépare les arguments en respectant les sous-expressions"""
        args = []
        current = []
        paren_depth = 0
        
        for char in args_str:
            if char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif char == ',' and paren_depth == 0:
                args.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        
        if current:
            args.append(''.join(current).strip())
        
        return args
    
    def _extract_alias(self, expr: str) -> Optional[str]:
        """Extrait l'alias AS d'une expression"""
        match = re.search(r'AS\s+(\w+)$', expr, re.IGNORECASE)
        return match.group(1) if match else None
    
    def parse(self, sql: str) -> Dict[str, Any]:
        """
        Parse generic SQL statement
        
        Args:
            sql (str): SQL statement
            
        Returns:
            Dict: Parsed SQL structure
        """
        sql = sql.strip()
        
        # Check for CREATE FUNCTION
        if sql.upper().startswith('CREATE FUNCTION'):
            return self.parse_create_function(sql)
        
        # Check for SELECT with functions
        elif sql.upper().startswith('SELECT'):
            return self.parse_select_with_functions(sql)
        
        # Default parsing for other statements
        else:
            # Simple keyword detection
            sql_upper = sql.upper()
            if sql_upper.startswith('INSERT'):
                return {'type': 'insert', 'sql': sql}
            elif sql_upper.startswith('UPDATE'):
                return {'type': 'update', 'sql': sql}
            elif sql_upper.startswith('DELETE'):
                return {'type': 'delete', 'sql': sql}
            elif sql_upper.startswith('CREATE TABLE'):
                return {'type': 'create_table', 'sql': sql}
            elif sql_upper.startswith('DROP TABLE'):
                return {'type': 'drop_table', 'sql': sql}
            else:
                return {'type': 'unknown', 'sql': sql}
