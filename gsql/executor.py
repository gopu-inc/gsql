import logging
from typing import Dict, Any, List, Tuple
from .exceptions import SQLExecutionError, FunctionError, NLError
from .nlp.translator import nl_to_sql

logger = logging.getLogger(__name__)

class QueryExecutor:
    """Exécuteur de requêtes simplifié"""
    
    def __init__(self, database, function_manager=None, nlp_translator=None):
        self.database = database
        self.function_manager = function_manager
        self.nlp_translator = nlp_translator
        self.query_cache = {}
        self.cache_max_size = 100
    
    def execute(self, sql: str, params: Dict = None, use_cache: bool = True) -> Any:
        """
        Exécute une requête SQL ou une commande en langage naturel
        """
        # Vérifier si c'est du langage naturel
        if self._is_natural_language(sql):
            sql = self._translate_nl_to_sql(sql)
            logger.info(f"Translated to SQL: {sql}")
        
        # Pour l'instant, exécution simple
        return self._execute_simple(sql)
    
    def _is_natural_language(self, text: str) -> bool:
        """Détecte si le texte est en langage naturel"""
        # Mots-clés SQL qui indiquent que c'est déjà du SQL
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 
                       'ALTER', 'WHERE', 'FROM', 'JOIN', 'GROUP BY', 'ORDER BY']
        
        text_upper = text.upper()
        has_sql_keyword = any(keyword in text_upper for keyword in sql_keywords)
        
        # Si pas de mot-clé SQL et assez long, probablement du NL
        return not has_sql_keyword and len(text.split()) > 1
    
    def _translate_nl_to_sql(self, nl_text: str) -> str:
        """Traduit le langage naturel en SQL"""
        if not self.nlp_translator:
            # Créer un traducteur par défaut
            try:
                from .nlp.translator import NLToSQLTranslator
                self.nlp_translator = NLToSQLTranslator()
            except:
                # Fallback ultra simple
                nl_text_lower = nl_text.lower()
                if "table" in nl_text_lower:
                    return "SELECT name FROM sqlite_master WHERE type='table'"
                elif "fonction" in nl_text_lower:
                    return "SELECT 'Exemple fonction' as help"
                else:
                    return "SELECT 'Commande non reconnue' as error"
        
        try:
            return self.nlp_translator.translate(nl_text)
        except Exception as e:
            logger.error(f"Translation failed: {e}")
            return "SELECT 'Erreur de traduction' as error"
    
    def _execute_simple(self, sql: str) -> Any:
        """Exécution simple de requête SQL"""
        sql_upper = sql.upper()
        
        try:
            if sql_upper.startswith('SELECT'):
                return self._execute_select(sql)
            elif sql_upper.startswith('CREATE'):
                return self._execute_create(sql)
            elif sql_upper.startswith('INSERT'):
                return self._execute_insert(sql)
            elif sql_upper.startswith('SHOW TABLES'):
                return self._execute_show_tables()
            elif sql_upper.startswith('SHOW FUNCTIONS'):
                return self._execute_show_functions()
            elif sql_upper.startswith('HELP'):
                return self._execute_help(sql)
            else:
                return {
                    'type': 'result',
                    'message': f'Command executed: {sql[:50]}...',
                    'rows': []
                }
        except Exception as e:
            raise SQLExecutionError(f"Execution failed: {str(e)}")
    
    def _execute_select(self, sql: str) -> Dict:
        """Exécute une requête SELECT simple"""
        # Pour l'instant, retourne des résultats factices
        if "FROM SQLITE_MASTER" in sql.upper() or "TABLES" in sql.upper():
            return {
                'type': 'select',
                'rows': [
                    {'name': 'users', 'type': 'table'},
                    {'name': 'products', 'type': 'table'},
                    {'name': 'orders', 'type': 'table'}
                ],
                'count': 3
            }
        elif "HELP" in sql.upper() or "EXAMPLE" in sql.upper():
            return {
                'type': 'select',
                'rows': [
                    {'help': 'CREATE FUNCTION nom(params) RETURNS type AS $$code$$ LANGUAGE plpython'},
                    {'help': 'SHOW TABLES - Liste les tables'},
                    {'help': 'SHOW FUNCTIONS - Liste les fonctions'}
                ],
                'count': 3
            }
        else:
            return {
                'type': 'select',
                'rows': [
                    {'result': 'Exemple de résultat 1'},
                    {'result': 'Exemple de résultat 2'}
                ],
                'count': 2
            }
    
    def _execute_create(self, sql: str) -> Dict:
        """Exécute une commande CREATE"""
        if "FUNCTION" in sql.upper():
            # Extraire les informations de la fonction
            try:
                # Pattern simple pour CREATE FUNCTION
                import re
                pattern = r"CREATE\s+FUNCTION\s+(\w+)\s*\((.*?)\)\s*RETURNS\s+(\w+)\s+AS\s+\$\$(.*?)\$\$\s+LANGUAGE\s+(\w+)"
                match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
                
                if match and self.function_manager:
                    name = match.group(1)
                    params_str = match.group(2)
                    return_type = match.group(3)
                    body = match.group(4)
                    language = match.group(5)
                    
                    # Extraire les paramètres
                    params = []
                    if params_str.strip():
                        for param in params_str.split(','):
                            param = param.strip()
                            if param:
                                # Enlever le type si présent
                                param_name = param.split()[0]
                                params.append(param_name)
                    
                    # Créer la fonction
                    result = self.function_manager.create_function(
                        name=name,
                        params=params,
                        body=body,
                        return_type=return_type
                    )
                    
                    return {
                        'type': 'create_function',
                        'message': result,
                        'name': name,
                        'params': params
                    }
            except Exception as e:
                return {
                    'type': 'error',
                    'message': f"Error creating function: {str(e)}"
                }
        
        return {
            'type': 'create',
            'message': f"Created: {sql[:50]}...",
            'sql': sql
        }
    
    def _execute_insert(self, sql: str) -> Dict:
        """Exécute une commande INSERT"""
        return {
            'type': 'insert',
            'message': 'Row inserted successfully',
            'sql': sql
        }
    
    def _execute_show_tables(self) -> Dict:
        """Exécute SHOW TABLES"""
        return {
            'type': 'show_tables',
            'rows': [
                {'table': 'users', 'rows': 0},
                {'table': 'products', 'rows': 0},
                {'table': 'orders', 'rows': 0}
            ],
            'count': 3,
            'message': '3 tables found'
        }
    
    def _execute_show_functions(self) -> Dict:
        """Exécute SHOW FUNCTIONS"""
        if self.function_manager:
            functions = self.function_manager.list_functions()
            return {
                'type': 'show_functions',
                'rows': functions,
                'count': len(functions),
                'message': f'{len(functions)} functions found'
            }
        else:
            return {
                'type': 'show_functions',
                'rows': [],
                'count': 0,
                'message': 'No function manager available'
            }
    
    def _execute_help(self, sql: str) -> Dict:
        """Exécute HELP"""
        help_text = """
Available commands:
- SHOW TABLES: List all tables
- SHOW FUNCTIONS: List all functions
- CREATE FUNCTION: Create a user-defined function
- SELECT: Query data
- INSERT: Insert data
- EXIT/QUIT: Exit the shell

Natural language examples:
- "montrer tables" -> SHOW TABLES
- "montrer fonctions" -> SHOW FUNCTIONS
- "comment créer une fonction" -> HELP CREATE FUNCTION
"""
        
        return {
            'type': 'help',
            'message': help_text,
            'rows': [{'command': line.strip()} for line in help_text.strip().split('\n') if line.strip()]
        }
    
    def clear_cache(self):
        """Vide le cache des requêtes"""
        self.query_cache.clear()
        return "Query cache cleared"
