import logging
from typing import Dict, Any, List, Tuple
from .exceptions import SQLExecutionError, FunctionError, NLError
from .nlp.translator import nl_to_sql

logger = logging.getLogger(__name__)

class QueryExecutor:
    """Exécuteur de requêtes avec support avancé"""
    
    def __init__(self, database, function_manager=None, nlp_translator=None):
        self.database = database
        self.function_manager = function_manager
        self.nlp_translator = nlp_translator
        self.prepared_statements = {}
        
        # Cache des résultats
        self.query_cache = {}
        self.cache_max_size = 100
    
    def execute(self, sql: str, params: Dict = None, use_cache: bool = True) -> Any:
        """
        Exécute une requête SQL ou une commande en langage naturel
        
        Args:
            sql (str): Requête SQL ou texte en langage naturel
            params (Dict): Paramètres pour les requêtes préparées
            use_cache (bool): Utiliser le cache des requêtes
            
        Returns:
            Résultats de la requête
        """
        # Vérifier si c'est du langage naturel
        if self._is_natural_language(sql):
            sql = self._translate_nl_to_sql(sql)
            logger.info(f"Translated to SQL: {sql}")
        
        # Vérifier le cache
        cache_key = self._generate_cache_key(sql, params)
        if use_cache and cache_key in self.query_cache:
            logger.debug(f"Cache hit for key: {cache_key}")
            return self.query_cache[cache_key]
        
        # Parsing et exécution
        try:
            parsed = self.database.parser.parse(sql)
            
            # Exécution selon le type
            if parsed['type'] == 'create_function':
                result = self._execute_create_function(parsed)
            elif parsed['type'] == 'select':
                result = self._execute_select_with_functions(parsed)
            elif parsed['type'] == 'call':
                result = self._execute_function_call(parsed)
            else:
                result = self.database.execute(sql)  # Fallback
            
            # Mise en cache
            if use_cache and parsed['type'] in ['select', 'call']:
                self._add_to_cache(cache_key, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Execution error: {str(e)}")
            raise SQLExecutionError(f"Execution failed: {str(e)}")
    
    def _is_natural_language(self, text: str) -> bool:
        """Détecte si le texte est en langage naturel"""
        # Mots-clés SQL qui indiquent que c'est déjà du SQL
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 
                       'ALTER', 'WHERE', 'FROM', 'JOIN', 'GROUP BY', 'ORDER BY']
        
        text_upper = text.upper()
        has_sql_keyword = any(keyword in text_upper for keyword in sql_keywords)
        
        # Si pas de mot-clé SQL et assez long, probablement du NL
        return not has_sql_keyword and len(text.split()) > 2
    
    def _translate_nl_to_sql(self, nl_text: str) -> str:
        """Traduit le langage naturel en SQL"""
        if not self.nlp_translator:
            # Créer un traducteur par défaut
            from .nlp.translator import NLToSQLTranslator
            self.nlp_translator = NLToSQLTranslator()
        
        try:
            return self.nlp_translator.translate(nl_text)
        except Exception as e:
            raise NLError(f"NL translation failed: {str(e)}")
    
    def _execute_create_function(self, parsed: Dict) -> Any:
        """Exécute CREATE FUNCTION"""
        if not self.function_manager:
            raise FunctionError("Function manager not initialized")
        
        return self.function_manager.create_function(
            name=parsed['name'],
            params=parsed['params'],
            body=parsed['body'],
            return_type=parsed['return_type']
        )
    
    def _execute_select_with_functions(self, parsed: Dict) -> List[Dict]:
        """Exécute SELECT avec appels de fonction"""
        # Récupérer les données de base
        base_query = self._rebuild_base_query(parsed)
        rows = self.database.execute(base_query)
        
        # Appliquer les fonctions
        if 'functions' in parsed and parsed['functions']:
            return self._apply_functions_to_rows(rows, parsed['functions'])
        
        return rows
    
    def _apply_functions_to_rows(self, rows: List[Dict], functions: List[Dict]) -> List[Dict]:
        """Applique les fonctions aux résultats"""
        if not self.function_manager:
            return rows
        
        results = []
        
        for row in rows:
            result_row = {}
            
            # Copier les colonnes existantes
            for key, value in row.items():
                result_row[key] = value
            
            # Appliquer les fonctions
            for func in functions:
                # Évaluer les arguments
                args = []
                for arg_expr in func['args']:
                    # Si l'argument fait référence à une colonne
                    if arg_expr in row:
                        args.append(row[arg_expr])
                    else:
                        # C'est une valeur littérale ou une expression
                        args.append(self._evaluate_expression(arg_expr, row))
                
                # Appeler la fonction
                func_result = self.function_manager.registry.call(
                    name=func['name'],
                    args=args,
                    context={'row': row}
                )
                
                # Nom de la colonne résultat
                col_name = func.get('alias', f"{func['name']}_{len(args)}")
                result_row[col_name] = func_result
            
            results.append(result_row)
        
        return results
    
    def _evaluate_expression(self, expr: str, context: Dict) -> Any:
        """Évalue une expression simple"""
        # Pour l'instant, retourne l'expression telle quelle
        # Une implémentation complète devrait évaluer les expressions arithmétiques
        try:
            # Essayer d'évaluer comme une expression Python
            return eval(expr, {}, context)
        except:
            return expr
    
    def prepare(self, name: str, sql: str):
        """Prépare une requête pour exécution ultérieure"""
        parsed = self.database.parser.parse(sql)
        self.prepared_statements[name] = {
            'sql': sql,
            'parsed': parsed,
            'params': self._extract_parameters(parsed)
        }
        return f"Statement '{name}' prepared"
    
    def execute_prepared(self, name: str, params: Dict = None):
        """Exécute une requête préparée"""
        if name not in self.prepared_statements:
            raise SQLExecutionError(f"Prepared statement '{name}' not found")
        
        stmt = self.prepared_statements[name]
        
        # Fusionner les paramètres
        if params:
            sql = self._bind_parameters(stmt['sql'], params)
            return self.execute(sql, use_cache=False)
        else:
            return self.execute(stmt['sql'], use_cache=False)
    
    def _generate_cache_key(self, sql: str, params: Dict = None) -> str:
        """Génère une clé de cache pour une requête"""
        import hashlib
        key_data = sql + (str(params) if params else "")
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _add_to_cache(self, key: str, result: Any):
        """Ajoute un résultat au cache"""
        if len(self.query_cache) >= self.cache_max_size:
            # Éviction LRU simple
            self.query_cache.pop(next(iter(self.query_cache)))
        
        self.query_cache[key] = result
    
    def clear_cache(self):
        """Vide le cache des requêtes"""
        self.query_cache.clear()
        return "Query cache cleared"
