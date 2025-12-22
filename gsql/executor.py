#!/usr/bin/env python3
"""
GSQL Query Executor - Exécuteur unifié avec support SQLite, fonctions et NLP
Version: 3.0 - Unifié et optimisé (SQLite only)
"""

import re
import json
import logging
import time
import threading
import sqlite3
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union
import tempfile
import os

from .exceptions import (
    SQLExecutionError, SQLSyntaxError, FunctionError,
    NLError, GSQLBaseException
)

logger = logging.getLogger(__name__)

class QueryExecutor:
    """Exécuteur de requêtes unifié pour GSQL"""
    
    def __init__(self, storage=None, function_manager=None, 
                 nlp_translator=None, cache_size=100, timeout=30):
        """
        Initialise l'exécuteur de requêtes
        
        Args:
            storage: Instance de SQLiteStorage
            function_manager: Gestionnaire de fonctions
            nlp_translator: Traducteur NL vers SQL
            cache_size: Taille du cache de requêtes
            timeout: Timeout par défaut en secondes
        """
        self.storage = storage
        self.function_manager = function_manager
        
        # Utiliser le traducteur NLP s'il est disponible
        if nlp_translator is not None:
            self.nlp_translator = nlp_translator
        else:
            # Essayer d'importer le traducteur NLP
            try:
                from .nlp.translator import NLToSQLTranslator, nl_to_sql
                self.nlp_translator = NLToSQLTranslator()
                self.nl_to_sql_func = nl_to_sql
            except ImportError:
                self.nlp_translator = None
                self.nl_to_sql_func = None
                logger.warning("NLP translator not available")
        
        # Configuration
        self.config = {
            'cache_size': cache_size,
            'timeout': timeout,
            'auto_translate_nl': True,
            'enable_cache': True,
            'verbose_errors': True,
            'max_recursion_depth': 50
        }
        
        # Caches
        self.query_cache = {}  # Cache des résultats
        self.plan_cache = {}   # Cache des plans d'exécution
        self.prepared_statements = {}  # Statements préparés
        
        # Statistiques
        self.stats = {
            'total_queries': 0,
            'nl_queries': 0,
            'cached_hits': 0,
            'execution_errors': 0,
            'avg_execution_time': 0,
            'start_time': time.time()
        }
        
        # Verrous
        self.lock = threading.RLock()
        self.cache_lock = threading.RLock()
        
        # Initialiser les fonctions intégrées si function_manager est fourni
        if self.function_manager:
            self._register_builtin_functions()
        
        logger.info(f"QueryExecutor initialized (cache_size={cache_size})")
    
    def _register_builtin_functions(self):
        """Enregistre les fonctions intégrées"""
        if not self.function_manager:
            return
            
        try:
            # Fonctions mathématiques
            self.function_manager.register('pow', self._func_pow)
            self.function_manager.register('sqrt', self._func_sqrt)
            self.function_manager.register('ceil', self._func_ceil)
            self.function_manager.register('floor', self._func_floor)
            self.function_manager.register('mod', self._func_mod)
            
            # Fonctions string
            self.function_manager.register('trim', self._func_trim)
            self.function_manager.register('replace', self._func_replace)
            self.function_manager.register('substr', self._func_substr)
            self.function_manager.register('instr', self._func_instr)
            
            # Fonctions date
            self.function_manager.register('date', self._func_date)
            self.function_manager.register('time', self._func_time)
            self.function_manager.register('datetime', self._func_datetime)
            self.function_manager.register('julianday', self._func_julianday)
            
            # Fonctions agrégation (pour compatibilité)
            self.function_manager.register('group_concat', self._func_group_concat)
            
            logger.debug(f"Registered {len(self.function_manager.functions)} built-in functions")
            
        except Exception as e:
            logger.warning(f"Failed to register some built-in functions: {e}")
    
    def _func_pow(self, args, context):
        """Puissance: pow(x, y)"""
        if len(args) != 2:
            raise FunctionError("POW expects 2 arguments")
        return float(args[0]) ** float(args[1])
    
    def _func_sqrt(self, args, context):
        """Racine carrée"""
        if len(args) != 1:
            raise FunctionError("SQRT expects 1 argument")
        return float(args[0]) ** 0.5
    
    def _func_ceil(self, args, context):
        """Arrondi supérieur"""
        import math
        if len(args) != 1:
            raise FunctionError("CEIL expects 1 argument")
        return math.ceil(float(args[0]))
    
    def _func_floor(self, args, context):
        """Arrondi inférieur"""
        import math
        if len(args) != 1:
            raise FunctionError("FLOOR expects 1 argument")
        return math.floor(float(args[0]))
    
    def _func_mod(self, args, context):
        """Modulo"""
        if len(args) != 2:
            raise FunctionError("MOD expects 2 arguments")
        return float(args[0]) % float(args[1])
    
    def _func_trim(self, args, context):
        """Trim des espaces"""
        if len(args) != 1:
            raise FunctionError("TRIM expects 1 argument")
        return str(args[0]).strip()
    
    def _func_replace(self, args, context):
        """Remplacement de texte"""
        if len(args) != 3:
            raise FunctionError("REPLACE expects 3 arguments")
        return str(args[0]).replace(str(args[1]), str(args[2]))
    
    def _func_substr(self, args, context):
        """Sous-chaîne"""
        if len(args) not in [2, 3]:
            raise FunctionError("SUBSTR expects 2 or 3 arguments")
        text = str(args[0])
        start = int(args[1])
        if len(args) == 3:
            length = int(args[2])
            return text[start-1:start-1+length]
        return text[start-1:]
    
    def _func_instr(self, args, context):
        """Position d'une sous-chaîne"""
        if len(args) != 2:
            raise FunctionError("INSTR expects 2 arguments")
        text = str(args[0])
        substr = str(args[1])
        return text.find(substr) + 1  # SQLite utilise 1-based index
    
    def _func_date(self, args, context):
        """Date actuelle"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d')
    
    def _func_time(self, args, context):
        """Heure actuelle"""
        from datetime import datetime
        return datetime.now().strftime('%H:%M:%S')
    
    def _func_datetime(self, args, context):
        """Date et heure actuelles"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def _func_julianday(self, args, context):
        """Jour julien"""
        from datetime import datetime
        # Implémentation simplifiée
        return datetime.now().timestamp() / 86400 + 2440587.5
    
    def _func_group_concat(self, args, context):
        """Concaténation de groupe (pour compatibilité)"""
        if not args:
            return ''
        return ','.join(str(arg) for arg in args)
    
    def execute(self, query: str, params: Dict = None, 
                use_nlp: bool = None, use_cache: bool = None) -> Dict:
        """
        Exécute une requête (SQL ou langage naturel)
        
        Args:
            query: Requête SQL ou texte en langage naturel
            params: Paramètres pour requête préparée
            use_nlp: Forcer/désactiver la traduction NL
            use_cache: Forcer/désactiver le cache
        
        Returns:
            Dict: Résultats formatés
        """
        start_time = time.time()
        original_query = query.strip()
        
        # Déterminer les paramètres d'exécution
        use_nlp = use_nlp if use_nlp is not None else self.config['auto_translate_nl']
        use_cache = use_cache if use_cache is not None else self.config['enable_cache']
        
        # Générer un ID de requête pour le cache
        query_id = self._generate_query_id(original_query, params)
        
        # Vérifier le cache
        cached_result = None
        if use_cache:
            cached_result = self._get_from_cache(query_id)
            if cached_result:
                self.stats['cached_hits'] += 1
                logger.debug(f"Cache hit for query: {query_id[:20]}...")
                return cached_result
        
        try:
            # Étape 1: Détection du type de requête
            query_type = self._detect_query_type(original_query)
            
            # Étape 2: Prétraitement
            processed_query = self._preprocess_query(original_query, query_type)
            
            # Étape 3: Traduction NL si nécessaire
            if use_nlp and query_type == 'NL':
                sql_query = self._translate_nl_to_sql(processed_query)
                self.stats['nl_queries'] += 1
                logger.info(f"NL translation: '{processed_query[:50]}...' -> '{sql_query}'")
            else:
                sql_query = processed_query
            
            # Étape 4: Validation syntaxique
            self._validate_query(sql_query)
            
            # Étape 5: Exécution via storage ou standalone
            if self.storage:
                result = self._execute_via_storage(sql_query, params)
            else:
                result = self._execute_standalone(sql_query, params)
            
            # Étape 6: Post-traitement
            result = self._postprocess_result(result, sql_query)
            
            # Ajouter des métadonnées
            exec_time = time.time() - start_time
            result['metadata'] = {
                'execution_time_ms': round(exec_time * 1000, 2),
                'query_type': query_type,
                'original_query': original_query[:100] + ('...' if len(original_query) > 100 else ''),
                'translated_query': sql_query if query_type == 'NL' else None,
                'timestamp': datetime.now().isoformat(),
                'cache_hit': cached_result is not None
            }
            
            # Mettre à jour les statistiques
            self.stats['total_queries'] += 1
            self.stats['avg_execution_time'] = (
                (self.stats['avg_execution_time'] * (self.stats['total_queries'] - 1) + exec_time) 
                / self.stats['total_queries']
            )
            
            # Mettre en cache si approprié
            if (use_cache and not cached_result and 
                result.get('success') and 
                query_type != 'DDL'):  # Ne pas cacher DDL
                self._add_to_cache(query_id, result)
            
            return result
            
        except Exception as e:
            self.stats['execution_errors'] += 1
            error_msg = str(e)
            
            # Log détaillé en mode verbose
            if self.config['verbose_errors']:
                logger.error(f"Query execution failed: {error_msg}")
                logger.debug(f"Failed query: {original_query}")
                if params:
                    logger.debug(f"Params: {params}")
            
            # Formater l'erreur
            return self._format_error(original_query, error_msg, start_time)
    
    def _generate_query_id(self, query: str, params: Dict) -> str:
        """Génère un ID unique pour la requête"""
        query_str = query + (json.dumps(params, sort_keys=True) if params else "")
        return hashlib.md5(query_str.encode()).hexdigest()[:16]
    
    def _get_from_cache(self, query_id: str) -> Optional[Dict]:
        """Récupère un résultat du cache"""
        with self.cache_lock:
            cached = self.query_cache.get(query_id)
            if cached and (time.time() - cached['timestamp'] < 300):  # 5 minutes
                return cached['result']
            elif cached:
                # Expiré, supprimer
                del self.query_cache[query_id]
        return None
    
    def _add_to_cache(self, query_id: str, result: Dict):
        """Ajoute un résultat au cache"""
        with self.cache_lock:
            # Limiter la taille du cache
            if len(self.query_cache) >= self.config['cache_size']:
                # Supprimer le plus ancien
                oldest_id = min(self.query_cache.keys(), 
                              key=lambda k: self.query_cache[k]['timestamp'])
                del self.query_cache[oldest_id]
            
            self.query_cache[query_id] = {
                'result': result,
                'timestamp': time.time(),
                'hits': 0
            }
    
    def _detect_query_type(self, query: str) -> str:
        """Détecte le type de requête"""
        query_lower = query.lower().strip()
        
        # Commandes pointées (SQLite style)
        if query_lower.startswith('.'):
            return 'DOT_COMMAND'
        
        # Commandes GSQL spéciales
        gsql_commands = ['show tables', 'show functions', 'describe ', 'schema ', 
                        'stats', 'vacuum', 'backup', 'help']
        if any(cmd in query_lower for cmd in gsql_commands):
            return 'GSQL_COMMAND'
        
        # Vérifier les mots-clés SQL
        sql_keywords = [
            'select', 'insert', 'update', 'delete', 'create', 'drop', 'alter',
            'with', 'explain', 'pragma', 'begin', 'commit', 'rollback'
        ]
        
        first_word = query_lower.split()[0] if query_lower.split() else ''
        
        if first_word in sql_keywords:
            # DDL vs DML
            if first_word in ['create', 'drop', 'alter', 'truncate']:
                return 'DDL'
            else:
                return 'DML'
        
        # Si pas de mot-clé SQL évident, c'est probablement du NL
        if len(query.split()) > 2:  # Plus qu'un seul mot
            return 'NL'
        
        # Par défaut
        return 'UNKNOWN'
    
    def _preprocess_query(self, query: str, query_type: str) -> str:
        """Prétraite la requête avant exécution"""
        # Nettoyer les espaces
        query = ' '.join(query.split())
        
        # Gestion des commandes pointées
        if query_type == 'DOT_COMMAND':
            return self._convert_dot_command(query)
        
        # Correction automatique de syntaxe courante
        query = self._auto_correct_syntax(query)
        
        # Gestion des guillemets
        query = self._normalize_quotes(query)
        
        # Ajouter un point-virgule si manquant (sauf pour certaines commandes)
        if not query.endswith(';') and query_type in ['DML', 'DDL']:
            query += ';'
        
        return query
    
    def _convert_dot_command(self, command: str) -> str:
        """Convertit les commandes pointées SQLite en SQL GSQL"""
        cmd = command[1:].lower()
        
        mapping = {
            'tables': 'SHOW TABLES',
            'schema': 'SHOW SCHEMA',
            'indexes': 'SHOW INDEXES',
            'stats': 'STATS',
            'help': 'HELP',
            'backup': 'BACKUP',
            'vacuum': 'VACUUM',
            'exit': 'EXIT',
            'quit': 'EXIT'
        }
        
        # Commandes avec paramètres
        if cmd.startswith('schema '):
            table = cmd[7:].strip()
            return f"DESCRIBE {table}"
        
        return mapping.get(cmd, 'HELP')  # Par défaut: aide
    
    def _auto_correct_syntax(self, query: str) -> str:
        """Corrige automatiquement les erreurs de syntaxe courantes"""
        corrections = [
            (r'\bINT\s+', 'INTO '),           # INT -> INTO
            (r'\bINT0\b', 'INTO'),            # INT0 -> INTO
            (r'\bFORM\b', 'FROM'),            # FORM -> FROM
            (r'\bWERE\b', 'WHERE'),           # WERE -> WHERE
            (r'\bVALUSE\b', 'VALUES'),        # VALUSE -> VALUES
            (r'\bTABEL\b', 'TABLE'),          # TABEL -> TABLE
            (r'\s+=\s+NULL\b', ' IS NULL'),   # = NULL -> IS NULL
            (r'\b!= NULL\b', ' IS NOT NULL'), # != NULL -> IS NOT NULL
        ]
        
        for pattern, replacement in corrections:
            query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
        
        return query
    
    def _normalize_quotes(self, query: str) -> str:
        """Normalise les guillemets dans la requête"""
        # Remplacer les guillemets doubles par simples pour les chaînes
        # Sauf pour les identifiants entourés de []
        in_string = False
        result = []
        
        for char in query:
            if char == "'":
                in_string = not in_string
                result.append(char)
            elif char == '"' and not in_string:
                # Guillemet double en dehors d'une chaîne -> probablement un identifiant
                result.append('"')
            else:
                result.append(char)
        
        return ''.join(result)
    
    def _translate_nl_to_sql(self, nl_query: str) -> str:
        """Traduit le langage naturel en SQL"""
        try:
            if self.nlp_translator:
                return self.nlp_translator.translate(nl_query)
            elif self.nl_to_sql_func:
                return self.nl_to_sql_func(nl_query)
            else:
                # Fallback simple
                return self._simple_nl_translation(nl_query)
        except Exception as e:
            logger.warning(f"NL translation failed: {e}")
            # Fallback simple
            return self._simple_nl_translation(nl_query)
    
    def _simple_nl_translation(self, nl_query: str) -> str:
        """Traduction NL simple (fallback)"""
        nl_lower = nl_query.lower()
        
        if "table" in nl_lower and "show" not in nl_lower:
            # Essayer d'extraire un nom de table
            words = nl_lower.split()
            for word in words:
                if word != "table" and len(word) > 2:
                    return f"SELECT * FROM {word}"
        
        if "table" in nl_lower:
            return "SHOW TABLES"
        elif "fonction" in nl_lower or "function" in nl_lower:
            return "SHOW FUNCTIONS"
        elif "aide" in nl_lower or "help" in nl_lower:
            return "HELP"
        else:
            return "SELECT 'Try: show tables, show functions, table [name]' as suggestion"
    
    def _validate_query(self, query: str):
        """Valide la syntaxe de la requête (validation basique)"""
        # Vérifier les requêtes dangereuses
        dangerous_patterns = [
            (r'DROP\s+DATABASE', "DROP DATABASE n'est pas autorisé"),
            (r'DROP\s+TABLE\s+\*', "DROP TABLE * n'est pas autorisé"),
            (r';\s*DROP', "Multiples requêtes DANGEREUX"),
            (r'--\s*DROP', "Commentaire suspect"),
            (r'/\*.*DROP.*\*/', "Commentaire multiligne suspect"),
        ]
        
        query_upper = query.upper()
        
        for pattern, message in dangerous_patterns:
            if re.search(pattern, query_upper, re.IGNORECASE | re.DOTALL):
                raise SQLSyntaxError(f"Requête potentiellement dangereuse: {message}")
        
        # Vérifier la syntaxe basique
        if not query.strip():
            raise SQLSyntaxError("Requête vide")
        
        # Vérifier les parenthèses équilibrées
        if query.count('(') != query.count(')'):
            raise SQLSyntaxError("Parenthèses non équilibrées")
    
    def _execute_via_storage(self, sql_query: str, params: Dict) -> Dict:
        """Exécute la requête via le système de stockage"""
        try:
            # Enregistrer les fonctions avant l'exécution
            self._register_functions_to_storage()
            
            # Exécuter via storage
            return self.storage.execute(sql_query, params)
            
        except Exception as e:
            # Gestion spéciale des erreurs de fonction
            if "no such function" in str(e).lower():
                # Essayer de réenregistrer les fonctions
                logger.warning(f"Function error, re-registering: {e}")
                self._register_functions_to_storage(force=True)
                
                # Réessayer
                return self.storage.execute(sql_query, params)
            raise
    
    def _register_functions_to_storage(self, force: bool = False):
        """Enregistre les fonctions utilisateur dans le storage"""
        if not self.storage or not self.function_manager:
            return
        
        try:
            # Enregistrer les fonctions du FunctionManager
            for name, func_info in self.function_manager.user_functions.items():
                if force or name not in getattr(self.storage, '_registered_functions', set()):
                    func = func_info['function']
                    num_params = len(func_info.get('params', []))
                    self.storage.register_function(name, func, num_params)
                    
                    # Mettre à jour le cache
                    if not hasattr(self.storage, '_registered_functions'):
                        self.storage._registered_functions = set()
                    self.storage._registered_functions.add(name)
            
            logger.debug(f"Registered functions to storage")
            
        except Exception as e:
            logger.warning(f"Failed to register functions to storage: {e}")
    
    def _execute_standalone(self, sql_query: str, params: Dict) -> Dict:
        """Exécute en mode standalone (sans storage)"""
        # Créer une connexion SQLite temporaire
        temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        temp_db.close()
        
        try:
            conn = sqlite3.connect(temp_db.name)
            conn.row_factory = sqlite3.Row
            
            # Configurer la connexion
            conn.execute("PRAGMA journal_mode=WAL")
            
            # Créer des tables par défaut pour les tests
            self._create_test_tables(conn)
            
            # Enregistrer les fonctions
            self._register_functions_to_sqlite(conn)
            
            # Exécuter la requête
            cursor = conn.cursor()
            
            if params:
                cursor.execute(sql_query, params)
            else:
                cursor.execute(sql_query)
            
            # Traiter les résultats
            if sql_query.strip().upper().startswith('SELECT'):
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                result_rows = []
                for row in rows:
                    result_rows.append(dict(zip(columns, row)))
                
                result = {
                    'type': 'select',
                    'rows': result_rows,
                    'columns': columns,
                    'count': len(result_rows),
                    'success': True
                }
            else:
                conn.commit()
                result = {
                    'type': 'command',
                    'rows_affected': cursor.rowcount,
                    'lastrowid': cursor.lastrowid,
                    'success': True
                }
            
            conn.close()
            os.unlink(temp_db.name)
            
            return result
            
        except Exception as e:
            if os.path.exists(temp_db.name):
                os.unlink(temp_db.name)
            raise SQLExecutionError(f"Standalone execution failed: {e}")
    
    def _create_test_tables(self, conn: sqlite3.Connection):
        """Crée des tables de test pour le mode standalone"""
        test_tables = [
            """CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                email TEXT,
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            """CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                price REAL,
                stock INTEGER DEFAULT 0
            )"""
        ]
        
        for table_sql in test_tables:
            conn.execute(table_sql)
        
        conn.commit()
    
    def _register_functions_to_sqlite(self, conn: sqlite3.Connection):
        """Enregistre les fonctions dans une connexion SQLite"""
        # Fonctions intégrées de base
        def sql_upper(text):
            return str(text).upper() if text else ''
        
        def sql_lower(text):
            return str(text).lower() if text else ''
        
        def sql_length(text):
            return len(str(text)) if text else 0
        
        conn.create_function('UPPER', 1, sql_upper)
        conn.create_function('LOWER', 1, sql_lower)
        conn.create_function('LENGTH', 1, sql_length)
        
        # Enregistrer les fonctions du function_manager si disponible
        if self.function_manager:
            for name, func_info in self.function_manager.functions.items():
                try:
                    func = func_info['call'] if 'call' in func_info else func_info
                    num_params = len(func_info.get('params', [])) if isinstance(func_info, dict) else -1
                    conn.create_function(name, num_params, func)
                except Exception as e:
                    logger.debug(f"Could not register function {name} to SQLite: {e}")
    
    def _postprocess_result(self, result: Dict, query: str) -> Dict:
        """Post-traite les résultats de la requête"""
        if not result.get('success'):
            return result
        
        # Formater les résultats SELECT
        if result.get('type') == 'select':
            # Limiter le nombre de lignes retournées pour les grandes requêtes
            max_display_rows = 1000
            rows = result.get('rows', [])
            
            if len(rows) > max_display_rows:
                result['rows'] = rows[:max_display_rows]
                result['total_rows'] = len(rows)
                result['truncated'] = True
                result['message'] = f"Displaying first {max_display_rows} of {len(rows)} rows"
            else:
                result['truncated'] = False
            
            # Ajouter un résumé
            result['summary'] = {
                'row_count': len(rows),
                'column_count': len(result.get('columns', [])),
                'query_type': 'SELECT'
            }
        
        # Formater les résultats de commande
        elif result.get('type') == 'command':
            rows_affected = result.get('rows_affected', 0)
            
            if 'CREATE' in query.upper():
                result['message'] = 'Table created successfully'
            elif 'INSERT' in query.upper():
                result['message'] = f'{rows_affected} row(s) inserted'
            elif 'UPDATE' in query.upper():
                result['message'] = f'{rows_affected} row(s) updated'
            elif 'DELETE' in query.upper():
                result['message'] = f'{rows_affected} row(s) deleted'
            elif 'DROP' in query.upper():
                result['message'] = 'Table dropped successfully'
        
        return result
    
    def _format_error(self, query: str, error_msg: str, start_time: float) -> Dict:
        """Formate une erreur d'exécution"""
        exec_time = time.time() - start_time
        
        # Extraire le type d'erreur
        error_type = "SQL_ERROR"
        if "no such table" in error_msg.lower():
            error_type = "TABLE_NOT_FOUND"
        elif "syntax error" in error_msg.lower():
            error_type = "SYNTAX_ERROR"
        elif "constraint" in error_msg.lower():
            error_type = "CONSTRAINT_ERROR"
        elif "timeout" in error_msg.lower():
            error_type = "TIMEOUT"
        
        return {
            'type': 'error',
            'success': False,
            'error_type': error_type,
            'error_message': error_msg,
            'query': query[:200] + ('...' if len(query) > 200 else ''),
            'execution_time_ms': round(exec_time * 1000, 2),
            'timestamp': datetime.now().isoformat(),
            'suggestion': self._get_error_suggestion(error_type, error_msg)
        }
    
    def _get_error_suggestion(self, error_type: str, error_msg: str) -> str:
        """Retourne une suggestion basée sur le type d'erreur"""
        suggestions = {
            'TABLE_NOT_FOUND': "Vérifiez le nom de la table avec SHOW TABLES",
            'SYNTAX_ERROR': "Vérifiez la syntaxe SQL. Utilisez HELP pour voir les exemples",
            'CONSTRAINT_ERROR': "Violation de contrainte. Vérifiez les valeurs insérées",
            'TIMEOUT': "La requête a pris trop de temps. Essayez avec un LIMIT",
            'SQL_ERROR': "Erreur d'exécution SQL. Vérifiez la requête"
        }
        
        return suggestions.get(error_type, "Consultez HELP pour plus d'informations")
    
    # ==================== FONCTIONS SPÉCIALES ====================
    
    def execute_batch(self, queries: List[str], stop_on_error: bool = True) -> Dict:
        """Exécute un batch de requêtes"""
        results = []
        total_start = time.time()
        
        for i, query in enumerate(queries):
            try:
                result = self.execute(query, use_cache=False)
                results.append({
                    'query_index': i,
                    'query': query[:50] + ('...' if len(query) > 50 else ''),
                    'success': result.get('success', False),
                    'rows_affected': result.get('rows_affected', 0),
                    'execution_time_ms': result.get('metadata', {}).get('execution_time_ms', 0)
                })
                
                # Arrêter sur erreur si demandé
                if stop_on_error and not result.get('success'):
                    break
                    
            except Exception as e:
                results.append({
                    'query_index': i,
                    'query': query[:50] + ('...' if len(query) > 50 else ''),
                    'success': False,
                    'error': str(e)
                })
                
                if stop_on_error:
                    break
        
        total_time = time.time() - total_start
        
        return {
            'type': 'batch',
            'results': results,
            'total_queries': len(queries),
            'successful': sum(1 for r in results if r.get('success')),
            'failed': sum(1 for r in results if not r.get('success')),
            'total_time_ms': round(total_time * 1000, 2),
            'success': all(r.get('success') for r in results)
        }
    
    def explain(self, query: str) -> Dict:
        """Explique le plan d'exécution d'une requête"""
        try:
            # Version simplifiée d'EXPLAIN
            explain_query = f"EXPLAIN QUERY PLAN {query}"
            result = self.execute(explain_query, use_cache=False)
            
            if result.get('success') and result.get('type') == 'select':
                # Formater le plan d'exécution
                plan = []
                for row in result.get('rows', []):
                    plan.append({
                        'id': row.get('id'),
                        'parent': row.get('parent'),
                        'detail': row.get('detail')
                    })
                
                return {
                    'type': 'explain',
                    'query': query,
                    'plan': plan,
                    'steps': len(plan),
                    'success': True
                }
            else:
                return {
                    'type': 'explain',
                    'query': query,
                    'plan': [],
                    'message': 'Could not generate query plan',
                    'success': False
                }
                
        except Exception as e:
            return {
                'type': 'explain',
                'query': query,
                'error': str(e),
                'success': False
            }
    
    def prepare_statement(self, query: str, name: str = None) -> str:
        """Prépare une requête pour exécution répétée"""
        if not name:
            name = f"stmt_{hashlib.md5(query.encode()).hexdigest()[:8]}"
        
        # Valider la requête
        self._validate_query(query)
        
        # Extraire les paramètres
        param_count = query.count('?')
        
        self.prepared_statements[name] = {
            'query': query,
            'param_count': param_count,
            'created_at': time.time(),
            'use_count': 0
        }
        
        logger.debug(f"Prepared statement '{name}' with {param_count} parameters")
        
        return name
    
    def execute_prepared(self, name: str, params: List = None) -> Dict:
        """Exécute une requête préparée"""
        if name not in self.prepared_statements:
            raise SQLExecutionError(f"Prepared statement '{name}' not found")
        
        stmt = self.prepared_statements[name]
        query = stmt['query']
        
        # Vérifier le nombre de paramètres
        if params and len(params) != stmt['param_count']:
            raise SQLExecutionError(
                f"Expected {stmt['param_count']} parameters, got {len(params)}"
            )
        
        # Exécuter
        result = self.execute(query, params if params else None)
        
        # Mettre à jour les statistiques
        stmt['use_count'] += 1
        
        return result
    
    # ==================== GESTION DU CACHE ====================
    
    def clear_cache(self, cache_type: str = 'all'):
        """Vide le cache"""
        with self.cache_lock:
            if cache_type in ['all', 'query']:
                self.query_cache.clear()
                logger.info("Query cache cleared")
            
            if cache_type in ['all', 'plan']:
                self.plan_cache.clear()
                logger.info("Plan cache cleared")
            
            if cache_type in ['all', 'prepared']:
                self.prepared_statements.clear()
                logger.info("Prepared statements cleared")
        
        return {
            'type': 'cache_clear',
            'cache_type': cache_type,
            'success': True,
            'message': f'Cache {cache_type} cleared'
        }
    
    def get_cache_stats(self) -> Dict:
        """Retourne les statistiques du cache"""
        with self.cache_lock:
            return {
                'query_cache': {
                    'size': len(self.query_cache),
                    'max_size': self.config['cache_size'],
                    'hits': self.stats['cached_hits'],
                    'hit_ratio': (
                        self.stats['cached_hits'] / self.stats['total_queries'] 
                        if self.stats['total_queries'] > 0 else 0
                    )
                },
                'plan_cache': {
                    'size': len(self.plan_cache)
                },
                'prepared_statements': {
                    'count': len(self.prepared_statements),
                    'most_used': sorted(
                        self.prepared_statements.items(),
                        key=lambda x: x[1]['use_count'],
                        reverse=True
                    )[:5]
                }
            }
    
    # ==================== STATISTIQUES ====================
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques de l'exécuteur"""
        uptime = time.time() - self.stats['start_time']
        
        return {
            'executor': {
                'uptime_seconds': round(uptime, 2),
                'total_queries': self.stats['total_queries'],
                'nl_queries': self.stats['nl_queries'],
                'execution_errors': self.stats['execution_errors'],
                'avg_execution_time_ms': round(self.stats['avg_execution_time'] * 1000, 2),
                'cache_hits': self.stats['cached_hits'],
                'cache_hit_ratio': (
                    self.stats['cached_hits'] / self.stats['total_queries'] 
                    if self.stats['total_queries'] > 0 else 0
                )
            },
            'cache': self.get_cache_stats(),
            'functions': {
                'builtin': len([f for f in self.function_manager.functions 
                              if f not in self.function_manager.user_functions]) if self.function_manager else 0,
                'user': len(self.function_manager.user_functions) if self.function_manager else 0
            },
            'config': self.config
        }
    
    def reset_stats(self):
        """Réinitialise les statistiques"""
        self.stats = {
            'total_queries': 0,
            'nl_queries': 0,
            'cached_hits': 0,
            'execution_errors': 0,
            'avg_execution_time': 0,
            'start_time': time.time()
        }
        
        return {
            'type': 'stats_reset',
            'success': True,
            'message': 'Statistics reset'
        }
    
    # ==================== CONFIGURATION ====================
    
    def configure(self, **kwargs):
        """Configure l'exécuteur"""
        for key, value in kwargs.items():
            if key in self.config:
                old_value = self.config[key]
                self.config[key] = value
                logger.info(f"Configuration changed: {key}={old_value} -> {value}")
            else:
                logger.warning(f"Unknown configuration key: {key}")
        
        return {
            'type': 'configuration',
            'config': self.config,
            'success': True,
            'message': 'Configuration updated'
        }
    
    def close(self):
        """Ferme proprement l'exécuteur"""
        # Nettoyer les ressources
        self.clear_cache()
        self.prepared_statements.clear()
        
        logger.info("QueryExecutor closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def __del__(self):
        try:
            self.close()
        except:
            pass


# ==================== FACTORY FUNCTIONS ====================

def create_executor(storage=None, **kwargs) -> QueryExecutor:
    """Crée un nouvel exécuteur de requêtes"""
    return QueryExecutor(storage, **kwargs)

def get_default_executor() -> Optional[QueryExecutor]:
    """Récupère l'exécuteur par défaut"""
    if not hasattr(get_default_executor, '_instance'):
        get_default_executor._instance = None
    
    return get_default_executor._instance

def set_default_executor(executor: QueryExecutor):
    """Définit l'exécuteur par défaut"""
    get_default_executor._instance = executor
