import logging
import sqlite3
import tempfile
import os
from typing import Dict, Any
from .exceptions import SQLExecutionError

logger = logging.getLogger(__name__)

class QueryExecutor:
    """Exécuteur de requêtes avec support SQLite comme backend"""
    
    def __init__(self, database, function_manager=None, nlp_translator=None):
        self.database = database
        self.function_manager = function_manager
        self.nlp_translator = nlp_translator
        
        # Créer une base SQLite temporaire
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # Connexion SQLite
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Pour avoir des dictionnaires
        self.cursor = self.conn.cursor()
        
        # Initialiser des tables par défaut
        self._init_database()
    
    def _init_database(self):
        """Initialiser la base de données avec des tables par défaut"""
        try:
            # Créer une table users si elle n'existe pas
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    age INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    price REAL,
                    quantity INTEGER,
                    category TEXT
                )
            """)
            
            self.conn.commit()
        except Exception as e:
            logger.warning(f"Could not init default tables: {e}")
    
    def execute(self, sql: str, params: Dict = None, use_cache: bool = True) -> Any:
        """
        Exécute une requête SQL ou NL
        """
        sql = sql.strip()
        
        # Traitement spécial pour les commandes GSQL
        if sql.startswith('.'):
            return self._execute_dot_command(sql)
        
        # Vérifier si c'est du langage naturel
        if self._is_natural_language(sql):
            sql = self._translate_nl_to_sql(sql)
            logger.info(f"Translated to SQL: {sql}")
        
        # Nettoyer et corriger le SQL
        sql = self._clean_sql(sql)
        
        # Exécuter via SQLite
        return self._execute_sqlite(sql)
    
    def _execute_dot_command(self, command: str) -> Dict:
        """Exécute une commande pointée (.tables, .help, etc.)"""
        cmd = command[1:].lower()
        
        if cmd in ['tables', 'table']:
            return self._execute_show_tables()
        elif cmd in ['help', '?']:
            return self._execute_help("")
        elif cmd == 'exit' or cmd == 'quit':
            return {'type': 'exit', 'message': 'Use EXIT or QUIT command'}
        elif cmd == 'schema':
            return {'type': 'help', 'message': 'Use: .tables, .help'}
        else:
            return {
                'type': 'error',
                'message': f'Unknown command: {command}. Try: .tables, .help'
            }
    
    def _is_natural_language(self, text: str) -> bool:
        """Détecte si le texte est en langage naturel"""
        # Si ça commence par un point, c'est une commande GSQL
        if text.startswith('.'):
            return False
        
        # Mots-clés SQL
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 
                       'ALTER', 'WHERE', 'FROM', 'JOIN', 'INTO', 'VALUES',
                       'GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET']
        
        text_upper = text.upper()
        has_sql_keyword = any(keyword in text_upper for keyword in sql_keywords)
        
        # Commandes GSQL spéciales
        gsql_commands = ['SHOW TABLES', 'SHOW FUNCTIONS', 'HELP']
        has_gsql_command = any(cmd in text_upper for cmd in gsql_commands)
        
        return not (has_sql_keyword or has_gsql_command) and len(text.split()) > 1
    
    def _translate_nl_to_sql(self, nl_text: str) -> str:
        """Traduit le langage naturel en SQL"""
        if not self.nlp_translator:
            # Fallback simple
            nl_lower = nl_text.lower()
            if "table" in nl_lower:
                return "SHOW TABLES"
            elif "fonction" in nl_lower:
                return "SHOW FUNCTIONS"
            else:
                return "HELP"
        
        try:
            return self.nlp_translator.translate(nl_text)
        except Exception as e:
            logger.error(f"Translation error: {e}")
            return "HELP"
    
    def _clean_sql(self, sql: str) -> str:
        """Nettoyer et corriger le SQL"""
        # Corriger les erreurs courantes
        sql = sql.replace('INT ', 'INTO ')  # Corriger INT -> INTO
        sql = sql.replace('INT0 ', 'INTO ')  # Corriger INT0 -> INTO
        
        # Ajouter des guillemets si manquants dans les valeurs de texte
        if 'VALUES' in sql.upper():
            # Pattern simple pour détecter des valeurs non guillemetées
            import re
            # Trouver la partie VALUES (...)
            values_match = re.search(r'VALUES\s*\((.+?)\)', sql, re.IGNORECASE)
            if values_match:
                values_str = values_match.group(1)
                # Vérifier si des valeurs texte n'ont pas de guillemets
                parts = values_str.split(',')
                cleaned_parts = []
                for part in parts:
                    part = part.strip()
                    # Si c'est un nombre ou NULL, garder tel quel
                    if part.replace('.', '', 1).isdigit() or part.upper() in ['NULL', 'TRUE', 'FALSE']:
                        cleaned_parts.append(part)
                    elif not (part.startswith("'") and part.endswith("'")):
                        # Ajouter des guillemets
                        cleaned_parts.append(f"'{part}'")
                    else:
                        cleaned_parts.append(part)
                
                # Reconstruire le SQL
                new_values = f"VALUES ({', '.join(cleaned_parts)})"
                sql = sql[:values_match.start()] + new_values + sql[values_match.end():]
        
        return sql
    
    def _execute_sqlite(self, sql: str) -> Dict:
        """Exécuter via SQLite"""
        try:
            # Commandes spéciales GSQL
            sql_upper = sql.upper()
            
            if sql_upper == 'SHOW TABLES':
                return self._execute_show_tables()
            elif sql_upper == 'SHOW FUNCTIONS':
                return self._execute_show_functions()
            elif sql_upper == 'HELP':
                return self._execute_help(sql)
            
            # Exécuter le SQL
            self.cursor.execute(sql)
            
            # Si c'est une requête SELECT, récupérer les résultats
            if sql_upper.startswith('SELECT'):
                rows = self.cursor.fetchall()
                column_names = [description[0] for description in self.cursor.description]
                
                result_rows = []
                for row in rows:
                    result_rows.append(dict(zip(column_names, row)))
                
                return {
                    'type': 'select',
                    'rows': result_rows,
                    'count': len(result_rows),
                    'columns': column_names
                }
            else:
                # COMMIT pour les autres commandes
                self.conn.commit()
                return {
                    'type': 'command',
                    'message': 'Command executed successfully',
                    'rows_affected': self.cursor.rowcount
                }
                
        except sqlite3.Error as e:
            error_msg = str(e)
            # Rendre les erreurs plus lisibles
            if "syntax error" in error_msg.lower():
                error_msg = f"Syntax error: {error_msg}"
            elif "no such table" in error_msg.lower():
                error_msg = f"Table not found: {error_msg}"
            
            raise SQLExecutionError(f"SQL error: {error_msg}")
        except Exception as e:
            raise SQLExecutionError(f"Execution error: {str(e)}")
    
    def _execute_show_tables(self) -> Dict:
        """Exécute SHOW TABLES"""
        try:
            self.cursor.execute("""
                SELECT 
                    name as table,
                    (SELECT COUNT(*) FROM sqlite_master WHERE type='table') as table_count
                FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            rows = self.cursor.fetchall()
            
            result_rows = []
            for row in rows:
                table_name = row[0]
                # Compter les lignes dans chaque table
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = self.cursor.fetchone()[0]
                except:
                    row_count = 0
                
                result_rows.append({
                    'table': table_name,
                    'rows': row_count
                })
            
            return {
                'type': 'show_tables',
                'rows': result_rows,
                'count': len(result_rows),
                'message': f'Found {len(result_rows)} table(s)'
            }
        except Exception as e:
            return {
                'type': 'show_tables',
                'rows': [],
                'count': 0,
                'message': f'Error: {str(e)}'
            }
    
    def _execute_show_functions(self) -> Dict:
        """Exécute SHOW FUNCTIONS"""
        functions = []
        
        # Fonctions intégrées
        builtins = [
            {'name': 'UPPER(text)', 'type': 'builtin', 'description': 'Convert to uppercase'},
            {'name': 'LOWER(text)', 'type': 'builtin', 'description': 'Convert to lowercase'},
            {'name': 'LENGTH(text)', 'type': 'builtin', 'description': 'String length'},
            {'name': 'ABS(number)', 'type': 'builtin', 'description': 'Absolute value'},
            {'name': 'ROUND(number, decimals)', 'type': 'builtin', 'description': 'Round number'}
        ]
        
        # Ajouter les fonctions utilisateur si disponibles
        if self.function_manager:
            try:
                user_funcs = self.function_manager.list_functions()
                for func in user_funcs:
                    if isinstance(func, dict):
                        functions.append(func)
            except:
                pass
        
        functions.extend(builtins)
        
        return {
            'type': 'show_functions',
            'rows': functions,
            'count': len(functions),
            'message': f'Found {len(functions)} function(s)'
        }
    
    def _execute_help(self, sql: str) -> Dict:
        """Exécute HELP"""
        help_text = """
GSQL Commands:

SQL Commands:
  SELECT * FROM table          - Query data
  INSERT INTO table VALUES     - Insert data  
  CREATE TABLE name (columns)  - Create table
  CREATE FUNCTION              - Create custom function

GSQL Special Commands:
  SHOW TABLES                  - List all tables
  SHOW FUNCTIONS               - List all functions
  HELP                         - This help message

Natural Language Examples:
  "table users"                -> SELECT * FROM users
  "montrer tables"             -> SHOW TABLES
  "montrer fonctions"          -> SHOW FUNCTIONS
  "combien de users"           -> SELECT COUNT(*) FROM users

Dot Commands (like SQLite):
  .tables                      - List tables
  .help                        - Show help
  .exit/.quit                  - Exit shell

Type any SQL command to execute it.
"""
        
        return {
            'type': 'help',
            'message': help_text,
            'rows': [{'help': line.strip()} for line in help_text.strip().split('\n') if line.strip()]
        }
    
    def clear_cache(self):
        """Vide le cache"""
        return "Cache cleared"
    
    def close(self):
        """Fermer la connexion"""
        if hasattr(self, 'conn'):
            self.conn.close()
        if hasattr(self, 'temp_db'):
            try:
                os.unlink(self.db_path)
            except:
                pass
    
    def __del__(self):
        """Destructeur"""
        self.close()
