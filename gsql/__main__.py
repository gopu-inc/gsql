#!/usr/bin/env python3
"""
GSQL Main Entry Point - Interactive Shell and CLI
Version: 3.1.0 - SQLite Only with Transaction Support
"""

import os
import sys
import cmd
import signal
import traceback
import readline
import atexit
import json
import re
import logging
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime

# ==================== IMPORTS ====================

# Définir __version__ si non présent
__version__ = "3.1.0"

# Import des modules GSQL
try:
    # Config simple si config.py n'existe pas
    try:
        from . import config
    except ImportError:
        # Créer un module config simple
        class SimpleConfig:
            _config = {
                'base_dir': str(Path.home() / '.gsql'),
                'log_level': 'INFO',
                'colors': True,
                'verbose_errors': False,
                'auto_commit': False,
                'transaction_timeout': 30
            }
            
            def get(self, key, default=None):
                keys = key.split('.')
                value = self._config
                for k in keys:
                    if isinstance(value, dict) and k in value:
                        value = value[k]
                    else:
                        return default
                return value
            
            def set(self, key, value):
                self._config[key] = value
            
            def to_dict(self):
                return self._config.copy()
            
            def update(self, **kwargs):
                self._config.update(kwargs)
        
        config = SimpleConfig()
    
    # Import du database.py
    from .database import create_database, Database, connect
    
    # Gérer les imports optionnels
    try:
        from .storage import SQLiteStorage, create_storage
        STORAGE_AVAILABLE = True
    except ImportError:
        STORAGE_AVAILABLE = False
        # Définir un fallback minimal
        class FallbackStorage:
            def __init__(self, *args, **kwargs):
                pass
            def execute(self, sql, params=None):
                return {'success': False, 'error': 'Storage not available'}
            def get_tables(self):
                return []
            def get_table_schema(self, table_name):
                return None
            def get_stats(self):
                return {}
            def backup(self, backup_path=None):
                return ""
            def close(self):
                pass
        
        def create_storage(*args, **kwargs):
            return FallbackStorage()
    
    # Fallback pour executor
    try:
        from .executor import create_executor, QueryExecutor
        EXECUTOR_AVAILABLE = True
    except ImportError:
        EXECUTOR_AVAILABLE = False
        
        class QueryExecutor:
            def __init__(self, storage=None):
                self.storage = storage
        
        def create_executor(storage=None):
            return QueryExecutor(storage)
    
    # Fallback pour functions
    try:
        from .functions import FunctionManager
        FUNCTIONS_AVAILABLE = True
    except ImportError:
        FUNCTIONS_AVAILABLE = False
        
        class FunctionManager:
            def __init__(self):
                pass
    
    # NLP non disponible
    NLP_AVAILABLE = False
    NLToSQLTranslator = None
    
    GSQL_AVAILABLE = True
    
except ImportError as e:
    print(f"Warning: Some imports failed: {e}")
    GSQL_AVAILABLE = True

# ==================== LOGGING ====================

def setup_logging(level='INFO', log_file=None):
    """Configure le logging"""
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            *([logging.FileHandler(log_file)] if log_file else [])
        ]
    )

logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================

DEFAULT_CONFIG = {
    'database': {
        'base_dir': str(Path.home() / '.gsql'),
        'auto_recovery': True,
        'buffer_pool_size': 100,
        'enable_wal': True,
        'transaction_timeout': 30,
        'max_transactions': 10
    },
    'executor': {
        'enable_nlp': False,
        'enable_learning': False,
        'auto_commit': False
    },
    'shell': {
        'prompt': 'gsql> ',
        'history_file': '.gsql_history',
        'max_history': 1000,
        'colors': True,
        'autocomplete': True,
        'show_transaction_status': True,
        'transaction_warning_time': 5
    }
}

# ==================== COLOR SUPPORT ====================

class Colors:
    """Codes de couleurs ANSI"""
    
    # Styles
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    ITALIC = '\033[3m'
    UNDERLINE = '\033[4m'
    BLINK = '\033[5m'
    REVERSE = '\033[7m'
    HIDDEN = '\033[8m'
    
    # Couleurs de texte
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    DEFAULT = '\033[39m'
    
    # Couleurs de fond
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'
    BG_DEFAULT = '\033[49m'
    
    # Couleurs spécifiques pour transactions
    TX_START = '\033[38;5;51m'     # Cyan clair
    TX_COMMIT = '\033[38;5;82m'    # Vert clair
    TX_ROLLBACK = '\033[38;5;208m' # Orange
    TX_ACTIVE = '\033[38;5;226m'   # Jaune vif
    TX_SAVEPOINT = '\033[38;5;183m' # Violet clair
    
    # Méthodes utilitaires
    @staticmethod
    def colorize(text, color_code):
        """Applique un code de couleur au texte"""
        return f"{color_code}{text}{Colors.RESET}"
    
    @staticmethod
    def success(text):
        """Texte de succès (vert)"""
        return Colors.colorize(text, Colors.GREEN)
    
    @staticmethod
    def error(text):
        """Texte d'erreur (rouge)"""
        return Colors.colorize(text, Colors.RED)
    
    @staticmethod
    def warning(text):
        """Texte d'avertissement (jaune)"""
        return Colors.colorize(text, Colors.YELLOW)
    
    @staticmethod
    def info(text):
        """Texte d'information (bleu)"""
        return Colors.colorize(text, Colors.CYAN)
    
    @staticmethod
    def highlight(text):
        """Texte en surbrillance (gras)"""
        return Colors.colorize(text, Colors.BOLD)
    
    @staticmethod
    def dim(text):
        """Texte atténué"""
        return Colors.colorize(text, Colors.DIM)
    
    @staticmethod
    def sql_keyword(text):
        """Mots-clés SQL (magenta)"""
        return Colors.colorize(text, Colors.MAGENTA)
    
    @staticmethod
    def sql_string(text):
        """Chaînes SQL (jaune)"""
        return Colors.colorize(text, Colors.YELLOW)
    
    @staticmethod
    def sql_number(text):
        """Nombres SQL (cyan)"""
        return Colors.colorize(text, Colors.CYAN)
    
    @staticmethod
    def sql_comment(text):
        """Commentaires SQL (vert)"""
        return Colors.colorize(text, Colors.GREEN)
    
    @staticmethod
    def tx_start(text):
        """Début de transaction"""
        return Colors.colorize(text, Colors.TX_START)
    
    @staticmethod
    def tx_commit(text):
        """Commit de transaction"""
        return Colors.colorize(text, Colors.TX_COMMIT)
    
    @staticmethod
    def tx_rollback(text):
        """Rollback de transaction"""
        return Colors.colorize(text, Colors.TX_ROLLBACK)
    
    @staticmethod
    def tx_active(text):
        """Transaction active"""
        return Colors.colorize(text, Colors.TX_ACTIVE)
    
    @staticmethod
    def tx_savepoint(text):
        """Savepoint"""
        return Colors.colorize(text, Colors.TX_SAVEPOINT)

# ==================== AUTO-COMPLETER ====================

class GSQLCompleter:
    """Auto-complétion pour le shell GSQL avec support transactions"""
    
    def __init__(self, database: Database = None):
        self.database = database
        self.keywords = [
            # Commandes transactionnelles
            'BEGIN', 'TRANSACTION', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
            'RELEASE', 'IMMEDIATE', 'EXCLUSIVE', 'DEFERRED',
            
            # Autres mots-clés SQL
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
            'ALTER', 'ADD', 'COLUMN', 'PRIMARY', 'KEY', 'FOREIGN',
            'REFERENCES', 'UNIQUE', 'NOT', 'NULL', 'DEFAULT',
            'CHECK', 'INDEX', 'VIEW', 'TRIGGER', 'EXPLAIN', 'ANALYZE',
            'VACUUM', 'BACKUP', 'SHOW', 'DESCRIBE', 'HELP', 'EXIT',
            'QUIT', 'AND', 'OR', 'LIKE', 'IN', 'BETWEEN', 'IS',
            'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
            'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'AS',
            'UNION', 'INTERSECT', 'EXCEPT', 'DISTINCT', 'ALL',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        ]
        
        # Commandes spéciales GSQL
        self.gsql_commands = [
            '.tables', '.schema', '.stats', '.help', '.backup',
            '.vacuum', '.exit', '.quit', '.clear', '.history',
            '.transactions', '.tx', '.autocommit', '.isolation'
        ]
        
        self.table_names = []
        self.column_names = {}
        
        if database and hasattr(database, 'storage'):
            self._refresh_schema()
    
    def _refresh_schema(self):
        """Rafraîchit le schéma depuis la base"""
        try:
            if self.database:
                # Récupérer les tables
                result = self.database.execute("SHOW TABLES")
                if result.get('success'):
                    self.table_names = [table['table'] for table in result.get('tables', [])]
                
                # Récupérer les colonnes pour chaque table
                self.column_names = {}
                for table in self.table_names:
                    try:
                        result = self.database.execute(f"DESCRIBE {table}")
                        if result.get('success'):
                            self.column_names[table] = [
                                col['field'] for col in result.get('columns', [])
                            ]
                    except:
                        pass
        except:
            pass
    
    def complete(self, text: str, state: int) -> Optional[str]:
        """Fonction de complétion pour readline avec support transactions"""
        if state == 0:
            # Préparer la liste des suggestions
            line = readline.get_line_buffer().lower()
            tokens = line.strip().split()
            
            if not tokens or len(tokens) == 1:
                # Complétion de commande
                if line.startswith('.'):
                    # Commandes dot
                    all_commands = self.gsql_commands
                    self.matches = [cmd for cmd in all_commands if cmd.lower().startswith(text.lower())]
                else:
                    # Commandes SQL
                    all_commands = self.keywords + self.table_names
                    self.matches = [cmd for cmd in all_commands if cmd.lower().startswith(text.lower())]
            
            elif len(tokens) >= 2:
                # Détection contextuelle
                if tokens[-2].lower() == 'begin':
                    # Complétion après BEGIN
                    tx_types = ['TRANSACTION', 'IMMEDIATE', 'EXCLUSIVE', 'DEFERRED']
                    self.matches = [tx for tx in tx_types if tx.lower().startswith(text.lower())]
                
                elif tokens[-2].lower() == 'begin' and tokens[-1].lower() in ['immediate', 'exclusive']:
                    # BEGIN IMMEDIATE/EXCLUSIVE TRANSACTION
                    self.matches = ['TRANSACTION'] if 'transaction'.startswith(text.lower()) else []
                
                elif tokens[-2].lower() == 'rollback' and tokens[-1].lower() == 'to':
                    # ROLLBACK TO SAVEPOINT
                    self.matches = ['SAVEPOINT'] if 'savepoint'.startswith(text.lower()) else []
                
                elif tokens[-1].lower() == 'savepoint':
                    # Après SAVEPOINT, attend un nom
                    self.matches = []
                
                elif tokens[-2].upper() == 'FROM' or tokens[-2].upper() == 'INTO':
                    # Complétion de table après FROM ou INTO
                    self.matches = [table for table in self.table_names if table.lower().startswith(text.lower())]
                
                elif tokens[-2].upper() == 'WHERE' or tokens[-2].upper() == 'SET':
                    # Complétion de colonne après WHERE ou SET
                    table = self._find_current_table(tokens)
                    if table and table in self.column_names:
                        self.matches = [col for col in self.column_names[table] if col.lower().startswith(text.lower())]
                    else:
                        self.matches = []
                else:
                    self.matches = []
            else:
                self.matches = []
        
        try:
            return self.matches[state]
        except IndexError:
            return None
    
    def _find_current_table(self, tokens: List[str]) -> Optional[str]:
        """Trouve la table courante dans les tokens"""
        for i, token in enumerate(tokens):
            if token.upper() == 'FROM' and i + 1 < len(tokens):
                return tokens[i + 1]
            elif token.upper() == 'UPDATE' and i + 1 < len(tokens):
                return tokens[i + 1]
            elif token.upper() == 'INTO' and i + 1 < len(tokens):
                return tokens[i + 1]
        return None

# ==================== SHELL COMMANDS ====================

class GSQLShell(cmd.Cmd):
    """Shell interactif GSQL avec support complet des transactions"""
    
    intro = Colors.info("GSQL Interactive Shell v3.1.0") + "\n" + Colors.dim("Type 'help' for commands, 'exit' to quit")
    prompt = Colors.info('gsql> ')
    ruler = Colors.dim('─')
    
    def __init__(self, gsql_app=None):
        super().__init__()
        self.gsql = gsql_app
        self.db = gsql_app.db if gsql_app else None
        self.executor = gsql_app.executor if gsql_app else None
        self.completer = gsql_app.completer if gsql_app else None
        self.show_tx_status = True
        self.tx_warning_time = 5
        self.current_tx_id = None
        self.tx_start_time = None
        self.auto_commit = False
        
        # Configuration du prompt
        self._update_prompt()
        
        # Configuration de l'historique
        self.history_file = Path.home() / ".gsql" / ".gsql_history"
        self.history_file.parent.mkdir(exist_ok=True, parents=True)
        self._setup_history()
        
        # Configuration de l'auto-complétion
        if self.completer:
            readline.set_completer(self.completer.complete)
            readline.parse_and_bind("tab: complete")
            readline.set_completer_delims(' \t\n`~!@#$%^&*()-=+[{]}\\|;:\'",<>/?')
    
    def _update_prompt(self):
        """Met à jour le prompt avec l'état des transactions"""
        if not self.db:
            self.prompt = Colors.info('gsql> ')
            return
        
        active_tx = 0
        if hasattr(self.db, 'active_transactions'):
            active_tx = len(self.db.active_transactions)
        
        if active_tx > 0:
            # Afficher le nombre de transactions actives
            tx_status = Colors.tx_active(f"[TX:{active_tx}]")
            self.prompt = f"{tx_status} {Colors.info('gsql> ')}"
            
            # Vérifier les transactions trop longues
            if self.tx_start_time and self.show_tx_status:
                elapsed = (datetime.now() - self.tx_start_time).total_seconds()
                if elapsed > self.tx_warning_time:
                    print(Colors.warning(f"⚠ Transaction active depuis {elapsed:.1f}s. Pensez à COMMIT ou ROLLBACK."))
        else:
            self.prompt = Colors.info('gsql> ')
    
    def _setup_history(self):
        """Configure l'historique de commandes"""
        try:
            readline.read_history_file(str(self.history_file))
        except FileNotFoundError:
            pass
        
        # Limiter la taille de l'historique
        readline.set_history_length(1000)
        
        # Enregistrer l'historique à la sortie
        atexit.register(readline.write_history_file, str(self.history_file))
    
    # ==================== COMMAND HANDLING ====================
    
    def default(self, line: str):
        """Gère les commandes SQL par défaut"""
        if not line.strip():
            return
        
        # Vérifier les commandes spéciales avec point
        if line.startswith('.'):
            self._handle_dot_command(line)
            return
        
        # Vérifier si c'est une commande transactionnelle
        if self._is_transaction_command(line):
            self._execute_transaction_command(line)
            return
        
        # Exécuter la requête SQL
        self._execute_sql(line)
    
    def _is_transaction_command(self, sql: str) -> bool:
        """Détecte si c'est une commande transactionnelle"""
        sql_upper = sql.strip().upper()
        transaction_commands = [
            'BEGIN',
            'BEGIN TRANSACTION',
            'BEGIN IMMEDIATE TRANSACTION',
            'BEGIN EXCLUSIVE TRANSACTION',
            'BEGIN DEFERRED TRANSACTION',
            'COMMIT',
            'ROLLBACK',
            'SAVEPOINT',
            'RELEASE SAVEPOINT',
            'ROLLBACK TO SAVEPOINT'
        ]
        
        for cmd in transaction_commands:
            if sql_upper.startswith(cmd):
                return True
        return False
    
    def _execute_transaction_command(self, sql: str):
        """Exécute une commande transactionnelle"""
        try:
            # Exécuter la commande
            result = self.db.execute(sql)
            
            # Mettre à jour l'état local
            sql_upper = sql.strip().upper()
            
            if sql_upper.startswith('BEGIN'):
                # Nouvelle transaction démarrée
                self.current_tx_id = result.get('tid')
                self.tx_start_time = datetime.now()
                print(Colors.tx_start(f"✓ Transaction {self.current_tx_id} started"))
                print(Colors.dim(f"Isolation: {result.get('isolation', 'DEFERRED')}"))
                
            elif sql_upper.startswith('COMMIT'):
                # Transaction validée
                print(Colors.tx_commit(f"✓ Transaction {result.get('tid', '?')} committed"))
                self.current_tx_id = None
                self.tx_start_time = None
                
            elif sql_upper.startswith('ROLLBACK'):
                # Transaction annulée
                if 'TO SAVEPOINT' in sql_upper:
                    savepoint = sql.split()[-1]
                    print(Colors.tx_rollback(f"↺ Rollback to savepoint '{savepoint}'"))
                else:
                    print(Colors.tx_rollback(f"↺ Transaction {result.get('tid', '?')} rolled back"))
                    self.current_tx_id = None
                    self.tx_start_time = None
                    
            elif sql_upper.startswith('SAVEPOINT'):
                savepoint = sql.split()[1] if len(sql.split()) > 1 else 'unknown'
                print(Colors.tx_savepoint(f"✓ Savepoint '{savepoint}' created"))
                
            elif sql_upper.startswith('RELEASE SAVEPOINT'):
                savepoint = sql.split()[2] if len(sql.split()) > 2 else 'unknown'
                print(Colors.tx_savepoint(f"✓ Savepoint '{savepoint}' released"))
            
            # Mettre à jour le prompt
            self._update_prompt()
            
        except Exception as e:
            print(Colors.error(f"Transaction error: {e}"))
    
    def _handle_dot_command(self, command: str):
        """Gère les commandes avec point (comme SQLite)"""
        parts = command[1:].strip().split()
        cmd = parts[0].lower() if parts else ""
        
        if cmd == 'tables':
            self._execute_sql("SHOW TABLES")
        elif cmd == 'schema':
            table = parts[1] if len(parts) > 1 else None
            if table:
                self._execute_sql(f"DESCRIBE {table}")
            else:
                print(Colors.error("Usage: .schema <table_name>"))
        elif cmd == 'stats':
            self._execute_sql("STATS")
        elif cmd == 'transactions' or cmd == 'tx':
            self.do_transactions("")
        elif cmd == 'help':
            self.do_help("")
        elif cmd == 'backup':
            file = parts[1] if len(parts) > 1 else None
            if file:
                self._execute_sql(f"BACKUP {file}")
            else:
                self._execute_sql("BACKUP")
        elif cmd == 'vacuum':
            self._execute_sql("VACUUM")
        elif cmd == 'autocommit':
            self._handle_autocommit(parts[1:] if len(parts) > 1 else [])
        elif cmd == 'isolation':
            self._handle_isolation(parts[1:] if len(parts) > 1 else [])
        elif cmd == 'exit' or cmd == 'quit':
            return True
        elif cmd == 'clear':
            os.system('clear' if os.name == 'posix' else 'cls')
        elif cmd == 'history':
            self._show_history()
        else:
            print(Colors.error(f"Unknown command: .{cmd}"))
            print(Colors.dim("Try .help for available commands"))
    
    def _handle_autocommit(self, args):
        """Gère la commande .autocommit"""
        if not args:
            status = "ON" if self.auto_commit else "OFF"
            print(f"Auto-commit: {Colors.highlight(status)}")
            return
        
        arg = args[0].lower()
        if arg in ['on', '1', 'true', 'yes']:
            self.auto_commit = True
            print(Colors.success("Auto-commit enabled"))
        elif arg in ['off', '0', 'false', 'no']:
            self.auto_commit = False
            print(Colors.success("Auto-commit disabled"))
        else:
            print(Colors.error("Usage: .autocommit [on|off]"))
    
    def _handle_isolation(self, args):
        """Gère la commande .isolation"""
        if not args:
            print(Colors.info("Isolation levels: DEFERRED, IMMEDIATE, EXCLUSIVE"))
            return
        
        level = args[0].upper()
        if level in ['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE']:
            self.isolation_level = level
            print(Colors.success(f"Isolation level set to: {level}"))
        else:
            print(Colors.error("Invalid isolation level. Use: DEFERRED, IMMEDIATE, EXCLUSIVE"))
    
    def _show_history(self):
        """Affiche l'historique des commandes"""
        try:
            histsize = readline.get_current_history_length()
            for i in range(1, histsize + 1):
                cmd = readline.get_history_item(i)
                print(f"{i:4d}  {cmd}")
        except:
            print(Colors.error("Could not display history"))
    
    def _execute_sql(self, sql: str):
        """Exécute une requête SQL et affiche le résultat"""
        if not self.db:
            print(Colors.error("No database connection"))
            return
        
        try:
            # Nettoyer la requête
            sql = sql.strip()
            if not sql:
                return
            
            # Vérifier s'il y a une transaction active pour les écritures
            sql_upper = sql.upper()
            is_write_operation = any(
                sql_upper.startswith(cmd) for cmd in 
                ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']
            )
            
            if is_write_operation and not hasattr(self.db, 'active_transactions') and not self.auto_commit:
                print(Colors.warning("⚠ No active transaction. Use BEGIN TRANSACTION or enable auto-commit."))
                print(Colors.dim("Add 'BEGIN TRANSACTION;' before your write operations."))
                return
            
            # Exécuter la requête
            start_time = datetime.now()
            result = self.db.execute(sql)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Afficher le résultat
            self._display_result(result, execution_time)
            
            # Mettre à jour le prompt si nécessaire
            self._update_prompt()
            
        except Exception as e:
            print(Colors.error(f"Error: {e}"))
    
    def _display_result(self, result: Dict, execution_time: float):
        """Affiche le résultat d'une requête avec support transactions"""
        if not result.get('success'):
            print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
            return
        
        query_type = result.get('type', '').lower()
        
        # ==================== TRANSACTION DISPLAY ====================
        if query_type == 'transaction':
            message = result.get('message', '')
            if 'started' in message.lower():
                print(Colors.tx_start(f"✓ Transaction started (ID: {result.get('tid', 'N/A')})"))
                print(Colors.dim(f"Isolation: {result.get('isolation', 'DEFERRED')}"))
            elif 'committed' in message.lower():
                print(Colors.tx_commit(f"✓ Transaction {result.get('tid', 'N/A')} committed"))
            elif 'rolled back' in message.lower():
                print(Colors.tx_rollback(f"↺ Transaction {result.get('tid', 'N/A')} rolled back"))
            return
        
        elif query_type == 'savepoint':
            print(Colors.tx_savepoint(f"✓ Savepoint '{result.get('name', 'N/A')}' created"))
            return
        # ==================== FIN TRANSACTION DISPLAY ====================
        
        elif query_type == 'select':
            rows = result.get('rows', [])
            columns = result.get('columns', [])
            count = result.get('count', 0)
            
            if count == 0:
                print(Colors.warning("No rows returned"))
            else:
                # Afficher l'en-tête avec indication de transaction
                if self.current_tx_id:
                    header = f"{Colors.tx_active('TX:' + str(self.current_tx_id))} | "
                else:
                    header = ""
                header += " | ".join(Colors.highlight(col) for col in columns)
                print(header)
                print(Colors.dim('─' * len(header)))
                
                # Afficher les données (limité à 50 lignes)
                for i, row in enumerate(rows[:50]):
                    if isinstance(row, dict):
                        values = [str(v) if v is not None else Colors.dim("NULL") for v in row.values()]
                    elif isinstance(row, (list, tuple)):
                        values = [str(v) if v is not None else Colors.dim("NULL") for v in row]
                    else:
                        values = [str(row)]
                    
                    # Colorer les valeurs
                    colored_values = []
                    for val in values:
                        if val == "NULL":
                            colored_values.append(Colors.dim(val))
                        elif val.isdigit() or (val.replace('.', '', 1).isdigit() and val.count('.') <= 1):
                            colored_values.append(Colors.sql_number(val))
                        elif val.startswith("'") and val.endswith("'"):
                            colored_values.append(Colors.sql_string(val))
                        else:
                            colored_values.append(val)
                    
                    print(" | ".join(colored_values))
                
                if len(rows) > 50:
                    print(Colors.dim(f"... and {len(rows) - 50} more rows"))
                
                print(Colors.dim(f"\n{count} row(s) returned"))
        
        elif query_type == 'insert':
            last_id = result.get('lastrowid', result.get('last_insert_id', 'N/A'))
            rows_affected = result.get('rows_affected', 0)
            
            print(Colors.success(f"✓ Row inserted"))
            if last_id and last_id != 'N/A':
                print(Colors.dim(f"ID: {last_id}"))
            print(Colors.dim(f"Rows affected: {rows_affected}"))
            
            # Afficher un warning si pas dans une transaction
            if hasattr(self.db, 'active_transactions') and not self.db.active_transactions and not self.auto_commit:
                print(Colors.warning("⚠ Warning: Insert not in transaction (auto-commit disabled)"))
        
        elif query_type == 'update' or query_type == 'delete':
            rows_affected = result.get('rows_affected', 0)
            print(Colors.success(f"✓ Query successful"))
            print(Colors.dim(f"Rows affected: {rows_affected}"))
            
            # Afficher un warning si pas dans une transaction
            if hasattr(self.db, 'active_transactions') and not self.db.active_transactions and not self.auto_commit:
                print(Colors.warning("⚠ Warning: Operation not in transaction (auto-commit disabled)"))
        
        elif query_type == 'show_tables':
            tables = result.get('tables', [])
            if tables:
                print(Colors.success(f"Found {len(tables)} table(s):"))
                for table in tables:
                    row_count = table.get('rows', 0)
                    size = table.get('size_kb', 0)
                    print(f"  • {Colors.highlight(table['table'])} "
                          f"({Colors.sql_number(str(row_count))} rows, "
                          f"{Colors.sql_number(f'{size}KB')})")
            else:
                print(Colors.warning("No tables found"))
        
        elif query_type == 'describe':
            columns = result.get('columns', [])
            if columns:
                print(Colors.success(f"Table structure:"))
                for col in columns:
                    null_str = "NOT NULL" if not col.get('null') else "NULL"
                    default_str = f"DEFAULT {col.get('default')}" if col.get('default') else ""
                    key_str = f" {col.get('key')}" if col.get('key') else ""
                    extra_str = f" {col.get('extra')}" if col.get('extra') else ""
                    
                    line = f"  {Colors.highlight(col['field'])} {col['type']} {null_str} {default_str}{key_str}{extra_str}"
                    print(line.strip())
            else:
                print(Colors.warning("No columns found"))
        
        elif query_type == 'stats':
            stats = result.get('database', {})
            print(Colors.success("Database statistics:"))
            
            # Statistiques de transaction
            if 'active_transactions' in stats:
                active_tx = stats['active_transactions']
                tx_color = Colors.RED if active_tx > 0 else Colors.GREEN
                print(f"  Active transactions: {tx_color}{active_tx}{Colors.RESET}")
            
            if 'transactions_total' in stats:
                print(f"  Total transactions: {stats['transactions_total']}")
            
            for key, value in stats.items():
                if key not in ['active_transactions', 'transactions_total']:
                    if isinstance(value, dict):
                        print(f"  {key}:")
                        for k, v in value.items():
                            print(f"    {k}: {v}")
                    else:
                        print(f"  {key}: {value}")
        
        elif query_type == 'vacuum':
            print(Colors.success("✓ Database optimized"))
        
        elif query_type == 'backup':
            print(Colors.success(f"✓ Backup created: {result.get('backup_file', 'N/A')}"))
        
        elif query_type == 'help':
            print(result.get('message', ''))
        
        else:
            print(Colors.success(f"✓ Query executed successfully"))
        
        # Afficher le temps d'exécution
        if 'execution_time' in result:
            time_str = f"{result['execution_time']:.3f}s"
        else:
            time_str = f"{execution_time:.3f}s"
        
        print(Colors.dim(f"Time: {time_str}"))
    
    # ==================== BUILT-IN COMMANDS ====================
    
    def do_help(self, arg: str):
        """Affiche l'aide"""
        help_text = f"""
{Colors.highlight("GSQL v3.1.0 - Complete Transaction Support")}

{Colors.underline("TRANSACTION COMMANDS (FULL SUPPORT):")}
  BEGIN TRANSACTION              - Start deferred transaction
  BEGIN IMMEDIATE TRANSACTION    - Start with immediate lock
  BEGIN EXCLUSIVE TRANSACTION    - Start with exclusive lock
  COMMIT                         - Commit current transaction
  ROLLBACK                       - Rollback current transaction
  SAVEPOINT name                 - Create savepoint
  ROLLBACK TO SAVEPOINT name     - Rollback to savepoint
  RELEASE SAVEPOINT name         - Release savepoint

{Colors.underline("SQL COMMANDS:")}
  SELECT * FROM table [WHERE condition] [LIMIT n]
  INSERT INTO table (col1, col2) VALUES (val1, val2)
  UPDATE table SET col=value [WHERE condition]
  DELETE FROM table [WHERE condition]
  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
  DROP TABLE name
  ALTER TABLE name ADD COLUMN col TYPE
  CREATE INDEX idx_name ON table(column)

{Colors.underline("GSQL SPECIAL COMMANDS:")}
  SHOW TABLES                    - List all tables
  DESCRIBE table                 - Show table structure
  STATS                          - Show database statistics
  VACUUM                         - Optimize database
  BACKUP [path]                  - Create database backup
  HELP                           - This help message

{Colors.underline("DOT COMMANDS (SQLite style):")}
  .tables                        - List tables
  .schema [table]                - Show schema
  .stats                         - Show stats
  .transactions / .tx            - Show active transactions
  .autocommit [on|off]           - Toggle auto-commit mode
  .isolation [level]             - Set isolation level
  .help                          - Show help
  .backup [file]                 - Create backup
  .vacuum                        - Optimize database
  .exit / .quit                  - Exit shell
  .clear                         - Clear screen
  .history                       - Show command history

{Colors.underline("SHELL COMMANDS:")}
  exit, quit, Ctrl+D             - Exit GSQL
  Ctrl+C                         - Cancel current command
  Ctrl+Z                         - Suspend (Unix only)

{Colors.underline("TRANSACTION TIPS:")}
  • Use BEGIN TRANSACTION before write operations
  • COMMIT to save changes, ROLLBACK to cancel
  • Use SAVEPOINT for nested rollbacks
  • Watch for transaction timeouts (>5s warning)
  • Enable auto-commit with .autocommit on
        """
        print(help_text.strip())
    
    def do_transactions(self, arg: str):
        """Affiche les transactions actives"""
        if not self.db:
            print(Colors.error("No database connection"))
            return
        
        active_tx = 0
        if hasattr(self.db, 'active_transactions'):
            active_tx = len(self.db.active_transactions)
        
        if active_tx == 0:
            print(Colors.info("No active transactions"))
            if self.auto_commit:
                print(Colors.dim("Auto-commit mode is enabled"))
            return
        
        print(Colors.success(f"Active transactions: {active_tx}"))
        print(Colors.dim("─" * 50))
        
        for tid, tx_info in self.db.active_transactions.items():
            state = tx_info.get('state', 'UNKNOWN')
            isolation = tx_info.get('isolation', 'DEFERRED')
            start_time = tx_info.get('start_time', 'N/A')
            
            # Calculer la durée
            duration_str = "N/A"
            if start_time:
                if isinstance(start_time, str):
                    try:
                        start_time = datetime.fromisoformat(start_time)
                    except:
                        pass
                
                if hasattr(start_time, 'isoformat'):
                    duration = (datetime.now() - start_time).total_seconds()
                    duration_str = f"{duration:.1f}s"
                    
                    # Avertissement si trop long
                    if duration > self.tx_warning_time:
                        duration_str = Colors.warning(f"{duration:.1f}s ⚠")
            
            # Couleur selon l'état
            if state == 'ACTIVE':
                state_color = Colors.tx_active
            elif state == 'COMMITTED':
                state_color = Colors.tx_commit
            elif state == 'ROLLED_BACK':
                state_color = Colors.tx_rollback
            else:
                state_color = Colors.dim
            
            print(f"{Colors.highlight('TID:')} {Colors.BOLD}{tid}{Colors.RESET}")
            print(f"  {Colors.highlight('State:')} {state_color(state)}")
            print(f"  {Colors.highlight('Isolation:')} {isolation}")
            print(f"  {Colors.highlight('Duration:')} {duration_str}")
            
            # Afficher les savepoints
            savepoints = tx_info.get('savepoints', [])
            if savepoints:
                print(f"  {Colors.highlight('Savepoints:')} {', '.join(savepoints)}")
            
            # Afficher les requêtes exécutées
            queries = tx_info.get('queries', [])
            if queries:
                print(f"  {Colors.highlight('Queries:')} {len(queries)} executed")
                if len(queries) <= 3:
                    for i, query in enumerate(queries[-3:], 1):
                        short_query = query[:50] + "..." if len(query) > 50 else query
                        print(f"    {i}. {Colors.dim(short_query)}")
            
            print(Colors.dim("─" * 50))
        
        print(Colors.warning(f"⚠ Total active transactions: {active_tx}"))
        print(Colors.dim("Use COMMIT or ROLLBACK to finish transactions"))
    
    def do_exit(self, arg: str):
        """Quitte le shell GSQL"""
        # Vérifier les transactions actives
        if self.db and hasattr(self.db, 'active_transactions'):
            active_tx = len(self.db.active_transactions)
            if active_tx > 0:
                print(Colors.warning(f"⚠ Warning: {active_tx} active transaction(s)"))
                response = input("Rollback all transactions? (y/N): ").strip().lower()
                if response in ['y', 'yes']:
                    # Rollback toutes les transactions
                    for tid in list(self.db.active_transactions.keys()):
                        try:
                            self.db.execute("ROLLBACK")
                        except:
                            pass
                    print(Colors.tx_rollback("All transactions rolled back"))
        
        print(Colors.info("Goodbye!"))
        return True
    
    def do_quit(self, arg: str):
        """Quitte le shell GSQL"""
        return self.do_exit(arg)
    
    def do_clear(self, arg: str):
        """Efface l'écran"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def do_history(self, arg: str):
        """Affiche l'historique des commandes"""
        self._show_history()
    
    def do_autocommit(self, arg: str):
        """Active/désactive le mode auto-commit"""
        self._handle_autocommit(arg.split() if arg else [])
    
    def do_isolation(self, arg: str):
        """Définit le niveau d'isolation"""
        self._handle_isolation(arg.split() if arg else [])
    
    # ==================== SHELL CONTROL ====================
    
    def emptyline(self):
        """Ne rien faire sur ligne vide"""
        pass
    
    def precmd(self, line: str) -> str:
        """Avant l'exécution de la commande"""
        # Enregistrer dans l'historique (sauf les commandes spéciales)
        if line and not line.startswith('.'):
            readline.add_history(line)
        return line
    
    def postcmd(self, stop: bool, line: str) -> bool:
        """Après l'exécution de la commande"""
        # Mettre à jour le prompt après chaque commande
        self._update_prompt()
        return stop
    
    def sigint_handler(self, signum, frame):
        """Gère Ctrl+C"""
        print("\n" + Colors.warning("Interrupted (Ctrl+C)"))
        
        # Si dans une transaction, proposer de rollback
        if self.current_tx_id:
            print(Colors.warning(f"⚠ Transaction {self.current_tx_id} is still active"))
            response = input("Rollback transaction? (y/N): ").strip().lower()
            if response in ['y', 'yes']:
                try:
                    self.db.execute("ROLLBACK")
                    print(Colors.tx_rollback(f"Transaction {self.current_tx_id} rolled back"))
                except:
                    print(Colors.error("Failed to rollback"))
        
        self._update_prompt()

# ==================== MAIN GSQL APPLICATION ====================

class GSQLApp:
    """Application GSQL principale avec support transactions"""
    
    def __init__(self):
        self.config = self._load_config()
        self.db = None
        self.executor = None
        self.function_manager = None
        self.nlp_translator = None
        self.completer = None
        
        # Configurer le logging
        setup_logging(
            level=self.config.get('log_level', 'INFO'),
            log_file=self.config.get('log_file')
        )
        
        logger.info(f"GSQL v{__version__} initialized (SQLite with Transaction Support)")
    
    def _load_config(self) -> Dict:
        """Charge la configuration"""
        user_config = config.to_dict()
        
        # Fusionner avec la configuration par défaut
        merged = DEFAULT_CONFIG.copy()
        
        # Mettre à jour avec la configuration utilisateur
        for section in ['database', 'executor', 'shell']:
            if section in user_config:
                merged[section].update(user_config[section])
        
        # Mettre à jour la configuration globale
        config.update(**merged.get('database', {}))
        
        return merged
    
    def _initialize(self, database_path: Optional[str] = None):
        """Initialise les composants GSQL"""
        try:
            print(Colors.info("Initializing GSQL with Transaction Support..."))
            
            # Créer la base de données
            db_config = self.config['database'].copy()
            if database_path:
                db_config['path'] = database_path
            
            self.db = create_database(**db_config)
            
            # Créer l'exécuteur
            self.executor = create_executor(storage=self.db.storage)
            
            # Initialiser les autres composants
            self.function_manager = FunctionManager()
            
            # Gestion du NLP
            if NLP_AVAILABLE and NLToSQLTranslator:
                self.nlp_translator = NLToSQLTranslator()
            else:
                self.nlp_translator = None
            
            # Configurer l'auto-complétion
            self.completer = GSQLCompleter(self.db)
            
            print(Colors.success("✓ GSQL ready with full transaction support!"))
            if hasattr(self.db, 'storage') and hasattr(self.db.storage, 'db_path'):
                print(Colors.dim(f"Database: {self.db.storage.db_path}"))
            print(Colors.dim(f"Buffer pool: {self.config['database']['buffer_pool_size']} pages"))
            print(Colors.dim(f"WAL mode: {'enabled' if self.config['database']['enable_wal'] else 'disabled'}"))
            print(Colors.dim(f"Type 'help' for commands\n"))
            
        except Exception as e:
            print(Colors.error(f"Failed to initialize GSQL: {e}"))
            traceback.print_exc()
            sys.exit(1)
    
    def run_shell(self, database_path: Optional[str] = None):
        """Lance le shell interactif"""
        # Initialiser
        self._initialize(database_path)
        
        # Créer et lancer le shell
        shell = GSQLShell(self)
        
        # Configurer le handler pour Ctrl+C
        signal.signal(signal.SIGINT, shell.sigint_handler)
        
        try:
            shell.cmdloop()
        except KeyboardInterrupt:
            print("\n" + Colors.info("Interrupted"))
        finally:
            self._cleanup()
    
    def run_query(self, query: str, database_path: Optional[str] = None):
        """Exécute une requête unique avec support transactions"""
        try:
            self._initialize(database_path)
            
            # Vérifier si c'est une transaction
            query_upper = query.strip().upper()
            is_transaction = any(
                query_upper.startswith(cmd) for cmd in 
                ['BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT']
            )
            
            # Exécuter la requête
            result = self.db.execute(query)
            
            # Afficher le résultat
            if result.get('success'):
                if is_transaction:
                    # Affichage spécial pour transactions
                    if query_upper.startswith('BEGIN'):
                        print(Colors.tx_start(f"✓ Transaction {result.get('tid', '?')} started"))
                    elif query_upper.startswith('COMMIT'):
                        print(Colors.tx_commit(f"✓ Transaction {result.get('tid', '?')} committed"))
                    elif query_upper.startswith('ROLLBACK'):
                        print(Colors.tx_rollback(f"↺ Transaction {result.get('tid', '?')} rolled back"))
                    elif query_upper.startswith('SAVEPOINT'):
                        savepoint = query.split()[1] if len(query.split()) > 1 else 'unknown'
                        print(Colors.tx_savepoint(f"✓ Savepoint '{savepoint}' created"))
                else:
                    print(Colors.success("Query executed successfully"))
                
                # Afficher les résultats pour SELECT
                if result.get('type') == 'select':
                    rows = result.get('rows', [])
                    if rows:
                        columns = result.get('columns', [])
                        # Afficher l'en-tête
                        print(" | ".join(columns))
                        print("─" * (len(columns) * 10))
                        # Afficher les données
                        for row in rows:
                            if isinstance(row, dict):
                                values = [str(v) if v is not None else "NULL" for v in row.values()]
                            elif isinstance(row, (list, tuple)):
                                values = [str(v) if v is not None else "NULL" for v in row]
                            else:
                                values = [str(row)]
                            print(" | ".join(values))
                        print(f"\n{len(rows)} row(s) returned")
                    else:
                        print("No rows returned")
                
                # Afficher les statistiques
                if 'execution_time' in result:
                    print(f"\nTime: {result['execution_time']:.3f}s")
                
                # Afficher les transactions actives
                if hasattr(self.db, 'active_transactions'):
                    active_tx = len(self.db.active_transactions)
                    if active_tx > 0:
                        print(Colors.tx_active(f"\nActive transactions: {active_tx}"))
                
                return result
            else:
                print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                return None
                
        except Exception as e:
            print(Colors.error(f"Error: {e}"))
            return None
        finally:
            self._cleanup()
    
    def run_script(self, script_path: str, database_path: Optional[str] = None):
        """Exécute un script SQL avec support transactions"""
        try:
            with open(script_path, 'r') as f:
                script = f.read()
            
            self._initialize(database_path)
            
            print(Colors.info(f"Executing script: {script_path}"))
            print(Colors.dim("─" * 40))
            
            # Exécuter le script
            results = self.db.execute_script(script)
            
            # Afficher les résultats
            success_count = sum(1 for r in results if r.get('success'))
            total_count = len(results)
            
            print(Colors.dim("─" * 40))
            print(f"Script execution completed: {Colors.success(str(success_count))}/{total_count} queries successful")
            
            # Afficher les transactions actives
            if hasattr(self.db, 'active_transactions'):
                active_tx = len(self.db.active_transactions)
                if active_tx > 0:
                    print(Colors.warning(f"⚠ Warning: {active_tx} transaction(s) still active"))
                    response = input("Rollback all transactions? (y/N): ").strip().lower()
                    if response in ['y', 'yes']:
                        self.db.execute("ROLLBACK")
                        print(Colors.tx_rollback("All transactions rolled back"))
            
            return results
            
        except Exception as e:
            print(Colors.error(f"Error executing script: {e}"))
            return None
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Nettoie les ressources"""
        try:
            if self.db:
                # Vérifier les transactions actives
                active_tx = 0
                if hasattr(self.db, 'active_transactions'):
                    active_tx = len(self.db.active_transactions)
                    if active_tx > 0:
                        print(Colors.warning(f"⚠ Closing database with {active_tx} active transaction(s)"))
                
                self.db.close()
                print(Colors.dim("Database closed"))
        except:
            pass

# ==================== MAIN FUNCTION ====================

def main():
    """Fonction principale avec support transactions"""
    if not GSQL_AVAILABLE:
        print(Colors.error("GSQL modules not available. Check installation."))
        sys.exit(1)
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description=f"GSQL v{__version__} - SQL Database with Full Transaction Support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
{Colors.underline("Examples:")}
  {Colors.highlight("gsql")}                         # Start interactive shell
  {Colors.highlight("gsql mydb.db")}                 # Open specific database
  {Colors.highlight("gsql -e \"BEGIN TRANSACTION\"")} # Execute transaction
  {Colors.highlight("gsql -f transaction.sql")}      # Execute transaction script
  {Colors.highlight("gsql -s demo_transaction.sql")} # Execute script with tx monitoring

{Colors.underline("Transaction Demo Script:")}
  # demo_transaction.sql
  BEGIN TRANSACTION;
  CREATE TABLE IF NOT EXISTS test (id INT, name TEXT);
  INSERT INTO test VALUES (1, 'Transaction Test');
  SAVEPOINT sp1;
  INSERT INTO test VALUES (2, 'Savepoint Test');
  ROLLBACK TO SAVEPOINT sp1;
  SELECT * FROM test;
  COMMIT;
        """
    )
    
    parser.add_argument(
        'database',
        nargs='?',
        help='Database file (optional, uses default if not specified)'
    )
    
    parser.add_argument(
        '-e', '--execute',
        help='Execute SQL query and exit'
    )
    
    parser.add_argument(
        '-f', '--file',
        help='Execute SQL from file and exit'
    )
    
    parser.add_argument(
        '-s', '--script',
        help='Execute SQL script with transaction monitoring'
    )
    
    parser.add_argument(
        '--no-color',
        action='store_true',
        help='Disable colored output'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )
    
    parser.add_argument(
        '--tx-timeout',
        type=int,
        default=30,
        help='Transaction timeout in seconds (default: 30)'
    )
    
    parser.add_argument(
        '--auto-commit',
        action='store_true',
        help='Enable auto-commit mode'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version=f'GSQL {__version__} (Transaction Support)'
    )
    
    args = parser.parse_args()
    
    # Configurer les couleurs
    if args.no_color:
        # Désactiver les couleurs
        for attr in dir(Colors):
            if not attr.startswith('_') and attr.isupper():
                setattr(Colors, attr, '')
    
    # Configurer le verbose
    if args.verbose:
        setup_logging(level='DEBUG')
    
    # Configurer le timeout des transactions
    if args.tx_timeout:
        DEFAULT_CONFIG['database']['transaction_timeout'] = args.tx_timeout
    
    # Configurer auto-commit
    if args.auto_commit:
        DEFAULT_CONFIG['executor']['auto_commit'] = True
    
    # Créer l'application
    app = GSQLApp()
    
    # Exécuter selon le mode
    if args.execute:
        # Mode exécution unique
        app.run_query(args.execute, args.database)
    elif args.file:
        # Mode fichier
        with open(args.file, 'r') as f:
            app.run_query(f.read(), args.database)
    elif args.script:
        # Mode script avec monitoring
        app.run_script(args.script, args.database)
    else:
        # Mode shell interactif
        app.run_shell(args.database)

# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    main()
