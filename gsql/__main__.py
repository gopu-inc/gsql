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
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

# ==================== IMPORTS ====================

# Définir __version__ si non présent
__version__ = "3.1.0"

# Import des modules GSQL avec fallbacks plus robustes
try:
    # Config simple si config.py n'existe pas
    try:
        from . import config
        CONFIG_AVAILABLE = True
    except ImportError:
        CONFIG_AVAILABLE = False
        # Créer un module config simple
        class SimpleConfig:
            def __init__(self):
                self._config = {
                    'base_dir': str(Path.home() / '.gsql'),
                    'log_level': 'INFO',
                    'colors': True,
                    'verbose_errors': True,
                    'auto_commit': False,
                    'transaction_timeout': 30,
                    'database': {
                        'enable_wal': True,
                        'buffer_pool_size': 100
                    }
                }
            
            def get(self, key: str, default=None):
                """Get nested config value using dot notation"""
                keys = key.split('.')
                value = self._config
                for k in keys:
                    if isinstance(value, dict) and k in value:
                        value = value[k]
                    else:
                        return default
                return value
            
            def set(self, key: str, value):
                """Set config value"""
                keys = key.split('.')
                data = self._config
                for k in keys[:-1]:
                    data = data.setdefault(k, {})
                data[keys[-1]] = value
            
            def to_dict(self):
                """Return full config dict"""
                return self._config.copy()
            
            def update(self, **kwargs):
                """Update config with kwargs"""
                self._config.update(kwargs)
        
        config = SimpleConfig()
    
    # Import du database.py
    try:
        from .database import create_database, Database, connect
        DATABASE_AVAILABLE = True
    except ImportError as e:
        DATABASE_AVAILABLE = False
        print(f"Warning: Database module not available: {e}")
        
        class MockDatabase:
            def __init__(self, *args, **kwargs):
                self.active_transactions = {}
                self.storage = None
            
            def execute(self, sql: str, params=None):
                return {'success': False, 'message': 'Database not available'}
            
            def execute_script(self, script: str):
                return []
            
            def close(self):
                pass
        
        def create_database(**kwargs):
            return MockDatabase()
        
        def connect(**kwargs):
            return MockDatabase()
        
        Database = MockDatabase
    
    # Gérer les imports optionnels
    try:
        from .storage import SQLiteStorage, create_storage
        STORAGE_AVAILABLE = True
    except ImportError:
        STORAGE_AVAILABLE = False
        
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
    # Définir des fallbacks pour les variables non définies
    if 'CONFIG_AVAILABLE' not in locals():
        CONFIG_AVAILABLE = False
    if 'DATABASE_AVAILABLE' not in locals():
        DATABASE_AVAILABLE = False

# ==================== LOGGING ====================

def setup_logging(level='INFO', log_file=None):
    """Configure le logging"""
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Formatter personnalisé
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    
    handlers = [console_handler]
    
    # Handler fichier si spécifié
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        handlers.append(file_handler)
    
    # Configuration principale
    logging.basicConfig(
        level=log_level,
        handlers=handlers
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
        'max_transactions': 10,
        'journal_mode': 'WAL',
        'synchronous': 'NORMAL'
    },
    'executor': {
        'enable_nlp': False,
        'enable_learning': False,
        'auto_commit': False,
        'max_execution_time': 30,
        'row_limit': 1000
    },
    'shell': {
        'prompt': 'gsql> ',
        'history_file': '.gsql_history',
        'max_history': 1000,
        'colors': True,
        'autocomplete': True,
        'show_transaction_status': True,
        'transaction_warning_time': 5,
        'pager': None
    }
}

# ==================== ENUMS & DATA CLASSES ====================

class IsolationLevel(Enum):
    """Niveaux d'isolation des transactions"""
    DEFERRED = "DEFERRED"
    IMMEDIATE = "IMMEDIATE"
    EXCLUSIVE = "EXCLUSIVE"

class TransactionState(Enum):
    """États des transactions"""
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ROLLED_BACK = "ROLLED_BACK"
    SAVEPOINT = "SAVEPOINT"

@dataclass
class TransactionInfo:
    """Information sur une transaction"""
    id: str
    state: TransactionState
    isolation: IsolationLevel
    start_time: datetime
    savepoints: List[str]
    queries: List[str]
    
    @property
    def duration(self) -> float:
        """Durée de la transaction en secondes"""
        return (datetime.now() - self.start_time).total_seconds()

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
    BLITE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    DEFAULT = '\033[39m'
    
    # Couleurs vives
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'
    
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
    TX_WARNING = '\033[38;5;214m'  # Orange vif
    
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
        return Colors.colorize(text, Colors.BLUE)
    
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
    
    @staticmethod
    def tx_warning(text):
        """Avertissement transaction"""
        return Colors.colorize(text, Colors.TX_WARNING)

# ==================== UTILITY FUNCTIONS ====================

def format_duration(seconds: float) -> str:
    """Formate une durée en secondes"""
    if seconds < 0.001:
        return f"{seconds*1_000_000:.0f}µs"
    elif seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = seconds / 60
        return f"{minutes:.1f}min"

def safe_str(value: Any, max_len: int = 50) -> str:
    """Convertit une valeur en string de manière sécurisée"""
    if value is None:
        return "NULL"
    
    text = str(value)
    if len(text) > max_len:
        return text[:max_len] + "..."
    return text

def detect_sql_type(sql: str) -> str:
    """Détecte le type de requête SQL"""
    sql_upper = sql.strip().upper()
    
    # Commandes transactionnelles
    if sql_upper.startswith(('BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE')):
        return 'transaction'
    
    # Autres types
    if sql_upper.startswith('SELECT'):
        return 'select'
    elif sql_upper.startswith('INSERT'):
        return 'insert'
    elif sql_upper.startswith('UPDATE'):
        return 'update'
    elif sql_upper.startswith('DELETE'):
        return 'delete'
    elif sql_upper.startswith('CREATE'):
        return 'create'
    elif sql_upper.startswith('DROP'):
        return 'drop'
    elif sql_upper.startswith('ALTER'):
        return 'alter'
    elif sql_upper.startswith('SHOW'):
        return 'show'
    elif sql_upper.startswith('DESCRIBE'):
        return 'describe'
    elif sql_upper.startswith('EXPLAIN'):
        return 'explain'
    elif sql_upper.startswith('VACUUM'):
        return 'vacuum'
    elif sql_upper.startswith('BACKUP'):
        return 'backup'
    else:
        return 'unknown'

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
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'WITH',
            'RECURSIVE', 'OVER', 'PARTITION', 'BY', 'ROW_NUMBER',
            'RANK', 'DENSE_RANK', 'LEAD', 'LAG', 'FIRST_VALUE',
            'LAST_VALUE', 'NTH_VALUE', 'NTILE'
        ]
        
        # Commandes spéciales GSQL
        self.gsql_commands = [
            '.tables', '.schema', '.stats', '.help', '.backup',
            '.vacuum', '.exit', '.quit', '.clear', '.history',
            '.transactions', '.tx', '.autocommit', '.isolation',
            '.mode', '.timer', '.headers', '.nullvalue', '.output',
            '.read', '.save', '.restore', '.dump'
        ]
        
        self.table_names = []
        self.column_names = {}
        self.view_names = []
        self.index_names = []
        
        if database and hasattr(database, 'storage'):
            self._refresh_schema()
    
    def _refresh_schema(self):
        """Rafraîchit le schéma depuis la base"""
        try:
            if self.database and hasattr(self.database, 'execute'):
                # Récupérer les tables
                result = self.database.execute("SELECT name FROM sqlite_master WHERE type='table'")
                if result.get('success') and result.get('rows'):
                    self.table_names = [row[0] if isinstance(row, (list, tuple)) else row.get('name', '') 
                                      for row in result.get('rows', [])]
                
                # Récupérer les vues
                result = self.database.execute("SELECT name FROM sqlite_master WHERE type='view'")
                if result.get('success') and result.get('rows'):
                    self.view_names = [row[0] if isinstance(row, (list, tuple)) else row.get('name', '')
                                      for row in result.get('rows', [])]
                
                # Récupérer les index
                result = self.database.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'")
                if result.get('success') and result.get('rows'):
                    self.index_names = [row[0] if isinstance(row, (list, tuple)) else row.get('name', '')
                                       for row in result.get('rows', [])]
                
                # Récupérer les colonnes pour chaque table
                self.column_names = {}
                for table in self.table_names:
                    try:
                        result = self.database.execute(f"PRAGMA table_info({table})")
                        if result.get('success') and result.get('rows'):
                            self.column_names[table] = [
                                row[1] if isinstance(row, (list, tuple)) else row.get('name', '')
                                for row in result.get('rows', [])
                            ]
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"Error refreshing schema: {e}")
    
    def complete(self, text: str, state: int) -> Optional[str]:
        """Fonction de complétion pour readline avec support transactions"""
        if state == 0:
            line = readline.get_line_buffer().strip()
            line_lower = line.lower()
            
            if not line:
                self.matches = []
            elif line.startswith('.'):
                # Commandes dot
                self.matches = [cmd for cmd in self.gsql_commands 
                              if cmd.lower().startswith(text.lower())]
            else:
                # Détection contextuelle basée sur le dernier mot-clé
                tokens = line.upper().split()
                
                if not tokens:
                    self.matches = [kw for kw in self.keywords if kw.lower().startswith(text.lower())]
                else:
                    last_token = tokens[-1] if tokens else ""
                    
                    # Complétion contextuelle
                    if last_token in ['FROM', 'JOIN', 'INTO', 'UPDATE']:
                        # Tables après FROM, JOIN, INTO, UPDATE
                        self.matches = [name for name in self.table_names + self.view_names 
                                      if name.lower().startswith(text.lower())]
                    elif last_token in ['TABLE', 'VIEW', 'INDEX'] and len(tokens) >= 2:
                        # Noms d'objets après CREATE/DROP TABLE/VIEW/INDEX
                        if tokens[-2] in ['CREATE', 'DROP']:
                            all_objects = self.table_names + self.view_names + self.index_names
                            self.matches = [name for name in all_objects 
                                          if name.lower().startswith(text.lower())]
                        else:
                            self.matches = []
                    elif last_token in ['ON'] and len(tokens) >= 3:
                        # Colonnes après ON dans les JOINs
                        table = tokens[-2] if len(tokens) >= 2 else None
                        if table and table in self.column_names:
                            self.matches = [col for col in self.column_names[table] 
                                          if col.lower().startswith(text.lower())]
                        else:
                            self.matches = []
                    elif any(tok in ['WHERE', 'SET', 'ORDER', 'GROUP'] for tok in tokens):
                        # Chercher la table la plus récente
                        table = None
                        for i, token in enumerate(tokens):
                            if token in ['FROM', 'JOIN', 'UPDATE'] and i + 1 < len(tokens):
                                table = tokens[i + 1]
                                break
                        
                        if table and table in self.column_names:
                            self.matches = [col for col in self.column_names[table] 
                                          if col.lower().startswith(text.lower())]
                        else:
                            # Toutes les colonnes de toutes les tables
                            all_columns = [col for cols in self.column_names.values() for col in cols]
                            self.matches = [col for col in all_columns 
                                          if col.lower().startswith(text.lower())]
                    else:
                        # Complétion par défaut (mots-clés + tables)
                        all_options = self.keywords + self.table_names + self.view_names
                        self.matches = [opt for opt in all_options 
                                      if opt.lower().startswith(text.lower())]
        
        try:
            return self.matches[state] if self.matches else None
        except IndexError:
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
        self.isolation_level = 'DEFERRED'
        self.show_headers = True
        self.show_timer = True
        self.null_value = 'NULL'
        self.output_file = None
        
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
                    print(Colors.tx_warning(f"⚠ Transaction active depuis {elapsed:.1f}s. Pensez à COMMIT ou ROLLBACK."))
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
        atexit.register(self._save_history)
    
    def _save_history(self):
        """Sauvegarde l'historique"""
        try:
            readline.write_history_file(str(self.history_file))
        except Exception as e:
            logger.debug(f"Failed to save history: {e}")
    
    def _write_output(self, text: str):
        """Écrit la sortie dans le fichier ou la console"""
        if self.output_file:
            try:
                with open(self.output_file, 'a', encoding='utf-8') as f:
                    f.write(text + '\n')
            except Exception as e:
                print(Colors.error(f"Error writing to output file: {e}"))
                self.output_file = None
                print(text)
        else:
            print(text)
    
    # ==================== COMMAND HANDLING ====================
    
    def default(self, line: str):
        """Gère les commandes SQL par défaut"""
        if not line.strip():
            return
        
        # Vérifier les commandes spéciales avec point
        if line.startswith('.'):
            self._handle_dot_command(line)
            return
        
        # Exécuter la requête SQL
        self._execute_sql(line)
    
    def _handle_dot_command(self, command: str):
        """Gère les commandes avec point (comme SQLite)"""
        parts = command[1:].strip().split()
        cmd = parts[0].lower() if parts else ""
        args = parts[1:] if len(parts) > 1 else []
        
        handlers = {
            'tables': self._dot_tables,
            'schema': self._dot_schema,
            'stats': self._dot_stats,
            'transactions': self._dot_transactions,
            'tx': self._dot_transactions,
            'help': self._dot_help,
            'backup': self._dot_backup,
            'vacuum': self._dot_vacuum,
            'autocommit': self._dot_autocommit,
            'isolation': self._dot_isolation,
            'mode': self._dot_mode,
            'timer': self._dot_timer,
            'headers': self._dot_headers,
            'nullvalue': self._dot_nullvalue,
            'output': self._dot_output,
            'read': self._dot_read,
            'dump': self._dot_dump,
            'exit': lambda a: True,
            'quit': lambda a: True,
            'clear': lambda a: self.do_clear(""),
            'history': lambda a: self._show_history(),
        }
        
        if cmd in handlers:
            result = handlers[cmd](args)
            if result is True:
                return True
        else:
            self._write_output(Colors.error(f"Unknown command: .{cmd}"))
            self._write_output(Colors.dim("Try .help for available commands"))
    
    def _dot_tables(self, args):
        """Commande .tables"""
        self._execute_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    
    def _dot_schema(self, args):
        """Commande .schema"""
        if args:
            table = args[0]
            self._execute_sql(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
        else:
            self._execute_sql("SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name")
    
    def _dot_stats(self, args):
        """Commande .stats"""
        if self.db and hasattr(self.db, 'execute'):
            result = self.db.execute("STATS")
            self._display_result(result, 0)
    
    def _dot_transactions(self, args):
        """Commande .transactions ou .tx"""
        self.do_transactions("")
    
    def _dot_help(self, args):
        """Commande .help"""
        self.do_help("")
    
    def _dot_backup(self, args):
        """Commande .backup"""
        file = args[0] if args else None
        if file:
            self._execute_sql(f"BACKUP '{file}'")
        else:
            self._execute_sql("BACKUP")
    
    def _dot_vacuum(self, args):
        """Commande .vacuum"""
        self._execute_sql("VACUUM")
    
    def _dot_autocommit(self, args):
        """Commande .autocommit"""
        if not args:
            status = "ON" if self.auto_commit else "OFF"
            self._write_output(f"Auto-commit: {Colors.highlight(status)}")
            return
        
        arg = args[0].lower()
        if arg in ['on', '1', 'true', 'yes']:
            self.auto_commit = True
            self._write_output(Colors.success("Auto-commit enabled"))
        elif arg in ['off', '0', 'false', 'no']:
            self.auto_commit = False
            self._write_output(Colors.success("Auto-commit disabled"))
        else:
            self._write_output(Colors.error("Usage: .autocommit [on|off]"))
    
    def _dot_isolation(self, args):
        """Commande .isolation"""
        if not args:
            self._write_output(Colors.info("Isolation levels: DEFERRED, IMMEDIATE, EXCLUSIVE"))
            self._write_output(f"Current: {Colors.highlight(self.isolation_level)}")
            return
        
        level = args[0].upper()
        if level in ['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE']:
            self.isolation_level = level
            self._write_output(Colors.success(f"Isolation level set to: {level}"))
        else:
            self._write_output(Colors.error("Invalid isolation level. Use: DEFERRED, IMMEDIATE, EXCLUSIVE"))
    
    def _dot_mode(self, args):
        """Commande .mode"""
        if not args:
            self._write_output(Colors.info("Available modes: list, column, line, csv, tabs, html, insert"))
            return
        
        mode = args[0].lower()
        self._write_output(Colors.info(f"Mode set to: {mode}"))
        # Implémentation à compléter selon les besoins
    
    def _dot_timer(self, args):
        """Commande .timer"""
        if not args:
            status = "ON" if self.show_timer else "OFF"
            self._write_output(f"Timer: {Colors.highlight(status)}")
            return
        
        arg = args[0].lower()
        if arg in ['on', '1', 'true', 'yes']:
            self.show_timer = True
            self._write_output(Colors.success("Timer enabled"))
        elif arg in ['off', '0', 'false', 'no']:
            self.show_timer = False
            self._write_output(Colors.success("Timer disabled"))
        else:
            self._write_output(Colors.error("Usage: .timer [on|off]"))
    
    def _dot_headers(self, args):
        """Commande .headers"""
        if not args:
            status = "ON" if self.show_headers else "OFF"
            self._write_output(f"Headers: {Colors.highlight(status)}")
            return
        
        arg = args[0].lower()
        if arg in ['on', '1', 'true', 'yes']:
            self.show_headers = True
            self._write_output(Colors.success("Headers enabled"))
        elif arg in ['off', '0', 'false', 'no']:
            self.show_headers = False
            self._write_output(Colors.success("Headers disabled"))
        else:
            self._write_output(Colors.error("Usage: .headers [on|off]"))
    
    def _dot_nullvalue(self, args):
        """Commande .nullvalue"""
        if not args:
            self._write_output(f"Null value: {Colors.highlight(self.null_value)}")
            return
        
        self.null_value = args[0]
        self._write_output(Colors.success(f"Null value set to: {self.null_value}"))
    
    def _dot_output(self, args):
        """Commande .output"""
        if not args:
            if self.output_file:
                self._write_output(f"Output file: {Colors.highlight(self.output_file)}")
            else:
                self._write_output("Output: stdout")
            return
        
        filename = args[0]
        if filename.lower() == 'stdout':
            self.output_file = None
            self._write_output(Colors.success("Output redirected to stdout"))
        else:
            self.output_file = filename
            self._write_output(Colors.success(f"Output redirected to: {filename}"))
    
    def _dot_read(self, args):
        """Commande .read"""
        if not args:
            self._write_output(Colors.error("Usage: .read <filename>"))
            return
        
        filename = args[0]
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                script = f.read()
            
            # Exécuter le script ligne par ligne
            for line in script.split(';'):
                line = line.strip()
                if line:
                    self._execute_sql(line)
                    
        except FileNotFoundError:
            self._write_output(Colors.error(f"File not found: {filename}"))
        except Exception as e:
            self._write_output(Colors.error(f"Error reading file: {e}"))
    
    def _dot_dump(self, args):
        """Commande .dump"""
        if self.db and hasattr(self.db.storage, 'connection'):
            try:
                conn = self.db.storage.connection
                for line in conn.iterdump():
                    self._write_output(line)
            except Exception as e:
                self._write_output(Colors.error(f"Error dumping database: {e}"))
    
    def _show_history(self):
        """Affiche l'historique des commandes"""
        try:
            histsize = readline.get_current_history_length()
            for i in range(max(1, histsize - 20), histsize + 1):
                cmd = readline.get_history_item(i)
                if cmd:
                    self._write_output(f"{i:4d}  {cmd}")
        except Exception:
            self._write_output(Colors.error("Could not display history"))
    
    def _execute_sql(self, sql: str):
        """Exécute une requête SQL et affiche le résultat"""
        if not self.db:
            self._write_output(Colors.error("No database connection"))
            return
        
        try:
            # Nettoyer la requête
            sql = sql.strip()
            if not sql:
                return
            
            # Vérifier si c'est une commande transactionnelle
            sql_upper = sql.upper()
            is_transaction_command = any(
                sql_upper.startswith(cmd) for cmd in 
                ['BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE']
            )
            
            if is_transaction_command:
                self._execute_transaction_command(sql)
                return
            
            # Vérifier s'il y a une transaction active pour les écritures
            is_write_operation = any(
                sql_upper.startswith(cmd) for cmd in 
                ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'REPLACE']
            )
            
            if is_write_operation:
                # Vérifier les transactions actives
                has_active_tx = False
                if hasattr(self.db, 'active_transactions'):
                    has_active_tx = len(self.db.active_transactions) > 0
                
                if not has_active_tx and not self.auto_commit:
                    self._write_output(Colors.warning("⚠ No active transaction. Use BEGIN TRANSACTION or enable auto-commit."))
                    self._write_output(Colors.dim("Add 'BEGIN TRANSACTION;' before your write operations."))
                    return
            
            # Exécuter la requête
            start_time = time.time()
            result = self.db.execute(sql)
            execution_time = time.time() - start_time
            
            # Afficher le résultat
            self._display_result(result, execution_time)
            
            # Mettre à jour le prompt si nécessaire
            self._update_prompt()
            
        except Exception as e:
            self._write_output(Colors.error(f"Error: {e}"))
            if hasattr(self.gsql, 'config') and self.gsql.config.get('verbose_errors'):
                traceback.print_exc()
    
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
                self._write_output(Colors.tx_start(f"✓ Transaction {self.current_tx_id} started"))
                isolation = result.get('isolation', 'DEFERRED')
                self._write_output(Colors.dim(f"Isolation: {isolation}"))
                
            elif sql_upper.startswith('COMMIT'):
                # Transaction validée
                self._write_output(Colors.tx_commit(f"✓ Transaction {result.get('tid', '?')} committed"))
                self.current_tx_id = None
                self.tx_start_time = None
                
            elif sql_upper.startswith('ROLLBACK'):
                # Transaction annulée
                if 'TO SAVEPOINT' in sql_upper:
                    savepoint = sql.split()[-1]
                    self._write_output(Colors.tx_rollback(f"↺ Rollback to savepoint '{savepoint}'"))
                else:
                    self._write_output(Colors.tx_rollback(f"↺ Transaction {result.get('tid', '?')} rolled back"))
                    self.current_tx_id = None
                    self.tx_start_time = None
                    
            elif sql_upper.startswith('SAVEPOINT'):
                savepoint = sql.split()[1] if len(sql.split()) > 1 else 'unknown'
                self._write_output(Colors.tx_savepoint(f"✓ Savepoint '{savepoint}' created"))
                
            elif sql_upper.startswith('RELEASE SAVEPOINT'):
                savepoint = sql.split()[2] if len(sql.split()) > 2 else 'unknown'
                self._write_output(Colors.tx_savepoint(f"✓ Savepoint '{savepoint}' released"))
            
            # Mettre à jour le prompt
            self._update_prompt()
            
        except Exception as e:
            self._write_output(Colors.error(f"Transaction error: {e}"))
    
    def _display_result(self, result: Dict, execution_time: float):
        """Affiche le résultat d'une requête avec support transactions"""
        if not result.get('success'):
            self._write_output(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
            return
        
        query_type = result.get('type', '').lower()
        
        # ==================== TRANSACTION DISPLAY ====================
        if query_type == 'transaction':
            message = result.get('message', '')
            if 'started' in message.lower():
                self._write_output(Colors.tx_start(f"✓ Transaction started (ID: {result.get('tid', 'N/A')})"))
                self._write_output(Colors.dim(f"Isolation: {result.get('isolation', 'DEFERRED')}"))
            elif 'committed' in message.lower():
                self._write_output(Colors.tx_commit(f"✓ Transaction {result.get('tid', 'N/A')} committed"))
            elif 'rolled back' in message.lower():
                self._write_output(Colors.tx_rollback(f"↺ Transaction {result.get('tid', 'N/A')} rolled back"))
            return
        
        elif query_type == 'savepoint':
            self._write_output(Colors.tx_savepoint(f"✓ Savepoint '{result.get('name', 'N/A')}' created"))
            return
        # ==================== FIN TRANSACTION DISPLAY ====================
        
        elif query_type == 'select':
            rows = result.get('rows', [])
            columns = result.get('columns', [])
            count = result.get('count', len(rows))
            
            if count == 0:
                self._write_output(Colors.warning("No rows returned"))
            else:
                # Afficher l'en-tête si demandé
                if self.show_headers and columns:
                    if self.current_tx_id:
                        header = f"{Colors.tx_active('TX:' + str(self.current_tx_id))} | "
                    else:
                        header = ""
                    header += " | ".join(Colors.highlight(col) for col in columns)
                    self._write_output(header)
                    self._write_output(Colors.dim('─' * len(header)))
                
                # Afficher les données (limité à 100 lignes par défaut)
                max_rows = self.gsql.config.get('executor.row_limit', 100) if self.gsql else 100
                rows_to_display = rows[:max_rows]
                
                for i, row in enumerate(rows_to_display):
                    if isinstance(row, dict):
                        values = [safe_str(v) if v is not None else self.null_value for v in row.values()]
                    elif isinstance(row, (list, tuple)):
                        values = [safe_str(v) if v is not None else self.null_value for v in row]
                    else:
                        values = [safe_str(row)]
                    
                    # Colorer les valeurs
                    colored_values = []
                    for val in values:
                        if val == self.null_value:
                            colored_values.append(Colors.dim(val))
                        elif val.isdigit() or (val.replace('.', '', 1).isdigit() and val.count('.') <= 1):
                            colored_values.append(Colors.sql_number(val))
                        elif (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                            colored_values.append(Colors.sql_string(val))
                        else:
                            colored_values.append(val)
                    
                    self._write_output(" | ".join(colored_values))
                
                if len(rows) > max_rows:
                    self._write_output(Colors.dim(f"... and {len(rows) - max_rows} more rows"))
                
                self._write_output(Colors.dim(f"\n{count} row(s) returned"))
        
        elif query_type == 'insert':
            last_id = result.get('lastrowid', result.get('last_insert_id', 'N/A'))
            rows_affected = result.get('rows_affected', 0)
            
            self._write_output(Colors.success(f"✓ Row inserted"))
            if last_id and last_id != 'N/A':
                self._write_output(Colors.dim(f"ID: {last_id}"))
            self._write_output(Colors.dim(f"Rows affected: {rows_affected}"))
            
            # Afficher un warning si pas dans une transaction
            if hasattr(self.db, 'active_transactions') and not self.db.active_transactions and not self.auto_commit:
                self._write_output(Colors.warning("⚠ Warning: Insert not in transaction (auto-commit disabled)"))
        
        elif query_type == 'update' or query_type == 'delete':
            rows_affected = result.get('rows_affected', 0)
            self._write_output(Colors.success(f"✓ Query successful"))
            self._write_output(Colors.dim(f"Rows affected: {rows_affected}"))
            
            # Afficher un warning si pas dans une transaction
            if hasattr(self.db, 'active_transactions') and not self.db.active_transactions and not self.auto_commit:
                self._write_output(Colors.warning("⚠ Warning: Operation not in transaction (auto-commit disabled)"))
        
        elif query_type == 'show_tables':
            tables = result.get('tables', [])
            if tables:
                self._write_output(Colors.success(f"Found {len(tables)} table(s):"))
                for table in tables:
                    row_count = table.get('rows', 0)
                    size = table.get('size_kb', 0)
                    self._write_output(f"  • {Colors.highlight(table['table'])} "
                          f"({Colors.sql_number(str(row_count))} rows, "
                          f"{Colors.sql_number(f'{size}KB')})")
            else:
                self._write_output(Colors.warning("No tables found"))
        
        elif query_type == 'describe':
            columns = result.get('columns', [])
            if columns:
                self._write_output(Colors.success(f"Table structure:"))
                for col in columns:
                    null_str = "NOT NULL" if not col.get('null') else "NULL"
                    default_str = f"DEFAULT {col.get('default')}" if col.get('default') else ""
                    key_str = f" {col.get('key')}" if col.get('key') else ""
                    extra_str = f" {col.get('extra')}" if col.get('extra') else ""
                    
                    line = f"  {Colors.highlight(col['field'])} {col['type']} {null_str} {default_str}{key_str}{extra_str}"
                    self._write_output(line.strip())
            else:
                self._write_output(Colors.warning("No columns found"))
        
        elif query_type == 'stats':
            stats = result.get('database', {})
            self._write_output(Colors.success("Database statistics:"))
            
            # Statistiques de transaction
            if 'active_transactions' in stats:
                active_tx = stats['active_transactions']
                tx_color = Colors.RED if active_tx > 0 else Colors.GREEN
                self._write_output(f"  Active transactions: {tx_color}{active_tx}{Colors.RESET}")
            
            if 'transactions_total' in stats:
                self._write_output(f"  Total transactions: {stats['transactions_total']}")
            
            for key, value in stats.items():
                if key not in ['active_transactions', 'transactions_total']:
                    if isinstance(value, dict):
                        self._write_output(f"  {key}:")
                        for k, v in value.items():
                            self._write_output(f"    {k}: {v}")
                    else:
                        self._write_output(f"  {key}: {value}")
        
        elif query_type == 'vacuum':
            self._write_output(Colors.success("✓ Database optimized"))
        
        elif query_type == 'backup':
            self._write_output(Colors.success(f"✓ Backup created: {result.get('backup_file', 'N/A')}"))
        
        elif query_type == 'help':
            self._write_output(result.get('message', ''))
        
        else:
            self._write_output(Colors.success(f"✓ Query executed successfully"))
        
        # Afficher le temps d'exécution
        if self.show_timer:
            if 'execution_time' in result:
                time_str = f"{result['execution_time']:.3f}s"
            else:
                time_str = f"{execution_time:.3f}s"
            
            self._write_output(Colors.dim(f"Time: {time_str}"))
    
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
  CREATE VIEW name AS SELECT ...

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
  .headers [on|off]              - Toggle column headers
  .timer [on|off]                - Toggle execution timer
  .mode [mode]                   - Set output mode
  .nullvalue [string]            - Set string for NULL values
  .output [filename]             - Redirect output to file
  .read [filename]               - Execute SQL from file
  .dump                          - Dump database as SQL
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
        self._write_output(help_text.strip())
    
    def do_transactions(self, arg: str):
        """Affiche les transactions actives"""
        if not self.db:
            self._write_output(Colors.error("No database connection"))
            return
        
        active_tx = 0
        if hasattr(self.db, 'active_transactions'):
            active_tx = len(self.db.active_transactions)
        
        if active_tx == 0:
            self._write_output(Colors.info("No active transactions"))
            if self.auto_commit:
                self._write_output(Colors.dim("Auto-commit mode is enabled"))
            return
        
        self._write_output(Colors.success(f"Active transactions: {active_tx}"))
        self._write_output(Colors.dim("─" * 50))
        
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
                        duration_str = Colors.tx_warning(f"{duration:.1f}s ⚠")
            
            # Couleur selon l'état
            if state == 'ACTIVE':
                state_color = Colors.tx_active
            elif state == 'COMMITTED':
                state_color = Colors.tx_commit
            elif state == 'ROLLED_BACK':
                state_color = Colors.tx_rollback
            else:
                state_color = Colors.dim
            
            self._write_output(f"{Colors.highlight('TID:')} {Colors.BOLD}{tid}{Colors.RESET}")
            self._write_output(f"  {Colors.highlight('State:')} {state_color(state)}")
            self._write_output(f"  {Colors.highlight('Isolation:')} {isolation}")
            self._write_output(f"  {Colors.highlight('Duration:')} {duration_str}")
            
            # Afficher les savepoints
            savepoints = tx_info.get('savepoints', [])
            if savepoints:
                self._write_output(f"  {Colors.highlight('Savepoints:')} {', '.join(savepoints)}")
            
            # Afficher les requêtes exécutées
            queries = tx_info.get('queries', [])
            if queries:
                self._write_output(f"  {Colors.highlight('Queries:')} {len(queries)} executed")
                if len(queries) <= 3:
                    for i, query in enumerate(queries[-3:], 1):
                        short_query = query[:50] + "..." if len(query) > 50 else query
                        self._write_output(f"    {i}. {Colors.dim(short_query)}")
            
            self._write_output(Colors.dim("─" * 50))
        
        self._write_output(Colors.warning(f"⚠ Total active transactions: {active_tx}"))
        self._write_output(Colors.dim("Use COMMIT or ROLLBACK to finish transactions"))
    
    def do_exit(self, arg: str):
        """Quitte le shell GSQL"""
        # Vérifier les transactions actives
        if self.db and hasattr(self.db, 'active_transactions'):
            active_tx = len(self.db.active_transactions)
            if active_tx > 0:
                self._write_output(Colors.warning(f"⚠ Warning: {active_tx} active transaction(s)"))
                response = input("Rollback all transactions? (y/N): ").strip().lower()
                if response in ['y', 'yes']:
                    # Rollback toutes les transactions
                    for tid in list(self.db.active_transactions.keys()):
                        try:
                            self.db.execute("ROLLBACK")
                        except:
                            pass
                    self._write_output(Colors.tx_rollback("All transactions rolled back"))
        
        self._write_output(Colors.info("Goodbye!"))
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
        self._dot_autocommit(arg.split() if arg else [])
    
    def do_isolation(self, arg: str):
        """Définit le niveau d'isolation"""
        self._dot_isolation(arg.split() if arg else [])
    
    # ==================== SHELL CONTROL ====================
    
    def emptyline(self):
        """Ne rien faire sur ligne vide"""
        pass
    
    def precmd(self, line: str) -> str:
        """Avant l'exécution de la commande"""
        # Enregistrer dans l'historique (sauf les commandes spéciales)
        if line and not line.startswith('.'):
            try:
                readline.add_history(line)
            except Exception:
                pass
        return line
    
    def postcmd(self, stop: bool, line: str) -> bool:
        """Après l'exécution de la commande"""
        # Mettre à jour le prompt après chaque commande
        self._update_prompt()
        return stop
    
    def sigint_handler(self, signum, frame):
        """Gère Ctrl+C"""
        self._write_output("\n" + Colors.warning("Interrupted (Ctrl+C)"))
        
        # Si dans une transaction, proposer de rollback
        if self.current_tx_id:
            self._write_output(Colors.warning(f"⚠ Transaction {self.current_tx_id} is still active"))
            response = input("Rollback transaction? (y/N): ").strip().lower()
            if response in ['y', 'yes']:
                try:
                    self.db.execute("ROLLBACK")
                    self._write_output(Colors.tx_rollback(f"Transaction {self.current_tx_id} rolled back"))
                except:
                    self._write_output(Colors.error("Failed to rollback"))
        
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
        user_config = config.to_dict() if CONFIG_AVAILABLE else {}
        
        # Fusionner avec la configuration par défaut
        merged = DEFAULT_CONFIG.copy()
        
        # Mettre à jour avec la configuration utilisateur
        for section in ['database', 'executor', 'shell']:
            if section in user_config:
                if isinstance(user_config[section], dict):
                    merged[section].update(user_config[section])
        
        return merged
    
    def _initialize(self, database_path: Optional[str] = None):
        """Initialise les composants GSQL"""
        try:
            print(Colors.info("Initializing GSQL with Transaction Support..."))
            
            # Créer la base de données
            db_config = self.config['database'].copy()
            if database_path:
                db_config['path'] = database_path
            
            if not DATABASE_AVAILABLE:
                print(Colors.warning("Warning: Database module not available, using fallback"))
            
            self.db = create_database(**db_config)
            
            # Créer l'exécuteur si disponible
            if EXECUTOR_AVAILABLE:
                self.executor = create_executor(storage=self.db.storage)
            
            # Initialiser les autres composants
            if FUNCTIONS_AVAILABLE:
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
            if self.config.get('verbose_errors'):
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
            if self.config.get('verbose_errors'):
                traceback.print_exc()
            return None
        finally:
            self._cleanup()
    
    def run_script(self, script_path: str, database_path: Optional[str] = None):
        """Exécute un script SQL avec support transactions"""
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                script = f.read()
            
            self._initialize(database_path)
            
            print(Colors.info(f"Executing script: {script_path}"))
            print(Colors.dim("─" * 40))
            
            # Exécuter le script
            results = []
            for stmt in self._split_statements(script):
                if stmt.strip():
                    result = self.db.execute(stmt)
                    results.append(result)
                    
                    if not result.get('success'):
                        print(Colors.error(f"Failed: {stmt[:50]}..."))
            
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
            if self.config.get('verbose_errors'):
                traceback.print_exc()
            return None
        finally:
            self._cleanup()
    
    def _split_statements(self, script: str) -> List[str]:
        """Sépare un script SQL en instructions individuelles"""
        # Simple split sur les points-virgules (à améliorer pour gérer les strings)
        statements = []
        current = ""
        in_string = False
        string_char = None
        
        for char in script:
            if char in ("'", '"') and (not in_string or string_char == char):
                in_string = not in_string
                string_char = char if in_string else None
            elif char == ';' and not in_string:
                statements.append(current.strip())
                current = ""
                continue
            current += char
        
        if current.strip():
            statements.append(current.strip())
        
        return statements
    
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
                logger.debug("Database closed")
        except Exception as e:
            logger.debug(f"Error during cleanup: {e}")

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
        help='Verbose output and errors'
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
        '--row-limit',
        type=int,
        default=1000,
        help='Maximum rows to display (default: 1000)'
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
        DEFAULT_CONFIG['verbose_errors'] = True
    
    # Configurer le timeout des transactions
    if args.tx_timeout:
        DEFAULT_CONFIG['database']['transaction_timeout'] = args.tx_timeout
    
    # Configurer auto-commit
    if args.auto_commit:
        DEFAULT_CONFIG['executor']['auto_commit'] = True
    
    # Configurer la limite de lignes
    if args.row_limit:
        DEFAULT_CONFIG['executor']['row_limit'] = args.row_limit
    
    # Créer l'application
    app = GSQLApp()
    
    # Exécuter selon le mode
    if args.execute:
        # Mode exécution unique
        app.run_query(args.execute, args.database)
    elif args.file:
        # Mode fichier
        with open(args.file, 'r', encoding='utf-8') as f:
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