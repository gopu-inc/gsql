#!/usr/bin/env python3
"""
GSQL Main Entry Point - Interactive Shell and CLI
Version: 3.0 - SQLite Only
"""

import os
import sys
import cmd
import signal
import traceback
import readline
import atexit
import json
import logging
import re  # Ajout pour la colorisation SQL
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime

# ==================== IMPORTS ====================

# Import des modules GSQL
try:
    from . import __version__
    from .config import config, setup_logging  # Corrigé l'import
    from .database import create_database, Database, connect
    from .executor import create_executor, QueryExecutor
    from .functions import FunctionManager
    from .storage import SQLiteStorage
    
    # Pour NLP, on essaie d'importer mais on a un fallback
    try:
        from .nlp.translator import NLToSQLTranslator
        NLP_AVAILABLE = True
    except ImportError:
        NLP_AVAILABLE = False
        NLToSQLTranslator = None
    
    GSQL_AVAILABLE = True
except ImportError as e:
    print(f"Error importing GSQL modules: {e}")
    GSQL_AVAILABLE = False
    traceback.print_exc()
    sys.exit(1)

# ==================== LOGGING ====================

logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================

DEFAULT_CONFIG = {
    'database': {
        'base_dir': str(Path.home() / '.gsql'),
        'auto_recovery': True,
        'buffer_pool_size': 100,
        'enable_wal': True
    },
    'executor': {
        'enable_nlp': False,  # Désactivé par défaut
        'enable_learning': False
    },
    'shell': {
        'prompt': 'gsql> ',
        'history_file': '.gsql_history',
        'max_history': 1000,
        'colors': True,
        'autocomplete': True,
        'multiline': True,  # Support multi-lignes
        'syntax_highlighting': True  # Colorisation syntaxique
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
    def sql_function(text):
        """Fonctions SQL (bleu)"""
        return Colors.colorize(text, Colors.BLUE)
    
    @staticmethod
    def sql_operator(text):
        """Opérateurs SQL (cyan clair)"""
        return Colors.colorize(text, Colors.CYAN + Colors.BOLD)

# ==================== AUTO-COMPLETER ====================

class GSQLCompleter:
    """Auto-complétion pour le shell GSQL"""
    
    def __init__(self, database: Database = None):
        self.database = database
        self.keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
            'ALTER', 'ADD', 'COLUMN', 'PRIMARY', 'KEY', 'FOREIGN',
            'REFERENCES', 'UNIQUE', 'NOT', 'NULL', 'DEFAULT',
            'CHECK', 'INDEX', 'VIEW', 'TRIGGER', 'BEGIN', 'COMMIT',
            'ROLLBACK', 'SAVEPOINT', 'RELEASE', 'EXPLAIN', 'ANALYZE',
            'VACUUM', 'BACKUP', 'SHOW', 'DESCRIBE', 'HELP', 'EXIT',
            'QUIT', 'AND', 'OR', 'LIKE', 'IN', 'BETWEEN', 'IS',
            'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
            'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'AS',
            'UNION', 'INTERSECT', 'EXCEPT', 'DISTINCT', 'ALL',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        ]
        
        # Fonctions SQL
        self.functions = [
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'UPPER', 'LOWER',
            'LENGTH', 'SUBSTR', 'TRIM', 'ROUND', 'ABS', 'NOW', 'DATE',
            'TIME', 'YEAR', 'MONTH', 'DAY', 'COALESCE', 'NULLIF'
        ]
        
        # Commandes spéciales GSQL
        self.gsql_commands = [
            '.tables', '.schema', '.stats', '.help', '.backup',
            '.vacuum', '.exit', '.quit', '.clear', '.history',
            '.mode', '.timer', '.headers', '.nullvalue', '.width'
        ]
        
        self.table_names = []
        self.column_names = {}
        
        if database and database.storage:
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
        """Fonction de complétion pour readline"""
        if state == 0:
            # Préparer la liste des suggestions
            line = readline.get_line_buffer()
            tokens = line.strip().split()
            
            if not tokens or len(tokens) == 1:
                # Complétion de commande
                all_commands = self.keywords + self.functions + self.gsql_commands + self.table_names
                self.matches = [cmd for cmd in all_commands if cmd.lower().startswith(text.lower())]
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
            elif tokens[-1].upper() in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']:
                # Complétion après une fonction d'agrégation
                table = self._find_current_table(tokens)
                if table and table in self.column_names:
                    self.matches = [col for col in self.column_names[table] if col.lower().startswith(text.lower())]
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
    """Shell interactif GSQL avec fonctionnalités avancées"""
    
    intro = Colors.info("GSQL Interactive Shell") + "\n" + Colors.dim("Type 'help' for commands, 'exit' to quit")
    prompt = Colors.info('gsql> ')
    ruler = Colors.dim('─')
    
    def __init__(self, gsql_app=None):
        super().__init__()
        self.gsql = gsql_app
        self.db = gsql_app.db if gsql_app else None
        self.executor = gsql_app.executor if gsql_app else None
        self.completer = gsql_app.completer if gsql_app else None
        
        # Configuration
        self.config = gsql_app.config if gsql_app else {}
        self.multiline_mode = False
        self.multiline_buffer = []
        
        # Options d'affichage
        self.show_headers = True
        self.show_timer = True
        self.null_value = "NULL"
        self.column_widths = {}
        
        # Configuration du prompt
        if self.config.get('shell', {}).get('colors', True):
            self.prompt = Colors.info('gsql> ')
        else:
            self.prompt = 'gsql> '
        
        # Configuration de l'historique
        self.history_file = Path(self.config.get('database', {}).get('base_dir', Path.home() / '.gsql')) / \
                           self.config.get('shell', {}).get('history_file', '.gsql_history')
        self._setup_history()
        
        # Configuration de l'auto-complétion
        if self.config.get('shell', {}).get('autocomplete', True) and self.completer:
            readline.set_completer(self.completer.complete)
            readline.parse_and_bind("tab: complete")
            readline.set_completer_delims(' \t\n`~!@#$%^&*()-=+[{]}\\|;:\'",<>/?')
        
        # Support multi-lignes
        if self.config.get('shell', {}).get('multiline', True):
            readline.parse_and_bind("set bind-tty-special-chars off")
    
    def _setup_history(self):
        """Configure l'historique de commandes"""
        try:
            self.history_file.parent.mkdir(parents=True, exist_ok=True)
            readline.read_history_file(str(self.history_file))
        except FileNotFoundError:
            pass
        
        # Limiter la taille de l'historique
        readline.set_history_length(self.config.get('shell', {}).get('max_history', 1000))
        
        # Enregistrer l'historique à la sortie
        atexit.register(readline.write_history_file, str(self.history_file))
    
    # ==================== MULTI-LINE SUPPORT ====================
    
    def preloop(self):
        """Avant le démarrage de la boucle"""
        print(Colors.dim("Tip: End SQL commands with ';' or use '\\' for multi-line"))
    
    def parseline(self, line):
        """Gère le support multi-lignes"""
        line = line.rstrip()
        
        # Vérifier si on est en mode multi-ligne
        if self.multiline_mode:
            if line.lower() == 'end' or line == ';':
                # Fin du mode multi-ligne
                full_sql = '\n'.join(self.multiline_buffer)
                self.multiline_buffer = []
                self.multiline_mode = False
                self.prompt = Colors.info('gsql> ')
                return None, None, full_sql
            else:
                # Ajouter à la ligne en cours
                self.multiline_buffer.append(line)
                self.prompt = Colors.dim('    -> ')
                return None, None, ''
        
        # Vérifier si la ligne commence un mode multi-lignes
        if line.endswith('\\'):
            self.multiline_mode = True
            self.multiline_buffer.append(line[:-1].strip())
            self.prompt = Colors.dim('    -> ')
            return None, None, ''
        
        return super().parseline(line)
    
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
        elif cmd == 'exit' or cmd == 'quit':
            return True
        elif cmd == 'clear':
            os.system('clear' if os.name == 'posix' else 'cls')
        elif cmd == 'history':
            self._show_history()
        elif cmd == 'mode':
            if len(parts) > 1:
                mode = parts[1].lower()
                if mode == 'list':
                    print(Colors.info("Available modes:"))
                    print("  list    - Show tables")
                    print("  insert  - Insert mode")
                    print("  line    - One value per line")
                    print("  column  - Left-aligned columns")
                    print("  html    - HTML output")
                    print("  csv     - CSV output")
                    print("  json    - JSON output")
                else:
                    print(Colors.warning(f"Mode '{mode}' not yet implemented"))
            else:
                print(Colors.info("Current mode: default"))
        elif cmd == 'timer':
            self.show_timer = not self.show_timer
            status = "ON" if self.show_timer else "OFF"
            print(Colors.info(f"Timer: {status}"))
        elif cmd == 'headers':
            self.show_headers = not self.show_headers
            status = "ON" if self.show_headers else "OFF"
            print(Colors.info(f"Headers: {status}"))
        elif cmd == 'nullvalue':
            value = parts[1] if len(parts) > 1 else "NULL"
            self.null_value = value
            print(Colors.info(f"Null value set to: {value}"))
        elif cmd == 'width':
            if len(parts) > 1:
                try:
                    width = int(parts[1])
                    for col in self.column_widths:
                        self.column_widths[col] = width
                    print(Colors.info(f"Default column width: {width}"))
                except ValueError:
                    print(Colors.error("Width must be a number"))
            else:
                print(Colors.info("Current column widths:"))
                for col, width in self.column_widths.items():
                    print(f"  {col}: {width}")
        else:
            print(Colors.error(f"Unknown command: .{cmd}"))
            print(Colors.dim("Try .help for available commands"))
    
    def _show_history(self):
        """Affiche l'historique des commandes"""
        try:
            histsize = readline.get_current_history_length()
            for i in range(max(1, histsize - 20), histsize + 1):
                cmd = readline.get_history_item(i)
                if cmd:
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
            
            # Afficher la requête colorée si configuré
            if self.config.get('shell', {}).get('syntax_highlighting', True):
                print(Colors.dim(f"Executing: {self._colorize_sql(sql)}"))
            
            # Exécuter la requête
            start_time = datetime.now()
            result = self.db.execute(sql)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Afficher le résultat
            self._display_result(result, execution_time)
            
        except Exception as e:
            print(Colors.error(f"Error: {e}"))
            if self.config.get('verbose_errors', True):
                traceback.print_exc()
    
    def _display_result(self, result: Dict, execution_time: float):
        """Affiche le résultat d'une requête"""
        if not result.get('success'):
            print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
            return
        
        query_type = result.get('type', '').lower()
        
        if query_type == 'select':
            rows = result.get('rows', [])
            columns = result.get('columns', [])
            count = result.get('count', 0)
            
            if count == 0:
                print(Colors.warning("No rows returned"))
            else:
                # Afficher l'en-tête si configuré
                if self.show_headers:
                    # Calculer les largeurs de colonnes
                    col_widths = []
                    for i, col in enumerate(columns):
                        max_len = len(str(col))
                        for row in rows:
                            val = str(row[i] if isinstance(row, (list, tuple)) else row)
                            max_len = max(max_len, len(val))
                        col_widths.append(min(max_len + 2, 50))  # Limite à 50
                    
                    # Afficher l'en-tête
                    header_parts = []
                    for i, col in enumerate(columns):
                        header_parts.append(Colors.highlight(f"{col:<{col_widths[i]}}"))
                    print("".join(header_parts))
                    print(Colors.dim('─' * sum(col_widths)))
                
                # Afficher les données (limité à 100 lignes)
                max_rows = 100
                for i, row in enumerate(rows[:max_rows]):
                    if isinstance(row, (list, tuple)):
                        values = [str(v) if v is not None else self.null_value for v in row]
                    else:
                        values = [str(row) if row is not None else self.null_value]
                    
                    # Formater la ligne
                    line_parts = []
                    for j, val in enumerate(values):
                        if val == self.null_value:
                            colored_val = Colors.dim(val)
                        elif val.isdigit() or (val.replace('.', '', 1).isdigit() and val.count('.') <= 1):
                            colored_val = Colors.sql_number(val)
                        elif val.startswith("'") and val.endswith("'"):
                            colored_val = Colors.sql_string(val)
                        else:
                            colored_val = val
                        
                        if 'col_widths' in locals():
                            line_parts.append(f"{colored_val:<{col_widths[j]}}")
                        else:
                            line_parts.append(colored_val)
                    
                    print("".join(line_parts))
                
                if len(rows) > max_rows:
                    print(Colors.dim(f"... and {len(rows) - max_rows} more rows"))
                
                print(Colors.dim(f"\n{count} row(s) returned"))
        
        elif query_type == 'insert':
            print(Colors.success(f"Row inserted (ID: {result.get('last_insert_id', 'N/A')})"))
            print(Colors.dim(f"Rows affected: {result.get('rows_affected', 0)}"))
        
        elif query_type == 'update' or query_type == 'delete':
            print(Colors.success(f"Query successful"))
            print(Colors.dim(f"Rows affected: {result.get('rows_affected', 0)}"))
        
        elif query_type == 'show_tables':
            tables = result.get('tables', [])
            if tables:
                print(Colors.success(f"Found {len(tables)} table(s):"))
                for table in tables:
                    print(f"  • {Colors.highlight(table['table'])} ({table.get('rows', 0)} rows)")
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
            for key, value in stats.items():
                if isinstance(value, dict):
                    print(f"  {key}:")
                    for k, v in value.items():
                        print(f"    {k}: {v}")
                else:
                    print(f"  {key}: {value}")
        
        elif query_type == 'vacuum':
            print(Colors.success("Database optimized"))
        
        elif query_type == 'backup':
            print(Colors.success(f"Backup created: {result.get('backup_file', 'N/A')}"))
        
        elif query_type == 'help':
            print(result.get('message', ''))
        
        else:
            print(Colors.success(f"Query executed successfully"))
        
        # Afficher le temps d'exécution si configuré
        if self.show_timer:
            if 'execution_time' in result:
                time_str = f"{result['execution_time']:.3f}s"
            else:
                time_str = f"{execution_time:.3f}s"
            
            print(Colors.dim(f"Time: {time_str}"))
    
    # ==================== BUILT-IN COMMANDS ====================
    
    def do_help(self, arg: str):
        """Affiche l'aide"""
        help_text = """
GSQL Commands:

SQL COMMANDS:
  SELECT * FROM table [WHERE condition] [LIMIT n]
  INSERT INTO table (col1, col2) VALUES (val1, val2)
  UPDATE table SET col=value [WHERE condition]
  DELETE FROM table [WHERE condition]
  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
  DROP TABLE name
  ALTER TABLE name ADD COLUMN col TYPE
  CREATE INDEX idx_name ON table(column)
  BEGIN TRANSACTION; ...; COMMIT/ROLLBACK

GSQL SPECIAL COMMANDS:
  SHOW TABLES                    - List all tables
  DESCRIBE table                 - Show table structure
  STATS                          - Show database statistics
  VACUUM                         - Optimize database
  BACKUP [path]                  - Create database backup
  HELP                           - This help message

DOT COMMANDS (SQLite style):
  .tables                        - List tables
  .schema [table]                - Show schema
  .stats                         - Show stats
  .help                          - Show help
  .backup [file]                 - Create backup
  .vacuum                        - Optimize database
  .exit / .quit                  - Exit shell
  .clear                         - Clear screen
  .history                       - Show command history
  .mode [mode]                   - Set output mode
  .timer [on|off]                - Toggle execution timer
  .headers [on|off]              - Toggle column headers
  .nullvalue STRING              - Set NULL display string
  .width NUM1 NUM2 ...           - Set column widths

SHELL FEATURES:
  Multi-line SQL: End line with \
  Syntax highlighting
  Auto-completion (Tab key)
  Command history (Up/Down arrows)
  Ctrl+C to cancel command
  Ctrl+D to exit

SHELL COMMANDS:
  exit, quit, Ctrl+D             - Exit GSQL
  Ctrl+C                         - Cancel current command
  help                           - Show this help
        """
        print(help_text.strip())
    
    def do_exit(self, arg: str):
        """Quitte le shell GSQL"""
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
    
    # ==================== SHELL CONTROL ====================
    
    def emptyline(self):
        """Ne rien faire sur ligne vide"""
        pass
    
    def precmd(self, line: str) -> str:
        """Avant l'exécution de la commande"""
        # Enregistrer dans l'historique (sauf les commandes spéciales et multi-lignes)
        if line and not line.startswith('.') and not self.multiline_mode:
            readline.add_history(line)
        return line
    
    def postcmd(self, stop: bool, line: str) -> bool:
        """Après l'exécution de la commande"""
        return stop
    
    def sigint_handler(self, signum, frame):
        """Gère Ctrl+C"""
        print("\n" + Colors.warning("Interrupted (Ctrl+C)"))
        self.prompt = Colors.info('gsql> ')
        self.multiline_mode = False
        self.multiline_buffer = []
    
    # ==================== SQL SYNTAX HIGHLIGHTING ====================
    
    def _colorize_sql(self, sql: str) -> str:
        """Colorise la syntaxe SQL"""
        if not self.config.get('shell', {}).get('syntax_highlighting', True):
            return sql
        
        # Mots-clés SQL
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
            'ALTER', 'ADD', 'COLUMN', 'PRIMARY', 'KEY', 'FOREIGN',
            'REFERENCES', 'UNIQUE', 'NOT', 'NULL', 'DEFAULT',
            'CHECK', 'INDEX', 'VIEW', 'TRIGGER', 'BEGIN', 'COMMIT',
            'ROLLBACK', 'SAVEPOINT', 'RELEASE', 'EXPLAIN', 'ANALYZE',
            'VACUUM', 'BACKUP', 'SHOW', 'DESCRIBE', 'HELP',
            'AND', 'OR', 'LIKE', 'IN', 'BETWEEN', 'IS',
            'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
            'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'AS',
            'UNION', 'INTERSECT', 'EXCEPT', 'DISTINCT', 'ALL',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        ]
        
        # Fonctions SQL
        functions = [
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'UPPER', 'LOWER',
            'LENGTH', 'SUBSTR', 'SUBSTRING', 'TRIM', 'LTRIM', 'RTRIM',
            'ROUND', 'ABS', 'CEIL', 'CEILING', 'FLOOR', 'RANDOM',
            'NOW', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
            'DATE', 'TIME', 'DATETIME', 'JULIANDAY', 'STRFTIME',
            'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
            'COALESCE', 'NULLIF', 'IFNULL', 'IIF', 'CASE'
        ]
        
        # Opérateurs
        operators = ['=', '<>', '!=', '<', '>', '<=', '>=', '+', '-', '*', '/', '%', '||']
        
        # Coloriser les mots-clés
        for keyword in keywords:
            pattern = rf'\b{keyword}\b'
            sql = re.sub(pattern, Colors.sql_keyword(keyword), sql, flags=re.IGNORECASE)
        
        # Coloriser les fonctions
        for func in functions:
            pattern = rf'\b{func}\b(?=\()'
            sql = re.sub(pattern, Colors.sql_function(func), sql, flags=re.IGNORECASE)
        
        # Coloriser les opérateurs
        for op in operators:
            sql = sql.replace(op, Colors.sql_operator(op))
        
        # Coloriser les chaînes
        sql = re.sub(r"'[^']*'", lambda m: Colors.sql_string(m.group(0)), sql)
        sql = re.sub(r'"[^"]*"', lambda m: Colors.sql_string(m.group(0)), sql)
        
        # Coloriser les nombres
        sql = re.sub(r'\b\d+\b', lambda m: Colors.sql_number(m.group(0)), sql)
        sql = re.sub(r'\b\d+\.\d+\b', lambda m: Colors.sql_number(m.group(0)), sql)
        
        # Coloriser les commentaires
        sql = re.sub(r'--.*$', lambda m: Colors.sql_comment(m.group(0)), sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', lambda m: Colors.sql_comment(m.group(0)), sql, flags=re.DOTALL)
        
        return sql

# ==================== MAIN GSQL APPLICATION ====================

class GSQLApp:
    """Application GSQL principale"""
    
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
        
        logger.info(f"GSQL v{__version__} initialized (SQLite only)")
    
    def _load_config(self) -> Dict:
        """Charge la configuration"""
        # Pour l'instant, utiliser la config par défaut
        # Vous pouvez ajouter la lecture depuis un fichier de config
        return DEFAULT_CONFIG.copy()
    
    def _initialize(self, database_path: Optional[str] = None):
        """Initialise les composants GSQL"""
        try:
            print(Colors.info("Initializing GSQL..."))
            
            # Créer la base de données
            db_config = self.config['database'].copy()
            if database_path:
                db_config['path'] = database_path
            
            self.db = create_database(**db_config)
            
            # Créer l'exécuteur
            self.executor = create_executor(storage=self.db.storage)
            
            # Initialiser les autres composants
            self.function_manager = FunctionManager()
            
            # Gestion du NLP - avec fallback si non disponible
            if NLP_AVAILABLE and NLToSQLTranslator:
                self.nlp_translator = NLToSQLTranslator()
            else:
                self.nlp_translator = None
            
            # Configurer l'auto-complétion
            self.completer = GSQLCompleter(self.db)
            
            print(Colors.success("✓ GSQL ready!"))
            print(Colors.dim(f"Database: {self.db.storage.db_path}"))
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
    
    def _cleanup(self):
        """Nettoie les ressources"""
        try:
            if self.db:
                self.db.close()
                print(Colors.dim("Database closed"))
        except:
            pass

# ==================== MAIN FUNCTION ====================

def main():
    """Fonction principale"""
    if not GSQL_AVAILABLE:
        print(Colors.error("GSQL modules not available. Check installation."))
        sys.exit(1)
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description=f"GSQL v{__version__} - SQL Database with Natural Language Interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  gsql                         # Start interactive shell
  gsql mydb.db                 # Open specific database
  gsql -e "SHOW TABLES"        # Execute single query
  gsql -f queries.sql          # Execute queries from file
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
        '--version',
        action='version',
        version=f'GSQL {__version__}'
    )
    
    args = parser.parse_args()
    
    # Créer l'application
    app = GSQLApp()
    
    # Configurer les couleurs
    if args.no_color:
        app.config['shell']['colors'] = False
        app.config['shell']['syntax_highlighting'] = False
    
    # Configurer le verbose
    if args.verbose:
        app.config['log_level'] = 'DEBUG'
        app.config['verbose_errors'] = True
    
    # Exécuter selon le mode
    if args.execute:
        # Mode exécution unique
        print(Colors.error("Single query execution not fully implemented yet"))
        print(Colors.dim("Use interactive shell instead"))
        # Vous pouvez implémenter cette partie
        # app.run_query(args.execute, args.database)
    elif args.file:
        # Mode fichier
        try:
            with open(args.file, 'r') as f:
                queries = f.read()
            print(Colors.error("File execution not fully implemented yet"))
            print(Colors.dim("Use interactive shell instead"))
            # Vous pouvez implémenter cette partie
            # app.run_query(queries, args.database)
        except Exception as e:
            print(Colors.error(f"Error reading file: {e}"))
            sys.exit(1)
    else:
        # Mode shell interactif
        app.run_shell(args.database)

# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    main()
