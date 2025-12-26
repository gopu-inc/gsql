#!/usr/bin/env python3
"""
GSQL Main Entry Point - Interactive Shell and CLI
Version: 3.10.0 - Rich Shell with System Integration
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
import re
import platform
import socket
import getpass
import time
import subprocess
import textwrap
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

# ==================== IMPORTS ====================

# Import des modules GSQL
try:
    # Version
    __version__ = "4.0.0-beta"
    
    # Configuration simplifiée
    class Config:
        def __init__(self):
            self.data = {
                'base_dir': str(Path.home() / '.gsql'),
                'colors': True,
                'verbose_errors': False,
                'log_level': 'INFO',
                'shell': {
                    'prompt_style': 'advanced',  # simple, advanced, professional
                    'show_system_info': True,
                    'show_time': True,
                    'show_path': True,
                    'max_history': 5000,
                    'auto_complete': True,
                    'syntax_highlight': True,
                    'multiline_mode': True,
                    'pager': 'less -R' if shutil.which('less') else None,
                }
            }
        
        def get(self, key, default=None):
            keys = key.split('.')
            data = self.data
            for k in keys:
                if isinstance(data, dict) and k in data:
                    data = data[k]
                else:
                    return default
            return data
        
        def set(self, key, value):
            keys = key.split('.')
            data = self.data
            for k in keys[:-1]:
                if k not in data:
                    data[k] = {}
                data = data[k]
            data[keys[-1]] = value
        
        def to_dict(self):
            return self.data.copy()
    
    config = Config()
    
    def setup_logging(level='INFO', log_file=None):
        """Configure le logging"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        handlers = []
        
        # Handler console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, level))
        console_formatter = logging.Formatter(log_format)
        console_handler.setFormatter(console_formatter)
        handlers.append(console_handler)
        
        # Handler fichier si spécifié
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(getattr(logging, level))
            file_formatter = logging.Formatter(log_format)
            file_handler.setFormatter(file_formatter)
            handlers.append(file_handler)
        
        # Configuration du logger racine
        logging.basicConfig(
            level=getattr(logging, level),
            format=log_format,
            handlers=handlers
        )
        
        # Désactiver les logs verbeux
        logging.getLogger('sqlite3').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    # Import des autres modules
    from .database import create_database, Database
    from .executor import create_executor
    from .functions import FunctionManager
    from .storage import SQLiteStorage
    
    # Pour NLP
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

# ==================== ENUMS & DATA CLASSES ====================

class ShellTheme(Enum):
    """Thèmes pour le shell"""
    DEFAULT = "default"
    DARK = "dark"
    LIGHT = "light"
    RETRO = "retro"
    MODERN = "modern"

class OutputFormat(Enum):
    """Formats de sortie"""
    TABLE = "table"
    CSV = "csv"
    JSON = "json"
    YAML = "yaml"
    XML = "xml"
    MARKDOWN = "markdown"

@dataclass
class SystemInfo:
    """Informations système"""
    platform: str = ""
    hostname: str = ""
    username: str = ""
    python_version: str = ""
    cpu_count: int = 0
    memory_total: int = 0
    memory_available: int = 0
    
    @classmethod
    def collect(cls):
        """Collecte les informations système"""
        import psutil
        return cls(
            platform=f"{platform.system()} {platform.release()} ({platform.machine()})",
            hostname=socket.gethostname(),
            username=getpass.getuser(),
            python_version=platform.python_version(),
            cpu_count=psutil.cpu_count(),
            memory_total=psutil.virtual_memory().total // (1024**3),  # GB
            memory_available=psutil.virtual_memory().available // (1024**3)  # GB
        )

# ==================== COLOR SUPPORT ====================

class Theme:
    """Gestionnaire de thèmes"""
    
    THEMES = {
        ShellTheme.DEFAULT: {
            'prompt': '\033[1;32m',  # Vert clair
            'text': '\033[0;37m',    # Blanc
            'error': '\033[1;31m',   # Rouge
            'warning': '\033[1;33m', # Jaune
            'info': '\033[1;36m',    # Cyan
            'success': '\033[1;32m', # Vert
            'dim': '\033[0;90m',     # Gris
            'highlight': '\033[1;35m', # Magenta
            'sql_keyword': '\033[1;34m', # Bleu
            'sql_string': '\033[0;33m',  # Jaune
            'sql_number': '\033[0;36m',  # Cyan
            'sql_comment': '\033[0;32m', # Vert
        },
        ShellTheme.DARK: {
            'prompt': '\033[1;35m',
            'text': '\033[0;37m',
            'error': '\033[1;31m',
            'warning': '\033[1;33m',
            'info': '\033[1;36m',
            'success': '\033[1;32m',
            'dim': '\033[0;90m',
            'highlight': '\033[1;95m',
            'sql_keyword': '\033[1;94m',
            'sql_string': '\033[0;93m',
            'sql_number': '\033[0;96m',
            'sql_comment': '\033[0;92m',
        }
    }
    
    def __init__(self, theme=ShellTheme.DEFAULT):
        self.theme = theme
        self.colors = self.THEMES.get(theme, self.THEMES[ShellTheme.DEFAULT])
    
    def colorize(self, text, color_key):
        """Colorise le texte avec une clé de thème"""
        if not config.get('colors', True):
            return text
        color = self.colors.get(color_key, '')
        return f"{color}{text}\033[0m"
    
    def prompt(self, text):
        return self.colorize(text, 'prompt')
    
    def error(self, text):
        return self.colorize(text, 'error')
    
    def warning(self, text):
        return self.colorize(text, 'warning')
    
    def info(self, text):
        return self.colorize(text, 'info')
    
    def success(self, text):
        return self.colorize(text, 'success')
    
    def dim(self, text):
        return self.colorize(text, 'dim')
    
    def highlight(self, text):
        return self.colorize(text, 'highlight')
    
    def sql_keyword(self, text):
        return self.colorize(text, 'sql_keyword')
    
    def sql_string(self, text):
        return self.colorize(text, 'sql_string')
    
    def sql_number(self, text):
        return self.colorize(text, 'sql_number')
    
    def sql_comment(self, text):
        return self.colorize(text, 'sql_comment')

# Initialiser le thème
theme = Theme()

# ==================== SHELL UTILITIES ====================

class ShellUtils:
    """Utilitaires pour le shell"""
    
    @staticmethod
    def clear_screen():
        """Efface l'écran"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    @staticmethod
    def get_terminal_size():
        """Récupère la taille du terminal"""
        try:
            size = shutil.get_terminal_size()
            return size.columns, size.lines
        except:
            return 80, 24
    
    @staticmethod
    def format_size(size_bytes):
        """Formate une taille en bytes"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    
    @staticmethod
    def format_time(seconds):
        """Formate un temps en secondes"""
        if seconds < 1:
            return f"{seconds*1000:.0f}ms"
        elif seconds < 60:
            return f"{seconds:.2f}s"
        else:
            minutes = int(seconds // 60)
            secs = seconds % 60
            return f"{minutes}m {secs:.0f}s"
    
    @staticmethod
    def progress_bar(iteration, total, length=50):
        """Affiche une barre de progression"""
        percent = int(100 * (iteration / float(total)))
        filled_length = int(length * iteration // total)
        bar = '█' * filled_length + '░' * (length - filled_length)
        return f"|{bar}| {percent}%"
    
    @staticmethod
    def table(data, headers=None, max_width=None):
        """Affiche des données en table"""
        if not data:
            return ""
        
        # Déterminer les largeurs de colonnes
        if headers:
            all_data = [headers] + data
        else:
            all_data = data
        
        col_widths = []
        for i in range(len(all_data[0])):
            col_width = max(len(str(row[i])) for row in all_data)
            if max_width and col_width > max_width:
                col_width = max_width
            col_widths.append(col_width)
        
        # Construire la table
        lines = []
        
        # En-tête
        if headers:
            header_line = "│ " + " │ ".join(
                f"{str(headers[i]):<{col_widths[i]}}" for i in range(len(headers))
            ) + " │"
            separator = "├" + "┼".join("─" * (w + 2) for w in col_widths) + "┤"
            lines.append(header_line)
            lines.append(separator)
        
        # Données
        for row in data:
            line_parts = []
            for i, cell in enumerate(row):
                cell_str = str(cell)
                if max_width and len(cell_str) > max_width:
                    cell_str = cell_str[:max_width-3] + "..."
                line_parts.append(f"{cell_str:<{col_widths[i]}}")
            lines.append("│ " + " │ ".join(line_parts) + " │")
        
        return "\n".join(lines)

# ==================== AUTO-COMPLETER ====================

class GSQLCompleter:
    """Auto-complétion avancée pour GSQL"""
    
    def __init__(self, database: Database = None):
        self.database = database
        
        # Commandes SQL complètes
        self.sql_keywords = [
            # DDL
            'CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'RENAME',
            # DML
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE',
            # Clauses
            'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY',
            'LIMIT', 'OFFSET', 'DISTINCT', 'ALL', 'AS',
            # Jointures
            'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN',
            'FULL JOIN', 'CROSS JOIN', 'ON', 'USING',
            # Contraintes
            'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'NOT NULL',
            'CHECK', 'DEFAULT', 'AUTOINCREMENT',
            # Transactions
            'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
            'RELEASE SAVEPOINT', 'SET TRANSACTION',
            # Autres
            'EXPLAIN', 'ANALYZE', 'VACUUM', 'PRAGMA',
            'WITH', 'RECURSIVE', 'UNION', 'INTERSECT', 'EXCEPT',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'EXISTS', 'IN', 'BETWEEN', 'LIKE', 'ILIKE',
            'IS NULL', 'IS NOT NULL', 'CAST'
        ]
        
        # Fonctions SQL
        self.sql_functions = [
            # Agrégation
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT',
            # Chaînes
            'LENGTH', 'UPPER', 'LOWER', 'SUBSTR', 'TRIM',
            'LTRIM', 'RTRIM', 'REPLACE', 'INSTR', 'HEX',
            'UNHEX', 'FORMAT', 'CONCAT', 'CONCAT_WS',
            # Mathématiques
            'ABS', 'ROUND', 'CEIL', 'FLOOR', 'RANDOM',
            'POWER', 'SQRT', 'EXP', 'LOG', 'LOG10',
            'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN',
            # Dates
            'DATE', 'TIME', 'DATETIME', 'JULIANDAY',
            'STRFTIME', 'CURRENT_DATE', 'CURRENT_TIME',
            'CURRENT_TIMESTAMP', 'YEAR', 'MONTH', 'DAY',
            'HOUR', 'MINUTE', 'SECOND',
            # Divers
            'COALESCE', 'NULLIF', 'IFNULL', 'IIF',
            'TYPEOF', 'QUOTE', 'GLOB', 'MATCH'
        ]
        
        # Commandes GSQL avancées
        self.gsql_commands = [
            # Dot commands
            '.tables', '.schema', '.indexes', '.stats', '.info',
            '.backup', '.restore', '.vacuum', '.optimize',
            '.import', '.export', '.dump', '.load',
            '.mode', '.headers', '.nullvalue', '.width',
            '.timer', '.echo', '.bail', '.changes',
            '.exit', '.quit', '.help', '.clear', '.history',
            '.system', '.read', '.shell', '.open', '.close',
            # System commands
            '.sysinfo', '.ps', '.df', '.du', '.top',
            '.network', '.disk', '.memory', '.cpu',
            # Admin commands
            '.users', '.permissions', '.logs', '.config',
            '.backup', '.restore', '.migrate', '.upgrade',
        ]
        
        # Shell builtins
        self.shell_commands = [
            'cd', 'pwd', 'ls', 'cat', 'echo', 'grep',
            'find', 'wc', 'head', 'tail', 'sort', 'uniq',
            'cp', 'mv', 'rm', 'mkdir', 'rmdir', 'chmod',
            'ps', 'kill', 'df', 'du', 'free', 'top',
        ]
        
        self.table_names = []
        self.column_names = {}
        self.view_names = []
        self.index_names = []
        
        if database:
            self._refresh_schema()
    
    def _refresh_schema(self):
        """Rafraîchit le schéma depuis la base"""
        try:
            if self.database:
                # Tables
                result = self.database.execute("SHOW TABLES")
                if result.get('success'):
                    self.table_names = [table['table'] for table in result.get('tables', [])]
                
                # Vues
                try:
                    result = self.database.execute("SELECT name FROM sqlite_master WHERE type='view'")
                    if result.get('success'):
                        self.view_names = [row[0] for row in result.get('rows', [])]
                except:
                    pass
                
                # Indexes
                try:
                    result = self.database.execute("SELECT name FROM sqlite_master WHERE type='index'")
                    if result.get('success'):
                        self.index_names = [row[0] for row in result.get('rows', [])]
                except:
                    pass
                
                # Colonnes
                self.column_names = {}
                for table in self.table_names:
                    try:
                        result = self.database.execute(f"PRAGMA table_info({table})")
                        if result.get('success'):
                            self.column_names[table] = [
                                col[1] for col in result.get('rows', [])
                            ]
                    except:
                        pass
                        
        except Exception as e:
            logger.debug(f"Error refreshing schema: {e}")
    
    def complete(self, text: str, state: int) -> Optional[str]:
        """Fonction de complétion pour readline"""
        if state == 0:
            line = readline.get_line_buffer()
            line_before = readline.get_line_buffer()[:readline.get_endidx()]
            
            # Détecter le contexte
            if line.startswith('.'):
                # Commande GSQL
                self.matches = [cmd for cmd in self.gsql_commands 
                               if cmd.startswith(text)]
            elif line.startswith('!'):
                # Commande shell
                self.matches = [f"!{cmd}" for cmd in self.shell_commands 
                               if cmd.startswith(text[1:] if text.startswith('!') else text)]
            elif ';' in line_before:
                # Nouvelle commande après un point-virgule
                self.matches = self._get_sql_completions(text, "")
            else:
                # SQL standard
                self.matches = self._get_sql_completions(text, line_before)
        
        try:
            return self.matches[state] if state < len(self.matches) else None
        except IndexError:
            return None
    
    def _get_sql_completions(self, text: str, context: str) -> List[str]:
        """Retourne les complétions SQL en fonction du contexte"""
        # Toutes les possibilités
        all_items = (self.sql_keywords + self.sql_functions + 
                    self.table_names + self.view_names + self.index_names)
        
        # Filtrer par texte
        matches = [item for item in all_items 
                  if item.lower().startswith(text.lower())]
        
        # Contexte spécifique
        if context:
            tokens = context.upper().split()
            last_token = tokens[-1] if tokens else ""
            
            if last_token == 'FROM' or last_token == 'INTO' or last_token == 'JOIN':
                matches = [item for item in self.table_names + self.view_names 
                          if item.lower().startswith(text.lower())]
            elif last_token == 'WHERE' or last_token == 'SET' or last_token == 'BY':
                # Trouver la table courante
                table = self._find_current_table(tokens)
                if table and table in self.column_names:
                    matches = [col for col in self.column_names[table] 
                              if col.lower().startswith(text.lower())]
        
        return matches
    
    def _find_current_table(self, tokens: List[str]) -> Optional[str]:
        """Trouve la table courante dans les tokens"""
        for i, token in enumerate(tokens):
            if token == 'FROM' and i + 1 < len(tokens):
                return tokens[i + 1]
            elif token == 'JOIN' and i + 1 < len(tokens):
                return tokens[i + 1]
            elif token == 'UPDATE' and i + 1 < len(tokens):
                return tokens[i + 1]
            elif token == 'INTO' and i + 1 < len(tokens):
                return tokens[i + 1]
        return None

# ==================== RICH PROMPT ====================

class RichPrompt:
    """Générateur de prompt riche"""
    
    def __init__(self, config, db=None):
        self.config = config
        self.db = db
        self.start_time = time.time()
        self.command_count = 0
        
        # Icônes pour différents systèmes
        self.icons = {
            'linux': '🐧',
            'darwin': '🍎', 
            'windows': '🪟',
            'default': '⚡'
        }
        
        # Couleurs pour différents états
        self.state_colors = {
            'normal': '\033[1;32m',  # Vert
            'error': '\033[1;31m',   # Rouge
            'warning': '\033[1;33m', # Jaune
            'multiline': '\033[1;36m' # Cyan
        }
    
    def get_system_icon(self):
        """Retourne l'icône du système"""
        system = platform.system().lower()
        return self.icons.get(system, self.icons['default'])
    
    def get_uptime(self):
        """Retourne le temps depuis le démarrage"""
        uptime = time.time() - self.start_time
        if uptime < 60:
            return f"{uptime:.0f}s"
        elif uptime < 3600:
            return f"{uptime/60:.0f}m"
        else:
            return f"{uptime/3600:.1f}h"
    
    def get_db_info(self):
        """Retourne les infos de la base"""
        if not self.db:
            return ""
        
        try:
            # Nombre de tables
            result = self.db.execute("SHOW TABLES")
            if result.get('success'):
                table_count = len(result.get('tables', []))
                return f"[T:{table_count}]"
        except:
            pass
        
        return ""
    
    def get_prompt(self, state='normal', multiline=False):
        """Génère le prompt"""
        style = self.config.get('shell.prompt_style', 'advanced')
        
        if style == 'simple':
            return self._simple_prompt(state, multiline)
        elif style == 'professional':
            return self._professional_prompt(state, multiline)
        else:  # advanced
            return self._advanced_prompt(state, multiline)
    
    def _simple_prompt(self, state, multiline):
        """Prompt simple"""
        color = self.state_colors.get(state, self.state_colors['normal'])
        
        if multiline:
            return f"{color}    -> \033[0m"
        else:
            return f"{color}gsql> \033[0m"
    
    def _advanced_prompt(self, state, multiline):
        """Prompt avancé"""
        color = self.state_colors.get(state, self.state_colors['normal'])
        icon = self.get_system_icon()
        user = getpass.getuser()
        host = socket.gethostname().split('.')[0]
        
        # Info système
        sys_info = ""
        if self.config.get('shell.show_system_info', True):
            sys_info = f"@{host}"
        
        # Info temps
        time_info = ""
        if self.config.get('shell.show_time', True):
            current_time = datetime.now().strftime("%H:%M")
            time_info = f" [{current_time}]"
        
        # Info base
        db_info = self.get_db_info()
        
        # Compteur de commandes
        self.command_count += 1
        
        if multiline:
            return f"{color}    ... \033[0m"
        else:
            return f"{color}{icon} {user}{sys_info}{time_info}{db_info} [{self.command_count}]>\033[0m "
    
    def _professional_prompt(self, state, multiline):
        """Prompt professionnel"""
        color = self.state_colors.get(state, self.state_colors['normal'])
        
        # Chemin courant
        path = ""
        if self.config.get('shell.show_path', True):
            cwd = os.getcwd()
            home = str(Path.home())
            if cwd.startswith(home):
                cwd = "~" + cwd[len(home):]
            path = f" {cwd}"
        
        # Uptime
        uptime = self.get_uptime()
        
        # Info Git si disponible
        git_info = self._get_git_info()
        
        if multiline:
            return f"{color}    | \033[0m"
        else:
            return f"{color}┌─[{getpass.getuser()}@{socket.gethostname().split('.')[0]}]{path}\n└─[{uptime}{git_info}]$ \033[0m"
    
    def _get_git_info(self):
        """Récupère les infos Git"""
        try:
            result = subprocess.run(
                ['git', 'branch', '--show-current'],
                capture_output=True,
                text=True,
                cwd=os.getcwd()
            )
            if result.returncode == 0:
                branch = result.stdout.strip()
                if branch:
                    return f" git:{branch}"
        except:
            pass
        return ""

# ==================== RICH SHELL ====================

class RichGSQLShell(cmd.Cmd):
    """Shell GSQL riche et avancé"""
    
    # Désactiver l'intro par défaut
    intro = None
    
    def __init__(self, gsql_app=None):
        super().__init__()
        self.gsql = gsql_app
        self.db = gsql_app.db if gsql_app else None
        self.completer = gsql_app.completer if gsql_app else None
        self.config = gsql_app.config if gsql_app else Config().data
        
        # Initialisations
        self.prompt_generator = RichPrompt(self.config, self.db)
        self.utils = ShellUtils()
        self.multiline_mode = False
        self.multiline_buffer = []
        self.current_database = ":memory:"
        
        # Options d'affichage
        self.output_format = OutputFormat.TABLE
        self.show_timer = True
        self.show_headers = True
        self.null_value = "NULL"
        self.pager_enabled = bool(self.config.get('shell.pager'))
        
        # Historique
        self.history_file = Path.home() / '.gsql' / 'history'
        self._setup_history()
        
        # Auto-complétion
        if self.config.get('shell.auto_complete', True) and self.completer:
            readline.set_completer(self.completer.complete)
            readline.parse_and_bind("tab: complete")
            readline.set_completer_delims(' \t\n`~!@#$%^&*()-=+[{]}\\|;:\'",<>/?')
        
        # Support multi-lignes
        if self.config.get('shell.multiline_mode', True):
            readline.parse_and_bind("set bind-tty-special-chars off")
        
        # Afficher le banner
        self._show_banner()
    
    def _show_banner(self):
        """Affiche le banner de bienvenue"""
        banner = f"""
{theme.info("╔═══════════════════════════════════════════════════════════╗")}
{theme.info("║")}  {theme.highlight("GSQL")} {theme.dim(f"v{__version__}")} - Advanced SQL Shell with Rich Features  {theme.info("║")}
{theme.info("╠═══════════════════════════════════════════════════════════╣")}
{theme.info("║")}  System: {theme.dim(platform.system())} {theme.dim(platform.release())} ({theme.dim(platform.machine())})      {theme.info("║")}
{theme.info("║")}  Host: {theme.dim(socket.gethostname())} | User: {theme.dim(getpass.getuser())}     {theme.info("║")}
{theme.info("║")}  Python: {theme.dim(platform.python_version())} | SQLite: {theme.dim(sqlite3.sqlite_version)}      {theme.info("║")}
{theme.info("╠═══════════════════════════════════════════════════════════╣")}
{theme.info("║")}  {theme.dim("Type .help for commands, .exit to quit")}                  {theme.info("║")}
{theme.info("║")}  {theme.dim("Use \\ for multi-line, ; to execute")}                     {theme.info("║")}
{theme.info("╚═══════════════════════════════════════════════════════════╝")}
        """
        print(banner.strip())
    
    def _setup_history(self):
        """Configure l'historique"""
        try:
            self.history_file.parent.mkdir(parents=True, exist_ok=True)
            readline.read_history_file(str(self.history_file))
        except FileNotFoundError:
            pass
        
        readline.set_history_length(self.config.get('shell.max_history', 5000))
        atexit.register(readline.write_history_file, str(self.history_file))
    
    # ==================== PROMPT MANAGEMENT ====================
    
    @property
    def prompt(self):
        """Retourne le prompt dynamique"""
        state = 'multiline' if self.multiline_mode else 'normal'
        return self.prompt_generator.get_prompt(state, self.multiline_mode)
    
    # ==================== COMMAND PROCESSING ====================
    
    def default(self, line: str):
        """Gère les commandes par défaut"""
        if not line.strip():
            return
        
        # Commandes shell
        if line.startswith('!'):
            self._execute_shell_command(line[1:])
            return
        
        # Commandes GSQL avec point
        if line.startswith('.'):
            self._handle_dot_command(line)
            return
        
        # SQL
        self._process_sql_command(line)
    
    def _process_sql_command(self, line: str):
        """Traite une commande SQL"""
        # Support multi-lignes
        if self.multiline_mode:
            if line.lower() in ['end', 'commit', ';']:
                # Exécuter le buffer
                full_sql = '\n'.join(self.multiline_buffer)
                self.multiline_buffer = []
                self.multiline_mode = False
                self._execute_sql(full_sql)
            else:
                # Ajouter à la ligne en cours
                self.multiline_buffer.append(line)
        elif line.endswith('\\'):
            # Démarrer le mode multi-lignes
            self.multiline_mode = True
            self.multiline_buffer.append(line[:-1].strip())
        else:
            # Exécuter directement
            self._execute_sql(line)
    
    def _execute_shell_command(self, command: str):
        """Exécute une commande shell"""
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True
            )
            print(result.stdout)
            if result.stderr:
                print(theme.error(result.stderr))
            print(theme.dim(f"[Exit code: {result.returncode}]"))
        except Exception as e:
            print(theme.error(f"Shell error: {e}"))
    
    # ==================== DOT COMMANDS ====================
    
    def _handle_dot_command(self, command: str):
        """Gère les commandes avec point"""
        parts = command[1:].strip().split()
        cmd = parts[0].lower() if parts else ""
        args = parts[1:] if len(parts) > 1 else []
        
        # Mapping des commandes
        commands = {
            'tables': self._cmd_tables,
            'schema': self._cmd_schema,
            'indexes': self._cmd_indexes,
            'stats': self._cmd_stats,
            'info': self._cmd_info,
            'backup': self._cmd_backup,
            'vacuum': self._cmd_vacuum,
            'import': self._cmd_import,
            'export': self._cmd_export,
            'mode': self._cmd_mode,
            'headers': self._cmd_headers,
            'timer': self._cmd_timer,
            'echo': self._cmd_echo,
            'nullvalue': self._cmd_nullvalue,
            'width': self._cmd_width,
            'exit': self._cmd_exit,
            'quit': self._cmd_exit,
            'help': self._cmd_help,
            'clear': self._cmd_clear,
            'history': self._cmd_history,
            'system': self._cmd_system,
            'sysinfo': self._cmd_sysinfo,
            'ps': self._cmd_ps,
            'df': self._cmd_df,
            'du': self._cmd_du,
            'top': self._cmd_top,
            'users': self._cmd_users,
            'logs': self._cmd_logs,
            'config': self._cmd_config,
        }
        
        if cmd in commands:
            try:
                if commands[cmd](*args) is True:
                    return True
            except TypeError as e:
                print(theme.error(f"Usage error: {e}"))
        else:
            print(theme.error(f"Unknown command: .{cmd}"))
            print(theme.dim("Type .help for available commands"))
    
    # ==================== SQL COMMANDS ====================
    
    def _execute_sql(self, sql: str):
        """Exécute une requête SQL"""
        if not self.db:
            print(theme.error("No database connection"))
            return
        
        try:
            sql = sql.strip()
            if not sql:
                return
            
            # Syntax highlighting
            if self.config.get('shell.syntax_highlight', True):
                print(theme.dim(f"Executing: {self._colorize_sql(sql)}"))
            
            # Exécution
            start_time = time.time()
            result = self.db.execute(sql)
            execution_time = time.time() - start_time
            
            # Affichage
            self._display_result(result, execution_time)
            
        except Exception as e:
            print(theme.error(f"SQL Error: {e}"))
            if self.config.get('verbose_errors', False):
                traceback.print_exc()
    
    def _display_result(self, result: Dict, execution_time: float):
        """Affiche le résultat"""
        if not result.get('success'):
            print(theme.error(f"Query failed: {result.get('message', 'Unknown error')}"))
            return
        
        query_type = result.get('type', '').lower()
        
        # Gestion par type de requête
        display_methods = {
            'select': self._display_select,
            'insert': self._display_insert,
            'update': self._display_update,
            'delete': self._display_delete,
            'show_tables': self._display_show_tables,
            'describe': self._display_describe,
            'stats': self._display_stats,
            'vacuum': self._display_vacuum,
            'backup': self._display_backup,
        }
        
        display_method = display_methods.get(query_type, self._display_generic)
        display_method(result)
        
        # Timer
        if self.show_timer:
            print(theme.dim(f"Time: {self.utils.format_time(execution_time)}"))
    
    def _display_select(self, result: Dict):
        """Affiche le résultat d'un SELECT"""
        rows = result.get('rows', [])
        columns = result.get('columns', [])
        count = result.get('count', 0)
        
        if count == 0:
            print(theme.warning("No rows returned"))
            return
        
        # Formater selon le format de sortie
        if self.output_format == OutputFormat.TABLE:
            # Table format
            if self.show_headers:
                print(theme.highlight(self.utils.table([], columns, max_width=50)))
            
            # Données
            data = []
            for row in rows[:100]:  # Limite à 100 lignes
                if isinstance(row, (list, tuple)):
                    data.append([str(v) if v is not None else self.null_value for v in row])
                else:
                    data.append([str(row)])
            
            table_str = self.utils.table(data, max_width=50)
            print(table_str)
            
            if len(rows) > 100:
                print(theme.dim(f"... and {len(rows) - 100} more rows"))
            
            print(theme.dim(f"{count} row(s) returned"))
            
        elif self.output_format == OutputFormat.CSV:
            # CSV format
            if self.show_headers:
                print(",".join(columns))
            for row in rows:
                if isinstance(row, (list, tuple)):
                    print(",".join(str(v) if v is not None else '' for v in row))
                else:
                    print(str(row))
        
        elif self.output_format == OutputFormat.JSON:
            # JSON format
            import json
            data = []
            for row in rows:
                if isinstance(row, (list, tuple)):
                    data.append(dict(zip(columns, row)))
                else:
                    data.append({columns[0]: row})
            print(json.dumps(data, indent=2, default=str))
    
    def _display_insert(self, result: Dict):
        """Affiche le résultat d'un INSERT"""
        print(theme.success(f"✓ Inserted successfully"))
        if 'last_insert_id' in result:
            print(theme.dim(f"Last insert ID: {result['last_insert_id']}"))
        if 'rows_affected' in result:
            print(theme.dim(f"Rows affected: {result['rows_affected']}"))
    
    def _display_update(self, result: Dict):
        """Affiche le résultat d'un UPDATE/DELETE"""
        print(theme.success(f"✓ Operation successful"))
        if 'rows_affected' in result:
            print(theme.dim(f"Rows affected: {result['rows_affected']}"))
    
    def _display_show_tables(self, result: Dict):
        """Affiche les tables"""
        tables = result.get('tables', [])
        if tables:
            print(theme.success(f"Found {len(tables)} table(s):"))
            for table in tables:
                rows = table.get('rows', '?')
                print(f"  • {theme.highlight(table['table'])} ({theme.dim(f'{rows} rows')})")
        else:
            print(theme.warning("No tables found"))
    
    def _display_describe(self, result: Dict):
        """Affiche la structure d'une table"""
        columns = result.get('columns', [])
        if columns:
            print(theme.success(f"Table structure:"))
            table_data = []
            for col in columns:
                null_str = "NOT NULL" if not col.get('null') else "NULL"
                default_str = f"DEFAULT {col.get('default')}" if col.get('default') else ""
                key_str = f" {col.get('key')}" if col.get('key') else ""
                table_data.append([
                    col['field'],
                    col['type'],
                    null_str,
                    default_str,
                    key_str
                ])
            
            headers = ["Field", "Type", "Null", "Default", "Key"]
            print(self.utils.table(table_data, headers, max_width=30))
        else:
            print(theme.warning("No columns found"))
    
    def _display_stats(self, result: Dict):
        """Affiche les statistiques"""
        stats = result.get('database', {})
        if stats:
            print(theme.success("Database statistics:"))
            for key, value in stats.items():
                if isinstance(value, dict):
                    print(f"  {key}:")
                    for k, v in value.items():
                        print(f"    {k}: {v}")
                else:
                    print(f"  {key}: {value}")
    
    def _display_generic(self, result: Dict):
        """Affiche un résultat générique"""
        print(theme.success("Query executed successfully"))
        if 'message' in result:
            print(theme.dim(result['message']))
    
    # ==================== DOT COMMAND IMPLEMENTATIONS ====================
    
    def _cmd_tables(self, *args):
        """Liste les tables"""
        self._execute_sql("SHOW TABLES")
    
    def _cmd_schema(self, *args):
        """Affiche le schéma d'une table"""
        if args:
            self._execute_sql(f"DESCRIBE {args[0]}")
        else:
            print(theme.error("Usage: .schema <table_name>"))
    
    def _cmd_indexes(self, *args):
        """Affiche les index"""
        table = args[0] if args else None
        if table:
            self._execute_sql(f"SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='{table}'")
        else:
            self._execute_sql("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index'")
    
    def _cmd_stats(self, *args):
        """Affiche les statistiques"""
        self._execute_sql("STATS")
    
    def _cmd_info(self, *args):
        """Affiche les infos de la base"""
        print(theme.success("Database Information:"))
        print(f"  Path: {theme.dim(self.db.storage.db_path)}")
        print(f"  Size: {theme.dim(self.utils.format_size(os.path.getsize(self.db.storage.db_path)))}")
        
        # Info tables
        result = self.db.execute("SHOW TABLES")
        if result.get('success'):
            tables = result.get('tables', [])
            print(f"  Tables: {theme.highlight(len(tables))}")
            
            # Taille par table
            print(f"  Tables details:")
            for table in tables[:5]:  # Limiter à 5 tables
                table_name = table['table']
                try:
                    count_result = self.db.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                    count = count_result.get('rows', [[0]])[0][0]
                    print(f"    {table_name}: {theme.dim(f'{count} rows')}")
                except:
                    print(f"    {table_name}: {theme.dim('error')}")
            
            if len(tables) > 5:
                print(f"    ... and {len(tables) - 5} more tables")
    
    def _cmd_backup(self, *args):
        """Crée une sauvegarde"""
        file = args[0] if args else f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        self._execute_sql(f"BACKUP {file}")
    
    def _cmd_vacuum(self, *args):
        """Optimise la base"""
        print(theme.info("Optimizing database..."))
        self._execute_sql("VACUUM")
    
    def _cmd_mode(self, *args):
        """Change le mode d'affichage"""
        if args:
            mode = args[0].lower()
            if mode in ['table', 'csv', 'json', 'yaml', 'xml', 'markdown']:
                self.output_format = OutputFormat(mode)
                print(theme.success(f"Output mode set to: {mode}"))
            elif mode == 'list':
                print(theme.info("Available modes:"))
                for fmt in OutputFormat:
                    print(f"  {fmt.value}")
            else:
                print(theme.error(f"Unknown mode: {mode}"))
        else:
            print(theme.info(f"Current mode: {self.output_format.value}"))
    
    def _cmd_headers(self, *args):
        """Active/désactive les en-têtes"""
        if args:
            arg = args[0].lower()
            if arg in ['on', 'yes', 'true', '1']:
                self.show_headers = True
            elif arg in ['off', 'no', 'false', '0']:
                self.show_headers = False
        self.show_headers = not self.show_headers
        status = "ON" if self.show_headers else "OFF"
        print(theme.success(f"Headers: {status}"))
    
    def _cmd_timer(self, *args):
        """Active/désactive le timer"""
        if args:
            arg = args[0].lower()
            if arg in ['on', 'yes', 'true', '1']:
                self.show_timer = True
            elif arg in ['off', 'no', 'false', '0']:
                self.show_timer = False
        self.show_timer = not self.show_timer
        status = "ON" if self.show_timer else "OFF"
        print(theme.success(f"Timer: {status}"))
    
    def _cmd_echo(self, *args):
        """Affiche du texte"""
        print(" ".join(args))
    
    def _cmd_nullvalue(self, *args):
        """Définit la valeur NULL"""
        if args:
            self.null_value = args[0]
            print(theme.success(f"Null value: '{self.null_value}'"))
        else:
            print(theme.info(f"Current null value: '{self.null_value}'"))
    
    def _cmd_width(self, *args):
        """Définit les largeurs de colonnes"""
        if args:
            try:
                width = int(args[0])
                print(theme.success(f"Column width: {width}"))
            except ValueError:
                print(theme.error("Width must be a number"))
        else:
            print(theme.info("Current width: auto"))
    
    def _cmd_exit(self, *args):
        """Quitte le shell"""
        print(theme.success("Goodbye!"))
        return True
    
    def _cmd_help(self, *args):
        """Affiche l'aide"""
        self.do_help("")
    
    def _cmd_clear(self, *args):
        """Efface l'écran"""
        self.utils.clear_screen()
    
    def _cmd_history(self, *args):
        """Affiche l'historique"""
        try:
            histsize = readline.get_current_history_length()
            start = max(1, histsize - 50) if not args else int(args[0])
            for i in range(start, histsize + 1):
                cmd = readline.get_history_item(i)
                if cmd:
                    print(f"{i:4d}  {cmd}")
        except Exception as e:
            print(theme.error(f"History error: {e}"))
    
    def _cmd_system(self, *args):
        """Exécute une commande système"""
        if args:
            self._execute_shell_command(" ".join(args))
        else:
            print(theme.error("Usage: .system <command>"))
    
    def _cmd_sysinfo(self, *args):
        """Affiche les informations système"""
        import psutil
        
        print(theme.success("System Information:"))
        
        # Platform
        print(f"  Platform: {theme.dim(platform.platform())}")
        print(f"  Hostname: {theme.dim(socket.gethostname())}")
        print(f"  Username: {theme.dim(getpass.getuser())}")
        print(f"  Python: {theme.dim(platform.python_version())}")
        
        # CPU
        cpu_percent = psutil.cpu_percent(interval=0.1)
        cpu_count = psutil.cpu_count()
        print(f"  CPU: {theme.dim(f'{cpu_count} cores, {cpu_percent}% used')}")
        
        # Memory
        memory = psutil.virtual_memory()
        print(f"  Memory: {theme.dim(self.utils.format_size(memory.used))} / "
              f"{self.utils.format_size(memory.total)} "
              f"({memory.percent}%)")
        
        # Disk
        disk = psutil.disk_usage('/')
        print(f"  Disk: {theme.dim(self.utils.format_size(disk.used))} / "
              f"{self.utils.format_size(disk.total)} "
              f"({disk.percent}%)")
        
        # Uptime
        boot_time = datetime.fromtimestamp(psutil.boot_time())
        uptime = datetime.now() - boot_time
        print(f"  Uptime: {theme.dim(str(uptime).split('.')[0])}")
    
    def _cmd_ps(self, *args):
        """Affiche les processus"""
        import psutil
        
        print(theme.success("Running Processes:"))
        headers = ["PID", "Name", "CPU%", "MEM%", "Status"]
        data = []
        
        for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'status']):
            try:
                info = proc.info
                data.append([
                    info['pid'],
                    info['name'][:20],
                    f"{info.get('cpu_percent', 0):.1f}",
                    f"{info.get('memory_percent', 0):.1f}",
                    info.get('status', 'unknown')[:10]
                ])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        # Trier par CPU
        data.sort(key=lambda x: float(x[2]) if x[2].replace('.', '', 1).isdigit() else 0, reverse=True)
        
        print(self.utils.table(data[:20], headers))
        print(theme.dim(f"Showing {min(20, len(data))} of {len(data)} processes"))
    
    def _cmd_df(self, *args):
        """Affiche l'usage du disque"""
        import psutil
        
        print(theme.success("Disk Usage:"))
        headers = ["Filesystem", "Size", "Used", "Free", "Use%", "Mount"]
        data = []
        
        for part in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(part.mountpoint)
                data.append([
                    part.device[:20],
                    self.utils.format_size(usage.total),
                    self.utils.format_size(usage.used),
                    self.utils.format_size(usage.free),
                    f"{usage.percent}%",
                    part.mountpoint[:30]
                ])
            except:
                pass
        
        print(self.utils.table(data, headers))
    
    def _cmd_du(self, *args):
        """Affiche l'usage du disque par dossier"""
        path = args[0] if args else "."
        
        print(theme.success(f"Disk Usage for: {path}"))
        
        try:
            total = 0
            for root, dirs, files in os.walk(path):
                for name in files:
                    try:
                        filepath = os.path.join(root, name)
                        total += os.path.getsize(filepath)
                    except:
                        pass
            
            print(f"  Total size: {theme.highlight(self.utils.format_size(total))}")
            
            # Top 10 des plus gros fichiers
            print(f"  Top files:")
            files_with_size = []
            for root, dirs, files in os.walk(path):
                for name in files[:10]:  # Limiter pour la performance
                    try:
                        filepath = os.path.join(root, name)
                        size = os.path.getsize(filepath)
                        files_with_size.append((filepath, size))
                    except:
                        pass
            
            # Trier et afficher
            files_with_size.sort(key=lambda x: x[1], reverse=True)
            for filepath, size in files_with_size[:5]:
                rel_path = os.path.relpath(filepath, path)
                print(f"    {self.utils.format_size(size):>10}  {rel_path[:50]}")
                
        except Exception as e:
            print(theme.error(f"Error: {e}"))
    
    def _cmd_top(self, *args):
        """Affiche les processus consommateurs"""
        self._cmd_ps()
    
    def _cmd_users(self, *args):
        """Affiche les utilisateurs connectés"""
        print(theme.success("Connected Users:"))
        try:
            import pwd
            for user in pwd.getpwall():
                print(f"  {user.pw_name} ({user.pw_gecos})")
        except:
            print(theme.dim("User information not available"))
    
    def _cmd_logs(self, *args):
        """Affiche les logs"""
        log_level = args[0] if args else 'INFO'
        print(theme.success(f"Logs (level: {log_level}):"))
        # Implémenter la lecture des logs
        print(theme.dim("Log viewing not implemented"))
    
    def _cmd_config(self, *args):
        """Affiche/Modifie la configuration"""
        if args:
            if len(args) == 2:
                # Set config
                key, value = args
                self.config[key] = value
                print(theme.success(f"Set {key} = {value}"))
            else:
                # Get config
                key = args[0]
                value = self.config.get(key, "Not set")
                print(f"{key} = {value}")
        else:
            # Show all config
            print(theme.success("Configuration:"))
            for key, value in self.config.items():
                if isinstance(value, dict):
                    print(f"  {key}:")
                    for k, v in value.items():
                        print(f"    {k}: {v}")
                else:
                    print(f"  {key}: {value}")
    
    # ==================== BUILT-IN COMMANDS ====================
    
    def do_help(self, arg: str):
        """Affiche l'aide complète"""
        help_text = """
{success}GSQL Advanced Shell - Command Reference{reset}

{info}SQL COMMANDS:{reset}
  All standard SQL commands are supported
  Examples:
    SELECT * FROM table WHERE condition;
    INSERT INTO table (col1, col2) VALUES (val1, val2);
    CREATE TABLE name (id INT PRIMARY KEY, name TEXT);

{info}DOT COMMANDS (GSQL Specific):{reset}

{highlight}Database:{reset}
  .tables [pattern]         - List tables (optional pattern)
  .schema <table>          - Show table structure
  .indexes [table]         - Show indexes
  .stats                   - Database statistics
  .info                    - Detailed database info
  .backup [file]           - Create backup
  .vacuum                  - Optimize database
  .import <file> <table>   - Import data
  .export <table> <file>   - Export data

{highlight}Display:{reset}
  .mode [table|csv|json]   - Set output format
  .headers [on|off]        - Toggle column headers
  .timer [on|off]          - Toggle execution timer
  .nullvalue <string>      - Set NULL display string
  .width <num>             - Set column width

{highlight}System:{reset}
  .sysinfo                 - Show system information
  .ps                      - Show running processes
  .df                      - Show disk usage
  .du [path]               - Show disk usage by directory
  .top                     - Show top processes
  .users                   - Show connected users

{highlight}Shell:{reset}
  .clear                   - Clear screen
  .history [n]             - Show last n commands
  .system <cmd>            - Execute system command
  .echo <text>             - Print text
  .help                    - This help
  .exit/.quit              - Exit GSQL

{highlight}Administration:{reset}
  .config [key] [value]    - View/Set configuration
  .logs [level]            - View logs
  .restore <file>          - Restore from backup

{info}SHELL FEATURES:{reset}
  • Multi-line SQL: End line with \
  • Syntax highlighting
  • Auto-completion (Tab key)
  • Command history (Up/Down arrows)
  • Shell commands: !<command>
  • Pager support for long output
  • Customizable prompt
  • System integration

{info}KEYBOARD SHORTCUTS:{reset}
  Ctrl+C     - Cancel current operation
  Ctrl+D     - Exit GSQL
  Ctrl+L     - Clear screen
  Ctrl+R     - Search history
  Tab        - Auto-completion

{info}EXAMPLES:{reset}
  .tables                           # List all tables
  .schema users                     # Show users table structure
  .mode json                        # Set JSON output
  !ls -la                           # Execute shell command
  .sysinfo                          # Show system info
  SELECT * FROM users LIMIT 10;     # SQL query
        """.format(
            success=theme.success(""),
            info=theme.info(""),
            highlight=theme.highlight(""),
            reset="\033[0m"
        )
        
        # Utiliser un pager pour l'aide si disponible
        if self.pager_enabled and len(help_text) > 1000:
            self._pager_output(help_text)
        else:
            print(help_text.strip())
    
    def do_exit(self, arg: str):
        """Quitte le shell"""
        return self._cmd_exit()
    
    def do_quit(self, arg: str):
        """Quitte le shell"""
        return self.do_exit(arg)
    
    def do_clear(self, arg: str):
        """Efface l'écran"""
        self._cmd_clear()
    
    def do_history(self, arg: str):
        """Affiche l'historique"""
        self._cmd_history(arg)
    
    # ==================== SHELL CONTROL ====================
    
    def emptyline(self):
        """Ne rien faire sur ligne vide"""
        pass
    
    def precmd(self, line: str) -> str:
        """Avant l'exécution de la commande"""
        # Enregistrer dans l'historique
        if line and not line.startswith('.') and not self.multiline_mode:
            readline.add_history(line)
        return line
    
    def postcmd(self, stop: bool, line: str) -> bool:
        """Après l'exécution de la commande"""
        return stop
    
    def sigint_handler(self, signum, frame):
        """Gère Ctrl+C"""
        print("\n" + theme.warning("^C"))
        self.multiline_mode = False
        self.multiline_buffer = []
    
    # ==================== UTILITIES ====================
    
    def _colorize_sql(self, sql: str) -> str:
        """Colorise la syntaxe SQL"""
        if not self.config.get('shell.syntax_highlight', True):
            return sql
        
        # Mots-clés SQL
        keywords = self.completer.sql_keywords if self.completer else []
        
        # Coloriser les mots-clés
        for keyword in keywords:
            pattern = rf'\b{keyword}\b'
            sql = re.sub(pattern, theme.sql_keyword(keyword), sql, flags=re.IGNORECASE)
        
        # Coloriser les chaînes
        sql = re.sub(r"'[^']*'", lambda m: theme.sql_string(m.group(0)), sql)
        sql = re.sub(r'"[^"]*"', lambda m: theme.sql_string(m.group(0)), sql)
        
        # Coloriser les nombres
        sql = re.sub(r'\b\d+\b', lambda m: theme.sql_number(m.group(0)), sql)
        sql = re.sub(r'\b\d+\.\d+\b', lambda m: theme.sql_number(m.group(0)), sql)
        
        # Coloriser les commentaires
        sql = re.sub(r'--.*$', lambda m: theme.sql_comment(m.group(0)), sql, flags=re.MULTILINE)
        
        return sql
    
    def _pager_output(self, text: str):
        """Affiche le texte via un pager"""
        if not self.pager_enabled:
            print(text)
            return
        
        try:
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
                f.write(text)
                temp_file = f.name
            
            pager_cmd = self.config.get('shell.pager', 'less -R')
            subprocess.run(f"{pager_cmd} {temp_file}", shell=True)
            os.unlink(temp_file)
        except:
            print(text)

# ==================== MAIN GSQL APPLICATION ====================

class GSQLApp:
    """Application GSQL principale"""
    
    def __init__(self):
        self.config = Config().data
        self.db = None
        self.completer = None
        
        # Configurer le logging
        setup_logging(
            level=self.config.get('log_level', 'INFO'),
            log_file=None
        )
        
        logger.info(f"GSQL v{__version__} - Rich Shell")
    
    def _initialize(self, database_path: Optional[str] = None):
        """Initialise les composants GSQL"""
        try:
            print(theme.info("Initializing GSQL Rich Shell..."))
            
            # Créer la base de données
            db_config = self.config.get('database', {}).copy()
            if database_path:
                db_config['path'] = database_path
            
            self.db = create_database(**db_config)
            
            # Configurer l'auto-complétion
            self.completer = GSQLCompleter(self.db)
            
            print(theme.success("✓ GSQL Rich Shell ready!"))
            
        except Exception as e:
            print(theme.error(f"Failed to initialize GSQL: {e}"))
            traceback.print_exc()
            sys.exit(1)
    
    def run_shell(self, database_path: Optional[str] = None):
        """Lance le shell interactif"""
        # Initialiser
        self._initialize(database_path)
        
        # Créer et lancer le shell
        shell = RichGSQLShell(self)
        
        # Configurer le handler pour Ctrl+C
        signal.signal(signal.SIGINT, shell.sigint_handler)
        
        try:
            shell.cmdloop()
        except KeyboardInterrupt:
            print("\n" + theme.info("Session interrupted"))
        except EOFError:
            print("\n" + theme.info("Session ended"))
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Nettoie les ressources"""
        try:
            if self.db:
                self.db.close()
                print(theme.dim("Database connection closed"))
        except:
            pass

# ==================== MAIN FUNCTION ====================

def main():
    """Fonction principale"""
    if not GSQL_AVAILABLE:
        print(theme.error("GSQL modules not available. Check installation."))
        sys.exit(1)
    
    # Ajouter sqlite3 pour la version
    import sqlite3
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description=f"GSQL v{__version__} - Advanced SQL Shell with System Integration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
{theme.info("Examples:")}
  gsql                          # Start interactive shell
  gsql mydb.db                  # Open specific database
  gsql --no-color               # Disable colors
  gsql --mode json              # Set JSON output mode
  gsql --prompt professional    # Use professional prompt
  gsql --help                   # Show full help

{theme.info("System:")} {platform.system()} {platform.release()} ({platform.machine()})
{theme.info("Python:")} {platform.python_version()}
{theme.info("SQLite:")} {sqlite3.sqlite_version}
        """
    )
    
    parser.add_argument(
        'database',
        nargs='?',
        help='Database file (uses :memory: if not specified)'
    )
    
    parser.add_argument(
        '--no-color',
        action='store_true',
        help='Disable colored output'
    )
    
    parser.add_argument(
        '--mode',
        choices=['table', 'csv', 'json', 'yaml'],
        default='table',
        help='Output format (default: table)'
    )
    
    parser.add_argument(
        '--prompt',
        choices=['simple', 'advanced', 'professional'],
        default='advanced',
        help='Prompt style (default: advanced)'
    )
    
    parser.add_argument(
        '--theme',
        choices=['default', 'dark', 'light'],
        default='default',
        help='Color theme (default: default)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output and errors'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version=f'GSQL {__version__} on {platform.system()} {platform.release()} ({platform.machine()})'
    )
    
    args = parser.parse_args()
    
    # Configurer l'application
    app = GSQLApp()
    
    # Appliquer les arguments
    if args.no_color:
        app.config['colors'] = False
        app.config['shell']['syntax_highlight'] = False
    
    if args.mode:
        app.config['shell']['default_mode'] = args.mode
    
    if args.prompt:
        app.config['shell']['prompt_style'] = args.prompt
    
    if args.theme:
        global theme
        theme = Theme(ShellTheme(args.theme))
    
    if args.verbose:
        app.config['log_level'] = 'DEBUG'
        app.config['verbose_errors'] = True
    
    # Mode shell interactif
    app.run_shell(args.database)

# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    main()
