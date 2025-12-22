#!/usr/bin/env python3
"""
GSQL - SQL Database with Natural Language Interface
Main CLI Interface - Version 3.0
"""
# Dans database.py - Remplacer le début du fichier :

# SUPPRIMER:
# import yaml

# GARDER:
# Ajoutez cet import en haut du fichier __main__.py
import cmd
# Ajoutez ces deux lignes :
import signal
import traceback
import sys
from .executor import create_executor
import os
import sqlite3
import json
import logging
import time
import threading
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import re
from .database import create_database
from .storage import SQLiteStorage, create_storage
from .exceptions import (
    GSQLBaseException, SQLSyntaxError, SQLExecutionError,
    ConstraintViolationError, TransactionError, FunctionError,
    BufferPoolError, StorageError  # Ajouter StorageError
)


# ==================== CONFIGURATION ====================

class Config:
    """Configuration de GSQL"""
    
    DEFAULT_CONFIG = {
        'database': {
            'path': None,  # Auto-détection
            'base_dir': '/root/.gsql',
            'auto_recovery': True,
            'buffer_pool_size': 100,
            'enable_wal': True
        },
        'executor': {
            'cache_size': 200,
            'timeout': 30,
            'auto_translate_nl': True,
            'enable_cache': True
        },
        'cli': {
            'prompt': 'gsql> ',
            'history_file': '~/.gsql_history',
            'max_history': 1000,
            'autocomplete': True,
            'colors': True,
            'verbose_errors': True,
            'show_time': False,
            'multiline_mode': True
        },
        'display': {
            'max_rows': 1000,
            'truncate_length': 50,
            'show_row_numbers': True,
            'border_style': 'rounded',
            'null_display': '[NULL]'
        },
        'logging': {
            'level': 'INFO',
            'file': None,
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
    }
    
    @staticmethod
    def load():
        """Charge la configuration"""
        config_path = Path.home() / '.gsql' / 'config.json'
        config = Config.DEFAULT_CONFIG.copy()
        
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                    Config._deep_update(config, user_config)
            except Exception as e:
                print(f"Warning: Could not load config: {e}")
        
        return config
    
    @staticmethod
    def _deep_update(target, source):
        """Mise à jour récursive de dictionnaire"""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                Config._deep_update(target[key], value)
            else:
                target[key] = value

# ==================== COLORS & FORMATTING ====================

class Colors:
    """Couleurs ANSI pour le terminal"""
    
    # Couleurs de base
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
    BG_BRIGHT_BLACK = '\033[100m'
    BG_BRIGHT_RED = '\033[101m'
    BG_BRIGHT_GREEN = '\033[102m'
    BG_BRIGHT_YELLOW = '\033[103m'
    BG_BRIGHT_BLUE = '\033[104m'
    BG_BRIGHT_MAGENTA = '\033[105m'
    BG_BRIGHT_CYAN = '\033[106m'
    BG_BRIGHT_WHITE = '\033[107m'
    
    @staticmethod
    def colorize(text, color_code):
        """Applique une couleur au texte si supporté"""
        if sys.stdout.isatty() and Config.load()['cli']['colors']:
            return f"{color_code}{text}{Colors.RESET}"
        return text
    
    @staticmethod
    def success(text):
        return Colors.colorize(text, Colors.GREEN)
    
    @staticmethod
    def error(text):
        return Colors.colorize(text, Colors.RED)
    
    @staticmethod
    def warning(text):
        return Colors.colorize(text, Colors.YELLOW)
    
    @staticmethod
    def info(text):
        return Colors.colorize(text, Colors.CYAN)
    
    @staticmethod
    def highlight(text):
        return Colors.colorize(text, Colors.BRIGHT_WHITE)
    
    @staticmethod
    def sql(text):
        return Colors.colorize(text, Colors.BRIGHT_CYAN)
    
    @staticmethod
    def value(text):
        return Colors.colorize(text, Colors.BRIGHT_YELLOW)
    
    @staticmethod
    def table(text):
        return Colors.colorize(text, Colors.BRIGHT_GREEN)
    
    @staticmethod
    def column(text):
        return Colors.colorize(text, Colors.BRIGHT_MAGENTA)
# Dans __main__.py, cherchez la classe Colors et ajoutez :
    # ... autres méthodes existantes ...
    
    @staticmethod
    def dim(text):
        """Retourne du texte avec effet 'dim' (moins visible)"""
        return f"\033[2m{text}\033[0m"
        
class TableFormatter:
    """Formatage de tables pour l'affichage"""
    
    @staticmethod
    def format_table(rows, columns=None, max_rows=1000, truncate=50):
        """Formate des données en tableau"""
        if not rows:
            return "No rows returned"
        
        # Déterminer les colonnes
        if columns is None and rows:
            if isinstance(rows[0], dict):
                columns = list(rows[0].keys())
            else:
                columns = [f"col{i}" for i in range(len(rows[0]))]
        
        # Tronquer si nécessaire
        display_rows = rows[:max_rows]
        truncated = len(rows) > max_rows
        
        # Calculer les largeurs de colonnes
        col_widths = {}
        for col in columns:
            # Largeur de l'en-tête
            col_widths[col] = len(str(col))
            # Largeur maximale des données
            for row in display_rows:
                if isinstance(row, dict):
                    value = row.get(col, '')
                else:
                    idx = columns.index(col) if col in columns else 0
                    value = row[idx] if idx < len(row) else ''
                
                str_value = str(value)
                if len(str_value) > truncate:
                    str_value = str_value[:truncate-3] + "..."
                
                col_widths[col] = max(col_widths[col], len(str_value))
        
        # Construire les bordures
        border_top = "╭" + "┬".join(["─" * (col_widths[col] + 2) for col in columns]) + "╮"
        border_mid = "├" + "┼".join(["─" * (col_widths[col] + 2) for col in columns]) + "┤"
        border_bot = "╰" + "┴".join(["─" * (col_widths[col] + 2) for col in columns]) + "╯"
        
        # Construire l'en-tête
        header = "│"
        for col in columns:
            header += f" {Colors.column(col.center(col_widths[col]))} │"
        
        # Construire les lignes
        lines = [border_top, header, border_mid]
        
        for i, row in enumerate(display_rows):
            line = "│"
            for col in columns:
                if isinstance(row, dict):
                    value = row.get(col, '')
                else:
                    idx = columns.index(col) if col in columns else i
                    value = row[idx] if idx < len(row) else ''
                
                # Formater la valeur
                str_value = str(value)
                if value is None:
                    str_value = Colors.DIM + Config.load()['display']['null_display'] + Colors.RESET
                elif len(str_value) > truncate:
                    str_value = Colors.VALUE(str_value[:truncate-3]) + Colors.DIM + "..." + Colors.RESET
                else:
                    str_value = Colors.VALUE(str_value)
                
                line += f" {str_value.ljust(col_widths[col])} │"
            
            lines.append(line)
        
        lines.append(border_bot)
        
        # Ajouter le résumé
        if truncated:
            lines.append(f"\n({max_rows} rows shown, {len(rows)} total)")
        else:
            lines.append(f"\n({len(rows)} row(s))")
        
        return "\n".join(lines)
    
    @staticmethod
    def format_simple(rows, columns=None):
        """Format simple pour les petites sorties"""
        if not rows:
            return ""
        
        output = []
        for i, row in enumerate(rows):
            if isinstance(row, dict):
                line = []
                for key, value in row.items():
                    line.append(f"{key}: {Colors.value(value)}")
                output.append(f"{i+1:3}. " + " | ".join(line))
            else:
                output.append(f"{i+1:3}. " + " | ".join(str(v) for v in row))
        
        return "\n".join(output)

# ==================== COMMAND HISTORY ====================

class CommandHistory:
    """Gestionnaire d'historique des commandes"""
    
    def __init__(self, history_file=None, max_size=1000):
        self.history_file = history_file
        self.max_size = max_size
        self.history = []
        self.current_index = 0
        
        self.load_history()
    
    def load_history(self):
        """Charge l'historique depuis le fichier"""
        if self.history_file:
            history_path = Path(self.history_file).expanduser()
            if history_path.exists():
                try:
                    with open(history_path, 'r', encoding='utf-8') as f:
                        self.history = [line.strip() for line in f if line.strip()]
                except Exception:
                    self.history = []
    
    def save_history(self):
        """Sauvegarde l'historique dans le fichier"""
        if self.history_file:
            history_path = Path(self.history_file).expanduser()
            try:
                history_path.parent.mkdir(parents=True, exist_ok=True)
                with open(history_path, 'w', encoding='utf-8') as f:
                    for cmd in self.history[-self.max_size:]:
                        f.write(cmd + '\n')
            except Exception:
                pass
    
    def add(self, command):
        """Ajoute une commande à l'historique"""
        if command and command.strip() and (not self.history or self.history[-1] != command):
            self.history.append(command)
            if len(self.history) > self.max_size:
                self.history.pop(0)
            self.save_history()
    
    def search(self, pattern):
        """Recherche dans l'historique"""
        return [cmd for cmd in self.history if pattern.lower() in cmd.lower()]
    
    def clear(self):
        """Efface l'historique"""
        self.history = []
        self.save_history()

# ==================== AUTOCOMPLETE ====================

class GSQLCompleter:
    """Auto-complétion pour GSQL"""
    
    def __init__(self, database):
        self.database = database
        self.keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
            'ALTER', 'ADD', 'COLUMN', 'INDEX', 'VIEW', 'TRIGGER',
            'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
            'SHOW', 'DESCRIBE', 'EXPLAIN', 'HELP', 'STATS',
            'VACUUM', 'BACKUP', 'RESTORE', 'EXIT', 'QUIT'
        ]
        
        self.tables = []
        self.functions = []
        self.update_suggestions()
    
    def update_suggestions(self):
        """Met à jour les suggestions depuis la base"""
        try:
            # Récupérer les tables
            result = self.database.execute("SHOW TABLES")
            if result.get('success'):
                self.tables = [t['table'] for t in result.get('tables', [])]
            
            # Récupérer les fonctions
            result = self.database.execute("SHOW FUNCTIONS")
            if result.get('success'):
                self.functions = [f['name'] for f in result.get('functions', [])]
                
        except Exception:
            pass
    
    def complete(self, text, state):
        """Fonction de complétion pour readline"""
        buffer = readline.get_line_buffer()
        
        # Complétion par mot-clé
        if state == 0:
            self.matches = [
                kw for kw in self.keywords 
                if kw.lower().startswith(text.lower())
            ]
        
        # Complétion par table
        if not self.matches and buffer.upper().endswith('FROM '):
            if state == 0:
                self.matches = [
                    tbl for tbl in self.tables 
                    if tbl.lower().startswith(text.lower())
                ]
        
        # Complétion par fonction
        if not self.matches and '(' in buffer:
            if state == 0:
                self.matches = [
                    func for func in self.functions 
                    if func.lower().startswith(text.lower())
                ]
        
        # Complétion par colonne (basique)
        if not self.matches and buffer.upper().count('SELECT') > 0 and buffer.upper().count('FROM') == 0:
            # On pourrait ajouter la logique pour suggérer les colonnes
            pass
        
        if state < len(self.matches):
            return self.matches[state]
        else:
            return None

# ==================== MULTILINE INPUT ====================

class MultilineInput:
    """Gestion de l'entrée multiligne"""
    
    def __init__(self, prompt="gsql> ", continuation="   -> "):
        self.prompt = prompt
        self.continuation = continuation
        self.buffer = []
        self.in_multiline = False
        
    def read(self):
        """Lit une entrée multiligne"""
        self.buffer = []
        
        # Première ligne
        line = input(self.prompt).strip()
        self.buffer.append(line)
        
        # Vérifier si multiligne (se termine par un backslash)
        if line.endswith('\\'):
            self.in_multiline = True
            line = line[:-1].strip()
            
            # Lire les lignes supplémentaires
            while True:
                try:
                    cont_line = input(self.continuation).strip()
                    if cont_line.endswith('\\'):
                        self.buffer.append(cont_line[:-1].strip())
                    else:
                        self.buffer.append(cont_line)
                        self.in_multiline = False
                        break
                except EOFError:
                    break
        else:
            self.in_multiline = False
        
        # Reconstituer la requête
        query = ' '.join(self.buffer)
        return query.strip()
    
    def reset(self):
        """Réinitialise le buffer"""
        self.buffer = []
        self.in_multiline = False

# ==================== MAIN GSQL SHELL ====================

class GSQLShell(cmd.Cmd):
    """Shell interactif GSQL"""
    
    intro = Colors.info("""
    ╔══════════════════════════════════════════════════╗
    ║                 GSQL v3.0                        ║
    ║  SQL Database with Natural Language Interface    ║
    ╚══════════════════════════════════════════════════╝
    
    Type SQL commands, natural language, or 'help' for help.
    Type 'exit' or 'quit' to exit.
    
    """)
    
    prompt = Colors.sql('gsql> ')
    ruler = Colors.dim('─')
    
    def __init__(self, database_path=None, config=None):
        super().__init__()
        
        # Configuration
        self.config = config or Config.load()
        
        # Initialiser la base de données
        self.db = None
        self.executor = None
        self.function_manager = None
        self.nlp_translator = None
        
        # Composants UI
        self.history = CommandHistory(
            self.config['cli']['history_file'],
            self.config['cli']['max_history']
        )
        
        self.multiline_input = MultilineInput(
            self.config['cli']['prompt'],
            Colors.dim('   -> ')
        )
        
        self.completer = None
        self.formatter = TableFormatter()
        
        # État
        self.current_transaction = None
        self.start_time = datetime.now()
        self.query_count = 0
        self.error_count = 0
        
        # Initialiser
        self._initialize(database_path)
        
        # Configurer readline
        if self.config['cli']['autocomplete'] and sys.platform != 'win32':
            self._setup_readline()
    
    def _initialize(self, database_path=None):
        """Initialise les composants GSQL"""
        try:
            print(Colors.info("Initializing GSQL..."))
            
            # Créer la base de données
            db_config = self.config['database'].copy()
            if database_path:
                db_config['path'] = database_path
            
            self.db = create_database(**db_config)
            
            # Créer l'exécuteur - version corrigée sans auto_translate_nl
            self.executor = create_executor(
                storage=self.db.storage,
                enable_nlp=self.config['executor'].get('enable_nlp', True),
                enable_learning=self.config['executor'].get('enable_learning', False)
                # auto_translate_nl supprimé car non supporté par QueryExecutor
            )
            
            # Initialiser les autres composants
            self.function_manager = FunctionManager()
            self.nlp_translator = NLToSQLTranslator()
            
            # Configurer l'auto-complétion
            self.completer = GSQLCompleter(self.db)
            
            print(Colors.success("✓ GSQL ready!"))
            print(Colors.dim(f"Database: {self.db.storage.db_path}"))
            print(Colors.dim(f"Type 'help' for commands\n"))
            
        except Exception as e:
            print(Colors.error(f"Failed to initialize GSQL: {e}"))
            traceback.print_exc()
            sys.exit(1)
    
    def _setup_readline(self):
        """Configure readline pour l'auto-complétion"""
        if sys.platform != 'win32':
            try:
                import readline
                readline.parse_and_bind("tab: complete")
                readline.set_completer(self.completer.complete)
                
                # Personnaliser le comportement de readline
                readline.set_completer_delims(' \t\n`~!@#$%^&*()-=+[{]}\\|;:\'",<>/?')
                
            except ImportError:
                print(Colors.warning("Readline not available, auto-completion disabled"))
    
    # ==================== COMMAND EXECUTION ====================
    
    def default(self, line):
        """Exécute une commande (SQL, NL ou GSQL)"""
        if not line.strip():
            return
        
        # Ajouter à l'historique
        self.history.add(line)
        
        # Mettre à jour les suggestions d'auto-complétion
        if self.completer:
            self.completer.update_suggestions()
        
        # Exécuter la commande
        self._execute_command(line)
    
    def _execute_command(self, command):
        """Exécute une commande et affiche le résultat"""
        start_time = time.time()
        self.query_count += 1
        
        try:
            # Traitement spécial pour certaines commandes
            if command.lower() in ['exit', 'quit', '\\q']:
                self.do_exit('')
                return
            
            # Exécuter via l'exécuteur
            result = self.executor.execute(
                command,
                use_nlp=self.config['executor']['auto_translate_nl'],
                use_cache=self.config['executor']['enable_cache']
            )
            
            # Afficher le résultat
            self._display_result(result, command, start_time)
            
            # Mettre à jour les statistiques
            if not result.get('success'):
                self.error_count += 1
            
        except KeyboardInterrupt:
            print(Colors.warning("\nQuery cancelled"))
            
        except Exception as e:
            self.error_count += 1
            self._display_error(command, e, start_time)
    
    def _display_result(self, result, command, start_time):
        """Affiche le résultat d'une commande"""
        exec_time = time.time() - start_time
        
        # Afficher le temps d'exécution si configuré
        if self.config['cli']['show_time']:
            time_str = Colors.dim(f"({exec_time:.3f}s)")
            print(f"\n{time_str}")
        
        # Gérer les différents types de résultats
        if result.get('type') == 'error':
            self._display_error_result(result)
            
        elif result.get('type') == 'select':
            self._display_select_result(result)
            
        elif result.get('type') == 'show_tables':
            self._display_show_tables(result)
            
        elif result.get('type') == 'show_functions':
            self._display_show_functions(result)
            
        elif result.get('type') == 'describe':
            self._display_describe(result)
            
        elif result.get('type') == 'stats':
            self._display_stats(result)
            
        elif result.get('type') == 'help':
            print(result.get('message', ''))
            
        elif result.get('type') == 'vacuum':
            print(Colors.success(result.get('message', 'Vacuum completed')))
            
        elif result.get('type') == 'backup':
            print(Colors.success(result.get('message', 'Backup completed')))
            
        elif result.get('type') == 'transaction':
            print(Colors.info(result.get('message', '')))
            if 'tid' in result:
                self.current_transaction = result['tid']
            
        elif result.get('type') == 'command':
            rows_affected = result.get('rows_affected', 0)
            if rows_affected > 0:
                print(Colors.success(f"{rows_affected} row(s) affected"))
            else:
                print(Colors.info("Command executed successfully"))
        
        # Afficher le message supplémentaire si présent
        if 'message' in result and result['type'] not in ['help', 'show_tables']:
            print(Colors.info(f"Note: {result['message']}"))
    
    def _display_error_result(self, result):
        """Affiche une erreur formatée"""
        error_type = result.get('error_type', 'ERROR')
        error_msg = result.get('error_message', 'Unknown error')
        suggestion = result.get('suggestion', '')
        
        print(Colors.error(f"\n╔══ {error_type} ══╗"))
        print(Colors.error(f"║ {error_msg}"))
        if suggestion:
            print(Colors.warning(f"║ Suggestion: {suggestion}"))
        print(Colors.error("╚══════════════════════════════════╝"))
    
    def _display_select_result(self, result):
        """Affiche le résultat d'un SELECT"""
        rows = result.get('rows', [])
        columns = result.get('columns', [])
        count = result.get('count', 0)
        
        if count == 0:
            print(Colors.info("No rows returned"))
            return
        
        # Formater le tableau
        table = self.formatter.format_table(
            rows, 
            columns,
            max_rows=self.config['display']['max_rows'],
            truncate=self.config['display']['truncate_length']
        )
        
        print(f"\n{table}")
        
        # Afficher les métadonnées si disponible
        if 'metadata' in result:
            meta = result['metadata']
            if meta.get('translated_query'):
                print(Colors.dim(f"Translated from: {meta['original_query']}"))
    
    def _display_show_tables(self, result):
        """Affiche la liste des tables"""
        tables = result.get('tables', [])
        count = result.get('count', 0)
        
        if count == 0:
            print(Colors.info("No tables found"))
            return
        
        print(Colors.table(f"\nTables ({count}):"))
        print(Colors.dim("─" * 60))
        
        for i, table in enumerate(tables, 1):
            table_name = Colors.table(table['table'])
            rows = Colors.value(str(table.get('rows', 0)))
            columns = ', '.join(table.get('columns', []))[:40]
            
            print(f"{i:3}. {table_name:20} {rows:>8} rows")
            if columns:
                print(f"     Columns: {Colors.dim(columns)}")
        
        print()
    
    def _display_show_functions(self, result):
        """Affiche la liste des fonctions"""
        functions = result.get('functions', [])
        count = result.get('count', 0)
        
        if count == 0:
            print(Colors.info("No functions found"))
            return
        
        print(Colors.table(f"\nFunctions ({count}):"))
        print(Colors.dim("─" * 60))
        
        # Séparer les fonctions intégrées et utilisateur
        builtin = [f for f in functions if f.get('type') == 'builtin']
        user = [f for f in functions if f.get('type') == 'user']
        
        if builtin:
            print(Colors.info("\nBuilt-in functions:"))
            for i, func in enumerate(builtin, 1):
                name = Colors.column(func['name'])
                desc = func.get('description', '')
                print(f"{i:3}. {name:30} {Colors.dim(desc)}")
        
        if user:
            print(Colors.info("\nUser-defined functions:"))
            for i, func in enumerate(user, 1):
                name = Colors.column(func['name'])
                params = ', '.join(func.get('params', []))
                returns = func.get('returns', '')
                created = func.get('created_at', '')
                
                sig = f"{name}({params}) -> {returns}"
                print(f"{i:3}. {sig:40}")
                if created:
                    print(f"     Created: {Colors.dim(created)}")
        
        print()
    
    def _display_describe(self, result):
        """Affiche la description d'une table"""
        table = result.get('table', '')
        columns = result.get('columns', [])
        indexes = result.get('indexes', [])
        
        print(Colors.table(f"\nStructure of table '{table}':"))
        print(Colors.dim("─" * 80))
        
        # Afficher les colonnes
        print(Colors.info("\nColumns:"))
        for col in columns:
            field = Colors.column(col['field'])
            col_type = col['type']
            null = "YES" if col['null'] else "NO"
            key = col['key']
            default = col['default'] or ''
            extra = col['extra']
            
            line = f"  {field:20} {col_type:15} {null:6} {key:8} {default:15} {extra}"
            print(line)
        
        # Afficher les index
        if indexes:
            print(Colors.info("\nIndexes:"))
            for idx in indexes:
                name = idx.get('name', '')
                unique = "UNIQUE" if idx.get('unique') else ""
                print(f"  {name:30} {unique}")
        
        print()
    
    def _display_stats(self, result):
        """Affiche les statistiques"""
        database_stats = result.get('database', {})
        storage_stats = result.get('storage', {})
        
        print(Colors.table("\nGSQL Statistics:"))
        print(Colors.dim("─" * 50))
        
        # Statistiques de la session
        uptime = datetime.now() - self.start_time
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        print(Colors.info("\nSession:"))
        print(f"  Uptime:           {hours:02d}:{minutes:02d}:{seconds:02d}")
        print(f"  Queries executed: {self.query_count}")
        print(f"  Errors:           {self.error_count}")
        print(f"  Success rate:     {(self.query_count - self.error_count) / self.query_count * 100:.1f}%" 
              if self.query_count > 0 else "  Success rate:     0%")
        
        # Statistiques de la base
        if database_stats:
            print(Colors.info("\nDatabase:"))
            for key, value in database_stats.items():
                if isinstance(value, (int, float)):
                    print(f"  {key:20} {value:,}")
                else:
                    print(f"  {key:20} {value}")
        
        # Statistiques du cache
        if storage_stats and 'buffer_pool' in storage_stats:
            bp = storage_stats['buffer_pool']
            print(Colors.info("\nBuffer Pool:"))
            print(f"  Size:             {bp.get('size', 0)}/{bp.get('max_size', 0)} pages")
            print(f"  Hit ratio:        {bp.get('hit_ratio', 0)*100:.1f}%")
        
        print()
    
    def _display_error(self, command, error, start_time):
        """Affiche une erreur d'exécution"""
        exec_time = time.time() - start_time
        
        error_type = type(error).__name__
        error_msg = str(error)
        
        print(Colors.error(f"\n╔══ {error_type} ══╗"))
        print(Colors.error(f"║ {error_msg}"))
        print(Colors.error(f"║ Query: {command[:100]}"))
        print(Colors.error(f"║ Time:  {exec_time:.3f}s"))
        print(Colors.error("╚══════════════════════════════════╝"))
        
        # Mode verbose
        if self.config['cli']['verbose_errors'] and not isinstance(error, KeyboardInterrupt):
            print(Colors.dim("\nTraceback:"))
            traceback.print_exc()
    
    # ==================== BUILT-IN COMMANDS ====================
    
    def do_help(self, arg):
        """Affiche l'aide"""
        help_text = """
GSQL Commands:

SQL COMMANDS:
  SELECT * FROM table [WHERE condition] [LIMIT n]
  INSERT INTO table (columns) VALUES (values)
  UPDATE table SET column=value [WHERE condition]
  DELETE FROM table [WHERE condition]
  CREATE TABLE name (column TYPE, ...)
  DROP TABLE name
  BEGIN [TRANSACTION]
  COMMIT
  ROLLBACK

GSQL SPECIAL COMMANDS:
  SHOW TABLES                    - List all tables
  DESCRIBE table                 - Show table structure
  SHOW FUNCTIONS                 - List all functions
  STATS                          - Show database statistics
  VACUUM                         - Optimize database
  BACKUP [path]                  - Create backup
  HISTORY                        - Show command history
  CONFIG                         - Show configuration
  CLEAR                          - Clear screen
  EXIT / QUIT                    - Exit GSQL

NATURAL LANGUAGE EXAMPLES:
  "show me all users"            -> SELECT * FROM users
  "create a products table"      -> CREATE TABLE products (...)
  "how many customers in Paris"  -> SELECT COUNT(*) FROM customers WHERE city='Paris'
  "describe users table"         -> DESCRIBE users

DOT COMMANDS (SQLite compatible):
  .tables                        - List tables
  .schema [table]                - Show schema
  .indexes [table]               - Show indexes
  .stats                         - Show statistics
  .backup [file]                 - Create backup
  .vacuum                        - Optimize
  .help                          - Show help
  .exit /.quit                   - Exit

Type any SQL command to execute it.
        """
        print(help_text)
    
    def do_history(self, arg):
        """Affiche l'historique des commandes"""
        args = shlex.split(arg)
        
        if args and args[0] == 'clear':
            self.history.clear()
            print(Colors.success("History cleared"))
            return
        
        # Afficher l'historique
        history = self.history.history
        
        if not history:
            print(Colors.info("No history"))
            return
        
        # Limiter si spécifié
        limit = int(args[0]) if args and args[0].isdigit() else len(history)
        display = history[-limit:]
        
        print(Colors.table(f"\nCommand History (last {len(display)}):"))
        print(Colors.dim("─" * 80))
        
        for i, cmd in enumerate(display, 1):
            print(f"{i:4}. {cmd[:100]}{'...' if len(cmd) > 100 else ''}")
        
        print()
    
    def do_config(self, arg):
        """Affiche ou modifie la configuration"""
        args = shlex.split(arg)
        
        if not args:
            # Afficher la configuration
            print(Colors.table("\nCurrent Configuration:"))
            print(Colors.dim("─" * 50))
            
            for section, settings in self.config.items():
                print(Colors.info(f"\n[{section.upper()}]"))
                for key, value in settings.items():
                    if isinstance(value, dict):
                        print(f"  {key}:")
                        for k, v in value.items():
                            print(f"    {k}: {v}")
                    else:
                        print(f"  {key}: {value}")
            
            print()
            return
        
        # Modification de configuration
        # Format: config section.key value
        if len(args) >= 2:
            try:
                parts = args[0].split('.')
                if len(parts) == 2:
                    section, key = parts
                    
                    if section in self.config and key in self.config[section]:
                        # Convertir le type si nécessaire
                        old_value = self.config[section][key]
                        if isinstance(old_value, bool):
                            new_value = args[1].lower() in ['true', 'yes', '1', 'on']
                        elif isinstance(old_value, int):
                            new_value = int(args[1])
                        elif isinstance(old_value, float):
                            new_value = float(args[1])
                        else:
                            new_value = args[1]
                        
                        self.config[section][key] = new_value
                        print(Colors.success(f"Configuration updated: {section}.{key} = {new_value}"))
                        
                        # Appliquer les changements dynamiques
                        self._apply_config_changes(section, key, new_value)
                    else:
                        print(Colors.error(f"Invalid config key: {section}.{key}"))
                else:
                    print(Colors.error("Format: config section.key value"))
                    
            except ValueError as e:
                print(Colors.error(f"Invalid value: {e}"))
            except Exception as e:
                print(Colors.error(f"Error updating config: {e}"))
    
    def _apply_config_changes(self, section, key, value):
        """Applique les changements de configuration dynamiquement"""
        if section == 'cli' and key == 'prompt':
            self.prompt = Colors.sql(value + ' ')
        elif section == 'executor' and key in ['cache_size', 'timeout']:
            if self.executor:
                self.executor.configure(**{key: value})
    
    def do_clear(self, arg):
        """Efface l'écran"""
        if sys.platform == 'win32':
            os.system('cls')
        else:
            os.system('clear')
    
    def do_source(self, arg):
        """Exécute un fichier de commandes SQL"""
        if not arg:
            print(Colors.error("Usage: source <filename>"))
            return
        
        try:
            filepath = Path(arg).expanduser()
            if not filepath.exists():
                print(Colors.error(f"File not found: {arg}"))
                return
            
            with open(filepath, 'r', encoding='utf-8') as f:
                commands = []
                current_cmd = []
                
                for line in f:
                    line = line.strip()
                    if line.startswith('--') or not line:
                        continue
                    
                    current_cmd.append(line)
                    if line.endswith(';'):
                        commands.append(' '.join(current_cmd)[:-1])  # Retirer le ;
                        current_cmd = []
                
                # Exécuter les commandes restantes
                if current_cmd:
                    commands.append(' '.join(current_cmd))
            
            # Exécuter en batch
            if commands:
                print(Colors.info(f"Executing {len(commands)} commands from {filepath}"))
                batch_result = self.executor.execute_batch(commands, stop_on_error=False)
                
                successful = batch_result['successful']
                failed = batch_result['failed']
                
                if successful > 0:
                    print(Colors.success(f"✓ {successful} command(s) executed successfully"))
                if failed > 0:
                    print(Colors.error(f"✗ {failed} command(s) failed"))
                
                print()
            
        except Exception as e:
            print(Colors.error(f"Error sourcing file: {e}"))
    
    def do_save(self, arg):
        """Sauvegarde les commandes dans un fichier"""
        args = shlex.split(arg)
        
        if not args:
            print(Colors.error("Usage: save <filename> [num_commands]"))
            return
        
        filename = args[0]
        num_cmds = int(args[1]) if len(args) > 1 and args[1].isdigit() else len(self.history.history)
        
        try:
            filepath = Path(filename).expanduser()
            commands = self.history.history[-num_cmds:]
            
            with open(filepath, 'w', encoding='utf-8') as f:
                for cmd in commands:
                    f.write(cmd + ';\n')
            
            print(Colors.success(f"Saved {len(commands)} commands to {filepath}"))
            
        except Exception as e:
            print(Colors.error(f"Error saving to file: {e}"))
    
    # ==================== TRANSACTION COMMANDS ====================
    
    def do_begin(self, arg):
        """Démarre une transaction"""
        isolation = arg.upper() if arg else "DEFERRED"
        result = self.db.begin_transaction(isolation)
        self._display_result(result, f"BEGIN {isolation}", time.time())
    
    def do_commit(self, arg):
        """Valide la transaction en cours"""
        if self.current_transaction is None:
            print(Colors.warning("No active transaction"))
            return
        
        result = self.db.commit_transaction(self.current_transaction)
        self._display_result(result, "COMMIT", time.time())
        self.current_transaction = None
    
    def do_rollback(self, arg):
        """Annule la transaction en cours"""
        if self.current_transaction is None:
            print(Colors.warning("No active transaction"))
            return
        
        result = self.db.rollback_transaction(self.current_transaction)
        self._display_result(result, "ROLLBACK", time.time())
        self.current_transaction = None
    
    # ==================== DOT COMMANDS ====================
    
    def do_dot(self, arg):
        """Exécute les commandes pointées (compatibilité SQLite)"""
        if not arg:
            return
        
        # Mapping des commandes pointées
        dot_commands = {
            'tables': 'SHOW TABLES',
            'schema': lambda a: f"DESCRIBE {a}" if a else "SHOW SCHEMA",
            'indexes': lambda a: f"SHOW INDEXES FROM {a}" if a else "SHOW INDEXES",
            'stats': 'STATS',
            'help': 'HELP',
            'backup': lambda a: f"BACKUP {a}" if a else "BACKUP",
            'vacuum': 'VACUUM',
            'exit': 'EXIT',
            'quit': 'EXIT',
            'timer': self._toggle_timer,
            'headers': self._toggle_headers,
            'nullvalue': self._set_nullvalue,
            'mode': self._set_output_mode
        }
        
        cmd_parts = arg.split()
        cmd_name = cmd_parts[0]
        cmd_args = ' '.join(cmd_parts[1:]) if len(cmd_parts) > 1 else ''
        
        if cmd_name in dot_commands:
            handler = dot_commands[cmd_name]
            
            if callable(handler):
                if cmd_name in ['timer', 'headers', 'nullvalue', 'mode']:
                    handler(cmd_args)
                else:
                    sql_cmd = handler(cmd_args)
                    self._execute_command(sql_cmd)
            else:
                self._execute_command(handler)
        else:
            print(Colors.error(f"Unknown dot command: .{cmd_name}"))
            print(Colors.info("Try: .tables, .schema, .help"))
    
    def _toggle_timer(self, arg):
        """Active/désactive le timer"""
        current = self.config['cli']['show_time']
        self.config['cli']['show_time'] = not current
        status = "ON" if not current else "OFF"
        print(Colors.success(f"Timer {status}"))
    
    def _toggle_headers(self, arg):
        """Active/désactive les en-têtes"""
        current = self.config['display']['show_row_numbers']
        self.config['display']['show_row_numbers'] = not current
        status = "ON" if not current else "OFF"
        print(Colors.success(f"Headers {status}"))
    
    def _set_nullvalue(self, arg):
        """Définit l'affichage des valeurs NULL"""
        if arg:
            self.config['display']['null_display'] = arg
            print(Colors.success(f"NULL display set to: {arg}"))
        else:
            print(Colors.info(f"Current NULL display: {self.config['display']['null_display']}"))
    
    def _set_output_mode(self, arg):
        """Définit le mode de sortie"""
        modes = ['table', 'csv', 'line', 'column', 'json']
        if arg in modes:
            # Implémenter le changement de mode
            print(Colors.success(f"Output mode set to: {arg}"))
        else:
            print(Colors.error(f"Invalid mode. Available: {', '.join(modes)}"))
    
    # ==================== OVERRIDES ====================
    
    def emptyline(self):
        """Ne rien faire sur ligne vide"""
        pass
    
    def precmd(self, line):
        """Traitement avant l'exécution de la commande"""
        # Traiter les commandes pointées
        if line.startswith('.'):
            self.do_dot(line[1:])
            return ''
        
        return line
    
    def postcmd(self, stop, line):
        """Traitement après l'exécution de la commande"""
        return stop
    
    def do_exit(self, arg):
        """Quitte GSQL"""
        print(Colors.info("\nClosing GSQL..."))
        
        # Afficher les statistiques de la session
        uptime = datetime.now() - self.start_time
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        print(Colors.table(f"\nSession Summary:"))
        print(Colors.dim("─" * 40))
        print(f"Duration:    {hours:02d}:{minutes:02d}:{seconds:02d}")
        print(f"Queries:     {self.query_count}")
        print(f"Errors:      {self.error_count}")
        success_rate = (self.query_count - self.error_count) / self.query_count * 100 if self.query_count > 0 else 0
        print(f"Success:     {success_rate:.1f}%")
        print()
        
        # Fermer proprement
        if self.db:
            self.db.close()
        
        print(Colors.success("Goodbye! 👋\n"))
        return True
    
    def do_quit(self, arg):
        """Alias pour exit"""
        return self.do_exit(arg)
    
    def do_EOF(self, arg):
        """Gestion de Ctrl+D"""
        print()
        return self.do_exit(arg)
    
    def precmd(self, line):
        """Log les commandes en mode debug"""
        if line and not line.startswith('.'):
            logger.debug(f"Command: {line}")
        return line

# ==================== SIGNAL HANDLING ====================

def signal_handler(sig, frame):
    """Gère les signaux d'interruption"""
    print(Colors.warning("\n\nInterrupted. Type 'exit' to quit or continue with commands."))
    sys.stdout.write(GSQLShell.prompt)
    sys.stdout.flush()

# ==================== MAIN FUNCTION ====================

def main():
    """Fonction principale de GSQL"""
    import argparse
    
    # Parser d'arguments
    parser = argparse.ArgumentParser(
        description='GSQL - SQL Database with Natural Language Interface',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  gsql                            # Interactive mode
  gsql mydatabase.db              # Open specific database
  gsql -e "SELECT * FROM users"   # Execute single command
  gsql -f script.sql              # Execute SQL file
  gsql --version                  # Show version
        
Natural Language Examples:
  "show tables"                   # List all tables
  "describe users"                # Show table structure
  "users in paris"                # SELECT * FROM users WHERE city='paris'
  "count products"                # SELECT COUNT(*) FROM products
        """
    )
    
    parser.add_argument('database', nargs='?', 
                       help='Database file (default: auto)')
    
    parser.add_argument('-e', '--execute', 
                       help='Execute single SQL command and exit')
    
    parser.add_argument('-f', '--file',
                       help='Execute SQL commands from file and exit')
    
    parser.add_argument('--version', action='store_true',
                       help='Show version information')
    
    parser.add_argument('--no-nlp', action='store_true',
                       help='Disable natural language processing')
    
    parser.add_argument('--no-color', action='store_true',
                       help='Disable colored output')
    
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    
    parser.add_argument('--config', 
                       help='Custom configuration file')
    
    args = parser.parse_args()
    
    # Afficher la version
    if args.version:
        from . import __version__
        print(f"GSQL v{__version__}")
        print("SQL Database with Natural Language Interface")
        return
    
    # Charger la configuration
    config = Config.load()
    
    # Appliquer les arguments de ligne de commande
    if args.no_nlp:
        config['executor']['auto_translate_nl'] = False
    
    if args.no_color:
        config['cli']['colors'] = False
    
    if args.verbose:
        config['logging']['level'] = 'DEBUG'
        config['cli']['verbose_errors'] = True
    
    if args.config:
        try:
            with open(args.config, 'r') as f:
                user_config = json.load(f)
                Config._deep_update(config, user_config)
        except Exception as e:
            print(f"Warning: Could not load config file: {e}")
    
    # Configurer le logging
    import logging as loglib
    log_level = getattr(loglib, config['logging']['level'].upper(), loglib.INFO)
    loglib.basicConfig(
        level=log_level,
        format=config['logging']['format'],
        filename=config['logging']['file']
    )
    
    # Mode non-interactif
    if args.execute or args.file:
        try:
            # Créer la base de données
            db = create_database(args.database, **config['database'])
            executor = create_executor(storage=db.storage, **config['executor'])
            
            if args.execute:
                # Exécuter une commande unique
                result = executor.execute(args.execute)
                
                if result.get('success'):
                    if result.get('type') == 'select':
                        formatter = TableFormatter()
                        print(formatter.format_table(
                            result.get('rows', []),
                            result.get('columns', []),
                            max_rows=config['display']['max_rows'],
                            truncate=config['display']['truncate_length']
                        ))
                    else:
                        print(Colors.success(result.get('message', 'Command executed')))
                else:
                    print(Colors.error(result.get('error_message', 'Error')))
                    sys.exit(1)
            
            elif args.file:
                # Exécuter un fichier
                with open(args.file, 'r') as f:
                    commands = [cmd.strip() for cmd in f.read().split(';') if cmd.strip()]
                
                batch_result = executor.execute_batch(commands, stop_on_error=False)
                
                successful = batch_result['successful']
                failed = batch_result['failed']
                
                if successful > 0:
                    print(Colors.success(f"✓ {successful} command(s) executed"))
                if failed > 0:
                    print(Colors.error(f"✗ {failed} command(s) failed"))
                    sys.exit(1)
            
            db.close()
            
        except Exception as e:
            print(Colors.error(f"Error: {e}"))
            sys.exit(1)
        
        return
    
    # Mode interactif
    try:
        # Configurer les handlers de signal
        signal.signal(signal.SIGINT, signal_handler)
        
        # Créer et lancer le shell
        shell = GSQLShell(args.database, config)
        shell.cmdloop()
        
    except KeyboardInterrupt:
        print(Colors.warning("\n\nInterrupted"))
        sys.exit(130)
        
    except Exception as e:
        print(Colors.error(f"Fatal error: {e}"))
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
