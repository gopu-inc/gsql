#!/usr/bin/env python3
"""
GSQL - SQL Database with Natural Language Interface
Main Entry Point: Interactive Shell and CLI
Version: 3.0 - SQLite Only
"""

import argparse
import atexit
import cmd
import json
import logging
import os
import re
import readline
import signal
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# ==================== IMPORTS ====================

try:
    from . import __version__, config, setup_logging
    from .database import create_database, Database, connect
    from .executor import create_executor, QueryExecutor
    from .functions import FunctionManager
    from .storage import SQLiteStorage
    
    # NLP avec fallback
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

# ==================== CONSTANTS ====================

DEFAULT_CONFIG = {
    'database': {
        'base_dir': str(Path.home() / '.gsql'),
        'auto_recovery': True,
        'buffer_pool_size': 100,
        'enable_wal': True
    },
    'executor': {
        'enable_nlp': False,
        'enable_learning': False
    },
    'shell': {
        'prompt': 'gsql> ',
        'history_file': '.gsql_history',
        'max_history': 1000,
        'colors': True,
        'autocomplete': True
    }
}

SQL_KEYWORDS = [
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

DOT_COMMANDS = [
    '.tables', '.schema', '.stats', '.help', '.backup',
    '.vacuum', '.exit', '.quit', '.clear', '.history'
]

# ==================== LOGGING ====================

logger = logging.getLogger(__name__)

# ==================== ANSI COLORS ====================

class Colors:
    """ANSI color codes for terminal output"""
    
    # Styles
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    UNDERLINE = '\033[4m'
    REVERSE = '\033[7m'
    
    # Foreground colors
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    # Background colors
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    
    @staticmethod
    def colorize(text: str, color_code: str) -> str:
        """Apply color to text"""
        return f"{color_code}{text}{Colors.RESET}"
    
    @staticmethod
    def success(text: str) -> str:
        return Colors.colorize(text, Colors.GREEN)
    
    @staticmethod
    def error(text: str) -> str:
        return Colors.colorize(text, Colors.RED)
    
    @staticmethod
    def warning(text: str) -> str:
        return Colors.colorize(text, Colors.YELLOW)
    
    @staticmethod
    def info(text: str) -> str:
        return Colors.colorize(text, Colors.CYAN)
    
    @staticmethod
    def highlight(text: str) -> str:
        return Colors.colorize(text, Colors.BOLD)
    
    @staticmethod
    def dim(text: str) -> str:
        return Colors.colorize(text, Colors.DIM)
    
    @staticmethod
    def sql_keyword(text: str) -> str:
        return Colors.colorize(text, Colors.MAGENTA)
    
    @staticmethod
    def sql_string(text: str) -> str:
        return Colors.colorize(text, Colors.YELLOW)
    
    @staticmethod
    def sql_number(text: str) -> str:
        return Colors.colorize(text, Colors.CYAN)
    
    @staticmethod
    def sql_comment(text: str) -> str:
        return Colors.colorize(text, Colors.GREEN)

# ==================== AUTOCOMPLETER ====================

class GSQLCompleter:
    """Autocompletion for GSQL shell"""
    
    def __init__(self, database: Optional[Database] = None):
        self.database = database
        self.keywords = SQL_KEYWORDS
        self.gsql_commands = DOT_COMMANDS
        self.table_names: List[str] = []
        self.column_names: Dict[str, List[str]] = {}
        
        if database and hasattr(database, 'storage'):
            self.refresh_schema()
    
    def refresh_schema(self) -> None:
        """Refresh schema information from database"""
        try:
            if not self.database:
                return
                
            # Get tables
            result = self.database.execute("SHOW TABLES")
            if result.get('success'):
                self.table_names = [
                    table['table'] 
                    for table in result.get('tables', [])
                ]
            
            # Get columns for each table
            self.column_names.clear()
            for table in self.table_names:
                try:
                    result = self.database.execute(f"DESCRIBE {table}")
                    if result.get('success'):
                        self.column_names[table] = [
                            col['field'] 
                            for col in result.get('columns', [])
                        ]
                except Exception:
                    continue
                    
        except Exception:
            logger.debug("Failed to refresh schema", exc_info=True)
    
    def complete(self, text: str, state: int) -> Optional[str]:
        """Completion function for readline"""
        if state == 0:
            line = readline.get_line_buffer()
            tokens = line.strip().split()
            
            if not tokens or len(tokens) == 1:
                # Command completion
                all_commands = self.keywords + self.gsql_commands + self.table_names
                self.matches = [
                    cmd for cmd in all_commands
                    if cmd.lower().startswith(text.lower())
                ]
            elif tokens[-2].upper() in ('FROM', 'INTO'):
                # Table completion
                self.matches = [
                    table for table in self.table_names
                    if table.lower().startswith(text.lower())
                ]
            elif tokens[-2].upper() in ('WHERE', 'SET'):
                # Column completion
                table = self._find_current_table(tokens)
                if table in self.column_names:
                    self.matches = [
                        col for col in self.column_names[table]
                        if col.lower().startswith(text.lower())
                    ]
                else:
                    self.matches = []
            else:
                self.matches = []
        
        try:
            return self.matches[state] if self.matches else None
        except (IndexError, AttributeError):
            return None
    
    @staticmethod
    def _find_current_table(tokens: List[str]) -> Optional[str]:
        """Find current table from tokens"""
        for i, token in enumerate(tokens):
            token_upper = token.upper()
            if i + 1 < len(tokens):
                if token_upper in ('FROM', 'UPDATE'):
                    return tokens[i + 1]
                elif token_upper == 'INTO':
                    # Skip table name if we have INSERT INTO table
                    if i > 0 and tokens[i-1].upper() == 'INSERT':
                        return tokens[i + 1]
        return None

# ==================== RESULT DISPLAY ====================

class ResultDisplay:
    """Handle display of query results"""
    
    MAX_ROWS_DISPLAY = 50
    
    @staticmethod
    def display_select(result: Dict, execution_time: float) -> None:
        """Display SELECT query results"""
        rows = result.get('rows', [])
        columns = result.get('columns', [])
        count = result.get('count', 0)
        
        if count == 0:
            print(Colors.warning("No rows returned"))
            return
        
        # Display header
        header = " | ".join(Colors.highlight(col) for col in columns)
        separator = Colors.dim('─' * len(header))
        print(f"{header}\n{separator}")
        
        # Display data (limited)
        for i, row in enumerate(rows[:ResultDisplay.MAX_ROWS_DISPLAY]):
            if isinstance(row, (list, tuple)):
                values = [str(v) if v is not None else "NULL" for v in row]
            else:
                values = [str(row)]
            
            # Color values
            colored_values = []
            for val in values:
                if val == "NULL":
                    colored_values.append(Colors.dim(val))
                elif val.replace('.', '', 1).isdigit() and val.count('.') <= 1:
                    colored_values.append(Colors.sql_number(val))
                elif val.startswith("'") and val.endswith("'"):
                    colored_values.append(Colors.sql_string(val))
                else:
                    colored_values.append(val)
            
            print(" | ".join(colored_values))
        
        # Show truncation notice
        if len(rows) > ResultDisplay.MAX_ROWS_DISPLAY:
            remaining = len(rows) - ResultDisplay.MAX_ROWS_DISPLAY
            print(Colors.dim(f"... and {remaining} more rows"))
        
        print(Colors.dim(f"\n{count} row(s) returned"))
    
    @staticmethod
    def display_insert(result: Dict) -> None:
        """Display INSERT query results"""
        print(Colors.success(f"Row inserted (ID: {result.get('last_insert_id', 'N/A')})"))
        print(Colors.dim(f"Rows affected: {result.get('rows_affected', 0)}"))
    
    @staticmethod
    def display_update_delete(result: Dict) -> None:
        """Display UPDATE/DELETE query results"""
        print(Colors.success("Query successful"))
        print(Colors.dim(f"Rows affected: {result.get('rows_affected', 0)}"))
    
    @staticmethod
    def display_show_tables(result: Dict) -> None:
        """Display SHOW TABLES results"""
        tables = result.get('tables', [])
        if tables:
            print(Colors.success(f"Found {len(tables)} table(s):"))
            for table in tables:
                print(f"  • {Colors.highlight(table['table'])} ({table.get('rows', 0)} rows)")
        else:
            print(Colors.warning("No tables found"))
    
    @staticmethod
    def display_describe(result: Dict) -> None:
        """Display DESCRIBE results"""
        columns = result.get('columns', [])
        if columns:
            print(Colors.success("Table structure:"))
            for col in columns:
                null_str = "NOT NULL" if not col.get('null') else "NULL"
                default_str = f"DEFAULT {col.get('default')}" if col.get('default') else ""
                key_str = f" {col.get('key')}" if col.get('key') else ""
                extra_str = f" {col.get('extra')}" if col.get('extra') else ""
                
                line = f"  {Colors.highlight(col['field'])} {col['type']} {null_str} {default_str}{key_str}{extra_str}"
                print(line.strip())
        else:
            print(Colors.warning("No columns found"))
    
    @staticmethod
    def display_stats(result: Dict) -> None:
        """Display STATS results"""
        stats = result.get('database', {})
        print(Colors.success("Database statistics:"))
        
        def print_nested(data: Dict, indent: int = 0) -> None:
            for key, value in data.items():
                prefix = "  " * indent
                if isinstance(value, dict):
                    print(f"{prefix}{key}:")
                    print_nested(value, indent + 1)
                else:
                    print(f"{prefix}{key}: {value}")
        
        print_nested(stats)
    
    @staticmethod
    def display_generic(result: Dict, query_type: str, execution_time: float) -> None:
        """Display generic query results"""
        handlers = {
            'select': ResultDisplay.display_select,
            'insert': ResultDisplay.display_insert,
            'update': ResultDisplay.display_update_delete,
            'delete': ResultDisplay.display_update_delete,
            'show_tables': ResultDisplay.display_show_tables,
            'describe': ResultDisplay.display_describe,
            'stats': ResultDisplay.display_stats,
            'vacuum': lambda r: print(Colors.success("Database optimized")),
            'backup': lambda r: print(Colors.success(f"Backup created: {r.get('backup_file', 'N/A')}")),
            'help': lambda r: print(r.get('message', ''))
        }
        
        handler = handlers.get(query_type)
        if handler:
            handler(result)
        else:
            print(Colors.success(f"Query executed successfully"))
        
        # Show execution time
        time_str = f"{result.get('execution_time', execution_time):.3f}s"
        print(Colors.dim(f"Time: {time_str}"))

# ==================== SHELL ====================

class GSQLShell(cmd.Cmd):
    """GSQL Interactive Shell"""
    
    intro = Colors.info("GSQL Interactive Shell") + "\n" + Colors.dim("Type 'help' for commands, 'exit' to quit")
    prompt = Colors.info('gsql> ')
    ruler = Colors.dim('─')
    
    def __init__(self, gsql_app=None):
        super().__init__()
        self.gsql = gsql_app
        self.db = gsql_app.db if gsql_app else None
        self.executor = gsql_app.executor if gsql_app else None
        self.completer = gsql_app.completer if gsql_app else None
        self.history_file = None
        
        self._setup_history()
        self._setup_autocomplete()
    
    def _setup_history(self) -> None:
        """Setup command history"""
        if not self.gsql:
            return
            
        config_data = self.gsql.config.get('shell', {})
        history_file = config_data.get('history_file', '.gsql_history')
        base_dir = self.gsql.config['database'].get('base_dir')
        
        self.history_file = Path(base_dir) / history_file
        
        try:
            readline.read_history_file(str(self.history_file))
        except FileNotFoundError:
            pass
        
        max_history = config_data.get('max_history', 1000)
        readline.set_history_length(max_history)
        
        atexit.register(readline.write_history_file, str(self.history_file))
    
    def _setup_autocomplete(self) -> None:
        """Setup autocomplete"""
        config_data = self.gsql.config.get('shell', {}) if self.gsql else {}
        
        if not config_data.get('autocomplete', True) or not self.completer:
            return
            
        readline.set_completer(self.completer.complete)
        readline.parse_and_bind("tab: complete")
        readline.set_completer_delims(' \t\n`~!@#$%^&*()-=+[{]}\\|;:\'",<>/?')
    
    def default(self, line: str) -> None:
        """Handle default commands (SQL queries)"""
        if not line.strip():
            return
        
        if line.startswith('.'):
            self._handle_dot_command(line)
        else:
            self._execute_sql(line)
    
    def _handle_dot_command(self, command: str) -> bool:
        """Handle dot commands (SQLite style)"""
        parts = command[1:].strip().split()
        if not parts:
            return False
            
        cmd = parts[0].lower()
        args = parts[1:]
        
        handlers = {
            'tables': lambda: self._execute_sql("SHOW TABLES"),
            'schema': lambda: self._handle_schema(args),
            'stats': lambda: self._execute_sql("STATS"),
            'help': lambda: self.do_help(""),
            'backup': lambda: self._handle_backup(args),
            'vacuum': lambda: self._execute_sql("VACUUM"),
            'exit': lambda: True,
            'quit': lambda: True,
            'clear': lambda: os.system('clear' if os.name == 'posix' else 'cls'),
            'history': lambda: self._show_history()
        }
        
        handler = handlers.get(cmd)
        if handler:
            result = handler()
            if result is True:  # For exit/quit
                return True
        else:
            print(Colors.error(f"Unknown command: .{cmd}"))
            print(Colors.dim("Try .help for available commands"))
        
        return False
    
    def _handle_schema(self, args: List[str]) -> None:
        """Handle .schema command"""
        if not args:
            print(Colors.error("Usage: .schema <table_name>"))
            return
        self._execute_sql(f"DESCRIBE {args[0]}")
    
    def _handle_backup(self, args: List[str]) -> None:
        """Handle .backup command"""
        if args:
            self._execute_sql(f"BACKUP {args[0]}")
        else:
            self._execute_sql("BACKUP")
    
    def _show_history(self) -> None:
        """Show command history"""
        try:
            hist_size = readline.get_current_history_length()
            for i in range(1, hist_size + 1):
                cmd = readline.get_history_item(i)
                print(f"{i:4d}  {cmd}")
        except Exception:
            print(Colors.error("Could not display history"))
    
    def _execute_sql(self, sql: str) -> None:
        """Execute SQL query"""
        if not self.db:
            print(Colors.error("No database connection"))
            return
        
        try:
            sql = sql.strip()
            if not sql:
                return
            
            start_time = datetime.now()
            result = self.db.execute(sql)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            if result.get('success'):
                ResultDisplay.display_generic(result, result.get('type', ''), execution_time)
            else:
                print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                
        except Exception as e:
            print(Colors.error(f"Error: {e}"))
            config_data = self.gsql.config if self.gsql else {}
            if config_data.get('verbose_errors', True):
                traceback.print_exc()
    
    # ==================== BUILT-IN COMMANDS ====================
    
    def do_help(self, arg: str) -> None:
        """Show help"""
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

SHELL COMMANDS:
  exit, quit, Ctrl+D             - Exit GSQL
  Ctrl+C                         - Cancel current command
        """
        print(help_text.strip())
    
    def do_exit(self, arg: str) -> bool:
        """Exit GSQL shell"""
        print(Colors.info("Goodbye!"))
        return True
    
    def do_quit(self, arg: str) -> bool:
        """Exit GSQL shell"""
        return self.do_exit(arg)
    
    def do_clear(self, arg: str) -> None:
        """Clear screen"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def do_history(self, arg: str) -> None:
        """Show command history"""
        self._show_history()
    
    # ==================== SHELL CONTROL ====================
    
    def emptyline(self) -> None:
        """Do nothing on empty line"""
        pass
    
    def precmd(self, line: str) -> str:
        """Pre-command processing"""
        if line and not line.startswith('.'):
            readline.add_history(line)
        return line
    
    def postcmd(self, stop: bool, line: str) -> bool:
        """Post-command processing"""
        return stop
    
    def sigint_handler(self, signum: int, frame: Any) -> None:
        """Handle Ctrl+C"""
        print(f"\n{Colors.warning('Interrupted (Ctrl+C)')}")
        self.prompt = Colors.info('gsql> ')

# ==================== MAIN APPLICATION ====================

class GSQLApp:
    """Main GSQL Application"""
    
    def __init__(self):
        self.config = self._load_config()
        self.db: Optional[Database] = None
        self.executor: Optional[QueryExecutor] = None
        self.function_manager: Optional[FunctionManager] = None
        self.nlp_translator = None
        self.completer: Optional[GSQLCompleter] = None
        
        setup_logging(
            level=self.config.get('log_level', 'INFO'),
            log_file=self.config.get('log_file')
        )
        
        logger.info(f"GSQL v{__version__} initialized")
    
    def _load_config(self) -> Dict:
        """Load configuration"""
        user_config = config.to_dict()
        merged = DEFAULT_CONFIG.copy()
        
        # Merge configurations
        for section in ['database', 'executor', 'shell']:
            if section in user_config:
                merged[section].update(user_config[section])
        
        # Update global config
        config.update(**merged.get('database', {}))
        
        return merged
    
    def initialize(self, database_path: Optional[str] = None) -> None:
        """Initialize GSQL components"""
        print(Colors.info("Initializing GSQL..."))
        
        try:
            # Database
            db_config = self.config['database'].copy()
            if database_path:
                db_config['path'] = database_path
            
            self.db = create_database(**db_config)
            
            # Executor
            self.executor = create_executor(storage=self.db.storage)
            
            # Function manager
            self.function_manager = FunctionManager()
            
            # NLP (optional)
            self._setup_nlp()
            
            # Autocompleter
            self.completer = GSQLCompleter(self.db)
            
            print(Colors.success("✓ GSQL ready!"))
            print(Colors.dim(f"Database: {self.db.storage.db_path}"))
            print(Colors.dim("Type 'help' for commands\n"))
            
        except Exception as e:
            print(Colors.error(f"Failed to initialize GSQL: {e}"))
            traceback.print_exc()
            sys.exit(1)
    
    def _setup_nlp(self) -> None:
        """Setup NLP features if available"""
        if NLP_AVAILABLE and NLToSQLTranslator:
            self.nlp_translator = NLToSQLTranslator()
        elif self.config['executor'].get('enable_nlp', False):
            print(Colors.warning("NLP features not available. Install NLTK for NLP support."))
    
    def run_shell(self, database_path: Optional[str] = None) -> None:
        """Run interactive shell"""
        self.initialize(database_path)
        shell = GSQLShell(self)
        
        signal.signal(signal.SIGINT, shell.sigint_handler)
        
        try:
            shell.cmdloop()
        except KeyboardInterrupt:
            print(f"\n{Colors.info('Interrupted')}")
        finally:
            self.cleanup()
    
    def run_query(self, query: str, database_path: Optional[str] = None) -> Optional[Dict]:
        """Execute single query"""
        try:
            self.initialize(database_path)
            result = self.db.execute(query)
            
            if result.get('success'):
                print(Colors.success("Query executed successfully"))
                
                # Display SELECT results
                if result.get('type') == 'select':
                    rows = result.get('rows', [])
                    if rows:
                        columns = result.get('columns', [])
                        print(" | ".join(columns))
                        print("─" * (len(columns) * 10))
                        for row in rows:
                            print(" | ".join(
                                str(v) if v is not None else "NULL" 
                                for v in row
                            ))
                        print(f"\n{len(rows)} row(s) returned")
                    else:
                        print("No rows returned")
                
                # Show execution time
                if 'execution_time' in result:
                    print(f"\nTime: {result['execution_time']:.3f}s")
                
                return result
            else:
                print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                return None
                
        except Exception as e:
            print(Colors.error(f"Error: {e}"))
            return None
        finally:
            self.cleanup()
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            if self.db:
                self.db.close()
                print(Colors.dim("Database closed"))
        except Exception:
            pass

# ==================== COMMAND LINE INTERFACE ====================

def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser"""
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
    
    return parser

def main() -> None:
    """Main entry point"""
    if not GSQL_AVAILABLE:
        print(Colors.error("GSQL modules not available. Check installation."))
        sys.exit(1)
    
    parser = create_parser()
    args = parser.parse_args()
    
    # Configure output
    if args.no_color:
        config.set('colors', False)
    
    # Configure logging
    if args.verbose:
        config.set('log_level', 'DEBUG')
        config.set('verbose_errors', True)
    
    # Create and run application
    app = GSQLApp()
    
    try:
        if args.execute:
            app.run_query(args.execute, args.database)
        elif args.file:
            with open(args.file, 'r') as f:
                queries = f.read()
            app.run_query(queries, args.database)
        else:
            app.run_shell(args.database)
    except FileNotFoundError as e:
        print(Colors.error(f"File not found: {e}"))
        sys.exit(1)
    except Exception as e:
        print(Colors.error(f"Unexpected error: {e}"))
        if args.verbose:
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
