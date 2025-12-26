#!/usr/bin/env python3
"""
GSQL - SQL Database with Natural Language Interface
Main Entry Point: Interactive Shell and CLI
Version: 3.10.0 - Advanced SQLite with NLP
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
    from .database import create_database, Database
    from .executor import create_executor, QueryExecutor
    from .functions import FunctionManager
    from .storage import SQLiteStorage
    
    # NLP avec création automatique des patterns
    try:
        from .nlp.translator import NLToSQLTranslator, create_translator, nl_to_sql
        NLP_AVAILABLE = True
    except ImportError as e:
        print(f"Warning: NLP module not available: {e}")
        NLP_AVAILABLE = False
        NLToSQLTranslator = None
        create_translator = None
        nl_to_sql = None
    
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
        'enable_wal': True,
        'journal_mode': 'WAL',
        'synchronous': 'NORMAL'
    },
    'executor': {
        'enable_nlp': False,
        'enable_learning': True,
        'nlp_confidence_threshold': 0.4,
        'nlp_auto_learn': True,
        'query_timeout': 30,
        'max_rows': 1000
    },
    'shell': {
        'prompt': 'gsql> ',
        'history_file': '.gsql_history',
        'max_history': 1000,
        'colors': True,
        'autocomplete': True,
        'rich_ui': True,
        'border_style': 'rounded',
        'show_timer': True,
        'show_row_count': True,
        'page_size': 20
    }
}

# Commandes SQL étendues
SQL_COMMANDS = {
    'data_query': [
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'REPLACE',
        'WITH', 'RECURSIVE', 'UNION', 'INTERSECT', 'EXCEPT'
    ],
    'schema': [
        'CREATE TABLE', 'CREATE INDEX', 'CREATE VIEW', 'CREATE TRIGGER',
        'ALTER TABLE', 'DROP TABLE', 'DROP INDEX', 'DROP VIEW', 'DROP TRIGGER',
        'RENAME TABLE', 'ADD COLUMN', 'DROP COLUMN'
    ],
    'transaction': [
        'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE'
    ],
    'utility': [
        'EXPLAIN', 'ANALYZE', 'VACUUM', 'BACKUP', 'RESTORE',
        'CHECKPOINT', 'REINDEX', 'PRAGMA'
    ],
    'gsql_specific': [
        'SHOW TABLES', 'SHOW INDEXES', 'SHOW FUNCTIONS', 'SHOW TRIGGERS',
        'DESCRIBE', 'STATS', 'HELP', 'EXPORT', 'IMPORT',
        'BACKUP TO', 'RESTORE FROM', 'OPTIMIZE', 'INFO'
    ]
}

# Commandes avec point étendues
DOT_COMMANDS = [
    # Informations
    '.tables', '.schema', '.indexes', '.functions', '.triggers',
    '.stats', '.info', '.status', '.version',
    
    # Exécution
    '.run', '.execute', '.explain', '.analyze',
    
    # Gestion
    '.backup', '.restore', '.vacuum', '.optimize', '.reindex',
    '.import', '.export', '.clone',
    
    # Configuration
    '.mode', '.width', '.timer', '.headers', '.nullvalue',
    '.echo', '.bail', '.timeout',
    
    # Shell
    '.help', '.exit', '.quit', '.clear', '.history',
    '.reset', '.save', '.load',
    
    # NLP
    '.nlp', '.nlp_stats', '.nlp_learn', '.nlp_patterns'
]

# ==================== LOGGING ====================

logger = logging.getLogger(__name__)

# ==================== ANSI COLORS EXTENDED ====================

class Colors:
    """Extended ANSI color codes for rich terminal output"""
    
    # Reset
    RESET = '\033[0m'
    
    # Styles
    BOLD = '\033[1m'
    DIM = '\033[2m'
    ITALIC = '\033[3m'
    UNDERLINE = '\033[4m'
    BLINK = '\033[5m'
    REVERSE = '\033[7m'
    HIDDEN = '\033[8m'
    
    # Regular colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    # Bright colors
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'
    
    # Background colors
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'
    
    @staticmethod
    def colorize(text: str, *styles: str) -> str:
        """Apply multiple styles to text"""
        if not styles:
            return text
        return f"{''.join(styles)}{text}{Colors.RESET}"
    
    @staticmethod
    def success(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_GREEN, Colors.BOLD)
    
    @staticmethod
    def error(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_RED, Colors.BOLD)
    
    @staticmethod
    def warning(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_YELLOW, Colors.BOLD)
    
    @staticmethod
    def info(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_CYAN, Colors.BOLD)
    
    @staticmethod
    def highlight(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_WHITE, Colors.BOLD)
    
    @staticmethod
    def dim(text: str) -> str:
        return Colors.colorize(text, Colors.DIM)
    
    @staticmethod
    def sql_keyword(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_MAGENTA, Colors.BOLD)
    
    @staticmethod
    def sql_string(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_YELLOW)
    
    @staticmethod
    def sql_number(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_CYAN)
    
    @staticmethod
    def sql_comment(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_GREEN)
    
    @staticmethod
    def sql_table(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_BLUE, Colors.BOLD)
    
    @staticmethod
    def sql_column(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_GREEN)
    
    @staticmethod
    def sql_function(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_MAGENTA)
    
    @staticmethod
    def sql_type(text: str) -> str:
        return Colors.colorize(text, Colors.BRIGHT_CYAN, Colors.ITALIC)

# ==================== PROGRESS BAR ====================

class ProgressBar:
    """Display progress bars for long operations"""
    
    @staticmethod
    def create(iteration: int, total: int, length: int = 40, 
               prefix: str = "", suffix: str = "") -> str:
        """Create a progress bar string"""
        percent = int(100 * (iteration / float(total)))
        filled_length = int(length * iteration // total)
        bar = '█' * filled_length + '░' * (length - filled_length)
        
        return f"{prefix} |{Colors.BRIGHT_GREEN}{bar[:filled_length]}{Colors.DIM}{bar[filled_length:]}{Colors.RESET}| {percent}% {suffix}"
    
    @staticmethod
    def spinner(iteration: int) -> str:
        """Create a spinner animation"""
        spinners = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        return spinners[iteration % len(spinners)]

# ==================== AUTOCOMPLETER ====================

class GSQLCompleter:
    """Advanced autocompletion for GSQL shell"""
    
    def __init__(self, database: Optional[Database] = None):
        self.database = database
        self.keywords = []
        for category in SQL_COMMANDS.values():
            self.keywords.extend(category)
        self.gsql_commands = DOT_COMMANDS
        self.table_names: List[str] = []
        self.column_names: Dict[str, List[str]] = {}
        self.function_names: List[str] = []
        
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
            
            # Get functions
            try:
                result = self.database.execute("SHOW FUNCTIONS")
                if result.get('success'):
                    self.function_names = [
                        func['function'] 
                        for func in result.get('functions', [])
                    ]
            except Exception:
                self.function_names = []
                    
        except Exception:
            logger.debug("Failed to refresh schema", exc_info=True)
    
    def complete(self, text: str, state: int) -> Optional[str]:
        """Completion function for readline"""
        if state == 0:
            line = readline.get_line_buffer()
            tokens = line.strip().split()
            
            if not tokens or len(tokens) == 1:
                # Command completion
                all_commands = self.keywords + self.gsql_commands + self.table_names + self.function_names
                self.matches = [
                    cmd for cmd in all_commands
                    if cmd.lower().startswith(text.lower())
                ]
            elif tokens[-2].upper() in ('FROM', 'INTO', 'JOIN', 'UPDATE'):
                # Table completion
                self.matches = [
                    table for table in self.table_names
                    if table.lower().startswith(text.lower())
                ]
            elif tokens[-2].upper() in ('WHERE', 'SET', 'ORDER BY', 'GROUP BY', 'HAVING'):
                # Column completion
                table = self._find_current_table(tokens)
                if table in self.column_names:
                    self.matches = [
                        col for col in self.column_names[table]
                        if col.lower().startswith(text.lower())
                    ]
                else:
                    self.matches = []
            elif tokens[-1].upper() in ('FUNCTION', 'FUNCTIONS'):
                # Function completion
                self.matches = [
                    func for func in self.function_names
                    if func.lower().startswith(text.lower())
                ]
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
                if token_upper in ('FROM', 'UPDATE', 'JOIN'):
                    return tokens[i + 1]
                elif token_upper == 'INTO':
                    if i > 0 and tokens[i-1].upper() == 'INSERT':
                        return tokens[i + 1]
        return None

# ==================== RESULT DISPLAY ====================

class ResultDisplay:
    """Rich display of query results with multiple output formats"""
    
    MAX_ROWS_DISPLAY = 50
    COLUMN_WIDTH_LIMIT = 30
    
    def __init__(self, rich_ui: bool = True, mode: str = 'table'):
        self.rich_ui = rich_ui
        self.mode = mode  # 'table', 'csv', 'json', 'list'
        self.show_headers = True
        self.null_value = 'NULL'
    
    def display_select(self, result: Dict, execution_time: float) -> None:
        """Display SELECT query results"""
        rows = result.get('rows', [])
        columns = result.get('columns', [])
        count = result.get('count', 0)
        
        if count == 0:
            print(Colors.warning("No rows returned"))
            return
        
        if self.rich_ui and self.mode == 'table':
            self._display_table(columns, rows, count, execution_time)
        elif self.mode == 'csv':
            self._display_csv(columns, rows)
        elif self.mode == 'json':
            self._display_json(columns, rows)
        elif self.mode == 'list':
            self._display_list(columns, rows)
        else:
            self._display_simple(columns, rows, count, execution_time)
    
    def _display_table(self, columns: List[str], rows: List, count: int, execution_time: float) -> None:
        """Display results in table format"""
        # Calculate column widths
        col_widths = []
        for i, col in enumerate(columns):
            max_len = len(str(col))
            for row in rows[:self.MAX_ROWS_DISPLAY]:
                if i < len(row):
                    max_len = max(max_len, len(str(row[i] if row[i] is not None else self.null_value)))
            col_widths.append(min(max_len, self.COLUMN_WIDTH_LIMIT))
        
        # Create header
        if self.show_headers:
            header = " | ".join(
                Colors.highlight(str(col).ljust(width))
                for col, width in zip(columns, col_widths)
            )
            print(header)
            print(Colors.dim('─' * len(header)))
        
        # Create rows
        for i, row in enumerate(rows[:self.MAX_ROWS_DISPLAY]):
            values = []
            for j, cell in enumerate(row):
                width = col_widths[j] if j < len(col_widths) else 20
                if cell is None:
                    cell_str = Colors.dim(self.null_value.ljust(width))
                elif isinstance(cell, (int, float)):
                    cell_str = Colors.sql_number(str(cell).ljust(width))
                elif isinstance(cell, str) and (cell.startswith("'") and cell.endswith("'") or 
                                                cell.startswith('"') and cell.endswith('"')):
                    cell_str = Colors.sql_string(str(cell).ljust(width))
                else:
                    cell_str = str(cell).ljust(width)
                values.append(cell_str)
            
            print(" | ".join(values))
        
        # Show truncation notice
        if len(rows) > self.MAX_ROWS_DISPLAY:
            remaining = len(rows) - self.MAX_ROWS_DISPLAY
            print(Colors.dim(f"... and {remaining} more rows"))
        
        # Show summary
        summary = f"{count} row(s) returned"
        if execution_time > 0:
            summary += f" in {execution_time:.3f}s"
        print(Colors.dim(f"\n{summary}"))
    
    def _display_csv(self, columns: List[str], rows: List) -> None:
        """Display results in CSV format"""
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        if self.show_headers:
            writer.writerow(columns)
        
        for row in rows:
            writer.writerow([cell if cell is not None else self.null_value for cell in row])
        
        print(output.getvalue())
    
    def _display_json(self, columns: List[str], rows: List) -> None:
        """Display results in JSON format"""
        data = []
        for row in rows:
            item = {}
            for i, col in enumerate(columns):
                if i < len(row):
                    item[col] = row[i] if row[i] is not None else None
            data.append(item)
        
        print(json.dumps(data, indent=2))
    
    def _display_list(self, columns: List[str], rows: List) -> None:
        """Display results in list format"""
        for i, row in enumerate(rows[:self.MAX_ROWS_DISPLAY]):
            print(f"\nRow {i + 1}:")
            for j, col in enumerate(columns):
                if j < len(row):
                    value = row[j] if row[j] is not None else Colors.dim(self.null_value)
                    print(f"  {Colors.highlight(col)}: {value}")
    
    def _display_simple(self, columns: List[str], rows: List, count: int, execution_time: float) -> None:
        """Simple display for non-rich UI"""
        print(f"\nResults ({count} rows):")
        print("-" * 40)
        
        for i, row in enumerate(rows[:self.MAX_ROWS_DISPLAY]):
            values = []
            for j, cell in enumerate(row):
                if j < len(columns):
                    col_name = columns[j]
                    value = cell if cell is not None else self.null_value
                    values.append(f"{col_name}={value}")
            print(" | ".join(values))
        
        if execution_time > 0:
            print(f"\nTime: {execution_time:.3f}s")
    
    def display_show_tables(self, result: Dict) -> None:
        """Display SHOW TABLES results"""
        tables = result.get('tables', [])
        if not tables:
            print(Colors.warning("No tables found"))
            return
        
        print(Colors.success(f"Found {len(tables)} table(s):"))
        
        if self.rich_ui:
            headers = ["Table", "Rows", "Type", "Created"]
            rows = []
            for table in tables:
                rows.append([
                    Colors.sql_table(table.get('table', '')),
                    str(table.get('rows', 0)),
                    table.get('type', 'table'),
                    table.get('created_at', 'N/A')
                ])
            self._display_table(headers, rows, len(tables), 0)
        else:
            for table in tables:
                print(f"  • {Colors.highlight(table['table'])} ({table.get('rows', 0)} rows)")
    
    def display_describe(self, result: Dict) -> None:
        """Display DESCRIBE results"""
        columns = result.get('columns', [])
        if not columns:
            print(Colors.warning("No columns found"))
            return
        
        table_name = result.get('table_name', 'Unknown Table')
        print(Colors.success(f"Table structure: {table_name}"))
        
        if self.rich_ui:
            headers = ["Column", "Type", "Null", "Key", "Default", "Extra"]
            rows = []
            for col in columns:
                rows.append([
                    Colors.sql_column(col.get('field', '')),
                    Colors.sql_type(col.get('type', '')),
                    "✓" if col.get('null') else "✗",
                    col.get('key', ''),
                    col.get('default', 'NULL'),
                    col.get('extra', '')
                ])
            self._display_table(headers, rows, len(columns), 0)
        else:
            for col in columns:
                null_str = "NULL" if col.get('null') else "NOT NULL"
                default_str = f"DEFAULT {col.get('default')}" if col.get('default') else ""
                key_str = f" {col.get('key')}" if col.get('key') else ""
                extra_str = f" {col.get('extra')}" if col.get('extra') else ""
                
                line = f"  {Colors.highlight(col['field'])} {col['type']} {null_str} {default_str}{key_str}{extra_str}"
                print(line.strip())
    
    def display_stats(self, result: Dict) -> None:
        """Display STATS results"""
        stats = result.get('database', {})
        
        def print_stat(key: str, value: Any, indent: int = 0) -> None:
            prefix = "  " * indent
            if isinstance(value, dict):
                print(f"{prefix}{Colors.highlight(key)}:")
                for k, v in value.items():
                    print_stat(k, v, indent + 1)
            else:
                print(f"{prefix}{Colors.sql_column(key)}: {Colors.sql_number(str(value))}")
        
        print(Colors.success("Database statistics:"))
        for key, value in stats.items():
            print_stat(key, value)
    
    def display_generic(self, result: Dict, query_type: str, execution_time: float) -> None:
        """Display generic query results"""
        handlers = {
            'select': self.display_select,
            'insert': self._display_insert,
            'update': self._display_update_delete,
            'delete': self._display_update_delete,
            'show_tables': self.display_show_tables,
            'describe': self.display_describe,
            'stats': self.display_stats,
            'vacuum': self._display_vacuum,
            'backup': self._display_backup,
            'help': self._display_help,
            'show_functions': self._display_functions,
            'export': self._display_export,
            'import': self._display_import
        }
        
        handler = handlers.get(query_type)
        if handler:
            handler(result)
        else:
            print(Colors.success("Query executed successfully"))
            if 'rows_affected' in result:
                print(Colors.dim(f"Rows affected: {result['rows_affected']}"))
        
        # Show execution time
        if execution_time > 0 and config.get('shell', {}).get('show_timer', True):
            time_str = f"{execution_time:.3f}s"
            print(Colors.dim(f"Time: {time_str}"))
    
    def _display_insert(self, result: Dict) -> None:
        """Display INSERT results"""
        print(Colors.success(f"✓ Row inserted successfully"))
        if 'last_insert_id' in result:
            print(Colors.dim(f"Last insert ID: {result['last_insert_id']}"))
        if 'rows_affected' in result:
            print(Colors.dim(f"Rows affected: {result['rows_affected']}"))
    
    def _display_update_delete(self, result: Dict) -> None:
        """Display UPDATE/DELETE results"""
        query_type = "UPDATE" if result.get('type') == 'update' else "DELETE"
        print(Colors.success(f"✓ {query_type} executed successfully"))
        if 'rows_affected' in result:
            rows = result['rows_affected']
            print(Colors.dim(f"Rows affected: {rows}"))
            if rows == 0:
                print(Colors.warning("  (No rows matched the condition)"))
    
    def _display_vacuum(self, result: Dict) -> None:
        """Display VACUUM results"""
        print(Colors.success("✓ Database optimized"))
        if 'freed_space' in result:
            print(Colors.dim(f"Freed space: {result['freed_space']} bytes"))
    
    def _display_backup(self, result: Dict) -> None:
        """Display BACKUP results"""
        print(Colors.success("✓ Backup created successfully"))
        if 'backup_file' in result:
            print(Colors.dim(f"Backup file: {result['backup_file']}"))
        if 'size' in result:
            print(Colors.dim(f"Backup size: {result['size']} bytes"))
    
    def _display_help(self, result: Dict) -> None:
        """Display HELP results"""
        if 'message' in result:
            print(result['message'])
        else:
            self._show_help()
    
    def _display_functions(self, result: Dict) -> None:
        """Display SHOW FUNCTIONS results"""
        functions = result.get('functions', [])
        if not functions:
            print(Colors.warning("No functions found"))
            return
        
        print(Colors.success(f"Found {len(functions)} function(s):"))
        
        if self.rich_ui:
            headers = ["Function", "Type", "Description"]
            rows = []
            for func in functions:
                rows.append([
                    Colors.sql_function(func.get('function', '')),
                    func.get('type', 'builtin'),
                    func.get('description', '')
                ])
            self._display_table(headers, rows, len(functions), 0)
        else:
            for func in functions:
                print(f"  • {Colors.highlight(func['function'])} ({func.get('type', 'builtin')})")
    
    def _display_export(self, result: Dict) -> None:
        """Display EXPORT results"""
        print(Colors.success("✓ Export completed"))
        if 'export_file' in result:
            print(Colors.dim(f"Export file: {result['export_file']}"))
        if 'rows_exported' in result:
            print(Colors.dim(f"Rows exported: {result['rows_exported']}"))
    
    def _display_import(self, result: Dict) -> None:
        """Display IMPORT results"""
        print(Colors.success("✓ Import completed"))
        if 'rows_imported' in result:
            print(Colors.dim(f"Rows imported: {result['rows_imported']}"))
    
    def _show_help(self) -> None:
        """Show comprehensive help"""
        help_text = """
GSQL Commands Reference:

DATA QUERY COMMANDS:
  SELECT [DISTINCT] columns FROM table [WHERE condition]
          [GROUP BY columns] [HAVING condition] [ORDER BY columns]
          [LIMIT n] [OFFSET n]
  INSERT INTO table [(columns)] VALUES (values)
  UPDATE table SET column=value [WHERE condition]
  DELETE FROM table [WHERE condition]

SCHEMA COMMANDS:
  CREATE TABLE name (col1 TYPE [CONSTRAINTS], ...)
  CREATE [UNIQUE] INDEX idx_name ON table(column)
  CREATE VIEW view_name AS SELECT ...
  ALTER TABLE name ADD COLUMN col TYPE
  ALTER TABLE name RENAME TO new_name
  DROP TABLE [IF EXISTS] name
  DROP INDEX [IF EXISTS] idx_name

TRANSACTION COMMANDS:
  BEGIN [TRANSACTION]
  COMMIT [TRANSACTION]
  ROLLBACK [TRANSACTION]
  SAVEPOINT name
  RELEASE [SAVEPOINT] name

UTILITY COMMANDS:
  EXPLAIN [QUERY PLAN] query
  ANALYZE [table]
  VACUUM [INTO 'filename']
  BACKUP [TO 'filename']
  PRAGMA pragma_name [= value]

GSQL SPECIAL COMMANDS:
  SHOW TABLES                    - List all tables
  SHOW INDEXES [FROM table]      - List indexes
  SHOW FUNCTIONS                 - List available functions
  DESCRIBE table                 - Show table structure
  STATS                          - Show database statistics
  HELP [command]                 - Show help
  EXPORT table TO 'file' [FORMAT csv|json]
  IMPORT 'file' INTO table [FORMAT csv|json]
  OPTIMIZE                       - Optimize database
  INFO                           - System information

DOT COMMANDS (.commands):
  .tables [pattern]              - List tables matching pattern
  .schema [table]                - Show schema for table
  .indexes [table]               - Show indexes for table
  .stats                         - Database statistics
  .info                          - System info
  .mode [table|csv|json|list]    - Set output mode
  .headers [on|off]              - Toggle headers
  .timer [on|off]                - Toggle execution timer
  .width NUM1 NUM2 ...           - Set column widths
  .nullvalue STRING              - Set NULL display string
  .backup [file]                 - Create backup
  .restore [file]                - Restore from backup
  .vacuum                        - Optimize database
  .help [command]                - Show help
  .exit / .quit                  - Exit shell
  .clear                         - Clear screen
  .history                       - Show command history

NATURAL LANGUAGE COMMANDS (NLP):
  show tables                    - List tables
  count [table]                  - Count rows
  describe [table]               - Table structure
  average [column] from [table]  - Calculate average
  sum [column] from [table]      - Calculate sum
  max/min [column] from [table]  - Find max/min
  add to [table] values (...)    - Insert data
  delete from [table] where ...  - Delete data
  update [table] set ... where .. - Update data

SHELL COMMANDS:
  Ctrl+C                         - Cancel current operation
  Ctrl+L                         - Clear screen
  Ctrl+D                         - Exit GSQL
  Up/Down arrows                - Navigate history
  Tab                           - Auto-completion
        """
        print(help_text.strip())

# ==================== SHELL ====================

class GSQLShell(cmd.Cmd):
    """GSQL Interactive Shell with advanced features"""
    
    intro = ""
    prompt = ""
    ruler = Colors.dim('─' * 60)
    
    def __init__(self, gsql_app=None):
        super().__init__()
        self.gsql = gsql_app
        self.db = gsql_app.db if gsql_app else None
        self.executor = gsql_app.executor if gsql_app else None
        self.nlp_translator = gsql_app.nlp_translator if gsql_app else None
        self.nlp_enabled = gsql_app.nlp_enabled if gsql_app else False
        self.completer = gsql_app.completer if gsql_app else None
        self.history_file = None
        self.rich_ui = gsql_app.config.get('shell', {}).get('rich_ui', True) if gsql_app else True
        self.result_display = ResultDisplay(
            rich_ui=self.rich_ui,
            mode=gsql_app.config.get('shell', {}).get('mode', 'table') if gsql_app else 'table'
        )
        self._current_transaction = None
        self._batch_mode = False
        self._batch_queries = []
        
        self._setup_history()
        self._setup_autocomplete()
        self._setup_prompt()
        
        # Print banner
        self._print_banner()
    
    def _print_banner(self) -> None:
        """Print GSQL banner"""
        if self.rich_ui:
            banner = f"""
{Colors.BRIGHT_CYAN}╔══════════════════════════════════════════════════════════════╗{Colors.RESET}
{Colors.BRIGHT_CYAN}║{Colors.RESET}      {Colors.BOLD}G S Q L  -  GraphQL-inspired SQL Interface{Colors.RESET}        {Colors.BRIGHT_CYAN}║{Colors.RESET}
{Colors.BRIGHT_CYAN}║{Colors.RESET}               {Colors.DIM}Version {__version__} - SQLite Backend{Colors.RESET}             {Colors.BRIGHT_CYAN}║{Colors.RESET}
{Colors.BRIGHT_CYAN}╚══════════════════════════════════════════════════════════════╝{Colors.RESET}
            """
            print(banner)
        
        welcome = f"Welcome to GSQL {__version__}"
        if self.nlp_enabled:
            welcome += f" with {Colors.success('NLP enabled')}"
        
        print(f"\n{welcome}")
        print(Colors.dim("Type '.help' for commands or 'exit' to quit"))
        
        if self.db and hasattr(self.db, 'storage'):
            db_path = getattr(self.db.storage, 'db_path', 'unknown')
            db_name = Path(db_path).stem or "main"
            print(Colors.dim(f"Connected to: {db_name}"))
    
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
    
    def _setup_prompt(self) -> None:
        """Setup dynamic prompt"""
        if self._current_transaction:
            prompt_suffix = " [TXN]"
        elif self._batch_mode:
            prompt_suffix = " [BATCH]"
        else:
            prompt_suffix = ""
        
        if self.rich_ui:
            self.prompt = f"{Colors.BRIGHT_CYAN}gsql{prompt_suffix}>{Colors.RESET} "
        else:
            self.prompt = f"gsql{prompt_suffix}> "
    
    def precmd(self, line: str) -> str:
        """Pre-command processing"""
        self._setup_prompt()
        if line and not line.startswith('.') and not self._batch_mode:
            readline.add_history(line)
        return line
    
    def default(self, line: str) -> Optional[bool]:
        """Handle default commands (SQL/NLP queries)"""
        if not line.strip():
            return None
        
        # Handle batch mode
        if self._batch_mode:
            if line.strip().lower() in ['.end', 'end', 'commit']:
                return self._end_batch_mode()
            self._batch_queries.append(line)
            return None
        
        # Check for batch mode start
        if line.strip().lower() in ['.begin', 'begin', 'start batch']:
            self._start_batch_mode()
            return None
        
        # Handle dot commands
        if line.startswith('.'):
            return self._handle_dot_command(line)
        
        # Handle NLP queries
        if self.nlp_enabled and self._is_nlp_query(line):
            return self._handle_nlp_query(line)
        
        # Handle SQL queries
        self._execute_sql(line)
        return None
    
    def _is_nlp_query(self, query: str) -> bool:
        """Detect if query is natural language"""
        # Skip obvious SQL commands
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 
                       'CREATE', 'DROP', 'ALTER', 'BEGIN', 'COMMIT',
                       'ROLLBACK', 'EXPLAIN', 'ANALYZE', 'VACUUM',
                       'PRAGMA', 'WITH', 'RECURSIVE']
        
        query_upper = query.strip().upper()
        for keyword in sql_keywords:
            if query_upper.startswith(keyword):
                return False
        
        # Detect natural language patterns
        nlp_patterns = [
            r'^(show|display|list|get)\s+',
            r'^(how many|count of|number of)\s+',
            r'^(what is|tell me|give me)\s+',
            r'^(find|search|look for)\s+',
            r'^(average|mean|sum|total|max|min|maximum|minimum)\s+',
            r'^(add|insert|create new|new)\s+',
            r'^(delete|remove|erase)\s+',
            r'^(update|change|modify|edit)\s+',
            r'\b(where|from|having|order by|group by)\b',
            r'\b(table|tables|column|columns|row|rows)\b'
        ]
        
        query_lower = query.lower()
        return any(re.search(pattern, query_lower) for pattern in nlp_patterns)
    
    # ==================== BATCH MODE ====================
    
    def _start_batch_mode(self) -> None:
        """Start batch query mode"""
        self._batch_mode = True
        self._batch_queries = []
        print(Colors.info("Batch mode started. Enter queries, type '.end' to execute or 'cancel' to abort."))
    
    def _end_batch_mode(self) -> bool:
        """End batch mode and execute queries"""
        if not self._batch_queries:
            print(Colors.warning("No queries in batch"))
            self._batch_mode = False
            return False
        
        print(Colors.info(f"Executing {len(self._batch_queries)} queries in batch..."))
        
        success_count = 0
        for i, query in enumerate(self._batch_queries, 1):
            print(Colors.dim(f"\nQuery {i}/{len(self._batch_queries)}: {query[:50]}..."))
            try:
                if self._execute_sql(query, silent=True):
                    success_count += 1
            except Exception as e:
                print(Colors.error(f"  Failed: {e}"))
        
        print(Colors.success(f"\nBatch completed: {success_count}/{len(self._batch_queries)} queries successful"))
        self._batch_mode = False
        self._batch_queries = []
        return True
    
    # ==================== DOT COMMANDS ====================
    
    def _handle_dot_command(self, command: str) -> Optional[bool]:
        """Handle dot commands"""
        parts = command[1:].strip().split()
        if not parts:
            return False
        
        cmd = parts[0].lower()
        args = parts[1:]
        
        # Enhanced dot command handlers
        handlers = {
            # Information commands
            'tables': lambda: self._execute_sql(f"SHOW TABLES {' '.join(args)}"),
            'schema': lambda: self._handle_schema(args),
            'indexes': lambda: self._handle_indexes(args),
            'functions': lambda: self._execute_sql("SHOW FUNCTIONS"),
            'stats': lambda: self._execute_sql("STATS"),
            'info': lambda: self._execute_sql("INFO"),
            'status': lambda: self._show_status(),
            'version': lambda: print(f"GSQL {__version__}"),
            
            # Execution commands
            'run': lambda: self._handle_run(args),
            'execute': lambda: self._handle_execute(args),
            'explain': lambda: self._handle_explain(args),
            'analyze': lambda: self._handle_analyze(args),
            
            # Management commands
            'backup': lambda: self._handle_backup(args),
            'restore': lambda: self._handle_restore(args),
            'vacuum': lambda: self._execute_sql("VACUUM"),
            'optimize': lambda: self._execute_sql("OPTIMIZE"),
            'reindex': lambda: self._execute_sql("REINDEX"),
            'import': lambda: self._handle_import(args),
            'export': lambda: self._handle_export(args),
            'clone': lambda: self._handle_clone(args),
            
            # Configuration commands
            'mode': lambda: self._handle_mode(args),
            'width': lambda: self._handle_width(args),
            'timer': lambda: self._handle_timer(args),
            'headers': lambda: self._handle_headers(args),
            'nullvalue': lambda: self._handle_nullvalue(args),
            'echo': lambda: self._handle_echo(args),
            'bail': lambda: self._handle_bail(args),
            'timeout': lambda: self._handle_timeout(args),
            
            # Shell commands
            'help': lambda: self._handle_help(args),
            'exit': lambda: True,
            'quit': lambda: True,
            'clear': self._clear_screen,
            'history': lambda: self._show_history(),
            'reset': self._reset_shell,
            'save': lambda: self._handle_save(args),
            'load': lambda: self._handle_load(args),
            
            # NLP commands
            'nlp': lambda: self._handle_nlp_command(args),
            'nlp_stats': lambda: self._show_nlp_stats(),
            'nlp_learn': lambda: self._learn_nlp_pattern(args),
            'nlp_patterns': lambda: self._show_nlp_patterns(args),
        }
        
        handler = handlers.get(cmd)
        if handler:
            result = handler()
            if result is True:  # For exit/quit
                return True
        else:
            print(Colors.error(f"Unknown command: .{cmd}"))
            print(Colors.dim("Type '.help' for available commands"))
        
        return False
    
    def _handle_schema(self, args: List[str]) -> None:
        """Handle .schema command"""
        if not args:
            # Show all schemas
            if self.db:
                result = self.db.execute("SHOW TABLES")
                if result.get('success'):
                    tables = result.get('tables', [])
                    for table in tables:
                        self._execute_sql(f"DESCRIBE {table['table']}", silent=True)
        else:
            self._execute_sql(f"DESCRIBE {args[0]}")
    
    def _handle_indexes(self, args: List[str]) -> None:
        """Handle .indexes command"""
        if args:
            self._execute_sql(f"SHOW INDEXES FROM {args[0]}")
        else:
            self._execute_sql("SHOW INDEXES")
    
    def _handle_run(self, args: List[str]) -> None:
        """Handle .run command - execute SQL from file"""
        if not args:
            print(Colors.error("Usage: .run <filename>"))
            return
        
        try:
            with open(args[0], 'r') as f:
                content = f.read()
            
            # Split by semicolons for multiple queries
            queries = [q.strip() for q in content.split(';') if q.strip()]
            
            for query in queries:
                self._execute_sql(query)
                
        except FileNotFoundError:
            print(Colors.error(f"File not found: {args[0]}"))
        except Exception as e:
            print(Colors.error(f"Error: {e}"))
    
    def _handle_execute(self, args: List[str]) -> None:
        """Handle .execute command - execute SQL directly"""
        if not args:
            print(Colors.error("Usage: .execute <sql>"))
            return
        
        self._execute_sql(' '.join(args))
    
    def _handle_explain(self, args: List[str]) -> None:
        """Handle .explain command"""
        if not args:
            print(Colors.error("Usage: .explain <sql>"))
            return
        
        self._execute_sql(f"EXPLAIN {' '.join(args)}")
    
    def _handle_analyze(self, args: List[str]) -> None:
        """Handle .analyze command"""
        if args:
            self._execute_sql(f"ANALYZE {args[0]}")
        else:
            self._execute_sql("ANALYZE")
    
    def _handle_backup(self, args: List[str]) -> None:
        """Handle .backup command"""
        if args:
            self._execute_sql(f"BACKUP TO '{args[0]}'")
        else:
            self._execute_sql("BACKUP")
    
    def _handle_restore(self, args: List[str]) -> None:
        """Handle .restore command"""
        if not args:
            print(Colors.error("Usage: .restore <filename>"))
            return
        
        self._execute_sql(f"RESTORE FROM '{args[0]}'")
    
    def _handle_import(self, args: List[str]) -> None:
        """Handle .import command"""
        if len(args) < 2:
            print(Colors.error("Usage: .import <filename> <table> [FORMAT csv|json]"))
            return
        
        filename = args[0]
        table = args[1]
        format = args[2] if len(args) > 2 else 'csv'
        
        self._execute_sql(f"IMPORT '{filename}' INTO {table} FORMAT {format}")
    
    def _handle_export(self, args: List[str]) -> None:
        """Handle .export command"""
        if len(args) < 2:
            print(Colors.error("Usage: .export <table> <filename> [FORMAT csv|json]"))
            return
        
        table = args[0]
        filename = args[1]
        format = args[2] if len(args) > 2 else 'csv'
        
        self._execute_sql(f"EXPORT {table} TO '{filename}' FORMAT {format}")
    
    def _handle_clone(self, args: List[str]) -> None:
        """Handle .clone command"""
        if not args:
            print(Colors.error("Usage: .clone <new_database>"))
            return
        
        new_db = args[0]
        print(Colors.info(f"Cloning database to {new_db}..."))
        # Implementation would depend on database cloning functionality
    
    def _handle_mode(self, args: List[str]) -> None:
        """Handle .mode command"""
        if not args:
            current_mode = self.result_display.mode
            print(Colors.info(f"Current mode: {current_mode}"))
            print(Colors.dim("Available modes: table, csv, json, list"))
            return
        
        mode = args[0].lower()
        if mode in ['table', 'csv', 'json', 'list']:
            self.result_display.mode = mode
            print(Colors.success(f"Output mode set to: {mode}"))
        else:
            print(Colors.error(f"Invalid mode: {mode}"))
            print(Colors.dim("Available modes: table, csv, json, list"))
    
    def _handle_width(self, args: List[str]) -> None:
        """Handle .width command"""
        if not args:
            print(Colors.info("Current column widths: auto"))
            return
        
        try:
            widths = [int(w) for w in args]
            print(Colors.success(f"Column widths set: {widths}"))
            # Note: Actual width implementation would be in ResultDisplay
        except ValueError:
            print(Colors.error("Invalid width values"))
    
    def _handle_timer(self, args: List[str]) -> None:
        """Handle .timer command"""
        if not args:
            current = config.get('shell', {}).get('show_timer', True)
            status = "on" if current else "off"
            print(Colors.info(f"Timer is {status}"))
            return
        
        if args[0].lower() in ['on', 'yes', 'true', '1']:
            config.set('shell.show_timer', True)
            print(Colors.success("Timer enabled"))
        else:
            config.set('shell.show_timer', False)
            print(Colors.success("Timer disabled"))
    
    def _handle_headers(self, args: List[str]) -> None:
        """Handle .headers command"""
        if not args:
            current = self.result_display.show_headers
            status = "on" if current else "off"
            print(Colors.info(f"Headers are {status}"))
            return
        
        if args[0].lower() in ['on', 'yes', 'true', '1']:
            self.result_display.show_headers = True
            print(Colors.success("Headers enabled"))
        else:
            self.result_display.show_headers = False
            print(Colors.success("Headers disabled"))
    
    def _handle_nullvalue(self, args: List[str]) -> None:
        """Handle .nullvalue command"""
        if not args:
            current = self.result_display.null_value
            print(Colors.info(f"NULL value display: '{current}'"))
            return
        
        self.result_display.null_value = args[0]
        print(Colors.success(f"NULL value set to: '{args[0]}'"))
    
    def _handle_echo(self, args: List[str]) -> None:
        """Handle .echo command"""
        if not args:
            print(Colors.info("Usage: .echo <message>"))
            return
        
        print(' '.join(args))
    
    def _handle_bail(self, args: List[str]) -> None:
        """Handle .bail command"""
        if not args:
            current = config.get('executor', {}).get('stop_on_error', True)
            status = "on" if current else "off"
            print(Colors.info(f"Bail is {status}"))
            return
        
        if args[0].lower() in ['on', 'yes', 'true', '1']:
            config.set('executor.stop_on_error', True)
            print(Colors.success("Stop on error enabled"))
        else:
            config.set('executor.stop_on_error', False)
            print(Colors.success("Stop on error disabled"))
    
    def _handle_timeout(self, args: List[str]) -> None:
        """Handle .timeout command"""
        if not args:
            current = config.get('executor', {}).get('query_timeout', 30)
            print(Colors.info(f"Query timeout: {current} seconds"))
            return
        
        try:
            timeout = int(args[0])
            config.set('executor.query_timeout', timeout)
            print(Colors.success(f"Query timeout set to {timeout} seconds"))
        except ValueError:
            print(Colors.error("Invalid timeout value"))
    
    def _handle_help(self, args: List[str]) -> None:
        """Handle .help command"""
        if not args:
            self.do_help("")
        elif args[0].startswith('.'):
            # Show help for specific dot command
            cmd = args[0][1:]
            help_texts = {
                'tables': "List tables: .tables [pattern]",
                'schema': "Show schema: .schema [table]",
                'indexes': "Show indexes: .indexes [table]",
                'stats': "Database statistics: .stats",
                'mode': "Set output mode: .mode [table|csv|json|list]",
                'timer': "Toggle timer: .timer [on|off]",
                'headers': "Toggle headers: .headers [on|off]",
                'backup': "Create backup: .backup [file]",
                'restore': "Restore backup: .restore [file]",
                'vacuum': "Optimize database: .vacuum",
                'help': "Show help: .help [command]",
                'exit': "Exit shell: .exit",
                'clear': "Clear screen: .clear",
                'history': "Show history: .history",
                'nlp': "NLP commands: .nlp [on|off|stats|learn]"
            }
            
            if cmd in help_texts:
                print(Colors.info(help_texts[cmd]))
            else:
                print(Colors.warning(f"No help available for .{cmd}"))
        else:
            # Show help for SQL command
            print(Colors.info(f"Help for {args[0]}: ..."))
            # Could expand with detailed SQL command help
    
    def _clear_screen(self) -> None:
        """Clear screen"""
        os.system('clear' if os.name == 'posix' else 'cls')
        self._print_banner()
    
    def _show_history(self) -> None:
        """Show command history"""
        try:
            hist_size = readline.get_current_history_length()
            if hist_size == 0:
                print(Colors.info("No command history"))
                return
            
            print(Colors.info(f"Command history (last {min(hist_size, 20)} commands):"))
            start = max(1, hist_size - 19)
            for i in range(start, hist_size + 1):
                cmd = readline.get_history_item(i)
                print(f"  {i:4d}: {cmd}")
                
        except Exception:
            print(Colors.error("Could not display history"))
    
    def _reset_shell(self) -> None:
        """Reset shell state"""
        self._current_transaction = None
        self._batch_mode = False
        self._batch_queries = []
        self.result_display = ResultDisplay(self.rich_ui)
        print(Colors.success("Shell reset"))
    
    def _handle_save(self, args: List[str]) -> None:
        """Handle .save command - save queries to file"""
        if not args:
            print(Colors.error("Usage: .save <filename>"))
            return
        
        try:
            with open(args[0], 'w') as f:
                hist_size = readline.get_current_history_length()
                for i in range(1, hist_size + 1):
                    cmd = readline.get_history_item(i)
                    if cmd:
                        f.write(cmd + '\n')
            print(Colors.success(f"History saved to {args[0]}"))
        except Exception as e:
            print(Colors.error(f"Error saving history: {e}"))
    
    def _handle_load(self, args: List[str]) -> None:
        """Handle .load command - load queries from file"""
        if not args:
            print(Colors.error("Usage: .load <filename>"))
            return
        
        try:
            with open(args[0], 'r') as f:
                for line in f:
                    cmd = line.strip()
                    if cmd:
                        readline.add_history(cmd)
            print(Colors.success(f"History loaded from {args[0]}"))
        except Exception as e:
            print(Colors.error(f"Error loading history: {e}"))
    
    def _show_status(self) -> None:
        """Show shell status"""
        status = []
        status.append(f"Database: {getattr(self.db.storage, 'db_path', 'unknown') if self.db else 'Not connected'}")
        status.append(f"NLP: {'Enabled' if self.nlp_enabled else 'Disabled'}")
        status.append(f"Output mode: {self.result_display.mode}")
        status.append(f"Headers: {'On' if self.result_display.show_headers else 'Off'}")
        status.append(f"Timer: {'On' if config.get('shell', {}).get('show_timer', True) else 'Off'}")
        if self._current_transaction:
            status.append("Transaction: Active")
        if self._batch_mode:
            status.append(f"Batch mode: Active ({len(self._batch_queries)} queries)")
        
        print(Colors.info("Shell status:"))
        for line in status:
            print(f"  {line}")
    
    # ==================== NLP COMMANDS ====================
    
    def _handle_nlp_command(self, args: List[str]) -> None:
        """Handle .nlp command"""
        if not args:
            # Show NLP status
            if self.nlp_enabled:
                print(Colors.success("NLP is enabled"))
                if self.nlp_translator:
                    stats = self.nlp_translator.get_statistics()
                    print(Colors.dim(f"  Patterns: {stats['total_patterns']}"))
                    print(Colors.dim(f"  Translations: {stats['total_translations']}"))
                    print(Colors.dim(f"  Cache size: {stats['cache_size']}"))
            else:
                print(Colors.warning("NLP is disabled"))
            return
        
        cmd = args[0].lower()
        
        if cmd == 'on':
            if not NLP_AVAILABLE:
                print(Colors.error("NLP module not available. Install NLTK."))
                return
            
            if self.gsql:
                self.gsql.setup_nlp(enable_nlp=True)
                self.nlp_enabled = True
                self.nlp_translator = self.gsql.nlp_translator
                print(Colors.success("NLP enabled"))
            else:
                print(Colors.error("Cannot enable NLP"))
        
        elif cmd == 'off':
            if self.gsql:
                self.gsql.setup_nlp(enable_nlp=False)
                self.nlp_enabled = False
                self.nlp_translator = None
                print(Colors.success("NLP disabled"))
        
        else:
            print(Colors.error(f"Unknown NLP command: {cmd}"))
            print(Colors.dim("Available: .nlp [on|off|stats|learn|patterns]"))
    
    def _show_nlp_stats(self) -> None:
        """Show NLP statistics"""
        if not self.nlp_translator:
            print(Colors.error("NLP not initialized"))
            return
        
        stats = self.nlp_translator.get_statistics()
        
        print(Colors.info("NLP Statistics:"))
        print(f"  Patterns loaded: {stats['total_patterns']}")
        print(f"  Total translations: {stats['total_translations']}")
        print(f"  Cache size: {stats['cache_size']}")
        print(f"  NLTK available: {stats['nltk_available']}")
        print(f"  Database context: {'Loaded' if stats['db_context_loaded'] else 'Not loaded'}")
        
        if stats['top_patterns']:
            print(Colors.info("\nTop 5 patterns:"))
            for i, pattern in enumerate(stats['top_patterns'], 1):
                print(f"  {i}. {pattern['pattern']} (used {pattern['usage_count']} times)")
    
    def _learn_nlp_pattern(self, args: List[str]) -> None:
        """Learn a new NLP pattern"""
        if not self.nlp_translator:
            print(Colors.error("NLP not initialized"))
            return
        
        if len(args) < 2:
            print(Colors.error("Usage: .nlp_learn <nl_query> <sql_query>"))
            return
        
        nl_query = ' '.join(args[:-1])
        sql_query = args[-1]
        
        success = self.nlp_translator.learn_from_example(nl_query, sql_query)
        if success:
            print(Colors.success("Pattern learned successfully"))
        else:
            print(Colors.error("Failed to learn pattern"))
    
    def _show_nlp_patterns(self, args: List[str]) -> None:
        """Show NLP patterns"""
        if not self.nlp_translator:
            print(Colors.error("NLP not initialized"))
            return
        
        pattern_type = args[0] if args else None
        
        if pattern_type and pattern_type in self.nlp_translator.patterns:
            patterns = self.nlp_translator.patterns[pattern_type]
            print(Colors.info(f"{pattern_type.upper()} patterns ({len(patterns)}):"))
            for pattern in patterns:
                print(f"  • {pattern.nl_pattern} → {pattern.sql_template}")
        else:
            # Show all patterns
            total = 0
            for ptype, patterns in self.nlp_translator.patterns.items():
                print(Colors.info(f"{ptype.upper()} patterns ({len(patterns)}):"))
                for pattern in patterns[:5]:  # Show first 5 of each type
                    print(f"  • {pattern.nl_pattern}")
                if len(patterns) > 5:
                    print(f"  ... and {len(patterns) - 5} more")
                total += len(patterns)
                print()
            print(Colors.dim(f"Total patterns: {total}"))
    
    def _handle_nlp_query(self, nl_query: str) -> Optional[bool]:
        """Handle natural language query"""
        if not self.nlp_translator:
            print(Colors.error("NLP translator not available"))
            return False
        
        try:
            # Translate NL to SQL
            translation = self.nlp_translator.translate(nl_query)
            
            # Display translation info
            print(Colors.info(f"NL Query: {nl_query}"))
            print(Colors.info(f"SQL Translation: {translation['sql']}"))
            print(Colors.info(f"Confidence: {translation['confidence']:.2f}"))
            
            if translation['explanation']:
                print(Colors.dim(f"Explanation: {translation['explanation']}"))
            
            # Check confidence threshold
            confidence_threshold = self.gsql.config['executor'].get('nlp_confidence_threshold', 0.3) if self.gsql else 0.3
            
            if translation['confidence'] < confidence_threshold:
                print(Colors.warning(f"⚠ Low confidence ({translation['confidence']:.2f} < {confidence_threshold})"))
                
                # Ask for confirmation
                response = input(Colors.info("Execute this query? (y/N) ")).lower()
                if response not in ['y', 'yes', 'o', 'oui']:
                    print(Colors.info("Query cancelled"))
                    return False
            
            # Execute the SQL query
            return self._execute_sql(translation['sql'], is_nlp=True, nl_query=nl_query)
            
        except Exception as e:
            print(Colors.error(f"NLP Error: {e}"))
            return False
    
    # ==================== SQL EXECUTION ====================
    
    def _execute_sql(self, sql: str, is_nlp: bool = False, nl_query: str = None, silent: bool = False) -> bool:
        """Execute SQL query"""
        if not self.db:
            if not silent:
                print(Colors.error("No database connection"))
            return False
        
        try:
            sql = sql.strip()
            if not sql:
                return False
            
            # Show executing status
            if not silent and self.rich_ui and len(sql) > 50:
                print(Colors.dim(f"Executing: {sql[:50]}..."))
            elif not silent and self.rich_ui:
                print(Colors.dim(f"Executing: {sql}"))
            
            # Check for transaction commands
            sql_upper = sql.upper()
            if sql_upper.startswith('BEGIN'):
                self._current_transaction = 'active'
                self._setup_prompt()
            elif sql_upper.startswith(('COMMIT', 'ROLLBACK')):
                self._current_transaction = None
                self._setup_prompt()
            
            start_time = datetime.now()
            result = self.db.execute(sql)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            if result.get('success'):
                if not silent:
                    self.result_display.display_generic(result, result.get('type', ''), execution_time)
                
                # Learn from successful NLP translation
                if is_nlp and nl_query and self.nlp_translator:
                    auto_learn = self.gsql.config['executor'].get('nlp_auto_learn', True) if self.gsql else True
                    if auto_learn:
                        self.nlp_translator.learn_from_example(
                            nl_query=nl_query,
                            sql_query=sql,
                            feedback_score=0.8
                        )
                        if not silent:
                            print(Colors.dim("Pattern learned from successful execution"))
                
                return True
            else:
                if not silent:
                    print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                
                # Learn from failed NLP translation with lower score
                if is_nlp and nl_query and self.nlp_translator:
                    self.nlp_translator.learn_from_example(
                        nl_query=nl_query,
                        sql_query=sql,
                        feedback_score=0.2
                    )
                
                return False
                
        except Exception as e:
            if not silent:
                print(Colors.error(f"Error: {e}"))
            
            config_data = self.gsql.config if self.gsql else {}
            if config_data.get('verbose_errors', True) and not silent:
                traceback.print_exc()
            
            return False
    
    # ==================== BUILT-IN COMMANDS ====================
    
    def do_help(self, arg: str) -> None:
        """Show help"""
        if arg:
            # Specific command help
            if arg.startswith('.'):
                self._handle_help([arg[1:]])
            else:
                print(Colors.info(f"Help for '{arg}': Not implemented yet"))
        else:
            # General help
            help_text = """
GSQL Quick Reference:

SQL COMMANDS:
  SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER
  BEGIN, COMMIT, ROLLBACK, EXPLAIN, ANALYZE, VACUUM

GSQL SPECIAL:
  SHOW TABLES, SHOW INDEXES, SHOW FUNCTIONS
  DESCRIBE table, STATS, HELP, EXPORT, IMPORT

DOT COMMANDS:
  .tables [pattern]    - List tables
  .schema [table]      - Show table structure  
  .stats              - Database statistics
  .mode [table|csv|json|list] - Set output mode
  .timer [on|off]     - Toggle execution timer
  .headers [on|off]   - Toggle column headers
  .backup [file]      - Create backup
  .vacuum             - Optimize database
  .help [command]     - Show help
  .exit / .quit       - Exit shell
  .clear              - Clear screen
  .history            - Show command history

NLP COMMANDS:
  .nlp [on|off]       - Enable/disable NLP
  .nlp_stats          - Show NLP statistics
  .nlp_learn          - Learn new NLP pattern

Type '.help <command>' for more information on a specific command.
            """
            print(help_text.strip())
    
    def do_exit(self, arg: str) -> bool:
        """Exit GSQL shell"""
        if self._batch_mode:
            response = input(Colors.warning("Batch mode active. Exit anyway? (y/N) ")).lower()
            if response not in ['y', 'yes']:
                return False
        
        if self._current_transaction:
            response = input(Colors.warning("Transaction active. Exit anyway? (y/N) ")).lower()
            if response not in ['y', 'yes']:
                return False
        
        print(Colors.info("Goodbye!"))
        return True
    
    def do_quit(self, arg: str) -> bool:
        """Exit GSQL shell"""
        return self.do_exit(arg)
    
    def do_clear(self, arg: str) -> None:
        """Clear screen"""
        self._clear_screen()
    
    def do_history(self, arg: str) -> None:
        """Show command history"""
        self._show_history()
    
    # ==================== SHELL CONTROL ====================
    
    def emptyline(self) -> None:
        """Do nothing on empty line"""
        pass
    
    def postcmd(self, stop: bool, line: str) -> bool:
        """Post-command processing"""
        return stop
    
    def sigint_handler(self, signum: int, frame: Any) -> None:
        """Handle Ctrl+C"""
        if self._batch_mode:
            print(f"\n{Colors.warning('Batch mode cancelled')}")
            self._batch_mode = False
            self._batch_queries = []
        else:
            print(f"\n{Colors.warning('Command interrupted (Ctrl+C)')}")
        self._setup_prompt()

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
        self.nlp_enabled = False
        self.nlp_patterns_file = None
        
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
    
    def setup_nlp(self, enable_nlp: bool = False, 
                  patterns_file: Optional[str] = None,
                  confidence_threshold: float = 0.4) -> None:
        """Setup NLP with automatic patterns creation"""
        if not NLP_AVAILABLE:
            logger.warning("NLP module not available. Install NLTK for NLP support.")
            return
        
        self.nlp_enabled = enable_nlp
        
        if not self.nlp_enabled:
            logger.info("NLP is disabled")
            return
        
        # Déterminer le fichier de patterns
        if patterns_file:
            self.nlp_patterns_file = patterns_file
        else:
            # Chemin par défaut
            nlp_dir = Path(self.config['database'].get('base_dir')) / 'nlp'
            nlp_dir.mkdir(exist_ok=True, parents=True)
            self.nlp_patterns_file = str(nlp_dir / 'patterns.json')
        
        # Mettre à jour la configuration
        self.config['executor']['enable_nlp'] = True
        self.config['executor']['nlp_confidence_threshold'] = confidence_threshold
        config.set('executor.enable_nlp', True)
        config.set('executor.nlp_confidence_threshold', confidence_threshold)
        
        logger.info(f"NLP enabled with patterns file: {self.nlp_patterns_file}")
        logger.info(f"NLP confidence threshold: {confidence_threshold}")
    
    def initialize_nlp(self, database_path: Optional[str] = None) -> bool:
        """Initialize NLP translator"""
        if not self.nlp_enabled:
            return False
        
        if not NLP_AVAILABLE:
            logger.warning("NLP module not available")
            return False
        
        try:
            # Créer le traducteur
            self.nlp_translator = create_translator(
                db_path=database_path or (self.db.storage.db_path if self.db else None),
                patterns_path=self.nlp_patterns_file
            )
            
            logger.info("NLP translator initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize NLP translator: {e}")
            return False
    
    def initialize(self, database_path: Optional[str] = None) -> None:
    """Initialize GSQL components"""
    print(Colors.info("Initializing GSQL..."))
    
    try:
        # Database - filtrer uniquement les paramètres supportés
        db_config = self.config['database'].copy()
        
        # Paramètres supportés par create_database
        supported_params = ['base_dir', 'auto_recovery', 'buffer_pool_size', 'enable_wal']
        filtered_config = {k: v for k, v in db_config.items() if k in supported_params}
        
        if database_path:
            filtered_config['path'] = database_path
        
        self.db = create_database(**filtered_config)
        
        # Configurer les paramètres SQLite avancés après création
        if self.db and hasattr(self.db, 'storage'):
            # Configurer journal_mode si spécifié
            journal_mode = db_config.get('journal_mode')
            if journal_mode and hasattr(self.db.storage, 'connection'):
                try:
                    cursor = self.db.storage.connection.cursor()
                    cursor.execute(f"PRAGMA journal_mode = {journal_mode}")
                except:
                    pass
            
            # Configurer synchronous si spécifié
            synchronous = db_config.get('synchronous')
            if synchronous and hasattr(self.db.storage, 'connection'):
                try:
                    cursor = self.db.storage.connection.cursor()
                    cursor.execute(f"PRAGMA synchronous = {synchronous}")
                except:
                    pass
        
        # Executor
        self.executor = create_executor(storage=self.db.storage)
        
        # Function manager
        self.function_manager = FunctionManager()
        
        # NLP (optional)
        if self.nlp_enabled:
            nlp_success = self.initialize_nlp(database_path)
            if nlp_success:
                print(Colors.success("✓ NLP initialized"))
            else:
                print(Colors.warning("⚠ NLP initialization failed"))
        
        # Autocompleter
        self.completer = GSQLCompleter(self.db)
        
        print(Colors.success("✓ GSQL initialized successfully"))
        print(Colors.dim(f"Database: {self.db.storage.db_path}"))
        
        if self.nlp_enabled:
            print(Colors.dim(f"NLP: Enabled (confidence: {self.config['executor'].get('nlp_confidence_threshold', 0.4)})"))
        
    except Exception as e:
        print(Colors.error(f"Failed to initialize GSQL: {e}"))
        traceback.print_exc()
        sys.exit(1)
    
    def run_shell(self, database_path: Optional[str] = None) -> None:
        """Run interactive shell"""
        self.initialize(database_path)
        shell = GSQLShell(self)
        
        signal.signal(signal.SIGINT, shell.sigint_handler)
        
        try:
            shell.cmdloop()
        except KeyboardInterrupt:
            print(f"\n{Colors.info('Session terminated')}")
        finally:
            self.cleanup()
    
    def run_nlp_query(self, nl_query: str, database_path: Optional[str] = None) -> Optional[Dict]:
        """Execute a natural language query"""
        if not self.nlp_enabled:
            # Activer NLP pour cette exécution
            self.setup_nlp(enable_nlp=True)
        
        try:
            self.initialize(database_path)
            
            if not hasattr(self, 'nlp_translator') or not self.nlp_translator:
                print(Colors.error("NLP translator not initialized"))
                return None
            
            # Traduire la requête NL
            translation = self.nlp_translator.translate(nl_query)
            
            print(Colors.info(f"NL Query: {nl_query}"))
            print(Colors.info(f"Translated to SQL: {translation['sql']}"))
            print(Colors.info(f"Confidence: {translation['confidence']:.2f}"))
            
            if translation['explanation']:
                print(Colors.dim(f"Explanation: {translation['explanation']}"))
            
            # Vérifier la confiance
            confidence_threshold = self.config['executor'].get('nlp_confidence_threshold', 0.4)
            if translation['confidence'] < confidence_threshold:
                print(Colors.warning(f"⚠ Low confidence ({translation['confidence']:.2f} < {confidence_threshold})"))
                
                # Demander confirmation
                response = input(Colors.info("Execute this query? (y/N) ")).lower()
                if response not in ['y', 'yes', 'o', 'oui']:
                    print(Colors.info("Query cancelled"))
                    return None
            
            # Exécuter la requête SQL
            result = self.db.execute(translation['sql'])
            
            # Afficher le résultat
            if result.get('success'):
                display = ResultDisplay(self.config['shell'].get('rich_ui', True))
                display.display_generic(result, result.get('type', ''), 
                                      result.get('execution_time', 0))
                return result
            else:
                print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                return None
                
        except Exception as e:
            print(Colors.error(f"Error: {e}"))
            return None
        finally:
            self.cleanup()
    
    def run_query(self, query: str, database_path: Optional[str] = None) -> Optional[Dict]:
        """Execute single query (SQL or NLP)"""
        # Détecter si c'est une requête NLP
        if self.nlp_enabled and self._is_nlp_query(query):
            return self.run_nlp_query(query, database_path)
        else:
            # Exécuter comme requête SQL normale
            try:
                self.initialize(database_path)
                result = self.db.execute(query)
                
                if result.get('success'):
                    display = ResultDisplay(self.config['shell'].get('rich_ui', True))
                    display.display_generic(result, result.get('type', ''), 
                                          result.get('execution_time', 0))
                    return result
                else:
                    print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                    return None
                    
            except Exception as e:
                print(Colors.error(f"Error: {e}"))
                return None
            finally:
                self.cleanup()
    
    def _is_nlp_query(self, query: str) -> bool:
        """Detect if query is natural language"""
        # Ne pas traiter les commandes SQL évidentes
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 
                       'CREATE', 'DROP', 'SHOW', 'DESCRIBE',
                       'ALTER', 'BEGIN', 'COMMIT', 'ROLLBACK',
                       'EXPLAIN', 'ANALYZE', 'VACUUM', 'PRAGMA',
                       'WITH', 'RECURSIVE']
        
        query_upper = query.strip().upper()
        for keyword in sql_keywords:
            if query_upper.startswith(keyword):
                return False
        
        # Détecter le langage naturel
        nlp_patterns = [
            r'^(show|display|list|get)\s+',
            r'^(how many|count of|number of)\s+',
            r'^(what is|tell me|give me)\s+',
            r'^(find|search|look for)\s+',
            r'^(average|mean|sum|total|max|min|maximum|minimum)\s+',
            r'^(add|insert|create new|new)\s+',
            r'^(delete|remove|erase)\s+',
            r'^(update|change|modify|edit)\s+'
        ]
        
        query_lower = query.lower()
        return any(re.search(pattern, query_lower) for pattern in nlp_patterns)
    
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
        description=f"GSQL v{__version__} - Advanced SQL Database Interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  gsql                         # Start interactive shell
  gsql mydb.db                 # Open specific database
  gsql -e "SHOW TABLES"        # Execute single query
  gsql --nlp "show tables"     # Execute NLP query
  gsql -f queries.sql          # Execute queries from file
  gsql --enable-nlp            # Enable NLP in shell
  gsql --mode csv              # Set output mode to CSV

Output Modes:
  table (default) - Formatted table
  csv            - Comma-separated values
  json           - JSON format
  list           - List format

NLP Examples:
  show tables
  count users
  average salary from employees
  add new user John Doe
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
        '--nlp',
        help='Execute Natural Language query and exit'
    )
    
    parser.add_argument(
        '-f', '--file',
        help='Execute SQL from file and exit'
    )
    
    parser.add_argument(
        '--enable-nlp',
        action='store_true',
        help='Enable Natural Language Processing in interactive mode'
    )
    
    parser.add_argument(
        '--nlp-confidence',
        type=float,
        default=0.4,
        help='NLP confidence threshold (default: 0.4)'
    )
    
    parser.add_argument(
        '--nlp-patterns',
        help='Custom NLP patterns file'
    )
    
    parser.add_argument(
        '--mode',
        choices=['table', 'csv', 'json', 'list'],
        help='Output mode for query results'
    )
    
    parser.add_argument(
        '--no-color',
        action='store_true',
        help='Disable colored output'
    )
    
    parser.add_argument(
        '--simple-ui',
        action='store_true',
        help='Use simple UI instead of rich UI'
    )
    
    parser.add_argument(
        '--no-headers',
        action='store_true',
        help='Disable column headers in output'
    )
    
    parser.add_argument(
        '--no-timer',
        action='store_true',
        help='Disable execution timer'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output and error messages'
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
    
    # Configure UI mode
    if args.simple_ui:
        config.set('shell.rich_ui', False)
    
    # Configure output mode
    if args.mode:
        config.set('shell.mode', args.mode)
    
    # Configure headers
    if args.no_headers:
        config.set('shell.show_headers', False)
    
    # Configure timer
    if args.no_timer:
        config.set('shell.show_timer', False)
    
    # Configure logging
    if args.verbose:
        config.set('log_level', 'DEBUG')
        config.set('verbose_errors', True)
    
    # Create and run application
    app = GSQLApp()
    
    # Setup NLP based on arguments
    nlp_enabled = args.enable_nlp or bool(args.nlp)
    app.setup_nlp(
        enable_nlp=nlp_enabled,
        patterns_file=args.nlp_patterns,
        confidence_threshold=args.nlp_confidence
    )
    
    try:
        if args.nlp:
            # Execute NLP query directly
            app.run_nlp_query(args.nlp, args.database)
        elif args.execute:
            # Execute SQL query
            app.run_query(args.execute, args.database)
        elif args.file:
            # Execute from file
            with open(args.file, 'r') as f:
                queries = f.read()
            app.run_query(queries, args.database)
        else:
            # Interactive shell
            app.run_shell(args.database)
            
    except FileNotFoundError as e:
        print(Colors.error(f"File not found: {e}"))
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n{Colors.info('Interrupted by user')}")
        sys.exit(0)
    except Exception as e:
        print(Colors.error(f"Unexpected error: {e}"))
        if args.verbose:
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
