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
        'autocomplete': True,
        'rich_ui': True,
        'border_style': 'rounded'
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
    '.vacuum', '.exit', '.quit', '.clear', '.history',
    '.mode', '.width', '.timer'
]

# ==================== LOGGING ====================

logger = logging.getLogger(__name__)

# ==================== RICH UI COMPONENTS ====================

class BoxChars:
    """Box-drawing characters for rich UI"""
    
    # Simple border
    SIMPLE = {
        'horizontal': '─',
        'vertical': '│',
        'top_left': '┌',
        'top_right': '┐',
        'bottom_left': '└',
        'bottom_right': '┘',
        'left_tee': '├',
        'right_tee': '┤',
        'top_tee': '┬',
        'bottom_tee': '┴',
        'cross': '┼'
    }
    
    # Rounded border
    ROUNDED = {
        'horizontal': '─',
        'vertical': '│',
        'top_left': '╭',
        'top_right': '╮',
        'bottom_left': '╰',
        'bottom_right': '╯',
        'left_tee': '├',
        'right_tee': '┤',
        'top_tee': '┬',
        'bottom_tee': '┴',
        'cross': '┼'
    }
    
    # Double border
    DOUBLE = {
        'horizontal': '═',
        'vertical': '║',
        'top_left': '╔',
        'top_right': '╗',
        'bottom_left': '╚',
        'bottom_right': '╝',
        'left_tee': '╠',
        'right_tee': '╣',
        'top_tee': '╦',
        'bottom_tee': '╩',
        'cross': '╬'
    }
    
    # Bold border
    BOLD = {
        'horizontal': '━',
        'vertical': '┃',
        'top_left': '┏',
        'top_right': '┓',
        'bottom_left': '┗',
        'bottom_right': '┛',
        'left_tee': '┣',
        'right_tee': '┫',
        'top_tee': '┳',
        'bottom_tee': '┻',
        'cross': '╋'
    }
    
    @staticmethod
    def get_style(style_name: str) -> Dict[str, str]:
        """Get border style by name"""
        styles = {
            'simple': BoxChars.SIMPLE,
            'rounded': BoxChars.ROUNDED,
            'double': BoxChars.DOUBLE,
            'bold': BoxChars.BOLD
        }
        return styles.get(style_name.lower(), BoxChars.ROUNDED)

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
    DEFAULT = '\033[39m'
    
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
    BG_DEFAULT = '\033[49m'
    
    # RGB colors (24-bit)
    @staticmethod
    def rgb(r: int, g: int, b: int) -> str:
        """Generate 24-bit color code"""
        return f'\033[38;2;{r};{g};{b}m'
    
    @staticmethod
    def bg_rgb(r: int, g: int, b: int) -> str:
        """Generate 24-bit background color code"""
        return f'\033[48;2;{r};{g};{b}m'
    
    # Gradients
    @staticmethod
    def gradient(text: str, start_rgb: Tuple[int, int, int], 
                 end_rgb: Tuple[int, int, int]) -> str:
        """Create gradient text effect"""
        if len(text) == 0:
            return text
        
        result = []
        for i, char in enumerate(text):
            ratio = i / max(len(text) - 1, 1)
            r = int(start_rgb[0] + (end_rgb[0] - start_rgb[0]) * ratio)
            g = int(start_rgb[1] + (end_rgb[1] - start_rgb[1]) * ratio)
            b = int(start_rgb[2] + (end_rgb[2] - start_rgb[2]) * ratio)
            result.append(f"{Colors.rgb(r, g, b)}{char}")
        
        result.append(Colors.RESET)
        return ''.join(result)
    
    # Utility methods
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
    def title(text: str) -> str:
        """Styled title with gradient"""
        return Colors.gradient(text, (255, 100, 100), (100, 100, 255))

# ==================== RICH UI HELPERS ====================

class RichUI:
    """Rich UI helper methods"""
    
    @staticmethod
    def print_banner() -> None:
        """Print GSQL banner"""
        banner = f"""
{Colors.rgb(100, 200, 255)}╔══════════════════════════════════════════════════════════════╗{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}                                                              {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}      {Colors.title('╔══════════════════════════════════╗')}           {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}      {Colors.title('║')}  {Colors.gradient('██╗░░██╗░██████╗░██╗░░░░░██╗░░░░░', (100, 200, 255), (255, 100, 200))}  {Colors.title('║')}           {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}      {Colors.title('║')}  {Colors.gradient('██║░░██║██╔════╝░██║░░░░░██║░░░░░', (100, 200, 255), (255, 100, 200))}  {Colors.title('║')}           {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}      {Colors.title('║')}  {Colors.gradient('███████║██║░░██╗░██║░░░░░██║░░░░░', (100, 200, 255), (255, 100, 200))}  {Colors.title('║')}           {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}      {Colors.title('║')}  {Colors.gradient('██╔══██║██║░░╚██╗██║░░░░░██║░░░░░', (100, 200, 255), (255, 100, 200))}  {Colors.title('║')}           {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}      {Colors.title('║')}  {Colors.gradient('██║░░██║╚██████╔╝███████╗███████╗', (100, 200, 255), (255, 100, 200))}  {Colors.title('║')}           {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}      {Colors.title('║')}  {Colors.gradient('╚═╝░░╚═╝░╚═════╝░╚══════╝╚══════╝', (100, 200, 255), (255, 100, 200))}  {Colors.title('║')}           {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}      {Colors.title('╚══════════════════════════════════╝')}           {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}                                                              {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}           {Colors.highlight('G raphical')} {Colors.dim('|')} {Colors.highlight('S tructured')} {Colors.dim('|')} {Colors.highlight('Q uery')} {Colors.dim('|')} {Colors.highlight('L anguage')}          {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}                                                              {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}          {Colors.info(f'Version {__version__}')} • {Colors.warning('SQLite Backend')}           {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}║{Colors.RESET}                                                              {Colors.rgb(100, 200, 255)}║{Colors.RESET}
{Colors.rgb(100, 200, 255)}╚══════════════════════════════════════════════════════════════╝{Colors.RESET}
        """
        print(banner.strip())
    
    @staticmethod
    def create_box(title: str, content: str, style: str = 'rounded', 
                   width: int = 60) -> str:
        """Create a bordered box"""
        chars = BoxChars.get_style(style)
        
        # Prepare content lines
        lines = content.split('\n')
        max_line_len = max(len(line) for line in lines) if lines else 0
        box_width = max(width, max_line_len + 4)
        
        # Create box
        result = []
        
        # Top border with title
        title_str = f" {title} " if title else ""
        top_line = (chars['top_left'] + 
                   chars['horizontal'] * (box_width - 2) + 
                   chars['top_right'])
        
        if title_str:
            title_pos = (box_width - len(title_str)) // 2
            top_line = (chars['top_left'] + 
                       chars['horizontal'] * (title_pos - 1) +
                       title_str +
                       chars['horizontal'] * (box_width - title_pos - len(title_str) - 1) +
                       chars['top_right'])
        
        result.append(Colors.colorize(top_line, Colors.BRIGHT_BLUE))
        
        # Content
        for line in lines:
            padded_line = line.ljust(box_width - 2)
            result.append(f"{Colors.colorize(chars['vertical'], Colors.BRIGHT_BLUE)}"
                         f"{Colors.colorize(padded_line, Colors.WHITE)}"
                         f"{Colors.colorize(chars['vertical'], Colors.BRIGHT_BLUE)}")
        
        # Bottom border
        bottom_line = (chars['bottom_left'] + 
                      chars['horizontal'] * (box_width - 2) + 
                      chars['bottom_right'])
        result.append(Colors.colorize(bottom_line, Colors.BRIGHT_BLUE))
        
        return '\n'.join(result)
    
    @staticmethod
    def print_table(headers: List[str], rows: List[List[Any]], 
                    title: str = "", max_width: int = 80) -> None:
        """Print a formatted table"""
        if not rows:
            print(Colors.warning("No data to display"))
            return
        
        # Calculate column widths
        col_widths = []
        for i, header in enumerate(headers):
            max_len = len(str(header))
            for row in rows:
                if i < len(row):
                    max_len = max(max_len, len(str(row[i])))
            col_widths.append(min(max_len, 30))  # Limit width
        
        # Adjust for total width
        total_width = sum(col_widths) + (len(headers) - 1) * 3 + 4
        if total_width > max_width:
            scale = max_width / total_width
            col_widths = [int(w * scale) for w in col_widths]
        
        # Create table
        chars = BoxChars.get_style('rounded')
        
        # Top border
        top_line = chars['top_left']
        for i, width in enumerate(col_widths):
            top_line += chars['horizontal'] * (width + 2)
            if i < len(col_widths) - 1:
                top_line += chars['top_tee']
        top_line += chars['top_right']
        
        # Header
        header_line = chars['vertical']
        for i, (header, width) in enumerate(zip(headers, col_widths)):
            header_line += f" {Colors.highlight(header.ljust(width))} {chars['vertical']}"
        
        # Separator
        sep_line = chars['left_tee']
        for i, width in enumerate(col_widths):
            sep_line += chars['horizontal'] * (width + 2)
            if i < len(col_widths) - 1:
                sep_line += chars['cross']
        sep_line += chars['right_tee']
        
        # Rows
        row_lines = []
        for row in rows:
            row_line = chars['vertical']
            for i, (cell, width) in enumerate(zip(row, col_widths)):
                cell_str = str(cell)[:width-3] + "..." if len(str(cell)) > width else str(cell)
                colored_cell = Colors.sql_number(cell_str) if isinstance(cell, (int, float)) else cell_str
                row_line += f" {colored_cell.ljust(width)} {chars['vertical']}"
            row_lines.append(row_line)
        
        # Bottom border
        bottom_line = chars['bottom_left']
        for i, width in enumerate(col_widths):
            bottom_line += chars['horizontal'] * (width + 2)
            if i < len(col_widths) - 1:
                bottom_line += chars['bottom_tee']
        bottom_line += chars['bottom_right']
        
        # Print table
        if title:
            print(f"\n{Colors.info(title)}")
        
        print(Colors.colorize(top_line, Colors.BRIGHT_BLUE))
        print(header_line)
        print(Colors.colorize(sep_line, Colors.BRIGHT_BLUE))
        for row_line in row_lines:
            print(row_line)
        print(Colors.colorize(bottom_line, Colors.BRIGHT_BLUE))
    
    @staticmethod
    def progress_bar(iteration: int, total: int, length: int = 40, 
                     prefix: str = "", suffix: str = "") -> str:
        """Create a progress bar"""
        percent = int(100 * (iteration / float(total)))
        filled_length = int(length * iteration // total)
        bar = Colors.BRIGHT_GREEN + '█' * filled_length + Colors.DIM + '░' * (length - filled_length)
        
        return f"{prefix} |{bar}{Colors.RESET}| {percent}% {suffix}"
    
    @staticmethod
    def print_status(message: str, status: str = "info") -> None:
        """Print a status message with icon"""
        icons = {
            "success": "✓",
            "error": "✗",
            "warning": "⚠",
            "info": "ℹ",
            "question": "?",
            "hourglass": "⏳",
            "check": "✔"
        }
        
        colors = {
            "success": Colors.BRIGHT_GREEN,
            "error": Colors.BRIGHT_RED,
            "warning": Colors.BRIGHT_YELLOW,
            "info": Colors.BRIGHT_CYAN,
            "question": Colors.BRIGHT_MAGENTA,
            "hourglass": Colors.BRIGHT_BLUE,
            "check": Colors.BRIGHT_GREEN
        }
        
        icon = icons.get(status, "•")
        color = colors.get(status, Colors.WHITE)
        
        print(f"{color}{icon}{Colors.RESET} {message}")

# ==================== AUTOCOMPLETER ====================

class GSQLCompleter:
    """Rich autocompletion for GSQL shell"""
    
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
        """Completion function for readline with rich suggestions"""
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
    """Rich display of query results"""
    
    MAX_ROWS_DISPLAY = 50
    COLUMN_WIDTH_LIMIT = 30
    
    def __init__(self, rich_ui: bool = True):
        self.rich_ui = rich_ui
    
    def display_select(self, result: Dict, execution_time: float) -> None:
        """Display SELECT query results with rich formatting"""
        rows = result.get('rows', [])
        columns = result.get('columns', [])
        count = result.get('count', 0)
        
        if count == 0:
            RichUI.print_status("No rows returned", "info")
            return
        
        if self.rich_ui:
            # Convert rows to list of lists for table display
            table_data = []
            for row in rows[:self.MAX_ROWS_DISPLAY]:
                if isinstance(row, (list, tuple)):
                    table_data.append(list(row))
                else:
                    table_data.append([row])
            
            RichUI.print_table(columns, table_data, 
                             title=f"Query Results ({count} rows)")
            
            if len(rows) > self.MAX_ROWS_DISPLAY:
                remaining = len(rows) - self.MAX_ROWS_DISPLAY
                print(f"\n{Colors.dim(f'... and {remaining} more rows')}")
        else:
            # Simple display
            print(f"\n{Colors.info('Results:')}")
            header = " | ".join(Colors.highlight(col) for col in columns)
            print(header)
            print(Colors.dim('─' * len(header)))
            
            for i, row in enumerate(rows[:self.MAX_ROWS_DISPLAY]):
                if isinstance(row, (list, tuple)):
                    values = [str(v) if v is not None else "NULL" for v in row]
                else:
                    values = [str(row)]
                
                colored_values = []
                for val in values:
                    if val == "NULL":
                        colored_values.append(Colors.dim(val))
                    elif val.replace('.', '', 1).isdigit() and val.count('.') <= 1:
                        colored_values.append(Colors.sql_number(val))
                    else:
                        colored_values.append(val)
                
                print(" | ".join(colored_values))
        
        print(f"\n{Colors.dim(f'Total rows: {count} | Time: {execution_time:.3f}s')}")
    
    def display_insert(self, result: Dict) -> None:
        """Display INSERT query results"""
        if self.rich_ui:
            box_content = (f"Row inserted successfully!\n\n"
                         f"Last Insert ID: {result.get('last_insert_id', 'N/A')}\n"
                         f"Rows Affected: {result.get('rows_affected', 0)}")
            print(RichUI.create_box("INSERT SUCCESS", box_content, style='rounded'))
        else:
            print(Colors.success(f"✓ Row inserted (ID: {result.get('last_insert_id', 'N/A')})"))
            print(Colors.dim(f"Rows affected: {result.get('rows_affected', 0)}"))
    
    def display_update_delete(self, result: Dict) -> None:
        """Display UPDATE/DELETE query results"""
        rows_affected = result.get('rows_affected', 0)
        query_type = "UPDATE" if result.get('type') == 'update' else "DELETE"
        
        if self.rich_ui:
            icon = "✓" if rows_affected > 0 else "ℹ"
            color = Colors.BRIGHT_GREEN if rows_affected > 0 else Colors.BRIGHT_YELLOW
            
            box_content = (f"{query_type} query executed successfully!\n\n"
                         f"Rows Affected: {rows_affected}")
            
            if rows_affected == 0:
                box_content += "\n\nNote: No rows matched the WHERE condition"
            
            print(RichUI.create_box(f"{query_type} SUCCESS", box_content, style='rounded'))
        else:
            print(Colors.success(f"Query successful"))
            print(Colors.dim(f"Rows affected: {rows_affected}"))
    
    def display_show_tables(self, result: Dict) -> None:
        """Display SHOW TABLES results"""
        tables = result.get('tables', [])
        
        if not tables:
            RichUI.print_status("No tables found in database", "warning")
            return
        
        if self.rich_ui:
            # Prepare table data
            headers = ["Table Name", "Rows", "Type"]
            table_data = []
            
            for table in tables:
                table_data.append([
                    Colors.sql_table(table['table']),
                    str(table.get('rows', 0)),
                    table.get('type', 'table')
                ])
            
            RichUI.print_table(headers, table_data, 
                             title=f"Database Tables ({len(tables)} tables)")
        else:
            print(Colors.success(f"Found {len(tables)} table(s):"))
            for table in tables:
                print(f"  • {Colors.highlight(table['table'])} ({table.get('rows', 0)} rows)")
    
    def display_describe(self, result: Dict) -> None:
        """Display DESCRIBE results"""
        columns = result.get('columns', [])
        
        if not columns:
            RichUI.print_status("No columns found", "warning")
            return
        
        if self.rich_ui:
            # Prepare table data
            headers = ["Column", "Type", "Null", "Key", "Default", "Extra"]
            table_data = []
            
            for col in columns:
                table_data.append([
                    Colors.sql_column(col['field']),
                    col['type'],
                    "✓" if col.get('null') else "✗",
                    col.get('key', ''),
                    col.get('default', 'NULL'),
                    col.get('extra', '')
                ])
            
            table_name = result.get('table_name', 'Unknown Table')
            RichUI.print_table(headers, table_data, 
                             title=f"Table Structure: {table_name}")
        else:
            print(Colors.success("Table structure:"))
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
        
        if self.rich_ui:
            content_lines = []
            
            def add_stat(key: str, value: Any, level: int = 0) -> None:
                prefix = "  " * level
                if isinstance(value, dict):
                    content_lines.append(f"{prefix}{Colors.highlight(key)}:")
                    for k, v in value.items():
                        add_stat(k, v, level + 1)
                else:
                    content_lines.append(f"{prefix}{Colors.sql_column(key)}: {Colors.sql_number(str(value))}")
            
            for key, value in stats.items():
                add_stat(key, value)
            
            content = "\n".join(content_lines)
            print(RichUI.create_box("DATABASE STATISTICS", content, style='double'))
        else:
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
    
    def display_generic(self, result: Dict, query_type: str, execution_time: float) -> None:
        """Display generic query results"""
        handlers = {
            'select': self.display_select,
            'insert': self.display_insert,
            'update': self.display_update_delete,
            'delete': self.display_update_delete,
            'show_tables': self.display_show_tables,
            'describe': self.display_describe,
            'stats': self.display_stats,
            'vacuum': lambda r: RichUI.print_status("Database optimized successfully", "success"),
            'backup': lambda r: RichUI.print_status(
                f"Backup created: {r.get('backup_file', 'N/A')}", "success"),
            'help': lambda r: print(r.get('message', ''))
        }
        
        handler = handlers.get(query_type)
        if handler:
            handler(result)
        else:
            if self.rich_ui:
                RichUI.print_status(f"Query executed successfully", "success")
            else:
                print(Colors.success(f"Query executed successfully"))
        
        # Show execution time in footer
        if self.rich_ui:
            time_str = f"{result.get('execution_time', execution_time):.3f}s"
            footer = f"Execution time: {Colors.sql_number(time_str)}"
            print(f"\n{Colors.dim('─' * 40)}")
            print(f"{Colors.dim(footer)}")
        else:
            time_str = f"{result.get('execution_time', execution_time):.3f}s"
            print(Colors.dim(f"Time: {time_str}"))

# ==================== SHELL ====================

class GSQLShell(cmd.Cmd):
    """GSQL Interactive Shell with rich UI"""
    
    intro = ""
    prompt = ""
    ruler = Colors.dim('─' * 60)
    
    def __init__(self, gsql_app=None):
        super().__init__()
        self.gsql = gsql_app
        self.db = gsql_app.db if gsql_app else None
        self.executor = gsql_app.executor if gsql_app else None
        self.completer = gsql_app.completer if gsql_app else None
        self.history_file = None
        self.rich_ui = gsql_app.config.get('shell', {}).get('rich_ui', True) if gsql_app else True
        self.result_display = ResultDisplay(self.rich_ui)
        
        self._setup_history()
        self._setup_autocomplete()
        self._setup_prompt()
        
        # Print banner if rich UI is enabled
        if self.rich_ui:
            RichUI.print_banner()
            self.intro = self._get_welcome_message()
        else:
            self.intro = Colors.info("GSQL Interactive Shell") + "\n" + Colors.dim("Type 'help' for commands, 'exit' to quit")
    
    def _get_welcome_message(self) -> str:
        """Get welcome message based on time of day"""
        hour = datetime.now().hour
        
        if hour < 12:
            greeting = "Good morning"
        elif hour < 18:
            greeting = "Good afternoon"
        else:
            greeting = "Good evening"
        
        welcome = f"{greeting}! Welcome to GSQL {__version__}"
        
        # Create welcome box
        content = (f"{welcome}\n\n"
                  f"Type {Colors.highlight('.help')} for available commands\n"
                  f"Type {Colors.highlight('.tables')} to list all tables\n"
                  f"Type {Colors.highlight('exit')} or {Colors.highlight('.quit')} to exit")
        
        return RichUI.create_box("WELCOME", content, style='rounded')
    
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
        if self.rich_ui:
            self.prompt = (f"{Colors.rgb(100, 200, 255)}┌──({Colors.RESET}"
                          f"{Colors.rgb(255, 100, 200)}GSQL{Colors.RESET}"
                          f"{Colors.rgb(100, 200, 255)})─[{Colors.RESET}"
                          f"{Colors.rgb(100, 255, 200)}${{db_name}}{Colors.RESET}"
                          f"{Colors.rgb(100, 200, 255)}]\n└─{Colors.RESET}"
                          f"{Colors.rgb(255, 200, 100)}${Colors.RESET} ")
        else:
            self.prompt = Colors.info('gsql> ')
    
    def _update_prompt(self) -> None:
        """Update prompt with current database info"""
        if not self.db or not hasattr(self.db, 'storage'):
            db_name = "no-db"
        else:
            db_path = getattr(self.db.storage, 'db_path', 'unknown')
            db_name = Path(db_path).stem or "main"
        
        # Update prompt with current database
        if self.rich_ui:
            self.prompt = (f"{Colors.rgb(100, 200, 255)}┌──({Colors.RESET}"
                          f"{Colors.rgb(255, 100, 200)}GSQL{Colors.RESET}"
                          f"{Colors.rgb(100, 200, 255)})─[{Colors.RESET}"
                          f"{Colors.rgb(100, 255, 200)}{db_name}{Colors.RESET}"
                          f"{Colors.rgb(100, 200, 255)}]\n└─{Colors.RESET}"
                          f"{Colors.rgb(255, 200, 100)}${Colors.RESET} ")
    
    def precmd(self, line: str) -> str:
        """Update prompt before each command"""
        self._update_prompt()
        if line and not line.startswith('.'):
            readline.add_history(line)
        return line
    
    def default(self, line: str) -> Optional[bool]:
        """Handle default commands (SQL queries)"""
        if not line.strip():
            return None
        
        if line.startswith('.'):
            return self._handle_dot_command(line)
        else:
            self._execute_sql(line)
            return None
    
    def _handle_dot_command(self, command: str) -> Optional[bool]:
        """Handle dot commands with rich UI"""
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
            'clear': self._clear_screen,
            'history': lambda: self._show_history(),
            'mode': lambda: self._handle_mode(args),
            'width': lambda: self._handle_width(args),
            'timer': lambda: self._handle_timer(args)
        }
        
        handler = handlers.get(cmd)
        if handler:
            result = handler()
            if result is True:  # For exit/quit
                return True
        else:
            if self.rich_ui:
                RichUI.print_status(f"Unknown command: .{cmd}", "error")
            else:
                print(Colors.error(f"Unknown command: .{cmd}"))
                print(Colors.dim("Try .help for available commands"))
        
        return False
    
    def _clear_screen(self) -> None:
        """Clear screen with style"""
        if self.rich_ui:
            os.system('clear' if os.name == 'posix' else 'cls')
            RichUI.print_banner()
            print(self._get_welcome_message())
        else:
            os.system('clear' if os.name == 'posix' else 'cls')
    
    def _handle_schema(self, args: List[str]) -> None:
        """Handle .schema command"""
        if not args:
            RichUI.print_status("Usage: .schema <table_name>", "error")
            return
        self._execute_sql(f"DESCRIBE {args[0]}")
    
    def _handle_backup(self, args: List[str]) -> None:
        """Handle .backup command"""
        if args:
            self._execute_sql(f"BACKUP {args[0]}")
        else:
            self._execute_sql("BACKUP")
    
    def _handle_mode(self, args: List[str]) -> None:
        """Handle .mode command"""
        if not args:
            RichUI.print_status("Current mode: rich" if self.rich_ui else "simple", "info")
            return
        
        mode = args[0].lower()
        if mode in ['rich', 'simple']:
            self.rich_ui = (mode == 'rich')
            self.result_display = ResultDisplay(self.rich_ui)
            RichUI.print_status(f"Display mode set to: {mode}", "success")
        else:
            RichUI.print_status("Available modes: rich, simple", "error")
    
    def _handle_width(self, args: List[str]) -> None:
        """Handle .width command"""
        if not args:
            RichUI.print_status("Usage: .width <columns>", "error")
            return
        
        try:
            widths = [int(w) for w in args]
            RichUI.print_status(f"Column widths set: {widths}", "success")
        except ValueError:
            RichUI.print_status("Invalid width values", "error")
    
    def _handle_timer(self, args: List[str]) -> None:
        """Handle .timer command"""
        if not args:
            RichUI.print_status("Usage: .timer on|off", "error")
            return
        
        if args[0].lower() in ['on', 'yes', 'true']:
            config.set('show_timer', True)
            RichUI.print_status("Query timer enabled", "success")
        else:
            config.set('show_timer', False)
            RichUI.print_status("Query timer disabled", "success")
    
    def _show_history(self) -> None:
        """Show command history with rich display"""
        try:
            hist_size = readline.get_current_history_length()
            if hist_size == 0:
                RichUI.print_status("No command history", "info")
                return
            
            if self.rich_ui:
                headers = ["#", "Command"]
                rows = []
                for i in range(1, min(hist_size, 20) + 1):
                    cmd = readline.get_history_item(i)
                    rows.append([str(i), cmd[:50] + "..." if len(cmd) > 50 else cmd])
                
                RichUI.print_table(headers, rows, title=f"Command History (last {len(rows)} commands)")
            else:
                for i in range(1, hist_size + 1):
                    cmd = readline.get_history_item(i)
                    print(f"{i:4d}  {cmd}")
                    
        except Exception:
            RichUI.print_status("Could not display history", "error")
    
    def _execute_sql(self, sql: str) -> None:
        """Execute SQL query with rich feedback"""
        if not self.db:
            RichUI.print_status("No database connection", "error")
            return
        
        try:
            sql = sql.strip()
            if not sql:
                return
            
            # Show executing status
            if self.rich_ui and len(sql) > 50:
                RichUI.print_status(f"Executing: {sql[:50]}...", "hourglass")
            elif self.rich_ui:
                RichUI.print_status(f"Executing: {sql}", "hourglass")
            
            start_time = datetime.now()
            result = self.db.execute(sql)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            if result.get('success'):
                self.result_display.display_generic(result, result.get('type', ''), execution_time)
            else:
                if self.rich_ui:
                    error_box = RichUI.create_box(
                        "QUERY ERROR",
                        f"Error: {result.get('message', 'Unknown error')}\n\n"
                        f"SQL: {sql[:100]}{'...' if len(sql) > 100 else ''}",
                        style='double'
                    )
                    print(error_box)
                else:
                    print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                
        except Exception as e:
            if self.rich_ui:
                error_box = RichUI.create_box(
                    "EXECUTION ERROR",
                    f"Exception: {str(e)}\n\n"
                    f"Check logs for details.",
                    style='double'
                )
                print(error_box)
            else:
                print(Colors.error(f"Error: {e}"))
            
            config_data = self.gsql.config if self.gsql else {}
            if config_data.get('verbose_errors', True):
                traceback.print_exc()
    
    # ==================== BUILT-IN COMMANDS ====================
    
    def do_help(self, arg: str) -> None:
        """Show help with rich formatting"""
        if self.rich_ui:
            help_content = """
{cyan}SQL COMMANDS:{reset}
  {bold}SELECT{reset} * FROM table [WHERE condition] [LIMIT n]
  {bold}INSERT{reset} INTO table (col1, col2) VALUES (val1, val2)
  {bold}UPDATE{reset} table SET col=value [WHERE condition]
  {bold}DELETE{reset} FROM table [WHERE condition]
  {bold}CREATE TABLE{reset} name (col1 TYPE, col2 TYPE, ...)
  {bold}DROP TABLE{reset} name
  {bold}ALTER TABLE{reset} name ADD COLUMN col TYPE
  {bold}CREATE INDEX{reset} idx_name ON table(column)

{cyan}GSQL SPECIAL COMMANDS:{reset}
  {bold}SHOW TABLES{reset}                    - List all tables
  {bold}DESCRIBE table{reset}                 - Show table structure
  {bold}STATS{reset}                          - Show database statistics
  {bold}VACUUM{reset}                         - Optimize database
  {bold}BACKUP [path]{reset}                  - Create database backup
  {bold}HELP{reset}                           - This help message

{cyan}DOT COMMANDS (SQLite style):{reset}
  {bold}.tables{reset}                        - List tables
  {bold}.schema [table]{reset}                - Show schema
  {bold}.stats{reset}                         - Show stats
  {bold}.help{reset}                          - Show help
  {bold}.backup [file]{reset}                 - Create backup
  {bold}.vacuum{reset}                        - Optimize database
  {bold}.exit / .quit{reset}                  - Exit shell
  {bold}.clear{reset}                         - Clear screen
  {bold}.history{reset}                       - Show command history
  {bold}.mode [rich|simple]{reset}            - Change display mode
  {bold}.width <cols>{reset}                  - Set column widths
  {bold}.timer [on|off]{reset}                - Toggle query timer

{cyan}SHELL COMMANDS:{reset}
  {bold}exit, quit, Ctrl+D{reset}             - Exit GSQL
  {bold}Ctrl+C{reset}                         - Cancel current command
  {bold}Ctrl+L{reset}                         - Clear screen
            """.format(
                cyan=Colors.BRIGHT_CYAN,
                bold=Colors.BOLD,
                reset=Colors.RESET
            )
            
            print(RichUI.create_box("GSQL HELP", help_content, style='double', width=70))
        else:
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
  .mode [rich|simple]            - Change display mode
  .width <cols>                  - Set column widths
  .timer [on|off]                - Toggle query timer

SHELL COMMANDS:
  exit, quit, Ctrl+D             - Exit GSQL
  Ctrl+C                         - Cancel current command
            """
            print(help_text.strip())
    
    def do_exit(self, arg: str) -> bool:
        """Exit GSQL shell with style"""
        if self.rich_ui:
            goodbye = f"Thank you for using GSQL {__version__}!"
            print(RichUI.create_box("GOODBYE", goodbye, style='rounded'))
        else:
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
        """Handle Ctrl+C with style"""
        if self.rich_ui:
            print(f"\n{Colors.BRIGHT_YELLOW}⚠{Colors.RESET} {Colors.warning('Command interrupted (Ctrl+C)')}")
        else:
            print(f"\n{Colors.warning('Interrupted (Ctrl+C)')}")
        self._update_prompt()

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
        if self.config['shell'].get('rich_ui', True):
            print(Colors.rgb(100, 200, 255) + "╔══════════════════════════════════════════════════════════════╗")
            print("║                   Initializing GSQL...                   ║")
            print("╚══════════════════════════════════════════════════════════════╝" + Colors.RESET)
        else:
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
            
            if self.config['shell'].get('rich_ui', True):
                RichUI.print_status("✓ GSQL initialized successfully", "success")
                print(Colors.dim(f"Database: {self.db.storage.db_path}"))
            else:
                print(Colors.success("✓ GSQL ready!"))
                print(Colors.dim(f"Database: {self.db.storage.db_path}"))
                print(Colors.dim("Type 'help' for commands\n"))
            
        except Exception as e:
            if self.config['shell'].get('rich_ui', True):
                error_box = RichUI.create_box(
                    "INITIALIZATION ERROR",
                    f"Failed to initialize GSQL:\n\n{str(e)}",
                    style='double'
                )
                print(error_box)
            else:
                print(Colors.error(f"Failed to initialize GSQL: {e}"))
            
            traceback.print_exc()
            sys.exit(1)
    
    def _setup_nlp(self) -> None:
        """Setup NLP features if available"""
        if NLP_AVAILABLE and NLToSQLTranslator:
            self.nlp_translator = NLToSQLTranslator()
        elif self.config['executor'].get('enable_nlp', False):
            if self.config['shell'].get('rich_ui', True):
                RichUI.print_status("NLP features not available. Install NLTK for NLP support.", "warning")
            else:
                print(Colors.warning("NLP features not available. Install NLTK for NLP support."))
    
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
    
    def run_query(self, query: str, database_path: Optional[str] = None) -> Optional[Dict]:
        """Execute single query"""
        try:
            self.initialize(database_path)
            result = self.db.execute(query)
            
            if result.get('success'):
                if self.config['shell'].get('rich_ui', True):
                    RichUI.print_status("Query executed successfully", "success")
                else:
                    print(Colors.success("Query executed successfully"))
                
                # Display SELECT results
                if result.get('type') == 'select':
                    rows = result.get('rows', [])
                    if rows:
                        display = ResultDisplay(self.config['shell'].get('rich_ui', True))
                        display.display_select(result, result.get('execution_time', 0))
                    else:
                        RichUI.print_status("No rows returned", "info")
                
                # Show execution time
                if 'execution_time' in result:
                    time_str = f"{result['execution_time']:.3f}s"
                    print(f"\n{Colors.dim(f'Time: {time_str}')}")
                
                return result
            else:
                if self.config['shell'].get('rich_ui', True):
                    RichUI.print_status(f"Query failed: {result.get('message', 'Unknown error')}", "error")
                else:
                    print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                return None
                
        except Exception as e:
            if self.config['shell'].get('rich_ui', True):
                RichUI.print_status(f"Error: {e}", "error")
            else:
                print(Colors.error(f"Error: {e}"))
            return None
        finally:
            self.cleanup()
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            if self.db:
                self.db.close()
                if self.config['shell'].get('rich_ui', True):
                    RichUI.print_status("Database connection closed", "info")
                else:
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
        '--simple-ui',
        action='store_true',
        help='Use simple UI instead of rich UI'
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
    
    # Configure UI mode
    if args.simple_ui:
        config.set('shell.rich_ui', False)
    
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
