#!/usr/bin/env python3
"""
GSQL CLI - Interface en ligne de commande complète
"""

import sys
import os
import cmd
import shlex
import logging
import argparse
import readline
import atexit
from pathlib import Path
from typing import List, Optional

# Configuration du logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gsql.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== SYSTEME DE COULEURS ====================

class Colors:
    """Gestion des couleurs ANSI"""
    
    # Couleurs de base
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    # Styles
    BRIGHT = '\033[1m'
    DIM = '\033[2m'
    NORMAL = '\033[22m'
    RESET = '\033[0m'
    
    # Backgrounds
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'
    
    # Combinaisons pratiques
    PROMPT = BRIGHT + GREEN
    SUCCESS = BRIGHT + GREEN
    ERROR = BRIGHT + RED
    WARNING = BRIGHT + YELLOW
    INFO = BRIGHT + CYAN
    HEADER = BRIGHT + BLUE
    SQL = BRIGHT + YELLOW
    DATA = WHITE
    COLUMN = CYAN
    
    @staticmethod
    def colorize_sql(sql: str) -> str:
        """Colorise le code SQL"""
        if not sql:
            return sql
        
        # Mots-clés SQL
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
            'ALTER', 'JOIN', 'GROUP BY', 'ORDER BY', 'LIMIT',
            'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL',
            'TRUE', 'FALSE', 'ASC', 'DESC', 'DISTINCT', 'UNIQUE',
            'PRIMARY KEY', 'FOREIGN KEY', 'REFERENCES', 'CONSTRAINT',
            'INDEX', 'VIEW', 'TRIGGER', 'BEGIN', 'END', 'COMMIT',
            'ROLLBACK', 'EXPLAIN', 'VACUUM', 'HAVING', 'OFFSET'
        ]
        
        colored = sql
        
        # Coloriser les mots-clés
        for keyword in keywords:
            import re
            pattern = r'\b' + re.escape(keyword) + r'\b'
            replacement = Colors.SQL + keyword + Colors.RESET
            colored = re.sub(pattern, replacement, colored, flags=re.IGNORECASE)
        
        # Coloriser les chaînes
        import re
        colored = re.sub(r"'[^']*'", Colors.GREEN + r'\g<0>' + Colors.RESET, colored)
        colored = re.sub(r'"[^"]*"', Colors.GREEN + r'\g<0>' + Colors.RESET, colored)
        
        # Coloriser les nombres
        colored = re.sub(r'\b\d+(\.\d+)?\b', Colors.YELLOW + r'\g<0>' + Colors.RESET, colored)
        
        return colored

# ==================== GESTION DE L'HISTORIQUE ====================

class HistoryManager:
    """Gestionnaire d'historique des commandes"""
    
    def __init__(self, history_file: str = ".gsql_history"):
        self.history_file = Path.home() / history_file
        self.history_file.touch(exist_ok=True)
        self.max_length = 1000
        
        # Charger l'historique
        readline.read_history_file(str(self.history_file))
        readline.set_history_length(self.max_length)
        
        # Configuration readline
        readline.parse_and_bind('tab: complete')
        readline.set_completer(self._completer)
        
        # Sauvegarder à la sortie
        atexit.register(self.save_history)
    
    def save_history(self):
        """Sauvegarde l'historique"""
        readline.write_history_file(str(self.history_file))
    
    def _completer(self, text: str, state: int) -> Optional[str]:
        """Auto-complétion des commandes"""
        commands = [
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP',
            'ALTER', 'SHOW', 'HELP', 'EXIT', 'QUIT', 'CLEAR',
            'BEGIN', 'COMMIT', 'ROLLBACK', 'EXPLAIN', 'DESCRIBE',
            '.tables', '.help', '.exit', '.quit', '.clear', '.schema',
            '.version', '.history', '.import', '.export', '.stats'
        ]
        
        matches = [cmd for cmd in commands if cmd.lower().startswith(text.lower())]
        
        if state < len(matches):
            return matches[state]
        return None

# ==================== FORMATAGE DES RESULTATS ====================

class ResultFormatter:
    """Formate les résultats de requêtes"""
    
    @staticmethod
    def format_table(headers: List[str], rows: List[List], max_rows: int = 50) -> str:
        """Formate un tableau"""
        if not rows:
            return f"{Colors.WARNING}📭 No data to display{Colors.RESET}"
        
        # Calculer les largeurs de colonnes
        col_widths = []
        for i, header in enumerate(headers):
            max_width = len(str(header))
            for row in rows[:max_rows]:
                if i < len(row):
                    max_width = max(max_width, len(str(row[i])))
            col_widths.append(max_width)
        
        # Construire le tableau
        output = []
        
        # En-tête
        header_line = " │ ".join(
            f"{Colors.HEADER}{str(h).ljust(w)}{Colors.RESET}" 
            for h, w in zip(headers, col_widths)
        )
        output.append(header_line)
        
        # Séparateur
        separator = "─┼─".join("─" * w for w in col_widths)
        output.append(separator)
        
        # Lignes
        for i, row in enumerate(rows[:max_rows]):
            row_values = []
            for j, value in enumerate(row):
                if j < len(col_widths):
                    cell = str(value)[:col_widths[j]].ljust(col_widths[j])
                    # Alterner les couleurs des lignes
                    if i % 2 == 0:
                        row_values.append(f"{Colors.DATA}{cell}{Colors.RESET}")
                    else:
                        row_values.append(f"{Colors.DIM}{cell}{Colors.RESET}")
            
            output.append(" │ ".join(row_values))
        
        # Message si trop de lignes
        if len(rows) > max_rows:
            remaining = len(rows) - max_rows
            output.append(f"{Colors.INFO}... and {remaining} more rows{Colors.RESET}")
        
        return "\n".join(output)
    
    @staticmethod
    def format_result(result) -> str:
        """Formate un résultat quelconque"""
        if result is None:
            return f"{Colors.SUCCESS}✅ Command executed successfully{Colors.RESET}"
        
        if isinstance(result, str):
            return f"{Colors.INFO}📋 {result}{Colors.RESET}"
        
        if isinstance(result, dict):
            return ResultFormatter._format_dict_result(result)
        
        if isinstance(result, list):
            return ResultFormatter._format_list_result(result)
        
        return f"{Colors.INFO}📋 {result}{Colors.RESET}"
    
    @staticmethod
    def _format_dict_result(result: dict) -> str:
        """Formate un résultat dictionnaire"""
        # Résultat de type tables
        if result.get('type') == 'tables' and 'rows' in result:
            rows = result['rows']
            if not rows:
                return f"{Colors.WARNING}📭 No tables found{Colors.RESET}"
            
            output = [f"{Colors.INFO}📊 {result.get('message', 'Tables:')}{Colors.RESET}"]
            
            for table in rows:
                name = table.get('table', table.get('name', 'unknown'))
                row_count = table.get('rows', table.get('row_count', 0))
                columns = table.get('columns', 'N/A')
                
                output.append(
                    f"  {Colors.COLUMN}{name:20}{Colors.RESET} | "
                    f"{Colors.DATA}{row_count:4} rows{Colors.RESET} | "
                    f"Columns: {columns}"
                )
            
            return "\n".join(output)
        
        # Résultat de type select
        if result.get('type') == 'select' and 'rows' in result:
            rows = result['rows']
            count = result.get('count', len(rows))
            
            if count == 0:
                return f"{Colors.WARNING}📭 No results found{Colors.RESET}"
            
            if not rows:
                return f"{Colors.INFO}📊 Empty result set{Colors.RESET}"
            
            # Extraire les en-têtes
            if rows and isinstance(rows[0], dict):
                headers = list(rows[0].keys())
                table_rows = [[row.get(h, '') for h in headers] for row in rows]
            else:
                headers = ['Result']
                table_rows = [[str(row)] for row in rows]
            
            output = [
                f"{Colors.INFO}📊 Results: {count} row(s){Colors.RESET}",
                ResultFormatter.format_table(headers, table_rows)
            ]
            
            return "\n".join(output)
        
        # Message simple
        if 'message' in result:
            msg_type = result.get('type', 'info')
            
            if msg_type == 'error':
                icon = "❌"
                color = Colors.ERROR
            elif msg_type == 'warning':
                icon = "⚠"
                color = Colors.WARNING
            elif msg_type == 'success':
                icon = "✅"
                color = Colors.SUCCESS
            else:
                icon = "📋"
                color = Colors.INFO
            
            return f"{color}{icon} {result['message']}{Colors.RESET}"
        
        # Affichage JSON
        import json
        return f"{Colors.INFO}📋 {json.dumps(result, indent=2, default=str)}{Colors.RESET}"
    
    @staticmethod
    def _format_list_result(result: list) -> str:
        """Formate un résultat liste"""
        if not result:
            return f"{Colors.WARNING}📭 No results{Colors.RESET}"
        
        output = [f"{Colors.INFO}📊 Results: {len(result)} item(s){Colors.RESET}"]
        
        for i, item in enumerate(result[:50], 1):
            if isinstance(item, dict):
                item_str = str(item)[:100] + ("..." if len(str(item)) > 100 else "")
            else:
                item_str = str(item)
            
            # Alterner les couleurs
            if i % 2 == 0:
                output.append(f"  {Colors.DATA}{i:3}. {item_str}{Colors.RESET}")
            else:
                output.append(f"  {Colors.DIM}{i:3}. {item_str}{Colors.RESET}")
        
        if len(result) > 50:
            remaining = len(result) - 50
            output.append(f"{Colors.INFO}... and {remaining} more items{Colors.RESET}")
        
        return "\n".join(output)

# ==================== BASE DE DONNEES SIMPLE ====================

class SimpleDatabase:
    """Base de données simple avec données d'exemple"""
    
    def __init__(self, db_path: str = ":memory:"):
        import sqlite3
        import tempfile
        
        if db_path == ":memory:":
            # Mode mémoire
            self.conn = sqlite3.connect(":memory:")
            self.temp_file = None
        else:
            # Mode fichier
            if not db_path.endswith('.db'):
                db_path += '.db'
            self.conn = sqlite3.connect(db_path)
            self.temp_file = None
        
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        
        # Initialiser avec des données d'exemple
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialise la base avec des tables et données"""
        
        # Table users
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                full_name TEXT,
                age INTEGER,
                city TEXT,
                country TEXT DEFAULT 'France',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1
            )
        """)
        
        # Données d'exemple pour users
        self.cursor.execute("SELECT COUNT(*) FROM users")
        if self.cursor.fetchone()[0] == 0:
            users = [
                ('alice', 'alice@example.com', 'Alice Dupont', 28, 'Paris', 'France'),
                ('bob', 'bob@example.com', 'Bob Martin', 35, 'Lyon', 'France'),
                ('charlie', 'charlie@example.com', 'Charlie Durand', 42, 'Marseille', 'France'),
                ('diana', 'diana@example.com', 'Diana Leroy', 31, 'Toulouse', 'France'),
                ('eric', 'eric@example.com', 'Eric Petit', 26, 'Nice', 'France'),
                ('fiona', 'fiona@example.com', 'Fiona Bernard', 39, 'Bordeaux', 'France'),
                ('george', 'george@example.com', 'George Lambert', 45, 'Lille', 'France'),
                ('helen', 'helen@example.com', 'Helen Moreau', 29, 'Strasbourg', 'France')
            ]
            self.cursor.executemany(
                "INSERT INTO users (username, email, full_name, age, city, country) VALUES (?, ?, ?, ?, ?, ?)",
                users
            )
        
        # Table products
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                price REAL NOT NULL,
                category TEXT,
                stock_quantity INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_available INTEGER DEFAULT 1
            )
        """)
        
        # Données d'exemple pour products
        self.cursor.execute("SELECT COUNT(*) FROM products")
        if self.cursor.fetchone()[0] == 0:
            products = [
                ('Laptop Pro', 'High-performance laptop', 1299.99, 'Electronics', 15),
                ('Smartphone X', 'Latest smartphone model', 899.99, 'Electronics', 30),
                ('Tablet Lite', 'Lightweight tablet', 499.99, 'Electronics', 25),
                ('Wireless Headphones', 'Noise-cancelling headphones', 199.99, 'Audio', 40),
                ('Desk Chair', 'Ergonomic office chair', 299.99, 'Furniture', 10),
                ('Coffee Maker', 'Automatic coffee machine', 89.99, 'Kitchen', 50),
                ('Backpack', 'Water-resistant backpack', 49.99, 'Accessories', 100),
                ('Smart Watch', 'Fitness tracking watch', 249.99, 'Wearables', 20)
            ]
            self.cursor.executemany(
                "INSERT INTO products (name, description, price, category, stock_quantity) VALUES (?, ?, ?, ?, ?)",
                products
            )
        
        # Table orders
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                product_id INTEGER,
                quantity INTEGER NOT NULL,
                total_price REAL NOT NULL,
                status TEXT DEFAULT 'pending',
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # Données d'exemple pour orders
        self.cursor.execute("SELECT COUNT(*) FROM orders")
        if self.cursor.fetchone()[0] == 0:
            orders = [
                (1, 1, 1, 1299.99, 'completed'),
                (1, 3, 2, 999.98, 'completed'),
                (2, 2, 1, 899.99, 'shipped'),
                (3, 4, 1, 199.99, 'pending'),
                (4, 5, 1, 299.99, 'processing'),
                (5, 6, 3, 269.97, 'completed')
            ]
            self.cursor.executemany(
                "INSERT INTO orders (user_id, product_id, quantity, total_price, status) VALUES (?, ?, ?, ?, ?)",
                orders
            )
        
        # Table employees
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                department TEXT,
                salary REAL,
                hire_date TEXT,
                email TEXT UNIQUE
            )
        """)
        
        # Données d'exemple pour employees
        self.cursor.execute("SELECT COUNT(*) FROM employees")
        if self.cursor.fetchone()[0] == 0:
            employees = [
                ('John', 'Doe', 'IT', 75000, '2020-01-15', 'john.doe@company.com'),
                ('Jane', 'Smith', 'HR', 65000, '2019-03-22', 'jane.smith@company.com'),
                ('Robert', 'Johnson', 'Sales', 85000, '2018-07-10', 'robert.johnson@company.com'),
                ('Emily', 'Davis', 'Marketing', 70000, '2021-05-30', 'emily.davis@company.com'),
                ('Michael', 'Wilson', 'IT', 80000, '2017-11-05', 'michael.wilson@company.com')
            ]
            self.cursor.executemany(
                "INSERT INTO employees (first_name, last_name, department, salary, hire_date, email) VALUES (?, ?, ?, ?, ?, ?)",
                employees
            )
        
        self.conn.commit()
    
    def execute(self, sql: str):
        """Exécute une commande SQL"""
        sql = sql.strip()
        sql_upper = sql.upper()
        
        # Commandes spéciales
        if sql_upper == "SHOW TABLES" or sql_upper == ".TABLES":
            return self._show_tables()
        elif sql_upper == "SHOW FUNCTIONS":
            return self._show_functions()
        elif sql_upper == "HELP":
            return self._show_help()
        
        try:
            self.cursor.execute(sql)
            
            if sql_upper.startswith("SELECT"):
                rows = self.cursor.fetchall()
                column_names = [desc[0] for desc in self.cursor.description] if self.cursor.description else ['Result']
                
                result_rows = []
                for row in rows:
                    if len(column_names) == 1:
                        result_rows.append({column_names[0]: row[0]})
                    else:
                        result_rows.append(dict(zip(column_names, row)))
                
                return {
                    'type': 'select',
                    'rows': result_rows,
                    'count': len(result_rows),
                    'columns': column_names
                }
            else:
                self.conn.commit()
                return {
                    'type': 'command',
                    'message': 'Command executed successfully',
                    'rows_affected': self.cursor.rowcount
                }
                
        except Exception as e:
            return {
                'type': 'error',
                'message': f'SQL Error: {str(e)}'
            }
    
    def _show_tables(self):
        """Affiche la liste des tables"""
        self.cursor.execute("""
            SELECT name as table_name
            FROM sqlite_master 
            WHERE type='table' 
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        
        tables = []
        for row in self.cursor.fetchall():
            table_name = row['table_name']
            
            # Compter les lignes
            try:
                self.cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                row_count = self.cursor.fetchone()['count']
            except:
                row_count = 0
            
            # Récupérer les colonnes
            try:
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col['name'] for col in self.cursor.fetchall()]
                columns_str = ', '.join(columns[:3])
                if len(columns) > 3:
                    columns_str += f'... (+{len(columns)-3})'
            except:
                columns_str = 'N/A'
            
            tables.append({
                'table': table_name,
                'rows': row_count,
                'columns': columns_str
            })
        
        return {
            'type': 'tables',
            'rows': tables,
            'count': len(tables),
            'message': f'Found {len(tables)} table(s)'
        }
    
    def _show_functions(self):
        """Affiche la liste des fonctions"""
        functions = [
            {'name': 'COUNT()', 'type': 'Aggregate', 'description': 'Count rows'},
            {'name': 'SUM()', 'type': 'Aggregate', 'description': 'Sum values'},
            {'name': 'AVG()', 'type': 'Aggregate', 'description': 'Average'},
            {'name': 'MIN()', 'type': 'Aggregate', 'description': 'Minimum'},
            {'name': 'MAX()', 'type': 'Aggregate', 'description': 'Maximum'},
            {'name': 'UPPER()', 'type': 'String', 'description': 'Uppercase'},
            {'name': 'LOWER()', 'type': 'String', 'description': 'Lowercase'},
            {'name': 'LENGTH()', 'type': 'String', 'description': 'String length'},
            {'name': 'SUBSTR()', 'type': 'String', 'description': 'Substring'},
            {'name': 'TRIM()', 'type': 'String', 'description': 'Trim spaces'},
            {'name': 'DATE()', 'type': 'Date', 'description': 'Current date'},
            {'name': 'TIME()', 'type': 'Date', 'description': 'Current time'},
            {'name': 'DATETIME()', 'type': 'Date', 'description': 'Current datetime'},
            {'name': 'ABS()', 'type': 'Math', 'description': 'Absolute value'},
            {'name': 'ROUND()', 'type': 'Math', 'description': 'Round number'},
            {'name': 'RANDOM()', 'type': 'Math', 'description': 'Random number'}
        ]
        
        return {
            'type': 'functions',
            'rows': functions,
            'count': len(functions),
            'message': f'Found {len(functions)} function(s)'
        }
    
    def _show_help(self):
        """Affiche l'aide"""
        help_text = f"""
{Colors.HEADER}╔══════════════════════════════════════════════════╗
║               GSQL COMMAND REFERENCE                ║
╚══════════════════════════════════════════════════╝{Colors.RESET}

{Colors.PROMPT}📊 DATABASE COMMANDS:{Colors.RESET}
  .tables                    - List all tables
  .schema [table]            - Show table structure
  .import FILE TABLE         - Import CSV data
  .export TABLE FILE         - Export table to CSV
  .stats                     - Show database statistics
  .dump                      - Export entire database

{Colors.PROMPT}🔍 QUERY COMMANDS:{Colors.RESET}
  SELECT * FROM table        - Query all data
  SELECT col1, col2 FROM table WHERE condition
  SELECT * FROM table ORDER BY col [ASC|DESC]
  SELECT * FROM table LIMIT N
  SELECT * FROM table GROUP BY col
  SELECT COUNT(*), AVG(col) FROM table

{Colors.PROMPt}📝 DATA MANIPULATION:{Colors.RESET}
  INSERT INTO table (col1, col2) VALUES (val1, val2)
  UPDATE table SET col=value WHERE condition
  DELETE FROM table WHERE condition
  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
  DROP TABLE table_name
  ALTER TABLE table ADD COLUMN col TYPE

{Colors.PROMPT}🔄 TRANSACTIONS:{Colors.RESET}
  BEGIN TRANSACTION          - Start transaction
  COMMIT                     - Commit changes
  ROLLBACK                   - Rollback changes

{Colors.PROMPT}🎯 DOT COMMANDS:{Colors.RESET}
  .help                      - This help message
  .exit / .quit              - Exit GSQL
  .clear                     - Clear screen
  .version                   - Show version
  .history                   - Show command history
  .mode MODE                 - Set output mode
  .timer ON|OFF              - Toggle query timer

{Colors.PROMPT}📈 EXAMPLE QUERIES:{Colors.RESET}
  {Colors.SQL}SELECT * FROM users LIMIT 5{Colors.RESET}
  {Colors.SQL}SELECT username, email FROM users WHERE age > 30{Colors.RESET}
  {Colors.SQL}SELECT city, COUNT(*) FROM users GROUP BY city{Colors.RESET}
  {Colors.SQL}SELECT p.name, o.quantity, o.total_price{Colors.RESET}
  {Colors.SQL}  FROM orders o JOIN products p ON o.product_id = p.id{Colors.RESET}
  {Colors.SQL}  WHERE o.status = 'completed'{Colors.RESET}

{Colors.PROMPT}💡 TIPS:{Colors.RESET}
  • Use TAB for auto-completion
  • Use ↑↓ arrows for command history
  • End commands with ; or press Enter
  • Prefix with . for special commands
  • All tables come with sample data
"""
        return {
            'type': 'help',
            'message': help_text
        }
    
    def close(self):
        """Ferme la base de données"""
        if self.conn:
            self.conn.close()
        if self.temp_file and os.path.exists(self.temp_file):
            os.unlink(self.temp_file)

# ==================== INTERFACE CLI ====================

class GSQLCLI(cmd.Cmd):
    """Interface en ligne de commande principale"""
    
    intro = f"""
{Colors.HEADER}╔══════════════════════════════════════════════════╗
║                GSQL INTERACTIVE SHELL               ║
║                Version 2.0 Complete                 ║
╚══════════════════════════════════════════════════╝{Colors.RESET}

{Colors.INFO}Database initialized with sample data{Colors.RESET}
{Colors.INFO}Type {Colors.PROMPT}.help{Colors.INFO} for commands, {Colors.PROMPT}.exit{Colors.INFO} to quit{Colors.RESET}
{Colors.INFO}Try: {Colors.PROMPT}.tables{Colors.INFO} or {Colors.PROMPT}SELECT * FROM users LIMIT 3{Colors.RESET}

"""
    
    prompt = f'{Colors.PROMPT}gsql>{Colors.RESET} '
    
    def __init__(self, db_path: str = ":memory:", use_nlp: bool = False):
        super().__init__()
        
        # Initialiser l'historique
        self.history = HistoryManager()
        
        # Initialiser la base de données
        self.db_path = db_path
        self.db = SimpleDatabase(db_path)
        
        # Variables d'état
        self.timer_enabled = False
        self.output_mode = 'table'  # table, csv, json, line
        self.show_headers = True
        
        print(f"{Colors.SUCCESS}✅ Connected to database: {db_path}{Colors.RESET}")
        
        # Afficher les tables disponibles
        result = self.db.execute("SHOW TABLES")
        tables = result.get('rows', [])
        if tables:
            table_names = ', '.join([t['table'] for t in tables[:5]])
            if len(tables) > 5:
                table_names += f'... (+{len(tables)-5} more)'
            print(f"{Colors.INFO}📊 Available tables: {table_names}{Colors.RESET}")
    
    # ========== COMMANDES SPECIALES ==========
    
    def do_help(self, arg):
        """Affiche l'aide: help [command]"""
        if arg:
            # Aide spécifique à une commande
            cmd_func = getattr(self, 'do_' + arg, None)
            if cmd_func and cmd_func.__doc__:
                print(f"\n{Colors.HEADER}{arg}:{Colors.RESET} {cmd_func.__doc__}")
            else:
                print(f"\n{Colors.WARNING}No help available for '{arg}'{Colors.RESET}")
        else:
            # Aide générale
            result = self.db.execute("HELP")
            print(ResultFormatter.format_result(result))
    
    def do_exit(self, arg):
        """Quitte GSQL: exit"""
        print(f"\n{Colors.SUCCESS}👋 Goodbye!{Colors.RESET}")
        self.db.close()
        return True
    
    def do_quit(self, arg):
        """Quitte GSQL: quit"""
        return self.do_exit(arg)
    
    def do_clear(self, arg):
        """Efface l'écran: clear"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def do_history(self, arg):
        """Affiche l'historique des commandes: history [N]"""
        import readline
        history_length = readline.get_current_history_length()
        
        if arg and arg.isdigit():
            count = int(arg)
            start = max(0, history_length - count)
        else:
            count = 20
            start = max(0, history_length - 20)
        
        print(f"\n{Colors.HEADER}Command History (last {count}):{Colors.RESET}")
        for i in range(start, history_length):
            cmd = readline.get_history_item(i + 1)
            print(f"  {Colors.DIM}{i+1:4}{Colors.RESET} {cmd}")
    
    def do_version(self, arg):
        """Affiche la version: version"""
        print(f"{Colors.HEADER}GSQL Version 2.0 Complete{Colors.RESET}")
        print(f"{Colors.INFO}Database: SQLite with sample data{Colors.RESET}")
        print(f"{Colors.INFO}Features: Full SQL support, colors, history, auto-completion{Colors.RESET}")
    
    def do_stats(self, arg):
        """Affiche les statistiques de la base: stats"""
        result = self.db.execute("SHOW TABLES")
        tables = result.get('rows', [])
        
        total_rows = sum(t['rows'] for t in tables)
        
        print(f"\n{Colors.HEADER}📈 Database Statistics:{Colors.RESET}")
        print(f"{Colors.INFO}Total tables: {len(tables)}{Colors.RESET}")
        print(f"{Colors.INFO}Total rows: {total_rows}{Colors.RESET}")
        print(f"{Colors.INFO}Database file: {self.db_path}{Colors.RESET}")
        
        if tables:
            print(f"\n{Colors.INFO}Table details:{Colors.RESET}")
            for table in tables:
                print(f"  {table['table']:20} - {table['rows']:4} rows")
    
    def do_mode(self, arg):
        """Change le mode d'affichage: mode [table|csv|json|line]"""
        modes = ['table', 'csv', 'json', 'line']
        
        if not arg:
            print(f"{Colors.INFO}Current mode: {self.output_mode}{Colors.RESET}")
            print(f"{Colors.INFO}Available modes: {', '.join(modes)}{Colors.RESET}")
            return
        
        arg = arg.lower()
        if arg in modes:
            self.output_mode = arg
            print(f"{Colors.SUCCESS}✅ Output mode set to: {arg}{Colors.RESET}")
        else:
            print(f"{Colors.ERROR}❌ Invalid mode. Use: {', '.join(modes)}{Colors.RESET}")
    
    def do_timer(self, arg):
        """Active/désactive le timer: timer [on|off]"""
        if arg.lower() in ['on', 'yes', 'true', '1']:
            self.timer_enabled = True
            print(f"{Colors.SUCCESS}✅ Query timer enabled{Colors.RESET}")
        elif arg.lower() in ['off', 'no', 'false', '0']:
            self.timer_enabled = False
            print(f"{Colors.INFO}Query timer disabled{Colors.RESET}")
        else:
            status = "enabled" if self.timer_enabled else "disabled"
            print(f"{Colors.INFO}Query timer is currently {status}{Colors.RESET}")
            print(f"{Colors.INFO}Use: timer on|off{Colors.RESET}")
    
    def do_schema(self, arg):
        """Affiche le schéma d'une table: schema [table]"""
        if not arg:
            # Afficher toutes les tables
            result = self.db.execute("SHOW TABLES")
            print(ResultFormatter.format_result(result))
            return
        
        table_name = arg.strip()
        
        # Requête pour obtenir le schéma
        try:
            self.db.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.db.cursor.fetchall()
            
            if not columns:
                print(f"{Colors.ERROR}❌ Table '{table_name}' not found{Colors.RESET}")
                return
            
            print(f"\n{Colors.HEADER}📋 Schema for table '{table_name}':{Colors.RESET}")
            print(f"{Colors.INFO}{'Column':20} {'Type':10} {'Nullable'} {'Primary Key'}{Colors.RESET}")
            print("-" * 50)
            
            for col in columns:
                col_name = col['name']
                col_type = col['type']
                not_null = "NOT NULL" if col['notnull'] else "NULL"
                pk = "PK" if col['pk'] else ""
                
                print(f"{Colors.COLUMN}{col_name:20}{Colors.RESET} "
                      f"{Colors.DATA}{col_type:10}{Colors.RESET} "
                      f"{not_null:8} {pk}")
            
            # Informations supplémentaires
            self.db.cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            row_count = self.db.cursor.fetchone()['count']
            print(f"\n{Colors.INFO}Total rows: {row_count}{Colors.RESET}")
            
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
    
    # ========== COMMANDES SQL ==========
    
    def default(self, line):
        """
        Gestion par défaut des commandes SQL et pointées
        """
        line = line.strip()
        if not line:
            return
        
        # Commandes pointées
        if line.startswith('.'):
            cmd_name = line[1:].split()[0] if ' ' in line else line[1:]
            cmd_func = getattr(self, 'do_' + cmd_name, None)
            
            if cmd_func:
                # Extraire les arguments
                args = line[len('.' + cmd_name):].strip()
                cmd_func(args)
            else:
                print(f"{Colors.ERROR}❌ Unknown command: {line}{Colors.RESET}")
                print(f"{Colors.INFO}Type {Colors.PROMPT}.help{Colors.INFO} for available commands{Colors.RESET}")
            return
        
        # Commandes SQL
        self._execute_sql(line)
    
    def _execute_sql(self, sql: str):
        """Exécute une commande SQL"""
        import time
        
        # Afficher le SQL colorisé
        if sql.upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER')):
            print(f"{Colors.INFO}📝 Executing SQL...{Colors.RESET}")
            print(f"{Colors.SQL}{Colors.colorize_sql(sql)}{Colors.RESET}")
        
        # Mesurer le temps si timer activé
        start_time = time.time() if self.timer_enabled else None
        
        # Exécuter la commande
        result = self.db.execute(sql)
        
        # Afficher le temps d'exécution
        if self.timer_enabled and start_time:
            elapsed = (time.time() - start_time) * 1000  # en ms
            print(f"{Colors.INFO}⏱️  Query took: {elapsed:.2f} ms{Colors.RESET}")
        
        # Afficher le résultat selon le mode
        if self.output_mode == 'json':
            import json
            print(json.dumps(result, indent=2, default=str))
        elif self.output_mode == 'csv' and result.get('type') == 'select':
            self._output_as_csv(result)
        elif self.output_mode == 'line' and result.get('type') == 'select':
            self._output_as_line(result)
        else:
            # Mode table par défaut
            print(ResultFormatter.format_result(result))
    
    def _output_as_csv(self, result: dict):
        """Affiche le résultat en format CSV"""
        rows = result.get('rows', [])
        if not rows:
            print(f"{Colors.WARNING}📭 No data to export{Colors.RESET}")
            return
        
        if rows and isinstance(rows[0], dict):
            headers = list(rows[0].keys())
            # En-tête
            print(','.join(headers))
            # Données
            for row in rows:
                values = []
                for h in headers:
                    value = str(row.get(h, '')).replace('"', '""')
                    if ',' in value or '"' in value:
                        value = f'"{value}"'
                    values.append(value)
                print(','.join(values))
    
    def _output_as_line(self, result: dict):
        """Affiche le résultat en lignes simples"""
        rows = result.get('rows', [])
        count = result.get('count', 0)
        
        print(f"{Colors.INFO}📊 Results: {count} row(s){Colors.RESET}")
        
        for i, row in enumerate(rows[:100], 1):
            if isinstance(row, dict):
                line = ' | '.join(f"{k}: {v}" for k, v in row.items())
            else:
                line = str(row)
            
            print(f"{Colors.DIM}{i:4}{Colors.RESET} {line}")
        
        if len(rows) > 100:
            print(f"{Colors.INFO}... and {len(rows)-100} more rows{Colors.RESET}")
    
    # ========== COMMANDES DE DEMONSTRATION ==========
    
    def do_demo(self, arg):
        """Exécute une démonstration: demo [basic|advanced|all]"""
        demos = {
            'basic': self._demo_basic,
            'advanced': self._demo_advanced,
            'all': self._demo_all
        }
        
        if not arg:
            arg = 'basic'
        
        if arg in demos:
            print(f"\n{Colors.HEADER}🎬 Running {arg} demo...{Colors.RESET}")
            demos[arg]()
        else:
            print(f"{Colors.ERROR}❌ Unknown demo: {arg}{Colors.RESET}")
            print(f"{Colors.INFO}Available demos: {', '.join(demos.keys())}{Colors.RESET}")
    
    def _demo_basic(self):
        """Démo basique"""
        queries = [
            "SELECT * FROM users LIMIT 3",
            "SELECT name, price FROM products WHERE price > 500",
            "SELECT city, COUNT(*) as user_count FROM users GROUP BY city",
            "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department"
        ]
        
        for query in queries:
            print(f"\n{Colors.INFO}📝 {query}{Colors.RESET}")
            result = self.db.execute(query)
            print(ResultFormatter.format_result(result))
            print()
    
    def _demo_advanced(self):
        """Démo avancée"""
        queries = [
            """SELECT u.username, p.name, o.quantity, o.total_price 
               FROM orders o 
               JOIN users u ON o.user_id = u.id 
               JOIN products p ON o.product_id = p.id 
               WHERE o.status = 'completed'""",
            
            """SELECT category, 
                      COUNT(*) as product_count,
                      AVG(price) as avg_price,
                      SUM(stock_quantity) as total_stock
               FROM products 
               GROUP BY category 
               HAVING COUNT(*) > 1 
               ORDER BY avg_price DESC""",
            
            """SELECT 
                   CASE 
                       WHEN age < 30 THEN 'Under 30'
                       WHEN age BETWEEN 30 AND 40 THEN '30-40'
                       ELSE 'Over 40'
                   END as age_group,
                   COUNT(*) as user_count,
                   AVG(age) as avg_age
               FROM users 
               GROUP BY age_group 
               ORDER BY user_count DESC"""
        ]
        
        for query in queries:
            print(f"\n{Colors.INFO}📝 Advanced query:{Colors.RESET}")
            print(f"{Colors.SQL}{Colors.colorize_sql(query)}{Colors.RESET}")
            result = self.db.execute(query)
            print(ResultFormatter.format_result(result))
            print()
    
    def _demo_all(self):
        """Toutes les démos"""
        self._demo_basic()
        self._demo_advanced()
    
    # ========== COMMANDES UTILITAIRES ==========
    
    def do_import(self, arg):
        """Importe un fichier CSV: import file.csv [table]"""
        if not arg:
            print(f"{Colors.ERROR}❌ Usage: import file.csv [table_name]{Colors.RESET}")
            return
        
        parts = arg.split()
        if len(parts) < 1:
            print(f"{Colors.ERROR}❌ Please specify filename{Colors.RESET}")
            return
        
        filename = parts[0]
        table_name = parts[1] if len(parts) > 1 else Path(filename).stem
        
        print(f"{Colors.INFO}📥 Importing {filename} to table {table_name}...{Colors.RESET}")
        
        try:
            import csv
            
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # Créer la table
                create_sql = f"CREATE TABLE {table_name} ({', '.join([f'{h} TEXT' for h in headers])})"
                self.db.execute(create_sql)
                
                # Insérer les données
                count = 0
                for row in reader:
                    values = [f"'{v.replace("'", "''")}'" for v in row]
                    insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(values)})"
                    self.db.execute(insert_sql)
                    count += 1
                
                print(f"{Colors.SUCCESS}✅ Imported {count} rows into {table_name}{Colors.RESET}")
                
        except Exception as e:
            print(f"{Colors.ERROR}❌ Import failed: {str(e)}{Colors.RESET}")
    
    def do_export(self, arg):
        """Exporte une table vers CSV: export table [file.csv]"""
        if not arg:
            print(f"{Colors.ERROR}❌ Usage: export table_name [file.csv]{Colors.RESET}")
            return
        
        parts = arg.split()
        table_name = parts[0]
        filename = parts[1] if len(parts) > 1 else f"{table_name}.csv"
        
        print(f"{Colors.INFO}📤 Exporting {table_name} to {filename}...{Colors.RESET}")
        
        try:
            import csv
            
            # Récupérer les données
            result = self.db.execute(f"SELECT * FROM {table_name}")
            rows = result.get('rows', [])
            
            if not rows:
                print(f"{Colors.WARNING}⚠ Table {table_name} is empty{Colors.RESET}")
                return
            
            # Écrire le CSV
            with open(filename, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            
            print(f"{Colors.SUCCESS}✅ Exported {len(rows)} rows to {filename}{Colors.RESET}")
            
        except Exception as e:
            print(f"{Colors.ERROR}❌ Export failed: {str(e)}{Colors.RESET}")
    
    def do_dump(self, arg):
        """Exporte toute la base: dump [file.sql]"""
        filename = arg if arg else "gsql_dump.sql"
        
        print(f"{Colors.INFO}💾 Dumping database to {filename}...{Colors.RESET}")
        
        try:
            # Récupérer toutes les tables
            result = self.db.execute("SHOW TABLES")
            tables = result.get('rows', [])
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("-- GSQL Database Dump\n")
                f.write(f"-- Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"-- Database: {self.db_path}\n\n")
                
                for table_info in tables:
                    table_name = table_info['table']
                    
                    # Schéma de la table
                    f.write(f"\n-- Table: {table_name}\n")
                    f.write(f"DROP TABLE IF EXISTS {table_name};\n")
                    
                    self.db.cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                    create_sql = self.db.cursor.fetchone()[0]
                    f.write(f"{create_sql};\n\n")
                    
                    # Données
                    result = self.db.execute(f"SELECT * FROM {table_name}")
                    rows = result.get('rows', [])
                    
                    if rows:
                        f.write(f"-- Data for {table_name}\n")
                        for row in rows:
                            keys = list(row.keys())
                            values = []
                            for k in keys:
                                v = row[k]
                                if v is None:
                                    values.append('NULL')
                                elif isinstance(v, (int, float)):
                                    values.append(str(v))
                                else:
                                    values.append(f"'{str(v).replace("'", "''")}'")
                            
                            sql = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({', '.join(values)});\n"
                            f.write(sql)
                        f.write("\n")
            
            print(f"{Colors.SUCCESS}✅ Database dumped to {filename}{Colors.RESET}")
            
        except Exception as e:
            print(f"{Colors.ERROR}❌ Dump failed: {str(e)}{Colors.RESET}")
    
    # ========== GESTION DES ERREURS ==========
    
    def emptyline(self):
        """Ne rien faire sur ligne vide"""
        pass
    
    def do_EOF(self, arg):
        """Gestion de Ctrl-D"""
        print()
        return self.do_exit(arg)

# ==================== FONCTION PRINCIPALE ====================

def main():
    """Point d'entrée principal"""
    
    parser = argparse.ArgumentParser(
        description='GSQL - Interactive SQL Database Shell',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
{Colors.HEADER}Examples:{Colors.RESET}
  {Colors.PROMPT}gsql{Colors.RESET}                      # Interactive mode with sample data
  {Colors.PROMPT}gsql mydatabase.db{Colors.RESET}        # Use specific database file
  {Colors.PROMPT}gsql --sql "SELECT * FROM users"{Colors.RESET}  # Single command mode
  {Colors.PROMPT}gsql --demo{Colors.RESET}               # Run demonstration
  {Colors.PROMPT}gsql --no-colors{Colors.RESET}          # Disable colors
  {Colors.PROMPT}gsql --help{Colors.RESET}               # Show this help

{Colors.HEADER}Quick Start:{Colors.RESET}
  1. Type {Colors.PROMPT}.tables{Colors.RESET} to see available tables
  2. Type {Colors.PROMPT}SELECT * FROM users LIMIT 3{Colors.RESET} to query data
  3. Type {Colors.PROMPT}.help{Colors.RESET} for all commands
  4. Type {Colors.PROMPT}.exit{Colors.RESET} to quit
"""
    )
    
    parser.add_argument(
        'database',
        nargs='?',
        default=':memory:',
        help='Database file (default: in-memory with sample data)'
    )
    
    parser.add_argument(
        '--sql',
        help='Execute single SQL command and exit'
    )
    
    parser.add_argument(
        '--demo',
        action='store_true',
        help='Run demonstration and exit'
    )
    
    parser.add_argument(
        '--no-colors',
        action='store_true',
        help='Disable colored output'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    
    parser.add_argument(
        '--version',
        action='store_true',
        help='Show version and exit'
    )
    
    args = parser.parse_args()
    
    # Version
    if args.version:
        print(f"{Colors.HEADER}GSQL Version 2.0 Complete{Colors.RESET}")
        sys.exit(0)
    
    # Désactiver les couleurs si demandé
    if args.no_colors:
        for attr in dir(Colors):
            if not attr.startswith('_'):
                setattr(Colors, attr, '')
    
    # Mode verbose
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Mode commande unique
    if args.sql:
        try:
            db = SimpleDatabase(args.database)
            result = db.execute(args.sql)
            print(ResultFormatter.format_result(result))
            db.close()
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
            sys.exit(1)
        sys.exit(0)
    
    # Mode démonstration
    if args.demo:
        cli = GSQLCLI(args.database)
        cli._demo_all()
        cli.db.close()
        sys.exit(0)
    
    # Mode interactif
    try:
        cli = GSQLCLI(args.database)
        cli.cmdloop()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}Interrupted by user{Colors.RESET}")
        sys.exit(0)
    except Exception as e:
        print(f"{Colors.ERROR}❌ Fatal error: {str(e)}{Colors.RESET}")
        sys.exit(1)

if __name__ == '__main__':
    import time
    main()
