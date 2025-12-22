#!/usr/bin/env python3
"""
GSQL CLI - Version corrigée
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
import time

# ==================== CORRECTIONS RAPIDES ====================

# Désactiver temporairement les couleurs si problèmes
USE_COLORS = True

class SimpleColors:
    """Couleurs simplifiées"""
    if USE_COLORS:
        RED = '\033[91m'
        GREEN = '\033[92m'
        YELLOW = '\033[93m'
        BLUE = '\033[94m'
        CYAN = '\033[96m'
        WHITE = '\033[97m'
        RESET = '\033[0m'
        BOLD = '\033[1m'
    else:
        RED = GREEN = YELLOW = BLUE = CYAN = WHITE = RESET = BOLD = ''

Colors = SimpleColors  # Utiliser la version simple

# ==================== DATABASE SIMPLIFIEE ====================

class SimpleDatabase:
    """Base de données simplifiée mais fonctionnelle"""
    
    def __init__(self, db_path=":memory:"):
        import sqlite3
        import tempfile
        
        self.db_path = db_path
        
        if db_path == ":memory:":
            self.conn = sqlite3.connect(":memory:")
            self.is_temp = True
        else:
            # S'assurer que c'est un fichier .db
            if not db_path.endswith('.db'):
                db_path = db_path + '.db'
            self.db_path = db_path
            self.conn = sqlite3.connect(db_path)
            self.is_temp = False
        
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        
        # Initialiser les tables par défaut
        self._init_default_tables()
    
    def _init_default_tables(self):
        """Initialise les tables par défaut"""
        # Table users
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                full_name TEXT,
                age INTEGER,
                city TEXT,
                country TEXT DEFAULT 'France',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insérer des données d'exemple si vide
        self.cursor.execute("SELECT COUNT(*) FROM users")
        if self.cursor.fetchone()[0] == 0:
            users = [
                ('alice', 'alice@example.com', 'Alice Dupont', 28, 'Paris'),
                ('bob', 'bob@example.com', 'Bob Martin', 35, 'Lyon'),
                ('charlie', 'charlie@example.com', 'Charlie Durand', 42, 'Marseille')
            ]
            self.cursor.executemany(
                "INSERT INTO users (username, email, full_name, age, city) VALUES (?, ?, ?, ?, ?)",
                users
            )
        
        # Table products
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT,
                stock INTEGER DEFAULT 0
            )
        ''')
        
        self.cursor.execute("SELECT COUNT(*) FROM products")
        if self.cursor.fetchone()[0] == 0:
            products = [
                ('Laptop', 999.99, 'Electronics', 10),
                ('Phone', 699.99, 'Electronics', 25),
                ('Tablet', 399.99, 'Electronics', 15)
            ]
            self.cursor.executemany(
                "INSERT INTO products (name, price, category, stock) VALUES (?, ?, ?, ?)",
                products
            )
        
        self.conn.commit()
    
    def execute(self, sql):
        """Exécute une commande SQL"""
        sql = sql.strip().upper()
        
        # Commandes spéciales
        if sql == "SHOW TABLES" or sql == ".TABLES":
            return self._show_tables()
        elif sql == "SHOW FUNCTIONS":
            return self._show_functions()
        elif sql == "HELP":
            return self._show_help()
        
        # Commandes SQL normales
        try:
            self.cursor.execute(sql)
            
            if sql.startswith("SELECT"):
                rows = self.cursor.fetchall()
                if self.cursor.description:
                    columns = [desc[0] for desc in self.cursor.description]
                    result = [dict(zip(columns, row)) for row in rows]
                else:
                    result = [row[0] for row in rows] if rows else []
                
                return {
                    'type': 'select',
                    'rows': result,
                    'count': len(result)
                }
            else:
                self.conn.commit()
                return {
                    'type': 'command',
                    'message': 'Command executed',
                    'rows': self.cursor.rowcount
                }
                
        except Exception as e:
            return {
                'type': 'error',
                'message': str(e)
            }
    
    def _show_tables(self):
        """Affiche les tables"""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = []
        for row in self.cursor.fetchall():
            table_name = row[0]
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cursor.fetchone()[0]
            tables.append({
                'table': table_name,
                'rows': count
            })
        
        return {
            'type': 'tables',
            'rows': tables,
            'count': len(tables),
            'message': f'{len(tables)} tables'
        }
    
    def _show_functions(self):
        """Affiche les fonctions"""
        functions = [
            {'name': 'COUNT()', 'type': 'builtin'},
            {'name': 'SUM()', 'type': 'builtin'},
            {'name': 'AVG()', 'type': 'builtin'},
            {'name': 'UPPER()', 'type': 'builtin'},
            {'name': 'LOWER()', 'type': 'builtin'}
        ]
        
        return {
            'type': 'functions',
            'rows': functions,
            'count': len(functions)
        }
    
    def _show_help(self):
        """Affiche l'aide"""
        help_text = f"""
{Colors.BOLD}GSQL Commands:{Colors.RESET}

{Colors.GREEN}SQL:{Colors.RESET}
  SELECT * FROM table
  INSERT INTO table VALUES (...)
  CREATE TABLE name (columns)
  DROP TABLE name
  UPDATE table SET column=value
  DELETE FROM table

{Colors.GREEN}Special:{Colors.RESET}
  SHOW TABLES    - List tables
  SHOW FUNCTIONS - List functions
  HELP           - This message

{Colors.GREEN}Examples:{Colors.RESET}
  SELECT * FROM users
  CREATE TABLE products (id INT, name TEXT)
  INSERT INTO products VALUES (1, 'Laptop')
  SHOW TABLES
"""
        return {
            'type': 'help',
            'message': help_text
        }
    
    def close(self):
        """Ferme la connexion"""
        if self.conn:
            self.conn.close()

# ==================== CLI CORRIGEE ====================

class FixedGSQLCLI(cmd.Cmd):
    """CLI corrigée sans bugs"""
    
    intro = f"""
{Colors.BOLD}{Colors.BLUE}╔══════════════════════════════════╗
║                  GSQL            ║
╚══════════════════════════════════╝{Colors.RESET}

Type {Colors.GREEN}help{Colors.RESET} for commands, {Colors.GREEN}exit{Colors.RESET} to quit.
Database ready with sample data.
"""
    
    prompt = f'{Colors.GREEN}gsql>{Colors.RESET} '
    
    def __init__(self, db_path=":memory:"):
        super().__init__()
        self.db = SimpleDatabase(db_path)
        print(f"{Colors.GREEN}✓ Connected to: {db_path}{Colors.RESET}")
        
        # Afficher les tables
        result = self.db.execute("SHOW TABLES")
        if result['rows']:
            tables = [t['table'] for t in result['rows']]
            print(f"{Colors.CYAN}📊 Tables: {', '.join(tables)}{Colors.RESET}")
    
    # ===== COMMANDES DE BASE =====
    
    def do_exit(self, arg):
        """Exit GSQL"""
        print(f"{Colors.GREEN}👋 Goodbye!{Colors.RESET}")
        self.db.close()
        return True
    
    def do_quit(self, arg):
        """Exit GSQL"""
        return self.do_exit(arg)
    
    def do_clear(self, arg):
        """Clear screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def do_help(self, arg):
        """Show help"""
        result = self.db.execute("HELP")
        self._print_result(result)
    
    # ===== COMMANDES SPECIALES =====
    
    def do_tables(self, arg):
        """Show tables"""
        result = self.db.execute("SHOW TABLES")
        self._print_result(result)
    
    def do_functions(self, arg):
        """Show functions"""
        result = self.db.execute("SHOW FUNCTIONS")
        self._print_result(result)
    
    def do_schema(self, arg):
        """Show table schema: schema [table]"""
        if not arg:
            self.do_tables("")
            return
        
        table = arg.strip()
        try:
            self.db.cursor.execute(f"PRAGMA table_info({table})")
            columns = self.db.cursor.fetchall()
            
            if not columns:
                print(f"{Colors.RED}Table '{table}' not found{Colors.RESET}")
                return
            
            print(f"\n{Colors.BOLD}Schema for '{table}':{Colors.RESET}")
            print(f"{Colors.CYAN}{'Column':15} {'Type':10} {'Nullable'}{Colors.RESET}")
            print("-" * 40)
            
            for col in columns:
                name = col[1]
                type_ = col[2]
                notnull = "NOT NULL" if col[3] else "NULL"
                print(f"{Colors.GREEN}{name:15}{Colors.RESET} {type_:10} {notnull}")
            
            # Row count
            self.db.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.db.cursor.fetchone()[0]
            print(f"\n{Colors.CYAN}Total rows: {count}{Colors.RESET}")
            
        except Exception as e:
            print(f"{Colors.RED}Error: {e}{Colors.RESET}")
    
    # ===== COMMANDES SQL =====
    
    def default(self, line):
        """Handle SQL commands and dot commands"""
        line = line.strip()
        if not line:
            return
        
        # Dot commands
        if line.startswith('.'):
            cmd = line[1:].lower()
            if cmd == 'tables':
                self.do_tables("")
            elif cmd == 'help':
                self.do_help("")
            elif cmd == 'exit' or cmd == 'quit':
                return self.do_exit("")
            elif cmd == 'clear':
                self.do_clear("")
            elif cmd == 'schema':
                parts = line.split()
                if len(parts) > 1:
                    self.do_schema(parts[1])
                else:
                    self.do_schema("")
            elif cmd.startswith('schema '):
                table = cmd[7:].strip()
                self.do_schema(table)
            else:
                print(f"{Colors.RED}Unknown command: {line}{Colors.RESET}")
                print(f"Try: .tables, .help, .schema [table], .exit")
            return
        
        # SQL commands
        self._execute_sql(line)
    
    def _execute_sql(self, sql):
        """Execute SQL command"""
        # Coloriser le SQL
        colored_sql = sql
        if USE_COLORS:
            for keyword in ['SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
                          'CREATE', 'TABLE', 'DROP', 'UPDATE', 'SET', 'DELETE']:
                import re
                colored_sql = re.sub(
                    r'\b' + keyword + r'\b',
                    f'{Colors.YELLOW}{keyword}{Colors.RESET}',
                    colored_sql,
                    flags=re.IGNORECASE
                )
        
        print(f"{Colors.CYAN}SQL:{Colors.RESET} {colored_sql}")
        
        # Exécuter
        result = self.db.execute(sql)
        self._print_result(result)
    
    def _print_result(self, result):
        """Print query result"""
        if not isinstance(result, dict):
            print(f"{Colors.WHITE}{result}{Colors.RESET}")
            return
        
        if result.get('type') == 'tables':
            rows = result.get('rows', [])
            if rows:
                print(f"\n{Colors.CYAN}📊 Tables ({len(rows)}):{Colors.RESET}")
                for table in rows:
                    print(f"  {Colors.GREEN}{table['table']:15}{Colors.RESET} - {table['rows']} rows")
            else:
                print(f"{Colors.YELLOW}No tables{Colors.RESET}")
        
        elif result.get('type') == 'select':
            rows = result.get('rows', [])
            count = result.get('count', 0)
            
            if count == 0:
                print(f"{Colors.YELLOW}No results{Colors.RESET}")
                return
            
            print(f"\n{Colors.CYAN}📊 Results: {count} row(s){Colors.RESET}")
            
            if rows and isinstance(rows[0], dict):
                # Table format
                headers = list(rows[0].keys())
                
                # Calculate widths
                widths = {}
                for h in headers:
                    widths[h] = len(str(h))
                    for row in rows[:20]:
                        if h in row:
                            widths[h] = max(widths[h], len(str(row[h])))
                
                # Print header
                header_line = " | ".join(f"{Colors.BLUE}{h:<{widths[h]}}{Colors.RESET}" for h in headers)
                print(header_line)
                print("-" * len(header_line.replace('\033[', 'XX').replace('m', 'X')))
                
                # Print rows
                for i, row in enumerate(rows[:20]):
                    values = []
                    for h in headers:
                        value = str(row.get(h, ''))
                        if i % 2 == 0:
                            values.append(f"{Colors.WHITE}{value:<{widths[h]}}{Colors.RESET}")
                        else:
                            values.append(f"{Colors.CYAN}{value:<{widths[h]}}{Colors.RESET}")
                    print(" | ".join(values))
                
                if len(rows) > 20:
                    print(f"{Colors.YELLOW}... and {len(rows)-20} more{Colors.RESET}")
            else:
                # Simple list
                for i, row in enumerate(rows[:20], 1):
                    print(f"{i:3}. {row}")
                if len(rows) > 20:
                    print(f"{Colors.YELLOW}... and {len(rows)-20} more{Colors.RESET}")
        
        elif result.get('type') == 'command':
            print(f"{Colors.GREEN}✓ {result.get('message', 'Done')}{Colors.RESET}")
        
        elif result.get('type') == 'help':
            print(result.get('message', ''))
        
        elif result.get('type') == 'error':
            print(f"{Colors.RED}✗ {result.get('message', 'Error')}{Colors.RESET}")
        
        else:
            print(f"{Colors.WHITE}{result}{Colors.RESET}")
    
    # ===== NLP SIMPLE =====
    
    def do_nl(self, arg):
        """Natural language query: nl [question]"""
        if not arg:
            print(f"{Colors.RED}Please provide a question{Colors.RESET}")
            return
        
        # Simple translation
        arg_lower = arg.lower()
        
        if 'table' in arg_lower and 'show' not in arg_lower:
            # Extract table name
            words = arg_lower.split()
            for word in words:
                if word != 'table' and len(word) > 2:
                    self._execute_sql(f"SELECT * FROM {word} LIMIT 5")
                    return
        
        if 'table' in arg_lower:
            self.do_tables("")
        elif 'help' in arg_lower:
            self.do_help("")
        elif 'user' in arg_lower:
            self._execute_sql("SELECT * FROM users LIMIT 5")
        elif 'product' in arg_lower:
            self._execute_sql("SELECT * FROM products LIMIT 5")
        else:
            print(f"{Colors.YELLOW}Try: 'show tables', 'table users', 'help'{Colors.RESET}")

# ==================== MAIN FUNCTION CORRECTED ====================

def main():
    """Main function with fixed argument parsing"""
    
    parser = argparse.ArgumentParser(
        description='GSQL - Fixed version',
        add_help=False  # We'll handle help manually
    )
    
    # Positional argument for database
    parser.add_argument(
        'database',
        nargs='?',
        default=':memory:',
        help='Database file (default: memory)'
    )
    
    # Optional arguments
    parser.add_argument(
        '--sql',
        help='Execute SQL command and exit'
    )
    
    parser.add_argument(
        '--help', '-h',
        action='store_true',
        help='Show help'
    )
    
    parser.add_argument(
        '--version', '-v',
        action='store_true',
        help='Show version'
    )
    
    parser.add_argument(
        '--no-colors',
        action='store_true',
        help='Disable colors'
    )
    
    parser.add_argument(
        '--create',
        action='store_true',
        help='Create new database file'
    )
    
    # Try to parse arguments
    try:
        args = parser.parse_args()
    except:
        # If parsing fails, show help
        print(f"{Colors.BOLD}GSQL - SQL Database Shell{Colors.RESET}")
        print(f"\nUsage: gsql [database] [options]")
        print(f"\nOptions:")
        print(f"  database            Database file (default: :memory:)")
        print(f"  --sql QUERY        Execute SQL and exit")
        print(f"  --help, -h         Show this help")
        print(f"  --version, -v      Show version")
        print(f"  --no-colors        Disable colors")
        print(f"  --create           Create new database")
        sys.exit(1)
    
    # Handle help
    if args.help:
        parser.print_help()
        sys.exit(0)
    
    # Handle version
    if args.version:
        print("GSQL 2.0 - Fixed Version")
        sys.exit(0)
    
    # Handle colors
    global USE_COLORS
    if args.no_colors:
        USE_COLORS = False
    
    # Handle create flag
    db_path = args.database
    if args.create and db_path != ':memory:':
        if not db_path.endswith('.db'):
            db_path += '.db'
        if os.path.exists(db_path):
            print(f"{Colors.RED}Database already exists: {db_path}{Colors.RESET}")
            sys.exit(1)
        print(f"{Colors.GREEN}Creating new database: {db_path}{Colors.RESET}")
    
    # Single command mode
    if args.sql:
        try:
            db = SimpleDatabase(db_path)
            result = db.execute(args.sql)
            cli = FixedGSQLCLI(db_path)
            cli._print_result(result)
            db.close()
        except Exception as e:
            print(f"{Colors.RED}Error: {e}{Colors.RESET}")
        sys.exit(0)
    
    # Interactive mode
    try:
        cli = FixedGSQLCLI(db_path)
        cli.cmdloop()
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}Fatal error: {e}{Colors.RESET}")
        sys.exit(1)

if __name__ == '__main__':
    main()
