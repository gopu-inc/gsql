#!/usr/bin/env python3
"""
GSQL COMPLETE - Version finale avec toutes les fonctionnalités
"""

import sys
import os
import cmd
import sqlite3
import json
import re
from datetime import datetime
from pathlib import Path

# ==================== COULEURS ====================

class Colors:
    """Couleurs pour le terminal"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    WHITE = '\033[97m'
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    @staticmethod
    def success(msg): return f"{Colors.GREEN}✓ {msg}{Colors.RESET}"
    @staticmethod
    def error(msg): return f"{Colors.RED}✗ {msg}{Colors.RESET}"
    @staticmethod
    def warning(msg): return f"{Colors.YELLOW}⚠ {msg}{Colors.RESET}"
    @staticmethod
    def info(msg): return f"{Colors.CYAN}ℹ {msg}{Colors.RESET}"
    @staticmethod
    def bold(msg): return f"{Colors.BOLD}{msg}{Colors.RESET}"
    @staticmethod
    def sql(msg): return f"{Colors.YELLOW}{msg}{Colors.RESET}"

# ==================== GESTION DES FONCTIONS ====================

class FunctionManager:
    """Gestionnaire de fonctions utilisateur"""
    
    def __init__(self, db_path=":memory:"):
        self.db_path = db_path
        self.functions = {}
        self._init_builtins()
    
    def _init_builtins(self):
        """Initialise les fonctions intégrées"""
        self.functions = {
            'UPPER': {
                'type': 'builtin',
                'description': 'Convert to uppercase',
                'call': lambda args: str(args[0]).upper() if args else ''
            },
            'LOWER': {
                'type': 'builtin', 
                'description': 'Convert to lowercase',
                'call': lambda args: str(args[0]).lower() if args else ''
            },
            'LENGTH': {
                'type': 'builtin',
                'description': 'String length',
                'call': lambda args: len(str(args[0])) if args else 0
            },
            'ABS': {
                'type': 'builtin',
                'description': 'Absolute value',
                'call': lambda args: abs(float(args[0])) if args else 0
            },
            'ROUND': {
                'type': 'builtin',
                'description': 'Round number',
                'call': lambda args: round(float(args[0]), int(args[1]) if len(args) > 1 else 0)
            },
            'CONCAT': {
                'type': 'builtin',
                'description': 'Concatenate strings',
                'call': lambda args: ''.join(str(a) for a in args)
            },
            'NOW': {
                'type': 'builtin',
                'description': 'Current timestamp',
                'call': lambda args: datetime.now().isoformat()
            },
            'DATE': {
                'type': 'builtin',
                'description': 'Current date',
                'call': lambda args: datetime.now().strftime('%Y-%m-%d')
            }
        }
    
    def create_function(self, name, params, body, returns="TEXT"):
        """Crée une nouvelle fonction utilisateur"""
        if name in self.functions:
            return False, f"Function '{name}' already exists"
        
        try:
            # Valider le nom
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
                return False, f"Invalid function name: '{name}'"
            
            # Créer la fonction dynamiquement
            func_code = f"""
def user_func({', '.join(params) if params else ''}):
    {body}
"""
            
            # Compiler et exécuter
            exec_globals = {}
            exec(func_code, {}, exec_globals)
            
            # Enregistrer
            self.functions[name] = {
                'type': 'user',
                'params': params,
                'returns': returns,
                'body': body,
                'call': exec_globals['user_func'],
                'created': datetime.now().isoformat()
            }
            
            return True, f"Function '{name}' created successfully"
            
        except SyntaxError as e:
            return False, f"Syntax error: {e}"
        except Exception as e:
            return False, f"Error creating function: {e}"
    
    def call_function(self, name, args):
        """Appelle une fonction"""
        if name not in self.functions:
            raise ValueError(f"Function '{name}' not found")
        
        func = self.functions[name]
        try:
            return func['call'](args)
        except Exception as e:
            raise ValueError(f"Error calling '{name}': {e}")
    
    def list_functions(self):
        """Liste toutes les fonctions"""
        result = []
        for name, info in self.functions.items():
            result.append({
                'name': name,
                'type': info['type'],
                'params': info.get('params', []),
                'returns': info.get('returns', 'ANY'),
                'description': info.get('description', ''),
                'created': info.get('created', '')
            })
        return result
    
    def drop_function(self, name):
        """Supprime une fonction utilisateur"""
        if name not in self.functions:
            return False, f"Function '{name}' not found"
        
        if self.functions[name]['type'] == 'builtin':
            return False, f"Cannot drop built-in function '{name}'"
        
        del self.functions[name]
        return True, f"Function '{name}' dropped"

# ==================== BASE DE DONNEES COMPLETE ====================

class CompleteDatabase:
    """Base de données complète avec fonctions"""
    
    def __init__(self, db_path=":memory:"):
        if db_path == ":memory:":
            self.conn = sqlite3.connect(":memory:")
            self.is_temp = True
        else:
            if not db_path.endswith('.db'):
                db_path += '.db'
            self.db_path = db_path
            self.conn = sqlite3.connect(db_path)
            self.is_temp = False
        
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        
        # Initialiser le gestionnaire de fonctions
        self.function_manager = FunctionManager(db_path)
        
        # Initialiser la base
        self._init_database()
    
    def _init_database(self):
        """Initialise la base avec des données d'exemple"""
        # Table users
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                full_name TEXT,
                age INTEGER,
                city TEXT DEFAULT 'Unknown',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Données d'exemple
        self.cursor.execute("SELECT COUNT(*) FROM users")
        if self.cursor.fetchone()[0] == 0:
            users = [
                ('alice', 'alice@example.com', 'Alice Dupont', 28, 'Paris'),
                ('bob', 'bob@example.com', 'Bob Martin', 35, 'Lyon'),
                ('charlie', 'charlie@example.com', 'Charlie Durand', 42, 'Marseille'),
                ('diana', 'diana@example.com', 'Diana Leroy', 31, 'Toulouse')
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
                description TEXT,
                price REAL NOT NULL,
                category TEXT,
                stock INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.cursor.execute("SELECT COUNT(*) FROM products")
        if self.cursor.fetchone()[0] == 0:
            products = [
                ('Laptop Pro', 'High-performance laptop', 1299.99, 'Electronics', 15),
                ('Smartphone X', 'Latest smartphone', 899.99, 'Electronics', 30),
                ('Tablet Lite', 'Lightweight tablet', 499.99, 'Electronics', 25),
                ('Headphones', 'Noise-cancelling', 199.99, 'Audio', 40)
            ]
            self.cursor.executemany(
                "INSERT INTO products (name, description, price, category, stock) VALUES (?, ?, ?, ?, ?)",
                products
            )
        
        # Table pour stocker les fonctions utilisateur
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS gsql_functions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                params TEXT,
                body TEXT NOT NULL,
                returns TEXT DEFAULT 'TEXT',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Charger les fonctions depuis la table
        self._load_functions_from_db()
        
        self.conn.commit()
    
    def _load_functions_from_db(self):
        """Charge les fonctions depuis la table"""
        try:
            self.cursor.execute("SELECT name, params, body, returns FROM gsql_functions")
            for row in self.cursor.fetchall():
                params = json.loads(row['params']) if row['params'] else []
                success, msg = self.function_manager.create_function(
                    row['name'], params, row['body'], row['returns']
                )
                if not success:
                    print(Colors.warning(f"Failed to load function {row['name']}: {msg}"))
        except:
            pass  # Table peut ne pas exister encore
    
    def _save_function_to_db(self, name, params, body, returns):
        """Sauvegarde une fonction dans la table"""
        try:
            params_json = json.dumps(params)
            self.cursor.execute(
                "INSERT INTO gsql_functions (name, params, body, returns) VALUES (?, ?, ?, ?)",
                (name, params_json, body, returns)
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Mettre à jour si existe déjà
            self.cursor.execute(
                "UPDATE gsql_functions SET params = ?, body = ?, returns = ? WHERE name = ?",
                (params_json, body, returns, name)
            )
            self.conn.commit()
    
    def execute(self, sql):
        """Exécute une commande SQL"""
        sql = sql.strip()
        sql_upper = sql.upper()
        
        # Commandes spéciales
        if sql_upper == "SHOW TABLES" or sql == ".tables":
            return self._show_tables()
        elif sql_upper == "SHOW FUNCTIONS":
            return self._show_functions()
        elif sql_upper == "HELP":
            return self._show_help()
        
        # CREATE FUNCTION
        if sql_upper.startswith("CREATE FUNCTION"):
            return self._create_function(sql)
        
        # DROP FUNCTION
        if sql_upper.startswith("DROP FUNCTION"):
            return self._drop_function(sql)
        
        # Commandes SQL normales
        try:
            # Remplacer les appels de fonction
            processed_sql = self._process_functions_in_sql(sql)
            
            self.cursor.execute(processed_sql)
            
            if sql_upper.startswith("SELECT"):
                rows = self.cursor.fetchall()
                if self.cursor.description:
                    columns = [desc[0] for desc in self.cursor.description]
                    result_rows = []
                    for row in rows:
                        row_dict = {}
                        for i, col in enumerate(columns):
                            row_dict[col] = row[i]
                        result_rows.append(row_dict)
                else:
                    result_rows = [row[0] for row in rows] if rows else []
                
                return {
                    'type': 'select',
                    'rows': result_rows,
                    'count': len(result_rows),
                    'success': True
                }
            else:
                self.conn.commit()
                return {
                    'type': 'command',
                    'message': 'Command executed successfully',
                    'rows_affected': self.cursor.rowcount,
                    'success': True
                }
                
        except Exception as e:
            return {
                'type': 'error',
                'message': str(e),
                'success': False
            }
    
    def _process_functions_in_sql(self, sql):
        """Traite les appels de fonction dans le SQL"""
        # Pattern pour détecter les fonctions: FUNC(arg1, arg2, ...)
        pattern = r'(\w+)\s*\(([^)]+)\)'
        
        def replace_func(match):
            func_name = match.group(1)
            args_str = match.group(2)
            
            # Vérifier si c'est une fonction connue
            if func_name in self.function_manager.functions:
                # Séparer les arguments
                args = []
                current = ""
                in_quotes = False
                quote_char = None
                paren_depth = 0
                
                for char in args_str:
                    if char in ['"', "'"] and not in_quotes:
                        in_quotes = True
                        quote_char = char
                        current += char
                    elif char == quote_char and in_quotes:
                        in_quotes = False
                        current += char
                    elif char == '(' and not in_quotes:
                        paren_depth += 1
                        current += char
                    elif char == ')' and not in_quotes:
                        paren_depth -= 1
                        current += char
                    elif char == ',' and not in_quotes and paren_depth == 0:
                        args.append(current.strip())
                        current = ""
                    else:
                        current += char
                
                if current:
                    args.append(current.strip())
                
                # Évaluer la fonction
                try:
                    # Nettoyer les arguments (enlever guillemets)
                    clean_args = []
                    for arg in args:
                        arg = arg.strip()
                        if (arg.startswith("'") and arg.endswith("'")) or \
                           (arg.startswith('"') and arg.endswith('"')):
                            arg = arg[1:-1]
                        clean_args.append(arg)
                    
                    result = self.function_manager.call_function(func_name, clean_args)
                    
                    # Retourner le résultat selon le type
                    if isinstance(result, (int, float)):
                        return str(result)
                    else:
                        return f"'{result}'"
                        
                except Exception as e:
                    return f"NULL -- Error: {e}"
            
            # Si pas une fonction, laisser tel quel
            return match.group(0)
        
        # Remplacer toutes les fonctions trouvées
        processed = re.sub(pattern, replace_func, sql)
        return processed
    
    def _create_function(self, sql):
        """Gère CREATE FUNCTION"""
        # Pattern: CREATE FUNCTION name(params) RETURNS type AS $$body$$ LANGUAGE plpython
        pattern = r"CREATE\s+FUNCTION\s+(\w+)\s*\((.*?)\)\s*RETURNS\s+(\w+)\s+AS\s+\$\$(.*?)\$\$\s+LANGUAGE\s+(\w+)"
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return {
                'type': 'error',
                'message': 'Invalid CREATE FUNCTION syntax. Use: CREATE FUNCTION name(params) RETURNS type AS $$code$$ LANGUAGE plpython',
                'success': False
            }
        
        name = match.group(1)
        params_str = match.group(2)
        returns = match.group(3)
        body = match.group(4).strip()
        language = match.group(5).lower()
        
        # Valider le langage
        if language not in ['plpython', 'python']:
            return {
                'type': 'error',
                'message': f"Unsupported language: {language}. Use 'python' or 'plpython'",
                'success': False
            }
        
        # Extraire les paramètres
        params = []
        if params_str.strip():
            for param in params_str.split(','):
                param = param.strip()
                if param:
                    # Enlever le type si présent: "param TYPE" -> "param"
                    parts = param.split()
                    param_name = parts[0]
                    params.append(param_name)
        
        # Créer la fonction
        success, message = self.function_manager.create_function(name, params, body, returns)
        
        if success:
            # Sauvegarder dans la table
            self._save_function_to_db(name, params, body, returns)
            
            return {
                'type': 'create_function',
                'message': message,
                'name': name,
                'params': params,
                'returns': returns,
                'success': True
            }
        else:
            return {
                'type': 'error',
                'message': message,
                'success': False
            }
    
    def _drop_function(self, sql):
        """Gère DROP FUNCTION"""
        pattern = r"DROP\s+FUNCTION\s+(\w+)"
        match = re.search(pattern, sql, re.IGNORECASE)
        
        if not match:
            return {
                'type': 'error',
                'message': 'Invalid DROP FUNCTION syntax. Use: DROP FUNCTION name',
                'success': False
            }
        
        name = match.group(1)
        success, message = self.function_manager.drop_function(name)
        
        if success:
            # Supprimer de la table
            try:
                self.cursor.execute("DELETE FROM gsql_functions WHERE name = ?", (name,))
                self.conn.commit()
            except:
                pass
            
            return {
                'type': 'drop_function',
                'message': message,
                'success': True
            }
        else:
            return {
                'type': 'error',
                'message': message,
                'success': False
            }
    
    def _show_tables(self):
        """Affiche les tables"""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = []
        for row in self.cursor.fetchall():
            table_name = row[0]
            try:
                self.cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                row_count = self.cursor.fetchone()['count']
            except:
                row_count = 0
            
            tables.append({
                'table': table_name,
                'rows': row_count
            })
        
        return {
            'type': 'tables',
            'rows': tables,
            'count': len(tables),
            'message': f'Found {len(tables)} table(s)',
            'success': True
        }
    
    def _show_functions(self):
        """Affiche les fonctions"""
        functions = self.function_manager.list_functions()
        
        return {
            'type': 'functions',
            'rows': functions,
            'count': len(functions),
            'message': f'Found {len(functions)} function(s)',
            'success': True
        }
    
    def _show_help(self):
        """Affiche l'aide"""
        help_text = f"""
{Colors.bold("GSQL COMPLETE - Command Reference")}

{Colors.GREEN}DATABASE COMMANDS:{Colors.RESET}
  SHOW TABLES                    - List all tables
  SHOW FUNCTIONS                 - List all functions
  HELP                           - This help

{Colors.GREEN}SQL COMMANDS:{Colors.RESET}
  SELECT * FROM table            - Query data
  INSERT INTO table VALUES (...) - Insert data
  CREATE TABLE name (columns)    - Create table
  DROP TABLE name                - Drop table
  UPDATE table SET col=value     - Update data
  DELETE FROM table              - Delete data

{Colors.GREEN}FUNCTION COMMANDS:{Colors.RESET}
  CREATE FUNCTION name(params) RETURNS type AS $$code$$ LANGUAGE plpython
  DROP FUNCTION name             - Drop function

{Colors.GREEN}DOT COMMANDS:{Colors.RESET}
  .tables                        - Alias for SHOW TABLES
  .functions                     - Alias for SHOW FUNCTIONS
  .help                          - This help
  .exit / .quit                  - Exit
  .clear                         - Clear screen

{Colors.GREEN}EXAMPLE FUNCTIONS:{Colors.RESET}
  CREATE FUNCTION greeting(name) RETURNS TEXT AS $$
      return f'Hello, {{name}}!'
  $$ LANGUAGE plpython

  CREATE FUNCTION discount(price, percent) RETURNS REAL AS $$
      return price * (1 - percent/100)
  $$ LANGUAGE plpython

  SELECT username, greeting(username) FROM users
  SELECT name, price, discount(price, 10) as discounted FROM products
"""
        return {
            'type': 'help',
            'message': help_text,
            'success': True
        }
    
    def close(self):
        """Ferme la connexion"""
        if self.conn:
            self.conn.close()

# ==================== INTERFACE CLI ====================

class CompleteCLI(cmd.Cmd):
    """Interface CLI complète"""
    
    intro = f"""
{Colors.BOLD}{Colors.BLUE}╔═════════════════════════════╗
║               GSQL          ║
║                             ║
╚═════════════════════════════╝{Colors.RESET}

Type {Colors.GREEN}help{Colors.RESET} for commands, {Colors.GREEN}exit{Colors.RESET} to quit.
Database ready with sample data and functions.
"""
    
    prompt = f'{Colors.GREEN}gsql>{Colors.RESET} '
    
    def __init__(self, db_path=":memory:"):
        super().__init__()
        self.db = CompleteDatabase(db_path)
        print(Colors.success(f"Connected to: {db_path}"))
        
        # Afficher les tables
        result = self.db.execute("SHOW TABLES")
        if result['success'] and result['rows']:
            tables = [t['table'] for t in result['rows'][:5]]
            print(Colors.info(f"Tables: {', '.join(tables)}"))
        
        # Afficher les fonctions intégrées
        result = self.db.execute("SHOW FUNCTIONS")
        if result['success'] and result['rows']:
            funcs = [f['name'] for f in result['rows'] if f['type'] == 'builtin'][:5]
            print(Colors.info(f"Built-in functions: {', '.join(funcs)}"))
    
    # ===== COMMANDES DE BASE =====
    
    def do_exit(self, arg):
        """Exit GSQL"""
        print(Colors.success("👋 Goodbye!"))
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
    
    # ===== COMMANDES SQL & DOT =====
    
    def default(self, line):
        """Handle all commands"""
        line = line.strip()
        if not line:
            return
        
        # Dot commands
        if line.startswith('.'):
            cmd = line[1:].lower().split()[0] if ' ' in line else line[1:]
            
            if cmd == 'tables':
                self.do_tables("")
            elif cmd == 'functions':
                self.do_functions("")
            elif cmd == 'help':
                self.do_help("")
            elif cmd in ['exit', 'quit']:
                return self.do_exit("")
            elif cmd == 'clear':
                self.do_clear("")
            else:
                print(Colors.error(f"Unknown command: {line}"))
                print(Colors.info("Try: .tables, .functions, .help, .exit"))
            return
        
        # SQL commands
        self._execute_sql(line)
    
    def _execute_sql(self, sql):
        """Execute SQL command"""
        # Coloriser les mots-clés SQL
        colored = sql
        keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
                   'CREATE', 'TABLE', 'DROP', 'FUNCTION', 'RETURNS', 'AS',
                   'LANGUAGE', 'UPDATE', 'SET', 'DELETE', 'SHOW', 'HELP']
        
        for kw in keywords:
            colored = re.sub(r'\b' + kw + r'\b', Colors.sql(kw), colored, flags=re.IGNORECASE)
        
        print(f"{Colors.CYAN}SQL:{Colors.RESET} {colored}")
        
        # Exécuter
        result = self.db.execute(sql)
        self._print_result(result)
    
    def _print_result(self, result):
        """Print formatted result"""
        if not isinstance(result, dict):
            print(result)
            return
        
        if not result.get('success', True):
            print(Colors.error(result.get('message', 'Error')))
            return
        
        if result.get('type') == 'tables':
            rows = result.get('rows', [])
            if rows:
                print(f"\n{Colors.BOLD}📊 Tables ({len(rows)}):{Colors.RESET}")
                for table in rows:
                    print(f"  {Colors.GREEN}{table['table']:20}{Colors.RESET} - {table['rows']} rows")
            else:
                print(Colors.warning("No tables found"))
        
        elif result.get('type') == 'functions':
            rows = result.get('rows', [])
            if rows:
                print(f"\n{Colors.BOLD}🔧 Functions ({len(rows)}):{Colors.RESET}")
                
                # Built-in functions
                builtins = [f for f in rows if f['type'] == 'builtin']
                if builtins:
                    print(f"\n  {Colors.CYAN}Built-in:{Colors.RESET}")
                    for func in builtins[:10]:
                        print(f"    {Colors.YELLOW}{func['name']}{Colors.RESET} - {func.get('description', '')}")
                
                # User functions
                user_funcs = [f for f in rows if f['type'] == 'user']
                if user_funcs:
                    print(f"\n  {Colors.MAGENTA}User-defined:{Colors.RESET}")
                    for func in user_funcs:
                        params = ', '.join(func.get('params', []))
                        print(f"    {Colors.GREEN}{func['name']}({params}){Colors.RESET} → {func.get('returns', 'ANY')}")
                
                if len(rows) > 10:
                    print(Colors.info(f"... and {len(rows)-10} more functions"))
            else:
                print(Colors.warning("No functions found"))
        
        elif result.get('type') == 'select':
            rows = result.get('rows', [])
            count = result.get('count', 0)
            
            if count == 0:
                print(Colors.warning("No results found"))
                return
            
            print(f"\n{Colors.BOLD}📊 Results: {count} row(s){Colors.RESET}")
            
            if rows and isinstance(rows[0], dict):
                headers = list(rows[0].keys())
                
                # Calculer les largeurs
                widths = {h: len(str(h)) for h in headers}
                for row in rows[:20]:
                    for h in headers:
                        widths[h] = max(widths[h], len(str(row.get(h, ''))))
                
                # En-tête
                header_line = " | ".join(f"{Colors.BLUE}{h:<{widths[h]}}{Colors.RESET}" for h in headers)
                print(header_line)
                print("-" * sum(widths.values() + [3 * (len(headers) - 1)]))
                
                # Données
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
                    print(Colors.info(f"... and {len(rows)-20} more rows"))
            else:
                for i, row in enumerate(rows[:20], 1):
                    print(f"{i:3}. {row}")
                if len(rows) > 20:
                    print(Colors.info(f"... and {len(rows)-20} more rows"))
        
        elif result.get('type') in ['command', 'create_function', 'drop_function']:
            print(Colors.success(result.get('message', 'Command executed')))
        
        elif result.get('type') == 'help':
            print(result.get('message', ''))
        
        else:
            print(result)
    
    # ===== NLP SIMPLE =====
    
    def do_nl(self, arg):
        """Natural language query: nl [question]"""
        if not arg:
            print(Colors.error("Please provide a question"))
            return
        
        arg_lower = arg.lower()
        
        # Traductions simples
        translations = {
            'show tables': 'SHOW TABLES',
            'list tables': 'SHOW TABLES',
            'show functions': 'SHOW FUNCTIONS',
            'list functions': 'SHOW FUNCTIONS',
            'help': 'HELP',
            'users': 'SELECT * FROM users LIMIT 5',
            'products': 'SELECT * FROM products LIMIT 5',
            'create function': 'HELP',
            'make function': 'HELP'
        }
        
        for key, sql in translations.items():
            if key in arg_lower:
                self._execute_sql(sql)
                return
        
        # Si "table" est mentionné
        if 'table' in arg_lower:
            words = arg_lower.split()
            for word in words:
                if word != 'table' and len(word) > 2:
                    self._execute_sql(f"SELECT * FROM {word} LIMIT 5")
                    return
            self.do_tables("")
            return
        
        print(Colors.info("Try: 'show tables', 'show functions', 'users', 'products', 'help'"))

# ==================== MAIN ====================

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='GSQL Complete - SQL Database with Functions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
{Colors.BOLD}Examples:{Colors.RESET}
  gsql                          # Interactive mode
  gsql mydatabase.db            # Use specific database
  gsql --sql "SHOW TABLES"      # Single command
  gsql --demo                   # Run demonstration

{Colors.BOLD}Quick Start:{Colors.RESET}
  1. Create a function:
     {Colors.GREEN}CREATE FUNCTION greeting(name) RETURNS TEXT AS $$ return f'Hello, {{name}}!' $$ LANGUAGE plpython{Colors.RESET}
  
  2. Use the function:
     {Colors.GREEN}SELECT username, greeting(username) FROM users{Colors.RESET}
  
  3. See all functions:
     {Colors.GREEN}SHOW FUNCTIONS{Colors.RESET}
"""
    )
    
    parser.add_argument(
        'database',
        nargs='?',
        default=':memory:',
        help='Database file (default: in-memory)'
    )
    
    parser.add_argument(
        '--sql',
        help='Execute SQL command and exit'
    )
    
    parser.add_argument(
        '--demo',
        action='store_true',
        help='Run function demonstration'
    )
    
    parser.add_argument(
        '--no-colors',
        action='store_true',
        help='Disable colors'
    )
    
    args = parser.parse_args()
    
    # Désactiver les couleurs
    if args.no_colors:
        for attr in dir(Colors):
            if not attr.startswith('_'):
                setattr(Colors, attr, '')
    
    # Mode commande unique
    if args.sql:
        db = CompleteDatabase(args.database)
        result = db.execute(args.sql)
        cli = CompleteCLI(args.database)
        cli._print_result(result)
        db.close()
        return
    
    # Mode démo
    if args.demo:
        cli = CompleteCLI(args.database)
        print(f"\n{Colors.BOLD}🎬 FUNCTION DEMONSTRATION{Colors.RESET}\n")
        
        # Créer une fonction
        print(f"{Colors.CYAN}1. Creating a function:{Colors.RESET}")
        cli._execute_sql("""
            CREATE FUNCTION greeting(name) RETURNS TEXT AS $$
                return f'Hello, {name}!'
            $$ LANGUAGE plpython
        """)
        
        # Utiliser la fonction
        print(f"\n{Colors.CYAN}2. Using the function:{Colors.RESET}")
        cli._execute_sql("SELECT username, greeting(username) FROM users LIMIT 3")
        
        # Créer une autre fonction
        print(f"\n{Colors.CYAN}3. Creating a discount function:{Colors.RESET}")
        cli._execute_sql("""
            CREATE FUNCTION discount(price, percent) RETURNS REAL AS $$
                return price * (1 - percent/100)
            $$ LANGUAGE plpython
        """)
        
        # Utiliser la fonction de discount
        print(f"\n{Colors.CYAN}4. Using discount function:{Colors.RESET}")
        cli._execute_sql("SELECT name, price, discount(price, 10) as discounted FROM products")
        
        # Montrer toutes les fonctions
        print(f"\n{Colors.CYAN}5. All functions:{Colors.RESET}")
        cli._execute_sql("SHOW FUNCTIONS")
        
        cli.db.close()
        return
    
    # Mode interactif
    try:
        cli = CompleteCLI(args.database)
        cli.cmdloop()
    except KeyboardInterrupt:
        print(f"\n{Colors.warning('Interrupted')}")
    except Exception as e:
        print(Colors.error(f"Fatal error: {e}"))

if __name__ == '__main__':
    main()
