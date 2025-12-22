#!/usr/bin/env python3
"""
GSQL CLI - Command line interface for GSQL database
"""

import sys
import os
import cmd
import shlex
import logging
import argparse
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gsql.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class GSQLCLI(cmd.Cmd):
    """GSQL Command Line Interface"""
    
    intro = """
    ╔══════════════════════════════════════╗
    ║      GSQL Interactive Shell          ║
    ║      Version 2.0                    ║
    ╚══════════════════════════════════════╝
    
    Type 'help' for commands, 'exit' to quit.
    Use 'nl' for natural language queries.
    Use '.tables' to list tables, '.help' for help.
    """
    
    prompt = 'gsql> '
    
    def __init__(self, database_path=None, use_nlp=True):
        super().__init__()
        
        # Import here to avoid circular imports
        try:
            from gsql.database import Database
            self.Database = Database
        except ImportError as e:
            print(f"❌ Failed to import Database: {e}")
            print("Please check your installation.")
            sys.exit(1)
        
        self.database_path = database_path or ':memory:'
        self.use_nlp = use_nlp
        
        # Initialize database
        try:
            self.db = Database(self.database_path, use_nlp=self.use_nlp)
            print(f"✅ Connected to database: {self.database_path}")
            logger.info(f"Database connected: {self.database_path}")
        except Exception as e:
            print(f"❌ Error initializing database: {str(e)}")
            logger.error(f"Database initialization error: {str(e)}")
            sys.exit(1)
    
    def do_sql(self, arg):
        """
        Execute SQL command: sql SELECT * FROM table
        
        Examples:
          sql SELECT * FROM users
          sql CREATE TABLE products (id INT, name TEXT)
          sql INSERT INTO users VALUES (1, 'Alice')
        """
        if not arg:
            print("❌ Please provide SQL command")
            return
        
        try:
            result = self.db.execute(arg)
            self._display_result(result)
        except Exception as e:
            print(f"❌ SQL Error: {str(e)}")
    
    def do_nl(self, arg):
        """
        Execute natural language query
        
        Examples:
          nl montrer tables
          nl table users
          nl combien de users
          nl aide
        """
        if not arg:
            print("❌ Please provide a question")
            return
        
        if not self.use_nlp:
            print("❌ NLP is not enabled. Start with --no-nlp to disable.")
            return
        
        try:
            print(f"🔍 Question: {arg}")
            result = self.db.execute_nl(arg)
            self._display_result(result)
        except Exception as e:
            print(f"❌ NLP Error: {str(e)}")
    
    def do_exec(self, arg):
        """Execute command (alias for default): exec SELECT * FROM table"""
        self.default(arg)
    
    def do_create(self, arg):
        """
        Create objects: create table|function
        
        Examples:
          create table users (id INT, name TEXT)
          create function name(params) RETURNS type AS $$code$$ LANGUAGE plpython
        """
        if not arg:
            print("❌ Please specify what to create: table|function")
            return
        
        try:
            if arg.lower().startswith('table'):
                sql = f"CREATE {arg}"
                result = self.db.execute(sql)
                self._display_result(result)
            elif arg.lower().startswith('function'):
                sql = f"CREATE {arg}"
                result = self.db.execute(sql)
                self._display_result(result)
            else:
                print("❌ Unknown create type. Use: create table|function")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_insert(self, arg):
        """
        Insert data: insert INTO table VALUES (values)
        
        Example: insert INTO users VALUES (1, 'Alice')
        """
        if not arg:
            print("❌ Please provide INSERT statement")
            return
        
        try:
            if not arg.upper().startswith('INTO'):
                arg = f"INTO {arg}"
            sql = f"INSERT {arg}"
            result = self.db.execute(sql)
            self._display_result(result)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_select(self, arg):
        """
        Select data: select * FROM table [WHERE condition]
        
        Examples:
          select * FROM users
          select name, age FROM users WHERE age > 25
        """
        if not arg:
            print("❌ Please provide SELECT statement")
            return
        
        try:
            sql = f"SELECT {arg}"
            result = self.db.execute(sql)
            self._display_result(result)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_show(self, arg):
        """
        Show information: show tables|functions
        
        Examples:
          show tables
          show functions
        """
        if not arg:
            print("❌ Please specify: tables|functions")
            return
        
        arg = arg.lower().strip()
        
        try:
            if arg == 'tables':
                result = self.db.execute("SHOW TABLES")
                self._display_result(result)
            elif arg == 'functions':
                result = self.db.execute("SHOW FUNCTIONS")
                self._display_result(result)
            else:
                print(f"❌ Unknown show command: {arg}")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_tables(self, arg):
        """List all tables (alias for .tables)"""
        self.do_show('tables')
    
    def do_functions(self, arg):
        """List all functions (alias for .tables)"""
        self.do_show('functions')
    
    def do_describe(self, arg):
        """
        Describe table structure: describe table_name
        
        Example: describe users
        """
        if not arg:
            print("❌ Please provide table name")
            return
        
        try:
            # Simple implementation - would need schema support
            print(f"📋 Table: {arg}")
            print("   Columns would be listed here")
            print("   (Full schema support coming soon)")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_import(self, arg):
        """
        Import CSV file: import file.csv [table_name]
        
        Examples:
          import users.csv
          import data.csv mytable
        """
        if not arg:
            print("❌ Please provide filename")
            return
        
        parts = arg.split()
        filename = parts[0]
        table_name = parts[1] if len(parts) > 1 else None
        
        try:
            result = self.db.import_csv(filename, table_name)
            print(f"✅ {result}")
        except Exception as e:
            print(f"❌ Import error: {str(e)}")
    
    def do_export(self, arg):
        """
        Export table to CSV: export table_name [file.csv]
        
        Examples:
          export users
          export products products.csv
        """
        if not arg:
            print("❌ Please provide table name")
            return
        
        parts = arg.split()
        table_name = parts[0]
        filename = parts[1] if len(parts) > 1 else f"{table_name}.csv"
        
        try:
            result = self.db.export_csv(table_name, filename)
            print(f"✅ {result}")
        except Exception as e:
            print(f"❌ Export error: {str(e)}")
    
    def do_cache(self, arg):
        """Cache operations: cache stats, cache clear"""
        if not arg:
            print("❌ Available subcommands: stats, clear")
            return
        
        subcmd = arg.lower().strip()
        
        if subcmd == 'stats':
            try:
                stats = self.db.get_cache_stats()
                print("\n" + "="*60)
                print(f"{'CACHE STATISTICS':^60}")
                print("="*60)
                for key, value in stats.items():
                    print(f"{key.replace('_', ' ').title():20}: {value}")
                print("="*60)
            except Exception as e:
                print(f"❌ Error: {str(e)}")
        
        elif subcmd == 'clear':
            try:
                result = self.db.clear_cache()
                print(f"✅ {result}")
            except Exception as e:
                print(f"❌ Error: {str(e)}")
        
        else:
            print(f"❌ Unknown subcommand: {subcmd}")
    
    def do_clear(self, arg):
        """Clear the screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def do_exit(self, arg):
        """Exit GSQL"""
        print("👋 Goodbye!")
        if hasattr(self, 'db'):
            self.db.close()
        return True
    
    def do_quit(self, arg):
        """Exit GSQL"""
        return self.do_exit(arg)
    
    def do_EOF(self, arg):
        """Exit on Ctrl-D"""
        print()
        return self.do_exit(arg)
    
    def default(self, line):
        """
        Default command handler
        
        Handles:
        - SQL commands (SELECT, INSERT, etc.)
        - Dot commands (.tables, .help)
        - GSQL commands (SHOW TABLES, etc.)
        """
        if not line.strip():
            return
        
        line = line.strip()
        
        # Handle dot commands
        if line.startswith('.'):
            self._handle_dot_command(line)
            return
        
        # Handle SQL/GSQL commands
        try:
            result = self.db.execute(line)
            self._display_result(result)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def _handle_dot_command(self, command):
        """Handle SQLite-style dot commands"""
        cmd = command[1:].lower().strip()
        
        if cmd in ['tables', 'table']:
            self.do_show('tables')
        elif cmd in ['help', '?']:
            self._display_help()
        elif cmd in ['exit', 'quit']:
            self.do_exit('')
        elif cmd == 'schema':
            print("📋 Schema commands:")
            print("  .tables - List tables")
            print("  .help   - Show help")
            print("  .exit   - Exit shell")
        elif cmd == 'version':
            print("📋 GSQL Version 2.0")
        elif cmd == 'timer':
            print("⏱️  Timer: ON (always)")
        elif cmd == 'headers':
            print("📋 Headers: ON (always)")
        elif cmd == 'mode':
            print("📋 Mode: list (tabular)")
        elif cmd == '':
            print("❌ Empty dot command")
        else:
            print(f"❌ Unknown dot command: {command}")
            print("   Try: .tables, .help, .exit")
    
    def _display_help(self):
        """Display help information"""
        help_text = """
GSQL Interactive Shell Help:

General Commands:
  help                        - Show this help
  exit, quit                  - Exit the shell
  clear                       - Clear screen

SQL Commands:
  sql <statement>             - Execute SQL statement
  select <columns> FROM <table> - Query data
  insert INTO <table> VALUES  - Insert data
  create table|function       - Create objects

GSQL Commands:
  show tables|functions       - List tables/functions
  tables, functions           - Aliases for show commands
  nl <question>               - Natural language query
  describe <table>            - Show table structure

Data Import/Export:
  import <file.csv> [table]   - Import CSV file
  export <table> [file.csv]   - Export to CSV

Dot Commands (SQLite-style):
  .tables                     - List all tables
  .help                       - Show this help
  .exit, .quit               - Exit shell

Natural Language Examples:
  "montrer tables"            -> Show tables
  "table users"               -> SELECT * FROM users
  "combien de users"          -> SELECT COUNT(*) FROM users
  "aide"                      -> Show help

Type any SQL statement to execute it directly.
"""
        print(help_text)
    
    def _display_result(self, result):
        """Display query results in a readable format"""
        if result is None:
            print("✅ Command executed successfully")
            return
        
        if isinstance(result, str):
            print(f"📋 {result}")
            return
        
        if isinstance(result, dict):
            # Handle different result types
            
            # HELP messages
            if result.get('type') == 'help' and 'message' in result:
                print(result['message'])
                return
            
            # SHOW TABLES
            if result.get('type') in ['show_tables', 'tables'] and 'rows' in result:
                rows = result['rows']
                if rows:
                    message = result.get('message', f'Found {len(rows)} table(s):')
                    print(f"\n📊 {message}")
                    
                    # Try to use tabulate for nice formatting
                    try:
                        from tabulate import tabulate
                        print(tabulate(rows, headers="keys", tablefmt="grid"))
                    except ImportError:
                        # Simple formatting
                        for row in rows:
                            if isinstance(row, dict):
                                table_name = row.get('table', 'unknown')
                                row_count = row.get('rows', 0)
                                print(f"  {table_name}: {row_count} rows")
                            else:
                                print(f"  {row}")
                else:
                    print("📭 No tables found")
                return
            
            # SHOW FUNCTIONS
            if result.get('type') == 'show_functions' and 'rows' in result:
                rows = result['rows']
                if rows:
                    message = result.get('message', f'Found {len(rows)} function(s):')
                    print(f"\n🔧 {message}")
                    
                    for row in rows:
                        if isinstance(row, dict):
                            func_name = row.get('name', 'unknown')
                            func_type = row.get('type', 'unknown')
                            desc = row.get('description', '')
                            
                            if func_type == 'builtin':
                                print(f"  📦 {func_name} - {desc}")
                            else:
                                created = row.get('created_at', '')
                                if hasattr(created, 'strftime'):
                                    created = created.strftime('%Y-%m-%d')
                                print(f"  👤 {func_name} - User function ({created})")
                        else:
                            print(f"  {row}")
                else:
                    print("📭 No functions found")
                return
            
            # SELECT results
            if 'rows' in result and result['rows']:
                rows = result['rows']
                count = result.get('count', len(rows))
                
                print(f"\n📊 Results: {count} row(s)")
                
                if rows:
                    # Try to use tabulate for nice formatting
                    try:
                        from tabulate import tabulate
                        print(tabulate(rows, headers="keys", tablefmt="grid"))
                    except ImportError:
                        # Simple table formatting
                        if isinstance(rows[0], dict):
                            headers = list(rows[0].keys())
                            
                            # Calculate column widths
                            col_widths = {}
                            for header in headers:
                                col_widths[header] = len(str(header))
                                for row in rows:
                                    if header in row:
                                        col_widths[header] = max(col_widths[header], len(str(row[header])))
                            
                            # Print header
                            header_line = " | ".join([str(h).ljust(col_widths[h]) for h in headers])
                            separator = "-+-".join(["-" * col_widths[h] for h in headers])
                            print(header_line)
                            print(separator)
                            
                            # Print rows
                            for row in rows[:50]:  # Limit to 50 rows
                                row_line = " | ".join([str(row.get(h, '')).ljust(col_widths[h]) for h in headers])
                                print(row_line)
                            
                            if len(rows) > 50:
                                print(f"... and {len(rows) - 50} more rows")
                        else:
                            for i, row in enumerate(rows[:50], 1):
                                print(f"{i:3}. {row}")
                            if len(rows) > 50:
                                print(f"... and {len(rows) - 50} more")
                else:
                    print("📭 No rows returned")
            
            elif 'message' in result:
                print(f"📋 {result['message']}")
            
            elif 'error' in result:
                print(f"❌ Error: {result['error']}")
            
            else:
                print(f"📋 Result: {result}")
        
        elif isinstance(result, list):
            if result:
                print(f"\n📊 Results: {len(result)} row(s)")
                for i, row in enumerate(result[:50], 1):
                    print(f"{i:3}. {row}")
                if len(result) > 50:
                    print(f"... and {len(result) - 50} more")
            else:
                print("📭 No results")
        
        else:
            print(f"📋 Result: {result}")
    
    def do_help(self, arg):
        """Show help information"""
        if arg:
            # Specific command help
            cmd_func = getattr(self, 'do_' + arg, None)
            if cmd_func and cmd_func.__doc__:
                print(f"\n{arg}: {cmd_func.__doc__}")
            else:
                print(f"\nNo help available for '{arg}'")
        else:
            self._display_help()

def main():
    """Main entry point for GSQL CLI"""
    parser = argparse.ArgumentParser(
        description='GSQL Database Engine with Natural Language Interface',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  gsql mydb.gsql                    # Interactive mode
  gsql --sql "SELECT * FROM users"  # Single SQL command
  gsql --nl "show all users"        # Natural language query
  gsql --no-nlp                     # Disable NLP features
        """
    )
    
    parser.add_argument(
        'database',
        nargs='?',
        default=':memory:',
        help='Database file path (default: :memory:)'
    )
    parser.add_argument(
        '--sql',
        help='Execute single SQL command and exit'
    )
    parser.add_argument(
        '--nl',
        help='Execute natural language query and exit'
    )
    parser.add_argument(
        '--no-nlp',
        action='store_true',
        help='Disable natural language processing'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    parser.add_argument(
        '--version',
        action='version',
        version='GSQL 2.0'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose mode enabled")
    
    # Single command mode
    if args.sql:
        try:
            from gsql.database import Database
            db = Database(args.database, use_nlp=not args.no_nlp)
            result = db.execute(args.sql)
            
            cli = GSQLCLI(args.database, use_nlp=not args.no_nlp)
            cli._display_result(result)
            
            db.close()
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            sys.exit(1)
    
    elif args.nl:
        try:
            from gsql.database import Database
            db = Database(args.database, use_nlp=not args.no_nlp)
            result = db.execute_nl(args.nl)
            
            cli = GSQLCLI(args.database, use_nlp=not args.no_nlp)
            cli._display_result(result)
            
            db.close()
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            sys.exit(1)
    
    else:
        # Interactive mode
        try:
            cli = GSQLCLI(args.database, use_nlp=not args.no_nlp)
            cli.cmdloop()
        except KeyboardInterrupt:
            print("\n\nInterrupted. Exiting...")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Fatal error: {str(e)}")
            sys.exit(1)

if __name__ == '__main__':
    main()
