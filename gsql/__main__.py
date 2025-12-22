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

# Import des couleurs
try:
    from gsql.cli.colors import Colors
    from gsql.cli.formatter import OutputFormatter as fmt
    COLORS_AVAILABLE = True
except ImportError:
    # Fallback sans couleurs
    class Colors:
        TITLE = HEADER = PROMPT = SUCCESS = ERROR = WARNING = INFO = HELP = ''
        TABLE = COLUMN = ROW = ROW_ALT = ''
        RESET = ''
    
    class fmt:
        @staticmethod
        def format_result(result): return str(result)
        @staticmethod
        def format_sql(sql): return sql
        @staticmethod
        def format_nlp_question(q): return f"🔍 Question: {q}"
        @staticmethod
        def format_nlp_sql(sql): return f"📊 SQL généré: {sql}"
    
    COLORS_AVAILABLE = False

class GSQLCLI(cmd.Cmd):
    """GSQL Command Line Interface avec couleurs"""
    
    intro = f"""
    {Colors.TITLE}╔══════════════════════════════════════╗
    ║      GSQL Interactive Shell          ║
    ║      Version 2.0 with Colors         ║
    ╚══════════════════════════════════════╝{Colors.RESET}
    
    Type {Colors.PROMPT}'help'{Colors.RESET} for commands, {Colors.PROMPT}'exit'{Colors.RESET} to quit.
    Use {Colors.PROMPT}'nl'{Colors.RESET} for natural language queries.
    Use {Colors.PROMPT}'.tables'{Colors.RESET} to list tables, {Colors.PROMPT}'.help'{Colors.RESET} for help.
    """
    
    prompt = f'{Colors.PROMPT}gsql>{Colors.RESET} '
    
    def __init__(self, database_path=None, use_nlp=True):
        super().__init__()
        
        # Import here to avoid circular imports
        try:
            from gsql.database import Database
            self.Database = Database
        except ImportError as e:
            print(f"{Colors.ERROR}❌ Failed to import Database: {e}{Colors.RESET}")
            print("Please check your installation.")
            sys.exit(1)
        
        self.database_path = database_path or ':memory:'
        self.use_nlp = use_nlp
        
        # Initialize database
        try:
            self.db = Database(self.database_path, use_nlp=self.use_nlp)
            print(f"{Colors.SUCCESS}✅ Connected to database: {self.database_path}{Colors.RESET}")
            logger.info(f"Database connected: {self.database_path}")
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error initializing database: {str(e)}{Colors.RESET}")
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
            print(f"{Colors.ERROR}❌ Please provide SQL command{Colors.RESET}")
            return
        
        try:
            result = self.db.execute(arg)
            print(fmt.format_result(result))
        except Exception as e:
            print(f"{Colors.ERROR}❌ SQL Error: {str(e)}{Colors.RESET}")
    
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
            print(f"{Colors.ERROR}❌ Please provide a question{Colors.RESET}")
            return
        
        if not self.use_nlp:
            print(f"{Colors.ERROR}❌ NLP is not enabled. Start with --no-nlp to disable.{Colors.RESET}")
            return
        
        try:
            print(fmt.format_nlp_question(arg))
            result = self.db.execute_nl(arg)
            print(fmt.format_result(result))
        except Exception as e:
            print(f"{Colors.ERROR}❌ NLP Error: {str(e)}{Colors.RESET}")
    
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
            print(f"{Colors.ERROR}❌ Please specify what to create: table|function{Colors.RESET}")
            return
        
        try:
            if arg.lower().startswith('table'):
                sql = f"CREATE {arg}"
                print(f"{Colors.INFO}📝 Creating table...{Colors.RESET}")
                print(f"{Colors.SQL_KEYWORD}{fmt.format_sql(sql)}{Colors.RESET}")
                result = self.db.execute(sql)
                print(fmt.format_result(result))
            elif arg.lower().startswith('function'):
                sql = f"CREATE {arg}"
                print(f"{Colors.INFO}📝 Creating function...{Colors.RESET}")
                print(f"{Colors.SQL_KEYWORD}{fmt.format_sql(sql)}{Colors.RESET}")
                result = self.db.execute(sql)
                print(fmt.format_result(result))
            else:
                print(f"{Colors.ERROR}❌ Unknown create type. Use: create table|function{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
    
    def do_insert(self, arg):
        """
        Insert data: insert INTO table VALUES (values)
        
        Example: insert INTO users VALUES (1, 'Alice')
        """
        if not arg:
            print(f"{Colors.ERROR}❌ Please provide INSERT statement{Colors.RESET}")
            return
        
        try:
            if not arg.upper().startswith('INTO'):
                arg = f"INTO {arg}"
            sql = f"INSERT {arg}"
            print(f"{Colors.INFO}📝 Inserting data...{Colors.RESET}")
            print(f"{Colors.SQL_KEYWORD}{fmt.format_sql(sql)}{Colors.RESET}")
            result = self.db.execute(sql)
            print(fmt.format_result(result))
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
    
    def do_select(self, arg):
        """
        Select data: select * FROM table [WHERE condition]
        
        Examples:
          select * FROM users
          select name, age FROM users WHERE age > 25
        """
        if not arg:
            print(f"{Colors.ERROR}❌ Please provide SELECT statement{Colors.RESET}")
            return
        
        try:
            sql = f"SELECT {arg}"
            print(f"{Colors.INFO}📝 Executing query...{Colors.RESET}")
            print(f"{Colors.SQL_KEYWORD}{fmt.format_sql(sql)}{Colors.RESET}")
            result = self.db.execute(sql)
            print(fmt.format_result(result))
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
    
    def do_show(self, arg):
        """
        Show information: show tables|functions
        
        Examples:
          show tables
          show functions
        """
        if not arg:
            print(f"{Colors.ERROR}❌ Please specify: tables|functions{Colors.RESET}")
            return
        
        arg = arg.lower().strip()
        
        try:
            if arg == 'tables':
                print(f"{Colors.INFO}📊 Listing tables...{Colors.RESET}")
                result = self.db.execute("SHOW TABLES")
                print(fmt.format_result(result))
            elif arg == 'functions':
                print(f"{Colors.INFO}🔧 Listing functions...{Colors.RESET}")
                result = self.db.execute("SHOW FUNCTIONS")
                print(fmt.format_result(result))
            else:
                print(f"{Colors.ERROR}❌ Unknown show command: {arg}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
    
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
            print(f"{Colors.ERROR}❌ Please provide table name{Colors.RESET}")
            return
        
        try:
            print(f"{Colors.INFO}📋 Table: {Colors.TABLE}{arg}{Colors.RESET}")
            print(f"{Colors.INFO}   Columns would be listed here{Colors.RESET}")
            print(f"{Colors.INFO}   (Full schema support coming soon){Colors.RESET}")
            
            # Essaye de récupérer le schéma
            try:
                result = self.db.execute(f"PRAGMA table_info({arg})")
                if result and 'rows' in result and result['rows']:
                    print(f"\n{Colors.INFO}📋 Schema found:{Colors.RESET}")
                    Colors.print_table(None, result['rows'])
            except:
                pass
                
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
    
    def do_import(self, arg):
        """
        Import CSV file: import file.csv [table_name]
        
        Examples:
          import users.csv
          import data.csv mytable
        """
        if not arg:
            print(f"{Colors.ERROR}❌ Please provide filename{Colors.RESET}")
            return
        
        parts = arg.split()
        filename = parts[0]
        table_name = parts[1] if len(parts) > 1 else None
        
        try:
            print(f"{Colors.INFO}📥 Importing {filename}...{Colors.RESET}")
            result = self.db.import_csv(filename, table_name)
            print(f"{Colors.SUCCESS}✅ {result}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.ERROR}❌ Import error: {str(e)}{Colors.RESET}")
    
    def do_export(self, arg):
        """
        Export table to CSV: export table_name [file.csv]
        
        Examples:
          export users
          export products products.csv
        """
        if not arg:
            print(f"{Colors.ERROR}❌ Please provide table name{Colors.RESET}")
            return
        
        parts = arg.split()
        table_name = parts[0]
        filename = parts[1] if len(parts) > 1 else f"{table_name}.csv"
        
        try:
            print(f"{Colors.INFO}📤 Exporting {table_name} to {filename}...{Colors.RESET}")
            result = self.db.export_csv(table_name, filename)
            print(f"{Colors.SUCCESS}✅ {result}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.ERROR}❌ Export error: {str(e)}{Colors.RESET}")
    
    def do_cache(self, arg):
        """Cache operations: cache stats, cache clear"""
        if not arg:
            print(f"{Colors.ERROR}❌ Available subcommands: stats, clear{Colors.RESET}")
            return
        
        subcmd = arg.lower().strip()
        
        if subcmd == 'stats':
            try:
                stats = self.db.get_cache_stats()
                print(f"\n{Colors.HEADER}{'='*60}{Colors.RESET}")
                print(f"{Colors.TITLE}{'CACHE STATISTICS':^60}{Colors.RESET}")
                print(f"{Colors.HEADER}{'='*60}{Colors.RESET}")
                for key, value in stats.items():
                    key_display = key.replace('_', ' ').title()
                    print(f"{Colors.COLUMN}{key_display:20}{Colors.RESET}: {Colors.INFO}{value}{Colors.RESET}")
                print(f"{Colors.HEADER}{'='*60}{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
        
        elif subcmd == 'clear':
            try:
                result = self.db.clear_cache()
                print(f"{Colors.SUCCESS}✅ {result}{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
        
        else:
            print(f"{Colors.ERROR}❌ Unknown subcommand: {subcmd}{Colors.RESET}")
    
    def do_clear(self, arg):
        """Clear the screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def do_exit(self, arg):
        """Exit GSQL"""
        print(f"{Colors.SUCCESS}👋 Goodbye!{Colors.RESET}")
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
            # Afficher le SQL colorisé
            if line.upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER')):
                print(f"{Colors.INFO}📝 Executing...{Colors.RESET}")
                print(f"{Colors.SQL_KEYWORD}{fmt.format_sql(line)}{Colors.RESET}")
            
            result = self.db.execute(line)
            print(fmt.format_result(result))
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
    
    def _handle_dot_command(self, command):
        """Handle SQLite-style dot commands"""
        cmd = command[1:].lower().strip()
        
        if cmd in ['tables', 'table']:
            print(f"{Colors.INFO}📊 Listing tables...{Colors.RESET}")
            self.do_show('tables')
        elif cmd in ['help', '?']:
            self._display_help()
        elif cmd in ['exit', 'quit']:
            self.do_exit('')
        elif cmd == 'schema':
            print(f"{Colors.INFO}📋 Schema commands:{Colors.RESET}")
            print(f"  {Colors.PROMPT}.tables{Colors.RESET} - List tables")
            print(f"  {Colors.PROMPT}.help{Colors.RESET}   - Show help")
            print(f"  {Colors.PROMPT}.exit{Colors.RESET}   - Exit shell")
        elif cmd == 'version':
            print(f"{Colors.TITLE}📋 GSQL Version 2.0 with Colors{Colors.RESET}")
        elif cmd == 'timer':
            print(f"{Colors.INFO}⏱️  Timer: ON (always){Colors.RESET}")
        elif cmd == 'headers':
            print(f"{Colors.INFO}📋 Headers: ON (always){Colors.RESET}")
        elif cmd == 'mode':
            print(f"{Colors.INFO}📋 Mode: list (tabular){Colors.RESET}")
        elif cmd == 'colors':
            if COLORS_AVAILABLE:
                print(f"{Colors.SUCCESS}✅ Colors: ENABLED{Colors.RESET}")
                self._demo_colors()
            else:
                print(f"{Colors.WARNING}⚠ Colors: DISABLED (install colorama){Colors.RESET}")
        elif cmd == '':
            print(f"{Colors.ERROR}❌ Empty dot command{Colors.RESET}")
        else:
            print(f"{Colors.ERROR}❌ Unknown dot command: {command}{Colors.RESET}")
            print(f"   Try: {Colors.PROMPT}.tables{Colors.RESET}, {Colors.PROMPT}.help{Colors.RESET}, {Colors.PROMPT}.exit{Colors.RESET}")
    
    def _demo_colors(self):
        """Démonstration des couleurs disponibles"""
        print(f"\n{Colors.TITLE}🎨 Available Colors:{Colors.RESET}")
        print(f"  {Colors.SUCCESS}Success message{Colors.RESET}")
        print(f"  {Colors.ERROR}Error message{Colors.RESET}")
        print(f"  {Colors.WARNING}Warning message{Colors.RESET}")
        print(f"  {Colors.INFO}Info message{Colors.RESET}")
        print(f"  {Colors.HELP}Help text{Colors.RESET}")
        print(f"\n{Colors.SQL_KEYWORD}SQL keywords{Colors.RESET}, {Colors.SQL_FUNCTION}functions(){Colors.RESET}")
        print(f"{Colors.SQL_STRING}'String values'{Colors.RESET}, {Colors.SQL_NUMBER}123.45{Colors.RESET}")
    
    def _display_help(self):
        """Display help information with colors"""
        help_text = f"""
{Colors.TITLE}GSQL Interactive Shell Help:{Colors.RESET}

{Colors.HEADER}General Commands:{Colors.RESET}
  {Colors.PROMPT}help{Colors.RESET}                        - Show this help
  {Colors.PROMPT}exit, quit{Colors.RESET}                  - Exit the shell
  {Colors.PROMPT}clear{Colors.RESET}                       - Clear screen
  {Colors.PROMPT}.colors{Colors.RESET}                     - Show color demo

{Colors.HEADER}SQL Commands:{Colors.RESET}
  {Colors.PROMPT}sql <statement>{Colors.RESET}             - Execute SQL statement
  {Colors.PROMPT}select <columns> FROM <table>{Colors.RESET} - Query data
  {Colors.PROMPT}insert INTO <table> VALUES{Colors.RESET}  - Insert data
  {Colors.PROMPT}create table|function{Colors.RESET}       - Create objects

{Colors.HEADER}GSQL Commands:{Colors.RESET}
  {Colors.PROMPT}show tables|functions{Colors.RESET}       - List tables/functions
  {Colors.PROMPT}tables, functions{Colors.RESET}           - Aliases for show commands
  {Colors.PROMPT}nl <question>{Colors.RESET}               - Natural language query
  {Colors.PROMPT}describe <table>{Colors.RESET}            - Show table structure

{Colors.HEADER}Data Import/Export:{Colors.RESET}
  {Colors.PROMPT}import <file.csv> [table]{Colors.RESET}   - Import CSV file
  {Colors.PROMPT}export <table> [file.csv]{Colors.RESET}   - Export to CSV

{Colors.HEADER}Dot Commands (SQLite-style):{Colors.RESET}
  {Colors.PROMPT}.tables{Colors.RESET}                     - List all tables
  {Colors.PROMPT}.help{Colors.RESET}                       - Show this help
  {Colors.PROMPT}.exit, .quit{Colors.RESET}               - Exit shell
  {Colors.PROMPT}.version{Colors.RESET}                    - Show version

{Colors.HEADER}Natural Language Examples:{Colors.RESET}
  {Colors.NLP_QUESTION}"montrer tables"{Colors.RESET}            -> Show tables
  {Colors.NLP_QUESTION}"table users"{Colors.RESET}               -> SELECT * FROM users
  {Colors.NLP_QUESTION}"combien de users"{Colors.RESET}          -> SELECT COUNT(*) FROM users
  {Colors.NLP_QUESTION}"aide"{Colors.RESET}                      -> Show help

Type any SQL statement to execute it directly.
"""
        print(help_text)
    
    def do_help(self, arg):
        """Show help information"""
        if arg:
            # Specific command help
            cmd_func = getattr(self, 'do_' + arg, None)
            if cmd_func and cmd_func.__doc__:
                print(f"\n{Colors.TITLE}{arg}:{Colors.RESET} {cmd_func.__doc__}")
            else:
                print(f"\n{Colors.ERROR}No help available for '{arg}'{Colors.RESET}")
        else:
            self._display_help()

def main():
    """Main entry point for GSQL CLI"""
    parser = argparse.ArgumentParser(
        description='GSQL Database Engine with Natural Language Interface',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  {Colors.PROMPT}gsql mydb.gsql{Colors.RESET}                    # Interactive mode
  {Colors.PROMPT}gsql --sql "SELECT * FROM users"{Colors.RESET}  # Single SQL command
  {Colors.PROMPT}gsql --nl "show all users"{Colors.RESET}        # Natural language query
  {Colors.PROMPT}gsql --no-nlp{Colors.RESET}                     # Disable NLP features
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
        '--no-colors',
        action='store_true',
        help='Disable colors in output'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    parser.add_argument(
        '--version',
        action='version',
        version='GSQL 2.0 with Colors'
    )
    
    args = parser.parse_args()
    
    # Disable colors if requested
    if args.no_colors:
        global COLORS_AVAILABLE
        COLORS_AVAILABLE = False
    
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
            print(fmt.format_result(result))
            
            db.close()
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
            sys.exit(1)
    
    elif args.nl:
        try:
            from gsql.database import Database
            db = Database(args.database, use_nlp=not args.no_nlp)
            result = db.execute_nl(args.nl)
            
            cli = GSQLCLI(args.database, use_nlp=not args.no_nlp)
            print(fmt.format_result(result))
            
            db.close()
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
            sys.exit(1)
    
    else:
        # Interactive mode
        try:
            cli = GSQLCLI(args.database, use_nlp=not args.no_nlp)
            cli.cmdloop()
        except KeyboardInterrupt:
            print(f"\n\n{Colors.WARNING}Interrupted. Exiting...{Colors.RESET}")
            sys.exit(0)
        except Exception as e:
            print(f"{Colors.ERROR}❌ Fatal error: {str(e)}{Colors.RESET}")
            sys.exit(1)

if __name__ == '__main__':
    main()
