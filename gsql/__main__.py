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
        
        self.database_path = database_path or 'default.gsql'
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
        """Execute SQL command: sql SELECT * FROM table"""
        if not arg:
            print("❌ Please provide SQL command")
            return
        
        try:
            result = self.db.execute(arg)
            self._display_result(result)
        except Exception as e:
            print(f"❌ SQL Error: {str(e)}")
    
    def do_nl(self, arg):
        """Execute natural language query: nl show all users"""
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
    
    def do_create_function(self, arg):
        """Create a user-defined function"""
        if not arg:
            print("❌ Please provide function definition")
            return
        
        try:
            result = self.db.create_function_from_sql(arg)
            print(f"✅ {result}")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_list_functions(self, arg):
        """List all available functions"""
        try:
            functions = self.db.list_functions()
            
            if not functions:
                print("📝 No functions available")
                return
            
            print("\n" + "="*60)
            print(f"{'FUNCTIONS':^60}")
            print("="*60)
            
            for i, func in enumerate(functions, 1):
                if func.get('type') == 'builtin':
                    print(f"{i:2}. 📦 {func['name']}()")
                else:
                    created = func.get('created_at', 'unknown')
                    if hasattr(created, 'strftime'):
                        created = created.strftime('%Y-%m-%d')
                    print(f"{i:2}. 👤 {func['name']}({func.get('params', '')}) → {func.get('return_type', 'TEXT')} ({created})")
            
            print("="*60)
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
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
    
    def do_import(self, arg):
        """Import CSV file: import file.csv [table_name]"""
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
        """Export table to CSV: export table_name [file.csv]"""
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
    
    def do_tables(self, arg):
        """List all tables in the database"""
        try:
            result = self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            self._display_result(result)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_schema(self, arg):
        """Show schema of a table: schema table_name"""
        if not arg:
            print("❌ Please provide table name")
            return
        
        try:
            # This is a simplified version - you'll need to implement proper schema query
            result = self.db.execute(f"PRAGMA table_info({arg})")
            self._display_result(result)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
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
    
    def default(self, line):
        """Default command handler - try to execute as SQL"""
        if not line.strip():
            return
        
        try:
            result = self.db.execute(line)
            self._display_result(result)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def _display_result(self, result):
        """Display query results in a readable format"""
        if result is None:
            print("✅ Command executed successfully")
            return
        
        if isinstance(result, str):
            print(f"📋 {result}")
            return
        
        if isinstance(result, dict):
            if 'rows' in result and result['rows']:
                rows = result['rows']
                print(f"\n📊 Results: {len(rows)} row(s)")
                
                # Simple table display
                try:
                    from tabulate import tabulate
                    print(tabulate(rows, headers="keys", tablefmt="grid"))
                except ImportError:
                    # Fallback if tabulate not available
                    headers = list(rows[0].keys())
                    print(" | ".join(headers))
                    print("-" * (len(" | ".join(headers))))
                    for row in rows[:20]:  # Limit to 20 rows
                        values = [str(row[h])[:30] for h in headers]
                        print(" | ".join(values))
                    
                    if len(rows) > 20:
                        print(f"... and {len(rows) - 20} more rows")
            
            elif 'message' in result:
                print(f"📋 {result['message']}")
            
            elif 'error' in result:
                print(f"❌ Error: {result['error']}")
        
        elif isinstance(result, list):
            if result:
                print(f"\n📊 Results: {len(result)} row(s)")
                for i, row in enumerate(result[:20], 1):
                    print(f"{i:3}. {row}")
                if len(result) > 20:
                    print(f"... and {len(result) - 20} more")
            else:
                print("📭 No results")
        
        else:
            print(f"📋 Result: {result}")

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
        version='GSQL 1.0.0'
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
