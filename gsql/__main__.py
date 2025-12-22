#!/usr/bin/env python3
"""
GSQL CLI - Version simplifiée sans erreurs de couleurs
"""

import sys
import os
import cmd
import logging
import argparse

# Désactiver les couleurs temporairement pour déboguer
USE_COLORS = False

if USE_COLORS:
    try:
        from colorama import init, Fore, Back, Style
        init(autoreset=True)
        
        class Colors:
            PROMPT = Fore.GREEN + Style.BRIGHT
            SUCCESS = Fore.GREEN + Style.BRIGHT
            ERROR = Fore.RED + Style.BRIGHT
            WARNING = Fore.YELLOW + Style.BRIGHT
            INFO = Fore.CYAN + Style.BRIGHT
            SQL = Fore.YELLOW + Style.BRIGHT
            RESET = Style.RESET_ALL
    except:
        USE_COLORS = False

if not USE_COLORS:
    class Colors:
        PROMPT = SUCCESS = ERROR = WARNING = INFO = SQL = RESET = ''

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class GSQLCLI(cmd.Cmd):
    """GSQL CLI simplifié"""
    
    intro = f"""
{Colors.INFO}╔══════════════════════════════════════╗
║      GSQL Interactive Shell          ║
║      Version 2.0 - Stable           ║
╚══════════════════════════════════════╝{Colors.RESET}

Type {Colors.PROMPT}help{Colors.RESET} for commands, {Colors.PROMPT}exit{Colors.RESET} to quit.
Use {Colors.PROMPT}.tables{Colors.RESET} to list tables.
"""
    
    prompt = f'{Colors.PROMPT}gsql>{Colors.RESET} '
    
    def __init__(self, database_path=None):
        super().__init__()
        
        try:
            from gsql.database import Database
            self.db = Database(database_path or ':memory:')
            print(f"{Colors.SUCCESS}✅ Connected to database{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
            sys.exit(1)
    
    def default(self, line):
        """Handle all commands"""
        line = line.strip()
        if not line:
            return
        
        # Dot commands
        if line.startswith('.'):
            self._handle_dot_command(line)
            return
        
        # Regular SQL/NL
        try:
            if USE_COLORS and any(line.upper().startswith(x) for x in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP']):
                print(f"{Colors.INFO}📝 Executing SQL...{Colors.RESET}")
                print(f"{Colors.SQL}{line}{Colors.RESET}")
            
            result = self.db.execute(line)
            self._display_result(result)
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error: {str(e)}{Colors.RESET}")
    
    def _handle_dot_command(self, command):
        """Handle dot commands"""
        cmd = command[1:].lower()
        
        if cmd in ['tables', 'table']:
            try:
                result = self.db.execute("SHOW TABLES")
                self._display_result(result)
            except:
                print(f"{Colors.WARNING}⚠ SHOW TABLES not available{Colors.RESET}")
        elif cmd in ['help', '?']:
            self._show_help()
        elif cmd in ['exit', 'quit']:
            return self.do_exit('')
        elif cmd == 'clear':
            os.system('cls' if os.name == 'nt' else 'clear')
        elif cmd == 'version':
            print(f"{Colors.INFO}GSQL Version 2.0{Colors.RESET}")
        else:
            print(f"{Colors.ERROR}❌ Unknown command: {command}{Colors.RESET}")
    
    def _display_result(self, result):
        """Display results"""
        if result is None:
            print(f"{Colors.SUCCESS}✅ Success{Colors.RESET}")
            return
        
        if isinstance(result, str):
            print(f"{Colors.INFO}📋 {result}{Colors.RESET}")
            return
        
        if isinstance(result, dict):
            # SHOW TABLES result
            if result.get('type') == 'show_tables' and 'rows' in result:
                rows = result['rows']
                if rows:
                    print(f"{Colors.INFO}📊 Tables:{Colors.RESET}")
                    for row in rows:
                        if isinstance(row, dict):
                            print(f"  {row.get('table', 'unknown')}: {row.get('rows', 0)} rows")
                        else:
                            print(f"  {row}")
                else:
                    print(f"{Colors.WARNING}📭 No tables{Colors.RESET}")
                return
            
            # SELECT results
            if 'rows' in result and result['rows']:
                rows = result['rows']
                print(f"{Colors.INFO}📊 Results: {len(rows)} row(s){Colors.RESET}")
                
                if rows and isinstance(rows[0], dict):
                    # Show as table
                    headers = list(rows[0].keys())
                    print(" | ".join(headers))
                    print("-" * (len(" | ".join(headers))))
                    for row in rows[:20]:
                        values = [str(row.get(h, '')) for h in headers]
                        print(" | ".join(values))
                    
                    if len(rows) > 20:
                        print(f"... and {len(rows) - 20} more")
                else:
                    for i, row in enumerate(rows[:20], 1):
                        print(f"{i:3}. {row}")
                    if len(rows) > 20:
                        print(f"... and {len(rows) - 20} more")
                return
            
            # Simple message
            if 'message' in result:
                print(f"{Colors.INFO}📋 {result['message']}{Colors.RESET}")
                return
        
        print(f"{Colors.INFO}📋 Result: {result}{Colors.RESET}")
    
    def _show_help(self):
        """Show help"""
        help_text = f"""
{Colors.INFO}GSQL Commands:{Colors.RESET}

{Colors.PROMPT}SQL Commands:{Colors.RESET}
  SELECT * FROM table
  INSERT INTO table VALUES (...)
  CREATE TABLE name (columns)
  CREATE FUNCTION ...

{Colors.PROMPT}Dot Commands:{Colors.RESET}
  .tables    - List tables
  .help      - This help
  .exit      - Exit
  .clear     - Clear screen
  .version   - Show version

{Colors.PROMPT}Examples:{Colors.RESET}
  CREATE TABLE users (id INT, name TEXT)
  INSERT INTO users VALUES (1, 'Alice')
  SELECT * FROM users
  .tables
"""
        print(help_text)
    
    def do_exit(self, arg):
        """Exit"""
        print(f"{Colors.SUCCESS}👋 Goodbye!{Colors.RESET}")
        return True
    
    def do_quit(self, arg):
        """Exit"""
        return self.do_exit(arg)
    
    def do_EOF(self, arg):
        """Exit on Ctrl-D"""
        print()
        return self.do_exit(arg)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='GSQL Database')
    parser.add_argument('database', nargs='?', default=':memory:', help='Database file')
    parser.add_argument('--sql', help='Execute SQL command')
    parser.add_argument('--no-colors', action='store_true', help='Disable colors')
    
    args = parser.parse_args()
    
    global USE_COLORS
    if args.no_colors:
        USE_COLORS = False
    
    # Single command mode
    if args.sql:
        try:
            from gsql.database import Database
            db = Database(args.database)
            result = db.execute(args.sql)
            cli = GSQLCLI(args.database)
            cli._display_result(result)
            db.close()
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
    else:
        # Interactive mode
        try:
            cli = GSQLCLI(args.database)
            cli.cmdloop()
        except KeyboardInterrupt:
            print("\n\nInterrupted.")
            sys.exit(0)
        except Exception as e:
            print(f"Fatal error: {e}")
            sys.exit(1)

if __name__ == '__main__':
    main()
