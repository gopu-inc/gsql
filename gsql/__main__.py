# gsql/gsql/__main__.py
#!/usr/bin/env python3
"""
GSQL Command Line Interface
"""

import sys
import os
import cmd

class GSQLCLI(cmd.Cmd):
    """Interactive CLI for GSQL"""
    
    intro = """
    ┌──────────────────────────────────────┐
    │          GSQL Database v1.0          │
    │       100% Pure Python - No Deps     │
    └──────────────────────────────────────┘
    
    Type SQL commands or .help for help
    """
    
    prompt = "gsql> "
    
    def __init__(self, db_path=None):
        super().__init__()
        self.db_path = db_path
        self.db = None
        self._connect()
    
    def _connect(self):
        """Connect to database"""
        try:
            from .database import GSQL
            self.db = GSQL(self.db_path)
            print(f"✅ Connected to {self.db_path or 'gsql.db'}")
            
            if self.db.tables:
                print(f"📊 Loaded {len(self.db.tables)} tables")
            else:
                print("📭 Database is empty")
                
        except Exception as e:
            print(f"❌ Connection error: {e}")
            sys.exit(1)
    
    def default(self, line):
        """Handle SQL commands"""
        line = line.strip()
        
        if not line:
            return
        
        # Special commands
        if line.lower() in ['exit', 'quit', '.exit', '.quit']:
            return self.do_exit('')
        
        if line.lower() in ['help', '.help', '?']:
            return self.do_help('')
        
        if line.startswith('.'):
            self._handle_dot_command(line[1:])
            return
        
        # SQL command
        try:
            result = self.db.execute(line)
            self._print_result(result)
        except Exception as e:
            print(f"❌ Error: {e}")
    
    def _handle_dot_command(self, cmd):
        """Handle .commands"""
        parts = cmd.split()
        if not parts:
            return
        
        command = parts[0].lower()
        
        if command == 'tables':
            self._list_tables()
        elif command == 'describe':
            table = parts[1] if len(parts) > 1 else ''
            self._describe_table(table)
        elif command == 'clear':
            os.system('clear' if os.name != 'nt' else 'cls')
        else:
            print(f"❌ Unknown command: .{command}")
    
    def _list_tables(self):
        """List all tables"""
        if not self.db.tables:
            print("📭 No tables in database")
            return
        
        print("\n📋 Tables:")
        print("─" * 40)
        for name, meta in self.db.tables.items():
            cols = len(meta.get('columns', []))
            rows = meta.get('row_count', 0)
            print(f"  {name:20} {cols:3} cols, {rows:5} rows")
        print()
    
    def _describe_table(self, table_name):
        """Describe table structure"""
        if not table_name:
            print("Usage: .describe <table_name>")
            return
        
        if table_name not in self.db.tables:
            print(f"❌ Table '{table_name}' doesn't exist")
            return
        
        meta = self.db.tables[table_name]
        print(f"\n📊 Structure of '{table_name}':")
        print("═" * 50)
        
        for col in meta.get('columns', []):
            name = col.get('name', '?')
            type_ = col.get('type', '?')
            constr = ', '.join(col.get('constraints', []))
            
            if constr:
                print(f"  {name:20} {type_:15} [{constr}]")
            else:
                print(f"  {name:20} {type_:15}")
        print()
    
    def _print_result(self, result):
        """Print query result"""
        result_type = result.get('type')
        
        if result_type == 'SELECT':
            data = result.get('data', [])
            
            if not data:
                print("📭 0 rows")
                return
            
            # Get column names
            cols = list(data[0].keys()) if data else []
            
            # Calculate column widths
            widths = []
            for col in cols:
                width = len(str(col))
                for row in data:
                    width = max(width, len(str(row.get(col, ''))))
                widths.append(width + 2)
            
            # Print header
            separator = '+' + '+'.join(['─' * w for w in widths]) + '+'
            print(separator)
            
            header = '|'
            for i, col in enumerate(cols):
                header += f' {col:<{widths[i]-1}}|'
            print(header)
            
            print(separator)
            
            # Print rows
            for row in data:
                row_str = '|'
                for i, col in enumerate(cols):
                    val = str(row.get(col, ''))
                    row_str += f' {val:<{widths[i]-1}}|'
                print(row_str)
            
            print(separator)
            print(f"📊 {len(data)} row(s)")
            
        elif result_type == 'INSERT':
            rows = result.get('rows_affected', 0)
            print(f"✅ Inserted {rows} row(s)")
            
        elif result_type == 'CREATE_TABLE':
            table = result.get('table', '')
            print(f"✅ Created table '{table}'")
            
        elif result_type == 'DELETE':
            rows = result.get('rows_affected', 0)
            print(f"✅ Deleted {rows} row(s)")
            
        else:
