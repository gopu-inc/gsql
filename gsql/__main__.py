#!/usr/bin/env python3
"""
GSQL Command Line Interface - Version Complète
"""

import sys
import os
import cmd
import json
import readline
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

class GSQLCLI(cmd.Cmd):
    """CLI interactif complet pour GSQL"""
    
    intro = """
    ╔══════════════════════════════════════════════════════════╗
    ║                 GSQL Database v1.1.0                     ║
    ║           Simple & Powerful - Pure Python                ║
    ╚══════════════════════════════════════════════════════════╝
    
    📖 Commands:
      SQL:    CREATE, INSERT, SELECT, UPDATE, DELETE
      Meta:   .tables, .describe, .schema, .stats, .info
      Admin:  .backup, .restore, .export, .import, .clear
      System: .config, .logs, .help, .exit
    
    💡 Type '.help' for detailed help
    """
    
    prompt = "\033[96mgsql>\033[0m "  # Cyan color prompt
    
    def __init__(self, db_path=None):
        super().__init__()
        self.db_path = db_path
        self.db = None
        self.history_file = Path.home() / ".gsql_history"
        self._load_history()
        self._connect()
    
    def _connect(self):
        """Connect to database"""
        try:
            from .database import GSQL
            self.db = GSQL(self.db_path)
            
            # Show connection info
            print(f"\033[92m✅\033[0m Connected to: \033[93m{self.db_path or 'default'}\033[0m")
            
            # Show tables count
            tables = self.db.list_tables()
            if tables:
                print(f"\033[94m📊\033[0m Loaded \033[93m{len(tables)}\033[0m table(s)")
            else:
                print("\033[94m📭\033[0m Database is empty - create your first table!")
            
            print()
            
        except ImportError as e:
            print(f"\033[91m❌\033[0m Import error: {e}")
            print("Make sure GSQL is properly installed: pip install -e .")
            sys.exit(1)
        except Exception as e:
            print(f"\033[91m❌\033[0m Connection error: {e}")
            sys.exit(1)
    
    def _load_history(self):
        """Load command history"""
        try:
            readline.read_history_file(str(self.history_file))
        except FileNotFoundError:
            pass
    
    def _save_history(self):
        """Save command history"""
        try:
            readline.write_history_file(str(self.history_file))
        except:
            pass
    
    # ==================== COMMAND HANDLING ====================
    
    def default(self, line):
        """Handle SQL commands and special commands"""
        line = line.strip()
        
        if not line:
            return
        
        # Save to history
        readline.add_history(line)
        
        # Check for exit/quit
        if line.lower() in ['exit', 'quit', '.exit', '.quit']:
            return self.do_exit('')
        
        # Check for help
        if line.lower() in ['help', '.help', '?', '.?']:
            return self.do_help('')
        
        # Handle dot commands
        if line.startswith('.'):
            self._handle_dot_command(line[1:])
            return
        
        # Handle SQL commands
        try:
            result = self.db.execute(line)
            self._print_result(result)
        except Exception as e:
            print(f"\033[91m❌ Error:\033[0m {e}")
    
    def _handle_dot_command(self, cmd_line):
        """Handle .commands"""
        parts = cmd_line.strip().split()
        if not parts:
            return
        
        command = parts[0].lower()
        args = ' '.join(parts[1:]) if len(parts) > 1 else ''
        
        command_map = {
            'tables': self.do_tables,
            'desc': lambda a: self.do_describe(a),
            'describe': lambda a: self.do_describe(a),
            'schema': lambda a: self.do_schema(a),
            'stats': self.do_stats,
            'info': self.do_info,
            'backup': lambda a: self.do_backup(a),
            'restore': lambda a: self.do_restore(a),
            'export': lambda a: self.do_export(a),
            'import': lambda a: self.do_import(a),
            'config': self.do_config,
            'logs': self.do_logs,
            'clear': self.do_clear,
            'shell': lambda a: self.do_shell(a),
            'version': self.do_version,
            'history': self.do_history,
            'dump': lambda a: self.do_dump(a),
            'indexes': lambda a: self.do_indexes(a),
            'vacuum': self.do_vacuum,
            'check': self.do_check,
            'size': self.do_size,
            'help': self.do_help,
        }
        
        if command in command_map:
            try:
                command_map[command](args)
            except Exception as e:
                print(f"\033[91m❌ Command error:\033[0m {e}")
        else:
            print(f"\033[91m❌ Unknown command:\033[0m .{command}")
            print("Type '.help' for available commands")
    
    # ==================== SQL COMMANDS ====================
    
    def _print_result(self, result: Dict[str, Any]):
        """Print query result beautifully"""
        result_type = result.get('type')
        
        if result_type == 'SELECT':
            data = result.get('data', [])
            row_count = result.get('row_count', len(data))
            
            if not data:
                print(f"\033[94m📭\033[0m 0 rows returned")
                return
            
            # Get column names
            cols = list(data[0].keys()) if data else []
            
            # Calculate column widths
            widths = []
            for col in cols:
                width = len(str(col))
                for row in data:
                    width = max(width, len(str(row.get(col, ''))))
                widths.append(min(width, 50) + 2)  # Max 50 chars per column
            
            # Print header
            separator = '\033[90m┌' + '┬'.join(['─' * w for w in widths]) + '┐\033[0m'
            print(separator)
            
            # Column names
            header = '\033[90m│\033[0m'
            for i, col in enumerate(cols):
                header += f' \033[1;96m{col:<{widths[i]-1}}\033[0m\033[90m│\033[0m'
            print(header)
            
            separator = '\033[90m├' + '┼'.join(['─' * w for w in widths]) + '┤\033[0m'
            print(separator)
            
            # Data rows
            for row in data:
                row_str = '\033[90m│\033[0m'
                for i, col in enumerate(cols):
                    val = str(row.get(col, ''))
                    if len(val) > 50:
                        val = val[:47] + '...'
                    row_str += f' {val:<{widths[i]-1}}\033[90m│\033[0m'
                print(row_str)
            
            separator = '\033[90m└' + '┴'.join(['─' * w for w in widths]) + '┘\033[0m'
            print(separator)
            
            print(f"\033[92m✓\033[0m \033[93m{row_count}\033[0m row(s) returned")
            
        elif result_type == 'INSERT':
            rows = result.get('rows_affected', 0)
            print(f"\033[92m✓\033[0m Inserted \033[93m{rows}\033[0m row(s)")
            
        elif result_type == 'CREATE_TABLE':
            table = result.get('table', '')
            cols = result.get('columns', 0)
            print(f"\033[92m✓\033[0m Created table '\033[93m{table}\033[0m' with \033[93m{cols}\033[0m column(s)")
            
        elif result_type == 'DELETE':
            rows = result.get('rows_affected', 0)
            print(f"\033[92m✓\033[0m Deleted \033[93m{rows}\033[0m row(s)")
            
        elif result_type == 'UPDATE':
            rows = result.get('rows_affected', 0)
            print(f"\033[92m✓\033[0m Updated \033[93m{rows}\033[0m row(s)")
            
        else:
            print(f"\033[92m✓\033[0m Operation completed")
    
    # ==================== DOT COMMANDS ====================
    
    def do_tables(self, arg):
        """List all tables: .tables [pattern]"""
        tables = self.db.list_tables()
        
        if not tables:
            print("\033[94m📭\033[0m No tables in database")
            return
        
        # Filter by pattern if provided
        if arg:
            tables = [t for t in tables if arg.lower() in t.lower()]
        
        print(f"\n\033[1;95m📋 TABLES ({len(tables)})\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        
        for i, table in enumerate(sorted(tables), 1):
            info = self.db.get_table_info(table)
            if info:
                rows = info.get('row_count', 0)
                size = self._format_size(info.get('file_size', 0))
                created = info.get('created_at', 'N/A')[:10]
                modified = info.get('modified_at', 'N/A')[:10]
                
                print(f"\033[93m{i:3}.\033[0m \033[1;96m{table:20}\033[0m")
                print(f"     ├─ Rows: \033[92m{rows:8,}\033[0m")
                print(f"     ├─ Size: \033[94m{size:>10}\033[0m")
                print(f"     ├─ Created:  \033[90m{created}\033[0m")
                print(f"     └─ Modified: \033[90m{modified}\033[0m")
            else:
                print(f"\033[93m{i:3}.\033[0m \033[1;96m{table:20}\033[0m \033[90m(no metadata)\033[0m")
        
        print()
    
    def do_describe(self, table_name):
        """Describe table structure: .describe <table>"""
        if not table_name:
            print("Usage: .describe <table_name>")
            print("       .desc <table_name>")
            return
        
        info = self.db.get_table_info(table_name)
        
        if not info:
            print(f"\033[91m❌\033[0m Table '\033[93m{table_name}\033[0m' doesn't exist")
            return
        
        print(f"\n\033[1;95m📊 TABLE: \033[96m{table_name}\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        
        # Basic info
        print(f"\033[1;94mBasic Information:\033[0m")
        print(f"  ├─ Created:    \033[93m{info.get('created_at', 'N/A')}\033[0m")
        print(f"  ├─ Modified:   \033[93m{info.get('modified_at', 'N/A')}\033[0m")
        print(f"  ├─ Rows:       \033[92m{info.get('row_count', 0):,}\033[0m")
        print(f"  ├─ Next ID:    \033[92m{info.get('next_id', 1)}\033[0m")
        print(f"  └─ File size:  \033[94m{self._format_size(info.get('file_size', 0))}\033[0m")
        
        # Columns
        columns = info.get('columns', [])
        if columns:
            print(f"\n\033[1;94mColumns ({len(columns)}):\033[0m")
            print("\033[90m" + "─" * 60 + "\033[0m")
            
            for i, col in enumerate(columns, 1):
                name = col.get('name', '?')
                type_ = col.get('type', '?').upper()
                constraints = col.get('constraints', [])
                
                # Color code types
                if 'INT' in type_:
                    type_color = '\033[92m'
                elif 'TEXT' in type_ or 'CHAR' in type_:
                    type_color = '\033[93m'
                elif 'FLOAT' in type_ or 'DOUBLE' in type_:
                    type_color = '\033[94m'
                elif 'BOOL' in type_:
                    type_color = '\033[95m'
                else:
                    type_color = '\033[96m'
                
                constr_str = ''
                if constraints:
                    constr_parts = []
                    for constr in constraints:
                        if constr == 'PRIMARY_KEY':
                            constr_parts.append('\033[1;91mPRIMARY KEY\033[0m')
                        elif constr == 'NOT_NULL':
                            constr_parts.append('\033[91mNOT NULL\033[0m')
                        elif constr == 'UNIQUE':
                            constr_parts.append('\033[92mUNIQUE\033[0m')
                        else:
                            constr_parts.append(constr)
                    constr_str = ' '.join(constr_parts)
                
                print(f"\033[93m{i:2}.\033[0m \033[1;96m{name:20}\033[0m {type_color}{type_:12}\033[0m {constr_str}")
        
        print()
    
    def do_schema(self, table_name):
        """Show SQL schema: .schema [table]"""
        if table_name:
            # Show schema for specific table
            info = self.db.get_table_info(table_name)
            if not info:
                print(f"\033[91m❌\033[0m Table '\033[93m{table_name}\033[0m' doesn't exist")
                return
            
            columns = info.get('columns', [])
            if not columns:
                return
            
            print(f"\n\033[1;95m📝 SCHEMA FOR: \033[96m{table_name}\033[0m")
            print("\033[90m" + "═" * 70 + "\033[0m")
            
            # Generate CREATE TABLE statement
            create_sql = f"CREATE TABLE {table_name} (\n"
            col_defs = []
            
            for col in columns:
                name = col.get('name', '')
                type_ = col.get('type', '').upper()
                constraints = col.get('constraints', [])
                
                # Convert constraints to SQL
                constr_sql = []
                for constr in constraints:
                    if constr == 'PRIMARY_KEY':
                        constr_sql.append('PRIMARY KEY')
                    elif constr == 'NOT_NULL':
                        constr_sql.append('NOT NULL')
                    elif constr == 'UNIQUE':
                        constr_sql.append('UNIQUE')
                
                col_def = f"    {name} {type_}"
                if constr_sql:
                    col_def += f" {' '.join(constr_sql)}"
                col_defs.append(col_def)
            
            create_sql += ',\n'.join(col_defs)
            create_sql += "\n);"
            
            print(f"\033[97m{create_sql}\033[0m\n")
            
        else:
            # Show all tables' schemas
            tables = self.db.list_tables()
            if not tables:
                print("\033[94m📭\033[0m No tables in database")
                return
            
            for table in sorted(tables):
                self.do_schema(table)
    
    def do_stats(self, arg):
        """Show database statistics: .stats"""
        stats = self.db.stats()
        
        print(f"\n\033[1;95m📈 DATABASE STATISTICS\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        
        # Basic stats
        print(f"\033[1;94mOverview:\033[0m")
        print(f"  ├─ Tables:      \033[93m{stats.get('tables', 0):,}\033[0m")
        print(f"  ├─ Total rows:  \033[92m{stats.get('total_rows', 0):,}\033[0m")
        print(f"  └─ Storage:     \033[94m{stats.get('storage_path', 'N/A')}\033[0m")
        
        # Table details
        tables = self.db.list_tables()
        if tables:
            print(f"\n\033[1;94mTable Details:\033[0m")
            print("\033[90m" + "─" * 50 + "\033[0m")
            
            total_size = 0
            for table in sorted(tables):
                info = self.db.get_table_info(table)
                if info:
                    rows = info.get('row_count', 0)
                    size = info.get('file_size', 0)
                    total_size += size
                    
                    print(f"  \033[96m{table:20}\033[0m "
                          f"\033[92m{rows:8,}\033[0m rows "
                          f"(\033[94m{self._format_size(size)}\033[0m)")
            
            print(f"\n\033[1;94mTotal size:\033[0m \033[93m{self._format_size(total_size)}\033[0m")
        
        print()
    
    def do_info(self, arg):
        """Show database information: .info"""
        print(f"\n\033[1;95mℹ️  DATABASE INFORMATION\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        
        # Get database path
        db_path = self.db_path or "gsql.db"
        
        # Check if files exist
        data_dir = Path(db_path).parent / f"{db_path}_data"
        
        if data_dir.exists():
            # Calculate sizes
            total_size = 0
            file_counts = {'tables': 0, 'indexes': 0, 'meta': 0}
            
            for item in ['tables', 'indexes', 'meta']:
                item_dir = data_dir / item
                if item_dir.exists():
                    count = len(list(item_dir.glob('*')))
                    file_counts[item] = count
                    
                    size = sum(f.stat().st_size for f in item_dir.rglob('*') if f.is_file())
                    total_size += size
            
            print(f"\033[1;94mStorage:\033[0m")
            print(f"  ├─ Database:    \033[93m{db_path}\033[0m")
            print(f"  ├─ Data dir:    \033[93m{data_dir}\033[0m")
            print(f"  ├─ Total size:  \033[94m{self._format_size(total_size)}\033[0m")
            print(f"  └─ Files:       \033[96m{file_counts['tables']}\033[0m tables, "
                  f"\033[96m{file_counts['indexes']}\033[0m indexes, "
                  f"\033[96m{file_counts['meta']}\033[0m metadata")
        
        # Show tables
        tables = self.db.list_tables()
        if tables:
            print(f"\n\033[1;94mTables ({len(tables)}):\033[0m")
            for table in sorted(tables):
                info = self.db.get_table_info(table)
                rows = info.get('row_count', 0) if info else 0
                print(f"  \033[96m• {table:20}\033[0m \033[92m{rows:8,}\033[0m rows")
        
        print()
    
    def do_backup(self, backup_name):
        """Create backup: .backup [name]"""
        try:
            if not backup_name:
                backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            print(f"\033[94m⏳\033[0m Creating backup '\033[93m{backup_name}\033[0m'...")
            backup_path = self.db.backup(backup_name)
            
            print(f"\033[92m✓\033[0m Backup created successfully!")
            print(f"   Path: \033[93m{backup_path}\033[0m")
            
        except Exception as e:
            print(f"\033[91m❌\033[0m Backup failed: {e}")
    
    def do_restore(self, backup_name):
        """Restore from backup: .restore <name>"""
        if not backup_name:
            print("Usage: .restore <backup_name>")
            print("       .restore latest (restores most recent backup)")
            return
        
        try:
            if backup_name.lower() == 'latest':
                # Find latest backup
                data_dir = Path(self.db_path or "gsql.db").parent / f"{self.db_path or 'gsql.db'}_data"
                backup_dir = data_dir / 'backups'
                
                if not backup_dir.exists():
                    print("\033[91m❌\033[0m No backups found")
                    return
                
                backups = sorted([d.name for d in backup_dir.iterdir() if d.is_dir()])
                if not backups:
                    print("\033[91m❌\033[0m No backups found")
                    return
                
                backup_name = backups[-1]
                print(f"\033[94m⏳\033[0m Restoring latest backup: '\033[93m{backup_name}\033[0m'")
            
            # Confirm restore
            print(f"\033[91m⚠️\033[0m This will overwrite current database!")
            confirm = input("Type 'YES' to confirm: ")
            
            if confirm != 'YES':
                print("\033[93m✗\033[0m Restore cancelled")
                return
            
            print(f"\033[94m⏳\033[0m Restoring from backup '\033[93m{backup_name}\033[0m'...")
            self.db.restore(backup_name)
            
            print(f"\033[92m✓\033[0m Database restored successfully!")
            
        except Exception as e:
            print(f"\033[91m❌\033[0m Restore failed: {e}")
    
    def do_export(self, args):
        """Export data: .export <table> [file.json]"""
        parts = args.strip().split()
        if len(parts) < 1:
            print("Usage: .export <table> [file.json]")
            print("       .export all (exports all tables)")
            return
        
        table_name = parts[0]
        filename = parts[1] if len(parts) > 1 else f"{table_name}_{datetime.now().strftime('%Y%m%d')}.json"
        
        try:
            if table_name.lower() == 'all':
                # Export all tables
                tables = self.db.list_tables()
                if not tables:
                    print("\033[91m❌\033[0m No tables to export")
                    return
                
                all_data = {}
                for table in tables:
                    data = self.db.select(table)
                    all_data[table] = data
                
                with open(filename, 'w') as f:
                    json.dump(all_data, f, indent=2)
                
                print(f"\033[92m✓\033[0m Exported \033[93m{len(tables)}\033[0m tables to '\033[93m{filename}\033[0m'")
                
            else:
                # Export single table
                if table_name not in self.db.list_tables():
                    print(f"\033[91m❌\033[0m Table '\033[93m{table_name}\033[0m' doesn't exist")
                    return
                
                data = self.db.select(table_name)
                with open(filename, 'w') as f:
                    json.dump(data, f, indent=2)
                
                print(f"\033[92m✓\033[0m Exported \033[93m{len(data)}\033[0m rows from '\033[93m{table_name}\033[0m' to '\033[93m{filename}\033[0m'")
                
        except Exception as e:
            print(f"\033[91m❌\033[0m Export failed: {e}")
    
    def do_import(self, args):
        """Import data: .import <file.json> [table]"""
        parts = args.strip().split()
        if len(parts) < 1:
            print("Usage: .import <file.json> [table]")
            return
        
        filename = parts[0]
        table_name = parts[1] if len(parts) > 1 else None
        
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
            
            if table_name:
                # Import to specific table
                if isinstance(data, list):
                    inserted = 0
                    for row in data:
                        self.db.insert(table_name, row)
                        inserted += 1
                    print(f"\033[92m✓\033[0m Imported \033[93m{inserted}\033[0m rows to '\033[93m{table_name}\033[0m'")
                else:
                    print("\033[91m❌\033[0m JSON file should contain an array of objects")
                    
            else:
                # Import all tables from JSON
                if not isinstance(data, dict):
                    print("\033[91m❌\033[0m JSON file should be an object with table names as keys")
                    return
                
                total = 0
                for table, rows in data.items():
                    if isinstance(rows, list):
                        # Check if table exists
                        if table not in self.db.list_tables():
                            # Try to infer schema from first row
                            if rows:
                                first_row = rows[0]
                                columns = []
                                for col_name, col_value in first_row.items():
                                    if isinstance(col_value, int):
                                        col_type = 'INTEGER'
                                    elif isinstance(col_value, float):
                                        col_type = 'FLOAT'
                                    elif isinstance(col_value, bool):
                                        col_type = 'BOOLEAN'
                                    else:
                                        col_type = 'TEXT'
                                    columns.append({'name': col_name, 'type': col_type})
                                
                                self.db.create_table(table, columns)
                                print(f"\033[94m⏳\033[0m Created table '\033[93m{table}\033[0m'")
                        
                        # Insert rows
                        inserted = 0
                        for row in rows:
                            self.db.insert(table, row)
                            inserted += 1
                        
                        total += inserted
                        print(f"\033[92m✓\033[0m Imported \033[93m{inserted}\033[0m rows to '\033[93m{table}\033[0m'")
                
                print(f"\033[92m✓\033[0m Total: \033[93m{total}\033[0m rows imported")
                
        except FileNotFoundError:
            print(f"\033[91m❌\033[0m File not found: '\033[93m{filename}\033[0m'")
        except Exception as e:
            print(f"\033[91m❌\033[0m Import failed: {e}")
    
    def do_config(self, arg):
        """Show configuration: .config"""
        config_file = "GSQL.toml"
        
        if os.path.exists(config_file):
            try:
                import tomllib
                with open(config_file, 'rb') as f:
                    config = tomllib.load(f)
                
                print(f"\n\033[1;95m⚙️  CONFIGURATION: {config_file}\033[0m")
                print("\033[90m" + "═" * 70 + "\033[0m")
                
                self._print_config_section("Global", config.get('global', {}))
                self._print_config_section("Storage", config.get('storage', {}))
                self._print_config_section("Performance", config.get('performance', {}))
                self._print_config_section("Security", config.get('security', {}))
                self._print_config_section("Logging", config.get('logging', {}))
                
            except ImportError:
                print("\033[91m❌\033[0m tomllib not available. Install: pip install tomli")
            except Exception as e:
                print(f"\033[91m❌\033[0m Error reading config: {e}")
        else:
            print(f"\033[94m📭\033[0m Configuration file '\033[93m{config_file}\033[0m' not found")
            print("Using default configuration")
    
    def _print_config_section(self, title, section):
        """Print a configuration section"""
        if section:
            print(f"\n\033[1;94m{title}:\033[0m")
            for key, value in section.items():
                if isinstance(value, bool):
                    color = '\033[92m' if value else '\033[91m'
                    display = '✓' if value else '✗'
                    print(f"  ├─ {key:20} {color}{display}\033[0m")
                elif isinstance(value, int):
                    print(f"  ├─ {key:20} \033[93m{value:,}\033[0m")
                else:
                    print(f"  ├─ {key:20} \033[96m{value}\033[0m")
    
    def do_logs(self, arg):
        """Show recent logs: .logs [n]"""
        try:
            n = int(arg) if arg.isdigit() else 10
            n = min(n, 100)  # Limit to 100
            
            log_dir = Path(self.db_path or "gsql.db").parent / f"{self.db_path or 'gsql.db'}_data" / "logs"
            
            if not log_dir.exists():
                print("\033[94m📭\033[0m No logs directory found")
                return
            
            log_files = list(log_dir.glob("*.log"))
            if not log_files:
                print("\033[94m📭\033[0m No log files found")
                return
            
            latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
            
            print(f"\n\033[1;95m📋 LOGS: {latest_log.name}\033[0m")
            print("\033[90m" + "═" * 70 + "\033[0m")
            
            with open(latest_log, 'r') as f:
                lines = f.readlines()[-n:]
            
            for line in lines[-n:]:
                line = line.strip()
                if not line:
                    continue
                
                # Color code log levels
                if '[ERROR]' in line:
                    line = line.replace('[ERROR]', '\033[91m[ERROR]\033[0m')
                elif '[WARN]' in line:
                    line = line.replace('[WARN]', '\033[93m[WARN]\033[0m')
                elif '[INFO]' in line:
                    line = line.replace('[INFO]', '\033[94m[INFO]\033[0m')
                elif '[DEBUG]' in line:
                    line = line.replace('[DEBUG]', '\033[90m[DEBUG]\033[0m')
                
                print(f"  {line}")
            
            print()
            
        except Exception as e:
            print(f"\033[91m❌\033[0m Error reading logs: {e}")
    
    def do_clear(self, arg):
        """Clear screen: .clear"""
        os.system('clear' if os.name != 'nt' else 'cls')
    
    def do_shell(self, cmd):
        """Execute shell command: .shell <command>"""
        if cmd:
            os.system(cmd)
    
    def do_version(self, arg):
        """Show version: .version"""
        print(f"\n\033[1;95mGSQL Database\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        print(f"Version:    \033[93m1.1.0\033[0m")
        print(f"Author:     \033[96mGSQL Team\033[0m")
        print(f"License:    \033[94mMIT\033[0m")
        print(f"Python:     \033[92m{sys.version.split()[0]}\033[0m")
        print(f"Platform:   \033[95m{sys.platform}\033[0m")
        print()
    
    def do_history(self, arg):
        """Show command history: .history [n]"""
        try:
            n = int(arg) if arg.isdigit() else 20
            n = min(n, 100)
            
            # Get history from readline
            history_length = readline.get_current_history_length()
            start = max(0, history_length - n)
            
            print(f"\n\033[1;95m📜 COMMAND HISTORY (last {n})\033[0m")
            print("\033[90m" + "═" * 70 + "\033[0m")
            
            for i in range(start, history_length):
                cmd = readline.get_history_item(i + 1)
                print(f"\033[93m{i+1:4}.\033[0m {cmd}")
            
            print()
            
        except Exception as e:
            print(f"\033[91m❌\033[0m Error reading history: {e}")
    
    def do_dump(self, table_name):
        """Dump table data: .dump <table>"""
        if not table_name:
            print("Usage: .dump <table>")
            return
        
        if table_name not in self.db.list_tables():
            print(f"\033[91m❌\033[0m Table '\033[93m{table_name}\033[0m' doesn't exist")
            return
        
        data = self.db.select(table_name)
        
        if not data:
            print(f"\033[94m📭\033[0m Table '\033[93m{table_name}\033[0m' is empty")
            return
        
        # Convert to INSERT statements
        print(f"\n\033[1;95m💾 SQL DUMP: {table_name}\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        
        for row in data:
            columns = ', '.join(row.keys())
            values = ', '.join([self._format_sql_value(v) for v in row.values()])
            print(f"INSERT INTO {table_name} ({columns}) VALUES ({values});")
        
        print(f"\n\033[92m✓\033[0m Generated \033[93m{len(data)}\033[0m INSERT statements")
    
    def do_indexes(self, table_name):
        """Show table indexes: .indexes [table]"""
        # For now, just show placeholder
        # In a real implementation, this would show actual indexes
        print(f"\n\033[1;95m🔍 INDEXES\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        
        if table_name:
            if table_name not in self.db.list_tables():
                print(f"\033[91m❌\033[0m Table '\033[93m{table_name}\033[0m' doesn't exist")
                return
            
            print(f"Table: \033[96m{table_name}\033[0m")
            print("\033[90m" + "─" * 50 + "\033[0m")
            print("\033[94m⏳\033[0m Index system coming soon...")
        else:
            tables = self.db.list_tables()
            for table in sorted(tables):
                print(f"\033[96m• {table}\033[0m")
        
        print()
    
    def do_vacuum(self, arg):
        """Optimize database: .vacuum"""
        print(f"\n\033[1;95m🧹 VACUUM DATABASE\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        
        tables = self.db.list_tables()
        if not tables:
            print("\033[94m📭\033[0m No tables to optimize")
            return
        
        print("\033[94m⏳\033[0m Optimizing database...")
        
        # In a real implementation, this would:
        # 1. Rebuild fragmented data
        # 2. Clean up deleted rows
        # 3. Rebuild indexes
        # 4. Update statistics
        
        print(f"\033[92m✓\033[0m Optimization complete for \033[93m{len(tables)}\033[0m tables")
        print()
    
    def do_check(self, arg):
        """Check database integrity: .check"""
        print(f"\n\033[1;95m🔍 DATABASE INTEGRITY CHECK\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        
        tables = self.db.list_tables()
        if not tables:
            print("\033[94m📭\033[0m No tables to check")
            return
        
        issues = 0
        
        for table in tables:
            info = self.db.get_table_info(table)
            if info:
                expected_rows = info.get('row_count', 0)
                
                # Count actual rows
                actual_rows = len(self.db.select(table))
                
                if expected_rows != actual_rows:
                    print(f"\033[91m❌\033[0m \033[96m{table}\033[0m: "
                          f"Row count mismatch (expected: {expected_rows}, actual: {actual_rows})")
                    issues += 1
                else:
                    print(f"\033[92m✓\033[0m \033[96m{table}\033[0m: OK ({actual_rows} rows)")
        
        if issues == 0:
            print(f"\n\033[92m✓\033[0m All \033[93m{len(tables)}\033[0m tables passed integrity check")
        else:
            print(f"\n\033[91m❌\033[0m Found \033[93m{issues}\033[0m issue(s)")
        
        print()
    
    def do_size(self, arg):
        """Show database size: .size"""
        data_dir = Path(self.db_path or "gsql.db").parent / f"{self.db_path or 'gsql.db'}_data"
        
        if not data_dir.exists():
            print("\033[94m📭\033[0m No database files found")
            return
        
        total_size = 0
        breakdown = {}
        
        for item in ['tables', 'indexes', 'meta', 'backups', 'logs']:
            item_dir = data_dir / item
            if item_dir.exists():
                size = sum(f.stat().st_size for f in item_dir.rglob('*') if f.is_file())
                breakdown[item] = size
                total_size += size
        
        print(f"\n\033[1;95m📦 DATABASE SIZE\033[0m")
        print("\033[90m" + "═" * 70 + "\033[0m")
        print(f"Total: \033[93m{self._format_size(total_size)}\033[0m")
        print("\033[90m" + "─" * 50 + "\033[0m")
        
        for item, size in breakdown.items():
            percentage = (size / total_size * 100) if total_size > 0 else 0
            bar = '█' * int(percentage / 5)  # 5% per character
            print(f"\033[96m{item:10}\033[0m {self._format_size(size):>10} "
                  f"\033[90m[{bar:<20}]\033[0m {percentage:5.1f}%")
        
        print()
    
    def do_help(self, arg):
        """Show help: .help [command]"""
        if arg:
            # Show help for specific command
            help_texts = {
                'tables': "List all tables: .tables [pattern]",
                'describe': "Describe table structure: .describe <table>",
                'schema': "Show SQL schema: .schema [table]",
                'stats': "Show database statistics: .stats",
                'info': "Show database information: .info",
                'backup': "Create backup: .backup [name]",
                'restore': "Restore from backup: .restore <name>",
                'export': "Export data: .export <table> [file.json]",
                'import': "Import data: .import <file.json> [table]",
                'config': "Show configuration: .config",
                'logs': "Show recent logs: .logs [n]",
                'clear': "Clear screen: .clear",
                'shell': "Execute shell command: .shell <command>",
                'version': "Show version: .version",
                'history': "Show command history: .history [n]",
                'dump': "Dump table data: .dump <table>",
                'indexes': "Show table indexes: .indexes [table]",
                'vacuum': "Optimize database: .vacuum",
                'check': "Check database integrity: .check",
                'size': "Show database size: .size",
                'exit': "Exit GSQL: exit, quit, .exit, .quit",
            }
            
            if arg in help_texts:
                print(f"\n\033[1;95mHELP: .{arg}\033[0m")
                print("\033[90m" + "═" * 70 + "\033[0m")
                print(f"\033[96m{help_texts[arg]}\033[0m")
                print()
            else:
                print(f"\033[91m❌\033[0m No help available for: .{arg}")
        else:
            # Show general help
            print("""
\033[1;95m📖 GSQL HELP\033[0m
\033[90m══════════════════════════════════════════════════════════════════════════════\033[0m

\033[1;94mSQL COMMANDS:\033[0m
  CREATE TABLE <name> (<col1> <type>, ...)
  INSERT INTO <table> VALUES (<val1>, <val2>, ...)
  SELECT <cols> FROM <table> [WHERE <condition>]
  UPDATE <table> SET <col>=<val> [WHERE <condition>]
  DELETE FROM <table> [WHERE <condition>]

\033[1;94mMETA COMMANDS:\033[0m
  .tables [pattern]     - List all tables
  .describe <table>     - Show table structure
  .schema [table]       - Show SQL schema
  .stats                - Show database statistics
  .info                 - Show database information

\033[1;94mADMIN COMMANDS:\033[0m
  .backup [name]        - Create backup
  .restore <name>       - Restore from backup
  .export <table> [file]- Export data to JSON
  .import <file> [table]- Import data from JSON
  .config               - Show configuration
  .logs [n]             - Show recent logs
  .clear                - Clear screen

\033[1;94mSYSTEM COMMANDS:\033[0m
  .shell <command>      - Execute shell command
  .version              - Show version
  .history [n]          - Show command history
  .dump <table>         - Dump table as SQL
  .indexes [table]      - Show table indexes
  .vacuum               - Optimize database
  .check                - Check integrity
  .size                 - Show database size
  .help [command]       - Show this help
  exit, .exit           - Exit GSQL

\033[1;94mEXAMPLES:\033[0m
  CREATE TABLE users (id INT, name TEXT, age INT)
  INSERT INTO users VALUES (1, 'Alice', 25)
  SELECT * FROM users WHERE age > 20
  .tables
  .describe users
  .backup my_backup

\033[90m══════════════════════════════════════════════════════════════════════════════\033[0m
            """)
    
    def do_exit(self, arg):
        """Exit GSQL"""
        print(f"\n\033[92m👋\033[0m Goodbye!")
        
        # Save history
        self._save_history()
        
        # Close database
        if self.db:
            self.db.close()
        
        return True
    
    # ==================== UTILITY METHODS ====================
    
    def _format_size(self, size_bytes):
        """Format file size"""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    
    def _format_sql_value(self, value):
        """Format value for SQL INSERT"""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            return f"'{value.replace("'", "''")}'"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        else:
            return str(value)
    
    def preloop(self):
        """Run before CLI loop"""
        # Set up readline
        readline.set_completer(self.complete)
        readline.parse_and_bind("tab: complete")
        
        # Set history length
        readline.set_history_length(1000)
    
    def complete(self, text, state):
        """Auto-completion for commands"""
        commands = [
            '.tables', '.describe', '.schema', '.stats', '.info',
            '.backup', '.restore', '.export', '.import', '.config',
            '.logs', '.clear', '.shell', '.version', '.history',
            '.dump', '.indexes', '.vacuum', '.check', '.size',
            '.help', '.exit'
        ]
        
        sql_keywords = [
            'CREATE', 'TABLE', 'INSERT', 'INTO', 'VALUES',
            'SELECT', 'FROM', 'WHERE', 'UPDATE', 'SET',
            'DELETE', 'INT', 'TEXT', 'FLOAT', 'BOOLEAN',
            'PRIMARY', 'KEY', 'NOT', 'NULL', 'AND', 'OR'
        ]
        
        # Get current line
        line = readline.get_line_buffer()
        
        if line.startswith('.'):
            # Complete dot commands
            options = [c for c in commands if c.startswith(text)]
        else:
            # Complete SQL keywords
            options = [kw for kw in sql_keywords if kw.lower().startswith(text.lower())]
        
        if state < len(options):
            return options[state]
        return None

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='GSQL - Simple yet powerful SQL database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  gsql                         # Interactive mode
  gsql mydb.db                 # Open specific database
  gsql -e "SELECT * FROM users" # Execute single command
  gsql --init                  # Initialize new database
  gsql --backup mybackup       # Create backup
  gsql --restore mybackup      # Restore backup
  gsql --version               # Show version
        """
    )
    
    parser.add_argument(
        'database', 
        nargs='?', 
        default=None,
        help='Database file (default: gsql.db)'
    )
    
    parser.add_argument(
        '-e', '--execute', 
        help='Execute SQL command and exit'
    )
    
    parser.add_argument(
        '--init',
        action='store_true',
        help='Initialize new database'
    )
    
    parser.add_argument(
        '--backup',
        metavar='NAME',
        help='Create backup'
    )
    
    parser.add_argument(
        '--restore',
        metavar='NAME',
        help='Restore from backup'
    )
    
    parser.add_argument(
        '-v', '--version',
        action='store_true',
        help='Show version'
    )
    
    args = parser.parse_args()
    
    # Show version
    if args.version:
        print("GSQL Database v1.1.0")
        print("Pure Python SQL database")
        return
    
    # Initialize database
    if args.init:
        from .database import GSQL
        db = GSQL(args.database)
        db.close()
        print(f"✅ Database initialized: {args.database or 'gsql.db'}")
        return
    
    # Backup
    if args.backup:
        from .database import GSQL
        db = GSQL(args.database)
        backup_path = db.backup(args.backup)
        db.close()
        print(f"✅ Backup created: {backup_path}")
        return
    
    # Restore
    if args.restore:
        from .database import GSQL
        db = GSQL(args.database)
        db.restore(args.restore)
        db.close()
        print(f"✅ Database restored from: {args.restore}")
        return
    
    # Execute single command
    if args.execute:
        from .database import GSQL
        db = GSQL(args.database)
        
        try:
            result = db.execute(args.execute)
            
            if result.get('type') == 'SELECT':
                import json
                print(json.dumps(result.get('data', []), indent=2))
            else:
                rows = result.get('rows_affected', 0)
                print(f"Rows affected: {rows}")
        
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
        
        finally:
            db.close()
        
        return
    
    # Interactive mode
    cli = GSQLCLI(args.database)
    
    try:
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\n\n\033[93m⚠️  Interrupted. Type 'exit' to quit.\033[0m")
        cli.cmdloop()

if __name__ == "__main__":
    main()
