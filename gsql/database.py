# gsql/gsql/database.py
"""
Main GSQL database class with persistent storage
"""

import json
import os
import shutil
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

from .exceptions import GSQLSyntaxError, GSQLExecutionError, GSQLTableError

class GSQL:
    """GSQL Database - Simple yet powerful SQL database"""
    
    def __init__(self, path: Optional[str] = None, config_file: Optional[str] = None):
        """
        Initialize GSQL database
        
        Args:
            path: Path to database file (creates if doesn't exist)
            config_file: Path to configuration file
        """
        self.path = path or "gsql.db"
        self.config_file = config_file or "GSQL.toml"
        
        # Initialize storage directory structure
        self._init_storage()
        
        # Dynamic imports to avoid circular dependencies
        try:
            from .parser import SQLParser
            from .storage import PersistentStorage
            from .executor import QueryExecutor
        except ImportError as e:
            raise ImportError(f"Failed to import GSQL modules: {e}. Make sure all modules are in place.")
        
        # Initialize components
        self.parser = SQLParser()
        self.storage = PersistentStorage(self.path, self.config_file)
        self.executor = QueryExecutor(self.storage)
        
        # Load metadata
        self.tables: Dict[str, Dict] = {}
        self._load_metadata()
        
        # Statistics
        self.stats = {
            'queries_executed': 0,
            'tables_created': 0,
            'rows_inserted': 0,
            'start_time': datetime.now()
        }
    
    def _init_storage(self):
        """Initialize storage directory structure"""
        # Create main data directory
        data_dir = self._get_data_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        subdirs = ['tables', 'indexes', 'meta', 'backups', 'logs', 'temp']
        for subdir in subdirs:
            (data_dir / subdir).mkdir(exist_ok=True)
        
        # Create default config if doesn't exist
        if not os.path.exists(self.config_file):
            self._create_default_config()
    
    def _create_default_config(self):
        """Create default configuration file"""
        try:
            import tomli_w
            
            config = {
                'database': {
                    'name': 'GSQL Database',
                    'version': '1.0',
                    'description': 'Simple yet powerful SQL database',
                    'path': str(Path(self.path).absolute())
                },
                'storage': {
                    'engine': 'json',
                    'compression': False,
                    'auto_backup': True,
                    'backup_interval': 3600,
                    'max_file_size': 104857600  # 100MB
                },
                'performance': {
                    'cache_size': 1000,
                    'query_cache': True,
                    'auto_index': True
                },
                'logging': {
                    'enabled': True,
                    'level': 'INFO',
                    'max_size': 10485760  # 10MB
                },
                'security': {
                    'encryption': False,
                    'checksum_verification': True
                }
            }
            
            with open(self.config_file, 'wb') as f:
                tomli_w.dump(config, f)
                
        except ImportError:
            # If tomli-w is not available, create a simple JSON config
            config = {
                'database': {
                    'name': 'GSQL Database',
                    'version': '1.0'
                }
            }
            with open(self.config_file.replace('.toml', '.json'), 'w') as f:
                json.dump(config, f, indent=2)
    
    def _get_data_dir(self) -> Path:
        """Get data directory path"""
        return Path(self.path).parent / f"{Path(self.path).stem}_data"
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query
        
        Args:
            sql: SQL query string
            
        Returns:
            Dictionary with query results
            
        Raises:
            GSQLSyntaxError: If SQL syntax is invalid
            GSQLExecutionError: If query execution fails
        """
        self.stats['queries_executed'] += 1
        
        try:
            # 1. Parse SQL
            ast = self.parser.parse(sql)
            
            # 2. Execute query
            result = self.executor.execute(ast)
            
            # 3. Update metadata and statistics
            self._update_after_execution(ast, result)
            
            # 4. Log successful execution
            self._log_operation('EXECUTE', sql, 'success')
            
            return result
            
        except (GSQLSyntaxError, GSQLExecutionError):
            # Re-raise known errors
            self._log_operation('EXECUTE', sql, 'error')
            raise
            
        except Exception as e:
            # Wrap unexpected errors
            self._log_operation('EXECUTE', sql, 'error')
            raise GSQLExecutionError(f"Unexpected error: {str(e)}")
    
    def query(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute SELECT query and return data
        
        Args:
            sql: SELECT query string
            
        Returns:
            List of dictionaries (rows)
        """
        result = self.execute(sql)
        return result.get('data', [])
    
    def create_table(self, name: str, columns: List[Dict]) -> None:
        """
        Create table via Python API
        
        Args:
            name: Table name
            columns: List of column definitions
        """
        self.storage.create_table(name, columns)
        self.tables[name] = {
            'columns': columns,
            'created_at': datetime.now().isoformat(),
            'row_count': 0
        }
        self.stats['tables_created'] += 1
        self._save_metadata()
        self._log_operation('CREATE_TABLE', f"Table '{name}' created", 'success')
    
    def insert(self, table: str, data: Union[Dict, List[Dict]]) -> int:
        """
        Insert data via Python API
        
        Args:
            table: Table name
            data: Single row as dict or list of rows
            
        Returns:
            Number of rows inserted
        """
        if isinstance(data, dict):
            data = [data]
        
        rows_inserted = 0
        for row in data:
            try:
                self.storage.insert(table, row)
                rows_inserted += 1
            except Exception as e:
                raise GSQLExecutionError(f"Failed to insert row: {e}")
        
        self.stats['rows_inserted'] += rows_inserted
        
        # Update table metadata
        if table in self.tables:
            self.tables[table]['row_count'] += rows_inserted
        
        self._save_metadata()
        self._log_operation('INSERT', f"{rows_inserted} rows into '{table}'", 'success')
        
        return rows_inserted
    
    def select(self, table: str, where: Optional[Dict] = None, 
               columns: Optional[List[str]] = None, limit: Optional[int] = None) -> List[Dict]:
        """
        Select data via Python API
        
        Args:
            table: Table name
            where: WHERE conditions as dict
            columns: List of columns to select
            limit: Maximum number of rows
            
        Returns:
            List of selected rows
        """
        return self.storage.select(table, where, columns, limit)
    
    def update(self, table: str, set_data: Dict, where: Dict) -> int:
        """
        Update data via Python API
        
        Args:
            table: Table name
            set_data: Values to update as dict
            where: WHERE conditions
            
        Returns:
            Number of rows updated
        """
        return self.storage.update(table, set_data, where)
    
    def delete(self, table: str, where: Dict) -> int:
        """
        Delete data via Python API
        
        Args:
            table: Table name
            where: WHERE conditions
            
        Returns:
            Number of rows deleted
        """
        return self.storage.delete(table, where)
    
    def create_index(self, table: str, column: str, index_type: str = 'btree') -> None:
        """
        Create index on column
        
        Args:
            table: Table name
            column: Column name
            index_type: Type of index ('btree', 'hash')
        """
        self.storage.create_index(table, column, index_type)
        self._log_operation('CREATE_INDEX', f"Index on {table}.{column}", 'success')
    
    def list_tables(self) -> List[str]:
        """
        List all tables in database
        
        Returns:
            List of table names
        """
        return self.storage.list_tables()
    
    def get_table_info(self, table: str) -> Optional[Dict]:
        """
        Get detailed table information
        
        Args:
            table: Table name
            
        Returns:
            Dictionary with table info or None if table doesn't exist
        """
        if table not in self.list_tables():
            return None
        
        # Get basic info from storage
        info = self.storage.get_table_info(table)
        
        # Add additional metadata
        if info:
            # Calculate approximate size
            data_file = self._get_data_dir() / 'tables' / f"{table}.json"
            if data_file.exists():
                info['file_size'] = data_file.stat().st_size
                info['file_modified'] = datetime.fromtimestamp(
                    data_file.stat().st_mtime
                ).isoformat()
            
            # Add from local cache
            if table in self.tables:
                info.update(self.tables[table])
        
        return info
    
    def backup(self, backup_name: Optional[str] = None) -> str:
        """
        Create database backup
        
        Args:
            backup_name: Optional backup name
            
        Returns:
            Path to backup directory
        """
        if not backup_name:
            backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        backup_path = self.storage.backup(backup_name)
        self._log_operation('BACKUP', f"Backup '{backup_name}' created", 'success')
        
        return backup_path
    
    def restore(self, backup_name: str) -> None:
        """
        Restore database from backup
        
        Args:
            backup_name: Name of backup to restore
        """
        self.storage.restore(backup_name)
        
        # Reload metadata
        self._load_metadata()
        
        self._log_operation('RESTORE', f"Restored from '{backup_name}'", 'success')
    
    def export_table(self, table: str, filepath: str) -> None:
        """
        Export table data to JSON file
        
        Args:
            table: Table name
            filepath: Path to output file
        """
        if table not in self.list_tables():
            raise GSQLTableError(f"Table '{table}' doesn't exist")
        
        data = self.select(table)
        
        export_data = {
            'table': table,
            'exported_at': datetime.now().isoformat(),
            'row_count': len(data),
            'data': data
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        self._log_operation('EXPORT', f"Table '{table}' to '{filepath}'", 'success')
    
    def import_table(self, filepath: str, table: Optional[str] = None) -> int:
        """
        Import table data from JSON file
        
        Args:
            filepath: Path to JSON file
            table: Table name (if None, use name from file)
            
        Returns:
            Number of rows imported
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Determine table name
        if table is None:
            if isinstance(data, dict) and 'table' in data:
                table = data['table']
            else:
                table = Path(filepath).stem
        
        # Get data
        if isinstance(data, dict) and 'data' in data:
            rows = data['data']
        elif isinstance(data, list):
            rows = data
        else:
            raise GSQLExecutionError("Invalid export format")
        
        # Create table if doesn't exist
        if table not in self.list_tables() and rows:
            # Infer schema from first row
            first_row = rows[0]
            columns = []
            for col_name, col_value in first_row.items():
                col_type = self._infer_type(col_value)
                columns.append({
                    'name': col_name,
                    'type': col_type
                })
            
            self.create_table(table, columns)
        
        # Insert rows
        rows_imported = self.insert(table, rows)
        
        self._log_operation('IMPORT', f"{rows_imported} rows to '{table}'", 'success')
        
        return rows_imported
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get database statistics
        
        Returns:
            Dictionary with statistics
        """
        tables = self.list_tables()
        total_rows = 0
        table_stats = {}
        
        for table in tables:
            info = self.get_table_info(table)
            if info:
                rows = info.get('row_count', 0)
                total_rows += rows
                table_stats[table] = {
                    'rows': rows,
                    'columns': len(info.get('columns', [])),
                    'created': info.get('created_at', 'N/A')
                }
        
        # Calculate uptime
        uptime = (datetime.now() - self.stats['start_time']).total_seconds()
        
        # Calculate storage size
        data_dir = self._get_data_dir()
        total_size = 0
        if data_dir.exists():
            for file in data_dir.rglob('*'):
                if file.is_file():
                    total_size += file.stat().st_size
        
        return {
            'database': self.path,
            'tables': len(tables),
            'total_rows': total_rows,
            'queries_executed': self.stats['queries_executed'],
            'tables_created': self.stats['tables_created'],
            'rows_inserted': self.stats['rows_inserted'],
            'uptime_seconds': uptime,
            'storage_size_bytes': total_size,
            'table_details': table_stats,
            'start_time': self.stats['start_time'].isoformat()
        }
    
    def vacuum(self) -> Dict[str, Any]:
        """
        Optimize database storage
        
        Returns:
            Dictionary with optimization results
        """
        results = {
            'tables_optimized': 0,
            'space_reclaimed': 0,
            'indexes_rebuilt': 0
        }
        
        tables = self.list_tables()
        
        for table in tables:
            try:
                # Get current file size
                table_file = self._get_data_dir() / 'tables' / f"{table}.json"
                if table_file.exists():
                    old_size = table_file.stat().st_size
                    
                    # Read and rewrite table (defragments JSON)
                    with open(table_file, 'r') as f:
                        data = json.load(f)
                    
                    with open(table_file, 'w') as f:
                        json.dump(data, f, separators=(',', ':'))
                    
                    new_size = table_file.stat().st_size
                    
                    results['space_reclaimed'] += max(0, old_size - new_size)
                    results['tables_optimized'] += 1
                    
            except Exception as e:
                # Log but don't fail on individual table errors
                self._log_operation('VACUUM', f"Error optimizing {table}: {e}", 'warning')
        
        self._log_operation('VACUUM', f"Optimized {results['tables_optimized']} tables", 'success')
        
        return results
    
    def check_integrity(self) -> Dict[str, Any]:
        """
        Check database integrity
        
        Returns:
            Dictionary with integrity check results
        """
        results = {
            'tables_checked': 0,
            'errors_found': 0,
            'warnings': [],
            'errors': []
        }
        
        tables = self.list_tables()
        
        for table in tables:
            try:
                info = self.get_table_info(table)
                if not info:
                    results['errors'].append(f"Table '{table}': Missing metadata")
                    results['errors_found'] += 1
                    continue
                
                # Check file exists
                table_file = self._get_data_dir() / 'tables' / f"{table}.json"
                if not table_file.exists():
                    results['errors'].append(f"Table '{table}': Data file missing")
                    results['errors_found'] += 1
                    continue
                
                # Verify JSON is valid
                try:
                    with open(table_file, 'r') as f:
                        data = json.load(f)
                    
                    # Check row count consistency
                    expected_rows = info.get('row_count', 0)
                    actual_rows = len(data) if isinstance(data, list) else 0
                    
                    if expected_rows != actual_rows:
                        results['warnings'].append(
                            f"Table '{table}': Row count mismatch "
                            f"(expected: {expected_rows}, actual: {actual_rows})"
                        )
                
                except json.JSONDecodeError:
                    results['errors'].append(f"Table '{table}': Invalid JSON data")
                    results['errors_found'] += 1
                
                results['tables_checked'] += 1
                
            except Exception as e:
                results['errors'].append(f"Table '{table}': Check failed: {e}")
                results['errors_found'] += 1
        
        self._log_operation('INTEGRITY_CHECK', 
                          f"Checked {results['tables_checked']} tables, "
                          f"found {results['errors_found']} errors", 
                          'success' if results['errors_found'] == 0 else 'warning')
        
        return results
    
    def _load_metadata(self) -> None:
        """Load metadata from storage"""
        try:
            meta_file = self._get_data_dir() / 'meta' / 'tables.json'
            if meta_file.exists():
                with open(meta_file, 'r') as f:
                    self.tables = json.load(f)
        except:
            # If metadata file is corrupted, start fresh
            self.tables = {}
    
    def _save_metadata(self) -> None:
        """Save metadata to storage"""
        try:
            meta_file = self._get_data_dir() / 'meta' / 'tables.json'
            with open(meta_file, 'w') as f:
                json.dump(self.tables, f, indent=2)
        except Exception as e:
            self._log_operation('SAVE_METADATA', f"Failed to save metadata: {e}", 'error')
    
    def _update_after_execution(self, ast: Dict, result: Dict) -> None:
        """Update metadata and statistics after query execution"""
        query_type = ast.get('type')
        
        if query_type == 'CREATE_TABLE':
            table = ast.get('table')
            if table and table not in self.tables:
                self.tables[table] = {
                    'columns': ast.get('columns', []),
                    'created_at': datetime.now().isoformat(),
                    'row_count': 0
                }
                self.stats['tables_created'] += 1
                self._save_metadata()
        
        elif query_type == 'INSERT':
            table = ast.get('table')
            rows = result.get('rows_affected', 0)
            
            if table and table in self.tables:
                self.tables[table]['row_count'] = self.tables[table].get('row_count', 0) + rows
            
            self.stats['rows_inserted'] += rows
            self._save_metadata()
        
        elif query_type == 'DELETE':
            table = ast.get('table')
            rows = result.get('rows_affected', 0)
            
            if table and table in self.tables:
                current = self.tables[table].get('row_count', 0)
                self.tables[table]['row_count'] = max(0, current - rows)
                self._save_metadata()
    
    def _log_operation(self, operation: str, details: str, status: str) -> None:
        """Log database operation"""
        try:
            log_dir = self._get_data_dir() / 'logs'
            log_dir.mkdir(exist_ok=True)
            
            log_file = log_dir / f"{datetime.now().strftime('%Y%m%d')}.log"
            
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'operation': operation,
                'details': details,
                'status': status,
                'database': self.path
            }
            
            with open(log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
                
        except:
            # Don't fail if logging fails
            pass
    
    def _infer_type(self, value: Any) -> str:
        """Infer column type from value"""
        if isinstance(value, int):
            return 'INTEGER'
        elif isinstance(value, float):
            return 'FLOAT'
        elif isinstance(value, bool):
            return 'BOOLEAN'
        elif isinstance(value, dict) or isinstance(value, list):
            return 'JSON'
        else:
            return 'TEXT'
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size for display"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"
    
    def close(self) -> None:
        """Close database connection"""
        try:
            # Save metadata
            self._save_metadata()
            
            # Close storage
            if hasattr(self.storage, 'close'):
                self.storage.close()
            
            # Save final statistics
            stats_file = self._get_data_dir() / 'meta' / 'stats.json'
            final_stats = self.get_stats()
            final_stats['end_time'] = datetime.now().isoformat()
            
            with open(stats_file, 'w') as f:
                json.dump(final_stats, f, indent=2)
            
            self._log_operation('CLOSE', 'Database closed', 'success')
            
        except Exception as e:
            self._log_operation('CLOSE', f"Error closing database: {e}", 'error')
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def __repr__(self) -> str:
        """String representation"""
        tables = self.list_tables()
        return (f"GSQL(database='{self.path}', "
                f"tables={len(tables)}, "
                f"path='{self._get_data_dir()}')")
    
    def __str__(self) -> str:
        """User-friendly string"""
        tables = self.list_tables()
        return (f"GSQL Database: {self.path}\n"
                f"Tables: {len(tables)}\n"
                f"Storage: {self._get_data_dir()}")


# Convenience function for quick access
def connect(database: Optional[str] = None) -> GSQL:
    """
    Connect to GSQL database (convenience function)
    
    Args:
        database: Database file path
        
    Returns:
        GSQL database instance
    """
    return GSQL(database)


# Export main class
__all__ = ['GSQL', 'connect', 'GSQLSyntaxError', 'GSQLExecutionError', 'GSQLTableError']
