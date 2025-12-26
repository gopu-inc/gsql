#!/usr/bin/env python3
"""
Database module for GSQL
Advanced database management with caching, transactions, and statistics
"""

import json
import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime
import hashlib
import time
import contextlib

from .storage import SQLiteStorage
from .executor import QueryExecutor

# ==================== CACHE MANAGEMENT ====================

class QueryCache:
    """LRU cache for query results"""
    
    def __init__(self, max_size: int = 100, ttl: int = 300):
        """
        Initialize cache
        
        Args:
            max_size: Maximum number of cached queries
            ttl: Time to live in seconds
        """
        self.max_size = max_size
        self.ttl = ttl
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.access_times: Dict[str, float] = {}
        self.lock = threading.RLock()
    
    def get(self, query_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached query result"""
        with self.lock:
            if query_hash not in self.cache:
                return None
            
            # Check if cache entry is expired
            if time.time() - self.access_times[query_hash] > self.ttl:
                del self.cache[query_hash]
                del self.access_times[query_hash]
                return None
            
            # Update access time
            self.access_times[query_hash] = time.time()
            return self.cache[query_hash]
    
    def set(self, query_hash: str, result: Dict[str, Any]) -> None:
        """Cache query result"""
        with self.lock:
            self.cache[query_hash] = result
            self.access_times[query_hash] = time.time()
            
            # Remove oldest entries if cache is full
            if len(self.cache) > self.max_size:
                # Find oldest accessed entry
                oldest_hash = min(self.access_times, key=self.access_times.get)
                del self.cache[oldest_hash]
                del self.access_times[oldest_hash]
    
    def clear(self) -> None:
        """Clear cache"""
        with self.lock:
            self.cache.clear()
            self.access_times.clear()
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self.lock:
            return {
                'size': len(self.cache),
                'max_size': self.max_size,
                'ttl': self.ttl,
                'hits': sum(1 for v in self.cache.values() if v.get('cached', False)),
                'memory_usage': sum(len(str(v)) for v in self.cache.values())
            }

# ==================== TRANSACTION MANAGEMENT ====================

class Transaction:
    """Database transaction context manager"""
    
    def __init__(self, database: 'Database'):
        self.database = database
        self.active = False
    
    def __enter__(self) -> 'Transaction':
        self.begin()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
    
    def begin(self) -> None:
        """Begin transaction"""
        if not self.active:
            self.database.execute("BEGIN TRANSACTION")
            self.active = True
    
    def commit(self) -> None:
        """Commit transaction"""
        if self.active:
            self.database.execute("COMMIT")
            self.active = False
    
    def rollback(self) -> None:
        """Rollback transaction"""
        if self.active:
            self.database.execute("ROLLBACK")
            self.active = False

# ==================== MAIN DATABASE CLASS ====================

class Database:
    """Main database class with advanced features"""
    
    def __init__(self, db_path: str = None, **kwargs):
        """
        Initialize database
        
        Args:
            db_path: Path to database file
            **kwargs: Additional parameters for storage configuration
                      Supported: base_dir, auto_recovery, buffer_pool_size, enable_wal
        """
        self.db_path = db_path
        
        # Filter valid parameters for storage
        valid_params = ['base_dir', 'auto_recovery', 'buffer_pool_size', 'enable_wal']
        storage_params = {k: v for k, v in kwargs.items() if k in valid_params}
        
        self.storage = SQLiteStorage(db_path, **storage_params)
        self.executor = QueryExecutor(self.storage)
        self.cache = QueryCache(max_size=100, ttl=300)
        self.lock = threading.RLock()
        self.transaction_depth = 0
        
        # Configure SQLite pragmas
        self._configure_pragmas(kwargs)
        
        # Initialize with default tables if new database
        if self.storage.is_new_database():
            self._initialize_default_tables()
    
    def _configure_pragmas(self, config: Dict) -> None:
        """Configure SQLite pragmas"""
        try:
            cursor = self.storage.connection.cursor()
            
            # Configure journal mode if specified
            journal_mode = config.get('journal_mode')
            if journal_mode in ['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'WAL', 'OFF']:
                cursor.execute(f"PRAGMA journal_mode = {journal_mode}")
            
            # Configure synchronous mode
            synchronous = config.get('synchronous')
            if synchronous in ['OFF', 'NORMAL', 'FULL', 'EXTRA']:
                cursor.execute(f"PRAGMA synchronous = {synchronous}")
            
            # Configure foreign keys
            cursor.execute("PRAGMA foreign_keys = ON")
            
            # Configure busy timeout
            cursor.execute("PRAGMA busy_timeout = 5000")
            
            # Configure cache size
            cache_size = config.get('cache_size', -2000)
            cursor.execute(f"PRAGMA cache_size = {cache_size}")
            
            # Configure temp store
            temp_store = config.get('temp_store', 0)
            cursor.execute(f"PRAGMA temp_store = {temp_store}")
            
        except Exception as e:
            print(f"Warning: Could not configure SQLite pragmas: {e}")
    
    def _initialize_default_tables(self):
        """Initialize default tables for new database"""
        with self.lock:
            # Create metadata table
            self.storage.execute("""
                CREATE TABLE IF NOT EXISTS gsql_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create query history table with more details
            self.storage.execute("""
                CREATE TABLE IF NOT EXISTS query_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_hash TEXT,
                    query_text TEXT,
                    query_type TEXT,
                    execution_time REAL,
                    success BOOLEAN,
                    rows_affected INTEGER,
                    error_message TEXT,
                    user_agent TEXT,
                    ip_address TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create index on query history
            self.storage.execute("""
                CREATE INDEX IF NOT EXISTS idx_query_history_timestamp 
                ON query_history(timestamp)
            """)
            
            # Create index on query history type
            self.storage.execute("""
                CREATE INDEX IF NOT EXISTS idx_query_history_type 
                ON query_history(query_type)
            """)
            
            # Create performance metrics table
            self.storage.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT,
                    metric_value REAL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert default metadata
            metadata = [
                ("version", "3.10.0", "GSQL Database Version"),
                ("created", datetime.now().isoformat(), "Database creation date"),
                ("encoding", "UTF-8", "Database encoding"),
                ("auto_vacuum", "0", "Auto-vacuum setting"),
                ("page_size", "4096", "Database page size"),
                ("gsql_compatible", "true", "GSQL compatibility flag")
            ]
            
            for key, value, description in metadata:
                self.storage.execute(
                    """INSERT OR IGNORE INTO gsql_metadata (key, value, description) 
                       VALUES (?, ?, ?)""",
                    (key, value, description)
                )
    
    def execute(self, query: str, params: Tuple = None, 
                use_cache: bool = True, user_context: Dict = None) -> Dict[str, Any]:
        """
        Execute a SQL query with advanced features
        
        Args:
            query: SQL query string
            params: Query parameters
            use_cache: Whether to use query cache
            user_context: User context information (user_agent, ip_address, etc.)
            
        Returns:
            Dict with query results
        """
        start_time = time.time()
        
        with self.lock:
            try:
                query = query.strip()
                if not query:
                    return {
                        'success': False,
                        'message': "Empty query"
                    }
                
                # Generate query hash for caching
                query_hash = None
                if use_cache and query.upper().startswith('SELECT'):
                    query_data = query + str(params) if params else query
                    query_hash = hashlib.md5(query_data.encode()).hexdigest()
                    
                    # Check cache
                    cached_result = self.cache.get(query_hash)
                    if cached_result:
                        cached_result['cached'] = True
                        cached_result['execution_time'] = time.time() - start_time
                        return cached_result
                
                # Execute query
                result = self.executor.execute(query, params)
                execution_time = time.time() - start_time
                
                # Add execution time to result
                result['execution_time'] = execution_time
                
                # Cache SELECT queries
                if (use_cache and query_hash and query.upper().startswith('SELECT') 
                    and result.get('success')):
                    self.cache.set(query_hash, result.copy())
                
                # Log query to history
                self._log_query(query, params, result, execution_time, user_context)
                
                # Update performance metrics
                self._update_performance_metrics(query, execution_time, result.get('success'))
                
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                error_result = {
                    'success': False,
                    'message': str(e),
                    'error_type': type(e).__name__,
                    'execution_time': execution_time
                }
                
                # Log failed query
                self._log_query(query, params, error_result, execution_time, user_context)
                
                return error_result
    
    def _log_query(self, query: str, params: Tuple, result: Dict, 
                   execution_time: float, user_context: Dict = None) -> None:
        """Log query to history table"""
        try:
            query_type = query.strip().split()[0].upper() if query.strip() else 'UNKNOWN'
            success = result.get('success', False)
            rows_affected = result.get('rows_affected', 0)
            error_message = result.get('message') if not success else None
            
            # Generate query hash
            query_data = query + str(params) if params else query
            query_hash = hashlib.md5(query_data.encode()).hexdigest()
            
            # Extract user context
            user_agent = user_context.get('user_agent') if user_context else None
            ip_address = user_context.get('ip_address') if user_context else None
            
            self.storage.execute(
                """INSERT INTO query_history 
                   (query_hash, query_text, query_type, execution_time, 
                    success, rows_affected, error_message, user_agent, ip_address) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (query_hash, query[:500], query_type, execution_time, 
                 success, rows_affected, error_message, user_agent, ip_address)
            )
        except:
            pass  # Don't crash if logging fails
    
    def _update_performance_metrics(self, query: str, execution_time: float, 
                                   success: bool) -> None:
        """Update performance metrics"""
        try:
            query_type = query.strip().split()[0].upper() if query.strip() else 'UNKNOWN'
            
            # Log execution time by query type
            self.storage.execute(
                """INSERT INTO performance_metrics (metric_name, metric_value) 
                   VALUES (?, ?)""",
                (f"execution_time_{query_type.lower()}", execution_time)
            )
            
            # Log success/failure
            metric_value = 1.0 if success else 0.0
            self.storage.execute(
                """INSERT INTO performance_metrics (metric_name, metric_value) 
                   VALUES (?, ?)""",
                (f"success_{query_type.lower()}", metric_value)
            )
        except:
            pass
    
    def transaction(self) -> Transaction:
        """Create a transaction context manager"""
        return Transaction(self)
    
    @contextlib.contextmanager
    def atomic(self):
        """Atomic transaction context manager (alternative syntax)"""
        transaction = self.transaction()
        try:
            transaction.begin()
            yield
            transaction.commit()
        except Exception:
            transaction.rollback()
            raise
    
    def begin_transaction(self) -> None:
        """Begin a transaction"""
        with self.lock:
            if self.transaction_depth == 0:
                self.execute("BEGIN TRANSACTION")
            self.transaction_depth += 1
    
    def commit_transaction(self) -> None:
        """Commit a transaction"""
        with self.lock:
            if self.transaction_depth > 0:
                self.transaction_depth -= 1
                if self.transaction_depth == 0:
                    self.execute("COMMIT")
    
    def rollback_transaction(self) -> None:
        """Rollback a transaction"""
        with self.lock:
            if self.transaction_depth > 0:
                self.transaction_depth = 0
                self.execute("ROLLBACK")
    
    def close(self):
        """Close database connection"""
        if self.storage:
            self.storage.close()
    
    # ==================== DATABASE OPERATIONS ====================
    
    def backup(self, backup_path: str = None) -> Dict[str, Any]:
        """Create database backup"""
        start_time = time.time()
        
        try:
            if backup_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_dir = Path(self.db_path).parent / "backups"
                backup_dir.mkdir(exist_ok=True)
                db_name = Path(self.db_path).stem
                backup_path = str(backup_dir / f"{db_name}_backup_{timestamp}.db")
            
            # Create backup using SQLite backup API
            source_conn = self.storage.connection
            dest_conn = sqlite3.connect(backup_path)
            
            with dest_conn:
                source_conn.backup(dest_conn)
            
            dest_conn.close()
            
            backup_size = Path(backup_path).stat().st_size
            
            return {
                'success': True,
                'backup_file': backup_path,
                'size': backup_size,
                'execution_time': time.time() - start_time
            }
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'execution_time': time.time() - start_time
            }
    
    def restore(self, backup_path: str) -> Dict[str, Any]:
        """Restore database from backup"""
        start_time = time.time()
        
        try:
            if not Path(backup_path).exists():
                return {
                    'success': False,
                    'message': f"Backup file not found: {backup_path}"
                }
            
            # Close current connection
            self.close()
            
            # Replace current database with backup
            import shutil
            shutil.copy2(backup_path, self.db_path)
            
            # Reinitialize storage
            self.storage = SQLiteStorage(self.db_path)
            self.executor = QueryExecutor(self.storage)
            
            return {
                'success': True,
                'message': "Database restored successfully",
                'execution_time': time.time() - start_time
            }
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'execution_time': time.time() - start_time
            }
    
    def vacuum(self, into_file: str = None) -> Dict[str, Any]:
        """Optimize database"""
        start_time = time.time()
        
        try:
            if into_file:
                result = self.execute(f"VACUUM INTO '{into_file}'")
            else:
                result = self.execute("VACUUM")
            
            if result.get('success'):
                # Analyze for better performance
                self.execute("ANALYZE")
                
                # Update statistics
                self._update_database_stats()
                
                return {
                    'success': True,
                    'message': "Database optimized successfully",
                    'execution_time': time.time() - start_time
                }
            else:
                return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'execution_time': time.time() - start_time
            }
    
    def _update_database_stats(self) -> None:
        """Update database statistics"""
        try:
            # Get database size
            db_size = Path(self.db_path).stat().st_size
            
            # Get table sizes
            result = self.execute("""
                SELECT name, (
                    SELECT SUM(pgsize) FROM dbstat WHERE name = tbl.name
                ) as size
                FROM sqlite_master as tbl
                WHERE type = 'table'
                ORDER BY size DESC
            """)
            
            if result.get('success'):
                table_sizes = result.get('rows', [])
                
                # Store in metadata
                for table in table_sizes:
                    self.storage.execute(
                        """INSERT OR REPLACE INTO gsql_metadata (key, value, description) 
                           VALUES (?, ?, ?)""",
                        (f"table_size_{table['name']}", str(table['size']), 
                         f"Size of table {table['name']}")
                    )
            
            # Store total size
            self.storage.execute(
                """INSERT OR REPLACE INTO gsql_metadata (key, value, description) 
                   VALUES (?, ?, ?)""",
                ("database_size", str(db_size), "Total database size in bytes")
            )
        except:
            pass
    
    # ==================== STATISTICS AND METRICS ====================
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        start_time = time.time()
        
        try:
            stats = {}
            
            # Basic info
            stats['path'] = self.db_path
            stats['size'] = Path(self.db_path).stat().st_size if Path(self.db_path).exists() else 0
            stats['created'] = datetime.fromtimestamp(
                Path(self.db_path).stat().st_ctime
            ).isoformat() if Path(self.db_path).exists() else None
            
            # SQLite version
            result = self.execute("SELECT sqlite_version() as version")
            if result.get('success'):
                stats['sqlite_version'] = result.get('rows', [{}])[0].get('version')
            
            # Table statistics
            tables_result = self.execute("""
                SELECT name, type, sql 
                FROM sqlite_master 
                WHERE type IN ('table', 'view', 'index', 'trigger')
                ORDER BY type, name
            """)
            
            if tables_result.get('success'):
                stats['objects'] = tables_result.get('rows', [])
                stats['table_count'] = len([o for o in stats['objects'] if o['type'] == 'table'])
                stats['view_count'] = len([o for o in stats['objects'] if o['type'] == 'view'])
                stats['index_count'] = len([o for o in stats['objects'] if o['type'] == 'index'])
                stats['trigger_count'] = len([o for o in stats['objects'] if o['type'] == 'trigger'])
            
            # Row count statistics
            row_counts = {}
            total_rows = 0
            if 'objects' in stats:
                for obj in stats['objects']:
                    if obj['type'] == 'table':
                        table_name = obj['name']
                        count_result = self.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                        if count_result.get('success'):
                            count = count_result.get('rows', [{}])[0].get('count', 0)
                            row_counts[table_name] = count
                            total_rows += count
            
            stats['row_counts'] = row_counts
            stats['total_rows'] = total_rows
            
            # Query history statistics
            history_result = self.execute("""
                SELECT 
                    COUNT(*) as total_queries,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_queries,
                    AVG(execution_time) as avg_execution_time,
                    MAX(execution_time) as max_execution_time,
                    MIN(execution_time) as min_execution_time
                FROM query_history
                WHERE timestamp > datetime('now', '-1 day')
            """)
            
            if history_result.get('success'):
                stats['recent_queries'] = history_result.get('rows', [{}])[0]
            
            # Performance metrics
            perf_result = self.execute("""
                SELECT metric_name, AVG(metric_value) as avg_value
                FROM performance_metrics
                WHERE timestamp > datetime('now', '-1 hour')
                GROUP BY metric_name
            """)
            
            if perf_result.get('success'):
                stats['performance_metrics'] = perf_result.get('rows', [])
            
            # Cache statistics
            stats['cache'] = self.cache.stats()
            
            # Connection info
            stats['connection'] = {
                'connected': self.storage.connection is not None,
                'in_transaction': self.transaction_depth > 0,
                'transaction_depth': self.transaction_depth
            }
            
            # SQLite pragmas
            pragmas = [
                'journal_mode', 'synchronous', 'foreign_keys',
                'cache_size', 'page_size', 'encoding'
            ]
            
            pragma_values = {}
            for pragma in pragmas:
                result = self.execute(f"PRAGMA {pragma}")
                if result.get('success'):
                    pragma_values[pragma] = result.get('rows', [{}])[0].get(pragma)
            
            stats['pragmas'] = pragma_values
            
            return {
                'success': True,
                'database': stats,
                'execution_time': time.time() - start_time
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'execution_time': time.time() - start_time
            }
    
    def get_query_history(self, limit: int = 100, 
                          query_type: str = None) -> Dict[str, Any]:
        """Get query history"""
        start_time = time.time()
        
        try:
            query = "SELECT * FROM query_history"
            params = []
            
            if query_type:
                query += " WHERE query_type = ?"
                params.append(query_type)
            
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            result = self.execute(query, tuple(params) if params else None)
            
            if result.get('success'):
                return {
                    'success': True,
                    'history': result.get('rows', []),
                    'count': result.get('count', 0),
                    'execution_time': time.time() - start_time
                }
            else:
                return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'execution_time': time.time() - start_time
            }
    
    def clear_history(self) -> Dict[str, Any]:
        """Clear query history"""
        try:
            result = self.execute("DELETE FROM query_history")
            if result.get('success'):
                result['message'] = "Query history cleared"
            return result
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }
    
    def clear_cache(self) -> Dict[str, Any]:
        """Clear query cache"""
        try:
            self.cache.clear()
            return {
                'success': True,
                'message': "Query cache cleared"
            }
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }
    
    # ==================== UTILITY METHODS ====================
    
    def export_schema(self, output_file: str = None) -> Dict[str, Any]:
        """Export database schema"""
        start_time = time.time()
        
        try:
            # Get all schema definitions
            result = self.execute("""
                SELECT type, name, sql 
                FROM sqlite_master 
                WHERE sql IS NOT NULL
                ORDER BY type, name
            """)
            
            if not result.get('success'):
                return result
            
            schema = {
                'version': '1.0',
                'export_date': datetime.now().isoformat(),
                'database': self.db_path,
                'tables': [],
                'views': [],
                'indexes': [],
                'triggers': []
            }
            
            for row in result.get('rows', []):
                item = {
                    'name': row['name'],
                    'sql': row['sql']
                }
                
                if row['type'] == 'table':
                    schema['tables'].append(item)
                elif row['type'] == 'view':
                    schema['views'].append(item)
                elif row['type'] == 'index':
                    schema['indexes'].append(item)
                elif row['type'] == 'trigger':
                    schema['triggers'].append(item)
            
            if output_file:
                with open(output_file, 'w') as f:
                    json.dump(schema, f, indent=2)
                
                return {
                    'success': True,
                    'message': f"Schema exported to {output_file}",
                    'schema': schema,
                    'output_file': output_file,
                    'execution_time': time.time() - start_time
                }
            else:
                return {
                    'success': True,
                    'schema': schema,
                    'execution_time': time.time() - start_time
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'execution_time': time.time() - start_time
            }
    
    def import_schema(self, schema_file: str) -> Dict[str, Any]:
        """Import database schema"""
        start_time = time.time()
        
        try:
            with open(schema_file, 'r') as f:
                schema = json.load(f)
            
            # Execute all SQL statements
            success_count = 0
            error_count = 0
            errors = []
            
            # Execute in transaction
            with self.transaction():
                # Execute table definitions
                for table in schema.get('tables', []):
                    try:
                        self.execute(table['sql'])
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        errors.append(f"Table {table['name']}: {str(e)}")
                
                # Execute view definitions
                for view in schema.get('views', []):
                    try:
                        self.execute(view['sql'])
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        errors.append(f"View {view['name']}: {str(e)}")
                
                # Execute index definitions
                for index in schema.get('indexes', []):
                    try:
                        self.execute(index['sql'])
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        errors.append(f"Index {index['name']}: {str(e)}")
            
            return {
                'success': error_count == 0,
                'message': f"Imported {success_count} objects, {error_count} errors",
                'success_count': success_count,
                'error_count': error_count,
                'errors': errors if errors else None,
                'execution_time': time.time() - start_time
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'execution_time': time.time() - start_time
            }
    
    def check_integrity(self) -> Dict[str, Any]:
        """Check database integrity"""
        start_time = time.time()
        
        try:
            # Quick check
            result = self.execute("PRAGMA quick_check")
            if not result.get('success'):
                return result
            
            quick_check = result.get('rows', [{}])[0].get('quick_check', 'ok')
            
            # Full integrity check if quick check passed
            if quick_check == 'ok':
                result = self.execute("PRAGMA integrity_check")
                if not result.get('success'):
                    return result
                
                integrity_check = result.get('rows', [{}])[0].get('integrity_check', 'ok')
            else:
                integrity_check = quick_check
            
            # Foreign key check
            result = self.execute("PRAGMA foreign_key_check")
            foreign_key_errors = result.get('rows', [])
            
            return {
                'success': True,
                'integrity': {
                    'quick_check': quick_check,
                    'integrity_check': integrity_check,
                    'foreign_key_errors': foreign_key_errors,
                    'is_ok': quick_check == 'ok' and integrity_check == 'ok' and not foreign_key_errors
                },
                'execution_time': time.time() - start_time
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'execution_time': time.time() - start_time
            }
    
    def optimize(self) -> Dict[str, Any]:
        """Perform comprehensive database optimization"""
        start_time = time.time()
        
        try:
            steps = []
            
            # Step 1: Vacuum
            vacuum_result = self.vacuum()
            steps.append({
                'step': 'vacuum',
                'success': vacuum_result.get('success', False),
                'message': vacuum_result.get('message', '')
            })
            
            # Step 2: Analyze
            analyze_result = self.execute("ANALYZE")
            steps.append({
                'step': 'analyze',
                'success': analyze_result.get('success', False),
                'message': analyze_result.get('message', '')
            })
            
            # Step 3: Reindex
            reindex_result = self.execute("REINDEX")
            steps.append({
                'step': 'reindex',
                'success': reindex_result.get('success', False),
                'message': reindex_result.get('message', '')
            })
            
            # Step 4: Clear cache
            cache_result = self.clear_cache()
            steps.append({
                'step': 'clear_cache',
                'success': cache_result.get('success', False),
                'message': cache_result.get('message', '')
            })
            
            # Check overall success
            all_success = all(step['success'] for step in steps)
            
            return {
                'success': all_success,
                'steps': steps,
                'message': "Database optimization completed",
                'execution_time': time.time() - start_time
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'execution_time': time.time() - start_time
            }

# ==================== FACTORY FUNCTIONS ====================

def create_database(db_path: str = None, **kwargs) -> Database:
    """Create a new database instance"""
    return Database(db_path, **kwargs)

def get_default_database() -> Optional[Database]:
    """Get the default database instance"""
    # Implement according to your needs
    # This could return a globally stored instance
    return None

def set_default_database(db: Database) -> None:
    """Set the default database instance"""
    # Store globally or in configuration
    pass

def connect(db_path: str = None, **kwargs) -> Database:
    """Connect to a database (alias for create_database)"""
    return create_database(db_path, **kwargs)

# ==================== TEST ====================

if __name__ == "__main__":
    # Test the database module
    import tempfile
    import os
    
    # Create temporary database
    temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    temp_db.close()
    
    try:
        # Create database
        db = create_database(temp_db.name)
        
        # Test basic operations
        print("Testing database operations...")
        
        # Create table
        result = db.execute("""
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print(f"Create table: {result.get('success')}")
        
        # Insert data
        result = db.execute(
            "INSERT INTO test_users (name, email, age) VALUES (?, ?, ?)",
            ("John Doe", "john@example.com", 30)
        )
        print(f"Insert: {result.get('success')}, ID: {result.get('last_insert_id')}")
        
        # Select data
        result = db.execute("SELECT * FROM test_users")
        print(f"Select: {result.get('success')}, Rows: {result.get('count')}")
        
        # Test cache
        result = db.execute("SELECT * FROM test_users", use_cache=True)
        print(f"Cached select: {result.get('cached', False)}")
        
        # Test transaction
        with db.transaction():
            db.execute("INSERT INTO test_users (name, email, age) VALUES (?, ?, ?)",
                      ("Jane Doe", "jane@example.com", 25))
        
        # Get stats
        stats = db.get_stats()
        print(f"Stats: {stats.get('success')}")
        
        # Backup
        backup_result = db.backup()
        print(f"Backup: {backup_result.get('success')}")
        
        # Cleanup
        db.close()
        
    finally:
        # Clean up temporary files
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)
        if 'backup_result' in locals() and backup_result.get('success'):
            backup_file = backup_result.get('backup_file')
            if backup_file and os.path.exists(backup_file):
                os.unlink(backup_file)
    
    print("\nDatabase tests completed!")
