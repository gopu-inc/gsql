#!/usr/bin/env python3
"""
GSQL Database Module - SQLite Backend Only
Version: 3.0 - Auto-recovery, no YAML
"""

import os
import sqlite3
import json
import logging
import time
import threading
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union
import re

from .storage import SQLiteStorage, create_storage
from .exceptions import (
    GSQLBaseException, SQLSyntaxError, SQLExecutionError,
    ConstraintViolationError, TransactionError, FunctionError,
    BufferPoolError, StorageError
)

logger = logging.getLogger(__name__)

class Database:
    """Base de données SQLite auto-récupérante"""
    
    def __init__(self, db_path=None, base_dir="/root/.gsql", 
                 buffer_pool_size=100, enable_wal=True, auto_recovery=True):
        """
        Initialise la base de données SQLite
        
        Args:
            db_path: Chemin de la base (None pour auto)
            base_dir: Répertoire de base pour GSQL
            buffer_pool_size: Taille du buffer pool
            enable_wal: Activer le mode WAL
            auto_recovery: Activer la récupération automatique
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # Configuration
        self.config = {
            'db_path': db_path,
            'base_dir': str(base_dir),
            'buffer_pool_size': buffer_pool_size,
            'enable_wal': enable_wal,
            'auto_recovery': auto_recovery,
            'auto_backup': True,
            'backup_interval': 24 * 3600,  # 24 heures
            'max_query_cache': 100,
            'query_timeout': 30,  # secondes
            'version': '3.0'
        }
        
        # Initialiser le moteur de stockage
        self.storage = create_storage(
            db_path=db_path,
            base_dir=base_dir,
            buffer_pool_size=buffer_pool_size,
            enable_wal=enable_wal
        )
        
        # État de la base
        self.is_open = True
        self.initialized = False
        self.lock = threading.RLock()
        
        # Cache des résultats
        self.query_cache = {}
        self.schema_cache = {}
        
        # Statistiques
        self.stats = {
            'queries_executed': 0,
            'queries_cached': 0,
            'transactions': 0,
            'errors': 0,
            'start_time': datetime.now(),
            'last_backup': None
        }
        
        # Journal de transaction actif
        self.active_transactions = {}
        
        # Initialiser les tables système
        self._initialize_database()
        
        logger.info(f"GSQL Database initialized (v{self.config['version']})")
    
    def _initialize_database(self, skip_recovery=False):
        """Initialise les tables et structures système"""
        try:
            with self.lock:
                # Vérifier si les tables système existent
                tables = self.storage.get_tables()
                
                # Créer des tables par défaut si nécessaire
                default_tables = self._get_default_tables()
                
                for table_name, table_sql in default_tables.items():
                    table_exists = any(t['table_name'] == table_name for t in tables)
                    
                    if not table_exists:
                        logger.info(f"Creating default table: {table_name}")
                        self.storage.execute(table_sql)
                
                self.initialized = True
                
                # Sauvegarder la configuration
                self._save_config()
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            if self.config['auto_recovery'] and not skip_recovery:
                logger.warning("Attempting auto-recovery...")
                self._auto_recover()
            else:
                raise
    
    def _get_default_tables(self) -> Dict[str, str]:
        """Retourne les tables par défaut à créer"""
        return {
            'users': """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    full_name TEXT,
                    age INTEGER CHECK(age >= 0),
                    city TEXT DEFAULT 'Unknown',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'products': """
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    price REAL NOT NULL CHECK(price >= 0),
                    category TEXT,
                    stock INTEGER DEFAULT 0 CHECK(stock >= 0),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    active BOOLEAN DEFAULT 1
                )
            """,
            
            'orders': """
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL CHECK(quantity > 0),
                    total_price REAL NOT NULL CHECK(total_price >= 0),
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
                )
            """,
            
            'logs': """
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    source TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }
    
    def _auto_recover(self, recursion_depth=0):
        """Tente une récupération automatique avec limite de récursion"""
        MAX_RECURSION = 3
        
        if recursion_depth >= MAX_RECURSION:
            logger.error(f"Max recursion depth ({MAX_RECURSION}) reached in auto-recovery")
            raise SQLExecutionError("Auto-recovery failed: max recursion depth exceeded")
        
        try:
            logger.warning(f"Starting auto-recovery (attempt {recursion_depth + 1})...")
            
            # Fermer et réouvrir le storage
            if self.storage:
                self.storage.close()
            
            # Réinitialiser le storage
            self.storage = create_storage(
                db_path=self.config['db_path'],
                base_dir=self.config['base_dir'],
                buffer_pool_size=self.config['buffer_pool_size'],
                enable_wal=self.config['enable_wal']
            )
            
            # Réinitialiser les tables SANS déclencher une nouvelle récupération
            self._initialize_database(skip_recovery=True)
            
            logger.info("Auto-recovery completed successfully")
            
        except Exception as e:
            logger.error(f"Auto-recovery attempt {recursion_depth + 1} failed: {e}")
            
            # Réessayer si pas encore à la limite
            if recursion_depth < MAX_RECURSION - 1:
                wait_time = 1 * (2 ** recursion_depth)
                time.sleep(wait_time)
                return self._auto_recover(recursion_depth + 1)
            else:
                raise SQLExecutionError(f"Database recovery failed after {MAX_RECURSION} attempts: {e}")
    
    def execute(self, sql: str, params: Dict = None, 
                use_cache: bool = True, timeout: int = None) -> Dict:
        """
        Exécute une requête SQL sur la base de données
        
        Args:
            sql: Requête SQL à exécuter
            params: Paramètres pour la requête préparée
            use_cache: Utiliser le cache de requêtes
            timeout: Timeout en secondes
        
        Returns:
            Dict: Résultats formatés de la requête
        """
        if not self.is_open:
            raise SQLExecutionError("Database is closed")
        
        # Détecter les commandes spéciales GSQL
        special_result = self._handle_special_commands(sql)
        if special_result:
            return special_result
        
        start_time = datetime.now()
        query_hash = None
        
        # Générer un hash pour le cache
        if use_cache and params is None:
            query_hash = hashlib.md5(sql.encode()).hexdigest()[:16]
            cached = self.query_cache.get(query_hash)
            if cached:
                self.stats['queries_cached'] += 1
                logger.debug(f"Query cache hit: {query_hash}")
                return cached
        
        try:
            with self.lock:
                # Exécuter la requête via le storage
                result = self.storage.execute(
                    sql=sql,
                    params=params,
                    use_cache=use_cache,
                    track_stats=True
                )
                
                # Ajouter des métadonnées
                execution_time = (datetime.now() - start_time).total_seconds()
                result['execution_time'] = round(execution_time, 3)
                result['timestamp'] = datetime.now().isoformat()
                
                # Mettre à jour les statistiques
                self.stats['queries_executed'] += 1
                
                # Mettre en cache les requêtes SELECT réussies
                if (use_cache and query_hash and 
                    result.get('success') and 
                    result.get('type') == 'select'):
                    
                    # Limiter la taille du cache
                    if len(self.query_cache) >= self.config['max_query_cache']:
                        # Supprimer la plus ancienne entrée
                        oldest_key = min(self.query_cache.keys(), 
                                        key=lambda k: self.query_cache[k]['timestamp'])
                        del self.query_cache[oldest_key]
                    
                    self.query_cache[query_hash] = result
                
                # Logger les requêtes importantes
                if execution_time > 1.0:  # Plus d'1 seconde
                    logger.warning(f"Slow query ({execution_time:.2f}s): {sql[:100]}...")
                
                return result
                
        except SQLExecutionError as e:
            self.stats['errors'] += 1
            logger.error(f"Query execution error: {e}")
            
            # Tentative de récupération pour certaines erreurs
            if "database is locked" in str(e) and self.config['auto_recovery']:
                logger.info("Database locked, attempting recovery...")
                self._auto_recover()
                
                # Réessayer la requête
                return self.execute(sql, params, use_cache=False)
            
            raise
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Unexpected error: {e}")
            raise SQLExecutionError(f"Database error: {str(e)}")
    
    def _handle_special_commands(self, sql: str) -> Optional[Dict]:
        """Gère les commandes spéciales GSQL"""
        sql_upper = sql.strip().upper()
        
        # SHOW TABLES
        if sql_upper == "SHOW TABLES" or sql == ".tables":
            return self._execute_show_tables()
        
        # SHOW FUNCTIONS
        elif sql_upper == "SHOW FUNCTIONS":
            return self._execute_show_functions()
        
        # DESCRIBE / SCHEMA
        elif sql_upper.startswith("DESCRIBE ") or sql_upper.startswith("SCHEMA "):
            table_name = sql.split()[1] if len(sql.split()) > 1 else None
            if table_name:
                return self._execute_describe_table(table_name)
        
        # STATS
        elif sql_upper == "STATS" or sql == ".stats":
            return self._execute_stats()
        
        # VACUUM
        elif sql_upper == "VACUUM":
            return self._execute_vacuum()
        
        # BACKUP
        elif sql_upper.startswith("BACKUP"):
            return self._execute_backup(sql)
        
        # HELP
        elif sql_upper == "HELP" or sql == ".help":
            return self._execute_help()
        
        return None
    
    def _execute_show_tables(self) -> Dict:
        """Exécute SHOW TABLES"""
        try:
            tables = self.storage.get_tables()
            
            # Formater les résultats
            formatted_tables = []
            for table in tables:
                formatted_tables.append({
                    'table': table['table_name'],
                    'rows': table.get('row_count', 0),
                    'size_kb': round(table.get('size_bytes', 0) / 1024, 2),
                    'columns': table.get('columns', []),
                    'last_analyzed': table.get('last_analyzed')
                })
            
            return {
                'type': 'show_tables',
                'tables': formatted_tables,
                'count': len(formatted_tables),
                'message': f'Found {len(formatted_tables)} table(s)',
                'success': True
            }
            
        except Exception as e:
            return {
                'type': 'show_tables',
                'tables': [],
                'count': 0,
                'message': f'Error: {str(e)}',
                'success': False
            }
    
    def _execute_show_functions(self) -> Dict:
        """Exécute SHOW FUNCTIONS"""
        try:
            # Récupérer les fonctions depuis le storage
            result = self.storage.execute("""
                SELECT name, params, returns, created_at, is_active
                FROM _gsql_functions
                WHERE is_active = 1
                ORDER BY name
            """)
            
            # Ajouter les fonctions intégrées
            builtin_funcs = [
                {'name': 'UPPER(text)', 'type': 'builtin', 'description': 'Convert to uppercase'},
                {'name': 'LOWER(text)', 'type': 'builtin', 'description': 'Convert to lowercase'},
                {'name': 'LENGTH(text)', 'type': 'builtin', 'description': 'String length'},
                {'name': 'ABS(number)', 'type': 'builtin', 'description': 'Absolute value'},
                {'name': 'ROUND(number, decimals)', 'type': 'builtin', 'description': 'Round number'},
                {'name': 'CONCAT(str1, str2, ...)', 'type': 'builtin', 'description': 'Concatenate strings'},
                {'name': 'NOW()', 'type': 'builtin', 'description': 'Current timestamp'},
                {'name': 'DATE()', 'type': 'builtin', 'description': 'Current date'}
            ]
            
            functions = []
            if result['success'] and result['type'] == 'select':
                for row in result['rows']:
                    functions.append({
                        'name': row['name'],
                        'type': 'user',
                        'params': json.loads(row['params']) if row['params'] else [],
                        'returns': row['returns'],
                        'created_at': row['created_at']
                    })
            
            functions.extend(builtin_funcs)
            
            return {
                'type': 'show_functions',
                'functions': functions,
                'count': len(functions),
                'message': f'Found {len(functions)} function(s)',
                'success': True
            }
            
        except Exception as e:
            return {
                'type': 'show_functions',
                'functions': [],
                'count': 0,
                'message': f'Error: {str(e)}',
                'success': False
            }
    
    def _execute_describe_table(self, table_name: str) -> Dict:
        """Exécute DESCRIBE <table>"""
        try:
            schema = self.storage.get_table_schema(table_name)
            
            if not schema:
                raise SQLExecutionError(f"Table '{table_name}' not found")
            
            # Formater le schéma
            columns_info = []
            for col_name, col_info in schema.get('columns', {}).items():
                column_desc = f"{col_name} {col_info.get('type', 'TEXT')}"
                
                if col_info.get('pk'):
                    column_desc += " PRIMARY KEY"
                if col_info.get('not_null'):
                    column_desc += " NOT NULL"
                if col_info.get('default'):
                    column_desc += f" DEFAULT {col_info['default']}"
                
                columns_info.append({
                    'field': col_name,
                    'type': col_info.get('type', 'TEXT'),
                    'null': not col_info.get('not_null', False),
                    'key': 'PRI' if col_info.get('pk') else '',
                    'default': col_info.get('default'),
                    'extra': 'auto_increment' if col_info.get('auto_increment') else ''
                })
            
            return {
                'type': 'describe',
                'table': table_name,
                'columns': columns_info,
                'indexes': schema.get('indexes', []),
                'foreign_keys': schema.get('foreign_keys', []),
                'count': len(columns_info),
                'message': f'Table structure of {table_name}',
                'success': True
            }
            
        except Exception as e:
            return {
                'type': 'describe',
                'table': table_name,
                'columns': [],
                'message': f'Error: {str(e)}',
                'success': False
            }
    
    def _execute_stats(self) -> Dict:
        """Exécute STATS pour voir les statistiques"""
        try:
            # Récupérer les statistiques du storage
            storage_stats = self.storage.get_stats()
            
            # Statistiques de la base
            db_stats = {
                'queries_executed': self.stats['queries_executed'],
                'queries_cached': self.stats['queries_cached'],
                'cache_hit_ratio': (
                    self.stats['queries_cached'] / self.stats['queries_executed'] 
                    if self.stats['queries_executed'] > 0 else 0
                ),
                'errors': self.stats['errors'],
                'uptime_seconds': (datetime.now() - self.stats['start_time']).total_seconds(),
                'query_cache_size': len(self.query_cache),
                'active_transactions': len(self.active_transactions)
            }
            
            return {
                'type': 'stats',
                'database': db_stats,
                'storage': storage_stats,
                'config': self.config,
                'message': 'Database statistics',
                'success': True
            }
            
        except Exception as e:
            return {
                'type': 'stats',
                'message': f'Error: {str(e)}',
                'success': False
            }
    
    def _execute_vacuum(self) -> Dict:
        """Exécute VACUUM pour optimiser la base"""
        try:
            success = self.storage.vacuum()
            
            return {
                'type': 'vacuum',
                'success': success,
                'message': 'Database optimization completed' if success else 'Optimization failed'
            }
            
        except Exception as e:
            return {
                'type': 'vacuum',
                'success': False,
                'message': f'Error: {str(e)}'
            }
    
    def _execute_backup(self, sql: str) -> Dict:
        """Exécute BACKUP [path]"""
        try:
            # Extraire le chemin optionnel
            parts = sql.split()
            backup_path = parts[1] if len(parts) > 1 else None
            
            # Créer la sauvegarde
            backup_file = self.storage.backup(backup_path)
            
            # Mettre à jour les statistiques
            self.stats['last_backup'] = datetime.now().isoformat()
            
            return {
                'type': 'backup',
                'success': True,
                'backup_file': backup_file,
                'message': f'Backup created: {backup_file}'
            }
            
        except Exception as e:
            return {
                'type': 'backup',
                'success': False,
                'message': f'Error: {str(e)}'
            }
    
    def _execute_help(self) -> Dict:
        """Exécute HELP pour afficher l'aide"""
        help_text = """
GSQL Database Commands:

DATA MANIPULATION:
  SELECT * FROM table [WHERE condition] [LIMIT n]
  INSERT INTO table (col1, col2) VALUES (val1, val2)
  UPDATE table SET col=value [WHERE condition]
  DELETE FROM table [WHERE condition]

DATA DEFINITION:
  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
  DROP TABLE name
  ALTER TABLE name ADD COLUMN col TYPE
  CREATE INDEX idx_name ON table(column)

DATABASE MANAGEMENT:
  SHOW TABLES                    - List all tables
  DESCRIBE table                 - Show table structure
  SHOW FUNCTIONS                 - List all functions
  STATS                          - Show database statistics
  VACUUM                         - Optimize database
  BACKUP [path]                  - Create database backup
  HELP                           - This help message

TRANSACTIONS:
  BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE]
  COMMIT
  ROLLBACK [TO SAVEPOINT name]
  SAVEPOINT name

DOT COMMANDS (compatible SQLite):
  .tables                        - List tables
  .schema [table]                - Show schema
  .stats                         - Show stats
  .help                          - Show help
  .backup [file]                 - Create backup
  .vacuum                        - Optimize database
"""
        
        return {
            'type': 'help',
            'message': help_text,
            'success': True
        }
    
    # ==================== TRANSACTION MANAGEMENT ====================
    
    def begin_transaction(self, isolation_level: str = "DEFERRED") -> Dict:
        """Démarre une nouvelle transaction"""
        try:
            tid = self.storage.begin_transaction(isolation_level)
            
            # Enregistrer la transaction
            self.active_transactions[tid] = {
                'start_time': datetime.now(),
                'isolation': isolation_level,
                'queries': []
            }
            
            self.stats['transactions'] += 1
            
            return {
                'type': 'transaction',
                'tid': tid,
                'isolation': isolation_level,
                'message': f'Transaction {tid} started',
                'success': True
            }
            
        except Exception as e:
            raise TransactionError(f"Failed to begin transaction: {e}")
    
    def commit_transaction(self, tid: int) -> Dict:
        """Valide une transaction"""
        try:
            success = self.storage.commit_transaction(tid)
            
            if tid in self.active_transactions:
                del self.active_transactions[tid]
            
            return {
                'type': 'transaction',
                'tid': tid,
                'message': f'Transaction {tid} committed',
                'success': success
            }
            
        except Exception as e:
            raise TransactionError(f"Failed to commit transaction {tid}: {e}")
    
    def rollback_transaction(self, tid: int, to_savepoint: str = None) -> Dict:
        """Annule une transaction"""
        try:
            success = self.storage.rollback_transaction(tid, to_savepoint)
            
            if not to_savepoint and tid in self.active_transactions:
                del self.active_transactions[tid]
            
            return {
                'type': 'transaction',
                'tid': tid,
                'message': f'Transaction {tid} rolled back' + 
                          (f' to {to_savepoint}' if to_savepoint else ''),
                'success': success
            }
            
        except Exception as e:
            raise TransactionError(f"Failed to rollback transaction {tid}: {e}")
    
    def create_savepoint(self, tid: int, name: str) -> Dict:
        """Crée un savepoint dans une transaction"""
        try:
            success = self.storage.savepoint(tid, name)
            
            if tid in self.active_transactions:
                self.active_transactions[tid]['savepoints'] = \
                    self.active_transactions[tid].get('savepoints', []) + [name]
            
            return {
                'type': 'savepoint',
                'tid': tid,
                'name': name,
                'message': f'Savepoint {name} created in transaction {tid}',
                'success': success
            }
            
        except Exception as e:
            raise TransactionError(f"Failed to create savepoint: {e}")
    
    # ==================== TABLE MANAGEMENT ====================
    
    def create_table(self, table_name: str, columns: Dict) -> Dict:
        """Crée une nouvelle table avec validation"""
        try:
            # Valider le nom de la table
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
                raise SQLSyntaxError(f"Invalid table name: '{table_name}'")
            
            # Construire la requête SQL
            column_defs = []
            for col_name, col_spec in columns.items():
                if isinstance(col_spec, str):
                    col_def = f"{col_name} {col_spec}"
                elif isinstance(col_spec, dict):
                    col_def = f"{col_name} {col_spec.get('type', 'TEXT')}"
                    
                    if col_spec.get('primary_key'):
                        col_def += " PRIMARY KEY"
                    if col_spec.get('auto_increment'):
                        col_def += " AUTOINCREMENT"
                    if col_spec.get('not_null'):
                        col_def += " NOT NULL"
                    if 'default' in col_spec:
                        default_val = col_spec['default']
                        if isinstance(default_val, str) and not default_val.upper() in ['CURRENT_TIMESTAMP', 'NULL']:
                            default_val = f"'{default_val}'"
                        col_def += f" DEFAULT {default_val}"
                    if col_spec.get('unique'):
                        col_def += " UNIQUE"
                    if 'check' in col_spec:
                        col_def += f" CHECK({col_spec['check']})"
                else:
                    raise SQLSyntaxError(f"Invalid column specification for '{col_name}'")
                
                column_defs.append(col_def)
            
            sql = f"CREATE TABLE {table_name} ({', '.join(column_defs)})"
            
            # Exécuter la création
            result = self.execute(sql)
            
            # Mettre à jour le cache de schéma
            self.storage._cache_table_schema(table_name)
            
            return {
                'type': 'create_table',
                'table': table_name,
                'columns': list(columns.keys()),
                'sql': sql,
                'message': f'Table {table_name} created successfully',
                'success': True
            }
            
        except Exception as e:
            raise SQLExecutionError(f"Failed to create table {table_name}: {e}")
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> Dict:
        """Supprime une table"""
        try:
            sql = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{table_name}"
            result = self.execute(sql)
            
            # Nettoyer le cache
            if table_name in self.schema_cache:
                del self.schema_cache[table_name]
            
            return {
                'type': 'drop_table',
                'table': table_name,
                'message': f'Table {table_name} dropped',
                'success': True
            }
            
        except Exception as e:
            raise SQLExecutionError(f"Failed to drop table {table_name}: {e}")
    
    def insert(self, table_name: str, values: Dict, 
               returning: str = None) -> Dict:
        """Insère une ligne dans une table"""
        try:
            # Construire la requête INSERT
            columns = ', '.join(values.keys())
            placeholders = ', '.join(['?' for _ in values])
            
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            if returning:
                sql += f" RETURNING {returning}"
            
            # Exécuter avec les valeurs
            result = self.execute(sql, tuple(values.values()))
            
            return {
                'type': 'insert',
                'table': table_name,
                'lastrowid': result.get('lastrowid'),
                'rows_affected': result.get('rows_affected', 0),
                'message': f'Row inserted into {table_name}',
                'success': True
            }
            
        except Exception as e:
            raise SQLExecutionError(f"Failed to insert into {table_name}: {e}")
    
    def select(self, table_name: str, columns: List[str] = None, 
               where: Dict = None, limit: int = None, 
               offset: int = 0, order_by: str = None) -> Dict:
        """Exécute une requête SELECT avec paramètres simplifiés"""
        try:
            # Construire la requête SELECT
            cols = ', '.join(columns) if columns else '*'
            sql = f"SELECT {cols} FROM {table_name}"
            
            # Ajouter WHERE
            params = []
            if where:
                conditions = []
                for col, val in where.items():
                    if isinstance(val, (list, tuple)):
                        placeholders = ', '.join(['?' for _ in val])
                        conditions.append(f"{col} IN ({placeholders})")
                        params.extend(val)
                    else:
                        conditions.append(f"{col} = ?")
                        params.append(val)
                
                if conditions:
                    sql += f" WHERE {' AND '.join(conditions)}"
            
            # Ajouter ORDER BY
            if order_by:
                sql += f" ORDER BY {order_by}"
            
            # Ajouter LIMIT et OFFSET
            if limit is not None:
                sql += f" LIMIT {limit}"
                if offset:
                    sql += f" OFFSET {offset}"
            
            # Exécuter la requête
            result = self.execute(sql, params if params else None)
            
            return result
            
        except Exception as e:
            raise SQLExecutionError(f"Failed to select from {table_name}: {e}")
    
    # ==================== FUNCTION MANAGEMENT ====================
    
    def register_function(self, name: str, func, num_params: int = -1) -> Dict:
        """Enregistre une fonction Python dans la base"""
        try:
            self.storage.register_function(name, func, num_params)
            
            return {
                'type': 'register_function',
                'name': name,
                'num_params': num_params,
                'message': f'Function {name} registered',
                'success': True
            }
            
        except Exception as e:
            raise FunctionError(f"Failed to register function {name}: {e}")
    
    # ==================== MAINTENANCE ====================
    
    def optimize(self) -> Dict:
        """Optimise la base de données"""
        try:
            # Exécuter plusieurs optimisations
            results = []
            
            # 1. VACUUM
            vacuum_result = self._execute_vacuum()
            results.append(('vacuum', vacuum_result['success']))
            
            # 2. ANALYZE pour les statistiques
            analyze_result = self.execute("ANALYZE")
            results.append(('analyze', analyze_result.get('success', False)))
            
            # 3. Nettoyer le cache
            self.query_cache.clear()
            results.append(('clear_cache', True))
            
            # 4. Nettoyer le buffer pool
            self.storage.buffer_pool.invalidate()
            results.append(('clear_buffer_pool', True))
            
            return {
                'type': 'optimize',
                'operations': results,
                'message': 'Database optimization completed',
                'success': all(r[1] for r in results)
            }
            
        except Exception as e:
            return {
                'type': 'optimize',
                'message': f'Error during optimization: {e}',
                'success': False
            }
    
    def check_health(self) -> Dict:
        """Vérifie la santé de la base de données"""
        try:
            health_checks = []
            
            # 1. Connexion
            health_checks.append(('connection', self.is_open and self.storage.is_connected))
            
            # 2. Tables système
            sys_tables = ['_gsql_metadata', '_gsql_schemas', '_gsql_functions']
            for table in sys_tables:
                result = self.execute(f"SELECT 1 FROM {table} LIMIT 1")
                health_checks.append((f'table_{table}', result.get('success', False)))
            
            # 3. Buffer pool
            bp_stats = self.storage.buffer_pool.get_stats()
            health_checks.append(('buffer_pool', bp_stats['hit_ratio'] > 0.5))
            
            # 4. Espace disque (estimation)
            import shutil
            disk_usage = shutil.disk_usage(self.base_dir)
            free_gb = disk_usage.free / (1024**3)
            health_checks.append(('disk_space', free_gb > 1))  # 1GB minimum
            
            # Calculer le score de santé
            passed = sum(1 for _, status in health_checks if status)
            total = len(health_checks)
            health_score = (passed / total) * 100
            
            status = 'HEALTHY' if health_score >= 80 else 'DEGRADED' if health_score >= 50 else 'CRITICAL'
            
            return {
                'type': 'health_check',
                'status': status,
                'score': round(health_score, 1),
                'checks': health_checks,
                'passed': passed,
                'total': total,
                'message': f'Health check: {status} ({health_score:.1f}%)',
                'success': health_score >= 50
            }
            
        except Exception as e:
            return {
                'type': 'health_check',
                'status': 'ERROR',
                'score': 0,
                'message': f'Health check failed: {e}',
                'success': False
            }
    
    def close(self):
        """Ferme la base de données proprement"""
        with self.lock:
            if self.is_open:
                try:
                    # Sauvegarder les statistiques
                    self._save_stats()
                    
                    # Fermer le storage
                    self.storage.close()
                    
                    # Fermer les transactions actives
                    for tid in list(self.active_transactions.keys()):
                        try:
                            self.rollback_transaction(tid)
                        except:
                            pass
                    
                    self.is_open = False
                    self.initialized = False
                    
                    logger.info("Database closed")
                    
                except Exception as e:
                    logger.error(f"Error closing database: {e}")
    
    def _save_stats(self):
        """Sauvegarde les statistiques d'utilisation"""
        stats_file = self.base_dir / "gsql_stats.json"
        try:
            # Convertir datetime en chaîne pour JSON
            stats_data = {
                'stats': {
                    'queries_executed': self.stats['queries_executed'],
                    'queries_cached': self.stats['queries_cached'],
                    'transactions': self.stats['transactions'],
                    'errors': self.stats['errors'],
                    'start_time': self.stats['start_time'].isoformat() if hasattr(self.stats['start_time'], 'isoformat') else str(self.stats['start_time']),
                    'last_backup': self.stats['last_backup']
                },
                'last_run': datetime.now().isoformat(),
                'version': self.config['version']
            }
            
            with open(stats_file, 'w') as f:
                json.dump(stats_data, f, indent=2, default=str)
                
        except Exception as e:
            logger.warning(f"Failed to save stats: {e}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def __del__(self):
        try:
            self.close()
        except:
            pass


# ==================== FACTORY FUNCTIONS ====================

def create_database(db_path=None, **kwargs) -> Database:
    """
    Crée une nouvelle instance de base de données
    Accepte 'path' comme alias pour 'db_path' pour la compatibilité
    """
    # Si 'path' est fourni dans kwargs, l'utiliser comme db_path
    if 'path' in kwargs:
        db_path = kwargs.pop('path')
    
    return Database(db_path, **kwargs)

def get_default_database() -> Optional[Database]:
    """Récupère l'instance de base de données par défaut"""
    # Cette fonction peut gérer une instance globale
    if not hasattr(get_default_database, '_instance'):
        get_default_database._instance = None
    
    return get_default_database._instance

def set_default_database(db: Database):
    """Définit l'instance de base de données par défaut"""
    get_default_database._instance = db

def connect(db_path: str, **kwargs) -> Database:
    """Connecte à une base de données GSQL (alias pour create_database)"""
    return create_database(db_path, **kwargs)
