#!/usr/bin/env python3
"""
GSQL Database Module - SQLite Backend Only - VERSION CORRIGÉE
Version: 3.1.0 - Tous les bugs fixés
"""

import os
import sqlite3
import json
import logging
import time
import threading
import hashlib
import re
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union
from contextlib import contextmanager

from .storage import SQLiteStorage, create_storage
from .exceptions import (
    GSQLBaseException, SQLSyntaxError, SQLExecutionError,
    ConstraintViolationError, TransactionError, FunctionError,
    BufferPoolError, StorageError
)

logger = logging.getLogger(__name__)

class TransactionContext:
    """Context manager pour les transactions sécurisées"""
    
    def __init__(self, db, isolation_level="DEFERRED"):
        self.db = db
        self.isolation_level = isolation_level
        self.tid = None
    
    def __enter__(self):
        # Débuter la transaction
        result = self.db.begin_transaction(self.isolation_level)
        # CORRECTION : Extraire tid correctement
        if isinstance(result, dict):
            self.tid = result.get('tid')
        else:
            self.tid = result
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # Commit si pas d'exception
            if self.tid is not None:
                self.db.commit_transaction(self.tid)
        else:
            # Rollback en cas d'exception
            if self.tid is not None:
                try:
                    self.db.rollback_transaction(self.tid)
                except:
                    pass
        return False  # Ne pas supprimer l'exception

class PreparedStatement:
    """Requête préparée pour exécution multiple"""
    
    def __init__(self, db, sql):
        self.db = db
        self.sql = sql
        self.params_template = self._extract_params(sql)
    
    def _extract_params(self, sql):
        """Extrait les paramètres de la requête"""
        return sql.count('?')
    
    def execute(self, params=None):
        """Exécute la requête préparée avec les paramètres"""
        if params is not None:
            if isinstance(params, (list, tuple)):
                if self.params_template > 0 and len(params) != self.params_template:
                    raise SQLExecutionError(
                        f"Expected {self.params_template} parameters, got {len(params)}"
                    )
            elif isinstance(params, dict):
                # Pour les paramètres nommés
                pass
        
        return self.db.execute(self.sql, params)

class Database:
    """Base de données SQLite auto-récupérante - Version Corrigée"""
    
    def __init__(self, db_path=None, base_dir="/root/.gsql", 
                 buffer_pool_size=100, enable_wal=True, auto_recovery=True,
                 create_default_tables=True):
        """
        Initialise la base de données SQLite
        
        Args:
            db_path: Chemin de la base (None pour auto)
            base_dir: Répertoire de base pour GSQL
            buffer_pool_size: Taille du buffer pool
            enable_wal: Activer le mode WAL
            auto_recovery: Activer la récupération automatique
            create_default_tables: Créer tables par défaut (users, products, etc.)
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
            'backup_interval': 24 * 3600,
            'max_query_cache': 100,
            'query_timeout': 30,
            'version': '3.1.0',
            'create_default_tables': create_default_tables
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
        self.transaction_counter = 0
        
        # Initialiser les tables système SEULEMENT si demandé
        if create_default_tables:
            self._initialize_database()
        else:
            logger.info(f"GSQL Database initialized without default tables (v{self.config['version']})")
    
    def _save_config(self):
        """Sauvegarde la configuration dans un fichier JSON"""
        config_file = self.base_dir / "gsql_config.json"
        try:
            with open(config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save config: {e}")
    
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
        Exécute une requête SQL sur la base de données - CORRIGÉ
        
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
        
        # CORRECTION : Générer un hash pour le cache
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
                result = self.storage.execute(sql, params)
                
                # CORRECTION : Gérer les erreurs du storage
                if not result.get('success', True):
                    error_msg = result.get('error', 'Unknown error')
                    return {
                        'success': False,
                        'message': error_msg,
                        'error': error_msg,
                        'type': 'error',
                        'execution_time': 0,
                        'timestamp': datetime.now().isoformat()
                    }
                
                # Ajouter des métadonnées
                execution_time = (datetime.now() - start_time).total_seconds()
                result['execution_time'] = round(execution_time, 3)
                result['timestamp'] = datetime.now().isoformat()
                
                # Déterminer le type de requête
                sql_upper = sql.upper().strip()
                if sql_upper.startswith('SELECT'):
                    result['type'] = 'select'
                elif sql_upper.startswith('INSERT'):
                    result['type'] = 'insert'
                elif sql_upper.startswith('UPDATE'):
                    result['type'] = 'update'
                elif sql_upper.startswith('DELETE'):
                    result['type'] = 'delete'
                elif sql_upper.startswith('CREATE'):
                    result['type'] = 'create'
                elif sql_upper.startswith('DROP'):
                    result['type'] = 'drop'
                elif sql_upper.startswith('BEGIN'):
                    result['type'] = 'transaction'
                elif sql_upper.startswith('COMMIT') or sql_upper.startswith('ROLLBACK'):
                    result['type'] = 'transaction'
                elif sql_upper.startswith('SAVEPOINT'):
                    result['type'] = 'savepoint'
                elif sql_upper.startswith('VACUUM'):
                    result['type'] = 'vacuum'
                elif sql_upper.startswith('BACKUP'):
                    result['type'] = 'backup'
                else:
                    result['type'] = 'other'
                
                # Mettre à jour les statistiques
                self.stats['queries_executed'] += 1
                
                # Mettre en cache les requêtes SELECT réussies
                if (use_cache and query_hash and 
                    result.get('success') and 
                    result['type'] == 'select'):
                    
                    # Limiter la taille du cache
                    if len(self.query_cache) >= self.config['max_query_cache']:
                        oldest_key = min(self.query_cache.keys(), 
                                        key=lambda k: self.query_cache[k]['timestamp'])
                        del self.query_cache[oldest_key]
                    
                    self.query_cache[query_hash] = result
                
                # Logger les requêtes lentes
                if execution_time > 1.0:
                    logger.warning(f"Slow query ({execution_time:.2f}s): {sql[:100]}...")
                
                return result
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Query execution error: {e}")
            
            # Tentative de récupération
            if "database is locked" in str(e) and self.config['auto_recovery']:
                logger.info("Database locked, attempting recovery...")
                self._auto_recover()
                
                # Réessayer la requête
                return self.execute(sql, params, use_cache=False)
            
            # Retourner l'erreur proprement
            return {
                'success': False,
                'message': str(e),
                'error': str(e),
                'type': 'error',
                'execution_time': (datetime.now() - start_time).total_seconds(),
                'timestamp': datetime.now().isoformat()
            }
    
    def execute_script(self, sql_script: str) -> List[Dict]:
        """
        Exécute un script SQL multi-requêtes
        """
        results = []
        
        # Supprimer les commentaires /* */ d'abord
        sql_script = re.sub(r'/\*.*?\*/', '', sql_script, flags=re.DOTALL)
        
        # Séparer les requêtes intelligemment
        queries = []
        current_query = []
        in_string = False
        string_char = None
        
        for line in sql_script.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            # Gérer les commentaires de ligne
            if '--' in line and not in_string:
                line = line.split('--')[0]
                if not line:
                    continue
            
            i = 0
            while i < len(line):
                char = line[i]
                
                # Gérer les chaînes
                if char in ("'", '"') and (i == 0 or line[i-1] != '\\'):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None
                
                # Gérer la fin de requête
                elif char == ';' and not in_string:
                    current_query.append(line[:i+1])
                    query_text = ' '.join(current_query).strip()
                    if query_text:
                        queries.append(query_text)
                    current_query = []
                    line = line[i+1:]
                    i = 0
                    continue
                
                i += 1
            
            if line:
                current_query.append(line)
        
        # Ajouter la dernière requête si elle existe
        if current_query:
            query_text = ' '.join(current_query).strip()
            if query_text:
                queries.append(query_text)
        
        # Exécuter les requêtes
        for query in queries:
            try:
                result = self.execute(query)
                results.append(result)
            except Exception as e:
                results.append({
                    'success': False,
                    'message': str(e),
                    'error': str(e),
                    'query': query[:100] + '...' if len(query) > 100 else query
                })
        
        return results
    
    def prepare(self, sql: str) -> PreparedStatement:
        """Prépare une requête SQL pour exécution multiple"""
        return PreparedStatement(self, sql)
    
    def _handle_special_commands(self, sql: str) -> Optional[Dict]:
        """Gère les commandes spéciales GSQL - CORRIGÉ"""
        sql_upper = sql.strip().upper()
        
        # SHOW TABLES
        if sql_upper == "SHOW TABLES" or sql == ".tables":
            return self._execute_show_tables()
        
        # DESCRIBE / SCHEMA
        elif sql_upper.startswith("DESCRIBE ") or sql_upper.startswith("SCHEMA "):
            parts = sql.split()
            table_name = parts[1] if len(parts) > 1 else None
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
        """Exécute SHOW TABLES - CORRIGÉ"""
        try:
            tables = self.storage.get_tables()
            
            formatted_tables = []
            for table in tables:
                formatted_tables.append({
                    'table': table['table_name'],
                    'rows': table.get('row_count', 0),
                    'size_kb': 0,
                    'columns': table.get('columns', []),
                    'last_analyzed': None
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
    
    def _execute_describe_table(self, table_name: str) -> Dict:
        """Exécute DESCRIBE <table> - CORRIGÉ"""
        try:
            schema = self.storage.get_table_schema(table_name)
            
            if not schema:
                return {
                    'type': 'describe',
                    'table': table_name,
                    'columns': [],
                    'message': f"Table '{table_name}' not found",
                    'success': False
                }
            
            # CORRECTION : schema['columns'] est déjà une liste
            columns_info = schema.get('columns', [])
            
            return {
                'type': 'describe',
                'table': table_name,
                'columns': columns_info,  # Directement la liste
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
                'active_transactions': len(self.active_transactions),
                'transactions_total': self.transaction_counter
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
        """Exécute VACUUM pour optimiser la base - CORRIGÉ"""
        try:
            # Exécuter VACUUM via storage
            result = self.storage.execute("VACUUM")
            
            # Vérifier le résultat
            vacuum_success = False
            if isinstance(result, dict):
                vacuum_success = result.get('success', False)
            elif isinstance(result, bool):
                vacuum_success = result
            else:
                # Si c'est autre chose, considérer comme succès
                vacuum_success = True
            
            return {
                'type': 'vacuum',
                'success': vacuum_success,
                'message': 'Database optimization completed' if vacuum_success else 'Optimization failed'
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
GSQL Database Commands (v3.1.0 - Corrigée):

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
  STATS                          - Show database statistics
  VACUUM                         - Optimize database
  BACKUP [path]                  - Create database backup
  HELP                           - This help message

TRANSACTIONS:
  BEGIN TRANSACTION              - Start transaction
  COMMIT                         - Commit transaction
  ROLLBACK                       - Rollback transaction
  SAVEPOINT name                 - Create savepoint
  ROLLBACK TO SAVEPOINT name     - Rollback to savepoint

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
    
    # ==================== TRANSACTION MANAGEMENT CORRIGÉ ====================
    
    def begin_transaction(self, isolation_level: str = "DEFERRED"):
        """Démarre une nouvelle transaction - CORRIGÉ"""
        try:
            # CORRECTION : Utiliser execute() pour que tout soit cohérent
            isolation_sql = {
                "DEFERRED": "BEGIN DEFERRED TRANSACTION",
                "IMMEDIATE": "BEGIN IMMEDIATE TRANSACTION", 
                "EXCLUSIVE": "BEGIN EXCLUSIVE TRANSACTION"
            }.get(isolation_level.upper(), "BEGIN TRANSACTION")
            
            # Exécuter via execute()
            result = self.execute(isolation_sql)
            
            # Générer un TID
            tid = self.transaction_counter
            
            # Enregistrer la transaction
            self.active_transactions[tid] = {
                'start_time': datetime.now(),
                'isolation': isolation_level,
                'queries': [],
                'state': 'ACTIVE'
            }
            
            self.transaction_counter += 1
            
            # CORRECTION : Retourner directement le TID pour compatibilité
            return {
                'type': 'transaction',
                'tid': tid,
                'isolation': isolation_level,
                'message': f'Transaction {tid} started',
                'success': True
            }
            
        except Exception as e:
            raise TransactionError(f"Failed to begin transaction: {e}")
    
    def commit_transaction(self, tid):
        """Valide une transaction - CORRIGÉ"""
        try:
            # CORRECTION : Vérifier si tid est un dict
            if isinstance(tid, dict):
                tid = tid.get('tid', 0)
            
            # Exécuter COMMIT
            result = self.execute("COMMIT")
            
            if tid in self.active_transactions:
                self.active_transactions[tid]['state'] = 'COMMITTED'
                del self.active_transactions[tid]
            
            return {
                'type': 'transaction',
                'tid': tid,
                'message': f'Transaction {tid} committed',
                'success': result.get('success', False)
            }
            
        except Exception as e:
            # Rollback automatique en cas d'erreur
            try:
                self.execute("ROLLBACK")
            except:
                pass
                
            if tid in self.active_transactions:
                del self.active_transactions[tid]
                
            raise TransactionError(f"Failed to commit transaction {tid}: {e}")
    
    def rollback_transaction(self, tid, to_savepoint: str = None):
        """Annule une transaction - CORRIGÉ"""
        try:
            # CORRECTION : Vérifier si tid est un dict
            if isinstance(tid, dict):
                tid = tid.get('tid', 0)
            
            sql = f"ROLLBACK{' TO SAVEPOINT ' + to_savepoint if to_savepoint else ''}"
            result = self.execute(sql)
            
            if not to_savepoint and tid in self.active_transactions:
                self.active_transactions[tid]['state'] = 'ROLLED_BACK'
                del self.active_transactions[tid]
            
            return {
                'type': 'transaction',
                'tid': tid,
                'message': f'Transaction {tid} rolled back' + 
                          (f' to {to_savepoint}' if to_savepoint else ''),
                'success': result.get('success', False)
            }
            
        except Exception as e:
            raise TransactionError(f"Failed to rollback transaction {tid}: {e}")
    
    def create_savepoint(self, tid, name: str):
        """Crée un savepoint dans une transaction - CORRIGÉ"""
        try:
            # CORRECTION : Vérifier si tid est un dict
            if isinstance(tid, dict):
                tid = tid.get('tid', 0)
            
            result = self.execute(f"SAVEPOINT {name}")
            
            if tid in self.active_transactions:
                self.active_transactions[tid]['savepoints'] = \
                    self.active_transactions[tid].get('savepoints', []) + [name]
            
            return {
                'type': 'savepoint',
                'tid': tid,
                'name': name,
                'message': f'Savepoint {name} created in transaction {tid}',
                'success': result.get('success', False)
            }
            
        except Exception as e:
            raise TransactionError(f"Failed to create savepoint: {e}")
    
    def transaction(self, isolation_level="DEFERRED"):
        """Retourne un context manager pour les transactions"""
        return TransactionContext(self, isolation_level)
    
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
    
    # ==================== BACKUP & RESTORE ====================
    
    def backup(self, backup_path: str = None, compress: bool = True) -> Dict:
        """Crée une sauvegarde de la base de données"""
        try:
            if backup_path is None:
                backup_dir = self.base_dir / "backups"
                backup_dir.mkdir(exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = backup_dir / f"backup_{timestamp}.db"
            
            # Utiliser la méthode de backup du storage
            backup_file = self.storage.backup(str(backup_path))
            
            # Compression optionnelle
            if compress:
                import gzip
                with open(backup_file, 'rb') as f_in:
                    with gzip.open(f"{backup_file}.gz", 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(backup_file)
                backup_file = f"{backup_file}.gz"
            
            self.stats['last_backup'] = datetime.now().isoformat()
            
            return {
                'type': 'backup',
                'success': True,
                'backup_file': str(backup_file),
                'size': os.path.getsize(backup_file),
                'message': f'Backup created: {backup_file}'
            }
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return {
                'type': 'backup',
                'success': False,
                'message': f'Backup failed: {e}'
            }
    
    def restore(self, backup_file: str) -> Dict:
        """Restaure la base de données depuis une sauvegarde"""
        try:
            # Vérifier le fichier
            if not os.path.exists(backup_file):
                raise FileNotFoundError(f"Backup file not found: {backup_file}")
            
            # Décompresser si nécessaire
            restore_file = backup_file
            if backup_file.endswith('.gz'):
                import gzip
                restore_file = backup_file[:-3]
                with gzip.open(backup_file, 'rb') as f_in:
                    with open(restore_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            
            # Fermer la base actuelle
            self.close()
            
            # Copier le fichier de backup
            shutil.copy2(restore_file, self.storage.db_path)
            
            # Réouvrir
            self.storage._connect()
            self.is_open = True
            
            # Nettoyer
            if backup_file != restore_file:
                os.remove(restore_file)
            
            return {
                'type': 'restore',
                'success': True,
                'message': f'Database restored from {backup_file}'
            }
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            # Réessayer d'ouvrir la base originale
            try:
                self.storage._connect()
                self.is_open = True
            except:
                pass
            
            return {
                'type': 'restore',
                'success': False,
                'message': f'Restore failed: {e}'
            }
    
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
            health_checks.append(('connection', self.is_open))
            
            # 2. Tables système
            sys_tables = ['_gsql_metadata', '_gsql_schemas', '_gsql_statistics']
            for table in sys_tables:
                try:
                    result = self.execute(f"SELECT 1 FROM {table} LIMIT 1")
                    health_checks.append((f'table_{table}', result.get('success', False)))
                except:
                    health_checks.append((f'table_{table}', False))
            
            # 3. Buffer pool
            try:
                bp_stats = self.storage.buffer_pool.get_stats()
                health_checks.append(('buffer_pool', bp_stats['hit_ratio'] >= 0))
            except:
                health_checks.append(('buffer_pool', False))
            
            # 4. Espace disque
            try:
                import shutil
                disk_usage = shutil.disk_usage(self.base_dir)
                free_gb = disk_usage.free / (1024**3)
                health_checks.append(('disk_space', free_gb > 1))
            except:
                health_checks.append(('disk_space', True))
            
            # Calculer le score de santé
            passed = sum(1 for _, status in health_checks if status)
            total = len(health_checks)
            health_score = (passed / total) * 100 if total > 0 else 0
            
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
                            if self.active_transactions[tid]['state'] == 'ACTIVE':
                                self.execute("ROLLBACK")
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
            stats_data = {
                'stats': {
                    'queries_executed': self.stats['queries_executed'],
                    'queries_cached': self.stats['queries_cached'],
                    'transactions': self.stats['transactions'],
                    'errors': self.stats['errors'],
                    'start_time': self.stats['start_time'].isoformat(),
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
    """Crée une nouvelle instance de base de données"""
    if 'path' in kwargs:
        db_path = kwargs.pop('path')
    
    return Database(db_path, **kwargs)

def get_default_database() -> Optional[Database]:
    """Récupère l'instance de base de données par défaut"""
    if not hasattr(get_default_database, '_instance'):
        get_default_database._instance = None
    
    return get_default_database._instance

def set_default_database(db: Database):
    """Définit l'instance de base de données par défaut"""
    get_default_database._instance = db

def connect(db_path: str, **kwargs) -> Database:
    """Connecte à une base de données GSQL"""
    return create_database(db_path, **kwargs)
