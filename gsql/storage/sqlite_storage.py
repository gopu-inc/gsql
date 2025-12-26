#!/usr/bin/env python3
"""
GSQL Storage Engine Complete - SQLite avec Buffer Pool et Transactions
Version: 3.1.0 - Transactions Fixées
"""

import os
import sqlite3
import json
import logging
import time
import threading
import pickle
import hashlib
from pathlib import Path
from datetime import datetime
from collections import OrderedDict
from typing import Dict, List, Any, Optional, Tuple, Union
import re

from .exceptions import (
    SQLExecutionError, TransactionError, BufferPoolError,
    SQLSyntaxError, ConstraintViolationError, StorageError
)

logger = logging.getLogger(__name__)


# ==================== BUFFER POOL ====================

class BufferPool:
    """Cache de pages en mémoire avec politique LRU"""
    
    def __init__(self, max_pages=100):
        self.max_pages = max_pages
        self.pool = OrderedDict()
        self.lock = threading.RLock()
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'total_size': 0
        }
        self.enabled = True
    
    def get(self, page_id: str):
        """Récupère une page du cache"""
        if not self.enabled:
            return None
        
        with self.lock:
            if page_id in self.pool:
                data, timestamp, count = self.pool.pop(page_id)
                self.pool[page_id] = (data, time.time(), count + 1)
                self.stats['hits'] += 1
                return data.copy() if hasattr(data, 'copy') else data
            self.stats['misses'] += 1
        return None
    
    def put(self, page_id: str, page_data: Any, priority: bool = False):
        """Ajoute une page au cache"""
        if not self.enabled:
            return
        
        with self.lock:
            if page_id in self.pool:
                old_data, _, _ = self.pool.pop(page_id)
                self.stats['total_size'] -= self._get_size(old_data)
            
            while len(self.pool) >= self.max_pages:
                evicted_id, (evicted_data, _, _) = self.pool.popitem(last=False)
                self.stats['evictions'] += 1
                self.stats['total_size'] -= self._get_size(evicted_data)
            
            size = self._get_size(page_data)
            self.pool[page_id] = (
                page_data.copy() if hasattr(page_data, 'copy') else page_data,
                time.time(),
                1
            )
            self.stats['total_size'] += size
            
            if priority:
                self.pool.move_to_end(page_id, last=True)
    
    def _get_size(self, data: Any) -> int:
        """Estime la taille en mémoire"""
        try:
            return len(pickle.dumps(data))
        except:
            return 1000
    
    def invalidate(self, page_id: str = None):
        """Invalide une page ou tout le cache"""
        with self.lock:
            if page_id:
                if page_id in self.pool:
                    data, _, _ = self.pool.pop(page_id)
                    self.stats['total_size'] -= self._get_size(data)
            else:
                self.pool.clear()
                self.stats['total_size'] = 0
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques du cache"""
        with self.lock:
            total_access = self.stats['hits'] + self.stats['misses']
            return {
                'size': len(self.pool),
                'max_size': self.max_pages,
                'hits': self.stats['hits'],
                'misses': self.stats['misses'],
                'evictions': self.stats['evictions'],
                'hit_ratio': self.stats['hits'] / total_access if total_access > 0 else 0,
                'total_size_mb': round(self.stats['total_size'] / (1024 * 1024), 2),
                'enabled': self.enabled
            }
    
    def enable(self, enabled: bool = True):
        """Active/désactive le buffer pool"""
        self.enabled = enabled
        if not enabled:
            self.invalidate()


# ==================== TRANSACTION MANAGER ====================

class TransactionManager:
    """Gestionnaire de transactions SQLite"""
    
    def __init__(self, storage):
        self.storage = storage
        self.active_transactions = {}
        self.transaction_counter = 0
        self.lock = threading.RLock()
        self.transaction_timeout = 30
    
    def begin(self, isolation_level: str = "DEFERRED") -> int:
        """Démarre une nouvelle transaction"""
        with self.lock:
            tid = self.transaction_counter
            self.transaction_counter += 1
            
            try:
                # Vérifier qu'on n'est pas déjà dans une transaction
                cursor = self.storage.conn.cursor()
                cursor.execute("SAVEPOINT gsql_tx_check")
                cursor.execute("RELEASE SAVEPOINT gsql_tx_check")
                
                # Déterminer le type de transaction
                if isolation_level.upper() == "IMMEDIATE":
                    cursor.execute("BEGIN IMMEDIATE TRANSACTION")
                elif isolation_level.upper() == "EXCLUSIVE":
                    cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
                else:
                    cursor.execute("BEGIN DEFERRED TRANSACTION")
                
                self.active_transactions[tid] = {
                    'start_time': time.time(),
                    'isolation': isolation_level,
                    'savepoints': [],
                    'state': 'ACTIVE',
                    'cursor': cursor
                }
                
                logger.info(f"Transaction {tid} started ({isolation_level})")
                return tid
                
            except Exception as e:
                self.transaction_counter -= 1
                raise TransactionError(f"Failed to begin transaction: {e}")
    
    def commit(self, tid: int) -> bool:
        """Valide une transaction"""
        with self.lock:
            if tid not in self.active_transactions:
                raise TransactionError(f"Transaction {tid} not found")
            
            trans = self.active_transactions[tid]
            
            if trans['state'] != 'ACTIVE':
                raise TransactionError(f"Transaction {tid} is already {trans['state']}")
            
            # Vérifier le timeout
            if time.time() - trans['start_time'] > self.transaction_timeout:
                logger.warning(f"Transaction {tid} timeout")
                return self.rollback(tid)
            
            try:
                # COMMIT direct
                cursor = trans.get('cursor')
                if cursor:
                    cursor.execute("COMMIT")
                    cursor.close()
                
                trans['state'] = 'COMMITTED'
                logger.info(f"Transaction {tid} committed")
                
                # Nettoyer
                del self.active_transactions[tid]
                
                # Invalider le buffer pool
                self.storage.buffer_pool.invalidate()
                
                return True
                
            except Exception as e:
                logger.error(f"Commit failed for transaction {tid}: {e}")
                # Rollback automatique
                try:
                    if trans.get('cursor'):
                        trans['cursor'].execute("ROLLBACK")
                        trans['cursor'].close()
                except:
                    pass
                
                trans['state'] = 'FAILED'
                del self.active_transactions[tid]
                raise TransactionError(f"Commit failed: {e}")
    
    def rollback(self, tid: int, to_savepoint: str = None) -> bool:
        """Annule une transaction"""
        with self.lock:
            if tid not in self.active_transactions:
                # Si la transaction n'existe pas, c'est peut-être déjà rollback
                logger.warning(f"Transaction {tid} not found for rollback")
                return True
            
            trans = self.active_transactions[tid]
            
            try:
                cursor = trans.get('cursor')
                
                if to_savepoint:
                    # Vérifier que le savepoint existe
                    if to_savepoint not in trans['savepoints']:
                        raise TransactionError(f"Savepoint '{to_savepoint}' not found")
                    
                    if cursor:
                        cursor.execute(f"ROLLBACK TO SAVEPOINT {to_savepoint}")
                    logger.info(f"Transaction {tid} rolled back to savepoint {to_savepoint}")
                else:
                    # Rollback complet
                    if cursor:
                        cursor.execute("ROLLBACK")
                        cursor.close()
                    
                    trans['state'] = 'ROLLED_BACK'
                    logger.info(f"Transaction {tid} rolled back")
                    
                    # Nettoyer
                    del self.active_transactions[tid]
                
                return True
                
            except Exception as e:
                logger.error(f"Rollback failed for transaction {tid}: {e}")
                trans['state'] = 'CORRUPTED'
                # Essayer de fermer le curseur
                try:
                    if trans.get('cursor'):
                        trans['cursor'].close()
                except:
                    pass
                
                # Supprimer quand même
                if tid in self.active_transactions:
                    del self.active_transactions[tid]
                
                return False
    
    def savepoint(self, tid: int, name: str) -> bool:
        """Crée un savepoint dans la transaction"""
        with self.lock:
            if tid not in self.active_transactions:
                raise TransactionError(f"Transaction {tid} not found")
            
            trans = self.active_transactions[tid]
            
            if trans['state'] != 'ACTIVE':
                raise TransactionError(f"Cannot create savepoint, transaction is {trans['state']}")
            
            try:
                cursor = trans.get('cursor')
                if cursor:
                    cursor.execute(f"SAVEPOINT {name}")
                
                trans['savepoints'].append(name)
                logger.debug(f"Savepoint '{name}' created in transaction {tid}")
                return True
                
            except Exception as e:
                raise TransactionError(f"Savepoint failed: {e}")
    
    def get_active_transactions(self) -> List[Dict]:
        """Liste les transactions actives"""
        with self.lock:
            return [
                {
                    'tid': tid,
                    'age': round(time.time() - data['start_time'], 2),
                    'isolation': data['isolation'],
                    'savepoints': len(data['savepoints']),
                    'state': data['state']
                }
                for tid, data in self.active_transactions.items()
                if data['state'] == 'ACTIVE'
            ]


# ==================== SQLITE STORAGE ====================

class SQLiteStorage:
    """Moteur de stockage SQLite complet"""
    
    VERSION = "3.1.0"
    SCHEMA_VERSION = 2
    
    def __init__(self, db_path=None, base_dir="/root/.gsql", 
                 buffer_pool_size=100, enable_wal=True):
        
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        if db_path is None or db_path == ":memory:":
            self.db_path = self.base_dir / "gsql_data.db"
        else:
            self.db_path = Path(db_path)
            if not self.db_path.is_absolute():
                self.db_path = self.base_dir / self.db_path
        
        self.conn = None
        self.is_connected = False
        self.connection_lock = threading.RLock()
        self.recovery_mode = False
        
        self.buffer_pool = BufferPool(max_pages=buffer_pool_size)
        self.transaction_manager = TransactionManager(self)
        
        self.config = {
            'enable_wal': enable_wal,
            'auto_vacuum': 'FULL',
            'busy_timeout': 10000,
            'cache_size': -2000,
            'journal_mode': 'WAL' if enable_wal else 'DELETE'
        }
        
        self.meta_file = self.base_dir / "storage_meta.json"
        self.recovery_flag = self.base_dir / ".recovery_needed"
        self.backup_dir = self.base_dir / "backups"
        self.backup_dir.mkdir(exist_ok=True)
        
        self.schema_cache = {}
        self.table_cache = {}
        self.query_cache = {}
        
        self._initialize()
    
    def _initialize(self):
        """Initialise la connexion"""
        try:
            self._connect()
            if self._check_integrity():
                logger.info(f"Storage initialized: {self.db_path}")
            else:
                logger.warning("Integrity check failed, attempting recovery...")
                self._recover_database()
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            self._create_new_database()
    
    def _create_new_database(self):
        """Crée une nouvelle base de données vierge"""
        try:
            if self.db_path.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                old_path = self.db_path.with_suffix(f".corrupted_{timestamp}.db")
                os.rename(str(self.db_path), str(old_path))
                logger.warning(f"Moved corrupted database to {old_path}")
            
            self._connect()
            logger.info("Created new database")
        except Exception as e:
            logger.error(f"Failed to create new database: {e}")
            raise
    
    def _connect(self, retries=3):
        """Établit la connexion SQLite"""
        for attempt in range(retries):
            try:
                with self.connection_lock:
                    if self.conn:
                        try:
                            self.conn.close()
                        except:
                            pass
                    
                    self.conn = sqlite3.connect(
                        str(self.db_path),
                        timeout=self.config['busy_timeout'] / 1000,
                        check_same_thread=False,
                        isolation_level=None
                    )
                    
                    # Configuration de base
                    self.conn.execute("PRAGMA foreign_keys = ON")
                    
                    # Configuration WAL
                    if self.config['enable_wal']:
                        self.conn.execute(f"PRAGMA journal_mode = {self.config['journal_mode']}")
                    
                    self.conn.execute(f"PRAGMA auto_vacuum = {self.config['auto_vacuum']}")
                    self.conn.execute(f"PRAGMA cache_size = {self.config['cache_size']}")
                    self.conn.execute("PRAGMA synchronous = NORMAL")
                    self.conn.execute("PRAGMA temp_store = MEMORY")
                    
                    self.is_connected = True
                    logger.debug(f"Connected to SQLite (attempt {attempt+1})")
                    return True
                    
            except sqlite3.Error as e:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"Connection failed: {e}, retrying in {wait}s")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed to connect after {retries} attempts")
                    self.is_connected = False
                    raise StorageError(f"Could not connect to database: {e}")
            except Exception as e:
                logger.error(f"Unexpected connection error: {e}")
                self.is_connected = False
                raise
    
    def _configure_connection(self):
        """Configure la connexion"""
        if not self.conn:
            return
        
        self._create_system_tables()
    
    def _create_system_tables(self):
        """Crée les tables système"""
        system_tables = [
            """CREATE TABLE IF NOT EXISTS _gsql_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            """CREATE TABLE IF NOT EXISTS _gsql_schemas (
                table_name TEXT PRIMARY KEY,
                schema_json TEXT NOT NULL,
                row_count INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            """CREATE TABLE IF NOT EXISTS _gsql_transactions_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tid INTEGER NOT NULL,
                operation TEXT NOT NULL,
                table_name TEXT,
                data_before TEXT,
                data_after TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            """CREATE TABLE IF NOT EXISTS _gsql_statistics (
                metric TEXT PRIMARY KEY,
                value INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        ]
        
        try:
            cursor = self.conn.cursor()
            for table_sql in system_tables:
                cursor.execute(table_sql)
            self.conn.commit()
        except Exception as e:
            logger.warning(f"Failed to create some system tables: {e}")
    
    def _check_integrity(self) -> bool:
        """Vérifie l'intégrité de la base"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            return result[0] == "ok"
        except Exception as e:
            logger.error(f"Integrity check failed: {e}")
            return False
    
    def _recover_database(self):
        """Tente une récupération"""
        logger.info("Starting database recovery...")
        self.recovery_mode = True
        
        try:
            if self._restore_from_backup():
                logger.info("Restored from backup")
                return True
            
            if self._attempt_sqlite_recovery():
                logger.info("SQLite recovery succeeded")
                return True
            
            logger.warning("Full database reset required")
            self._reset_database()
            return True
            
        except Exception as e:
            logger.error(f"Recovery failed: {e}")
            raise
        finally:
            self.recovery_mode = False
    
    def _restore_from_backup(self) -> bool:
        """Restaure depuis backup"""
        try:
            backup_files = sorted(self.backup_dir.glob("backup_*.db"))
            if not backup_files:
                return False
            
            latest_backup = backup_files[-1]
            
            temp_conn = sqlite3.connect(str(latest_backup))
            temp_cursor = temp_conn.cursor()
            temp_cursor.execute("PRAGMA integrity_check")
            integrity_result = temp_cursor.fetchone()
            temp_conn.close()
            
            if integrity_result and integrity_result[0] == "ok":
                if self.conn:
                    self.conn.close()
                os.replace(str(latest_backup), str(self.db_path))
                self._connect()
                return True
                
        except Exception as e:
            logger.error(f"Backup restoration failed: {e}")
        
        return False
    
    def _attempt_sqlite_recovery(self) -> bool:
        """Tente une récupération SQLite"""
        try:
            if not self.conn:
                return False
            
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA wal_checkpoint(FULL)")
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            
            if result and result[0] == "ok":
                cursor.execute("VACUUM")
                self.conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"SQLite recovery failed: {e}")
        
        return False
    
    def _reset_database(self):
        """Réinitialise complètement"""
        try:
            if self.conn:
                self.conn.close()
            
            if self.db_path.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                corrupted_path = self.db_path.with_suffix(f".corrupted_{timestamp}.db")
                os.rename(str(self.db_path), str(corrupted_path))
            
            self._connect()
            self._create_system_tables()
            logger.warning(f"Database reset completed. Old file: {corrupted_path}")
            
        except Exception as e:
            logger.error(f"Database reset failed: {e}")
            raise
    
    def execute(self, query: str, params: Tuple = None) -> Dict:
        """Exécute une requête SQL"""
        if not self.is_connected:
            self._connect()
        
        if params is None:
            params = ()
        
        try:
            query = query.strip()
            if not query:
                return {'success': False, 'error': 'Empty query'}
            
            # Convertir les paramètres en tuple
            if isinstance(params, dict):
                params = tuple(params.values())
            elif isinstance(params, list):
                params = tuple(params)
            elif not isinstance(params, tuple):
                params = (params,) if params else ()
            
            start_time = time.time()
            cursor = self.conn.cursor()
            
            # Exécuter la requête
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            execution_time = time.time() - start_time
            
            # Détecter le type de requête
            query_upper = query.lstrip().upper()
            
            # Construire le résultat
            result = {
                'success': True, 
                'execution_time': round(execution_time, 4),
                'timestamp': datetime.now().isoformat()
            }
            
            if query_upper.startswith("SELECT"):
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                
                formatted_rows = []
                for row in rows:
                    if len(column_names) == len(row):
                        formatted_rows.append(dict(zip(column_names, row)))
                    else:
                        formatted_rows.append(row)
                
                result.update({
                    'type': 'select',
                    'count': len(rows),
                    'columns': column_names,
                    'rows': formatted_rows
                })
                
            elif query_upper.startswith("INSERT"):
                lastrowid = cursor.lastrowid
                rowcount = cursor.rowcount
                
                result.update({
                    'type': 'insert',
                    'lastrowid': lastrowid,
                    'last_insert_id': lastrowid,
                    'rows_affected': rowcount
                })
                
            elif query_upper.startswith("UPDATE"):
                result.update({
                    'type': 'update',
                    'rows_affected': cursor.rowcount
                })
                
            elif query_upper.startswith("DELETE"):
                result.update({
                    'type': 'delete',
                    'rows_affected': cursor.rowcount
                })
                
            elif query_upper.startswith("CREATE"):
                result.update({
                    'type': 'create',
                    'rows_affected': cursor.rowcount
                })
                
            elif query_upper.startswith("DROP"):
                result.update({
                    'type': 'drop',
                    'rows_affected': cursor.rowcount
                })
                
            elif query_upper.startswith("BEGIN"):
                result.update({
                    'type': 'transaction',
                    'message': 'Transaction started'
                })
                
            elif query_upper.startswith("COMMIT"):
                result.update({
                    'type': 'transaction',
                    'message': 'Transaction committed'
                })
                
            elif query_upper.startswith("ROLLBACK"):
                result.update({
                    'type': 'transaction',
                    'message': 'Transaction rolled back'
                })
                
            elif query_upper.startswith("SAVEPOINT"):
                result.update({
                    'type': 'savepoint',
                    'message': 'Savepoint created'
                })
                
            else:
                result.update({
                    'type': 'other',
                    'rows_affected': cursor.rowcount
                })
            
            # Commit seulement si pas de transaction active
            active_tx = self.transaction_manager.get_active_transactions()
            if not active_tx:
                self.conn.commit()
            
            # Mettre à jour les statistiques
            self._update_statistics(query_upper.split()[0] if query_upper else "OTHER", execution_time)
            
            return result
            
        except sqlite3.Error as e:
            error_msg = str(e)
            logger.error(f"SQL execution error: {error_msg}")
            
            # Essayer de rollback
            try:
                self.conn.rollback()
            except:
                pass
            
            # Messages d'erreur clairs
            if "database is locked" in error_msg:
                return {'success': False, 'error': 'Database is locked, please try again'}
            elif "no such table" in error_msg:
                return {'success': False, 'error': 'Table not found'}
            elif "no such savepoint" in error_msg:
                return {'success': False, 'error': 'Savepoint not found'}
            elif "cannot commit - no transaction is active" in error_msg:
                return {'success': False, 'error': 'No active transaction to commit'}
            elif "cannot rollback - no transaction is active" in error_msg:
                return {'success': False, 'error': 'No active transaction to rollback'}
            elif "UNIQUE constraint failed" in error_msg:
                return {'success': False, 'error': 'Duplicate entry'}
            elif "FOREIGN KEY constraint failed" in error_msg:
                return {'success': False, 'error': 'Foreign key violation'}
            else:
                return {'success': False, 'error': f'SQL error: {error_msg}'}
                
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return {'success': False, 'error': f'Unexpected error: {e}'}
    
    def _update_statistics(self, query_type: str, execution_time: float):
        """Met à jour les statistiques"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """INSERT OR IGNORE INTO _gsql_statistics (metric, value) 
                   VALUES (?, 0)""",
                (f"query_count_{query_type}",)
            )
            cursor.execute(
                """UPDATE _gsql_statistics 
                   SET value = value + 1,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE metric = ?""",
                (f"query_count_{query_type}",)
            )
            self.conn.commit()
        except:
            pass
    
    def get_table_schema(self, table: str) -> Dict:
        """Récupère le schéma d'une table"""
        try:
            cache_key = f"schema_{table}"
            if cache_key in self.schema_cache:
                return self.schema_cache[cache_key]
            
            cursor = self.conn.cursor()
            cursor.execute(f'PRAGMA table_info("{table}")')
            columns_data = cursor.fetchall()
            
            if not columns_data:
                return None
            
            columns = []
            for col in columns_data:
                columns.append({
                    'field': col[1],
                    'type': col[2],
                    'null': col[3] == 0,
                    'key': 'PRI' if col[5] > 0 else '',
                    'default': col[4],
                    'extra': 'AUTOINCREMENT' if col[5] == 1 and 'INT' in col[2].upper() else ''
                })
            
            schema = {
                'table': table,
                'columns': columns,
                'row_count': self._get_table_row_count(table)
            }
            
            self.schema_cache[cache_key] = schema
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema for {table}: {e}")
            return None
    
    def _get_table_row_count(self, table: str) -> int:
        """Récupère le nombre de lignes"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            return cursor.fetchone()[0]
        except:
            return 0
    
    def get_tables(self) -> List[Dict]:
        """Récupère la liste des tables"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT name, type, sql 
                FROM sqlite_master 
                WHERE type IN ('table', 'view') 
                AND name NOT LIKE 'sqlite_%'
                AND name NOT LIKE '_gsql_%'
                ORDER BY type, name
            """)
            
            tables = []
            for row in cursor.fetchall():
                table_name = row[0]
                try:
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    row_count = cursor.fetchone()[0]
                except:
                    row_count = 0
                
                tables.append({
                    'table_name': table_name,
                    'type': row[1],
                    'row_count': row_count,
                    'sql': row[2]
                })
            
            return tables
            
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            return []
    
    # ============ API TRANSACTIONS ============
    
    def begin_transaction(self, isolation_level: str = "DEFERRED") -> int:
        """Démarre une transaction"""
        return self.transaction_manager.begin(isolation_level)
    
    def commit_transaction(self, tid: int) -> bool:
        """Valide une transaction"""
        return self.transaction_manager.commit(tid)
    
    def rollback_transaction(self, tid: int, to_savepoint: str = None) -> bool:
        """Annule une transaction"""
        return self.transaction_manager.rollback(tid, to_savepoint)
    
    def create_savepoint(self, tid: int, name: str) -> bool:
        """Crée un savepoint"""
        return self.transaction_manager.savepoint(tid, name)
    
    # ============ BACKUP ET MAINTENANCE ============
    
    def backup(self, backup_name: str = None) -> str:
        """Crée une sauvegarde"""
        if backup_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"backup_{timestamp}.db"
        
        backup_path = self.backup_dir / backup_name
        
        try:
            import shutil
            shutil.copy2(str(self.db_path), str(backup_path))
            logger.info(f"Backup created: {backup_path}")
            return str(backup_path)
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            raise StorageError(f"Backup failed: {e}")
    
    def vacuum(self) -> bool:
        """Optimise la base"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("VACUUM")
            self.conn.commit()
            self.buffer_pool.invalidate()
            self.schema_cache.clear()
            logger.info("Database vacuum completed")
            return True
        except Exception as e:
            logger.error(f"Vacuum failed: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Récupère les statistiques"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE 'sqlite_%'
            """)
            table_count = cursor.fetchone()[0]
            
            try:
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                size_bytes = page_count * page_size
            except:
                size_bytes = 0
            
            custom_stats = {}
            try:
                cursor.execute("SELECT metric, value FROM _gsql_statistics")
                for row in cursor.fetchall():
                    custom_stats[row[0]] = row[1]
            except:
                pass
            
            return {
                'database': {
                    'path': str(self.db_path),
                    'tables': table_count,
                    'size_mb': round(size_bytes / (1024 * 1024), 2),
                    'connection_status': self.is_connected
                },
                'performance': self.buffer_pool.get_stats(),
                'transactions': {
                    'active': len(self.transaction_manager.get_active_transactions()),
                    'total': self.transaction_manager.transaction_counter
                },
                'cache': {
                    'schema': len(self.schema_cache),
                    'tables': len(self.table_cache)
                },
                'statistics': custom_stats
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Ferme la connexion"""
        try:
            with self.connection_lock:
                # Fermer les transactions actives
                active_tx = self.transaction_manager.get_active_transactions()
                if active_tx:
                    logger.warning(f"Closing storage with {len(active_tx)} active transactions")
                    for tx in active_tx:
                        try:
                            self.transaction_manager.rollback(tx['tid'])
                        except:
                            pass
                
                if self.conn:
                    self.conn.close()
                    self.conn = None
                    self.is_connected = False
                    
                    self.buffer_pool.invalidate()
                    self.schema_cache.clear()
                    self.table_cache.clear()
                
                logger.info("Storage closed")
        except Exception as e:
            logger.error(f"Error closing storage: {e}")
    
    def __enter__(self):
        """Support du contexte manager"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fermeture automatique"""
        self.close()


# ==================== API PUBLIQUE ====================

def create_storage(db_path=None, **kwargs) -> SQLiteStorage:
    """Crée une instance de stockage SQLite"""
    return SQLiteStorage(db_path, **kwargs)


def quick_query(query: str, db_path=None) -> Dict:
    """Exécute une requête rapide"""
    storage = SQLiteStorage(db_path)
    try:
        result = storage.execute(query)
        return result
    finally:
        storage.close()
