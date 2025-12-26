#!/usr/bin/env python3
"""
GSQL Storage Engine Complete - SQLite avec Buffer Pool et Transactions ACID
Version: 3.2.0 - Transactions Complètes et Stables
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
from collections import OrderedDict, defaultdict
from typing import Dict, List, Any, Optional, Tuple, Union, Set
import re
import traceback
import shutil

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


# ==================== BASE STORAGE CLASS ====================

class Storage:
    """Classe de base pour le stockage"""
    
    def __init__(self, db_path=None, **kwargs):
        self.db_path = db_path
        self.connection = None
        self.base_dir = Path(kwargs.get('base_dir', '/root/.gsql'))
        self.buffer_pool = BufferPool(max_pages=kwargs.get('buffer_pool_size', 100))
        
    def open(self):
        """Ouvre la connexion au stockage"""
        raise NotImplementedError
        
    def close(self):
        """Ferme la connexion"""
        raise NotImplementedError
        
    def is_new_database(self):
        """Vérifie si la base de données est nouvelle"""
        raise NotImplementedError
        
    def get_connection(self):
        """Retourne la connexion"""
        return self.connection
        
    def configure_pragmas(self, pragmas):
        """Configure les pragmas SQLite"""
        if not self.connection:
            return
        
        try:
            cursor = self.connection.cursor()
            for pragma, value in pragmas.items():
                cursor.execute(f"PRAGMA {pragma} = {value}")
            self.connection.commit()
            logger.debug(f"SQLite pragmas configured: {pragmas}")
        except Exception as e:
            logger.warning(f"Could not configure SQLite pragmas: {e}")


# ==================== TRANSACTION MANAGER CORRIGÉ ====================

class TransactionManager:
    """Gestionnaire de transactions SQLite corrigé et stable"""
    
    def __init__(self, storage):
        self.storage = storage
        self.active_transactions = {}
        self.transaction_counter = 0
        self.lock = threading.RLock()
        self.transaction_timeout = 30
        
        # Pour le débogage et le suivi
        self.transaction_log = []
    
    def begin(self, isolation_level: str = "DEFERRED") -> Dict:
        """Démarre une nouvelle transaction - VERSION CORRIGÉE"""
        with self.lock:
            tid = self.transaction_counter
            self.transaction_counter += 1
            
            try:
                # Créer un nouveau curseur pour cette transaction
                cursor = self.storage.connection.cursor()
                
                # Déterminer le type de transaction
                sql = "BEGIN "
                if isolation_level.upper() == "IMMEDIATE":
                    sql += "IMMEDIATE "
                elif isolation_level.upper() == "EXCLUSIVE":
                    sql += "EXCLUSIVE "
                else:
                    sql += "DEFERRED "
                sql += "TRANSACTION"
                
                cursor.execute(sql)
                
                # Stocker les informations de la transaction
                self.active_transactions[tid] = {
                    'start_time': time.time(),
                    'isolation': isolation_level,
                    'savepoints': [],
                    'state': 'ACTIVE',
                    'cursor': cursor,
                    'operations': 0
                }
                
                # Journaliser
                self.transaction_log.append({
                    'time': datetime.now().isoformat(),
                    'tid': tid,
                    'action': 'BEGIN',
                    'isolation': isolation_level
                })
                
                logger.info(f"Transaction {tid} started ({isolation_level})")
                return {'success': True, 'tid': tid, 'message': f'Transaction {tid} started'}
                
            except Exception as e:
                self.transaction_counter -= 1
                error_msg = f"Failed to begin transaction: {e}"
                logger.error(error_msg)
                return {'success': False, 'error': error_msg}
    
    def _execute_in_transaction(self, tid: int, sql: str, params: Tuple = None) -> Dict:
        """Exécute une requête dans le contexte d'une transaction spécifique"""
        if tid not in self.active_transactions:
            return {'success': False, 'error': f'Transaction {tid} not found'}
        
        trans = self.active_transactions[tid]
        
        if trans['state'] != 'ACTIVE':
            return {'success': False, 'error': f'Transaction {tid} is {trans["state"]}'}
        
        try:
            cursor = trans['cursor']
            
            # Exécuter la requête
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            # Mettre à jour le compteur d'opérations
            trans['operations'] += 1
            
            # Construire le résultat
            result = {'success': True}
            
            # Détecter le type de requête
            sql_upper = sql.lstrip().upper()
            
            if sql_upper.startswith("SELECT"):
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
                
            elif sql_upper.startswith("INSERT"):
                result.update({
                    'type': 'insert',
                    'lastrowid': cursor.lastrowid,
                    'rows_affected': cursor.rowcount
                })
                
            elif sql_upper.startswith("UPDATE"):
                result.update({
                    'type': 'update',
                    'rows_affected': cursor.rowcount
                })
                
            elif sql_upper.startswith("DELETE"):
                result.update({
                    'type': 'delete',
                    'rows_affected': cursor.rowcount
                })
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Transaction {tid} execution error: {error_msg}")
            return {'success': False, 'error': error_msg, 'tid': tid}
    
    def commit(self, tid: int) -> Dict:
        """Valide une transaction - VERSION CORRIGÉE"""
        with self.lock:
            if tid not in self.active_transactions:
                return {
                    'success': False, 
                    'error': f'Transaction {tid} not found or already completed',
                    'tid': tid
                }
            
            trans = self.active_transactions[tid]
            
            if trans['state'] != 'ACTIVE':
                return {
                    'success': False,
                    'error': f'Transaction {tid} is already {trans["state"]}',
                    'tid': tid,
                    'state': trans['state']
                }
            
            # Vérifier le timeout
            elapsed = time.time() - trans['start_time']
            if elapsed > self.transaction_timeout:
                logger.warning(f"Transaction {tid} timeout after {elapsed:.1f}s")
                # Rollback automatique
                rollback_result = self._rollback_internal(tid)
                return {
                    'success': False,
                    'error': f'Transaction {tid} timeout after {elapsed:.1f}s',
                    'tid': tid,
                    'auto_rollback': rollback_result.get('success', False)
                }
            
            try:
                cursor = trans['cursor']
                cursor.execute("COMMIT")
                cursor.close()
                
                trans['state'] = 'COMMITTED'
                logger.info(f"Transaction {tid} committed successfully")
                
                # Journaliser
                self.transaction_log.append({
                    'time': datetime.now().isoformat(),
                    'tid': tid,
                    'action': 'COMMIT',
                    'operations': trans['operations'],
                    'duration': elapsed
                })
                
                # Nettoyer
                del self.active_transactions[tid]
                
                # Invalider le buffer pool
                self.storage.buffer_pool.invalidate()
                
                return {
                    'success': True,
                    'tid': tid,
                    'message': f'Transaction {tid} committed successfully',
                    'operations': trans['operations'],
                    'duration': elapsed
                }
                
            except Exception as e:
                logger.error(f"Commit failed for transaction {tid}: {e}")
                
                # Rollback automatique en cas d'échec
                try:
                    trans['cursor'].execute("ROLLBACK")
                    trans['cursor'].close()
                except:
                    pass
                
                trans['state'] = 'FAILED'
                del self.active_transactions[tid]
                
                return {
                    'success': False,
                    'error': f'Commit failed: {e}',
                    'tid': tid
                }
    
    def _rollback_internal(self, tid: int, to_savepoint: str = None) -> Dict:
        """Méthode interne pour rollback"""
        if tid not in self.active_transactions:
            return {'success': False, 'error': f'Transaction {tid} not found'}
        
        trans = self.active_transactions[tid]
        
        try:
            cursor = trans['cursor']
            
            if to_savepoint:
                if to_savepoint not in trans['savepoints']:
                    return {'success': False, 'error': f'Savepoint {to_savepoint} not found'}
                
                cursor.execute(f"ROLLBACK TO SAVEPOINT {to_savepoint}")
                
                # Retirer les savepoints après celui-ci
                sp_index = trans['savepoints'].index(to_savepoint)
                trans['savepoints'] = trans['savepoints'][:sp_index + 1]
                
                message = f'Rolled back to savepoint {to_savepoint}'
            else:
                cursor.execute("ROLLBACK")
                cursor.close()
                
                trans['state'] = 'ROLLED_BACK'
                message = 'Transaction rolled back completely'
                
                # Nettoyer
                del self.active_transactions[tid]
            
            # Journaliser
            self.transaction_log.append({
                'time': datetime.now().isoformat(),
                'tid': tid,
                'action': 'ROLLBACK',
                'savepoint': to_savepoint,
                'operations': trans.get('operations', 0)
            })
            
            return {'success': True, 'tid': tid, 'message': message}
            
        except Exception as e:
            logger.error(f"Rollback failed for transaction {tid}: {e}")
            
            # Nettoyer même en cas d'erreur
            try:
                if trans.get('cursor'):
                    trans['cursor'].close()
            except:
                pass
            
            if tid in self.active_transactions:
                del self.active_transactions[tid]
            
            return {'success': False, 'error': f'Rollback failed: {e}', 'tid': tid}
    
    def rollback(self, tid: int, to_savepoint: str = None) -> Dict:
        """Annule une transaction - VERSION CORRIGÉE"""
        with self.lock:
            return self._rollback_internal(tid, to_savepoint)
    
    def savepoint(self, tid: int, name: str) -> Dict:
        """Crée un savepoint dans la transaction - VERSION CORRIGÉE"""
        with self.lock:
            if tid not in self.active_transactions:
                return {'success': False, 'error': f'Transaction {tid} not found'}
            
            trans = self.active_transactions[tid]
            
            if trans['state'] != 'ACTIVE':
                return {'success': False, 'error': f'Cannot create savepoint, transaction is {trans["state"]}'}
            
            try:
                cursor = trans['cursor']
                cursor.execute(f"SAVEPOINT {name}")
                
                trans['savepoints'].append(name)
                logger.debug(f"Savepoint '{name}' created in transaction {tid}")
                
                return {
                    'success': True,
                    'tid': tid,
                    'savepoint': name,
                    'message': f"Savepoint '{name}' created"
                }
                
            except Exception as e:
                error_msg = f"Savepoint failed: {e}"
                logger.error(error_msg)
                return {'success': False, 'error': error_msg}
    
    def get_active_transactions(self) -> List[Dict]:
        """Liste les transactions actives"""
        with self.lock:
            return [
                {
                    'tid': tid,
                    'age': round(time.time() - data['start_time'], 2),
                    'isolation': data['isolation'],
                    'savepoints': len(data['savepoints']),
                    'state': data['state'],
                    'operations': data['operations']
                }
                for tid, data in self.active_transactions.items()
                if data['state'] == 'ACTIVE'
            ]
    
    def get_transaction_log(self, limit: int = 100) -> List[Dict]:
        """Retourne le journal des transactions"""
        with self.lock:
            return self.transaction_log[-limit:] if self.transaction_log else []


# ==================== SQLITE STORAGE COMPLET ====================

class SQLiteStorage(Storage):
    """Moteur de stockage SQLite avec transactions ACID stables"""
    
    VERSION = "3.2.0"
    SCHEMA_VERSION = 3
    
    def __init__(self, db_path=None, base_dir="/root/.gsql", 
                 buffer_pool_size=100, enable_wal=True, auto_recovery=True):
        
        super().__init__(db_path, base_dir=base_dir, buffer_pool_size=buffer_pool_size)
        
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        if db_path is None or db_path == ":memory:":
            self.db_path = self.base_dir / "gsql_data.db"
        else:
            self.db_path = Path(db_path)
            if not self.db_path.is_absolute():
                self.db_path = self.base_dir / self.db_path
        
        self.connection = None
        self.is_connected = False
        self.connection_lock = threading.RLock()
        self.recovery_mode = False
        
        self.transaction_manager = TransactionManager(self)
        
        self.config = {
            'enable_wal': enable_wal,
            'auto_vacuum': 'FULL',
            'busy_timeout': 10000,
            'cache_size': -2000,
            'journal_mode': 'WAL' if enable_wal else 'DELETE',
            'auto_recovery': auto_recovery
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
            self.open()
            if self._check_integrity():
                logger.info(f"Storage initialized: {self.db_path}")
            else:
                if self.config['auto_recovery']:
                    logger.warning("Integrity check failed, attempting recovery...")
                    self._recover_database()
                else:
                    raise StorageError("Database integrity check failed")
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            self._create_new_database()
    
    def open(self):
        """Établit la connexion SQLite"""
        return self._connect()
    
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
                    if self.connection:
                        try:
                            self.connection.close()
                        except:
                            pass
                    
                    self.connection = sqlite3.connect(
                        str(self.db_path),
                        timeout=self.config['busy_timeout'] / 1000,
                        check_same_thread=False,
                        isolation_level=None  # Auto-commit par défaut
                    )
                    
                    # Configuration de base
                    self.connection.execute("PRAGMA foreign_keys = ON")
                    
                    # Configuration WAL
                    if self.config['enable_wal']:
                        self.connection.execute(f"PRAGMA journal_mode = {self.config['journal_mode']}")
                    
                    self.connection.execute(f"PRAGMA auto_vacuum = {self.config['auto_vacuum']}")
                    self.connection.execute(f"PRAGMA cache_size = {self.config['cache_size']}")
                    self.connection.execute("PRAGMA synchronous = NORMAL")
                    self.connection.execute("PRAGMA temp_store = MEMORY")
                    
                    self.is_connected = True
                    
                    # Créer les tables système
                    self._create_system_tables()
                    
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
            cursor = self.connection.cursor()
            for table_sql in system_tables:
                cursor.execute(table_sql)
            self.connection.commit()
        except Exception as e:
            logger.warning(f"Failed to create some system tables: {e}")
    
    def is_new_database(self):
        """
        Vérifie si la base de données est nouvelle (vide).
        """
        try:
            cursor = self.connection.cursor()
            
            # Vérifier si la table 'sqlite_master' a des tables utilisateur
            cursor.execute("""
                SELECT COUNT(*) FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)
            user_table_count = cursor.fetchone()[0]
            
            # Vérifier si les tables système de GSQL existent
            cursor.execute("""
                SELECT COUNT(*) FROM sqlite_master 
                WHERE type='table' AND name IN ('_gsql_metadata', '_gsql_schemas')
            """)
            system_table_count = cursor.fetchone()[0]
            
            return user_table_count == 0 or system_table_count == 0
            
        except Exception as e:
            # Si erreur, considérer comme nouvelle base
            logger.warning(f"Error checking if database is new: {e}")
            return True
    
    def get_connection(self):
        """Retourne la connexion SQLite"""
        return self.connection
    
    def _check_integrity(self) -> bool:
        """Vérifie l'intégrité de la base"""
        try:
            cursor = self.connection.cursor()
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
                if self.connection:
                    self.connection.close()
                os.replace(str(latest_backup), str(self.db_path))
                self._connect()
                return True
                
        except Exception as e:
            logger.error(f"Backup restoration failed: {e}")
        
        return False
    
    def _attempt_sqlite_recovery(self) -> bool:
        """Tente une récupération SQLite"""
        try:
            if not self.connection:
                return False
            
            cursor = self.connection.cursor()
            cursor.execute("PRAGMA wal_checkpoint(FULL)")
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            
            if result and result[0] == "ok":
                cursor.execute("VACUUM")
                self.connection.commit()
                return True
                
        except Exception as e:
            logger.error(f"SQLite recovery failed: {e}")
        
        return False
    
    def _reset_database(self):
        """Réinitialise complètement"""
        try:
            if self.connection:
                self.connection.close()
            
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
    
    def execute(self, query: str, params: Tuple = None, tid: int = None) -> Dict:
        """Exécute une requête SQL - VERSION CORRIGÉE"""
        if not self.is_connected:
            self._connect()
        
        if params is None:
            params = ()
        
        # Convertir les paramètres en tuple
        if isinstance(params, dict):
            params = tuple(params.values())
        elif isinstance(params, list):
            params = tuple(params)
        elif not isinstance(params, tuple):
            params = (params,) if params else ()
        
        try:
            query = query.strip()
            if not query:
                return {'success': False, 'error': 'Empty query'}
            
            start_time = time.time()
            
            # Vérifier si on exécute dans une transaction
            if tid is not None:
                # Utiliser le gestionnaire de transactions
                result = self.transaction_manager._execute_in_transaction(tid, query, params)
                if result.get('success'):
                    result['execution_time'] = round(time.time() - start_time, 4)
                    result['timestamp'] = datetime.now().isoformat()
                return result
            
            # Exécution normale (hors transaction)
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            execution_time = time.time() - start_time
            
            # Construire le résultat
            result = {
                'success': True, 
                'execution_time': round(execution_time, 4),
                'timestamp': datetime.now().isoformat()
            }
            
            # Détecter le type de requête
            query_upper = query.lstrip().upper()
            
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
                    'message': 'Use begin_transaction() for explicit transactions'
                })
                
            elif query_upper.startswith("COMMIT"):
                result.update({
                    'type': 'transaction',
                    'message': 'Use commit_transaction() for explicit transactions'
                })
                
            elif query_upper.startswith("ROLLBACK"):
                result.update({
                    'type': 'transaction',
                    'message': 'Use rollback_transaction() for explicit transactions'
                })
                
            elif query_upper.startswith("SAVEPOINT"):
                result.update({
                    'type': 'savepoint',
                    'message': 'Use create_savepoint() for explicit savepoints'
                })
                
            else:
                result.update({
                    'type': 'other',
                    'rows_affected': cursor.rowcount
                })
            
            # Commit automatique pour les exécutions hors transaction
            self.connection.commit()
            
            # Mettre à jour les statistiques
            self._update_statistics(query_upper.split()[0] if query_upper else "OTHER", execution_time)
            
            return result
            
        except sqlite3.Error as e:
            error_msg = str(e)
            logger.error(f"SQL execution error: {error_msg}")
            
            # Rollback pour les erreurs
            try:
                self.connection.rollback()
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
            elif "syntax error" in error_msg:
                return {'success': False, 'error': f'SQL syntax error: {error_msg}'}
            else:
                return {'success': False, 'error': f'SQL error: {error_msg}'}
                
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            try:
                self.connection.rollback()
            except:
                pass
            return {'success': False, 'error': f'Unexpected error: {e}'}
    
    def _update_statistics(self, query_type: str, execution_time: float):
        """Met à jour les statistiques"""
        try:
            cursor = self.connection.cursor()
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
            self.connection.commit()
        except:
            pass
    
    def get_table_schema(self, table: str) -> Dict:
        """Récupère le schéma d'une table"""
        try:
            cache_key = f"schema_{table}"
            if cache_key in self.schema_cache:
                return self.schema_cache[cache_key]
            
            cursor = self.connection.cursor()
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
            cursor = self.connection.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            return cursor.fetchone()[0]
        except:
            return 0
    
    def get_tables(self) -> List[Dict]:
        """Récupère la liste des tables"""
        try:
            cursor = self.connection.cursor()
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
    
    # ============ API TRANSACTIONS STABLE ============
    
    def begin_transaction(self, isolation_level: str = "DEFERRED") -> Dict:
        """Démarre une transaction"""
        return self.transaction_manager.begin(isolation_level)
    
    def commit_transaction(self, tid: int) -> Dict:
        """Valide une transaction"""
        return self.transaction_manager.commit(tid)
    
    def rollback_transaction(self, tid: int, to_savepoint: str = None) -> Dict:
        """Annule une transaction"""
        return self.transaction_manager.rollback(tid, to_savepoint)
    
    def create_savepoint(self, tid: int, name: str) -> Dict:
        """Crée un savepoint"""
        return self.transaction_manager.savepoint(tid, name)
    
    def execute_in_transaction(self, tid: int, query: str, params: Tuple = None) -> Dict:
        """
        Exécute une requête dans une transaction spécifique
        
        Args:
            tid: Transaction ID
            query: Requête SQL
            params: Paramètres de la requête
        
        Returns:
            Résultat de l'exécution
        """
        if tid not in self.transaction_manager.active_transactions:
            return {'success': False, 'error': f'Transaction {tid} not found'}
        
        return self.transaction_manager._execute_in_transaction(tid, query, params)
    
    def get_transaction_status(self, tid: int = None) -> Dict:
        """Récupère le statut des transactions"""
        if tid is not None:
            if tid in self.transaction_manager.active_transactions:
                trans = self.transaction_manager.active_transactions[tid]
                return {
                    'tid': tid,
                    'state': trans['state'],
                    'isolation': trans['isolation'],
                    'age_seconds': round(time.time() - trans['start_time'], 2),
                    'operations': trans['operations'],
                    'savepoints': trans['savepoints']
                }
            else:
                return {'error': f'Transaction {tid} not found'}
        else:
            return {
                'active_count': len(self.transaction_manager.get_active_transactions()),
                'total_started': self.transaction_manager.transaction_counter,
                'active_transactions': self.transaction_manager.get_active_transactions()
            }
    
    def atomic_transaction(self, operations: List[Dict]) -> Dict:
        """
        Exécute plusieurs opérations dans une transaction atomique
        
        Args:
            operations: Liste de dicts avec 'query' et 'params'
        
        Returns:
            Résultat global de la transaction
        """
        if not operations:
            return {'success': False, 'error': 'No operations provided'}
        
        tx_result = self.begin_transaction("DEFERRED")
        if not tx_result.get('success'):
            return tx_result
        
        tid = tx_result['tid']
        results = []
        
        try:
            for i, op in enumerate(operations):
                query = op.get('query', '')
                params = op.get('params')
                
                if not query:
                    results.append({
                        'index': i,
                        'success': False,
                        'error': 'Empty query'
                    })
                    raise SQLExecutionError(f"Empty query at index {i}")
                
                result = self.execute_in_transaction(tid, query, params)
                results.append({
                    'index': i,
                    'success': result.get('success', False),
                    'result': result
                })
                
                if not result.get('success'):
                    error = result.get('error', 'Unknown error')
                    raise SQLExecutionError(f"Operation {i} failed: {error}")
            
            # Tout a réussi, commit
            commit_result = self.commit_transaction(tid)
            
            return {
                'success': True,
                'transaction_id': tid,
                'operations_executed': len(operations),
                'commit_result': commit_result,
                'operation_results': results,
                'message': f'Successfully executed {len(operations)} operations in transaction'
            }
            
        except Exception as e:
            # Rollback en cas d'erreur
            rollback_result = self.rollback_transaction(tid)
            
            return {
                'success': False,
                'error': str(e),
                'transaction_id': tid,
                'operations_executed_before_failure': len([r for r in results if r.get('success')]),
                'failed_operation_index': len(results),
                'operation_results': results,
                'rollback_result': rollback_result
            }
    
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
            cursor = self.connection.cursor()
            cursor.execute("VACUUM")
            self.connection.commit()
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
            cursor = self.connection.cursor()
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
                    'connection_status': self.is_connected,
                    'version': self.VERSION
                },
                'performance': self.buffer_pool.get_stats(),
                'transactions': {
                    'active': len(self.transaction_manager.get_active_transactions()),
                    'total_started': self.transaction_manager.transaction_counter,
                    'recent_log': self.transaction_manager.get_transaction_log(10)
                },
                'cache': {
                    'schema': len(self.schema_cache),
                    'tables': len(self.table_cache),
                    'query': len(self.query_cache)
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
                
                if self.connection:
                    self.connection.close()
                    self.connection = None
                    self.is_connected = False
                    
                    self.buffer_pool.invalidate()
                    self.schema_cache.clear()
                    self.table_cache.clear()
                    self.query_cache.clear()
                
                logger.info("Storage closed")
        except Exception as e:
            logger.error(f"Error closing storage: {e}")
    
    def __enter__(self):
        """Support du contexte manager"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fermeture automatique"""
        self.close()


# ==================== TRANSACTION CONTEXT ====================

class TransactionContext:
    """Context manager pour les transactions sécurisées"""
    
    def __init__(self, storage, isolation_level: str = "DEFERRED"):
        self.storage = storage
        self.isolation_level = isolation_level
        self.tid = None
    
    def __enter__(self):
        """Démarre la transaction"""
        result = self.storage.begin_transaction(self.isolation_level)
        if not result.get('success'):
            raise TransactionError(f"Failed to begin transaction: {result.get('error')}")
        
        self.tid = result['tid']
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Termine la transaction (commit ou rollback)"""
        if self.tid is None:
            return False
        
        try:
            if exc_type is None:
                # Tout s'est bien passé, commit
                result = self.storage.commit_transaction(self.tid)
                if not result.get('success'):
                    logger.error(f"Commit failed: {result.get('error')}")
            else:
                # Exception levée, rollback
                logger.warning(f"Rollback transaction {self.tid} due to exception: {exc_val}")
                self.storage.rollback_transaction(self.tid)
            
            return False  # Propager l'exception si elle existe
        
        except Exception as e:
            logger.error(f"Error in transaction cleanup: {e}")
            return False
    
    def execute(self, query: str, params: Tuple = None) -> Dict:
        """Exécute une requête dans la transaction"""
        if self.tid is None:
            raise TransactionError("Transaction not started")
        
        return self.storage.execute_in_transaction(self.tid, query, params)


# ==================== API PUBLIQUE ====================

def create_storage(db_path=None, **kwargs) -> SQLiteStorage:
    """Crée une instance de stockage SQLite"""
    return SQLiteStorage(db_path, **kwargs)


def get_storage_stats(storage) -> Dict[str, Any]:
    """
    Get statistics about storage
    
    Args:
        storage: Storage instance
        
    Returns:
        Dict with statistics
    """
    stats = {
        'type': 'SQLite',
        'path': str(storage.db_path) if hasattr(storage, 'db_path') else 'unknown',
        'connected': storage.connection is not None if hasattr(storage, 'connection') else False,
        'tables_count': 0,
        'total_rows': 0,
        'file_size': 0,
        'last_modified': None
    }
    
    try:
        if hasattr(storage, 'db_path') and storage.db_path:
            path = Path(storage.db_path)
            if path.exists():
                stats['file_size'] = path.stat().st_size
                stats['last_modified'] = datetime.fromtimestamp(path.stat().st_mtime).isoformat()
        
        if hasattr(storage, 'connection') and storage.connection:
            cursor = storage.connection.cursor()
            
            # Get table count
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            stats['tables_count'] = cursor.fetchone()[0]
            
            # Get total rows (approx)
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            total_rows = 0
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    total_rows += cursor.fetchone()[0]
                except:
                    continue
            
            stats['total_rows'] = total_rows
            
    except Exception as e:
        stats['error'] = str(e)
    
    return stats


def atomic_transaction(operations: List[Dict], db_path=None, isolation_level: str = "DEFERRED") -> Dict:
    """
    Exécute des opérations dans une transaction atomique
    
    Args:
        operations: Liste de dicts avec 'query' et 'params'
        db_path: Chemin de la base de données
        isolation_level: Niveau d'isolation
    
    Returns:
        Résultat de la transaction
    """
    storage = SQLiteStorage(db_path)
    try:
        return storage.atomic_transaction(operations)
    finally:
        storage.close()


def quick_query(query: str, params: Tuple = None, db_path=None) -> Dict:
    """Exécute une requête rapide"""
    storage = SQLiteStorage(db_path)
    try:
        result = storage.execute(query, params)
        return result
    finally:
        storage.close()
