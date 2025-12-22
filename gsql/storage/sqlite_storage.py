#!/usr/bin/env python3
"""
GSQL Storage Engine Complete - SQLite avec Buffer Pool, Transactions et Auto-Recovery
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
    """Cache de pages en mémoire avec politique LRU et statistiques"""

    def __init__(self, max_pages=100):
        self.max_pages = max_pages
        self.pool = OrderedDict()  # page_id -> (data, timestamp, access_count)
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
                # Mettre à jour comme récemment utilisé (LRU)
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
            # Si la page existe déjà, la mettre à jour
            if page_id in self.pool:
                old_data, _, _ = self.pool.pop(page_id)
                self.stats['total_size'] -= self._get_size(old_data)

            # Si le cache est plein, évincer la page LRU
            while len(self.pool) >= self.max_pages:
                evicted_id, (evicted_data, _, _) = self.pool.popitem(last=False)
                self.stats['evictions'] += 1
                self.stats['total_size'] -= self._get_size(evicted_data)
                logger.debug(f"BufferPool: Éviction page {evicted_id}")

            # Ajouter la nouvelle page
            size = self._get_size(page_data)
            self.pool[page_id] = (
                page_data.copy() if hasattr(page_data, 'copy') else page_data,
                time.time(),
                1
            )
            self.stats['total_size'] += size

            # Si priorité élevée, déplacer au début
            if priority:
                self.pool.move_to_end(page_id, last=True)

    def _get_size(self, data: Any) -> int:
        """Estime la taille en mémoire"""
        try:
            return len(pickle.dumps(data))
        except:
            return 1000  # Estimation par défaut

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
            return {
                'size': len(self.pool),
                'max_size': self.max_pages,
                'hits': self.stats['hits'],
                'misses': self.stats['misses'],
                'evictions': self.stats['evictions'],
                'hit_ratio': (
                    self.stats['hits'] / (self.stats['hits'] + self.stats['misses'])
                    if (self.stats['hits'] + self.stats['misses']) > 0 else 0
                ),
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
    """Gestionnaire de transactions ACID avec support multi-thread"""

    def __init__(self, storage):
        self.storage = storage
        self.active_transactions = {}  # tid -> transaction data
        self.transaction_counter = 0
        self.lock = threading.RLock()
        self.transaction_timeout = 30  # secondes

    def begin(self, isolation_level: str = "DEFERRED") -> int:
        """Démarre une nouvelle transaction"""
        with self.lock:
            tid = self.transaction_counter
            self.transaction_counter += 1

            # Définir le niveau d'isolation
            isolation_sql = {
                "DEFERRED": "BEGIN DEFERRED TRANSACTION",
                "IMMEDIATE": "BEGIN IMMEDIATE TRANSACTION", 
                "EXCLUSIVE": "BEGIN EXCLUSIVE TRANSACTION"
            }.get(isolation_level, "BEGIN")

            try:
                self.storage._execute_raw(isolation_sql)
            except Exception as e:
                raise TransactionError(f"Failed to begin transaction: {e}")

            self.active_transactions[tid] = {
                'start_time': time.time(),
                'isolation': isolation_level,
                'changes': {},
                'savepoints': [],
                'state': 'ACTIVE'
            }

            logger.debug(f"Transaction {tid} started ({isolation_level})")
            return tid

    def commit(self, tid: int) -> bool:
        """Valide une transaction"""
        with self.lock:
            if tid not in self.active_transactions:
                raise TransactionError(f"Transaction {tid} not found")

            trans = self.active_transactions[tid]

            # Vérifier le timeout
            if time.time() - trans['start_time'] > self.transaction_timeout:
                self.rollback(tid)
                raise TransactionError(f"Transaction {tid} timeout")

            try:
                self.storage._execute_raw("COMMIT")
                logger.debug(f"Transaction {tid} committed")

                # Nettoyer les métadonnées de la transaction
                del self.active_transactions[tid]

                # Mettre à jour le buffer pool
                for page_id in trans.get('changes', {}):
                    self.storage.buffer_pool.invalidate(page_id)

                return True

            except Exception as e:
                self.rollback(tid)
                raise TransactionError(f"Commit failed: {e}")

    def rollback(self, tid: int, to_savepoint: str = None) -> bool:
        """Annule une transaction ou revient à un savepoint"""
        with self.lock:
            if tid not in self.active_transactions:
                raise TransactionError(f"Transaction {tid} not found")

            try:
                if to_savepoint:
                    self.storage._execute_raw(f"ROLLBACK TO SAVEPOINT {to_savepoint}")
                    logger.debug(f"Transaction {tid} rolled back to {to_savepoint}")
                else:
                    self.storage._execute_raw("ROLLBACK")
                    logger.debug(f"Transaction {tid} rolled back")

                    # Nettoyer
                    del self.active_transactions[tid]

                return True

            except Exception as e:
                raise TransactionError(f"Rollback failed: {e}")

    def savepoint(self, tid: int, name: str) -> bool:
        """Crée un savepoint dans la transaction"""
        with self.lock:
            if tid not in self.active_transactions:
                return False

            try:
                self.storage._execute_raw(f"SAVEPOINT {name}")
                self.active_transactions[tid]['savepoints'].append(name)
                return True
            except Exception as e:
                raise TransactionError(f"Savepoint failed: {e}")

    def get_active_transactions(self) -> List[Dict]:
        """Liste les transactions actives"""
        with self.lock:
            return [
                {
                    'tid': tid,
                    'age': time.time() - data['start_time'],
                    'isolation': data['isolation'],
                    'savepoints': len(data['savepoints']),
                    'state': data['state']
                }
                for tid, data in self.active_transactions.items()
            ]

# ==================== SQLITE STORAGE WITH AUTO-RECOVERY ====================

class SQLiteStorage:
    """Moteur de stockage SQLite complet avec buffer pool et transactions"""

    VERSION = "3.0"
    SCHEMA_VERSION = 2

    def __init__(self, db_path=None, base_dir="/root/.gsql", 
                 buffer_pool_size=100, enable_wal=True):

        # Configuration
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # Déterminer le chemin de la base
        if db_path is None or db_path == ":memory:":
            self.db_path = self.base_dir / "gsql_data.db"
        else:
            self.db_path = Path(db_path)
            if not self.db_path.is_absolute():
                self.db_path = self.base_dir / self.db_path

        # État
        self.conn = None
        self.is_connected = False
        self.connection_lock = threading.RLock()
        self.recovery_mode = False

        # Buffer Pool
        self.buffer_pool = BufferPool(max_pages=buffer_pool_size)

        # Transaction Manager
        self.transaction_manager = TransactionManager(self)

        # Configuration
        self.config = {
            'enable_wal': enable_wal,
            'auto_vacuum': 'FULL',
            'busy_timeout': 10000,
            'cache_size': -2000,  # 2MB en pages
            'journal_mode': 'WAL' if enable_wal else 'DELETE'
        }

        # Fichiers de contrôle
        self.meta_file = self.base_dir / "storage_meta.json"
        self.recovery_flag = self.base_dir / ".recovery_needed"
        self.backup_dir = self.base_dir / "backups"
        self.backup_dir.mkdir(exist_ok=True)

        # Cache
        self.schema_cache = {}
        self.table_cache = {}
        self.query_cache = {}  # Cache de requêtes préparées

        # Initialisation
        self._initialize()

    def _initialize(self):
        """Initialise la connexion et vérifie l'intégrité"""
        try:
            # Tentative de connexion
            self._connect()

            # Vérifier l'intégrité
            if self._check_integrity():
                logger.info(f"Storage initialized: {self.db_path}")
            else:
                logger.warning("Integrity check failed, attempting recovery...")
                self._recover_database()

        except Exception as e:
            logger.error(f"Initialization failed: {e}")
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

                    # Connexion avec paramètres optimisés
                    self.conn = sqlite3.connect(
                        str(self.db_path),
                        timeout=self.config['busy_timeout'] / 1000,
                        check_same_thread=False,
                        isolation_level=None  # Auto-commit par défaut
                    )

                    # Configuration SQLite
                    self._configure_connection()

                    self.is_connected = True
                    logger.debug(f"Connected to SQLite (attempt {attempt+1})")
                    return True

            except Exception as e:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"Connection failed: {e}, retrying in {wait}s")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed to connect after {retries} attempts")
                    self.is_connected = False
                    raise

    def _configure_connection(self):
        """Configure les paramètres SQLite"""
        if not self.conn:
            return

        cursor = self.conn.cursor()

        # Activer WAL pour meilleures performances concurrentielles
        if self.config['enable_wal']:
            cursor.execute(f"PRAGMA journal_mode={self.config['journal_mode']}")

        # Autres optimisations
        cursor.execute(f"PRAGMA auto_vacuum={self.config['auto_vacuum']}")
        cursor.execute(f"PRAGMA cache_size={self.config['cache_size']}")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA temp_store=MEMORY")

        # Créer les tables système si nécessaire
        self._create_system_tables()

    def _create_system_tables(self):
        """Crée les tables système nécessaires"""
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
            logger.error(f"Failed to create system tables: {e}")
            raise

    def _check_integrity(self) -> bool:
        """Vérifie l'intégrité de la base de données"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            return result[0] == "ok"
        except Exception as e:
            logger.error(f"Integrity check failed: {e}")
            return False

    def _recover_database(self):
        """Tente une récupération de la base de données"""
        logger.info("Starting database recovery...")
        self.recovery_mode = True

        try:
            # 1. Vérifier si une sauvegarde existe
            if self._restore_from_backup():
                logger.info("Restored from backup")
                return True

            # 2. Tentative de réparation SQLite
            if self._attempt_sqlite_recovery():
                logger.info("SQLite recovery succeeded")
                return True

            # 3. Réinitialisation complète
            logger.warning("Full database reset required")
            self._reset_database()
            return True

        except Exception as e:
            logger.error(f"Recovery failed: {e}")
            raise
        finally:
            self.recovery_mode = False

    def _restore_from_backup(self) -> bool:
        """Restaure depuis la dernière sauvegarde valide"""
        try:
            backup_files = sorted(self.backup_dir.glob("backup_*.db"))
            if not backup_files:
                return False

            latest_backup = backup_files[-1]

            # Vérifier l'intégrité de la sauvegarde
            temp_conn = sqlite3.connect(str(latest_backup))
            temp_cursor = temp_conn.cursor()
            temp_cursor.execute("PRAGMA integrity_check")
            integrity_result = temp_cursor.fetchone()
            temp_conn.close()

            if integrity_result and integrity_result[0] == "ok":
                # Remplacer la base corrompue
                if self.conn:
                    self.conn.close()
                os.replace(str(latest_backup), str(self.db_path))

                # Recréer la connexion
                self._connect()
                return True

        except Exception as e:
            logger.error(f"Backup restoration failed: {e}")

        return False

    def _attempt_sqlite_recovery(self) -> bool:
        """Tente une récupération via les outils SQLite"""
        try:
            if not self.conn:
                return False

            # Essayer la commande de récupération
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA wal_checkpoint(FULL)")
            
            # Vérifier à nouveau l'intégrité
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()

            if result and result[0] == "ok":
                # Nettoyer les données temporaires
                cursor.execute("VACUUM")
                self.conn.commit()
                return True

        except Exception as e:
            logger.error(f"SQLite recovery failed: {e}")

        return False

    def _reset_database(self):
        """Réinitialise complètement la base de données"""
        try:
            if self.conn:
                self.conn.close()

            if self.db_path.exists():
                # Sauvegarder l'ancien fichier
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                corrupted_path = self.db_path.with_suffix(f".corrupted_{timestamp}.db")
                os.rename(str(self.db_path), str(corrupted_path))

            # Recréer la base
            self._connect()
            self._create_system_tables()

            logger.warning(f"Database reset completed. Old file: {corrupted_path}")

        except Exception as e:
            logger.error(f"Database reset failed: {e}")
            raise

    def execute(self, query: str, params: Tuple = None) -> Dict:
        """Exécute une requête SQL avec gestion d'erreurs"""
        try:
            # Vérifier la connexion
            if not self.is_connected:
                self._connect()

            # Préparer les paramètres
            if params is None:
                params = ()

            # Vérifier le cache de requêtes
            query_hash = hashlib.md5(query.encode()).hexdigest()
            if query_hash in self.query_cache:
                cursor = self.query_cache[query_hash]
            else:
                cursor = self.conn.cursor()
                self.query_cache[query_hash] = cursor

            # Exécuter la requête
            start_time = time.time()
            cursor.execute(query, params)
            execution_time = time.time() - start_time

            # Gérer le résultat selon le type de requête
            query_upper = query.strip().upper()
            result = {'success': True, 'execution_time': execution_time}

            if query_upper.startswith("SELECT"):
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                
                result.update({
                    'type': 'SELECT',
                    'count': len(rows),
                    'columns': column_names,
                    'rows': rows
                })

                # Mettre à jour les statistiques du buffer pool
                self.buffer_pool.put(f"query_{query_hash}", {
                    'columns': column_names,
                    'rows': rows,
                    'timestamp': time.time()
                }, priority=True)

            elif query_upper.startswith("INSERT"):
                lastrowid = cursor.lastrowid
                rowcount = cursor.rowcount
                
                result.update({
                    'type': 'INSERT',
                    'last_insert_id': lastrowid,
                    'rows_affected': rowcount
                })

                # Mettre à jour le cache de schéma
                table = self._extract_table_name(query)
                if table:
                    self._invalidate_schema_cache(table)

            elif query_upper.startswith("UPDATE") or query_upper.startswith("DELETE"):
                result.update({
                    'type': 'UPDATE' if query_upper.startswith("UPDATE") else 'DELETE',
                    'rows_affected': cursor.rowcount
                })

                # Invalider les caches concernés
                table = self._extract_table_name(query)
                if table:
                    self._invalidate_schema_cache(table)
                    self.buffer_pool.invalidate(f"table_{table}")

            elif query_upper.startswith("CREATE") or query_upper.startswith("DROP"):
                result.update({
                    'type': 'SCHEMA_CHANGE',
                    'operation': 'CREATE' if query_upper.startswith("CREATE") else 'DROP'
                })

                # Réinitialiser les caches de schéma
                self.schema_cache.clear()

            else:
                result.update({
                    'type': 'OTHER',
                    'rows_affected': cursor.rowcount
                })

            # Commit pour les non-SELECT
            if not query_upper.startswith("SELECT"):
                self.conn.commit()

            # Mettre à jour les statistiques
            self._update_statistics(query_upper, execution_time)

            return result

        except sqlite3.Error as e:
            logger.error(f"SQL execution error: {e}")
            
            # Tentative de récupération pour certaines erreurs
            if "database is locked" in str(e):
                return self._handle_locked_database(query, params)
            elif "database disk image is malformed" in str(e):
                self._recover_database()
                raise StorageError("Database recovered, please retry operation")
            
            raise SQLExecutionError(f"SQL error: {e}")

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    def _extract_table_name(self, query: str) -> Optional[str]:
        """Extrait le nom de table d'une requête SQL"""
        # Regex simplifiée pour extraire les noms de tables
        patterns = [
            r"FROM\s+(\w+)",
            r"INSERT\s+INTO\s+(\w+)",
            r"UPDATE\s+(\w+)",
            r"DELETE\s+FROM\s+(\w+)",
            r"CREATE\s+TABLE\s+(\w+)",
            r"DROP\s+TABLE\s+(\w+)"
        ]

        query_upper = query.upper()
        for pattern in patterns:
            match = re.search(pattern, query_upper, re.IGNORECASE)
            if match:
                return match.group(1)

        return None

    def _invalidate_schema_cache(self, table: str):
        """Invalide le cache pour une table spécifique"""
        if table in self.schema_cache:
            del self.schema_cache[table]
        if table in self.table_cache:
            del self.table_cache[table]

    def _update_statistics(self, query_type: str, execution_time: float):
        """Met à jour les statistiques d'exécution"""
        try:
            cursor = self.conn.cursor()
            
            # Incrémenter le compteur pour ce type de requête
            cursor.execute(
                """INSERT INTO _gsql_statistics (metric, value) 
                   VALUES (?, 1)
                   ON CONFLICT(metric) DO UPDATE SET 
                   value = value + 1,
                   updated_at = CURRENT_TIMESTAMP""",
                (f"query_count_{query_type.split()[0]}",)
            )

            # Mettre à jour le temps d'exécution moyen
            cursor.execute(
                """INSERT INTO _gsql_statistics (metric, value) 
                   VALUES (?, ?)
                   ON CONFLICT(metric) DO UPDATE SET 
                   value = (value + ?) / 2,
                   updated_at = CURRENT_TIMESTAMP""",
                (f"avg_time_{query_type.split()[0]}", execution_time, execution_time)
            )

            self.conn.commit()
        except Exception as e:
            logger.warning(f"Failed to update statistics: {e}")

    def _handle_locked_database(self, query: str, params: Tuple) -> Dict:
        """Gère les erreurs de verrouillage de base de données"""
        logger.warning("Database locked, retrying...")
        
        for retry in range(3):
            try:
                time.sleep(0.1 * (2 ** retry))  # Backoff exponentiel
                
                cursor = self.conn.cursor()
                cursor.execute(query, params)
                
                if not query.strip().upper().startswith("SELECT"):
                    self.conn.commit()
                
                return {
                    'success': True,
                    'retry_count': retry + 1,
                    'message': f"Query succeeded after {retry + 1} retries"
                }
                
            except Exception as e:
                if retry == 2:
                    logger.error(f"Database still locked after retries: {e}")
                    raise StorageError("Database is locked, please try again later")

        return {'success': False, 'error': 'Database locked'}

    def _execute_raw(self, sql: str):
        """Exécute du SQL brut (pour les transactions)"""
        if not self.conn:
            raise StorageError("Not connected to database")
        
        cursor = self.conn.cursor()
        cursor.execute(sql)

    def _build_insert_sql(self, table: str, data: Dict) -> Tuple[str, List]:
        """Construit une requête INSERT à partir d'un dictionnaire"""
        if not data:
            raise SQLExecutionError("No data provided for INSERT")

        columns = []
        placeholders = []
        values = []

        for column, value in data.items():
            columns.append(column)
            placeholders.append("?")
            
            # CORRECTION DU BUG: Échapper les guillemets doubles dans la f-string
            if isinstance(value, str):
                escaped_value = value.replace("'", "''")
                values.append(f"'{escaped_value}'")
            else:
                values.append(value)

        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        return sql, values

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

    def get_table_schema(self, table: str) -> Dict:
        """Récupère le schéma d'une table"""
        if table in self.schema_cache:
            return self.schema_cache[table]

        try:
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()

            schema = {
                'table_name': table,
                'columns': [
                    {
                        'name': col[1],
                        'type': col[2],
                        'not_null': bool(col[3]),
                        'default_value': col[4],
                        'primary_key': bool(col[5])
                    }
                    for col in columns
                ],
                'primary_keys': [col[1] for col in columns if col[5] > 0],
                'timestamp': time.time()
            }

            self.schema_cache[table] = schema
            return schema

        except Exception as e:
            logger.error(f"Failed to get schema for {table}: {e}")
            raise

    def backup(self, backup_name: str = None) -> str:
        """Crée une sauvegarde de la base de données"""
        if backup_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"backup_{timestamp}.db"

        backup_path = self.backup_dir / backup_name

        try:
            # Utiliser la sauvegarde SQLite
            backup_conn = sqlite3.connect(str(backup_path))
            self.conn.backup(backup_conn)
            backup_conn.close()

            logger.info(f"Backup created: {backup_path}")
            return str(backup_path)

        except Exception as e:
            logger.error(f"Backup failed: {e}")
            raise StorageError(f"Backup failed: {e}")

    def vacuum(self):
        """Optimise la base de données"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("VACUUM")
            self.conn.commit()
            logger.info("Database vacuum completed")
        except Exception as e:
            logger.error(f"Vacuum failed: {e}")
            raise

    def get_stats(self) -> Dict:
        """Récupère les statistiques du stockage"""
        try:
            cursor = self.conn.cursor()

            # Statistiques de base de données
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()

            cursor.execute("PRAGMA page_count")
            page_count = cursor.fetchone()[0]

            cursor.execute("PRAGMA page_size")
            page_size = cursor.fetchone()[0]

            # Statistiques personnalisées
            cursor.execute("SELECT metric, value FROM _gsql_statistics")
            stats_rows = cursor.fetchall()
            custom_stats = {row[0]: row[1] for row in stats_rows}

            return {
                'database': {
                    'path': str(self.db_path),
                    'tables': len(tables),
                    'size_mb': round((page_count * page_size) / (1024 * 1024), 2),
                    'connection_status': self.is_connected
                },
                'performance': {
                    'buffer_pool': self.buffer_pool.get_stats(),
                    'query_cache_size': len(self.query_cache),
                    'schema_cache_size': len(self.schema_cache)
                },
                'transactions': {
                    'active': self.transaction_manager.get_active_transactions(),
                    'total_started': self.transaction_manager.transaction_counter
                },
                'statistics': custom_stats
            }

        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {
                'error': str(e),
                'database': {'connection_status': self.is_connected}
            }

    def close(self):
        """Ferme la connexion à la base de données"""
        try:
            with self.connection_lock:
                if self.conn:
                    # Sauvegarder avant de fermer si demandé
                    if self.recovery_flag.exists():
                        self.backup("pre_close_backup.db")
                        self.recovery_flag.unlink()

                    self.conn.close()
                    self.conn = None
                    self.is_connected = False
                    
                    # Vider les caches
                    self.buffer_pool.invalidate()
                    self.query_cache.clear()
                    self.schema_cache.clear()

                logger.info("Storage closed")
                
        except Exception as e:
            logger.error(f"Error closing storage: {e}")

    def __enter__(self):
        """Support du contexte manager"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fermeture automatique en sortie de contexte"""
        self.close()

# ==================== API SIMPLIFIÉE ====================

def create_storage(db_path=None, **kwargs) -> SQLiteStorage:
    """Crée une instance de stockage SQLite"""
    return SQLiteStorage(db_path, **kwargs)

def quick_query(query: str, db_path=None) -> Dict:
    """Exécute une requête rapide sans gestion de connexion persistante"""
    storage = SQLiteStorage(db_path)
    try:
        result = storage.execute(query)
        return result
    finally:
        storage.close()
