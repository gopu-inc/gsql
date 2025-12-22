#!/usr/bin/env python3
"""
GSQL Storage Engine Complete - SQLite avec Buffer Pool, Transactions et Auto-Recovery
"""

# Dans storage.py - Remplacer le début du fichier :

# SUPPRIMER:
# import yaml

# GARDER:
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
    SQLSyntaxError, ConstraintViolationError, StorageError  # Ajouter StorageError
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
            
            """CREATE TABLE IF NOT EXISTS _gsql_functions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                params TEXT,
                body TEXT NOT NULL,
                returns TEXT DEFAULT 'TEXT',
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            """CREATE TABLE IF NOT EXISTS _gsql_indexes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                index_name TEXT UNIQUE NOT NULL,
                columns TEXT NOT NULL,
                index_type TEXT DEFAULT 'BTREE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            """CREATE TABLE IF NOT EXISTS _gsql_stats (
                table_name TEXT PRIMARY KEY,
                total_rows INTEGER DEFAULT 0,
                last_analyzed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                size_bytes INTEGER DEFAULT 0
            )""",
            
            """CREATE TABLE IF NOT EXISTS _gsql_query_cache (
                query_hash TEXT PRIMARY KEY,
                query_text TEXT NOT NULL,
                result_hash TEXT,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                use_count INTEGER DEFAULT 0
            )"""
        ]
        
        cursor = self.conn.cursor()
        for table_sql in system_tables:
            cursor.execute(table_sql)
        
        # Initialiser les métadonnées
        cursor.execute("""
            INSERT OR IGNORE INTO _gsql_metadata (key, value) VALUES 
            ('version', ?),
            ('schema_version', ?),
            ('created_at', ?)
        """, (self.VERSION, self.SCHEMA_VERSION, datetime.now().isoformat()))
        
        self.conn.commit()
    
    def _check_integrity(self) -> bool:
        """Vérifie l'intégrité de la base de données"""
        try:
            cursor = self.conn.cursor()
            
            # 1. Vérifier l'intégrité SQLite
            cursor.execute("PRAGMA integrity_check")
            integrity = cursor.fetchone()[0]
            if integrity != "ok":
                logger.error(f"Integrity check failed: {integrity}")
                return False
            
            # 2. Vérifier les tables système
            required_tables = [
                '_gsql_metadata', '_gsql_schemas', '_gsql_functions',
                'sqlite_master'
            ]
            
            for table in required_tables:
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,)
                )
                if not cursor.fetchone():
                    logger.warning(f"System table missing: {table}")
                    return False
            
            # 3. Vérifier la version du schéma
            cursor.execute("SELECT value FROM _gsql_metadata WHERE key='schema_version'")
            version_row = cursor.fetchone()
            if not version_row or int(version_row[0]) < self.SCHEMA_VERSION:
                logger.warning(f"Schema version mismatch: {version_row}")
                return False
            
            logger.debug("Integrity check passed")
            return True
            
        except Exception as e:
            logger.error(f"Integrity check error: {e}")
            return False
    
    def _recover_database(self):
        """Tente de récupérer une base corrompue"""
        logger.warning("Starting database recovery...")
        self.recovery_mode = True
        
        try:
            # 1. Sauvegarder l'ancienne base
            backup_path = self._create_backup()
            logger.info(f"Backup created: {backup_path}")
            
            # 2. Fermer la connexion
            if self.conn:
                self.conn.close()
            
            # 3. Essayez de réparer avec .dump/.restore
            recovered = self._attempt_repair()
            
            if recovered:
                logger.info("Database recovered successfully")
                self.recovery_mode = False
                return True
            else:
                # 4. Reconstruction complète
                logger.warning("Repair failed, performing full rebuild...")
                return self._rebuild_database()
                
        except Exception as e:
            logger.error(f"Recovery failed: {e}")
            return self._rebuild_database()
    
    def _attempt_repair(self) -> bool:
        """Tente de réparer avec dump/restore"""
        try:
            import tempfile
            dump_file = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
            dump_path = dump_file.name
            dump_file.close()
            
            # Dump de la base corrompue
            self._dump_database(str(dump_path))
            
            # Supprimer l'ancien fichier
            if self.db_path.exists():
                self.db_path.unlink()
            
            # Restaurer depuis le dump
            self._restore_database(str(dump_path))
            
            # Recréer la connexion
            self._connect()
            
            # Vérifier à nouveau
            return self._check_integrity()
            
        except Exception as e:
            logger.error(f"Repair attempt failed: {e}")
            return False
    
    def _dump_database(self, output_path: str):
        """Crée un dump SQL de la base"""
        if not self.conn:
            raise SQLExecutionError("Not connected")
        
        cursor = self.conn.cursor()
        
        with open(output_path, 'w') as f:
            # Écrire l'en-tête
            f.write("-- GSQL Database Dump\n")
            f.write(f"-- Generated: {datetime.now()}\n")
            f.write(f"-- Version: {self.VERSION}\n\n")
            
            # Dump les tables utilisateur (sauf système)
            cursor.execute("""
                SELECT name, sql FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE '_gsql_%'
                AND name NOT LIKE 'sqlite_%'
            """)
            
            for table_name, table_sql in cursor.fetchall():
                f.write(f"{table_sql};\n\n")
                
                # Dump les données
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()
                
                if rows:
                    # Récupérer les noms de colonnes
                    col_cursor = self.conn.cursor()
                    col_cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = [col[1] for col in col_cursor.fetchall()]
                    
                    for row in rows:
                        values = []
                        for value in row:
                            if value is None:
                                values.append("NULL")
                            elif isinstance(value, str):
                                values.append(f"'{value.replace("'", "''")}'")
                            elif isinstance(value, (int, float)):
                                values.append(str(value))
                            else:
                                values.append(f"'{str(value)}'")
                        
                        f.write(f"INSERT INTO {table_name} ({', '.join(columns)}) ")
                        f.write(f"VALUES ({', '.join(values)});\n")
                    
                    f.write("\n")
    
    def _restore_database(self, dump_path: str):
        """Restaure depuis un dump SQL"""
        with open(dump_path, 'r') as f:
            sql_script = f.read()
        
        # Créer une nouvelle connexion
        temp_conn = sqlite3.connect(str(self.db_path))
        temp_conn.executescript(sql_script)
        temp_conn.commit()
        temp_conn.close()
    
    def _rebuild_database(self) -> bool:
        """Reconstruction complète de la base"""
        logger.warning("Performing complete database rebuild...")
        
        try:
            # 1. Supprimer l'ancien fichier
            if self.db_path.exists():
                self.db_path.unlink()
            
            # 2. Recréer la connexion
            self._connect()
            
            # 3. Recréer les tables système
            self._create_system_tables()
            
            # 4. Tenter de restaurer les schémas depuis le cache
            self._restore_from_cache()
            
            logger.info("Database rebuilt successfully")
            return True
            
        except Exception as e:
            logger.error(f"Rebuild failed: {e}")
            # Dernier recours: base vide mais fonctionnelle
            return self._connect()
    
    def _restore_from_cache(self):
        """Tente de restaurer les données depuis le cache"""
        if self.meta_file.exists():
            try:
                with open(self.meta_file, 'r') as f:
                    cache = json.load(f)
                
                # Restaurer les schémas de table
                for table_name, schema in cache.get('schemas', {}).items():
                    self._cache_table_schema(table_name, schema)
                
                logger.info(f"Restored {len(cache.get('schemas', {}))} schemas from cache")
            except Exception as e:
                logger.warning(f"Cache restore failed: {e}")
    
    def _create_backup(self) -> Path:
        """Crée une sauvegarde de la base"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = self.backup_dir / f"backup_{timestamp}.db"
        
        if self.db_path.exists():
            import shutil
            shutil.copy2(self.db_path, backup_path)
        
        # Nettoyer les vieilles sauvegardes (garder les 5 dernières)
        backups = sorted(self.backup_dir.glob("backup_*.db"))
        for old_backup in backups[:-5]:
            old_backup.unlink()
        
        return backup_path
    
    def _execute_raw(self, sql: str, params: Tuple = None) -> sqlite3.Cursor:
        """Exécute une requête SQL brute (sans gestion d'erreur avancée)"""
        if not self.is_connected:
            self._connect()
        
        cursor = self.conn.cursor()
        
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        
        return cursor
    
    # ==================== PUBLIC API ====================
    
    def execute(self, sql: str, params: Dict = None, 
                use_cache: bool = True, track_stats: bool = True) -> Dict:
        """
        Exécute une requête SQL avec gestion d'erreur complète
        
        Returns:
            Dict: Résultats formatés
        """
        start_time = time.time()
        query_hash = None
        
        # Vérifier la connexion
        if not self.is_connected:
            if not self._connect():
                raise SQLExecutionError("Database not available")
        
        # Gestion du cache de requêtes
        if use_cache and params is None:
            query_hash = hashlib.md5(sql.encode()).hexdigest()[:16]
            cached = self.query_cache.get(query_hash)
            if cached and (time.time() - cached['timestamp'] < 300):  # 5 minutes
                logger.debug(f"Query cache hit: {query_hash}")
                return cached['result']
        
        try:
            with self.connection_lock:
                cursor = self.conn.cursor()
                
                # Exécuter la requête
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                
                # Détecter le type de requête
                sql_upper = sql.strip().upper()
                
                # Requête SELECT
                if sql_upper.startswith("SELECT"):
                    rows = cursor.fetchall()
                    
                    # Formater les résultats
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        result_rows = []
                        
                        for row in rows:
                            row_dict = {}
                            for i, col in enumerate(columns):
                                row_dict[col] = row[i]
                            result_rows.append(row_dict)
                        
                        result = {
                            'type': 'select',
                            'rows': result_rows,
                            'columns': columns,
                            'count': len(result_rows),
                            'success': True
                        }
                    else:
                        result = {
                            'type': 'select',
                            'rows': rows,
                            'count': len(rows),
                            'success': True
                        }
                
                # Requête PRAGMA ou autre commande
                elif sql_upper.startswith("PRAGMA") or sql_upper.startswith("EXPLAIN"):
                    rows = cursor.fetchall()
                    result = {
                        'type': 'pragma',
                        'rows': rows,
                        'success': True
                    }
                
                # Requête d'écriture (INSERT, UPDATE, DELETE, CREATE, etc.)
                else:
                    self.conn.commit()
                    
                    # Mettre à jour les statistiques
                    if track_stats and sql_upper.startswith(("INSERT", "UPDATE", "DELETE")):
                        self._update_table_stats()
                    
                    result = {
                        'type': 'command',
                        'rows_affected': cursor.rowcount,
                        'lastrowid': cursor.lastrowid,
                        'success': True
                    }
                
                # Calculer le temps d'exécution
                exec_time = (time.time() - start_time) * 1000
                result['execution_time_ms'] = round(exec_time, 2)
                
                # Mettre en cache si applicable
                if use_cache and query_hash and sql_upper.startswith("SELECT"):
                    self.query_cache[query_hash] = {
                        'result': result,
                        'timestamp': time.time(),
                        'sql': sql
                    }
                
                # Limiter la taille du cache de requêtes
                if len(self.query_cache) > 100:
                    oldest = min(self.query_cache.items(), key=lambda x: x[1]['timestamp'])
                    del self.query_cache[oldest[0]]
                
                return result
                
        except sqlite3.Error as e:
            error_msg = str(e)
            
            # Tentative de reconnexion pour certaines erreurs
            if any(err in error_msg for err in ["database is locked", "no such table", "disk I/O"]):
                logger.warning(f"Database error, attempting reconnect: {error_msg}")
                time.sleep(0.1)
                self._connect()
                
                # Réessayer une fois
                try:
                    return self.execute(sql, params, use_cache=False, track_stats=False)
                except:
                    pass
            
            raise SQLExecutionError(f"SQL error: {error_msg}")
        
        except Exception as e:
            raise SQLExecutionError(f"Execution error: {str(e)}")
    
    def begin_transaction(self, isolation_level: str = "DEFERRED") -> int:
        """Démarre une nouvelle transaction"""
        return self.transaction_manager.begin(isolation_level)
    
    def commit_transaction(self, tid: int) -> bool:
        """Valide une transaction"""
        return self.transaction_manager.commit(tid)
    
    def rollback_transaction(self, tid: int, to_savepoint: str = None) -> bool:
        """Annule une transaction"""
        return self.transaction_manager.rollback(tid, to_savepoint)
    
    def savepoint(self, tid: int, name: str) -> bool:
        """Crée un savepoint"""
        return self.transaction_manager.savepoint(tid, name)
    
    def _update_table_stats(self):
        """Met à jour les statistiques des tables"""
        try:
            cursor = self.conn.cursor()
            
            # Récupérer toutes les tables utilisateur
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE '_gsql_%'
                AND name NOT LIKE 'sqlite_%'
            """)
            
            for (table_name,) in cursor.fetchall():
                # Compter les lignes
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                
                # Mettre à jour les stats
                cursor.execute("""
                    INSERT OR REPLACE INTO _gsql_stats 
                    (table_name, total_rows, last_analyzed)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (table_name, row_count))
                
                # Mettre à jour le cache de schéma
                self._cache_table_schema(table_name)
            
            self.conn.commit()
            
        except Exception as e:
            logger.debug(f"Stats update failed: {e}")
    
    def _cache_table_schema(self, table_name: str, schema: Dict = None):
        """Cache le schéma d'une table"""
        if not schema:
            try:
                cursor = self.conn.cursor()
                
                # Récupérer les informations de la table
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                schema = {
                    'columns': {},
                    'indexes': [],
                    'foreign_keys': []
                }
                
                for col in columns:
                    schema['columns'][col[1]] = {
                        'type': col[2],
                        'not_null': bool(col[3]),
                        'default': col[4],
                        'pk': bool(col[5])
                    }
                
                # Récupérer les index
                cursor.execute(f"PRAGMA index_list({table_name})")
                indexes = cursor.fetchall()
                
                for idx in indexes:
                    schema['indexes'].append({
                        'name': idx[1],
                        'unique': bool(idx[2])
                    })
                
                # Récupérer les clés étrangères
                cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                fks = cursor.fetchall()
                
                for fk in fks:
                    schema['foreign_keys'].append({
                        'from': fk[3],
                        'to_table': fk[2],
                        'to_column': fk[4]
                    })
                
            except Exception as e:
                logger.warning(f"Failed to cache schema for {table_name}: {e}")
                return
        
        # Mettre en cache
        self.schema_cache[table_name] = {
            'schema': schema,
            'timestamp': time.time()
        }
        
        # Sauvegarder dans la table système
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO _gsql_schemas 
                (table_name, schema_json, last_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (table_name, json.dumps(schema)))
            self.conn.commit()
        except Exception as e:
            logger.debug(f"Failed to save schema to system table: {e}")
    
    def get_table_schema(self, table_name: str) -> Optional[Dict]:
        """Récupère le schéma d'une table"""
        # Vérifier le cache
        cached = self.schema_cache.get(table_name)
        if cached and (time.time() - cached['timestamp'] < 3600):  # 1 heure
            return cached['schema']
        
        # Charger depuis la base
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT schema_json FROM _gsql_schemas WHERE table_name = ?",
                (table_name,)
            )
            row = cursor.fetchone()
            
            if row:
                schema = json.loads(row[0])
                self.schema_cache[table_name] = {
                    'schema': schema,
                    'timestamp': time.time()
                }
                return schema
        except:
            pass
        
        # Générer depuis PRAGMA
        self._cache_table_schema(table_name)
        return self.schema_cache.get(table_name, {}).get('schema')
    
    def get_tables(self) -> List[Dict]:
        """Récupère la liste des tables avec statistiques"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT 
                    t.name as table_name,
                    COALESCE(s.row_count, 0) as row_count,
                    COALESCE(s.size_bytes, 0) as size_bytes,
                    s.last_analyzed
                FROM sqlite_master t
                LEFT JOIN _gsql_stats s ON t.name = s.table_name
                WHERE t.type = 'table'
                AND t.name NOT LIKE '_gsql_%'
                AND t.name NOT LIKE 'sqlite_%'
                ORDER BY t.name
            """)
            
            tables = []
            for row in cursor.fetchall():
                table_info = dict(row)
                
                # Ajouter les informations de schéma
                schema = self.get_table_schema(table_info['table_name'])
                if schema:
                    table_info['columns'] = list(schema.get('columns', {}).keys())
                    table_info['index_count'] = len(schema.get('indexes', []))
                
                tables.append(table_info)
            
            return tables
            
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            return []
    
    def register_function(self, name: str, func, num_params: int = -1):
        """Enregistre une fonction Python comme fonction SQLite"""
        if not self.is_connected:
            self._connect()
        
        self.conn.create_function(name, num_params, func)
        logger.debug(f"Function registered: {name}")
    
    def vacuum(self):
        """Exécute VACUUM pour optimiser la base"""
        try:
            self.execute("VACUUM")
            logger.info("Database vacuum completed")
            return True
        except Exception as e:
            logger.error(f"Vacuum failed: {e}")
            return False
    
    def backup(self, backup_path: str = None) -> str:
        """Crée une sauvegarde de la base"""
        if backup_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = str(self.backup_dir / f"manual_backup_{timestamp}.db")
        
        import shutil
        shutil.copy2(self.db_path, backup_path)
        
        logger.info(f"Manual backup created: {backup_path}")
        return backup_path
    
    def get_stats(self) -> Dict:
        """Récupère les statistiques du système"""
        try:
            cursor = self.conn.cursor()
            
            # Statistiques de base
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0] - 5  # Exclure tables système
            
            cursor.execute("SELECT SUM(total_rows) FROM _gsql_stats")
            total_rows = cursor.fetchone()[0] or 0
            
            # Taille de la base
            db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
            
            return {
                'database': {
                    'path': str(self.db_path),
                    'size_mb': round(db_size / (1024 * 1024), 2),
                    'table_count': table_count,
                    'total_rows': total_rows,
                    'is_connected': self.is_connected,
                    'recovery_mode': self.recovery_mode
                },
                'buffer_pool': self.buffer_pool.get_stats(),
                'transactions': {
                    'active': len(self.transaction_manager.active_transactions),
                    'total': self.transaction_manager.transaction_counter
                },
                'cache': {
                    'schema_cache_size': len(self.schema_cache),
                    'query_cache_size': len(self.query_cache),
                    'table_cache_size': len(self.table_cache)
                },
                'config': self.config
            }
            
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Ferme la connexion proprement"""
        with self.connection_lock:
            try:
                # Sauvegarder les métadonnées
                self._save_metadata()
                
                # Fermer la connexion
                if self.conn:
                    self.conn.close()
                    self.is_connected = False
                
                # Vider les caches
                self.buffer_pool.invalidate()
                self.schema_cache.clear()
                self.query_cache.clear()
                
                logger.info("Storage closed")
                
            except Exception as e:
                logger.error(f"Error closing storage: {e}")
    
    def _save_metadata(self):
        """Sauvegarde les métadonnées importantes"""
        try:
            metadata = {
                'version': self.VERSION,
                'last_backup': datetime.now().isoformat(),
                'schemas': {},
                'config': self.config
            }
            
            # Sauvegarder les schémas importants
            for table_name, cache in self.schema_cache.items():
                metadata['schemas'][table_name] = cache['schema']
            
            with open(self.meta_file, 'w') as f:
                json.dump(metadata, f, indent=2)
                
        except Exception as e:
            logger.debug(f"Failed to save metadata: {e}")
    
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

def create_storage(db_path=None, **kwargs) -> SQLiteStorage:
    """Factory pour créer une instance de stockage"""
    return SQLiteStorage(db_path, **kwargs)

def get_storage_stats() -> Dict:
    """Récupère les statistiques du système de stockage global"""
    # Cette fonction pourrait gérer plusieurs instances de stockage
    return {'storage_engine': 'SQLite with Buffer Pool', 'version': SQLiteStorage.VERSION}
