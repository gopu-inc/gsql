import os
import pickle
import threading
from collections import OrderedDict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class BufferPool:
    """Cache de pages en mémoire avec politique LRU"""
    
    def __init__(self, max_pages=100):
        self.max_pages = max_pages
        self.pool = OrderedDict()
        self.lock = threading.RLock()
        self.hits = 0
        self.misses = 0
        
    def get(self, page_id):
        """Récupère une page du cache"""
        with self.lock:
            if page_id in self.pool:
                page = self.pool.pop(page_id)
                self.pool[page_id] = page
                self.hits += 1
                return page.copy()
            self.misses += 1
        return None
    
    def put(self, page_id, page_data):
        """Ajoute une page au cache"""
        with self.lock:
            if page_id in self.pool:
                self.pool.pop(page_id)
            elif len(self.pool) >= self.max_pages:
                self.pool.popitem(last=False)
            self.pool[page_id] = page_data.copy()
    
    def stats(self):
        """Retourne les statistiques du cache"""
        return {
            'size': len(self.pool),
            'max_size': self.max_pages,
            'hits': self.hits,
            'misses': self.misses,
            'hit_ratio': self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0
        }

class TransactionManager:
    """Gestion des transactions ACID"""
    
    def __init__(self, storage):
        self.storage = storage
        self.active_transactions = {}
        self.transaction_counter = 0
        self.lock = threading.RLock()
        
    def begin(self):
        """Démarre une nouvelle transaction"""
        with self.lock:
            tid = self.transaction_counter
            self.transaction_counter += 1
            self.active_transactions[tid] = {
                'start_time': datetime.now(),
                'changes': {},
                'rollback_log': []
            }
            logger.info(f"Transaction {tid} started")
            return tid
    
    def commit(self, tid):
        """Valide une transaction"""
        with self.lock:
            if tid not in self.active_transactions:
                raise TransactionError(f"Transaction {tid} not found")
            
            try:
                # Applique tous les changements
                for page_id, data in self.active_transactions[tid]['changes'].items():
                    self.storage.write_page(page_id, data)
                
                # Nettoie les logs
                del self.active_transactions[tid]
                logger.info(f"Transaction {tid} committed")
                
            except Exception as e:
                self.rollback(tid)
                raise TransactionError(f"Commit failed: {str(e)}")
    
    def rollback(self, tid):
        """Annule une transaction"""
        with self.lock:
            if tid not in self.active_transactions:
                raise TransactionError(f"Transaction {tid} not found")
            
            # Restaure l'état précédent
            for log_entry in reversed(self.active_transactions[tid]['rollback_log']):
                self.storage.write_page(log_entry['page_id'], log_entry['old_data'])
            
            del self.active_transactions[tid]
            logger.info(f"Transaction {tid} rolled back")

class StorageEngine:
    """Moteur de stockage avec buffer pool et transactions"""
    
    def __init__(self, db_path, page_size=4096, buffer_pool_size=100):
        self.db_path = db_path
        self.page_size = page_size
        self.buffer_pool = BufferPool(max_pages=buffer_pool_size)
        self.transaction_manager = TransactionManager(self)
        self.lock = threading.RLock()
        
        # Créé le fichier s'il n'existe pas
        if not os.path.exists(db_path):
            self._initialize_db()
    
    def _initialize_db(self):
        """Initialise une nouvelle base de données"""
        with open(self.db_path, 'wb') as f:
            # En-tête de la base de données
            header = {
                'page_size': self.page_size,
                'created_at': datetime.now().isoformat(),
                'version': '1.0'
            }
            pickle.dump(header, f)
            f.write(b'\x00' * (self.page_size - f.tell()))
    
    def read_page(self, page_id):
        """Lit une page depuis le disque ou le cache"""
        # D'abord, vérifier le cache
        cached = self.buffer_pool.get(page_id)
        if cached:
            return cached
        
        # Lecture depuis le disque
        with self.lock:
            with open(self.db_path, 'rb') as f:
                f.seek(page_id * self.page_size)
                data = f.read(self.page_size)
                
                # Mettre en cache
                self.buffer_pool.put(page_id, data)
                return data
    
    def write_page(self, page_id, data):
        """Écrit une page (utiliser dans une transaction)"""
        with self.lock:
            # Log pour rollback
            tid = self._get_current_transaction()
            if tid is not None:
                old_data = self.read_page(page_id)
                self.transaction_manager.active_transactions[tid]['rollback_log'].append({
                    'page_id': page_id,
                    'old_data': old_data
                })
                
                # Stocker le changement
                self.transaction_manager.active_transactions[tid]['changes'][page_id] = data
            
            # Écriture immédiate (WAL serait mieux)
            with open(self.db_path, 'r+b') as f:
                f.seek(page_id * self.page_size)
                f.write(data)
    
    def _get_current_transaction(self):
        """Récupère l'ID de la transaction courante"""
        for tid in self.transaction_manager.active_transactions:
            return tid
        return None
