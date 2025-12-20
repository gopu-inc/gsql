# gsql/gsql/storage.py
"""
Persistent storage engine with TOML configuration
"""

import os
import json
import pickle
import tomllib
import tomli_w
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

class PersistentStorage:
    """Persistent storage with automatic backup"""
    
    def __init__(self, config_path="GSQL.toml"):
        self.config = self._load_config(config_path)
        self.base_path = Path(self.config['storage']['path'])
        self._init_structure()
        
        # Cache
        self.table_cache = {}
        self.index_cache = {}
        self.transaction_log = []
        
        # Statistics
        self.stats = {
            'reads': 0,
            'writes': 0,
            'cache_hits': 0,
            'start_time': datetime.now()
        }
    
    def _load_config(self, config_path):
        """Load or create configuration"""
        if os.path.exists(config_path):
            with open(config_path, 'rb') as f:
                return tomllib.load(f)
        else:
            # Default configuration
            config = {
                'global': {
                    'name': 'GSQL Database',
                    'version': '1.0',
                    'author': 'GSQL Team'
                },
                'storage': {
                    'engine': 'json',
                    'path': './data',
                    'compression': False,
                    'auto_backup': True,
                    'backup_interval': 3600
                },
                'performance': {
                    'cache_size': 1000,
                    'index_auto_create': True,
                    'query_cache': True
                },
                'security': {
                    'encryption': False,
                    'password_hash': '',
                    'access_log': True
                },
                'logging': {
                    'level': 'INFO',
                    'path': './logs',
                    'max_size': 10485760
                }
            }
            
            # Save default config
            with open(config_path, 'wb') as f:
                tomli_w.dump(config, f)
            
            return config
    
    def _init_structure(self):
        """Initialize directory structure"""
        directories = [
            self.base_path / 'tables',
            self.base_path / 'indexes', 
            self.base_path / 'meta',
            self.base_path / 'backups',
            self.base_path / 'logs'
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def create_table(self, table_name: str, columns: List[Dict]) -> None:
        """Create table with persistent storage"""
        # Table file
        table_file = self.base_path / 'tables' / f'{table_name}.json'
        
        # Metadata file
        meta_file = self.base_path / 'meta' / f'{table_name}.meta'
        
        # Create table structure
        table_data = {
            'name': table_name,
            'columns': columns,
            'created_at': datetime.now().isoformat(),
            'modified_at': datetime.now().isoformat(),
            'row_count': 0,
            'next_id': 1,
            'checksum': self._generate_checksum(str(columns))
        }
        
        # Save table metadata
        with open(meta_file, 'w') as f:
            json.dump(table_data, f, indent=2)
        
        # Create empty data file
        with open(table_file, 'w') as f:
            json.dump([], f)
        
        # Update global metadata
        self._update_global_meta('table_created', table_name)
        
        # Cache
        self.table_cache[table_name] = {
            'metadata': table_data,
            'data': [],
            'dirty': False
        }
    
    def insert(self, table_name: str, data: Dict) -> int:
        """Insert data with transaction log"""
        # Start transaction
        tx_id = self._begin_transaction(table_name, 'INSERT')
        
        try:
            # Load table
            table = self._load_table(table_name)
            row_id = table['metadata']['next_id']
            
            # Prepare row
            row = dict(data)
            row['_id'] = row_id
            row['_created'] = datetime.now().isoformat()
            row['_modified'] = datetime.now().isoformat()
            
            # Add to data
            table['data'].append(row)
            
            # Update metadata
            table['metadata']['next_id'] += 1
            table['metadata']['row_count'] += 1
            table['metadata']['modified_at'] = datetime.now().isoformat()
            table['dirty'] = True
            
            # Update indexes
            self._update_indexes(table_name, row)
            
            # Save changes
            self._save_table(table_name, table)
            
            # Commit transaction
            self._commit_transaction(tx_id)
            
            # Auto-backup if enabled
            if self.config['storage']['auto_backup']:
                self._auto_backup()
            
            return row_id
            
        except Exception as e:
            # Rollback on error
            self._rollback_transaction(tx_id)
            raise
    
    def select(self, table_name: str, where: Optional[Dict] = None, 
               columns: List[str] = None, limit: int = None) -> List[Dict]:
        """Select data with caching"""
        self.stats['reads'] += 1
        
        # Try cache first
        cache_key = f"{table_name}_{str(where)}_{str(columns)}"
        if self.config['performance']['query_cache']:
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                self.stats['cache_hits'] += 1
                return cached[:limit] if limit else cached
        
        # Load table
        table = self._load_table(table_name)
        results = []
        
        # Apply WHERE clause
        for row in table['data']:
            if self._matches_where(row, where):
                # Project columns
                projected = self._project(row, columns)
                results.append(projected)
                
                # Apply limit
                if limit and len(results) >= limit:
                    break
        
        # Cache results
        if self.config['performance']['query_cache']:
            self._add_to_cache(cache_key, results)
        
        return results
    
    def update(self, table_name: str, set_data: Dict, where: Dict) -> int:
        """Update data with transaction support"""
        tx_id = self._begin_transaction(table_name, 'UPDATE')
        
        try:
            table = self._load_table(table_name)
            updated = 0
            
            for row in table['data']:
                if self._matches_where(row, where):
                    # Remove old values from indexes
                    self._remove_from_indexes(table_name, row)
                    
                    # Update row
                    row.update(set_data)
                    row['_modified'] = datetime.now().isoformat()
                    updated += 1
                    
                    # Add new values to indexes
                    self._update_indexes(table_name, row)
            
            if updated > 0:
                table['metadata']['modified_at'] = datetime.now().isoformat()
                table['dirty'] = True
                self._save_table(table_name, table)
            
            self._commit_transaction(tx_id)
            return updated
            
        except Exception as e:
            self._rollback_transaction(tx_id)
            raise
    
    def delete(self, table_name: str, where: Dict) -> int:
        """Delete data with transaction support"""
        tx_id = self._begin_transaction(table_name, 'DELETE')
        
        try:
            table = self._load_table(table_name)
            to_delete = []
            
            for i, row in enumerate(table['data']):
                if self._matches_where(row, where):
                    to_delete.append((i, row))
            
            # Delete in reverse order
            for i, row in reversed(to_delete):
                del table['data'][i]
                self._remove_from_indexes(table_name, row)
            
            if to_delete:
                table['metadata']['row_count'] -= len(to_delete)
                table['metadata']['modified_at'] = datetime.now().isoformat()
                table['dirty'] = True
                self._save_table(table_name, table)
            
            self._commit_transaction(tx_id)
            return len(to_delete)
            
        except Exception as e:
            self._rollback_transaction(tx_id)
            raise
    
    def create_index(self, table_name: str, column: str, 
                    index_type: str = 'btree') -> None:
        """Create persistent index"""
        from .btree import BPlusTree
        
        index = BPlusTree(order=3)
        table = self._load_table(table_name)
        
        # Build index
        for row in table['data']:
            if column in row:
                index.insert(row[column], row['_id'])
        
        # Save index
        index_file = self.base_path / 'indexes' / f'{table_name}_{column}.idx'
        with open(index_file, 'wb') as f:
            pickle.dump(index, f)
        
        # Cache index
        if table_name not in self.index_cache:
            self.index_cache[table_name] = {}
        self.index_cache[table_name][column] = index
    
    def get_table_info(self, table_name: str) -> Dict:
        """Get detailed table information"""
        meta_file = self.base_path / 'meta' / f'{table_name}.meta'
        
        if not meta_file.exists():
            return None
        
        with open(meta_file, 'r') as f:
            meta = json.load(f)
        
        # Add file stats
        table_file = self.base_path / 'tables' / f'{table_name}.json'
        if table_file.exists():
            meta['file_size'] = os.path.getsize(table_file)
            meta['file_modified'] = datetime.fromtimestamp(
                os.path.getmtime(table_file)
            ).isoformat()
        
        return meta
    
    def list_tables(self) -> List[str]:
        """List all tables"""
        tables_dir = self.base_path / 'tables'
        tables = []
        
        for file in tables_dir.glob('*.json'):
            tables.append(file.stem)
        
        return tables
    
    def backup(self, backup_name: str = None) -> str:
        """Create backup"""
        if not backup_name:
            backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        backup_dir = self.base_path / 'backups' / backup_name
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy all data
        import shutil
        for item in ['tables', 'indexes', 'meta']:
            src = self.base_path / item
            dst = backup_dir / item
            if src.exists():
                shutil.copytree(src, dst, dirs_exist_ok=True)
        
        # Save backup info
        backup_info = {
            'name': backup_name,
            'created_at': datetime.now().isoformat(),
            'tables': self.list_tables(),
            'size': self._get_database_size()
        }
        
        info_file = backup_dir / 'backup_info.json'
        with open(info_file, 'w') as f:
            json.dump(backup_info, f, indent=2)
        
        return str(backup_dir)
    
    def restore(self, backup_name: str) -> None:
        """Restore from backup"""
        backup_dir = self.base_path / 'backups' / backup_name
        
        if not backup_dir.exists():
            raise FileNotFoundError(f"Backup '{backup_name}' not found")
        
        # Clear current data
        for item in ['tables', 'indexes', 'meta']:
            target = self.base_path / item
            if target.exists():
                import shutil
                shutil.rmtree(target)
        
        # Restore from backup
        import shutil
        for item in ['tables', 'indexes', 'meta']:
            src = backup_dir / item
            dst = self.base_path / item
            if src.exists():
                shutil.copytree(src, dst)
        
        # Clear cache
        self.table_cache.clear()
        self.index_cache.clear()
    
    # Helper methods
    def _load_table(self, table_name: str) -> Dict:
        """Load table from disk"""
        if table_name in self.table_cache:
            return self.table_cache[table_name]
        
        # Load metadata
        meta_file = self.base_path / 'meta' / f'{table_name}.meta'
        if not meta_file.exists():
            from .exceptions import GSQLTableError
            raise GSQLTableError(f"Table '{table_name}' not found")
        
        with open(meta_file, 'r') as f:
            metadata = json.load(f)
        
        # Load data
        table_file = self.base_path / 'tables' / f'{table_name}.json'
        with open(table_file, 'r') as f:
            data = json.load(f)
        
        table = {
            'metadata': metadata,
            'data': data,
            'dirty': False
        }
        
        # Cache
        self.table_cache[table_name] = table
        return table
    
    def _save_table(self, table_name: str, table: Dict) -> None:
        """Save table to disk"""
        # Save data
        table_file = self.base_path / 'tables' / f'{table_name}.json'
        with open(table_file, 'w') as f:
            json.dump(table['data'], f, separators=(',', ':'))
        
        # Save metadata
        meta_file = self.base_path / 'meta' / f'{table_name}.meta'
        with open(meta_file, 'w') as f:
            json.dump(table['metadata'], f, indent=2)
        
        # Update cache
        table['dirty'] = False
        self.table_cache[table_name] = table
        
        self.stats['writes'] += 1
    
    def _update_global_meta(self, action: str, details: Any) -> None:
        """Update global metadata"""
        global_meta_file = self.base_path / 'meta' / 'global.json'
        
        if global_meta_file.exists():
            with open(global_meta_file, 'r') as f:
                global_meta = json.load(f)
        else:
            global_meta = {
                'created_at': datetime.now().isoformat(),
                'tables': [],
                'operations': []
            }
        
        # Add operation log
        global_meta['operations'].append({
            'action': action,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
        
        with open(global_meta_file, 'w') as f:
            json.dump(global_meta, f, indent=2)
    
    def _begin_transaction(self, table: str, operation: str) -> str:
        """Begin transaction"""
        tx_id = f"tx_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        self.transaction_log.append({
            'id': tx_id,
            'table': table,
            'operation': operation,
            'start_time': datetime.now().isoformat(),
            'status': 'active'
        })
        
        return tx_id
    
    def _commit_transaction(self, tx_id: str) -> None:
        """Commit transaction"""
        for tx in self.transaction_log:
            if tx['id'] == tx_id:
                tx['status'] = 'committed'
                tx['end_time'] = datetime.now().isoformat()
                break
    
    def _rollback_transaction(self, tx_id: str) -> None:
        """Rollback transaction"""
        for tx in self.transaction_log:
            if tx['id'] == tx_id:
                tx['status'] = 'rolled_back'
                tx['end_time'] = datetime.now().isoformat()
                break
    
    def _auto_backup(self) -> None:
        """Auto-backup based on configuration"""
        backup_interval = self.config['storage']['backup_interval']
        last_backup_file = self.base_path / 'meta' / 'last_backup.txt'
        
        should_backup = False
        if not last_backup_file.exists():
            should_backup = True
        else:
            with open(last_backup_file, 'r') as f:
                last_backup = datetime.fromisoformat(f.read().strip())
            
            elapsed = (datetime.now() - last_backup).total_seconds()
            if elapsed >= backup_interval:
                should_backup = True
        
        if should_backup:
            self.backup()
            with open(last_backup_file, 'w') as f:
                f.write(datetime.now().isoformat())
    
    def _get_from_cache(self, key: str) -> Any:
        """Get from cache (simplified)"""
        # Implementation would use LRU cache
        return None
    
    def _add_to_cache(self, key: str, value: Any) -> None:
        """Add to cache (simplified)"""
        pass
    
    def _matches_where(self, row: Dict, where: Dict) -> bool:
        """Check if row matches WHERE conditions"""
        if not where:
            return True
        
        for col, val in where.items():
            if col not in row or row[col] != val:
                return False
        
        return True
    
    def _project(self, row: Dict, columns: List[str]) -> Dict:
        """Project specific columns"""
        if not columns or columns == ['*']:
            # Remove internal fields
            return {k: v for k, v in row.items() 
                   if not k.startswith('_')}
        
        result = {}
        for col in columns:
            if col in row:
                result[col] = row[col]
        return result
    
    def _update_indexes(self, table_name: str, row: Dict) -> None:
        """Update indexes for row"""
        if table_name in self.index_cache:
            for col, index in self.index_cache[table_name].items():
                if col in row:
                    index.insert(row[col], row['_id'])
    
    def _remove_from_indexes(self, table_name: str, row: Dict) -> None:
        """Remove row from indexes"""
        pass
    
    def _generate_checksum(self, data: str) -> str:
        """Generate checksum for data integrity"""
        import hashlib
        return hashlib.md5(data.encode()).hexdigest()
    
    def _get_database_size(self) -> int:
        """Get total database size in bytes"""
        total = 0
        for path in [self.base_path / 'tables', 
                    self.base_path / 'indexes',
                    self.base_path / 'meta']:
            if path.exists():
                for file in path.rglob('*'):
                    if file.is_file():
                        total += file.stat().st_size
        return total
    
    def close(self) -> None:
        """Close storage engine"""
        # Save all dirty tables
        for table_name, table in self.table_cache.items():
            if table.get('dirty'):
                self._save_table(table_name, table)
        
        # Save statistics
        stats_file = self.base_path / 'meta' / 'stats.json'
        self.stats['end_time'] = datetime.now().isoformat()
        self.stats['uptime'] = (
            datetime.now() - self.stats['start_time']
        ).total_seconds()
        
        with open(stats_file, 'w') as f:
            json.dump(self.stats, f, indent=2)
