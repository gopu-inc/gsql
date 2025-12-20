# gsql/gsql/storage.py
"""
Storage engine with indexing
"""

import json
import os
import pickle
from pathlib import Path

class StorageEngine:
    """Storage engine for GSQL"""
    
    def __init__(self, db_path="gsql.db"):
        self.db_path = db_path
        self.data_dir = Path(db_path).parent / f"{db_path}_data"
        self.data_dir.mkdir(exist_ok=True)
        
        # In-memory cache
        self.table_cache = {}
        self.indexes = {}  # table -> column -> BPlusTree
    
    def create_table(self, table_name, columns):
        """Create new table"""
        table_file = self.data_dir / f"{table_name}.json"
        
        structure = {
            'metadata': {
                'name': table_name,
                'columns': columns,
                'row_count': 0,
                'next_id': 1
            },
            'data': []
        }
        
        with open(table_file, 'w') as f:
            json.dump(structure, f, indent=2)
        
        self.table_cache[table_name] = structure
        self.indexes[table_name] = {}
    
    def insert(self, table_name, data):
        """Insert data"""
        table = self._load_table(table_name)
        
        if isinstance(data, dict):
            data = [data]
        
        rows_inserted = 0
        for row in data:
            row_id = table['metadata']['next_id']
            row['_id'] = row_id
            table['metadata']['next_id'] += 1
            table['metadata']['row_count'] += 1
            table['data'].append(row)
            rows_inserted += 1
            
            # Update indexes
            self._update_indexes(table_name, row_id, row)
        
        self._save_table(table_name, table)
        return rows_inserted
    
    def select(self, table_name, where=None, columns=None):
        """Select data with filtering"""
        table = self._load_table(table_name)
        results = []
        
        # Try to use index if possible
        if where and len(where) == 1:
            for col, val in where.items():
                if table_name in self.indexes and col in self.indexes[table_name]:
                    row_ids = self.indexes[table_name][col].search(val)
                    for row_id in row_ids:
                        row = self._find_row_by_id(table, row_id)
                        if row and self._row_matches(row, where):
                            results.append(self._project(row, columns))
                    return results
        
        # Sequential scan
        for row in table['data']:
            if self._row_matches(row, where):
                results.append(self._project(row, columns))
        
        return results
    
    def delete(self, table_name, where):
        """Delete data"""
        table = self._load_table(table_name)
        rows_deleted = 0
        
        to_delete = []
        for i, row in enumerate(table['data']):
            if self._row_matches(row, where):
                to_delete.append((i, row['_id']))
        
        # Delete in reverse order
        for i, row_id in reversed(to_delete):
            del table['data'][i]
            table['metadata']['row_count'] -= 1
            rows_deleted += 1
            
            # Remove from indexes
            self._remove_from_indexes(table_name, row_id)
        
        self._save_table(table_name, table)
        return rows_deleted
    
    def create_index(self, table_name, column, index_type='btree'):
        """Create index on column"""
        from .btree import BPlusTree
        
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        table = self._load_table(table_name)
        index = BPlusTree(order=3)
        
        # Build index from existing data
        for row in table['data']:
            if column in row:
                index.insert(row[column], row['_id'])
        
        self.indexes[table_name][column] = index
        self._save_index(table_name, column, index)
    
    def _load_table(self, table_name):
        """Load table from disk"""
        if table_name in self.table_cache:
            return self.table_cache[table_name]
        
        table_file = self.data_dir / f"{table_name}.json"
        if not table_file.exists():
            from .exceptions import GQLTableError
            raise GQLTableError(f"Table '{table_name}' doesn't exist")
        
        with open(table_file, 'r') as f:
            table = json.load(f)
        
        self.table_cache[table_name] = table
        return table
    
    def _save_table(self, table_name, table):
        """Save table to disk"""
        table_file = self.data_dir / f"{table_name}.json"
        with open(table_file, 'w') as f:
            json.dump(table, f, separators=(',', ':'))
        
        self.table_cache[table_name] = table
    
    def _save_index(self, table_name, column, index):
        """Save index to disk"""
        index_file = self.data_dir / f"{table_name}_{column}.idx"
        with open(index_file, 'wb') as f:
            pickle.dump(index, f)
    
    def _load_index(self, table_name, column):
        """Load index from disk"""
        index_file = self.data_dir / f"{table_name}_{column}.idx"
        if index_file.exists():
            with open(index_file, 'rb') as f:
                return pickle.load(f)
        return None
    
    def _find_row_by_id(self, table, row_id):
        """Find row by ID"""
        for row in table['data']:
            if row.get('_id') == row_id:
                return row
        return None
    
    def _row_matches(self, row, where):
        """Check if row matches WHERE conditions"""
        if not where:
            return True
        
        for col, val in where.items():
            if col not in row or row[col] != val:
                return False
        
        return True
    
    def _project(self, row, columns):
        """Project specific columns"""
        if not columns or columns == ['*']:
            # Remove internal fields
            return {k: v for k, v in row.items() if not k.startswith('_')}
        
        result = {}
        for col in columns:
            if col in row:
                result[col] = row[col]
        return result
    
    def _update_indexes(self, table_name, row_id, row):
        """Update all indexes for a row"""
        if table_name in self.indexes:
            for col, index in self.indexes[table_name].items():
                if col in row:
                    index.insert(row[col], row_id)
    
    def _remove_from_indexes(self, table_name, row_id):
        """Remove row from all indexes"""
        # For MVP, we'll just note that indexes need rebuilding
        pass
    
    def close(self):
        """Close storage engine"""
        # Save all cached tables
        for table_name, table in self.table_cache.items():
            self._save_table(table_name, table)
