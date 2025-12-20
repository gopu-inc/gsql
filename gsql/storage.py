# gsql/gsql/storage.py - SIMPLIFIÉ
"""
Storage for GSQL - Simplified
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

class PersistentStorage:
    """Persistent storage - Simplified"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.data_dir = Path(db_path).parent / f"{Path(db_path).stem}_data"
        self.data_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (self.data_dir / 'tables').mkdir(exist_ok=True)
        (self.data_dir / 'meta').mkdir(exist_ok=True)
    
    def create_table(self, name: str, columns: List[Dict]) -> None:
        """Create table"""
        table_file = self.data_dir / 'tables' / f'{name}.json'
        with open(table_file, 'w') as f:
            json.dump([], f)
        
        meta_file = self.data_dir / 'meta' / f'{name}.json'
        with open(meta_file, 'w') as f:
            json.dump({
                'name': name,
                'columns': columns,
                'created_at': datetime.now().isoformat(),
                'row_count': 0
            }, f, indent=2)
    
    def insert(self, table: str, data: Dict) -> int:
        """Insert data"""
        table_file = self.data_dir / 'tables' / f'{table}.json'
        
        if table_file.exists():
            with open(table_file, 'r') as f:
                rows = json.load(f)
        else:
            rows = []
        
        rows.append(data)
        
        with open(table_file, 'w') as f:
            json.dump(rows, f, indent=2)
        
        # Update metadata
        meta_file = self.data_dir / 'meta' / f'{table}.json'
        if meta_file.exists():
            with open(meta_file, 'r') as f:
                meta = json.load(f)
            meta['row_count'] = len(rows)
            meta['modified_at'] = datetime.now().isoformat()
            with open(meta_file, 'w') as f:
                json.dump(meta, f, indent=2)
        
        return len(rows)
    
    def select(self, table: str, where: Optional[Dict] = None,
               columns: List[str] = None, limit: int = None) -> List[Dict]:
        """Select data"""
        table_file = self.data_dir / 'tables' / f'{table}.json'
        if not table_file.exists():
            return []
        
        with open(table_file, 'r') as f:
            rows = json.load(f)
        
        results = []
        for row in rows:
            if where:
                match = all(row.get(k) == v for k, v in where.items())
                if not match:
                    continue
            
            if columns and columns != ['*']:
                filtered = {k: v for k, v in row.items() if k in columns}
                results.append(filtered)
            else:
                results.append(row)
            
            if limit and len(results) >= limit:
                break
        
        return results
    
    def list_tables(self):
        """List all tables"""
        tables_dir = self.data_dir / 'tables'
        if not tables_dir.exists():
            return []
        return [f.stem for f in tables_dir.glob('*.json')]
    
    def get_table_info(self, table):
        """Get table information"""
        meta_file = self.data_dir / 'meta' / f'{table}.json'
        if meta_file.exists():
            with open(meta_file, 'r') as f:
                return json.load(f)
        return None
    
    def close(self):
        """Close storage"""
        pass
