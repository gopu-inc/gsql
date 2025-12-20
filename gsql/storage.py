# gsql/storage.py
import json
import pickle
import os
import msgpack
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from collections import OrderedDict
from .index import BPlusTreeIndex, HashIndex

class StorageEngine:
    """Moteur de stockage avec indexation"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.data_dir = Path(db_path).parent / f"{db_path}_data"
        self.data_dir.mkdir(exist_ok=True)
        
        # Cache en mémoire
        self.table_cache = {}
        self.indexes = {}  # table -> column -> index
        
        # Charger les index existants
        self._load_indexes()
    
    def create_table(self, table_name: str, columns: List[Dict]) -> None:
        """Créer une nouvelle table"""
        table_file = self.data_dir / f"{table_name}.json"
        
        # Structure de la table
        table_structure = {
            'metadata': {
                'name': table_name,
                'columns': columns,
                'created_at': datetime.now().isoformat(),
                'row_count': 0,
                'next_id': 1
            },
            'data': [],
            'indexes': {}
        }
        
        with open(table_file, 'w', encoding='utf-8') as f:
            json.dump(table_structure, f, indent=2)
    
    def insert(self, table_name: str, data: Union[Dict, List[Dict]]) -> int:
        """Insérer des données"""
        table = self._load_table(table_name)
        rows_inserted = 0
        
        # Normaliser en liste
        if isinstance(data, dict):
            data = [data]
        
        for row in data:
            # Générer un ID unique
            row_id = table['metadata']['next_id']
            row['_id'] = row_id
            table['metadata']['next_id'] += 1
            
            # Valider les types de données
            self._validate_row(row, table['metadata']['columns'])
            
            # Ajouter la ligne
            table['data'].append(row)
            rows_inserted += 1
            
            # Mettre à jour les index
            self._update_indexes(table_name, row_id, row)
        
        # Sauvegarder
        self._save_table(table_name, table)
        return rows_inserted
    
    def select(self, table_name: str, where: Optional[Dict] = None,
               columns: List[str] = None) -> List[Dict]:
        """Sélectionner des données avec filtrage"""
        table = self._load_table(table_name)
        results = []
        
        # Vérifier si on peut utiliser un index
        if where and len(where) == 1:
            for col, value in where.items():
                if self._has_index(table_name, col):
                    row_ids = self.indexes[table_name][col].search(value)
                    for row_id in row_ids:
                        row = next((r for r in table['data'] if r['_id'] == row_id), None)
                        if row and self._matches_where(row, where):
                            results.append(self._project_columns(row, columns))
                    return results
        
        # Sinon, scan séquentiel
        for row in table['data']:
            if self._matches_where(row, where):
                results.append(self._project_columns(row, columns))
        
        return results
    
    def delete(self, table_name: str, where: Dict) -> int:
        """Supprimer des données"""
        table = self._load_table(table_name)
        rows_to_delete = []
        
        # Trouver les lignes à supprimer
        for i, row in enumerate(table['data']):
            if self._matches_where(row, where):
                rows_to_delete.append((i, row['_id']))
        
        # Supprimer en ordre inverse
        for i, row_id in reversed(rows_to_delete):
            del table['data'][i]
            # Supprimer des index
            self._remove_from_indexes(table_name, row_id)
        
        # Sauvegarder
        self._save_table(table_name, table)
        return len(rows_to_delete)
    
    def update(self, table_name: str, set_values: Dict, where: Dict) -> int:
        """Mettre à jour des données"""
        table = self._load_table(table_name)
        updated_count = 0
        
        for row in table['data']:
            if self._matches_where(row, where):
                # Supprimer l'ancienne valeur des index
                self._remove_from_indexes(table_name, row['_id'])
                
                # Mettre à jour la ligne
                row.update(set_values)
                updated_count += 1
                
                # Ajouter la nouvelle valeur aux index
                self._update_indexes(table_name, row['_id'], row)
        
        self._save_table(table_name, table)
        return updated_count
    
    def create_index(self, table_name: str, column: str, 
                    index_type: str = 'btree') -> None:
        """Créer un index sur une colonne"""
        table = self._load_table(table_name)
        
        # Initialiser l'index
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        if index_type == 'btree':
            index = BPlusTreeIndex()
        elif index_type == 'hash':
            index = HashIndex()
        else:
            raise ValueError(f"Type d'index non supporté: {index_type}")
        
        # Construire l'index avec les données existantes
        for row in table['data']:
            if column in row:
                index.insert(row[column], row['_id'])
        
        # Sauvegarder l'index
        self.indexes[table_name][column] = index
        self._save_index(table_name, column, index)
    
    def _load_table(self, table_name: str) -> Dict:
        """Charger une table depuis le disque"""
        if table_name in self.table_cache:
            return self.table_cache[table_name]
        
        table_file = self.data_dir / f"{table_name}.json"
        if not table_file.exists():
            raise FileNotFoundError(f"Table {table_name} n'existe pas")
        
        with open(table_file, 'r', encoding='utf-8') as f:
            table = json.load(f)
        
        self.table_cache[table_name] = table
        return table
    
    def _save_table(self, table_name: str, table: Dict) -> None:
        """Sauvegarder une table sur le disque"""
        table_file = self.data_dir / f"{table_name}.json"
        
        # Compacter le JSON
        with open(table_file, 'w', encoding='utf-8') as f:
            json.dump(table, f, separators=(',', ':'))
        
        self.table_cache[table_name] = table
    
    def _matches_where(self, row: Dict, where: Optional[Dict]) -> bool:
        """Vérifier si une ligne correspond aux conditions WHERE"""
        if not where:
            return True
        
        for col, value in where.items():
            if col not in row or row[col] != value:
                return False
        
        return True
    
    def _project_columns(self, row: Dict, columns: List[str]) -> Dict:
        """Sélectionner seulement certaines colonnes"""
        if not columns or columns == ['*']:
            return {k: v for k, v in row.items() if not k.startswith('_')}
        
        result = {}
        for col in columns:
            if col in row:
                result[col] = row[col]
        return result
    
    def _update_indexes(self, table_name: str, row_id: int, row: Dict) -> None:
        """Mettre à jour tous les index pour une ligne"""
        if table_name in self.indexes:
            for col, index in self.indexes[table_name].items():
                if col in row:
                    index.insert(row[col], row_id)
    
    def _remove_from_indexes(self, table_name: str, row_id: int) -> None:
        """Supprimer une ligne de tous les index"""
        if table_name in self.indexes:
            for index in self.indexes[table_name].values():
                index.remove_by_id(row_id)
    
    def _has_index(self, table_name: str, column: str) -> bool:
        """Vérifier si un index existe"""
        return (table_name in self.indexes and 
                column in self.indexes[table_name])
    
    def _save_index(self, table_name: str, column: str, index) -> None:
        """Sauvegarder un index sur le disque"""
        index_file = self.data_dir / f"{table_name}_{column}.idx"
        with open(index_file, 'wb') as f:
            pickle.dump(index, f)
    
    def _load_indexes(self) -> None:
        """Charger tous les index depuis le disque"""
        for idx_file in self.data_dir.glob("*.idx"):
            with open(idx_file, 'rb') as f:
                index = pickle.load(f)
            
            # Extraire table et colonne du nom de fichier
            parts = idx_file.stem.split('_')
            if len(parts) >= 2:
                table_name = parts[0]
                column = parts[1]
                
                if table_name not in self.indexes:
                    self.indexes[table_name] = {}
                self.indexes[table_name][column] = index
    
    def close(self) -> None:
        """Fermer le moteur de stockage"""
        # Sauvegarder tous les index
        for table_name, columns in self.indexes.items():
            for column, index in columns.items():
                self._save_index(table_name, column, index)
