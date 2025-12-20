# gsql/database.py
import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from .parser import SQLParser
from .storage import StorageEngine
from .executor import QueryExecutor
from .exceptions import GSQLSyntaxError, GSQLExecutionError

class GSQL:
    """Base de données GSQL - Simple mais puissante"""
    
    def __init__(self, path: Optional[str] = None):
        """
        Initialiser GSQL
        Args:
            path: Chemin vers le fichier de base de données (optionnel)
        """
        self.path = path or "gsql.db"
        self.storage = StorageEngine(self.path)
        self.parser = SQLParser()
        self.executor = QueryExecutor(self.storage)
        
        # Métadonnées en mémoire
        self.tables: Dict[str, Dict] = {}
        self.transaction_active = False
        self.load_metadata()
    
    def __enter__(self):
        """Support context manager"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fermer proprement"""
        self.close()
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """
        Exécuter une requête SQL
        Returns:
            Dict avec 'type', 'data', 'rows_affected', etc.
        """
        try:
            # 1. Parser
            ast = self.parser.parse(sql)
            
            # 2. Exécuter
            result = self.executor.execute(ast)
            
            # 3. Sauvegarder si nécessaire
            if ast['type'] in ['CREATE_TABLE', 'INSERT', 'UPDATE', 'DELETE']:
                self.save_metadata()
            
            return result
            
        except Exception as e:
            raise GSQLExecutionError(f"Erreur d'exécution: {str(e)}")
    
    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Exécuter une requête SELECT et retourner les résultats"""
        result = self.execute(sql)
        return result.get('data', [])
    
    def create_table(self, name: str, columns: List[Dict]) -> None:
        """Créer une table directement (API Python)"""
        self.storage.create_table(name, columns)
        self.tables[name] = {
            'columns': columns,
            'indexes': {},
            'row_count': 0
        }
        self.save_metadata()
    
    def insert(self, table: str, data: Union[Dict, List[Dict]]) -> int:
        """Insérer des données (API Python)"""
        return self.storage.insert(table, data)
    
    def select(self, table: str, where: Optional[Dict] = None, 
               columns: List[str] = None) -> List[Dict]:
        """Sélectionner des données (API Python)"""
        return self.storage.select(table, where, columns)
    
    def create_index(self, table: str, column: str, 
                    index_type: str = 'btree') -> None:
        """Créer un index sur une colonne"""
        self.storage.create_index(table, column, index_type)
        if table in self.tables:
            self.tables[table]['indexes'][column] = index_type
    
    def export_json(self, path: str) -> None:
        """Exporter toute la base en JSON"""
        data = {
            'tables': self.tables,
            'data': self.storage.export_all_data()
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    
    def import_json(self, path: str) -> None:
        """Importer depuis JSON"""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for table_name, table_data in data['data'].items():
            self.create_table(table_name, table_data.get('columns', []))
            self.insert(table_name, table_data.get('rows', []))
    
    def load_metadata(self) -> None:
        """Charger les métadonnées"""
        meta_path = f"{self.path}.meta"
        if os.path.exists(meta_path):
            with open(meta_path, 'r', encoding='utf-8') as f:
                self.tables = json.load(f)
    
    def save_metadata(self) -> None:
        """Sauvegarder les métadonnées"""
        meta_path = f"{self.path}.meta"
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(self.tables, f, indent=2)
    
    def close(self) -> None:
        """Fermer la base de données"""
        self.save_metadata()
        self.storage.close()
    
    # Fonctions magiques pour une API Pythonique
    def __getitem__(self, table_name: str) -> 'TableProxy':
        """Accès aux tables via db['users']"""
        return TableProxy(self, table_name)
    
    def __contains__(self, table_name: str) -> bool:
        """Vérifier si une table existe"""
        return table_name in self.tables
    
    @property
    def table_names(self) -> List[str]:
        """Liste des tables"""
        return list(self.tables.keys())


class TableProxy:
    """Proxy pour accès Pythonique aux tables"""
    
    def __init__(self, db: GSQL, table: str):
        self.db = db
        self.table = table
    
    def insert(self, **kwargs) -> int:
        """Insérer une ligne: table.insert(name='Alice', age=25)"""
        return self.db.insert(self.table, kwargs)
    
    def find(self, **kwargs) -> List[Dict]:
        """Trouver des lignes: table.find(age=25)"""
        return self.db.select(self.table, kwargs)
    
    def all(self) -> List[Dict]:
        """Toutes les lignes"""
        return self.db.select(self.table)
