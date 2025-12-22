#!/usr/bin/env python3
"""
Stockage YAML pour GSQL
"""

import os
import yaml
import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class YAMLStorage:
    """Système de stockage basé sur YAML"""
    
    def __init__(self, db_path: str, config_path: str = None):
        """
        Initialise le stockage YAML
        
        Args:
            db_path: Chemin vers le fichier de base de données
            config_path: Chemin vers le fichier de configuration
        """
        self.db_path = Path(db_path)
        self.config_path = Path(config_path) if config_path else Path(__file__).parent.parent / 'config' / 'defaults.yaml'
        
        # Créer le répertoire si nécessaire
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Charger la configuration
        self.config = self._load_config()
        
        # Initialiser la structure de la base
        self._init_database()
        
        logger.info(f"YAML Storage initialized: {db_path}")
    
    def _load_config(self) -> Dict:
        """Charge la configuration depuis YAML"""
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Could not load config: {e}")
        
        # Configuration par défaut
        return {
            'database': {
                'version': '2.0',
                'encoding': 'utf-8'
            },
            'storage': {
                'format': 'yaml',
                'auto_save': True
            }
        }
    
    def _init_database(self):
        """Initialise la structure de la base de données"""
        if not self.db_path.exists():
            self._create_new_database()
        else:
            self._load_database()
    
    def _create_new_database(self):
        """Crée une nouvelle base de données"""
        self.data = {
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'version': self.config.get('database', {}).get('version', '2.0'),
                'encoding': 'utf-8',
                'last_modified': datetime.now().isoformat()
            },
            'schemas': {
                # Schémas par défaut
                'users': {
                    'columns': {
                        'id': {'type': 'INTEGER', 'primary_key': True},
                        'name': {'type': 'TEXT'},
                        'created_at': {'type': 'TIMESTAMP', 'default': 'CURRENT_TIMESTAMP'}
                    },
                    'indexes': [],
                    'constraints': []
                }
            },
            'tables': {},  # Données des tables
            'functions': {
                'builtins': self.config.get('functions', {}).get('builtins', []),
                'user': []  # Fonctions utilisateur
            },
            'indexes': {},  # Indexes
            'triggers': {}, # Triggers
            'views': {},    # Vues
            'transactions': {
                'active': [],
                'history': []
            },
            'nlp_patterns': self.config.get('nlp', {}).get('patterns', {})
        }
        
        self._save_database()
    
    def _load_database(self):
        """Charge la base de données depuis le fichier"""
        try:
            with open(self.db_path, 'r', encoding='utf-8') as f:
                self.data = yaml.safe_load(f) or {}
            
            # S'assurer que toutes les sections existent
            defaults = {
                'metadata': {},
                'schemas': {},
                'tables': {},
                'functions': {'builtins': [], 'user': []},
                'indexes': {},
                'triggers': {},
                'views': {},
                'transactions': {'active': [], 'history': []},
                'nlp_patterns': {}
            }
            
            for key, value in defaults.items():
                if key not in self.data:
                    self.data[key] = value
            
            logger.info(f"Database loaded from {self.db_path}")
            
        except Exception as e:
            logger.error(f"Error loading database: {e}")
            # Créer une nouvelle base en cas d'erreur
            self._create_new_database()
    
    def _save_database(self):
        """Sauvegarde la base de données dans le fichier"""
        try:
            # Mettre à jour les métadonnées
            self.data['metadata']['last_modified'] = datetime.now().isoformat()
            self.data['metadata']['size'] = len(str(self.data))
            
            # Sauvegarder
            with open(self.db_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            
            # Sauvegarde de sécurité
            self._create_backup()
            
            logger.debug(f"Database saved to {self.db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving database: {e}")
            return False
    
    def _create_backup(self):
        """Crée une sauvegarde"""
        backup_dir = self.db_path.parent / 'backups'
        backup_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = backup_dir / f"{self.db_path.stem}_{timestamp}.yaml"
        
        try:
            with open(backup_file, 'w', encoding='utf-8') as f:
                yaml.dump(self.data, f, default_flow_style=False, allow_unicode=True)
            
            # Garder seulement les 5 dernières sauvegardes
            backups = sorted(backup_dir.glob("*.yaml"))
            if len(backups) > 5:
                for old_backup in backups[:-5]:
                    old_backup.unlink()
                    
        except Exception as e:
            logger.warning(f"Could not create backup: {e}")
    
    # === GESTION DES TABLES ===
    
    def create_table(self, table_name: str, columns: Dict) -> bool:
        """
        Crée une nouvelle table
        
        Args:
            table_name: Nom de la table
            columns: Définition des colonnes
            
        Returns:
            bool: Succès de l'opération
        """
        if table_name in self.data['schemas']:
            logger.warning(f"Table {table_name} already exists")
            return False
        
        # Valider les colonnes
        validated_columns = {}
        for col_name, col_def in columns.items():
            if isinstance(col_def, str):
                validated_columns[col_name] = {'type': col_def}
            elif isinstance(col_def, dict):
                validated_columns[col_name] = col_def
            else:
                logger.error(f"Invalid column definition for {col_name}")
                return False
        
        # Ajouter le schéma
        self.data['schemas'][table_name] = {
            'columns': validated_columns,
            'created_at': datetime.now().isoformat(),
            'indexes': [],
            'constraints': []
        }
        
        # Initialiser les données
        self.data['tables'][table_name] = []
        
        self._save_database()
        logger.info(f"Table {table_name} created")
        return True
    
    def drop_table(self, table_name: str) -> bool:
        """Supprime une table"""
        if table_name not in self.data['schemas']:
            logger.warning(f"Table {table_name} does not exist")
            return False
        
        del self.data['schemas'][table_name]
        del self.data['tables'][table_name]
        
        # Supprimer les indexes associés
        indexes_to_remove = []
        for idx_name, idx_def in self.data['indexes'].items():
            if idx_def.get('table') == table_name:
                indexes_to_remove.append(idx_name)
        
        for idx_name in indexes_to_remove:
            del self.data['indexes'][idx_name]
        
        self._save_database()
        logger.info(f"Table {table_name} dropped")
        return True
    
    def get_table_schema(self, table_name: str) -> Optional[Dict]:
        """Récupère le schéma d'une table"""
        return self.data['schemas'].get(table_name)
    
    def list_tables(self) -> List[Dict]:
        """Liste toutes les tables"""
        tables = []
        for table_name, schema in self.data['schemas'].items():
            row_count = len(self.data['tables'].get(table_name, []))
            tables.append({
                'table': table_name,
                'rows': row_count,
                'created_at': schema.get('created_at', 'unknown'),
                'columns': list(schema['columns'].keys())
            })
        return tables
    
    # === GESTION DES DONNÉES ===
    
    def insert_row(self, table_name: str, values: Dict) -> int:
        """
        Insère une ligne dans une table
        
        Args:
            table_name: Nom de la table
            values: Valeurs à insérer
            
        Returns:
            int: ID de la ligne insérée ou -1 en cas d'erreur
        """
        if table_name not in self.data['tables']:
            logger.error(f"Table {table_name} does not exist")
            return -1
        
        # Valider les données selon le schéma
        schema = self.data['schemas'][table_name]['columns']
        validated_row = {}
        
        for col_name, col_def in schema.items():
            if col_name in values:
                # Convertir le type
                value = self._convert_value(values[col_name], col_def['type'])
                validated_row[col_name] = value
            elif 'default' in col_def:
                # Utiliser la valeur par défaut
                validated_row[col_name] = self._convert_value(col_def['default'], col_def['type'])
            elif col_def.get('not_null', False):
                logger.error(f"Column {col_name} cannot be NULL")
                return -1
            else:
                validated_row[col_name] = None
        
        # Générer un ID si c'est une clé primaire auto-incrément
        for col_name, col_def in schema.items():
            if col_def.get('primary_key', False) and col_def.get('auto_increment', False):
                if col_name not in validated_row or validated_row[col_name] is None:
                    # Générer un nouvel ID
                    existing_ids = [row.get(col_name, 0) for row in self.data['tables'][table_name] 
                                  if row.get(col_name) is not None]
                    new_id = max(existing_ids, default=0) + 1
                    validated_row[col_name] = new_id
        
        # Ajouter des métadonnées
        validated_row['_id'] = len(self.data['tables'][table_name])
        validated_row['_created_at'] = datetime.now().isoformat()
        
        # Insérer
        self.data['tables'][table_name].append(validated_row)
        
        self._save_database()
        logger.debug(f"Row inserted into {table_name}")
        return validated_row.get('id', validated_row['_id'])
    
    def _convert_value(self, value, target_type: str):
        """Convertit une valeur vers un type cible"""
        if value is None:
            return None
        
        target_type = target_type.upper()
        
        try:
            if target_type in ['INTEGER', 'INT', 'BIGINT']:
                return int(value)
            elif target_type in ['REAL', 'FLOAT', 'DOUBLE']:
                return float(value)
            elif target_type in ['TEXT', 'VARCHAR', 'CHAR']:
                return str(value)
            elif target_type == 'BOOLEAN':
                if isinstance(value, str):
                    return value.lower() in ['true', '1', 'yes', 'y']
                return bool(value)
            elif target_type in ['TIMESTAMP', 'DATETIME']:
                if isinstance(value, str):
                    # Essayer de parser la date
                    try:
                        return datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except:
                        return value
                return value
            else:
                return value
        except (ValueError, TypeError):
            return value
    
    def select_rows(self, table_name: str, conditions: Dict = None, 
                   columns: List[str] = None, limit: int = None, 
                   offset: int = 0) -> List[Dict]:
        """
        Sélectionne des lignes d'une table
        
        Args:
            table_name: Nom de la table
            conditions: Conditions WHERE
            columns: Colonnes à retourner
            limit: Nombre maximum de résultats
            offset: Décalage
            
        Returns:
            List[Dict]: Lignes sélectionnées
        """
        if table_name not in self.data['tables']:
            return []
        
        rows = self.data['tables'][table_name]
        
        # Filtrer selon les conditions
        if conditions:
            filtered_rows = []
            for row in rows:
                match = True
                for col, value in conditions.items():
                    if col not in row or row[col] != value:
                        match = False
                        break
                if match:
                    filtered_rows.append(row)
            rows = filtered_rows
        
        # Sélectionner des colonnes spécifiques
        if columns:
            result = []
            for row in rows:
                filtered_row = {}
                for col in columns:
                    if col in row:
                        filtered_row[col] = row[col]
                result.append(filtered_row)
            rows = result
        
        # Appliquer limit et offset
        start = offset
        end = None if limit is None else offset + limit
        rows = rows[start:end]
        
        return rows
    
    def update_rows(self, table_name: str, values: Dict, conditions: Dict = None) -> int:
        """
        Met à jour des lignes
        
        Returns:
            int: Nombre de lignes mises à jour
        """
        if table_name not in self.data['tables']:
            return 0
        
        updated_count = 0
        for row in self.data['tables'][table_name]:
            # Vérifier les conditions
            if conditions:
                match = True
                for col, value in conditions.items():
                    if col not in row or row[col] != value:
                        match = False
                        break
                if not match:
                    continue
            
            # Mettre à jour les valeurs
            for col, value in values.items():
                if col in self.data['schemas'][table_name]['columns']:
                    row[col] = value
            
            row['_updated_at'] = datetime.now().isoformat()
            updated_count += 1
        
        if updated_count > 0:
            self._save_database()
        
        return updated_count
    
    def delete_rows(self, table_name: str, conditions: Dict = None) -> int:
        """
        Supprime des lignes
        
        Returns:
            int: Nombre de lignes supprimées
        """
        if table_name not in self.data['tables']:
            return 0
        
        if not conditions:
            # Supprimer toutes les lignes
            count = len(self.data['tables'][table_name])
            self.data['tables'][table_name] = []
            self._save_database()
            return count
        
        # Supprimer selon les conditions
        rows_to_keep = []
        deleted_count = 0
        
        for row in self.data['tables'][table_name]:
            match = True
            for col, value in conditions.items():
                if col not in row or row[col] != value:
                    match = False
                    break
            
            if match:
                deleted_count += 1
            else:
                rows_to_keep.append(row)
        
        self.data['tables'][table_name] = rows_to_keep
        
        if deleted_count > 0:
            self._save_database()
        
        return deleted_count
    
    # === GESTION DES FONCTIONS ===
    
    def create_function(self, func_def: Dict) -> bool:
        """
        Crée une nouvelle fonction utilisateur
        
        Args:
            func_def: Définition de la fonction
            
        Returns:
            bool: Succès de l'opération
        """
        func_name = func_def.get('name')
        if not func_name:
            logger.error("Function name is required")
            return False
        
        # Vérifier si la fonction existe déjà
        for func in self.data['functions']['user']:
            if func['name'] == func_name:
                logger.warning(f"Function {func_name} already exists")
                return False
        
        # Ajouter des métadonnées
        func_def['created_at'] = datetime.now().isoformat()
        func_def['type'] = 'user'
        
        self.data['functions']['user'].append(func_def)
        self._save_database()
        
        logger.info(f"Function {func_name} created")
        return True
    
    def get_function(self, func_name: str) -> Optional[Dict]:
        """Récupère une fonction par son nom"""
        # Chercher dans les fonctions utilisateur
        for func in self.data['functions']['user']:
            if func['name'] == func_name:
                return func
        
        # Chercher dans les fonctions intégrées
        for func in self.data['functions']['builtins']:
            if func['name'] == func_name:
                return func
        
        return None
    
    def list_functions(self) -> List[Dict]:
        """Liste toutes les fonctions"""
        functions = []
        
        # Fonctions intégrées
        for func in self.data['functions']['builtins']:
            functions.append({
                'name': func['name'],
                'type': 'builtin',
                'params': func.get('params', []),
                'returns': func.get('returns', 'ANY'),
                'description': func.get('description', 'Built-in function')
            })
        
        # Fonctions utilisateur
        for func in self.data['functions']['user']:
            functions.append({
                'name': func['name'],
                'type': 'user',
                'params': func.get('params', []),
                'returns': func.get('returns', 'ANY'),
                'created_at': func.get('created_at'),
                'description': func.get('description', 'User-defined function')
            })
        
        return functions
    
    def drop_function(self, func_name: str) -> bool:
        """Supprime une fonction utilisateur"""
        for i, func in enumerate(self.data['functions']['user']):
            if func['name'] == func_name:
                del self.data['functions']['user'][i]
                self._save_database()
                logger.info(f"Function {func_name} dropped")
                return True
        
        logger.warning(f"Function {func_name} not found")
        return False
    
    # === GESTION DES INDEXES ===
    
    def create_index(self, index_name: str, table_name: str, columns: List[str]) -> bool:
        """Crée un index"""
        if index_name in self.data['indexes']:
            logger.warning(f"Index {index_name} already exists")
            return False
        
        self.data['indexes'][index_name] = {
            'table': table_name,
            'columns': columns,
            'created_at': datetime.now().isoformat()
        }
        
        # Ajouter la référence dans le schéma de la table
        if table_name in self.data['schemas']:
            self.data['schemas'][table_name]['indexes'].append(index_name)
        
        self._save_database()
        logger.info(f"Index {index_name} created on {table_name}({', '.join(columns)})")
        return True
    
    # === GESTION NLP ===
    
    def add_nlp_pattern(self, language: str, pattern: str, sql: str) -> bool:
        """Ajoute un pattern NLP"""
        if language not in self.data['nlp_patterns']:
            self.data['nlp_patterns'][language] = []
        
        self.data['nlp_patterns'][language].append({
            'pattern': pattern,
            'sql': sql,
            'added_at': datetime.now().isoformat()
        })
        
        self._save_database()
        return True
    
    def get_nlp_patterns(self, language: str = None) -> Dict:
        """Récupère les patterns NLP"""
        if language:
            return self.data['nlp_patterns'].get(language, [])
        return self.data['nlp_patterns']
    
    # === UTILITAIRES ===
    
    def get_stats(self) -> Dict:
        """Récupère les statistiques de la base"""
        total_rows = sum(len(table_data) for table_data in self.data['tables'].values())
        
        return {
            'tables': len(self.data['schemas']),
            'rows': total_rows,
            'functions': len(self.data['functions']['user']) + len(self.data['functions']['builtins']),
            'indexes': len(self.data['indexes']),
            'size_bytes': len(yaml.dump(self.data).encode('utf-8')),
            'last_modified': self.data['metadata']['last_modified'],
            'created_at': self.data['metadata']['created_at']
        }
    
    def export_to_yaml(self, output_path: str) -> bool:
        """Exporte la base complète vers un fichier YAML"""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.data, f, default_flow_style=False, allow_unicode=True)
            return True
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False
    
    def import_from_yaml(self, input_path: str) -> bool:
        """Importe une base depuis un fichier YAML"""
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                imported_data = yaml.safe_load(f)
            
            # Fusionner avec les données existantes
            for key, value in imported_data.items():
                self.data[key] = value
            
            self._save_database()
            return True
        except Exception as e:
            logger.error(f"Import failed: {e}")
            return False
    
    def close(self):
        """Ferme le stockage"""
        self._save_database()
        logger.info("Storage closed")
