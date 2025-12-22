#!/usr/bin/env python3
"""
Database module for GSQL - Version complète et stable
"""

import os
import logging
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import yaml

from .exceptions import (
    GSQLBaseException, SQLSyntaxError, SQLExecutionError,
    ConstraintViolationError, TransactionError, FunctionError
)

logger = logging.getLogger(__name__)

class YAMLStorage:
    """Stockage YAML simple et efficace"""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        if not self.db_path.suffix in ['.yaml', '.yml']:
            self.db_path = self.db_path.with_suffix('.yaml')
        
        # Créer le dossier si nécessaire
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Charger ou initialiser la base
        self.data = self._load_or_create()
    
    def _load_or_create(self) -> Dict:
        """Charge ou crée la base de données"""
        if self.db_path.exists():
            try:
                with open(self.db_path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                    if data is None:
                        return self._default_structure()
                    return data
            except Exception as e:
                logger.error(f"Error loading YAML: {e}")
                return self._default_structure()
        else:
            return self._default_structure()
    
    def _default_structure(self) -> Dict:
        """Structure par défaut de la base"""
        return {
            'metadata': {
                'version': '2.0',
                'created_at': '2024-01-15',
                'engine': 'GSQL YAML'
            },
            'tables': {},
            'schemas': {},
            'functions': {
                'builtin': [
                    {'name': 'UPPER', 'type': 'string', 'description': 'Convert to uppercase'},
                    {'name': 'LOWER', 'type': 'string', 'description': 'Convert to lowercase'},
                    {'name': 'LENGTH', 'type': 'numeric', 'description': 'String length'},
                    {'name': 'ABS', 'type': 'numeric', 'description': 'Absolute value'},
                    {'name': 'ROUND', 'type': 'numeric', 'description': 'Round number'}
                ],
                'user': []
            },
            'config': {
                'auto_save': True,
                'backup_on_write': True
            }
        }
    
    def save(self):
        """Sauvegarde la base"""
        try:
            with open(self.db_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.data, f, default_flow_style=False, allow_unicode=True)
            logger.debug(f"Database saved to {self.db_path}")
        except Exception as e:
            logger.error(f"Error saving database: {e}")
    
    # === GESTION DES TABLES ===
    
    def create_table(self, table_name: str, columns: Dict) -> bool:
        """Crée une nouvelle table"""
        if table_name in self.data['tables']:
            return False
        
        # Initialiser la table
        self.data['tables'][table_name] = []
        self.data['schemas'][table_name] = {
            'columns': columns,
            'created_at': 'now',
            'row_count': 0
        }
        
        self.save()
        return True
    
    def insert(self, table_name: str, values: Dict) -> int:
        """Insère une ligne"""
        if table_name not in self.data['tables']:
            # Créer la table si elle n'existe pas
            columns = {k: 'TEXT' for k in values.keys()}
            self.create_table(table_name, columns)
        
        # Ajouter la ligne
        row_id = len(self.data['tables'][table_name])
        row = {'_id': row_id, **values}
        self.data['tables'][table_name].append(row)
        self.data['schemas'][table_name]['row_count'] = len(self.data['tables'][table_name])
        
        self.save()
        return row_id
    
    def select(self, table_name: str, where: Dict = None, 
               columns: List[str] = None, limit: int = None) -> List[Dict]:
        """Sélectionne des lignes"""
        if table_name not in self.data['tables']:
            return []
        
        rows = self.data['tables'][table_name]
        
        # Filtrer
        if where:
            filtered = []
            for row in rows:
                match = True
                for key, value in where.items():
                    if row.get(key) != value:
                        match = False
                        break
                if match:
                    filtered.append(row)
            rows = filtered
        
        # Sélectionner des colonnes
        if columns:
            result = []
            for row in rows:
                filtered_row = {}
                for col in columns:
                    if col in row:
                        filtered_row[col] = row[col]
                result.append(filtered_row)
            rows = result
        
        # Limite
        if limit:
            rows = rows[:limit]
        
        return rows
    
    def update(self, table_name: str, values: Dict, where: Dict = None) -> int:
        """Met à jour des lignes"""
        if table_name not in self.data['tables']:
            return 0
        
        updated = 0
        for row in self.data['tables'][table_name]:
            # Vérifier les conditions
            if where:
                match = True
                for key, value in where.items():
                    if row.get(key) != value:
                        match = False
                        break
                if not match:
                    continue
            
            # Mettre à jour
            for key, value in values.items():
                row[key] = value
            updated += 1
        
        if updated > 0:
            self.save()
        
        return updated
    
    def delete(self, table_name: str, where: Dict = None) -> int:
        """Supprime des lignes"""
        if table_name not in self.data['tables']:
            return 0
        
        if not where:
            # Supprimer tout
            count = len(self.data['tables'][table_name])
            self.data['tables'][table_name] = []
            self.data['schemas'][table_name]['row_count'] = 0
            self.save()
            return count
        
        # Filtrer
        keep_rows = []
        deleted = 0
        for row in self.data['tables'][table_name]:
            match = True
            for key, value in where.items():
                if row.get(key) != value:
                    match = False
                    break
            
            if match:
                deleted += 1
            else:
                keep_rows.append(row)
        
        self.data['tables'][table_name] = keep_rows
        self.data['schemas'][table_name]['row_count'] = len(keep_rows)
        
        if deleted > 0:
            self.save()
        
        return deleted
    
    def drop_table(self, table_name: str) -> bool:
        """Supprime une table"""
        if table_name not in self.data['tables']:
            return False
        
        del self.data['tables'][table_name]
        del self.data['schemas'][table_name]
        self.save()
        return True
    
    def get_tables(self) -> List[Dict]:
        """Liste les tables"""
        tables = []
        for name, schema in self.data['schemas'].items():
            tables.append({
                'name': name,
                'rows': schema.get('row_count', 0),
                'columns': list(schema.get('columns', {}).keys())
            })
        return tables
    
    def get_schema(self, table_name: str) -> Optional[Dict]:
        """Récupère le schéma d'une table"""
        return self.data['schemas'].get(table_name)
    
    # === GESTION DES FONCTIONS ===
    
    def create_function(self, name: str, params: List[str], 
                       body: str, returns: str = "TEXT") -> bool:
        """Crée une fonction"""
        # Vérifier si elle existe déjà
        for func in self.data['functions']['user']:
            if func['name'] == name:
                return False
        
        self.data['functions']['user'].append({
            'name': name,
            'params': params,
            'body': body,
            'returns': returns,
            'created_at': 'now'
        })
        
        self.save()
        return True
    
    def get_functions(self) -> List[Dict]:
        """Liste toutes les fonctions"""
        all_funcs = []
        
        # Built-in
        for func in self.data['functions']['builtin']:
            all_funcs.append({**func, 'type': 'builtin'})
        
        # User
        for func in self.data['functions']['user']:
            all_funcs.append({**func, 'type': 'user'})
        
        return all_funcs
    
    def drop_function(self, name: str) -> bool:
        """Supprime une fonction"""
        for i, func in enumerate(self.data['functions']['user']):
            if func['name'] == name:
                del self.data['functions']['user'][i]
                self.save()
                return True
        return False

class QueryExecutor:
    """Exécuteur de requêtes avec support YAML"""
    
    def __init__(self, storage):
        self.storage = storage
    
    def execute(self, sql: str) -> Any:
        """Exécute une requête SQL"""
        sql = sql.strip()
        
        # Commandes spéciales
        if sql.upper() == "SHOW TABLES":
            return self._execute_show_tables()
        elif sql.upper() == "SHOW FUNCTIONS":
            return self._execute_show_functions()
        elif sql.upper() == "HELP":
            return self._execute_help()
        
        # Détecter le type de requête
        sql_upper = sql.upper()
        
        if sql_upper.startswith("CREATE TABLE"):
            return self._execute_create_table(sql)
        elif sql_upper.startswith("CREATE FUNCTION"):
            return self._execute_create_function(sql)
        elif sql_upper.startswith("INSERT INTO"):
            return self._execute_insert(sql)
        elif sql_upper.startswith("SELECT"):
            return self._execute_select(sql)
        elif sql_upper.startswith("UPDATE"):
            return self._execute_update(sql)
        elif sql_upper.startswith("DELETE FROM"):
            return self._execute_delete(sql)
        elif sql_upper.startswith("DROP TABLE"):
            return self._execute_drop_table(sql)
        else:
            raise SQLExecutionError(f"Unsupported SQL: {sql}")
    
    def _execute_show_tables(self) -> Dict:
        """Exécute SHOW TABLES"""
        tables = self.storage.get_tables()
        return {
            'type': 'tables',
            'rows': tables,
            'count': len(tables),
            'message': f'Found {len(tables)} table(s)'
        }
    
    def _execute_show_functions(self) -> Dict:
        """Exécute SHOW FUNCTIONS"""
        functions = self.storage.get_functions()
        return {
            'type': 'functions',
            'rows': functions,
            'count': len(functions),
            'message': f'Found {len(functions)} function(s)'
        }
    
    def _execute_help(self) -> Dict:
        """Exécute HELP"""
        help_text = """
GSQL Commands:

SQL Commands:
  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
  INSERT INTO table VALUES (val1, val2, ...)
  SELECT * FROM table [WHERE condition]
  UPDATE table SET col=value [WHERE condition]
  DELETE FROM table [WHERE condition]
  DROP TABLE table

GSQL Commands:
  SHOW TABLES      - List all tables
  SHOW FUNCTIONS   - List all functions
  HELP             - This help

Natural Language:
  nl show tables
  nl table [name]
  nl help
"""
        return {
            'type': 'help',
            'message': help_text
        }
    
    def _execute_create_table(self, sql: str) -> Dict:
        """Exécute CREATE TABLE"""
        # Parser simple: CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
        import re
        
        pattern = r"CREATE TABLE (\w+)\s*\((.*)\)"
        match = re.search(pattern, sql, re.IGNORECASE)
        
        if not match:
            raise SQLSyntaxError("Invalid CREATE TABLE syntax")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        
        # Parser les colonnes
        columns = {}
        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            if not col_def:
                continue
            
            parts = col_def.split()
            if len(parts) >= 2:
                col_name = parts[0]
                col_type = parts[1].upper()
                columns[col_name] = col_type
            else:
                raise SQLSyntaxError(f"Invalid column definition: {col_def}")
        
        success = self.storage.create_table(table_name, columns)
        
        if success:
            return {
                'type': 'create_table',
                'message': f'Table {table_name} created successfully',
                'table': table_name,
                'columns': list(columns.keys())
            }
        else:
            raise SQLExecutionError(f"Table {table_name} already exists")
    
    def _execute_create_function(self, sql: str) -> Dict:
        """Exécute CREATE FUNCTION"""
        # Format: CREATE FUNCTION name(params) RETURNS type AS $$code$$ LANGUAGE plpython
        import re
        
        pattern = r"CREATE FUNCTION (\w+)\s*\((.*?)\)\s*RETURNS\s+(\w+)\s+AS\s+\$\$(.*?)\$\$\s+LANGUAGE\s+(\w+)"
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise SQLSyntaxError("Invalid CREATE FUNCTION syntax")
        
        name = match.group(1)
        params_str = match.group(2)
        returns = match.group(3)
        body = match.group(4).strip()
        language = match.group(5)
        
        # Extraire les paramètres
        params = []
        if params_str.strip():
            for param in params_str.split(','):
                param = param.strip()
                if param:
                    # Enlever le type si présent
                    param_name = param.split()[0]
                    params.append(param_name)
        
        success = self.storage.create_function(name, params, body, returns)
        
        if success:
            return {
                'type': 'create_function',
                'message': f'Function {name} created successfully',
                'name': name,
                'params': params
            }
        else:
            raise SQLExecutionError(f"Function {name} already exists")
    
    def _execute_insert(self, sql: str) -> Dict:
        """Exécute INSERT INTO"""
        import re
        
        # Pattern: INSERT INTO table VALUES (val1, val2, ...)
        pattern = r"INSERT INTO (\w+)\s+VALUES\s*\((.*?)\)"
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise SQLSyntaxError("Invalid INSERT syntax")
        
        table_name = match.group(1)
        values_str = match.group(2)
        
        # Parser les valeurs
        values = []
        current = ""
        in_quotes = False
        quote_char = None
        
        for char in values_str:
            if char in ["'", '"'] and (not in_quotes or quote_char == char):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                else:
                    in_quotes = False
                current += char
            elif char == ',' and not in_quotes:
                values.append(current.strip())
                current = ""
            else:
                current += char
        
        if current:
            values.append(current.strip())
        
        # Créer le dictionnaire de valeurs
        values_dict = {}
        for i, val in enumerate(values):
            # Enlever les guillemets
            if val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
            elif val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            
            values_dict[f"col{i}"] = val
        
        row_id = self.storage.insert(table_name, values_dict)
        
        return {
            'type': 'insert',
            'message': f'Row inserted with ID {row_id}',
            'table': table_name,
            'row_id': row_id
        }
    
    def _execute_select(self, sql: str) -> Dict:
        """Exécute SELECT"""
        import re
        
        # Pattern simple: SELECT * FROM table [WHERE conditions]
        pattern = r"SELECT (.*?) FROM (\w+)(?: WHERE (.*?))?(?: LIMIT (\d+))?$"
        match = re.search(pattern, sql, re.IGNORECASE)
        
        if not match:
            raise SQLSyntaxError("Invalid SELECT syntax")
        
        columns_str = match.group(1)
        table_name = match.group(2)
        where_str = match.group(3)
        limit_str = match.group(4)
        
        # Déterminer les colonnes
        if columns_str.strip() == "*":
            columns = None
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parser les conditions WHERE
        where = None
        if where_str:
            # Conditions simples: col = value
            where = {}
            conditions = where_str.split('AND')
            for cond in conditions:
                cond = cond.strip()
                if '=' in cond:
                    col, val = cond.split('=', 1)
                    col = col.strip()
                    val = val.strip()
                    
                    # Enlever les guillemets
                    if val.startswith("'") and val.endswith("'"):
                        val = val[1:-1]
                    elif val.startswith('"') and val.endswith('"'):
                        val = val[1:-1]
                    
                    where[col] = val
        
        # Limite
        limit = int(limit_str) if limit_str else None
        
        # Exécuter la requête
        rows = self.storage.select(table_name, where, columns, limit)
        
        return {
            'type': 'select',
            'rows': rows,
            'count': len(rows),
            'table': table_name
        }
    
    def _execute_update(self, sql: str) -> Dict:
        """Exécute UPDATE"""
        import re
        
        pattern = r"UPDATE (\w+) SET (.*?)(?: WHERE (.*?))?$"
        match = re.search(pattern, sql, re.IGNORECASE)
        
        if not match:
            raise SQLSyntaxError("Invalid UPDATE syntax")
        
        table_name = match.group(1)
        set_str = match.group(2)
        where_str = match.group(3)
        
        # Parser SET
        values = {}
        assignments = set_str.split(',')
        for assign in assignments:
            assign = assign.strip()
            if '=' in assign:
                col, val = assign.split('=', 1)
                col = col.strip()
                val = val.strip()
                
                # Enlever les guillemets
                if val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]
                elif val.startswith('"') and val.endswith('"'):
                    val = val[1:-1]
                
                values[col] = val
        
        # Parser WHERE
        where = None
        if where_str:
            where = {}
            if '=' in where_str:
                col, val = where_str.split('=', 1)
                col = col.strip()
                val = val.strip()
                
                if val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]
                elif val.startswith('"') and val.endswith('"'):
                    val = val[1:-1]
                
                where[col] = val
        
        updated = self.storage.update(table_name, values, where)
        
        return {
            'type': 'update',
            'message': f'{updated} row(s) updated',
            'table': table_name,
            'updated': updated
        }
    
    def _execute_delete(self, sql: str) -> Dict:
        """Exécute DELETE FROM"""
        import re
        
        pattern = r"DELETE FROM (\w+)(?: WHERE (.*?))?$"
        match = re.search(pattern, sql, re.IGNORECASE)
        
        if not match:
            raise SQLSyntaxError("Invalid DELETE syntax")
        
        table_name = match.group(1)
        where_str = match.group(2)
        
        # Parser WHERE
        where = None
        if where_str:
            where = {}
            if '=' in where_str:
                col, val = where_str.split('=', 1)
                col = col.strip()
                val = val.strip()
                
                if val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]
                elif val.startswith('"') and val.endswith('"'):
                    val = val[1:-1]
                
                where[col] = val
        
        deleted = self.storage.delete(table_name, where)
        
        return {
            'type': 'delete',
            'message': f'{deleted} row(s) deleted',
            'table': table_name,
            'deleted': deleted
        }
    
    def _execute_drop_table(self, sql: str) -> Dict:
        """Exécute DROP TABLE"""
        import re
        
        pattern = r"DROP TABLE (\w+)"
        match = re.search(pattern, sql, re.IGNORECASE)
        
        if not match:
            raise SQLSyntaxError("Invalid DROP TABLE syntax")
        
        table_name = match.group(1)
        success = self.storage.drop_table(table_name)
        
        if success:
            return {
                'type': 'drop_table',
                'message': f'Table {table_name} dropped',
                'table': table_name
            }
        else:
            raise SQLExecutionError(f"Table {table_name} does not exist")

class Database:
    """Classe principale de la base de données GSQL"""
    
    def __init__(self, db_path: str = ":memory:", use_nlp: bool = True):
        """
        Initialise la base de données
        
        Args:
            db_path: Chemin vers le fichier YAML ou ":memory:" pour en mémoire
            use_nlp: Activer le traitement du langage naturel
        """
        self.db_path = db_path
        self.use_nlp = use_nlp
        
        # Initialiser le stockage
        if db_path == ":memory:":
            # Mode mémoire - utiliser SQLite temporaire
            self._init_memory_mode()
        else:
            # Mode YAML
            self.storage = YAMLStorage(db_path)
        
        # Initialiser l'exécuteur
        self.executor = QueryExecutor(self.storage)
        
        logger.info(f"Database initialized: {db_path}")
    
    def _init_memory_mode(self):
        """Initialise le mode mémoire avec SQLite"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        
        self.conn = sqlite3.connect(self.temp_db.name)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        
        # Créer une table users par défaut avec des données
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INTEGER
            )
        """)
        
        # Insérer des données d'exemple si la table est vide
        self.cursor.execute("SELECT COUNT(*) FROM users")
        if self.cursor.fetchone()[0] == 0:
            sample_data = [
                (1, 'Alice', 'alice@example.com', 30),
                (2, 'Bob', 'bob@example.com', 25),
                (3, 'Charlie', 'charlie@example.com', 35)
            ]
            self.cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?)", sample_data)
            self.conn.commit()
        
        # Simuler le stockage YAML pour l'API
        class MemoryStorage:
            def __init__(self, cursor, conn):
                self.cursor = cursor
                self.conn = conn
            
            def get_tables(self):
                self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = []
                for row in self.cursor.fetchall():
                    table_name = row['name']
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = self.cursor.fetchone()[0]
                    tables.append({
                        'name': table_name,
                        'rows': row_count,
                        'columns': self._get_columns(table_name)
                    })
                return tables
            
            def _get_columns(self, table_name):
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                return [row['name'] for row in self.cursor.fetchall()]
            
            def select(self, table_name, where=None, columns=None, limit=None):
                # Construire la requête
                query = f"SELECT * FROM {table_name}"
                params = []
                
                if where:
                    conditions = []
                    for key, value in where.items():
                        conditions.append(f"{key} = ?")
                        params.append(value)
                    query += " WHERE " + " AND ".join(conditions)
                
                if limit:
                    query += f" LIMIT {limit}"
                
                self.cursor.execute(query, params)
                rows = []
                for row in self.cursor.fetchall():
                    rows.append(dict(row))
                return rows
        
        self.storage = MemoryStorage(self.cursor, self.conn)
    
    def execute(self, sql: str) -> Any:
        """
        Exécute une commande SQL
        
        Args:
            sql: Commande SQL
            
        Returns:
            Résultat de l'exécution
        """
        try:
            return self.executor.execute(sql)
        except Exception as e:
            if isinstance(e, GSQLBaseException):
                raise e
            raise SQLExecutionError(f"Execution failed: {str(e)}")
    
    def execute_nl(self, nl_query: str) -> Any:
        """
        Exécute une requête en langage naturel
        
        Args:
            nl_query: Requête en langage naturel
            
        Returns:
            Résultat de l'exécution
        """
        if not self.use_nlp:
            raise SQLExecutionError("NLP is not enabled")
        
        # Traduction simple
        nl_lower = nl_query.lower()
        
        if "table" in nl_lower and "show" not in nl_lower:
            # Extraire le nom de table
            words = nl_lower.split()
            for word in words:
                if word != "table" and len(word) > 2:
                    return self.execute(f"SELECT * FROM {word}")
        
        if "table" in nl_lower:
            return self.execute("SHOW TABLES")
        elif "fonction" in nl_lower or "function" in nl_lower:
            return self.execute("SHOW FUNCTIONS")
        elif "aide" in nl_lower or "help" in nl_lower:
            return self.execute("HELP")
        else:
            return {
                'type': 'help',
                'message': 'Try: show tables, table [name], help'
            }
    
    def create_function(self, name: str, params: List[str], 
                       body: str, returns: str = "TEXT") -> bool:
        """
        Crée une fonction utilisateur
        """
        if hasattr(self.storage, 'create_function'):
            return self.storage.create_function(name, params, body, returns)
        return False
    
    def list_tables(self) -> List[Dict]:
        """Liste les tables"""
        if hasattr(self.storage, 'get_tables'):
            return self.storage.get_tables()
        return []
    
    def list_functions(self) -> List[Dict]:
        """Liste les fonctions"""
        if hasattr(self.storage, 'get_functions'):
            return self.storage.get_functions()
        return []
    
    def close(self):
        """Ferme la base de données"""
        if hasattr(self, 'conn'):
            self.conn.close()
            os.unlink(self.temp_db.name)
        logger.info("Database closed")

# Fonctions utilitaires
def create_database(db_path: str, **kwargs) -> Database:
    """
    Crée une nouvelle base de données
    
    Args:
        db_path: Chemin vers le fichier
        **kwargs: Arguments supplémentaires
        
    Returns:
        Instance Database
    """
    # S'assurer que le dossier existe
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Supprimer le fichier existant s'il y a lieu
    if os.path.exists(db_path):
        os.remove(db_path)
    
    return Database(db_path, **kwargs)

def connect(db_path: str = ":memory:", **kwargs) -> Database:
    """
    Se connecte à une base de données
    
    Args:
        db_path: Chemin vers le fichier
        **kwargs: Arguments supplémentaires
        
    Returns:
        Instance Database
    """
    return Database(db_path, **kwargs)

# Variables globales
_default_db = None

def get_default_database() -> Optional[Database]:
    """Récupère la base de données par défaut"""
    return _default_db

def set_default_database(db: Database):
    """Définit la base de données par défaut"""
    global _default_db
    _default_db = db
