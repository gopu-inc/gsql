#!/usr/bin/env python3
"""
GSQL - A lightweight SQL database engine with natural language interface
Version: 3.1.0
SQLite Only - No YAML dependency
"""

import os
import sys
import logging
import warnings
from pathlib import Path
from typing import Optional, Dict, Any, Union, List

# ==================== VERSION & METADATA ====================

__version__ = "3.10.0"
__author__ = "Gopu Inc."
__license__ = "MIT"
__copyright__ = "Copyright 2024 Gopu Inc."
__description__ = "GSQL - SQL Database with Natural Language Interface"
__url__ = "https://github.com/gopu-inc/gsql"

# Package metadata
PACKAGE_NAME = "gsql"
BASE_DIR = Path(__file__).parent

# ==================== LOGGING CONFIGURATION ====================

def setup_logging(
    level: Union[int, str] = logging.INFO,
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Configure le logging pour GSQL
    
    Args:
        level: Niveau de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Chemin vers le fichier de log (optionnel)
        format_string: Format personnalisé pour les logs
    
    Returns:
        logging.Logger: Logger configuré
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path))
    
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=handlers
    )
    
    # Désactiver les logs trop verbeux par défaut
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("nltk").setLevel(logging.WARNING)
    
    return logging.getLogger(PACKAGE_NAME)

# Logger par défaut
logger = setup_logging()

# ==================== FEATURE DETECTION ====================

class FeatureDetection:
    """Détection des fonctionnalités disponibles"""
    
    @staticmethod
    def check_nltk() -> bool:
        """Vérifie si NLTK est disponible"""
        try:
            import nltk
            # Vérifier les données nécessaires
            try:
                nltk.data.find('tokenizers/punkt')
                nltk.data.find('corpora/stopwords')
                return True
            except LookupError:
                logger.warning("NLTK data files not found. Run: nltk.download('punkt'), nltk.download('stopwords')")
                return False
        except ImportError:
            logger.warning("NLTK not installed. Natural language features will be limited.")
            return False
    
    @staticmethod
    def check_sqlite() -> bool:
        """Vérifie la version de SQLite"""
        try:
            import sqlite3
            version = sqlite3.sqlite_version_info
            logger.debug(f"SQLite version: {version[0]}.{version[1]}.{version[2]}")
            return version >= (3, 8, 0)  # Nécessaire pour WAL
        except Exception as e:
            logger.error(f"SQLite check failed: {e}")
            return False
    
    @staticmethod
    def check_yaml() -> bool:
        """Vérifie si PyYAML est disponible - OPTIONNEL maintenant"""
        # YAML n'est plus requis pour GSQL
        # Nous utilisons uniquement JSON et SQLite
        return True  # Toujours True car optionnel
    
    @staticmethod
    def check_colorama() -> bool:
        """Vérifie si colorama est disponible (pour couleurs Windows)"""
        try:
            import colorama
            return True
        except ImportError:
            return False
    
    @staticmethod
    def check_tabulate() -> bool:
        """Vérifie si tabulate est disponible (pour table formatting)"""
        try:
            import tabulate
            return True
        except ImportError:
            return False
    
    @staticmethod
    def check_rich() -> bool:
        """Vérifie si rich est disponible (pour output avancé)"""
        try:
            import rich
            return True
        except ImportError:
            return False
    
    @staticmethod
    def get_all_features() -> Dict[str, bool]:
        """Retourne l'état de toutes les fonctionnalités"""
        return {
            'nltk': FeatureDetection.check_nltk(),
            'sqlite': FeatureDetection.check_sqlite(),
            'yaml': FeatureDetection.check_yaml(),  # Optionnel
            'colorama': FeatureDetection.check_colorama(),
            'tabulate': FeatureDetection.check_tabulate(),
            'rich': FeatureDetection.check_rich()
        }

# ==================== IMPORT DES MODULES ====================

# Import des exceptions d'abord (elles sont simples et sans dépendances)
try:
    from .exceptions import (
        GSQLBaseException,
        SQLSyntaxError,
        SQLExecutionError,
        ConstraintViolationError,
        TransactionError,
        FunctionError,
        NLError,
        BufferPoolError,
        StorageError,
        QueryError
    )
    EXCEPTIONS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Could not import exceptions: {e}")
    # Définir des classes factices pour éviter les erreurs
    class GSQLBaseException(Exception): pass
    class SQLSyntaxError(GSQLBaseException): pass
    class SQLExecutionError(GSQLBaseException): pass
    class ConstraintViolationError(GSQLBaseException): pass
    class TransactionError(GSQLBaseException): pass
    class FunctionError(GSQLBaseException): pass
    class NLError(GSQLBaseException): pass
    class BufferPoolError(GSQLBaseException): pass
    class StorageError(GSQLBaseException): pass
    class QueryError(GSQLBaseException): pass
    EXCEPTIONS_AVAILABLE = False

# Import du storage engine
try:
    from .storage import (
        SQLiteStorage,
        BufferPool,
        TransactionManager,
        create_storage,
        get_storage_stats
    )
    STORAGE_AVAILABLE = True
except ImportError as e:
    logger.error(f"Could not import storage module: {e}")
    SQLiteStorage = None
    BufferPool = None
    TransactionManager = None
    create_storage = None
    get_storage_stats = None
    STORAGE_AVAILABLE = False

# Import du module database
try:
    from .database import (
        Database,
        create_database,
        get_default_database,
        set_default_database,
        connect
    )
    DATABASE_AVAILABLE = True
except ImportError as e:
    logger.error(f"Could not import database module: {e}")
    Database = None
    create_database = None
    get_default_database = None
    set_default_database = None
    connect = None
    DATABASE_AVAILABLE = False

# Import de l'executor
try:
    from .executor import (
        QueryExecutor,
        create_executor,
        get_default_executor,
        set_default_executor
    )
    EXECUTOR_AVAILABLE = True
except ImportError as e:
    logger.error(f"Could not import executor module: {e}")
    QueryExecutor = None
    create_executor = None
    get_default_executor = None
    set_default_executor = None
    EXECUTOR_AVAILABLE = False

# IMPORT CORRIGÉ : Import des fonctions utilisateur
try:
    from .functions import FunctionManager, FunctionError
    FUNCTIONS_AVAILABLE = True
    UserFunctionRegistry = None  # Pas utilisé dans notre version
except ImportError as e:
    logger.warning(f"Could not import functions module: {e}")
    FunctionManager = None
    FunctionError = None
    UserFunctionRegistry = None
    FUNCTIONS_AVAILABLE = False

# Import du traducteur NLP
try:
    from .nlp.translator import (
        NLToSQLTranslator,
        nl_to_sql
    )
    NLP_AVAILABLE = True and FeatureDetection.check_nltk()
except ImportError as e:
    logger.warning(f"Could not import NLP module: {e}")
    NLToSQLTranslator = None
    nl_to_sql = None
    NLP_AVAILABLE = False

# Import des autres modules
try:
    from .parser import SQLParser
    PARSER_AVAILABLE = True
except ImportError:
    SQLParser = None
    PARSER_AVAILABLE = False

try:
    from .index import BPlusTreeIndex, HashIndex
    INDEX_AVAILABLE = True
except ImportError:
    BPlusTreeIndex = None
    HashIndex = None
    INDEX_AVAILABLE = False

try:
    from .btree import BPlusTree
    BTREE_AVAILABLE = True
except ImportError:
    BPlusTree = None
    BTREE_AVAILABLE = False

# ==================== CONFIGURATION GLOBALE ====================

class GSQLConfig:
    """Configuration globale de GSQL"""
    
    _instance = None
    _config = {
        'base_dir': Path.home() / '.gsql',
        'database_path': None,
        'auto_recovery': True,
        'buffer_pool_size': 100,
        'enable_wal': True,
        'nlp_enabled': True,
        'cache_size': 200,
        'timeout': 30,
        'colors': True,
        'verbose_errors': True,
        'log_level': 'INFO',
        'log_file': None
    }
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self):
        """Charge la configuration depuis le fichier"""
        config_file = self._config['base_dir'] / 'config.json'
        if config_file.exists():
            try:
                import json
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                self._config.update(user_config)
                logger.info(f"Configuration loaded from {config_file}")
            except Exception as e:
                logger.warning(f"Could not load config file: {e}")
    
    def save(self):
        """Sauvegarde la configuration dans le fichier"""
        config_file = self._config['base_dir'] / 'config.json'
        try:
            import json
            config_file.parent.mkdir(parents=True, exist_ok=True)
            with open(config_file, 'w') as f:
                json.dump(self._config, f, indent=2)
            logger.debug(f"Configuration saved to {config_file}")
        except Exception as e:
            logger.error(f"Could not save config file: {e}")
    
    def get(self, key: str, default=None):
        """Récupère une valeur de configuration"""
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any):
        """Définit une valeur de configuration"""
        self._config[key] = value
        logger.debug(f"Config updated: {key} = {value}")
    
    def update(self, **kwargs):
        """Met à jour plusieurs valeurs de configuration"""
        self._config.update(kwargs)
        logger.debug(f"Config updated with: {kwargs}")
    
    def reset(self):
        """Réinitialise la configuration aux valeurs par défaut"""
        self._config = GSQLConfig._config.copy()
        logger.info("Configuration reset to defaults")
    
    def to_dict(self) -> Dict[str, Any]:
        """Retourne la configuration comme dictionnaire"""
        return self._config.copy()

# Instance globale de configuration
config = GSQLConfig()

# ==================== UTILITY FUNCTIONS ====================

def get_version() -> str:
    """Retourne la version de GSQL"""
    return __version__

def get_features() -> Dict[str, bool]:
    """Retourne l'état des fonctionnalités"""
    return FeatureDetection.get_all_features()

def check_health() -> Dict[str, Any]:
    """
    Vérifie la santé du système GSQL
    
    Returns:
        Dict: Rapport de santé détaillé
    """
    features = get_features()
    
    health_report = {
        'version': __version__,
        'features': features,
        'status': 'HEALTHY',
        'issues': [],
        'recommendations': []
    }
    
    # Vérifications CRITIQUES (doivent être disponibles)
    if not features['sqlite']:
        health_report['status'] = 'CRITICAL'
        health_report['issues'].append('SQLite not available or version too old')
        health_report['recommendations'].append('Install/update SQLite to version 3.8+')
    
    if not STORAGE_AVAILABLE:
        health_report['status'] = 'CRITICAL' if health_report['status'] != 'CRITICAL' else 'CRITICAL'
        health_report['issues'].append('Storage module not available')
    
    if not DATABASE_AVAILABLE:
        health_report['status'] = 'CRITICAL' if health_report['status'] != 'CRITICAL' else 'CRITICAL'
        health_report['issues'].append('Database module not available')
    
    if not EXECUTOR_AVAILABLE:
        health_report['status'] = 'DEGRADED' if health_report['status'] != 'CRITICAL' else 'CRITICAL'
        health_report['issues'].append('Executor module not available')
    
    # Vérifications IMPORTANTES mais non critiques
    if not features['nltk'] and config.get('nlp_enabled', True):
        health_report['status'] = 'DEGRADED' if health_report['status'] == 'HEALTHY' else health_report['status']
        health_report['recommendations'].append(
            "Install NLTK for natural language features: pip install nltk"
        )
    
    if not features['colorama'] and sys.platform == 'win32':
        health_report['recommendations'].append(
            "Install colorama for colored output on Windows: pip install colorama"
        )
    
    # YAML est OPTIONNEL - juste une info
    if not FeatureDetection.check_yaml():
        health_report['recommendations'].append(
            "YAML optional for advanced configuration (not required for core functionality)"
        )
    
    # Si pas d'issues critiques, vérifier les warnings
    if health_report['status'] == 'HEALTHY' and health_report['issues']:
        health_report['status'] = 'DEGRADED'
    
    return health_report

def initialize(
    base_dir: Optional[str] = None,
    database_path: Optional[str] = None,
    nlp_enabled: Optional[bool] = None,
    **kwargs
) -> Database:
    """
    Initialise GSQL avec une configuration personnalisée
    
    Args:
        base_dir: Répertoire de base pour GSQL
        database_path: Chemin vers la base de données
        nlp_enabled: Activer/désactiver le NLP
        **kwargs: Autres paramètres de configuration
    
    Returns:
        Database: Instance de base de données initialisée
    """
    # Vérifier d'abord la santé
    health = check_health()
    if health['status'] == 'CRITICAL':
        raise RuntimeError(f"GSQL health check failed: {health['issues']}")
    
    # Mettre à jour la configuration
    if base_dir:
        config.set('base_dir', Path(base_dir))
    
    if database_path:
        config.set('database_path', database_path)
    
    if nlp_enabled is not None:
        config.set('nlp_enabled', nlp_enabled)
    
    if kwargs:
        config.update(**kwargs)
    
    # Créer la base de données
    if not DATABASE_AVAILABLE or Database is None:
        raise ImportError("Database module not available")
    
    db_path = config.get('database_path')
    
    return create_database(
        db_path=db_path,
        base_dir=str(config.get('base_dir')),
        auto_recovery=config.get('auto_recovery', True),
        buffer_pool_size=config.get('buffer_pool_size', 100),
        enable_wal=config.get('enable_wal', True)
    )

def run_cli(args: Optional[List[str]] = None):
    """
    Lance l'interface CLI de GSQL
    
    Args:
        args: Arguments de ligne de commande (similaire à sys.argv[1:])
    """
    if args is None:
        args = sys.argv[1:]
    
    # Vérifier que le module __main__ est disponible
    try:
        from . import __main__
        # Injecter les arguments
        sys.argv = [sys.argv[0]] + args
        __main__.main()
    except ImportError as e:
        logger.error(f"Could not start CLI: {e}")
        print("GSQL CLI is not available. Make sure __main__.py exists.")
        sys.exit(1)

def run_shell(database_path: Optional[str] = None):
    """
    Lance le shell interactif GSQL
    
    Args:
        database_path: Chemin vers la base de données
    """
    # Vérifier la santé d'abord
    health = check_health()
    if health['status'] == 'CRITICAL':
        print(f"CRITICAL: GSQL cannot start. Issues: {', '.join(health['issues'])}")
        sys.exit(1)
    
    from .__main__ import GSQLShell
    
    # Configurer pour le shell
    shell_args = []
    if database_path:
        shell_args.append(database_path)
    
    run_cli(shell_args)

# ==================== CONTEXT MANAGER ====================

class GSQLContext:
    """Context manager pour GSQL"""
    
    def __init__(self, database_path: Optional[str] = None, **kwargs):
        self.database_path = database_path
        self.kwargs = kwargs
        self.db = None
    
    def __enter__(self):
        """Entre dans le contexte"""
        self.db = initialize(
            database_path=self.database_path,
            **self.kwargs
        )
        return self.db
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Sort du contexte"""
        if self.db:
            self.db.close()
        
        if exc_type:
            logger.error(f"Error in GSQL context: {exc_val}")
            return False  # Propager l'exception
        
        return True

def context(database_path: Optional[str] = None, **kwargs) -> GSQLContext:
    """
    Crée un context manager pour GSQL
    
    Args:
        database_path: Chemin vers la base de données
        **kwargs: Paramètres de configuration
    
    Returns:
        GSQLContext: Context manager
    """
    return GSQLContext(database_path, **kwargs)

# ==================== SHORTCUT FUNCTIONS ====================

def query(sql: str, database_path: Optional[str] = None, **kwargs) -> Dict[str, Any]:
    """
    Exécute une requête SQL sur une base de données
    
    Args:
        sql: Requête SQL à exécuter
        database_path: Chemin vers la base de données
        **kwargs: Paramètres supplémentaires pour la requête
    
    Returns:
        Dict: Résultats de la requête
    """
    with context(database_path) as db:
        return db.execute(sql, **kwargs)

def create_table(table_name: str, columns: Dict[str, Any], 
                 database_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Crée une nouvelle table
    
    Args:
        table_name: Nom de la table
        columns: Définition des colonnes
        database_path: Chemin vers la base de données
    
    Returns:
        Dict: Résultat de l'opération
    """
    with context(database_path) as db:
        return db.create_table(table_name, columns)

def insert(table_name: str, values: Dict[str, Any], 
           database_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Insère une ligne dans une table
    
    Args:
        table_name: Nom de la table
        values: Valeurs à insérer
        database_path: Chemin vers la base de données
    
    Returns:
        Dict: Résultat de l'opération
    """
    with context(database_path) as db:
        return db.insert(table_name, values)

def select(table_name: str, columns: Optional[List[str]] = None,
           where: Optional[Dict[str, Any]] = None,
           database_path: Optional[str] = None, **kwargs) -> Dict[str, Any]:
    """
    Exécute une requête SELECT
    
    Args:
        table_name: Nom de la table
        columns: Colonnes à sélectionner
        where: Conditions WHERE
        database_path: Chemin vers la base de données
        **kwargs: Paramètres supplémentaires (limit, order_by, etc.)
    
    Returns:
        Dict: Résultats de la requête
    """
    with context(database_path) as db:
        return db.select(table_name, columns, where, **kwargs)

def backup(database_path: str, backup_path: Optional[str] = None) -> str:
    """
    Crée une sauvegarde de la base de données
    
    Args:
        database_path: Chemin vers la base de données source
        backup_path: Chemin pour la sauvegarde (optionnel)
    
    Returns:
        str: Chemin de la sauvegarde créée
    """
    with context(database_path) as db:
        result = db.execute(f"BACKUP {backup_path}" if backup_path else "BACKUP")
        if result.get('success'):
            return result.get('backup_file', 'Backup created')
        else:
            raise SQLExecutionError(result.get('message', 'Backup failed'))

def vacuum(database_path: str) -> bool:
    """
    Optimise la base de données
    
    Args:
        database_path: Chemin vers la base de données
    
    Returns:
        bool: Succès de l'opération
    """
    with context(database_path) as db:
        result = db.execute("VACUUM")
        return result.get('success', False)

def show_tables(database_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Affiche la liste des tables
    
    Args:
        database_path: Chemin vers la base de données
    
    Returns:
        List: Liste des tables
    """
    with context(database_path) as db:
        result = db.execute("SHOW TABLES")
        if result.get('success'):
            return result.get('tables', [])
        else:
            raise SQLExecutionError(result.get('message', 'Failed to show tables'))

def describe_table(table_name: str, database_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Décrit la structure d'une table
    
    Args:
        table_name: Nom de la table
        database_path: Chemin vers la base de données
    
    Returns:
        Dict: Structure de la table
    """
    with context(database_path) as db:
        result = db.execute(f"DESCRIBE {table_name}")
        if result.get('success'):
            return result
        else:
            raise SQLExecutionError(result.get('message', f'Failed to describe table {table_name}'))

# ==================== EXPORT DES SYMBOLES ====================

__all__ = [
    # Version et métadonnées
    '__version__',
    '__author__',
    '__license__',
    '__description__',
    '__url__',
    
    # Configuration
    'config',
    'GSQLConfig',
    
    # Fonctions utilitaires
    'get_version',
    'get_features',
    'check_health',
    'setup_logging',
    'initialize',
    'run_cli',
    'run_shell',
    'context',
    
    # Shortcut functions
    'query',
    'create_table',
    'insert',
    'select',
    'backup',
    'vacuum',
    'show_tables',
    'describe_table',
    
    # Exceptions
    'GSQLBaseException',
    'SQLSyntaxError',
    'SQLExecutionError',
    'ConstraintViolationError',
    'TransactionError',
    'FunctionError',
    'NLError',
    'BufferPoolError',
    'StorageError',
    'QueryError',
    
    # Modules principaux
    'Database',
    'create_database',
    'get_default_database',
    'set_default_database',
    'connect',
    
    'SQLiteStorage',
    'BufferPool',
    'TransactionManager',
    'create_storage',
    'get_storage_stats',
    
    'QueryExecutor',
    'create_executor',
    'get_default_executor',
    'set_default_executor',
    
    # Fonctions - CORRIGÉ
    'FunctionManager',
    'FunctionError',
    'UserFunctionRegistry',  # Gardé pour compatibilité
    
    # NLP
    'NLToSQLTranslator',
    'nl_to_sql',
    
    # Autres modules
    'SQLParser',
    'BPlusTreeIndex',
    'HashIndex',
    'BPlusTree',
    
    # Flags de disponibilité
    'EXCEPTIONS_AVAILABLE',
    'STORAGE_AVAILABLE',
    'DATABASE_AVAILABLE',
    'EXECUTOR_AVAILABLE',
    'FUNCTIONS_AVAILABLE',
    'NLP_AVAILABLE',
    'PARSER_AVAILABLE',
    'INDEX_AVAILABLE',
    'BTREE_AVAILABLE',
]

# ==================== INITIALIZATION ====================

def _initialize_package():
    """Initialise le package GSQL"""
    # Créer le répertoire de base si nécessaire
    base_dir = config.get('base_dir')
    if base_dir:
        Path(base_dir).mkdir(parents=True, exist_ok=True)
    
    # Configurer le logging
    log_level = config.get('log_level', 'INFO')
    log_file = config.get('log_file')
    setup_logging(log_level, log_file)
    
    # Vérifier les fonctionnalités
    features = get_features()
    
    # Avertissements pour les fonctionnalités importantes
    if not features['sqlite']:
        warnings.warn(
            "SQLite not available or version too old. GSQL requires SQLite 3.8+.",
            RuntimeWarning
        )
    
    if config.get('nlp_enabled', True) and not features['nltk']:
        logger.warning(
            "NLTK not available. Natural language features will be limited. "
            "Install with: pip install nltk"
        )
    
    # YAML est optionnel - juste une info
    try:
        import yaml
        logger.debug("YAML support available (optional)")
    except ImportError:
        logger.debug("YAML not installed (optional dependency)")
    
    logger.info(f"GSQL v{__version__} initialized (SQLite only)")
    logger.debug(f"Core features: SQLite={features['sqlite']}, NLTK={features['nltk']}")

# Initialiser au chargement du module
_initialize_package()

# ==================== DUNDER METHODS ====================

def __dir__():
    """Liste les symboles exportés"""
    return __all__

def __getattr__(name: str):
    """Gère l'accès aux attributs dynamiques"""
    if name in __all__:
        # Vérifier la disponibilité des modules critiques
        if name in ['Database', 'create_database', 'connect'] and not DATABASE_AVAILABLE:
            raise ImportError(f"Database module not available. Cannot access {name}")
        elif name in ['SQLiteStorage', 'BufferPool'] and not STORAGE_AVAILABLE:
            raise ImportError(f"Storage module not available. Cannot access {name}")
        elif name in ['QueryExecutor', 'create_executor'] and not EXECUTOR_AVAILABLE:
            raise ImportError(f"Executor module not available. Cannot access {name}")
        
        # L'attribut devrait être déjà chargé
        return globals().get(name)
    
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

# ==================== TESTS & EXAMPLES ====================

if __name__ == "__main__":
    """Exécute des tests basiques si le module est exécuté directement"""
    print(f"GSQL v{__version__}")
    print(f"Description: {__description__}")
    print(f"Author: {__author__}")
    print(f"License: {__license__}")
    print()
    
    # Afficher les fonctionnalités
    features = get_features()
    print("Features:")
    for feature, available in features.items():
        status = "✓" if available else "✗"
        print(f"  {status} {feature}")
    
    # Vérifier la santé
    health = check_health()
    print(f"\nHealth: {health['status']}")
    
    if health['status'] == 'CRITICAL':
        print("\nCRITICAL ISSUES (must be fixed):")
        for issue in health['issues']:
            print(f"  • {issue}")
    elif health['status'] == 'DEGRADED':
        print("\nISSUES (functionality limited):")
        for issue in health['issues']:
            print(f"  • {issue}")
    
    if health['recommendations']:
        print("\nRecommendations:")
        for rec in health['recommendations']:
            print(f"  • {rec}")
    
    # Tester les fonctionnalités de base
    if health['status'] != 'CRITICAL':
        print("\nTesting basic functionality...")
        try:
            # Test avec base en mémoire
            db = connect(":memory:")
            db.execute("CREATE TABLE test (id INTEGER, name TEXT)")
            db.execute("INSERT INTO test VALUES (1, 'Test')")
            result = db.execute("SELECT * FROM test")
            db.close()
            
            if result.get('success'):
                print("✓ Basic SQL functionality: OK")
            else:
                print("✗ Basic SQL functionality: FAILED")
        except Exception as e:
            print(f"✗ Basic test failed: {e}")
