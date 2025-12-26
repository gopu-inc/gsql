#!/usr/bin/env python3
"""
GSQL - SQL Database with Natural Language Interface
Main Entry Point: Interactive Shell and CLI
Version: 3.0 - SQLite Only
"""

import argparse
import atexit
import cmd
import json
import logging
import os
import re
import readline
import signal
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# ==================== IMPORTS ====================

try:
    from . import __version__, config, setup_logging
    from .database import create_database, Database, connect
    from .executor import create_executor, QueryExecutor
    from .functions import FunctionManager
    from .storage import SQLiteStorage
    
    # NLP avec création automatique des patterns
    try:
        from .nlp.translator import NLToSQLTranslator, create_translator, nl_to_sql
        NLP_AVAILABLE = True
    except ImportError as e:
        print(f"Warning: NLP module not available: {e}")
        NLP_AVAILABLE = False
        NLToSQLTranslator = None
        create_translator = None
        nl_to_sql = None
    
    GSQL_AVAILABLE = True
except ImportError as e:
    print(f"Error importing GSQL modules: {e}")
    GSQL_AVAILABLE = False
    traceback.print_exc()
    sys.exit(1)

# ==================== CONSTANTS ====================

DEFAULT_CONFIG = {
    'database': {
        'base_dir': str(Path.home() / '.gsql'),
        'auto_recovery': True,
        'buffer_pool_size': 100,
        'enable_wal': True
    },
    'executor': {
        'enable_nlp': False,  # NLP désactivé par défaut
        'enable_learning': False,
        'nlp_confidence_threshold': 0.3,
        'nlp_auto_learn': True
    },
    'shell': {
        'prompt': 'gsql> ',
        'history_file': '.gsql_history',
        'max_history': 1000,
        'colors': True,
        'autocomplete': True,
        'rich_ui': True,
        'border_style': 'rounded'
    }
}

# ... [RESTE DU CODE SIMILAIRE À L'AVANT] ...

# ==================== COMMAND LINE INTERFACE ====================

def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(
        description=f"GSQL v{__version__} - SQL Database with Natural Language Interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  gsql                         # Start interactive shell
  gsql mydb.db                 # Open specific database
  gsql -e "SHOW TABLES"        # Execute single query
  gsql -f queries.sql          # Execute queries from file
  gsql --enable-nlp            # Enable Natural Language Processing
  gsql --nlp "show all users"  # Execute NLP query directly
        """
    )
    
    parser.add_argument(
        'database',
        nargs='?',
        help='Database file (optional, uses default if not specified)'
    )
    
    parser.add_argument(
        '-e', '--execute',
        help='Execute SQL query and exit'
    )
    
    parser.add_argument(
        '--nlp',
        help='Execute Natural Language query and exit'
    )
    
    parser.add_argument(
        '-f', '--file',
        help='Execute SQL from file and exit'
    )
    
    parser.add_argument(
        '--enable-nlp',
        action='store_true',
        help='Enable Natural Language Processing in interactive mode'
    )
    
    parser.add_argument(
        '--nlp-confidence',
        type=float,
        default=0.3,
        help='NLP confidence threshold (default: 0.3)'
    )
    
    parser.add_argument(
        '--nlp-patterns',
        help='Custom NLP patterns file'
    )
    
    parser.add_argument(
        '--no-color',
        action='store_true',
        help='Disable colored output'
    )
    
    parser.add_argument(
        '--simple-ui',
        action='store_true',
        help='Use simple UI instead of rich UI'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version=f'GSQL {__version__}'
    )
    
    return parser

# ==================== MAIN APPLICATION ====================

class GSQLApp:
    """Main GSQL Application"""
    
    def __init__(self):
        self.config = self._load_config()
        self.db: Optional[Database] = None
        self.executor: Optional[QueryExecutor] = None
        self.function_manager: Optional[FunctionManager] = None
        self.nlp_translator = None
        self.completer: Optional[GSQLCompleter] = None
        self.nlp_enabled = False
        self.nlp_patterns_file = None
        
        setup_logging(
            level=self.config.get('log_level', 'INFO'),
            log_file=self.config.get('log_file')
        )
        
        logger.info(f"GSQL v{__version__} initialized")
    
    def _load_config(self) -> Dict:
        """Load configuration"""
        user_config = config.to_dict()
        merged = DEFAULT_CONFIG.copy()
        
        # Merge configurations
        for section in ['database', 'executor', 'shell']:
            if section in user_config:
                merged[section].update(user_config[section])
        
        # Update global config
        config.update(**merged.get('database', {}))
        
        return merged
    
    def setup_nlp(self, enable_nlp: bool = False, 
                  patterns_file: Optional[str] = None,
                  confidence_threshold: float = 0.3) -> None:
        """Setup NLP with automatic patterns creation"""
        if not NLP_AVAILABLE:
            logger.warning("NLP module not available. Install NLTK for NLP support.")
            return
        
        self.nlp_enabled = enable_nlp
        
        if not self.nlp_enabled:
            logger.info("NLP is disabled")
            return
        
        # Déterminer le fichier de patterns
        if patterns_file:
            self.nlp_patterns_file = patterns_file
        else:
            # Chemin par défaut
            nlp_dir = Path(self.config['database'].get('base_dir')) / 'nlp'
            nlp_dir.mkdir(exist_ok=True, parents=True)
            self.nlp_patterns_file = str(nlp_dir / 'patterns.json')
        
        # Mettre à jour la configuration
        self.config['executor']['enable_nlp'] = True
        self.config['executor']['nlp_confidence_threshold'] = confidence_threshold
        config.set('executor.enable_nlp', True)
        config.set('executor.nlp_confidence_threshold', confidence_threshold)
        
        logger.info(f"NLP enabled with patterns file: {self.nlp_patterns_file}")
        logger.info(f"NLP confidence threshold: {confidence_threshold}")
    
    def _ensure_nlp_patterns_exist(self) -> bool:
        """Ensure NLP patterns file exists, create if not"""
        if not self.nlp_enabled or not self.nlp_patterns_file:
            return False
        
        patterns_path = Path(self.nlp_patterns_file)
        
        if patterns_path.exists():
            # Vérifier si le fichier est valide
            try:
                with open(patterns_path, 'r', encoding='utf-8') as f:
                    json.load(f)
                logger.debug(f"NLP patterns file exists: {patterns_path}")
                return True
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in NLP patterns file: {patterns_path}")
                # Recréer le fichier
                return self._create_default_patterns_file()
        else:
            # Créer le fichier de patterns par défaut
            return self._create_default_patterns_file()
    
    def _create_default_patterns_file(self) -> bool:
        """Create default NLP patterns file"""
        if not self.nlp_patterns_file:
            return False
        
        patterns_path = Path(self.nlp_patterns_file)
        patterns_dir = patterns_path.parent
        
        try:
            # Créer le répertoire si nécessaire
            patterns_dir.mkdir(exist_ok=True, parents=True)
            
            # Patterns par défaut enrichis
            default_patterns = {
                "patterns": {
                    "select": [
                        {
                            "nl_pattern": "show tables",
                            "sql_template": "SHOW TABLES",
                            "examples": ["montre les tables", "liste les tables", "affiche les tables"],
                            "confidence": 0.95,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "select all from {table}",
                            "sql_template": "SELECT * FROM {table}",
                            "examples": ["tous les {table}", "affiche {table}", "montre tous les {table}"],
                            "confidence": 0.9,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "select {columns} from {table}",
                            "sql_template": "SELECT {columns} FROM {table}",
                            "examples": ["montre {columns} de {table}", "donne {columns} depuis {table}"],
                            "confidence": 0.8,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "count {table}",
                            "sql_template": "SELECT COUNT(*) FROM {table}",
                            "examples": ["combien de {table}", "nombre de {table}", "total {table}"],
                            "confidence": 0.85,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "select from {table} where {condition}",
                            "sql_template": "SELECT * FROM {table} WHERE {condition}",
                            "examples": ["{table} où {condition}", "trouve {table} avec {condition}"],
                            "confidence": 0.7,
                            "usage_count": 0,
                            "last_used": None
                        }
                    ],
                    "insert": [
                        {
                            "nl_pattern": "insert into {table} values {values}",
                            "sql_template": "INSERT INTO {table} VALUES ({values})",
                            "examples": ["ajoute à {table} valeurs {values}", "nouveau dans {table} {values}"],
                            "confidence": 0.75,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "add {values} to {table}",
                            "sql_template": "INSERT INTO {table} VALUES ({values})",
                            "examples": ["ajoute {values} dans {table}", "insère {values} dans {table}"],
                            "confidence": 0.7,
                            "usage_count": 0,
                            "last_used": None
                        }
                    ],
                    "update": [
                        {
                            "nl_pattern": "update {table} set {set_clause} where {condition}",
                            "sql_template": "UPDATE {table} SET {set_clause} WHERE {condition}",
                            "examples": ["modifie {table} mettre {set_clause} où {condition}"],
                            "confidence": 0.6,
                            "usage_count": 0,
                            "last_used": None
                        }
                    ],
                    "delete": [
                        {
                            "nl_pattern": "delete from {table} where {condition}",
                            "sql_template": "DELETE FROM {table} WHERE {condition}",
                            "examples": ["supprime de {table} où {condition}", "enlève {table} avec {condition}"],
                            "confidence": 0.65,
                            "usage_count": 0,
                            "last_used": None
                        }
                    ],
                    "aggregation": [
                        {
                            "nl_pattern": "average {column} from {table}",
                            "sql_template": "SELECT AVG({column}) FROM {table}",
                            "examples": ["moyenne de {column} dans {table}", "moyenne {column} {table}"],
                            "confidence": 0.8,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "sum {column} from {table}",
                            "sql_template": "SELECT SUM({column}) FROM {table}",
                            "examples": ["total de {column} dans {table}", "somme {column} {table}"],
                            "confidence": 0.8,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "max {column} from {table}",
                            "sql_template": "SELECT MAX({column}) FROM {table}",
                            "examples": ["maximum de {column} dans {table}", "plus grand {column} {table}"],
                            "confidence": 0.8,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "min {column} from {table}",
                            "sql_template": "SELECT MIN({column}) FROM {table}",
                            "examples": ["minimum de {column} dans {table}", "plus petit {column} {table}"],
                            "confidence": 0.8,
                            "usage_count": 0,
                            "last_used": None
                        }
                    ],
                    "show": [
                        {
                            "nl_pattern": "show columns from {table}",
                            "sql_template": "DESCRIBE {table}",
                            "examples": ["montre les colonnes de {table}", "décris {table}", "structure de {table}"],
                            "confidence": 0.85,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "show functions",
                            "sql_template": "SHOW FUNCTIONS",
                            "examples": ["montre les fonctions", "liste les fonctions", "fonctions disponibles"],
                            "confidence": 0.9,
                            "usage_count": 0,
                            "last_used": None
                        }
                    ],
                    "utility": [
                        {
                            "nl_pattern": "help",
                            "sql_template": "HELP",
                            "examples": ["aide", "comment utiliser", "commandes disponibles"],
                            "confidence": 0.95,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "stats",
                            "sql_template": "STATS",
                            "examples": ["statistiques", "info base de données", "état de la base"],
                            "confidence": 0.9,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "backup",
                            "sql_template": "BACKUP",
                            "examples": ["sauvegarde", "créer une sauvegarde", "backup base de données"],
                            "confidence": 0.85,
                            "usage_count": 0,
                            "last_used": None
                        },
                        {
                            "nl_pattern": "vacuum",
                            "sql_template": "VACUUM",
                            "examples": ["optimiser", "nettoyer la base", "compacter la base"],
                            "confidence": 0.8,
                            "usage_count": 0,
                            "last_used": None
                        }
                    ]
                },
                "synonyms": {
                    "show": ["montre", "affiche", "liste", "donne", "voir", "afficher", "lister"],
                    "select": ["choisir", "sélectionner", "extraire", "obtenir", "prendre"],
                    "insert": ["ajouter", "insérer", "créer", "nouveau", "ajoute", "insère"],
                    "update": ["modifier", "changer", "mettre à jour", "éditer", "actualiser"],
                    "delete": ["supprimer", "effacer", "enlever", "retirer", "ôter"],
                    "count": ["compter", "dénombrer", "total", "nombre", "combien"],
                    "average": ["moyenne", "moyen", "moy"],
                    "sum": ["somme", "total", "addition"],
                    "max": ["maximum", "plus grand", "plus haut", "maximal"],
                    "min": ["minimum", "plus petit", "plus bas", "minimal"],
                    "where": ["où", "dans lequel", "pour lequel", "quand", "lorsque"],
                    "order_by": ["trier par", "ordonner par", "classer par", "organiser par"],
                    "group_by": ["grouper par", "regrouper par", "agréger par"],
                    "limit": ["limiter à", "seulement", "premier", "dernier", "top"],
                    "join": ["joindre", "combiner", "relier", "lier", "connecter"],
                    "from": ["de", "depuis", "provenant de", "issu de"],
                    "table": ["table", "tableau", "liste", "ensemble", "collection"]
                },
                "metadata": {
                    "created": datetime.now().isoformat(),
                    "version": "1.0",
                    "language": "fr",
                    "description": "Default NLP patterns for GSQL",
                    "total_patterns": 20
                }
            }
            
            # Écrire le fichier
            with open(patterns_path, 'w', encoding='utf-8') as f:
                json.dump(default_patterns, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Created default NLP patterns file: {patterns_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create NLP patterns file: {e}")
            return False
    
    def initialize_nlp(self, database_path: Optional[str] = None) -> bool:
        """Initialize NLP translator"""
        if not self.nlp_enabled:
            return False
        
        if not NLP_AVAILABLE:
            logger.warning("NLP module not available")
            return False
        
        # Ensure patterns file exists
        if not self._ensure_nlp_patterns_exist():
            logger.error("Failed to initialize NLP patterns")
            return False
        
        try:
            # Créer le traducteur
            self.nlp_translator = create_translator(
                db_path=database_path or (self.db.storage.db_path if self.db else None),
                patterns_path=self.nlp_patterns_file
            )
            
            logger.info("NLP translator initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize NLP translator: {e}")
            return False
    
    def initialize(self, database_path: Optional[str] = None) -> None:
        """Initialize GSQL components"""
        if self.config['shell'].get('rich_ui', True):
            print(Colors.rgb(100, 200, 255) + "╔══════════════════════════════════════════════════════════════╗")
            print("║                   Initializing GSQL...                   ║")
            print("╚══════════════════════════════════════════════════════════════╝" + Colors.RESET)
        else:
            print(Colors.info("Initializing GSQL..."))
        
        try:
            # Database
            db_config = self.config['database'].copy()
            if database_path:
                db_config['path'] = database_path
            
            self.db = create_database(**db_config)
            
            # Executor
            self.executor = create_executor(storage=self.db.storage)
            
            # Function manager
            self.function_manager = FunctionManager()
            
            # NLP (optional)
            if self.nlp_enabled:
                nlp_success = self.initialize_nlp(database_path)
                if nlp_success:
                    RichUI.print_status("✓ NLP initialized", "success")
                else:
                    RichUI.print_status("⚠ NLP initialization failed", "warning")
            
            # Autocompleter
            self.completer = GSQLCompleter(self.db)
            
            if self.config['shell'].get('rich_ui', True):
                RichUI.print_status("✓ GSQL initialized successfully", "success")
                print(Colors.dim(f"Database: {self.db.storage.db_path}"))
                
                # Afficher l'état NLP
                if self.nlp_enabled:
                    print(Colors.dim(f"NLP: Enabled (confidence: {self.config['executor'].get('nlp_confidence_threshold', 0.3)})"))
                else:
                    print(Colors.dim("NLP: Disabled"))
                    
            else:
                print(Colors.success("✓ GSQL ready!"))
                print(Colors.dim(f"Database: {self.db.storage.db_path}"))
                if self.nlp_enabled:
                    print(Colors.dim(f"NLP: Enabled"))
                print(Colors.dim("Type 'help' for commands\n"))
            
        except Exception as e:
            if self.config['shell'].get('rich_ui', True):
                error_box = RichUI.create_box(
                    "INITIALIZATION ERROR",
                    f"Failed to initialize GSQL:\n\n{str(e)}",
                    style='double'
                )
                print(error_box)
            else:
                print(Colors.error(f"Failed to initialize GSQL: {e}"))
            
            traceback.print_exc()
            sys.exit(1)
    
    def run_shell(self, database_path: Optional[str] = None) -> None:
        """Run interactive shell"""
        self.initialize(database_path)
        shell = GSQLShell(self)
        
        signal.signal(signal.SIGINT, shell.sigint_handler)
        
        try:
            shell.cmdloop()
        except KeyboardInterrupt:
            print(f"\n{Colors.info('Session terminated')}")
        finally:
            self.cleanup()
    
    def run_nlp_query(self, nl_query: str, database_path: Optional[str] = None) -> Optional[Dict]:
        """Execute a natural language query"""
        if not self.nlp_enabled:
            # Activer NLP pour cette exécution
            self.setup_nlp(enable_nlp=True)
        
        try:
            self.initialize(database_path)
            
            if not hasattr(self, 'nlp_translator') or not self.nlp_translator:
                print(Colors.error("NLP translator not initialized"))
                return None
            
            # Traduire la requête NL
            translation = self.nlp_translator.translate(nl_query)
            
            print(Colors.info(f"NL Query: {nl_query}"))
            print(Colors.info(f"Translated to SQL: {translation['sql']}"))
            print(Colors.info(f"Confidence: {translation['confidence']:.2f}"))
            
            if translation['explanation']:
                print(Colors.dim(f"Explanation: {translation['explanation']}"))
            
            # Vérifier la confiance
            confidence_threshold = self.config['executor'].get('nlp_confidence_threshold', 0.3)
            if translation['confidence'] < confidence_threshold:
                print(Colors.warning(f"⚠ Low confidence ({translation['confidence']:.2f} < {confidence_threshold})"))
                
                # Demander confirmation
                response = input(Colors.info("Execute this query? (y/N) ")).lower()
                if response not in ['y', 'yes', 'o', 'oui']:
                    print(Colors.info("Query cancelled"))
                    return None
            
            # Exécuter la requête SQL
            result = self.db.execute(translation['sql'])
            
            # Afficher le résultat
            if result.get('success'):
                display = ResultDisplay(self.config['shell'].get('rich_ui', True))
                display.display_generic(result, result.get('type', ''), 
                                      result.get('execution_time', 0))
                return result
            else:
                print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                return None
                
        except Exception as e:
            print(Colors.error(f"Error: {e}"))
            return None
        finally:
            self.cleanup()
    
    def run_query(self, query: str, database_path: Optional[str] = None) -> Optional[Dict]:
        """Execute single query (SQL or NLP)"""
        # Détecter si c'est une requête NLP
        if self.nlp_enabled and self._is_nlp_query(query):
            return self.run_nlp_query(query, database_path)
        else:
            # Exécuter comme requête SQL normale
            try:
                self.initialize(database_path)
                result = self.db.execute(query)
                
                if result.get('success'):
                    display = ResultDisplay(self.config['shell'].get('rich_ui', True))
                    display.display_generic(result, result.get('type', ''), 
                                          result.get('execution_time', 0))
                    return result
                else:
                    print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                    return None
                    
            except Exception as e:
                print(Colors.error(f"Error: {e}"))
                return None
            finally:
                self.cleanup()
    
    def _is_nlp_query(self, query: str) -> bool:
        """Detect if query is natural language"""
        # Ne pas traiter les commandes SQL évidentes
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 
                       'CREATE', 'DROP', 'SHOW', 'DESCRIBE',
                       'ALTER', 'BEGIN', 'COMMIT', 'ROLLBACK']
        
        query_upper = query.strip().upper()
        for keyword in sql_keywords:
            if query_upper.startswith(keyword):
                return False
        
        # Détecter le langage naturel
        nlp_keywords = ['montre', 'affiche', 'combien', 'moyenne', 'somme',
                       'maximum', 'minimum', 'ajoute', 'supprime', 'modifie',
                       'liste', 'donne', 'trouve', 'cherche', 'où', 'avec',
                       'sans', 'par', 'pour', 'dans', 'sur', 'de', 'des']
        
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in nlp_keywords)
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            if self.db:
                self.db.close()
                if self.config['shell'].get('rich_ui', True):
                    RichUI.print_status("Database connection closed", "info")
                else:
                    print(Colors.dim("Database closed"))
        except Exception:
            pass

# ==================== SHELL ENHANCED FOR NLP ====================

class GSQLShell(cmd.Cmd):
    """GSQL Interactive Shell with enhanced NLP support"""
    
    intro = ""
    prompt = ""
    ruler = Colors.dim('─' * 60)
    
    def __init__(self, gsql_app=None):
        super().__init__()
        self.gsql = gsql_app
        self.db = gsql_app.db if gsql_app else None
        self.executor = gsql_app.executor if gsql_app else None
        self.completer = gsql_app.completer if gsql_app else None
        self.nlp_translator = gsql_app.nlp_translator if gsql_app else None
        self.nlp_enabled = gsql_app.nlp_enabled if gsql_app else False
        self.history_file = None
        self.rich_ui = gsql_app.config.get('shell', {}).get('rich_ui', True) if gsql_app else True
        self.result_display = ResultDisplay(self.rich_ui)
        
        self._setup_history()
        self._setup_autocomplete()
        self._setup_prompt()
        
        # Print banner if rich UI is enabled
        if self.rich_ui:
            RichUI.print_banner()
            self.intro = self._get_welcome_message()
        else:
            self.intro = Colors.info("GSQL Interactive Shell") + "\n" + Colors.dim("Type 'help' for commands, 'exit' to quit")
    
    def _get_welcome_message(self) -> str:
        """Get welcome message based on time of day"""
        hour = datetime.now().hour
        
        if hour < 12:
            greeting = "Good morning"
        elif hour < 18:
            greeting = "Good afternoon"
        else:
            greeting = "Good evening"
        
        welcome = f"{greeting}! Welcome to GSQL {__version__}"
        
        # Add NLP info if enabled
        if self.nlp_enabled:
            welcome += "\n" + Colors.success("✓ NLP is enabled")
            welcome += f"\nYou can use natural language queries!"
        
        # Create welcome box
        content = (f"{welcome}\n\n"
                  f"Type {Colors.highlight('.help')} for available commands\n"
                  f"Type {Colors.highlight('.tables')} to list all tables\n"
                  f"Type {Colors.highlight('exit')} or {Colors.highlight('.quit')} to exit")
        
        if self.nlp_enabled:
            content += f"\n\n{Colors.success('Try natural language:')}\n"
            content += f"• {Colors.dim('show tables')}\n"
            content += f"• {Colors.dim('count users')}\n"
            content += f"• {Colors.dim('average salary from employees')}"
        
        return RichUI.create_box("WELCOME", content, style='rounded')
    
    # ... [RESTE DU CODE DU SHELL SIMILAIRE] ...
    
    def default(self, line: str) -> Optional[bool]:
        """Handle default commands (SQL/NLP queries)"""
        if not line.strip():
            return None
        
        if line.startswith('.'):
            return self._handle_dot_command(line)
        elif self.nlp_enabled and self._is_nlp_query(line):
            return self._handle_nlp_query(line)
        else:
            self._execute_sql(line)
            return None
    
    def _is_nlp_query(self, query: str) -> bool:
        """Detect if query is natural language"""
        # Ne pas traiter les commandes SQL évidentes
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 
                       'CREATE', 'DROP', 'SHOW', 'DESCRIBE',
                       'ALTER', 'BEGIN', 'COMMIT', 'ROLLBACK']
        
        query_upper = query.strip().upper()
        for keyword in sql_keywords:
            if query_upper.startswith(keyword):
                return False
        
        # Détecter le langage naturel
        nlp_keywords = ['montre', 'affiche', 'combien', 'moyenne', 'somme',
                       'maximum', 'minimum', 'ajoute', 'supprime', 'modifie',
                       'liste', 'donne', 'trouve', 'cherche', 'où', 'avec',
                       'sans', 'par', 'pour', 'dans', 'sur', 'de', 'des',
                       'show', 'count', 'average', 'sum', 'max', 'min',
                       'add', 'insert', 'delete', 'update', 'where', 'from']
        
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in nlp_keywords)
    
    def _handle_nlp_query(self, nl_query: str) -> Optional[bool]:
        """Handle natural language query"""
        if not self.nlp_translator:
            RichUI.print_status("NLP translator not available", "error")
            return False
        
        try:
            # Translate NL to SQL
            translation = self.nlp_translator.translate(nl_query)
            
            # Display translation info
            if self.rich_ui:
                translation_box = RichUI.create_box(
                    "NLP TRANSLATION",
                    f"Natural Language: {Colors.highlight(nl_query)}\n\n"
                    f"SQL Translation: {Colors.sql_keyword(translation['sql'])}\n"
                    f"Confidence: {Colors.sql_number(f'{translation['confidence']:.2f}')}\n\n"
                    f"Explanation: {translation['explanation']}",
                    style='rounded'
                )
                print(translation_box)
            else:
                print(f"\n{Colors.info('NL Query:')} {nl_query}")
                print(f"{Colors.info('SQL Translation:')} {translation['sql']}")
                print(f"{Colors.info('Confidence:')} {translation['confidence']:.2f}")
                if translation['explanation']:
                    print(f"{Colors.info('Explanation:')} {translation['explanation']}")
            
            # Check confidence threshold
            confidence_threshold = 0.3
            if self.gsql:
                confidence_threshold = self.gsql.config['executor'].get('nlp_confidence_threshold', 0.3)
            
            if translation['confidence'] < confidence_threshold:
                if self.rich_ui:
                    RichUI.print_status(f"⚠ Low confidence ({translation['confidence']:.2f} < {confidence_threshold})", "warning")
                
                # Ask for confirmation
                if self.rich_ui:
                    response = input(f"{Colors.warning('Execute this query?')} (y/N) ").lower()
                else:
                    response = input(f"{Colors.warning('Execute this query? (y/N)')} ").lower()
                
                if response not in ['y', 'yes', 'o', 'oui']:
                    if self.rich_ui:
                        RichUI.print_status("Query cancelled", "info")
                    else:
                        print(Colors.info("Query cancelled"))
                    return False
            
            # Execute the SQL query
            return self._execute_sql(translation['sql'], is_nlp=True, nl_query=nl_query)
            
        except Exception as e:
            if self.rich_ui:
                error_box = RichUI.create_box(
                    "NLP ERROR",
                    f"Failed to process natural language query:\n\n{str(e)}",
                    style='double'
                )
                print(error_box)
            else:
                print(Colors.error(f"NLP Error: {e}"))
            return False
    
    def _execute_sql(self, sql: str, is_nlp: bool = False, nl_query: str = None) -> bool:
        """Execute SQL query with NLP context"""
        if not self.db:
            if self.rich_ui:
                RichUI.print_status("No database connection", "error")
            else:
                print(Colors.error("No database connection"))
            return False
        
        try:
            sql = sql.strip()
            if not sql:
                return False
            
            # Show executing status
            if self.rich_ui and len(sql) > 50:
                RichUI.print_status(f"Executing: {sql[:50]}...", "hourglass")
            elif self.rich_ui:
                RichUI.print_status(f"Executing: {sql}", "hourglass")
            
            start_time = datetime.now()
            result = self.db.execute(sql)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            if result.get('success'):
                # Display result
                self.result_display.display_generic(result, result.get('type', ''), execution_time)
                
                # Learn from successful NLP translation
                if is_nlp and nl_query and self.nlp_translator:
                    # Auto-learn if enabled
                    auto_learn = self.gsql.config['executor'].get('nlp_auto_learn', True) if self.gsql else True
                    if auto_learn:
                        self.nlp_translator.learn_from_example(
                            nl_query=nl_query,
                            sql_query=sql,
                            feedback_score=0.8  # Good feedback for successful execution
                        )
                        if self.rich_ui:
                            RichUI.print_status("Pattern learned from successful execution", "success")
                
                return True
            else:
                if self.rich_ui:
                    error_box = RichUI.create_box(
                        "QUERY ERROR",
                        f"Error: {result.get('message', 'Unknown error')}\n\n"
                        f"SQL: {sql[:100]}{'...' if len(sql) > 100 else ''}",
                        style='double'
                    )
                    print(error_box)
                else:
                    print(Colors.error(f"Query failed: {result.get('message', 'Unknown error')}"))
                
                # Learn from failed NLP translation with lower score
                if is_nlp and nl_query and self.nlp_translator:
                    self.nlp_translator.learn_from_example(
                        nl_query=nl_query,
                        sql_query=sql,
                        feedback_score=0.2  # Low feedback for failed execution
                    )
                
                return False
                
        except Exception as e:
            if self.rich_ui:
                error_box = RichUI.create_box(
                    "EXECUTION ERROR",
                    f"Exception: {str(e)}\n\n"
                    f"Check logs for details.",
                    style='double'
                )
                print(error_box)
            else:
                print(Colors.error(f"Error: {e}"))
            
            config_data = self.gsql.config if self.gsql else {}
            if config_data.get('verbose_errors', True):
                traceback.print_exc()
            
            return False
    
    # Add NLP commands to help
    def do_help(self, arg: str) -> None:
        """Show help with NLP commands"""
        if self.rich_ui:
            help_content = """
{cyan}SQL COMMANDS:{reset}
  {bold}SELECT{reset} * FROM table [WHERE condition] [LIMIT n]
  {bold}INSERT{reset} INTO table (col1, col2) VALUES (val1, val2)
  {bold}UPDATE{reset} table SET col=value [WHERE condition]
  {bold}DELETE{reset} FROM table [WHERE condition]
  {bold}CREATE TABLE{reset} name (col1 TYPE, col2 TYPE, ...)
  {bold}DROP TABLE{reset} name
  {bold}ALTER TABLE{reset} name ADD COLUMN col TYPE
  {bold}CREATE INDEX{reset} idx_name ON table(column)

{cyan}NATURAL LANGUAGE COMMANDS:{reset}
  {bold}show tables{reset}                    - List all tables
  {bold}show columns from table{reset}        - Show table structure
  {bold}count table{reset}                    - Count rows in table
  {bold}average column from table{reset}      - Calculate average
  {bold}sum column from table{reset}          - Calculate sum
  {bold}max/min column from table{reset}      - Find maximum/minimum
  {bold}add to table values{reset}            - Insert new row
  {bold}delete from table where{reset}        - Delete rows
  {bold}update table set where{reset}         - Update rows

{cyan}DOT COMMANDS (SQLite style):{reset}
  {bold}.tables{reset}                        - List tables
  {bold}.schema [table]{reset}                - Show schema
  {bold}.stats{reset}                         - Show stats
  {bold}.help{reset}                          - Show help
  {bold}.backup [file]{reset}                 - Create backup
  {bold}.vacuum{reset}                        - Optimize database
  {bold}.exit / .quit{reset}                  - Exit shell
  {bold}.clear{reset}                         - Clear screen
  {bold}.history{reset}                       - Show command history
  {bold}.nlp [on|off]{reset}                  - Enable/disable NLP
  {bold}.nlp stats{reset}                     - NLP statistics
  {bold}.nlp learn{reset}                     - Learn NLP pattern

{cyan}SHELL COMMANDS:{reset}
  {bold}exit, quit, Ctrl+D{reset}             - Exit GSQL
  {bold}Ctrl+C{reset}                         - Cancel current command
  {bold}Ctrl+L{reset}                         - Clear screen
            """.format(
                cyan=Colors.BRIGHT_CYAN,
                bold=Colors.BOLD,
                reset=Colors.RESET
            )
            
            print(RichUI.create_box("GSQL HELP", help_content, style='double', width=70))
        else:
            help_text = """
GSQL Commands:

SQL COMMANDS:
  SELECT * FROM table [WHERE condition] [LIMIT n]
  INSERT INTO table (col1, col2) VALUES (val1, val2)
  UPDATE table SET col=value [WHERE condition]
  DELETE FROM table [WHERE condition]
  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
  DROP TABLE name
  ALTER TABLE name ADD COLUMN col TYPE
  CREATE INDEX idx_name ON table(column)

NATURAL LANGUAGE COMMANDS:
  show tables                    - List all tables
  show columns from table        - Show table structure
  count table                    - Count rows in table
  average column from table      - Calculate average
  sum column from table          - Calculate sum
  max/min column from table      - Find maximum/minimum
  add to table values            - Insert new row
  delete from table where        - Delete rows
  update table set where         - Update rows

DOT COMMANDS (SQLite style):
  .tables                        - List tables
  .schema [table]                - Show schema
  .stats                         - Show stats
  .help                          - Show help
  .backup [file]                 - Create backup
  .vacuum                        - Optimize database
  .exit / .quit                  - Exit shell
  .clear                         - Clear screen
  .history                       - Show command history
  .nlp [on|off]                  - Enable/disable NLP
  .nlp stats                     - NLP statistics
  .nlp learn                     - Learn NLP pattern

SHELL COMMANDS:
  exit, quit, Ctrl+D             - Exit GSQL
  Ctrl+C                         - Cancel current command
            """
            print(help_text.strip())
    
    # Add NLP dot command handler
    def _handle_nlp_command(self, args: List[str]) -> None:
        """Handle .nlp command"""
        if not args:
            # Show NLP status
            if self.nlp_enabled:
                if self.rich_ui:
                    RichUI.print_status("NLP is enabled", "success")
                else:
                    print(Colors.success("NLP is enabled"))
                
                # Show NLP statistics if translator exists
                if self.nlp_translator:
                    stats = self.nlp_translator.get_statistics()
                    if self.rich_ui:
                        stats_content = f"Patterns loaded: {stats['total_patterns']}\n"
                        stats_content += f"Total translations: {stats['total_translations']}\n"
                        stats_content += f"NLTK available: {stats['nltk_available']}\n"
                        if stats['db_context_loaded']:
                            stats_content += "Database context: Loaded"
                        else:
                            stats_content += "Database context: Not loaded"
                        
                        print(RichUI.create_box("NLP STATISTICS", stats_content, style='rounded'))
                    else:
                        print(f"Patterns loaded: {stats['total_patterns']}")
                        print(f"Total translations: {stats['total_translations']}")
                        print(f"NLTK available: {stats['nltk_available']}")
            else:
                if self.rich_ui:
                    RichUI.print_status("NLP is disabled", "warning")
                else:
                    print(Colors.warning("NLP is disabled"))
            return
        
        cmd = args[0].lower()
        
        if cmd == 'on':
            if self.gsql:
                self.gsql.setup_nlp(enable_nlp=True)
                self.nlp_enabled = True
                if self.rich_ui:
                    RichUI.print_status("NLP enabled", "success")
                else:
                    print(Colors.success("NLP enabled"))
            else:
                if self.rich_ui:
                    RichUI.print_status("Cannot enable NLP", "error")
                else:
                    print(Colors.error("Cannot enable NLP"))
        
        elif cmd == 'off':
            if self.gsql:
                self.gsql.setup_nlp(enable_nlp=False)
                self.nlp_enabled = False
                if self.rich_ui:
                    RichUI.print_status("NLP disabled", "info")
                else:
                    print(Colors.info("NLP disabled"))
        
        elif cmd == 'stats' and self.nlp_translator:
            stats = self.nlp_translator.get_statistics()
            if self.rich_ui:
                # Create detailed stats display
                stats_lines = [
                    f"Total Patterns: {stats['total_patterns']}",
                    f"Total Translations: {stats['total_translations']}",
                    f"Cache Size: {stats['cache_size']}",
                    f"NLTK Available: {stats['nltk_available']}",
                    f"Database Context: {'Loaded' if stats['db_context_loaded'] else 'Not loaded'}",
                    "",
                    "Top 5 Patterns:"
                ]
                
                for i, pattern in enumerate(stats['top_patterns'], 1):
                    stats_lines.append(f"  {i}. {pattern['pattern']} (used {pattern['usage_count']} times)")
                
                print(RichUI.create_box("NLP DETAILED STATISTICS", "\n".join(stats_lines), style='double'))
        
        elif cmd == 'learn':
            if len(args) < 3:
                if self.rich_ui:
                    RichUI.print_status("Usage: .nlp learn <nl_query> <sql_query>", "error")
                else:
                    print(Colors.error("Usage: .nlp learn <nl_query> <sql_query>"))
                return
            
            nl_query = ' '.join(args[1:-1])
            sql_query = args[-1]
            
            if self.nlp_translator:
                success = self.nlp_translator.learn_from_example(nl_query, sql_query)
                if success:
                    if self.rich_ui:
                        RichUI.print_status("Pattern learned successfully", "success")
                    else:
                        print(Colors.success("Pattern learned successfully"))
                else:
                    if self.rich_ui:
                        RichUI.print_status("Failed to learn pattern", "error")
                    else:
                        print(Colors.error("Failed to learn pattern"))
        
        else:
            if self.rich_ui:
                RichUI.print_status(f"Unknown NLP command: {cmd}", "error")
            else:
                print(Colors.error(f"Unknown NLP command: {cmd}"))

# ==================== MAIN FUNCTION ====================

def main() -> None:
    """Main entry point"""
    if not GSQL_AVAILABLE:
        print(Colors.error("GSQL modules not available. Check installation."))
        sys.exit(1)
    
    parser = create_parser()
    args = parser.parse_args()
    
    # Configure output
    if args.no_color:
        config.set('colors', False)
    
    # Configure UI mode
    if args.simple_ui:
        config.set('shell.rich_ui', False)
    
    # Configure logging
    if args.verbose:
        config.set('log_level', 'DEBUG')
        config.set('verbose_errors', True)
    
    # Create and run application
    app = GSQLApp()
    
    # Setup NLP based on arguments
    nlp_enabled = args.enable_nlp or bool(args.nlp)
    app.setup_nlp(
        enable_nlp=nlp_enabled,
        patterns_file=args.nlp_patterns,
        confidence_threshold=args.nlp_confidence
    )
    
    try:
        if args.nlp:
            # Execute NLP query directly
            app.run_nlp_query(args.nlp, args.database)
        elif args.execute:
            # Execute SQL query
            app.run_query(args.execute, args.database)
        elif args.file:
            # Execute from file
            with open(args.file, 'r') as f:
                queries = f.read()
            app.run_query(queries, args.database)
        else:
            # Interactive shell
            app.run_shell(args.database)
            
    except FileNotFoundError as e:
        print(Colors.error(f"File not found: {e}"))
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n{Colors.info('Interrupted by user')}")
        sys.exit(0)
    except Exception as e:
        print(Colors.error(f"Unexpected error: {e}"))
        if args.verbose:
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
