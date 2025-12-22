#!/usr/bin/env python3
"""
GSQL - Un système de gestion de base de données SQL simplifié
Point d'entrée principal pour l'interface en ligne de commande
"""

import sys
import os
import argparse
import shlex
import readline
import atexit
import traceback
from pathlib import Path

# Ajouter le chemin courant au PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Définir les constantes
try:
    from gsql import __version__, __author__, __description__
except ImportError:
    __version__ = "1.0.0"
    __author__ = "Gopu Inc."
    __description__ = "GSQL Database Management System"


class GSQLCLI:
    """Interface en ligne de commande pour GSQL"""
    
    def __init__(self):
        self.database = None
        self.parser = None
        self.executor = None
        self.storage = None
        self.config = self.load_config()
        self.running = True
        self.current_db = None
        self.command_history = []
        self.prompt = "gsql> "
        self.history_file = Path.home() / ".gsql_history"
        self.modules_loaded = False
        self.last_command = ""
        
    def load_modules(self):
        """Charge les modules GSQL dynamiquement"""
        if self.modules_loaded:
            return True
            
        try:
            # Charger les modules principaux
            print("Chargement des modules GSQL...")
            
            # Import direct depuis le répertoire courant
            import database
            import parser
            import executor
            import exceptions
            
            self.Database = database.Database
            self.Parser = parser.Parser
            self.Executor = executor.Executor
            self.GSQLException = exceptions.GSQLException
            self.TableNotFoundError = exceptions.TableNotFoundError
            self.SyntaxError = exceptions.SyntaxError
            
            # Essayer de charger les modules optionnels
            try:
                from stockage import yaml_storage
                self.YAMLStorage = yaml_storage.YAMLStorage
                self.YAML_AVAILABLE = True
                print("✓ Module YAMLStorage chargé")
            except ImportError as e:
                self.YAML_AVAILABLE = False
                print(f"✗ Module YAMLStorage non disponible: {e}")
                
            try:
                from functions import user_functions
                self.register_user_functions = user_functions.register_user_functions
                self.FUNCTIONS_AVAILABLE = True
                print("✓ Fonctions utilisateur disponibles")
            except ImportError:
                self.FUNCTIONS_AVAILABLE = False
                print("✗ Fonctions utilisateur non disponibles")
                
            try:
                from nlp import translator
                self.translate_natural_language = translator.translate_natural_language
                self.NLP_AVAILABLE = True
                print("✓ Module NLP disponible")
            except ImportError:
                self.NLP_AVAILABLE = False
                print("✗ Module NLP non disponible")
                
            self.modules_loaded = True
            print("✓ Modules chargés avec succès")
            return True
            
        except ImportError as e:
            print(f"❌ Erreur de chargement des modules: {e}")
            traceback.print_exc()
            return False
        
    def load_config(self):
        """Charge la configuration"""
        # Configuration par défaut
        config = {
            'storage': {
                'data_dir': str(Path.home() / '.gsql'),
                'format': 'yaml'
            },
            'executor': {
                'transaction_mode': 'auto',
                'timeout': 30
            },
            'nlp': {
                'enabled': False,
                'default_language': 'fr'
            },
            'cli': {
                'prompt': 'gsql[{db}]> ',
                'history_size': 1000,
                'autocomplete': True,
                'colors': True
            }
        }
        
        # Essayer de charger la configuration depuis defaults.yaml
        try:
            import yaml
            config_path = Path(__file__).parent / 'config' / 'defaults.yaml'
            if config_path.exists():
                with open(config_path, 'r') as f:
                    default_config = yaml.safe_load(f)
                    if default_config:
                        config.update(default_config)
        except Exception as e:
            print(f"Note: Configuration par défaut non chargée: {e}")
            
        return config
        
    def setup_argparse(self) -> argparse.ArgumentParser:
        """Configure le parser d'arguments de ligne de commande"""
        parser = argparse.ArgumentParser(
            prog='gsql',
            description=__description__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            add_help=False,
            epilog="""
EXEMPLES:
  gsql                            # Mode interactif
  gsql -h                        # Afficher cette aide
  gsql -v                        # Afficher la version
  gsql --init-db ma_base         # Créer une nouvelle base
  gsql -d ma_base                # Se connecter à une base
  gsql -d ma_base -e "SHOW TABLES"  # Exécuter une commande
  gsql -d ma_base -f script.sql  # Exécuter un script

COMMANDES INTERACTIVES:
  \\? ou \\help       Afficher cette aide
  \\q ou \\quit       Quitter GSQL
  \\v ou \\version    Afficher la version
  \\c DATABASE       Se connecter à une base
  \\l ou \\list      Lister les bases disponibles
  \\dt ou \\d        Lister les tables
  \\d TABLE         Décrire une table
  \\e COMMANDE      Exécuter une commande shell
  \\history         Afficher l'historique
  \\clear ou \\cls   Effacer l'écran
  \\config          Afficher la configuration
  \\stats           Afficher les statistiques
  \\backup [FILE]   Sauvegarder la base
  \\restore FILE    Restaurer une sauvegarde
  \\nlp "texte"     Traduire du texte naturel en SQL
  \\timing          Activer/désactiver le chronométrage
  \\x               Basculer le mode affichage étendu

RACCOURCIS:
  Flèche haut/bas   Navigation dans l'historique
  Ctrl+R           Recherche dans l'historique
  Ctrl+D           Quitter (EOF)
  Ctrl+C           Annuler la commande
  Tab              Auto-complétion
            """
        )
        
        # Groupe pour le mode d'exécution
        execution = parser.add_argument_group('Mode d\'exécution')
        execution.add_argument(
            '-e', '--execute',
            metavar='COMMANDE',
            help='Exécuter une commande SQL'
        )
        execution.add_argument(
            '-f', '--file',
            metavar='FICHIER',
            help='Exécuter un fichier SQL'
        )
        execution.add_argument(
            '-i', '--interactive',
            action='store_true',
            help='Forcer le mode interactif'
        )
        
        # Groupe pour la gestion des bases
        database = parser.add_argument_group('Gestion des bases de données')
        database.add_argument(
            '-d', '--database',
            metavar='NOM',
            help='Base de données à utiliser'
        )
        database.add_argument(
            '--init-db',
            metavar='NOM',
            help='Créer une nouvelle base de données'
        )
        database.add_argument(
            '--data-dir',
            metavar='CHEMIN',
            default=str(Path.home() / '.gsql'),
            help='Répertoire des données (défaut: ~/.gsql)'
        )
        database.add_argument(
            '--host',
            metavar='HOTE',
            help='Hôte du serveur (pour connexion distante)'
        )
        database.add_argument(
            '--port',
            type=int,
            help='Port du serveur'
        )
        
        # Groupe pour la configuration
        config = parser.add_argument_group('Configuration')
        config.add_argument(
            '-c', '--config',
            metavar='FICHIER',
            help='Fichier de configuration'
        )
        config.add_argument(
            '--verbose', '-V',
            action='store_true',
            help='Mode verbeux'
        )
        config.add_argument(
            '--quiet', '-q',
            action='store_true',
            help='Mode silencieux'
        )
        config.add_argument(
            '--no-color',
            action='store_true',
            help='Désactiver les couleurs'
        )
        
        # Groupe pour les informations
        info = parser.add_argument_group('Informations')
        info.add_argument(
            '-v', '--version',
            action='store_true',
            help='Afficher la version'
        )
        info.add_argument(
            '-h', '--help',
            action='store_true',
            help='Afficher cette aide'
        )
        info.add_argument(
            '--license',
            action='store_true',
            help='Afficher la licence'
        )
        info.add_argument(
            '--credits',
            action='store_true',
            help='Afficher les crédits'
        )
        
        return parser
    
    def setup_readline(self):
        """Configure readline pour l'auto-complétion"""
        try:
            # Configurer l'auto-complétion
            readline.set_completer(self.completer)
            readline.set_completer_delims(' \t\n;')
            readline.parse_and_bind("tab: complete")
            
            # Charger l'historique
            if self.history_file.exists():
                try:
                    readline.read_history_file(str(self.history_file))
                except Exception:
                    pass
            
            # Limiter la taille de l'historique
            readline.set_history_length(self.config.get('cli', {}).get('history_size', 1000))
            
            # Sauvegarder l'historique à la sortie
            atexit.register(readline.write_history_file, str(self.history_file))
            
        except Exception as e:
            if self.config.get('verbose'):
                print(f"Note: readline non disponible: {e}")
    
    def completer(self, text, state):
        """Fonction d'auto-complétion"""
        try:
            # Commandes méta
            meta_commands = [
                '\\?', '\\help', '\\q', '\\quit', '\\exit',
                '\\v', '\\version', '\\c', '\\connect',
                '\\l', '\\list', '\\dt', '\\d',
                '\\e', '\\!', '\\history', '\\clear', '\\cls',
                '\\config', '\\stats', '\\backup', '\\restore',
                '\\nlp', '\\timing', '\\x', '\\watch'
            ]
            
            # Commandes SQL
            sql_keywords = [
                'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
                'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DATABASE',
                'DROP', 'ALTER', 'ADD', 'COLUMN', 'INDEX', 'VIEW',
                'GRANT', 'REVOKE', 'BEGIN', 'COMMIT', 'ROLLBACK',
                'SHOW', 'DESCRIBE', 'EXPLAIN', 'USE', 'EXIT', 'QUIT'
            ]
            
            # Noms de tables (si connecté)
            tables = []
            if self.database:
                try:
                    tables = self.database.list_tables()
                except:
                    pass
            
            # Filtrer les suggestions
            suggestions = []
            all_commands = meta_commands + sql_keywords + tables
            
            for cmd in all_commands:
                if cmd.lower().startswith(text.lower()):
                    suggestions.append(cmd)
            
            if state < len(suggestions):
                return suggestions[state]
            else:
                return None
                
        except Exception:
            return None
    
    def print_banner(self):
        """Affiche la bannière d'accueil"""
        if self.config.get('cli', {}).get('colors', True) and not self.config.get('no_color'):
            # Avec couleurs
            banner = f"""
\033[1;36m╔══════════════════════════════════════════════════════════╗\033[0m
\033[1;36m║\033[0m                    \033[1;33mGSQL v{__version__}\033[0m                          \033[1;36m║\033[0m
\033[1;36m║\033[0m          \033[1;32mSystème de Gestion de Base de Données\033[0m          \033[1;36m║\033[0m
\033[1;36m║\033[0m                \033[1;35mDéveloppé par {__author__}\033[0m                \033[1;36m║\033[0m
\033[1;36m╚══════════════════════════════════════════════════════════╝\033[0m

\033[1mTapez '\\?' pour l'aide, '\\q' pour quitter.\033[0m
            """
        else:
            # Sans couleurs
            banner = f"""
╔══════════════════════════════════════════════════════════╗
║                    GSQL v{__version__}                          ║
║          Système de Gestion de Base de Données          ║
║                Développé par {__author__}                ║
╚══════════════════════════════════════════════════════════╝

Tapez '\\?' pour l'aide, '\\q' pour quitter.
            """
        print(banner)
    
    def print_help(self, full=False):
        """Affiche l'aide des commandes"""
        if full:
            self.print_cli_help()
            return
            
        help_text = """
\033[1mCOMMANDES GSQL:\033[0m

\033[1;36m● Générales:\033[0m
  \\? ou \\help       Afficher cette aide
  \\q ou \\quit       Quitter GSQL
  \\v ou \\version    Afficher la version
  \\e COMMANDE      Exécuter une commande shell

\033[1;36m● Bases de données:\033[0m
  \\c DATABASE      Se connecter à une base
  \\l ou \\list      Lister les bases disponibles
  \\create DB       Créer une nouvelle base
  \\drop DB         Supprimer une base

\033[1;36m● Tables:\033[0m
  \\dt ou \\d        Lister les tables
  \\d TABLE         Décrire une table
  \\di             Lister les index
  \\dv             Lister les vues

\033[1;36m● Historique:\033[0m
  \\history         Afficher l'historique
  \\history clear   Effacer l'historique

\033[1;36m● Affichage:\033[0m
  \\clear ou \\cls   Effacer l'écran
  \\timing          Activer/désactiver le chronométrage
  \\x               Basculer le mode affichage étendu
  \\pset OPTION     Configurer l'affichage

\033[1;36m● Administration:\033[0m
  \\config          Afficher la configuration
  \\stats           Afficher les statistiques
  \\backup [FILE]   Sauvegarder la base
  \\restore FILE    Restaurer une sauvegarde
  \\vacuum         Nettoyer la base

\033[1;36m● Intelligence Artificielle:\033[0m
  \\nlp "texte"     Traduire du texte naturel en SQL

\033[1;36m● SQL Standard:\033[0m
  Toutes les commandes SQL standard sont supportées.

\033[1mRaccourcis:\033[0m Flèches (historique), Tab (auto-complétion), Ctrl+R (recherche)
"""
        print(help_text)
    
    def print_cli_help(self):
        """Affiche l'aide de la ligne de commande"""
        parser = self.setup_argparse()
        parser.print_help()
    
    def print_version(self, detailed=False):
        """Affiche la version"""
        if detailed:
            version_info = f"""
\033[1;33mGSQL Database Management System\033[0m
Version:      \033[1;32m{__version__}\033[0m
Auteur:       \033[1;35m{__author__}\033[0m
Description:  \033[1;36m{__description__}\033[0m

\033[1mModules disponibles:\033[0m
  • Database:        {'✓' if hasattr(self, 'Database') else '✗'}
  • Parser:          {'✓' if hasattr(self, 'Parser') else '✗'}
  • Executor:        {'✓' if hasattr(self, 'Executor') else '✗'}
  • YAML Storage:    {'✓' if self.YAML_AVAILABLE else '✗'}
  • NLP:             {'✓' if self.NLP_AVAILABLE else '✗'}
  • User Functions:  {'✓' if self.FUNCTIONS_AVAILABLE else '✗'}

\033[1mConfiguration:\033[0m
  • Répertoire données: {self.config.get('storage', {}).get('data_dir', '~/.gsql')}
  • Mode interactif:    {'Activé' if self.config.get('cli', {}).get('interactive', True) else 'Désactivé'}
  • Couleurs:           {'Activées' if self.config.get('cli', {}).get('colors', True) else 'Désactivées'}

Utilisez \033[1mgsql --help\033[0m pour plus d'informations.
"""
        else:
            version_info = f"GSQL v{__version__} - {__description__}"
        
        print(version_info)
    
    def init_database(self, db_name: str):
        """Initialise une nouvelle base de données"""
        try:
            data_dir = self.config.get('storage', {}).get('data_dir', str(Path.home() / '.gsql'))
            data_path = Path(data_dir).expanduser() / db_name
            
            if data_path.exists():
                print(f"La base de données '{db_name}' existe déjà.")
                return False
            
            data_path.mkdir(parents=True, exist_ok=True)
            
            # Créer un fichier de métadonnées
            meta_file = data_path / 'metadata.yaml'
            import yaml
            metadata = {
                'name': db_name,
                'created': str(datetime.datetime.now()),
                'version': __version__,
                'tables': []
            }
            with open(meta_file, 'w') as f:
                yaml.dump(metadata, f)
            
            print(f"\033[1;32m✓\033[0m Base de données '{db_name}' créée avec succès.")
            print(f"   Emplacement: {data_path}")
            return True
            
        except Exception as e:
            print(f"\033[1;31m✗\033[0m Erreur lors de la création: {e}")
            return False
    
    def connect_database(self, db_name: str):
        """Se connecte à une base de données"""
        if not self.load_modules():
            print("\033[1;31m✗\033[0m Impossible de charger les modules GSQL")
            return False
            
        try:
            data_dir = self.config.get('storage', {}).get('data_dir', str(Path.home() / '.gsql'))
            db_path = Path(data_dir).expanduser() / db_name
            
            if not db_path.exists():
                print(f"\033[1;33m?\033[0m La base '{db_name}' n'existe pas.")
                create = input(f"Voulez-vous la créer ? (o/N): ")
                if create.lower() in ('o', 'oui', 'y', 'yes'):
                    return self.init_database(db_name)
                return False
            
            if self.YAML_AVAILABLE:
                if self.database:
                    self.database.close()
                
                self.storage = self.YAMLStorage(str(db_path))
                self.database = self.Database(self.storage)
                self.parser = self.Parser()
                self.executor = self.Executor(self.database)
                self.current_db = db_name
                
                # Mettre à jour le prompt
                prompt_template = self.config.get('cli', {}).get('prompt', 'gsql[{db}]> ')
                self.prompt = prompt_template.format(db=db_name, user=os.getenv('USER', 'user'))
                
                # Enregistrer les fonctions utilisateur
                if self.FUNCTIONS_AVAILABLE:
                    self.register_user_functions(self.executor)
                
                print(f"\033[1;32m✓\033[0m Connecté à la base '{db_name}'")
                return True
            else:
                print("\033[1;31m✗\033[0m Module de stockage YAML non disponible")
                return False
                
        except Exception as e:
            print(f"\033[1;31m✗\033[0m Erreur de connexion: {e}")
            traceback.print_exc()
            return False
    
    def list_databases(self):
        """Liste les bases de données disponibles"""
        try:
            data_dir = self.config.get('storage', {}).get('data_dir', str(Path.home() / '.gsql'))
            db_path = Path(data_dir).expanduser()
            
            if not db_path.exists():
                print("Aucune base de données trouvée.")
                return
            
            databases = []
            for d in db_path.iterdir():
                if d.is_dir():
                    # Vérifier si c'est une base GSQL valide
                    if (d / 'metadata.yaml').exists() or any(d.glob('*.yaml')):
                        databases.append(d.name)
            
            if not databases:
                print("Aucune base de données GSQL trouvée.")
                return
            
            print("\n\033[1;36mBases de données disponibles:\033[0m")
            print("-" * 50)
            
            for db in sorted(databases):
                size = sum(f.stat().st_size for f in (db_path/db).rglob('*') if f.is_file())
                size_mb = size / (1024 * 1024)
                
                # Lire les métadonnées si disponibles
                meta_file = db_path / db / 'metadata.yaml'
                created = ""
                if meta_file.exists():
                    try:
                        import yaml
                        with open(meta_file, 'r') as f:
                            meta = yaml.safe_load(f)
                            created = meta.get('created', '').split()[0] if meta.get('created') else ""
                    except:
                        pass
                
                marker = " \033[1;32m*\033[0m" if db == self.current_db else ""
                created_info = f" ({created})" if created else ""
                print(f"  \033[1;33m{db:20}\033[0m {size_mb:6.2f} MB{created_info}{marker}")
            
            print(f"\nTotal: {len(databases)} base(s) de données")
            
        except Exception as e:
            print(f"Erreur: {e}")
    
    def list_tables(self, verbose=False):
        """Liste les tables de la base courante"""
        if not self.database:
            print("\033[1;31m✗\033[0m Non connecté à une base de données.")
            return
        
        try:
            tables = self.database.list_tables()
            if not tables:
                print("Aucune table dans la base de données.")
                return
            
            print(f"\n\033[1;36mTables dans la base '{self.current_db}':\033[0m")
            
            if verbose:
                print("-" * 70)
                print(f"{'Table':20} {'Lignes':>8} {'Taille':>10} {'Colonnes':>8} {'Index':>6}")
                print("-" * 70)
                
                for table in sorted(tables):
                    try:
                        info = self.database.get_table_info(table)
                        rows = info.get('row_count', 0)
                        size = info.get('size', 'N/A')
                        cols = len(info.get('columns', []))
                        idx = len(info.get('indexes', []))
                        print(f"{table:20} {rows:8} {str(size):>10} {cols:8} {idx:6}")
                    except:
                        print(f"{table:20} {'N/A':8} {'N/A':>10} {'N/A':8} {'N/A':6}")
                print("-" * 70)
            else:
                print("-" * 40)
                for i, table in enumerate(sorted(tables), 1):
                    print(f"  {i:2}. {table}")
                print("-" * 40)
            
            print(f"Total: {len(tables)} table(s)")
            
        except Exception as e:
            print(f"Erreur: {e}")
    
    def describe_table(self, table_name: str):
        """Décrit une table en détail"""
        if not self.database:
            print("\033[1;31m✗\033[0m Non connecté à une base de données.")
            return
        
        try:
            schema = self.database.get_table_schema(table_name)
            if not schema:
                print(f"\033[1;31m✗\033[0m Table '{table_name}' non trouvée.")
                return
            
            info = self.database.get_table_info(table_name)
            
            print(f"\n\033[1;36mDescription de la table '{table_name}':\033[0m")
            print("=" * 60)
            
            # Informations générales
            print(f"\033[1m● Informations générales:\033[0m")
            print(f"  Lignes:      {info.get('row_count', 0)}")
            print(f"  Taille:      {info.get('size', 'N/A')}")
            print(f"  Créée le:    {info.get('created', 'N/A')}")
            
            # Schéma
            print(f"\n\033[1m● Schéma:\033[0m")
            print("-" * 60)
            print(f"{'Colonne':20} {'Type':15} {'Nullable':10} {'Default':15}")
            print("-" * 60)
            
            for column in schema.get('columns', []):
                name = column.get('name', '')
                type_ = column.get('type', '')
                nullable = "\033[1;32mYES\033[0m" if column.get('nullable', True) else "\033[1;31mNO\033[0m"
                default = str(column.get('default', ''))
                print(f"{name:20} {type_:15} {nullable:10} {default:15}")
            
            # Index
            indexes = self.database.get_table_indexes(table_name)
            if indexes:
                print(f"\n\033[1m● Indexes ({len(indexes)}):\033[0m")
                for idx in indexes:
                    idx_type = idx.get('type', 'BTREE')
                    idx_cols = ', '.join(idx.get('columns', []))
                    print(f"  • {idx.get('name', '')} ({idx_type}) sur {idx_cols}")
            
            # Contraintes
            constraints = schema.get('constraints', [])
            if constraints:
                print(f"\n\033[1m● Contraintes:\033[0m")
                for const in constraints:
                    print(f"  • {const.get('type', '')}: {const.get('name', '')}")
            
            print("=" * 60)
            
        except Exception as e:
            print(f"Erreur: {e}")
    
    def execute_system_command(self, cmd: str):
        """Exécute une commande système"""
        try:
            import subprocess
            import shlex
            
            print(f"\033[1;36mExécution: {cmd}\033[0m")
            print("-" * 60)
            
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.stdout:
                print(result.stdout)
            
            if result.stderr:
                print(f"\033[1;33m{result.stderr}\033[0m")
            
            print(f"-" * 60)
            print(f"\033[1mCode de retour: {result.returncode}\033[0m")
            
        except Exception as e:
            print(f"\033[1;31mErreur: {e}\033[0m")
    
    def show_history(self, clear=False):
        """Affiche ou efface l'historique"""
        if clear:
            self.command_history.clear()
            readline.clear_history()
            print("\033[1;32m✓\033[0m Historique effacé.")
            return
        
        if not self.command_history:
            print("Historique vide.")
            return
        
        print(f"\n\033[1;36mHistorique des commandes:\033[0m")
        print("-" * 80)
        
        # Afficher les 50 dernières commandes
        for i, cmd in enumerate(self.command_history[-50:], 1):
            idx = len(self.command_history) - 50 + i
            if idx < 1:
                idx = i
            
            # Couper les commandes trop longues
            if len(cmd) > 75:
                cmd_display = cmd[:72] + "..."
            else:
                cmd_display = cmd
            
            print(f"{idx:4}: {cmd_display}")
        
        print(f"-" * 80)
        print(f"Total: {len(self.command_history)} commandes")
    
    def show_config(self):
        """Affiche la configuration courante"""
        print(f"\n\033[1;36mConfiguration GSQL:\033[0m")
        print("=" * 60)
        
        # Informations de connexion
        print(f"\033[1m● Connexion:\033[0m")
        if self.current_db:
            data_dir = self.config.get('storage', {}).get('data_dir', '~/.gsql')
            db_path = Path(data_dir).expanduser() / self.current_db
            print(f"  Base courante:  \033[1;33m{self.current_db}\033[0m")
            print(f"  Emplacement:    {db_path}")
        else:
            print(f"  Base courante:  \033[1;31mAucune\033[0m")
        
        # Configuration du stockage
        print(f"\n\033[1m● Stockage:\033[0m")
        storage = self.config.get('storage', {})
        print(f"  Répertoire:     {storage.get('data_dir', '~/.gsql')}")
        print(f"  Format:         {storage.get('format', 'yaml')}")
        print(f"  Compression:    {storage.get('compression', 'non')}")
        
        # Configuration de l'exécuteur
        print(f"\n\033[1m● Exécuteur:\033[0m")
        executor = self.config.get('executor', {})
        print(f"  Mode transaction: {executor.get('transaction_mode', 'auto')}")
        print(f"  Timeout:          {executor.get('timeout', 30)} secondes")
        print(f"  Cache:            {executor.get('cache_size', '100MB')}")
        
        # Configuration CLI
        print(f"\n\033[1m● Interface:\033[0m")
        cli = self.config.get('cli', {})
        print(f"  Couleurs:        {'Activées' if cli.get('colors', True) else 'Désactivées'}")
        print(f"  Auto-complétion: {'Activée' if cli.get('autocomplete', True) else 'Désactivée'}")
        print(f"  Historique:      {cli.get('history_size', 1000)} entrées")
        print(f"  Prompt:          {cli.get('prompt', 'gsql[{db}]> ')}")
        
        # Modules
        print(f"\n\033[1m● Modules:\033[0m")
        print(f"  YAML Storage:    {'✓' if self.YAML_AVAILABLE else '✗'}")
        print(f"  NLP:             {'✓' if self.NLP_AVAILABLE else '✗'}")
        print(f"  User Functions:  {'✓' if self.FUNCTIONS_AVAILABLE else '✗'}")
        
        print("=" * 60)
    
    def show_stats(self):
        """Affiche les statistiques de la base"""
        if not self.database:
            print("\033[1;31m✗\033[0m Non connecté à une base de données.")
            return
        
        try:
            stats = self.database.get_stats()
            
            print(f"\n\033[1;36mStatistiques de la base '{self.current_db}':\033[0m")
            print("=" * 60)
            
            # Statistiques générales
            print(f"\033[1m● Général:\033[0m")
            print(f"  Tables:               {stats.get('table_count', 0)}")
            print(f"  Lignes totales:       {stats.get('total_rows', 0):,}")
            print(f"  Indexes:              {stats.get('index_count', 0)}")
            print(f"  Vues:                 {stats.get('view_count', 0)}")
            
            # Taille
            print(f"\n\033[1m● Taille:\033[0m")
            data_size = stats.get('data_size', 0) / 1024  # Convertir en KB
            index_size = stats.get('index_size', 0) / 1024
            total_size = data_size + index_size
            
            print(f"  Données:             {data_size:,.2f} KB")
            print(f"  Indexes:             {index_size:,.2f} KB")
            print(f"  Total:               {total_size:,.2f} KB")
            
            # Performance
            print(f"\n\033[1m● Performance:\033[0m")
            print(f"  Transactions:         {stats.get('transaction_count', 0)}")
            print(f"  Requêtes exécutées:   {stats.get('query_count', 0):,}")
            print(f"  Cache hits:           {stats.get('cache_hits', 0):,}")
            print(f"  Cache misses:         {stats.get('cache_misses', 0):,}")
            
            if stats.get('cache_hits', 0) + stats.get('cache_misses', 0) > 0:
                hit_ratio = (stats.get('cache_hits', 0) / 
                           (stats.get('cache_hits', 0) + stats.get('cache_misses', 0))) * 100
                print(f"  Taux de cache:        {hit_ratio:.1f}%")
            
            # Activité récente
            print(f"\n\033[1m● Activité:\033[0m")
            print(f"  Dernière requête:     {stats.get('last_query_time', 'N/A')}")
            print(f"  Dernière modification: {stats.get('last_modified', 'N/A')}")
            print(f"  Créée le:             {stats.get('created', 'N/A')}")
            
            print("=" * 60)
            
        except Exception as e:
            print(f"Erreur: {e}")
    
    def backup_database(self, backup_path=None):
        """Sauvegarde la base de données"""
        if not self.database:
            print("\033[1;31m✗\033[0m Non connecté à une base de données.")
            return
        
        try:
            import datetime
            import zipfile
            
            if not backup_path:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"backup_{self.current_db}_{timestamp}.zip"
            
            print(f"Sauvegarde de la base '{self.current_db}'...")
            
            # Utiliser la méthode de sauvegarde de la base de données si disponible
            if hasattr(self.database, 'backup'):
                result = self.database.backup(backup_path)
                print(f"\033[1;32m✓\033[0m Sauvegarde créée: {result}")
            else:
                # Sauvegarde manuelle
                data_dir = self.config.get('storage', {}).get('data_dir', str(Path.home() / '.gsql'))
                db_path = Path(data_dir).expanduser() / self.current_db
                
                with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file in db_path.rglob('*'):
                        if file.is_file():
                            arcname = file.relative_to(db_path.parent)
                            zipf.write(file, arcname)
                
                print(f"\033[1;32m✓\033[0m Sauvegarde créée: {backup_path}")
                print(f"  Taille: {Path(backup_path).stat().st_size / 1024:.1f} KB")
            
        except Exception as e:
            print(f"\033[1;31m✗\033[0m Erreur de sauvegarde: {e}")
    
    def translate_nlp(self, text: str):
        """Traduit du texte naturel en SQL"""
        if not self.NLP_AVAILABLE:
            print("\033[1;33m?\033[0m Module NLP non disponible.")
            return None
        
        try:
            sql = self.translate_natural_language(text, self.config)
            
            print(f"\n\033[1;36mTraduction NLP:\033[0m")
            print("-" * 60)
            print(f"\033[1mTexte naturel:\033[0m")
            print(f"  {text}")
            print(f"\n\033[1mRequête SQL générée:\033[0m")
            print(f"  {sql}")
            print("-" * 60)
            
            return sql
            
        except Exception as e:
            print(f"\033[1;31m✗\033[0m Erreur de traduction: {e}")
            return None
    
    def handle_meta_command(self, command: str) -> bool:
        """Gère les commandes méta (commençant par \\)"""
        original_command = command
        
        # Supprimer le \ initial
        if command.startswith('\\'):
            command = command[1:]
        
        cmd_parts = shlex.split(command)
        if not cmd_parts:
            return True
        
        cmd = cmd_parts[0].lower()
        args = cmd_parts[1:] if len(cmd_parts) > 1 else []
        
        # Table de dispatch des commandes
        commands = {
            # Sortie
            'q': self.cmd_quit,
            'quit': self.cmd_quit,
            'exit': self.cmd_quit,
            
            # Aide
            '?': self.cmd_help,
            'help': self.cmd_help,
            
            # Version
            'v': self.cmd_version,
            'version': self.cmd_version,
            
            # Bases de données
            'c': self.cmd_connect,
            'connect': self.cmd_connect,
            'l': self.cmd_list_dbs,
            'list': self.cmd_list_dbs,
            'create': self.cmd_create_db,
            'drop': self.cmd_drop_db,
            
            # Tables
            'd': self.cmd_describe,
            'dt': self.cmd_list_tables,
            'di': self.cmd_list_indexes,
            'dv': self.cmd_list_views,
            
            # Système
            'e': self.cmd_system,
            '!': self.cmd_system,
            
            # Historique
            'history': self.cmd_history,
            
            # Affichage
            'clear': self.cmd_clear,
            'cls': self.cmd_clear,
            'timing': self.cmd_timing,
            'x': self.cmd_toggle_expanded,
            'pset': self.cmd_pset,
            
            # Administration
            'config': self.cmd_config,
            'stats': self.cmd_stats,
            'backup': self.cmd_backup,
            'restore': self.cmd_restore,
            'vacuum': self.cmd_vacuum,
            
            # NLP
            'nlp': self.cmd_nlp,
            
            # Autres
            'watch': self.cmd_watch,
        }
        
        # Exécuter la commande
        if cmd in commands:
            return commands[cmd](args)
        else:
            print(f"\033[1;31m✗\033[0m Commande inconnue: \\{cmd}")
            print(f"Tapez \\? pour la liste des commandes.")
            return True
    
    # Méthodes de commandes méta
    def cmd_quit(self, args):
        self.running = False
        print("\033[1;32mAu revoir!\033[0m")
        return False
    
    def cmd_help(self, args):
        self.print_help(full=len(args) > 0 and args[0] == 'full')
        return True
    
    def cmd_version(self, args):
        self.print_version(detailed=len(args) > 0 and args[0] == 'detailed')
        return True
    
    def cmd_connect(self, args):
        if not args:
            print("Usage: \\c DATABASE")
            return True
        self.connect_database(args[0])
        return True
    
    def cmd_list_dbs(self, args):
        self.list_databases()
        return True
    
    def cmd_create_db(self, args):
        if not args:
            print("Usage: \\create DATABASE")
            return True
        self.init_database(args[0])
        return True
    
    def cmd_drop_db(self, args):
        if not args:
            print("Usage: \\drop DATABASE")
            return True
        
        db_name = args[0]
        confirm = input(f"Êtes-vous sûr de vouloir supprimer la base '{db_name}' ? (o/N): ")
        if confirm.lower() in ('o', 'oui', 'y', 'yes'):
            data_dir = self.config.get('storage', {}).get('data_dir', str(Path.home() / '.gsql'))
            db_path = Path(data_dir).expanduser() / db_name
            
            if db_path.exists():
                import shutil
                shutil.rmtree(db_path)
                print(f"\033[1;32m✓\033[0m Base '{db_name}' supprimée.")
                
                if self.current_db == db_name:
                    self.current_db = None
                    self.database = None
                    self.prompt = "gsql> "
            else:
                print(f"\033[1;31m✗\033[0m Base '{db_name}' non trouvée.")
        
        return True
    
    def cmd_list_tables(self, args):
        self.list_tables(verbose=len(args) > 0 and args[0] == 'verbose')
        return True
    
    def cmd_describe(self, args):
        if not args:
            self.list_tables()
        else:
            self.describe_table(args[0])
        return True
    
    def cmd_list_indexes(self, args):
        if not self.database:
            print("\033[1;31m✗\033[0m Non connecté à une base de données.")
            return True
        
        try:
            tables = self.database.list_tables()
            print(f"\n\033[1;36mIndexes par table:\033[0m")
            print("-" * 60)
            
            for table in sorted(tables):
                indexes = self.database.get_table_indexes(table)
                if indexes:
                    print(f"\n\033[1m{table}:\033[0m")
                    for idx in indexes:
                        idx_type = idx.get('type', 'BTREE')
                        idx_cols = ', '.join(idx.get('columns', []))
                        print(f"  • {idx.get('name', '')} ({idx_type}) sur {idx_cols}")
            
            print("-" * 60)
            
        except Exception as e:
            print(f"Erreur: {e}")
        
        return True
    
    def cmd_list_views(self, args):
        print("\033[1;33m?\033[0m Fonctionnalité 'vues' non implémentée.")
        return True
    
    def cmd_system(self, args):
        if not args:
            print("Usage: \\e COMMANDE")
            return True
        self.execute_system_command(' '.join(args))
        return True
    
    def cmd_history(self, args):
        if args and args[0] == 'clear':
            self.show_history(clear=True)
        else:
            self.show_history()
        return True
    
    def cmd_clear(self, args):
        os.system('cls' if os.name == 'nt' else 'clear')
        return True
    
    def cmd_timing(self, args):
        print("\033[1;33m?\033[0m Fonctionnalité 'timing' non implémentée.")
        return True
    
    def cmd_toggle_expanded(self, args):
        print("\033[1;33m?\033[0m Fonctionnalité 'mode étendu' non implémentée.")
        return True
    
    def cmd_pset(self, args):
        if not args:
            print("Usage: \\pset OPTION [VALEUR]")
            return True
        print("\033[1;33m?\033[0m Configuration d'affichage non implémentée.")
        return True
    
    def cmd_config(self, args):
        self.show_config()
        return True
    
    def cmd_stats(self, args):
        self.show_stats()
        return True
    
    def cmd_backup(self, args):
        self.backup_database(args[0] if args else None)
        return True
    
    def cmd_restore(self, args):
        if not args:
            print("Usage: \\restore FICHIER")
            return True
        print("\033[1;33m?\033[0m Fonctionnalité 'restore' non implémentée.")
        return True
    
    def cmd_vacuum(self, args):
        if not self.database:
            print("\033[1;31m✗\033[0m Non connecté à une base de données.")
            return True
        
        try:
            if hasattr(self.database, 'vacuum'):
                self.database.vacuum()
                print("\033[1;32m✓\033[0m Nettoyage terminé.")
            else:
                print("\033[1;33m?\033[0m Fonctionnalité 'vacuum' non disponible.")
        except Exception as e:
            print(f"\033[1;31m✗\033[0m Erreur: {e}")
        
        return True
    
    def cmd_nlp(self, args):
        if not args:
            print("Usage: \\nlp \"texte en langage naturel\"")
            return True
        
        sql = self.translate_nlp(' '.join(args))
        if sql and input("\nExécuter cette requête ? (o/N): ").lower() in ('o', 'oui', 'y', 'yes'):
            return self.execute_query(sql)
        
        return True
    
    def cmd_watch(self, args):
        print("\033[1;33m?\033[0m Fonctionnalité 'watch' non implémentée.")
        return True
    
    def execute_query(self, query: str) -> bool:
        """Exécute une requête SQL"""
        if not self.database:
            print("\033[1;31m✗\033[0m Erreur: Non connecté à une base de données.")
            print("Utilisez '\\c NOM_BASE' pour vous connecter ou '\\create NOM_BASE' pour en créer une.")
            return False
        
        try:
            import time
            start_time = time.time()
            
            # Parser la requête
            parsed = self.parser.parse(query)
            
            # Exécuter la requête
            result = self.executor.execute(parsed)
            
            elapsed = time.time() - start_time
            
            # Afficher le résultat
            if result is not None:
                if isinstance(result, list):
                    # Résultat tabulaire
                    if result:
                        headers = list(result[0].keys())
                        
                        # Calculer les largeurs de colonnes
                        col_widths = [len(str(h)) for h in headers]
                        for row in result:
                            for i, h in enumerate(headers):
                                col_widths[i] = max(col_widths[i], len(str(row.get(h, ''))))
                        
                        # Afficher l'en-tête
                        header_line = " | ".join(
                            f"\033[1;36m{str(h).ljust(col_widths[i])}\033[0m" 
                            for i, h in enumerate(headers)
                        )
                        separator = "-+-".join("-" * w for w in col_widths)
                        
                        print(f"\n{header_line}")
                        print(separator)
                        
                        # Afficher les lignes
                        for row in result:
                            line = " | ".join(
                                str(row.get(h, '')).ljust(col_widths[i])
                                for i, h in enumerate(headers)
                            )
                            print(line)
                        
                        print(f"\n\033[1;32m✓\033[0m {len(result)} ligne{'s' if len(result) > 1 else ''} retournée{'s' if len(result) > 1 else ''}")
                    else:
                        print(f"\n\033[1;33m?\033[0m Aucun résultat")
                else:
                    # Résultat simple
                    print(f"\n\033[1;32m✓\033[0m {result}")
            
            # Afficher le temps d'exécution
            if elapsed > 0.1:  # Seulement si significatif
                print(f"Temps d'exécution: {elapsed:.3f} secondes")
            
            print()  # Ligne vide
            return True
            
        except self.SyntaxError as e:
            print(f"\033[1;31m✗\033[0m Erreur de syntaxe: {e}")
            return False
        except self.TableNotFoundError as e:
            print(f"\033[1;31m✗\033[0m Table non trouvée: {e}")
            return False
        except self.GSQLException as e:
            print(f"\033[1;31m✗\033[0m Erreur GSQL: {e}")
            return False
        except Exception as e:
            print(f"\033[1;31m✗\033[0m Erreur inattendue: {e}")
            if self.config.get('verbose'):
                traceback.print_exc()
            return False
    
    def interactive_mode(self):
        """Lance le mode interactif"""
        self.print_banner()
        self.setup_readline()
        
        print(f"Utilisez \033[1m\\c NOM_BASE\033[0m pour vous connecter à une base de données.")
        print(f"Utilisez \033[1m\\create NOM_BASE\033[0m pour créer une nouvelle base.\n")
        
        # Boucle principale interactive
        current_buffer = []
        multi_line = False
        
        while self.running:
            try:
                # Déterminer le prompt
                if multi_line:
                    prompt = "     -> "
                else:
                    prompt = self.prompt
                
                # Lire la ligne
                try:
                    line = input(prompt).strip()
                except EOFError:
                    print("\n\033[1;32mAu revoir!\033[0m")
                    break
                except KeyboardInterrupt:
                    print("\n\033[1;33m(Utilisez '\\q' pour quitter)\033[0m")
                    current_buffer = []
                    multi_line = False
                    continue
                
                # Ignorer les lignes vides
                if not line:
                    if multi_line:
                        continue
                    else:
                        continue
                
                # Sauvegarder dans l'historique
                if not multi_line or not current_buffer:
                    self.command_history.append(line)
                    readline.add_history(line)
                
                # Gestion des commandes multilignes
                if line.endswith('\\'):
                    # Continuer sur la ligne suivante
                    current_buffer.append(line[:-1].strip())
                    multi_line = True
                    continue
                else:
                    # Ajouter à la buffer
                    if current_buffer:
                        current_buffer.append(line)
                        buffer_text = ' '.join(current_buffer)
                    else:
                        buffer_text = line
                    
                    # Réinitialiser le mode multiligne
                    multi_line = False
                    current_buffer = []
                
                # Vérifier si la commande est complète
                if buffer_text.startswith('\\'):
                    # Commande méta - exécuter immédiatement
                    self.handle_meta_command(buffer_text)
                elif buffer_text.endswith(';') or ';' in buffer_text:
                    # Commander SQL avec point-virgule
                    # Diviser par les points-virgules
                    parts = buffer_text.split(';')
                    for part in parts:
                        part = part.strip()
                        if part:
                            if part.startswith('\\'):
                                self.handle_meta_command(part)
                            else:
                                self.execute_query(part)
                else:
                    # Commander SQL sans point-virgule - exécuter quand même
                    self.execute_query(buffer_text)
                
            except Exception as e:
                print(f"\033[1;31m✗\033[0m Erreur: {e}")
                if self.config.get('verbose'):
                    traceback.print_exc()
                current_buffer = []
                multi_line = False
    
    def execute_file(self, file_path: str):
        """Exécute les commandes depuis un fichier"""
        if not self.database:
            print("\033[1;31m✗\033[0m Erreur: Non connecté à une base de données.")
            return
        
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                print(f"\033[1;31m✗\033[0m Fichier non trouvé: {file_path}")
                return
            
            print(f"\033[1;36mExécution du fichier: {file_path}\033[0m")
            print("=" * 60)
            
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Diviser en lignes et traiter
            lines = content.split('\n')
            current_buffer = []
            
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                
                # Ignorer les commentaires et lignes vides
                if not line or line.startswith('--'):
                    continue
                
                # Gestion des commandes multilignes
                if line.endswith('\\'):
                    current_buffer.append(line[:-1].strip())
                    continue
                
                # Ajouter la ligne au buffer
                if current_buffer:
                    current_buffer.append(line)
                    buffer_text = ' '.join(current_buffer)
                    current_buffer = []
                else:
                    buffer_text = line
                
                # Exécuter la commande
                print(f"\n\033[1m[{line_num}] {buffer_text[:50]}...\033[0m" if len(buffer_text) > 50 else f"\n\033[1m[{line_num}] {buffer_text}\033[0m")
                
                if buffer_text.startswith('\\'):
                    self.handle_meta_command(buffer_text)
                else:
                    self.execute_query(buffer_text)
            
            print("=" * 60)
            print(f"\033[1;32m✓\033[0m Fichier exécuté avec succès")
            
        except Exception as e:
            print(f"\033[1;31m✗\033[0m Erreur lors de l'exécution du fichier: {e}")
            if self.config.get('verbose'):
                traceback.print_exc()
    
    def run(self, args=None):
        """Point d'entrée principal"""
        import datetime
        
        if args is None:
            args = sys.argv[1:]
        
        # Vérifier les options simples d'abord
        if '-h' in args or '--help' in args:
            self.print_cli_help()
            return
        
        if '-v' in args or '--version' in args:
            self.print_version(detailed='--version' in args)
            return
        
        if '--license' in args:
            print("GSQL - Licence MIT")
            print("Copyright (c) 2024 Gopu Inc.")
            return
        
        if '--credits' in args:
            print("GSQL - Crédits")
            print("Développé par Gopu Inc.")
            print("Contributeurs: L'équipe GSQL")
            return
        
        # Parser les arguments
        parser = self.setup_argparse()
        
        try:
            parsed_args = parser.parse_args(args)
        except SystemExit:
            return
        
        # Mettre à jour la configuration avec les arguments
        if parsed_args.verbose:
            self.config['verbose'] = True
        
        if parsed_args.quiet:
            self.config['cli']['colors'] = False
        
        if parsed_args.no_color:
            self.config['cli']['colors'] = False
        
        if parsed_args.config:
            try:
                import yaml
                with open(parsed_args.config, 'r') as f:
                    user_config = yaml.safe_load(f)
                    self.config.update(user_config)
            except Exception as e:
                print(f"Erreur de chargement de la configuration: {e}")
        
        # Initialiser une base de données
        if parsed_args.init_db:
            self.init_database(parsed_args.init_db)
            return
        
        # Se connecter à une base
        db_to_connect = None
        if parsed_args.database:
            db_to_connect = parsed_args.database
        
        if db_to_connect:
            if not self.connect_database(db_to_connect):
                print("Échec de la connexion. Mode interactif sans base.")
        
        # Exécuter une commande unique
        if parsed_args.execute:
            if not self.database:
                print("\033[1;31m✗\033[0m Erreur: Non connecté à une base de données.")
                print("Utilisez: gsql -d NOM_BASE -e 'COMMANDE'")
                return
            
            if parsed_args.execute.startswith('\\'):
                self.handle_meta_command(parsed_args.execute)
            else:
                self.execute_query(parsed_args.execute)
            return
        
        # Exécuter un fichier
        if parsed_args.file:
            if not self.database:
                print("\033[1;31m✗\033[0m Erreur: Non connecté à une base de données.")
                print("Utilisez: gsql -d NOM_BASE -f FICHIER")
                return
            
            self.execute_file(parsed_args.file)
            return
        
        # Mode interactif
        self.interactive_mode()


def main():
    """Fonction principale"""
    import datetime
    
    try:
        cli = GSQLCLI()
        cli.run()
    except KeyboardInterrupt:
        print("\n\n\033[1;33mInterruption par l'utilisateur\033[0m")
        sys.exit(0)
    except Exception as e:
        print(f"\n\033[1;31mErreur fatale:\033[0m {e}")
        if '--verbose' in sys.argv:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
