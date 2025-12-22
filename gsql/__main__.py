#!/usr/bin/env python3
"""
GSQL - Un système de gestion de base de données SQL simplifié
Point d'entrée principal pour l'interface en ligne de commande
"""

import sys
import os
import argparse
import shlex
from typing import List
from pathlib import Path

# Ajouter le chemin du module parent
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import des modules GSQL
try:
    from gsql import __version__, __author__, __description__
    
    # Import des autres modules
    from database import Database
    from parser import Parser
    from executor import Executor
    from exceptions import GSQLException, TableNotFoundError, SyntaxError
    
    # Essayer d'importer les modules optionnels
    try:
        from stockage.yaml_storage import YAMLStorage
        YAML_AVAILABLE = True
    except ImportError:
        print("Warning: YAMLStorage non disponible")
        YAML_AVAILABLE = False
        
    try:
        from config.defaults import load_default_config
        CONFIG_AVAILABLE = True
    except ImportError:
        print("Warning: Configuration par défaut non disponible")
        CONFIG_AVAILABLE = False
        
    try:
        from functions.user_functions import register_user_functions
        FUNCTIONS_AVAILABLE = True
    except ImportError:
        print("Warning: User functions non disponibles")
        FUNCTIONS_AVAILABLE = False
        
    try:
        from nlp.translator import translate_natural_language
        NLP_AVAILABLE = True
    except ImportError:
        print("Warning: Module NLP non disponible")
        NLP_AVAILABLE = False
        
except ImportError as e:
    print(f"Erreur d'importation: {e}")
    print("Assurez-vous que tous les modules GSQL sont disponibles.")
    sys.exit(1)


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
        
    def load_config(self):
        """Charge la configuration"""
        if CONFIG_AVAILABLE:
            return load_default_config()
        else:
            # Configuration par défaut
            return {
                'storage': {
                    'data_dir': '~/.gsql',
                    'format': 'yaml'
                },
                'executor': {
                    'transaction_mode': 'auto',
                    'timeout': 30
                },
                'nlp': {
                    'enabled': NLP_AVAILABLE,
                    'default_language': 'fr'
                }
            }
        
    def setup_argparse(self) -> argparse.ArgumentParser:
        """Configure le parser d'arguments de ligne de commande"""
        parser = argparse.ArgumentParser(
            description=__description__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Exemples d'utilisation:
  gsql                            # Mode interactif
  gsql --help                     # Afficher l'aide
  gsql --version                  # Afficher la version
  gsql -e "SELECT * FROM users"   # Exécuter une commande
  gsql -f query.sql               # Exécuter un fichier SQL
  gsql --init-db mydatabase       # Initialiser une nouvelle base
  gsql -d mydb                    # Se connecter à une base
  gsql -c ./config.yaml           # Spécifier un fichier de config
            
Commandes interactives:
  \\q, \\quit, exit     - Quitter GSQL
  \\h, \\help          - Afficher l'aide
  \\v, \\version       - Afficher la version
  \\d, \\dt            - Lister les tables
  \\d table_name       - Décrire une table
  \\l, \\list          - Lister les bases
  \\c db_name         - Se connecter à une base
  \\e command         - Exécuter une commande système
  \\history           - Afficher l'historique
  \\clear, \\cls       - Effacer l'écran
  \\config            - Afficher la configuration
  \\stats             - Afficher les statistiques
  \\backup [path]     - Sauvegarder la base
  \\nlp "requête"     - Traduire une requête naturelle en SQL
            """
        )
        
        # Mode d'exécution
        mode_group = parser.add_argument_group('Mode d\'exécution')
        mode_group.add_argument(
            '-e', '--execute',
            type=str,
            help='Exécuter une commande SQL et quitter'
        )
        mode_group.add_argument(
            '-f', '--file',
            type=str,
            help='Exécuter les commandes depuis un fichier'
        )
        mode_group.add_argument(
            '-i', '--interactive',
            action='store_true',
            default=False,
            help='Lancer le mode interactif'
        )
        
        # Gestion de la base de données
        db_group = parser.add_argument_group('Gestion de base de données')
        db_group.add_argument(
            '-d', '--database',
            type=str,
            help='Nom de la base de données à utiliser'
        )
        db_group.add_argument(
            '-I', '--init-db',
            type=str,
            help='Initialiser une nouvelle base de données'
        )
        db_group.add_argument(
            '--data-dir',
            type=str,
            default=None,
            help='Répertoire des données (défaut: ~/.gsql)'
        )
        
        # Configuration
        config_group = parser.add_argument_group('Configuration')
        config_group.add_argument(
            '-c', '--config',
            type=str,
            help='Fichier de configuration à utiliser'
        )
        config_group.add_argument(
            '--verbose',
            action='store_true',
            help='Mode verbeux'
        )
        config_group.add_argument(
            '--quiet',
            action='store_true',
            help='Mode silencieux'
        )
        
        # Informations
        info_group = parser.add_argument_group('Informations')
        info_group.add_argument(
            '-v', '--version',
            action='store_true',
            help='Afficher la version et quitter'
        )
        info_group.add_argument(
            '--license',
            action='store_true',
            help='Afficher les informations de licence'
        )
        
        # Arguments positionnels (pour compatibilité)
        parser.add_argument(
            'command',
            nargs='?',
            type=str,
            help='Commande SQL à exécuter'
        )
        parser.add_argument(
            'dbname',
            nargs='?',
            type=str,
            help='Nom de la base de données'
        )
        
        return parser
    
    def print_banner(self):
        """Affiche la bannière d'accueil"""
        banner = f"""
╔══════════════════════════════════════════════════════════╗
║                    GSQL v{__version__}                          ║
║          Système de Gestion de Base de Données          ║
║                Développé par {__author__}                ║
╚══════════════════════════════════════════════════════════╝
Tapez '\\help' pour l'aide, '\\quit' pour quitter.
        """
        print(banner)
    
    def print_help(self):
        """Affiche l'aide des commandes"""
        help_text = """
Commandes GSQL:

GÉNÉRALES:
  \\q, \\quit, exit     - Quitter GSQL
  \\h, \\help          - Afficher cette aide
  \\v, \\version       - Afficher la version
  \\e command         - Exécuter une commande système
  
BASES DE DONNÉES:
  \\l, \\list          - Lister les bases de données
  \\c db_name         - Se connecter à une base
  \\create db_name    - Créer une nouvelle base
  \\drop db_name      - Supprimer une base
  
TABLES:
  \\d, \\dt            - Lister les tables
  \\d table_name      - Décrire une table
  \\di                - Lister les index
  
EXÉCUTION:
  ;                  - Terminer une commande (optionnel)
  \\g                 - Exécuter la dernière commande
  \\p                - Afficher le tampon de commande
  
HISTORIQUE:
  \\history           - Afficher l'historique
  \\history clear     - Effacer l'historique
  
AFFICHAGE:
  \\clear, \\cls       - Effacer l'écran
  \\timing            - Activer/désactiver le chronométrage
  
ADMINISTRATION:
  \\config            - Afficher la configuration
  \\stats             - Afficher les statistiques
  \\backup [path]     - Sauvegarder la base
  \\restore [path]    - Restaurer une sauvegarde
  \\vacuum            - Nettoyer la base
  
INTELLIGENCE ARTIFICIELLE:
  \\nlp "requête"     - Traduire une requête naturelle en SQL (si disponible)
  
SQL STANDARD:
  Toutes les commandes SQL standard sont supportées:
  - SELECT, INSERT, UPDATE, DELETE
  - CREATE, DROP, ALTER TABLE
  - CREATE INDEX, DROP INDEX
  - BEGIN, COMMIT, ROLLBACK
        """
        print(help_text)
    
    def print_version(self):
        """Affiche la version"""
        version_info = f"""
GSQL Version: {__version__}
Auteur: {__author__}
Description: {__description__}

Modules disponibles:
  - Database: {'✓' if self.database else '✗'}
  - Parser: {'✓' if self.parser else '✗'}
  - Executor: {'✓' if self.executor else '✗'}
  - YAML Storage: {'✓' if YAML_AVAILABLE else '✗'}
  - NLP Translator: {'✓' if NLP_AVAILABLE else '✗'}
  - User Functions: {'✓' if FUNCTIONS_AVAILABLE else '✗'}
        """
        print(version_info)
    
    def init_database(self, db_name: str):
        """Initialise une nouvelle base de données"""
        try:
            data_dir = self.config.get('storage', {}).get('data_dir', '~/.gsql')
            data_path = Path(data_dir).expanduser() / db_name
            data_path.mkdir(parents=True, exist_ok=True)
            
            print(f"Base de données '{db_name}' créée dans {data_path}")
            return True
        except Exception as e:
            print(f"Erreur lors de la création de la base: {e}")
            return False
    
    def connect_database(self, db_name: str):
        """Se connecte à une base de données"""
        try:
            if self.database:
                self.database.close()
            
            data_dir = self.config.get('storage', {}).get('data_dir', '~/.gsql')
            db_path = Path(data_dir).expanduser() / db_name
            
            if not db_path.exists():
                print(f"Base de données '{db_name}' non trouvée.")
                print(f"Utilisez '\\create {db_name}' pour la créer.")
                return False
            
            if YAML_AVAILABLE:
                self.storage = YAMLStorage(str(db_path))
                self.database = Database(self.storage)
                self.parser = Parser()
                self.executor = Executor(self.database)
                self.current_db = db_name
                self.prompt = f"gsql[{db_name}]> "
                
                # Enregistrer les fonctions utilisateur si disponible
                if FUNCTIONS_AVAILABLE:
                    register_user_functions(self.executor)
                
                print(f"Connecté à la base de données '{db_name}'")
                return True
            else:
                print("Erreur: YAMLStorage non disponible")
                return False
                
        except Exception as e:
            print(f"Erreur de connexion: {e}")
            return False
    
    def list_databases(self):
        """Liste les bases de données disponibles"""
        try:
            data_dir = self.config.get('storage', {}).get('data_dir', '~/.gsql')
            db_path = Path(data_dir).expanduser()
            
            if not db_path.exists():
                print("Aucune base de données trouvée.")
                return
            
            databases = [d.name for d in db_path.iterdir() if d.is_dir()]
            
            if not databases:
                print("Aucune base de données trouvée.")
                return
            
            print("\nListe des bases de données:")
            print("-" * 40)
            for db in sorted(databases):
                size = sum(f.stat().st_size for f in (db_path/db).rglob('*') if f.is_file())
                size_mb = size / (1024 * 1024)
                marker = " *" if db == self.current_db else ""
                print(f"  {db:20} {size_mb:6.2f} MB{marker}")
            print()
        except Exception as e:
            print(f"Erreur: {e}")
    
    def list_tables(self):
        """Liste les tables de la base courante"""
        if not self.database:
            print("Non connecté à une base de données.")
            return
        
        try:
            tables = self.database.list_tables()
            if not tables:
                print("Aucune table dans la base de données.")
                return
            
            print("\nListe des tables:")
            print("-" * 60)
            print(f"{'Nom':20} {'Lignes':10} {'Index'}")
            print("-" * 60)
            
            for table in tables:
                try:
                    table_info = self.database.get_table_info(table)
                    row_count = table_info.get('row_count', 0)
                    indexes = len(table_info.get('indexes', []))
                    print(f"{table:20} {row_count:10} {indexes:5}")
                except:
                    print(f"{table:20} {'N/A':10} {'N/A':5}")
            print()
        except Exception as e:
            print(f"Erreur: {e}")
    
    def describe_table(self, table_name: str):
        """Décrit une table"""
        if not self.database:
            print("Non connecté à une base de données.")
            return
        
        try:
            schema = self.database.get_table_schema(table_name)
            if not schema:
                print(f"Table '{table_name}' non trouvée.")
                return
            
            print(f"\nStructure de la table '{table_name}':")
            print("-" * 60)
            print(f"{'Colonne':20} {'Type':15} {'Nullable':10}")
            print("-" * 60)
            
            for column in schema.get('columns', []):
                name = column.get('name', '')
                type_ = column.get('type', '')
                nullable = "YES" if column.get('nullable', True) else "NO"
                print(f"{name:20} {type_:15} {nullable:10}")
            
            # Afficher les index si disponibles
            try:
                indexes = self.database.get_table_indexes(table_name)
                if indexes:
                    print("\nIndexes:")
                    for idx in indexes:
                        print(f"  - {idx.get('name', 'unknown')}")
            except:
                pass
                
            print()
        except Exception as e:
            print(f"Erreur: {e}")
    
    def execute_system_command(self, cmd: str):
        """Exécute une commande système"""
        try:
            import subprocess
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print(result.stderr)
        except Exception as e:
            print(f"Erreur: {e}")
    
    def show_history(self):
        """Affiche l'historique des commandes"""
        if not self.command_history:
            print("Historique vide.")
            return
        
        print("\nHistorique des commandes:")
        print("-" * 60)
        for i, cmd in enumerate(self.command_history[-20:], 1):
            print(f"{i:3}: {cmd}")
        print()
    
    def show_config(self):
        """Affiche la configuration"""
        print("\nConfiguration GSQL:")
        print("-" * 60)
        
        # Configuration de la base
        if self.current_db:
            print(f"Base courante:      {self.current_db}")
        else:
            print("Base courante:      Aucune")
        
        # Configuration du stockage
        storage_config = self.config.get('storage', {})
        print(f"Répertoire données: {storage_config.get('data_dir', '~/.gsql')}")
        print(f"Format stockage:    {storage_config.get('format', 'yaml')}")
        
        # Configuration de l'exécuteur
        executor_config = self.config.get('executor', {})
        print(f"Mode transaction:   {executor_config.get('transaction_mode', 'auto')}")
        print(f"Timeout:            {executor_config.get('timeout', 30)}s")
        
        # Configuration NLP
        nlp_config = self.config.get('nlp', {})
        print(f"NLP activé:         {nlp_config.get('enabled', False)}")
        print()
    
    def show_stats(self):
        """Affiche les statistiques"""
        if not self.database:
            print("Non connecté à une base de données.")
            return
        
        try:
            stats = self.database.get_stats()
            print("\nStatistiques de la base de données:")
            print("-" * 60)
            print(f"Tables:             {stats.get('table_count', 0)}")
            print(f"Lignes totales:     {stats.get('total_rows', 0)}")
            print(f"Taille données:     {stats.get('data_size', 0):.2f} KB")
            print(f"Transactions:       {stats.get('transaction_count', 0)}")
            print()
        except Exception as e:
            print(f"Erreur: {e}")
    
    def backup_database(self, path: str = None):
        """Sauvegarde la base de données"""
        if not self.database:
            print("Non connecté à une base de données.")
            return
        
        try:
            if not path:
                import datetime
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                path = f"backup_{self.current_db}_{timestamp}.zip"
            
            backup_path = self.database.backup(path)
            print(f"Sauvegarde créée: {backup_path}")
        except Exception as e:
            print(f"Erreur lors de la sauvegarde: {e}")
    
    def translate_nlp(self, query: str):
        """Traduit une requête en langage naturel en SQL"""
        if not NLP_AVAILABLE:
            print("Module NLP non disponible.")
            return None
        
        try:
            sql_query = translate_natural_language(query, self.config)
            print(f"\nRequête SQL générée:")
            print("-" * 60)
            print(sql_query)
            print("-" * 60)
            return sql_query
        except Exception as e:
            print(f"Erreur de traduction: {e}")
            return None
    
    def handle_meta_command(self, command: str) -> bool:
        """Gère les commandes méta (commençant par \\)"""
        cmd_parts = shlex.split(command.strip())
        if not cmd_parts:
            return True
        
        cmd = cmd_parts[0].lower()
        args = cmd_parts[1:] if len(cmd_parts) > 1 else []
        
        # Commandes de sortie
        if cmd in ('\\q', '\\quit', 'quit', 'exit'):
            self.running = False
            print("Au revoir!")
            return True
        
        # Aide
        elif cmd in ('\\h', '\\help', 'help'):
            self.print_help()
            return True
        
        # Version
        elif cmd in ('\\v', '\\version', 'version'):
            self.print_version()
            return True
        
        # Lister les bases
        elif cmd in ('\\l', '\\list'):
            self.list_databases()
            return True
        
        # Se connecter à une base
        elif cmd in ('\\c',):
            if not args:
                print("Usage: \\c db_name")
                return True
            self.connect_database(args[0])
            return True
        
        # Créer une base
        elif cmd in ('\\create',):
            if not args:
                print("Usage: \\create db_name")
                return True
            self.init_database(args[0])
            return True
        
        # Lister les tables
        elif cmd in ('\\d', '\\dt'):
            if len(args) == 0:
                self.list_tables()
            else:
                self.describe_table(args[0])
            return True
        
        # Exécuter une commande système
        elif cmd in ('\\e',):
            if not args:
                print("Usage: \\e command")
                return True
            self.execute_system_command(' '.join(args))
            return True
        
        # Historique
        elif cmd in ('\\history',):
            if args and args[0] == 'clear':
                self.command_history.clear()
                print("Historique effacé.")
            else:
                self.show_history()
            return True
        
        # Configuration
        elif cmd in ('\\config',):
            self.show_config()
            return True
        
        # Statistiques
        elif cmd in ('\\stats',):
            self.show_stats()
            return True
        
        # Sauvegarde
        elif cmd in ('\\backup',):
            self.backup_database(args[0] if args else None)
            return True
        
        # Nettoyage
        elif cmd in ('\\vacuum',):
            if self.database:
                self.database.vacuum()
                print("Nettoyage terminé.")
            return True
        
        # NLP
        elif cmd in ('\\nlp',):
            if not args:
                print("Usage: \\nlp \"requête en langage naturel\"")
                return True
            sql_query = self.translate_nlp(' '.join(args))
            if sql_query and input("\nExécuter cette requête? (o/n): ").lower() == 'o':
                return self.execute_query(sql_query)
            return True
        
        # Effacer l'écran
        elif cmd in ('\\clear', '\\cls'):
            os.system('cls' if os.name == 'nt' else 'clear')
            return True
        
        # Commande méta inconnue
        else:
            print(f"Commande méta inconnue: {cmd}")
            print("Tapez \\help pour la liste des commandes.")
            return True
    
    def execute_query(self, query: str) -> bool:
        """Exécute une requête SQL"""
        if not self.database:
            print("Erreur: Non connecté à une base de données.")
            print("Utilisez '\\c db_name' pour vous connecter ou '\\create db_name' pour en créer une.")
            return False
        
        try:
            # Parser la requête
            parsed = self.parser.parse(query)
            
            # Exécuter la requête
            result = self.executor.execute(parsed)
            
            # Afficher le résultat
            if result:
                if isinstance(result, list):
                    # Résultat tabulaire
                    if result:
                        headers = list(result[0].keys())
                        # Calculer les largeurs de colonnes
                        col_widths = [len(h) for h in headers]
                        for row in result:
                            for i, h in enumerate(headers):
                                col_widths[i] = max(col_widths[i], len(str(row.get(h, ''))))
                        
                        # Afficher l'en-tête
                        header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
                        separator = "-+-".join("-" * w for w in col_widths)
                        print(f"\n{header_line}")
                        print(separator)
                        
                        # Afficher les lignes
                        for row in result:
                            line = " | ".join(str(row.get(h, '')).ljust(col_widths[i]) 
                                            for i, h in enumerate(headers))
                            print(line)
                        
                        print(f"\n({len(result)} ligne{'s' if len(result) > 1 else ''})\n")
                    else:
                        print("\n(Aucun résultat)\n")
                else:
                    # Résultat simple (CREATE, INSERT, etc.)
                    print(f"\n{result}\n")
            
            return True
            
        except SyntaxError as e:
            print(f"Erreur de syntaxe: {e}")
            return False
        except TableNotFoundError as e:
            print(f"Table non trouvée: {e}")
            return False
        except GSQLException as e:
            print(f"Erreur GSQL: {e}")
            return False
        except Exception as e:
            print(f"Erreur inattendue: {e}")
            return False
    
    def interactive_mode(self):
        """Lance le mode interactif"""
        self.print_banner()
        
        # Boucle principale interactive
        current_buffer = []
        
        while self.running:
            try:
                # Lire la ligne
                try:
                    line = input(self.prompt).strip()
                except EOFError:
                    print("\nAu revoir!")
                    break
                except KeyboardInterrupt:
                    print("\nUtilisez '\\quit' pour quitter.")
                    continue
                
                # Ignorer les lignes vides
                if not line:
                    continue
                
                # Sauvegarder dans l'historique
                self.command_history.append(line)
                
                # Ajouter à la buffer si multiligne
                current_buffer.append(line)
                buffer_text = ' '.join(current_buffer)
                
                # Vérifier si la commande est complète
                if buffer_text.startswith('\\') or buffer_text.endswith(';') or ';' in buffer_text:
                    # Nettoyer le point-virgule final
                    if buffer_text.endswith(';'):
                        buffer_text = buffer_text[:-1].strip()
                    
                    # Traiter la commande
                    if buffer_text.startswith('\\'):
                        self.handle_meta_command(buffer_text)
                    else:
                        self.execute_query(buffer_text)
                    
                    # Réinitialiser le buffer
                    current_buffer = []
                
            except Exception as e:
                print(f"Erreur: {e}")
                current_buffer = []
    
    def execute_file(self, file_path: str):
        """Exécute les commandes depuis un fichier"""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Diviser par les points-virgules
            queries = [q.strip() for q in content.split(';') if q.strip()]
            
            for query in queries:
                print(f"\n>>> {query}")
                if query.startswith('\\'):
                    self.handle_meta_command(query)
                else:
                    self.execute_query(query)
                    
        except FileNotFoundError:
            print(f"Fichier non trouvé: {file_path}")
        except Exception as e:
            print(f"Erreur lors de l'exécution du fichier: {e}")
    
    def run(self, args=None):
        """Point d'entrée principal"""
        parser = self.setup_argparse()
        
        if args is None:
            args = sys.argv[1:]
        
        # Si pas d'arguments, mode interactif par défaut
        if not args:
            self.interactive_mode()
            return
        
        parsed_args = parser.parse_args(args)
        
        # Traiter les options qui quittent immédiatement
        if parsed_args.version:
            self.print_version()
            return
        
        if parsed_args.license:
            print("GSQL - Licence MIT")
            print("Copyright (c) 2024 Gopu Inc.")
            return
        
        # Charger la configuration personnalisée si spécifiée
        if parsed_args.config:
            try:
                import yaml
                with open(parsed_args.config, 'r') as f:
                    custom_config = yaml.safe_load(f)
                    self.config.update(custom_config)
            except Exception as e:
                print(f"Erreur de chargement de la configuration: {e}")
        
        # Définir le répertoire de données
        if parsed_args.data_dir:
            if 'storage' not in self.config:
                self.config['storage'] = {}
            self.config['storage']['data_dir'] = parsed_args.data_dir
        
        # Initialiser une base de données si demandé
        if parsed_args.init_db:
            if self.init_database(parsed_args.init_db):
                print(f"Base de données '{parsed_args.init_db}' initialisée.")
            return
        
        # Gérer les arguments positionnels (compatibilité)
        db_to_connect = None
        if parsed_args.dbname:
            db_to_connect = parsed_args.dbname
        elif parsed_args.database:
            db_to_connect = parsed_args.database
        
        # Se connecter à une base de données si spécifiée
        if db_to_connect:
            if not self.connect_database(db_to_connect):
                return
        
        # Mode exécution de commande unique
        command_to_execute = None
        if parsed_args.execute:
            command_to_execute = parsed_args.execute
        elif parsed_args.command:
            command_to_execute = parsed_args.command
        
        if command_to_execute:
            if not self.database:
                print("Erreur: Non connecté à une base de données.")
                print("Utilisez: gsql -d dbname -e 'commande'")
                return
            
            if command_to_execute.startswith('\\'):
                self.handle_meta_command(command_to_execute)
            else:
                self.execute_query(command_to_execute)
            return
        
        # Mode fichier
        if parsed_args.file:
            if not self.database:
                print("Erreur: Non connecté à une base de données.")
                print("Utilisez: gsql -d dbname -f fichier.sql")
                return
            
            self.execute_file(parsed_args.file)
            return
        
        # Mode interactif
        self.interactive_mode()


def main():
    """Fonction principale"""
    cli = GSQLCLI()
    cli.run()


if __name__ == "__main__":
    main()
