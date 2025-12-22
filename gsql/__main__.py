#!/usr/bin/env python3
"""
GSQL - Un système de gestion de base de données SQL simplifié
Point d'entrée principal pour l'interface en ligne de commande
"""

import sys
import os
import argparse
import shlex
from pathlib import Path

# Définir les constantes directement si __init__.py n'existe pas
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
        self.modules_loaded = False
        
    def load_modules(self):
        """Charge les modules GSQL dynamiquement"""
        if self.modules_loaded:
            return True
            
        try:
            # Charger les modules principaux
            from database import Database
            from parser import Parser
            from executor import Executor
            from exceptions import GSQLException
            
            self.Database = Database
            self.Parser = Parser
            self.Executor = Executor
            self.GSQLException = GSQLException
            
            # Essayer de charger les modules optionnels
            try:
                from stockage.yaml_storage import YAMLStorage
                self.YAMLStorage = YAMLStorage
                self.YAML_AVAILABLE = True
            except ImportError:
                self.YAML_AVAILABLE = False
                print("Warning: YAMLStorage non disponible")
                
            try:
                from functions.user_functions import register_user_functions
                self.register_user_functions = register_user_functions
                self.FUNCTIONS_AVAILABLE = True
            except ImportError:
                self.FUNCTIONS_AVAILABLE = False
                
            try:
                from nlp.translator import translate_natural_language
                self.translate_natural_language = translate_natural_language
                self.NLP_AVAILABLE = True
            except ImportError:
                self.NLP_AVAILABLE = False
                
            self.modules_loaded = True
            return True
            
        except ImportError as e:
            print(f"Erreur de chargement des modules: {e}")
            return False
        
    def load_config(self):
        """Charge la configuration"""
        # Configuration par défaut
        config = {
            'storage': {
                'data_dir': '~/.gsql',
                'format': 'yaml'
            },
            'executor': {
                'transaction_mode': 'auto',
                'timeout': 30
            },
            'nlp': {
                'enabled': False,
                'default_language': 'fr'
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
        except:
            pass
            
        return config
        
    def setup_argparse(self) -> argparse.ArgumentParser:
        """Configure le parser d'arguments de ligne de commande"""
        parser = argparse.ArgumentParser(
            description=__description__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            add_help=False,  # Désactiver l'aide automatique pour éviter le conflit
            epilog="""
Exemples d'utilisation:
  gsql                            # Mode interactif
  gsql -h                        # Afficher l'aide
  gsql -v                        # Afficher la version
  gsql -e "SELECT * FROM users"  # Exécuter une commande
  gsql -f query.sql              # Exécuter un fichier SQL
  gsql --init-db mydatabase      # Initialiser une nouvelle base
  gsql -d mydb                   # Se connecter à une base
            
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
        
        # Gestion de la base de données
        db_group = parser.add_argument_group('Gestion de base de données')
        db_group.add_argument(
            '-d', '--database',
            type=str,
            help='Nom de la base de données à utiliser'
        )
        db_group.add_argument(
            '--init-db',
            type=str,
            help='Initialiser une nouvelle base de données'
        )
        db_group.add_argument(
            '--data-dir',
            type=str,
            default='~/.gsql',
            help='Répertoire des données (défaut: ~/.gsql)'
        )
        
        # Informations
        info_group = parser.add_argument_group('Informations')
        info_group.add_argument(
            '-v', '--version',
            action='store_true',
            help='Afficher la version et quitter'
        )
        info_group.add_argument(
            '-h', '--help',
            action='store_true',
            help='Afficher ce message d\'aide'
        )
        
        return parser
    
    def print_banner(self):
        """Affiche la bannière d'accueil"""
        banner = f"""
╔══════════════════════════════════════════════════╗
║               GSQL v{__version__}                    ║
║    Système de Gestion de Base de Données        ║
║           Développé par {__author__}          ║
╚══════════════════════════════════════════════════╝
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
  
TABLES:
  \\d, \\dt            - Lister les tables
  \\d table_name      - Décrire une table
  
EXÉCUTION:
  ;                  - Terminer une commande (optionnel)
  
HISTORIQUE:
  \\history           - Afficher l'historique
  
AFFICHAGE:
  \\clear, \\cls       - Effacer l'écran
  
ADMINISTRATION:
  \\config            - Afficher la configuration
  \\stats             - Afficher les statistiques
  
SQL STANDARD:
  Toutes les commandes SQL standard sont supportées:
  - SELECT, INSERT, UPDATE, DELETE
  - CREATE, DROP, ALTER TABLE
  - CREATE INDEX, DROP INDEX
        """
        print(help_text)
    
    def print_version(self):
        """Affiche la version"""
        version_info = f"""
GSQL Version: {__version__}
Auteur: {__author__}
Description: {__description__}
        """
        print(version_info)
    
    def print_cli_help(self):
        """Affiche l'aide de la ligne de commande"""
        parser = self.setup_argparse()
        parser.print_help()
    
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
        if not self.load_modules():
            print("Erreur: Impossible de charger les modules GSQL")
            return False
            
        try:
            if self.database:
                self.database.close()
            
            data_dir = self.config.get('storage', {}).get('data_dir', '~/.gsql')
            db_path = Path(data_dir).expanduser() / db_name
            
            if not db_path.exists():
                print(f"Base de données '{db_name}' non trouvée.")
                print(f"Utilisez '\\create {db_name}' pour la créer.")
                return False
            
            if self.YAML_AVAILABLE:
                self.storage = self.YAMLStorage(str(db_path))
                self.database = self.Database(self.storage)
                self.parser = self.Parser()
                self.executor = self.Executor(self.database)
                self.current_db = db_name
                self.prompt = f"gsql[{db_name}]> "
                
                # Enregistrer les fonctions utilisateur si disponible
                if self.FUNCTIONS_AVAILABLE:
                    self.register_user_functions(self.executor)
                
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
                marker = " *" if db == self.current_db else ""
                print(f"  {db}{marker}")
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
            print("-" * 40)
            for table in tables:
                print(f"  {table}")
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
            print("-" * 40)
            for column in schema.get('columns', []):
                name = column.get('name', '')
                type_ = column.get('type', '')
                print(f"  {name}: {type_}")
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
        print("-" * 40)
        for i, cmd in enumerate(self.command_history[-10:], 1):
            print(f"{i:2}: {cmd}")
        print()
    
    def show_config(self):
        """Affiche la configuration"""
        print("\nConfiguration GSQL:")
        print("-" * 40)
        
        if self.current_db:
            print(f"Base courante: {self.current_db}")
        
        storage_config = self.config.get('storage', {})
        print(f"Répertoire: {storage_config.get('data_dir', '~/.gsql')}")
        print()
    
    def show_stats(self):
        """Affiche les statistiques"""
        if not self.database:
            print("Non connecté à une base de données.")
            return
        
        try:
            stats = self.database.get_stats()
            print("\nStatistiques:")
            print("-" * 40)
            print(f"Tables: {stats.get('table_count', 0)}")
            print(f"Lignes totales: {stats.get('total_rows', 0)}")
            print()
        except Exception as e:
            print(f"Erreur: {e}")
    
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
                        # Afficher l'en-tête
                        header_line = " | ".join(headers)
                        separator = "-+-".join(["-" * len(h) for h in headers])
                        print(f"\n{header_line}")
                        print(separator)
                        
                        # Afficher les lignes
                        for row in result:
                            line = " | ".join(str(row.get(h, '')) for h in headers)
                            print(line)
                        
                        print(f"\n({len(result)} ligne{'s' if len(result) > 1 else ''})\n")
                    else:
                        print("\n(Aucun résultat)\n")
                else:
                    # Résultat simple
                    print(f"\n{result}\n")
            
            return True
            
        except Exception as e:
            print(f"Erreur: {e}")
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
                if buffer_text.startswith('\\') or buffer_text.endswith(';'):
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
        if args is None:
            args = sys.argv[1:]
        
        # Si pas d'arguments, mode interactif par défaut
        if not args:
            self.interactive_mode()
            return
        
        # Vérifier les options simples d'abord
        if '-h' in args or '--help' in args:
            self.print_cli_help()
            return
        
        if '-v' in args or '--version' in args:
            self.print_version()
            return
        
        # Parser les arguments
        parser = self.setup_argparse()
        
        try:
            parsed_args = parser.parse_args(args)
        except SystemExit:
            return  # Erreur de parsing
        
        # Initialiser une base de données
        if parsed_args.init_db:
            if self.init_database(parsed_args.init_db):
                print(f"Base de données '{parsed_args.init_db}' initialisée.")
            return
        
        # Se connecter à une base si spécifiée
        if parsed_args.database:
            if not self.connect_database(parsed_args.database):
                return
        
        # Exécuter une commande unique
        if parsed_args.execute:
            if not self.database:
                print("Erreur: Non connecté à une base de données.")
                print("Utilisez: gsql -d dbname -e 'commande'")
                return
            
            if parsed_args.execute.startswith('\\'):
                self.handle_meta_command(parsed_args.execute)
            else:
                self.execute_query(parsed_args.execute)
            return
        
        # Exécuter un fichier
        if parsed_args.file:
            if not self.database:
                print("Erreur: Non connecté à une base de données.")
                print("Utilisez: gsql -d dbname -f fichier.sql")
                return
            
            self.execute_file(parsed_args.file)
            return
        
        # Mode interactif par défaut
        self.interactive_mode()


def main():
    """Fonction principale"""
    cli = GSQLCLI()
    cli.run()


if __name__ == "__main__":
    main()
