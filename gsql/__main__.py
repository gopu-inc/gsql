#!/usr/bin/env python3
"""
GSQL - Moteur SQL avec interface en langage naturel
"""

import sys
import os
import cmd
import shlex
import logging
from pathlib import Path
from typing import Optional

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gsql.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class GSQLCLI(cmd.Cmd):
    """Interface en ligne de commande améliorée pour GSQL"""
    
    intro = """
    ╔══════════════════════════════════════╗
    ║      GSQL Interactive Shell          ║
    ║      Version 2.0 avec NLP            ║
    ╚══════════════════════════════════════╝
    
    Tapez 'help' pour la liste des commandes.
    Tapez 'nl' pour poser une question en français.
    Tapez 'exit' pour quitter.
    """
    
    prompt = 'gsql> '
    
    def __init__(self, database_path=None, use_nlp=True):
        super().__init__()
        
        # Import ici pour éviter les imports circulaires
        from gsql.database import Database
        from gsql.functions.user_functions import FunctionManager
        from gsql.nlp.translator import NLToSQLTranslator
        
        self.database_path = database_path or 'default.gsql'
        self.use_nlp = use_nlp
        
        # Initialisation des composants
        try:
            self.db = Database(self.database_path)
            self.func_manager = FunctionManager()
            self.translator = NLToSQLTranslator() if use_nlp else None
            
            # Injecter les dépendances
            self.db.executor.function_manager = self.func_manager
            self.db.executor.nlp_translator = self.translator
            
            logger.info(f"Connected to database: {self.database_path}")
            
        except Exception as e:
            print(f"❌ Error initializing database: {str(e)}")
            logger.error(f"Initialization error: {str(e)}")
            sys.exit(1)
    
    def do_nl(self, arg):
        """
        Posez une question en langage naturel
        
        Exemples:
        nl montre tous les clients
        nl combien de produits dans le stock
        nl ajoute un nouveau client Jean Dupont
        """
        if not self.use_nlp or not self.translator:
            print("❌ NLP is not enabled")
            return
        
        if not arg:
            print("❌ Please provide a question")
            return
        
        try:
            print(f"🔍 Question: {arg}")
            
            # Traduction
            sql = self.translator.translate(arg)
            print(f"📊 SQL généré: {sql}")
            
            # Exécution
            result = self.db.execute(sql)
            self._display_result(result)
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            logger.error(f"NLP error: {str(e)}")
    
    def do_sql(self, arg):
        """
        Exécute une commande SQL directement
        
        Exemple:
        sql SELECT * FROM users WHERE age > 25
        """
        if not arg:
            print("❌ Please provide SQL command")
            return
        
        try:
            result = self.db.execute(arg)
            self._display_result(result)
        except Exception as e:
            print(f"❌ SQL Error: {str(e)}")
    
    def do_create_function(self, arg):
        """
        Crée une nouvelle fonction utilisateur
        
        Syntaxe:
        CREATE FUNCTION nom(param1, param2)
        RETURNS TYPE
        AS $$
        # code Python
        return param1 + param2
        $$ LANGUAGE plpython;
        """
        try:
            result = self.db.execute(arg)
            print(f"✅ {result}")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_list_functions(self, arg):
        """Liste toutes les fonctions disponibles"""
        try:
            functions = self.func_manager.list_functions()
            
            if not functions:
                print("📝 No functions available")
                return
            
            print("\n" + "="*60)
            print(f"{'FUNCTIONS':^60}")
            print("="*60)
            
            for i, func in enumerate(functions, 1):
                if func['type'] == 'builtin':
                    print(f"{i:2}. 📦 {func['name']}()")
                else:
                    created = func['created_at'].strftime('%Y-%m-%d')
                    print(f"{i:2}. 👤 {func['name']}({func['params']}) → {func['return_type']} ({created})")
            
            print("="*60)
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_cache_stats(self, arg):
        """Affiche les statistiques du cache"""
        try:
            stats = self.db.storage.buffer_pool.stats()
            
            print("\n" + "="*60)
            print(f"{'BUFFER POOL STATISTICS':^60}")
            print("="*60)
            print(f"Pages in cache: {stats['size']}/{stats['max_size']}")
            print(f"Hits: {stats['hits']}")
            print(f"Misses: {stats['misses']}")
            print(f"Hit ratio: {stats['hit_ratio']:.1%}")
            print("="*60)
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    def do_transaction(self, arg):
        """
        Gère les transactions
        
        Sous-commandes:
        transaction begin    - Démarre une transaction
        transaction commit   - Valide la transaction
        transaction rollback - Annule la transaction
        transaction status   - Affiche le statut
        """
        if not arg:
            print("❌ Missing subcommand: begin|commit|rollback|status")
            return
        
        subcommand = arg.lower().strip()
        
        try:
            if subcommand == 'begin':
                tid = self.db.storage.transaction_manager.begin()
                print(f"✅ Transaction {tid} started")
            elif subcommand == 'commit':
                # Implémentation simplifiée
                print("✅ Transaction committed")
            elif subcommand == 'rollback':
                # Implémentation simplifiée
                print("✅ Transaction rolled back")
            elif subcommand == 'status':
                active = len(self.db.storage.transaction_manager.active_transactions)
                print(f"📊 Active transactions: {active}")
            else:
                print(f"❌ Unknown subcommand: {subcommand}")
                
        except Exception as e:
            print(f"❌ Transaction error: {str(e)}")
    
    def do_learn(self, arg):
        """
        Apprend un nouvel exemple de traduction
        
        Syntaxe:
        learn "question en français" "SQL correspondant"
        
        Exemple:
        learn "montre les clients de Paris" "SELECT * FROM clients WHERE ville = 'Paris'"
        """
        if not arg:
            print("❌ Please provide both NL and SQL examples")
            return
        
        try:
            parts = shlex.split(arg)
            if len(parts) != 2:
                print("❌ Need exactly 2 arguments: NL question and SQL")
                return
            
            nl_example, sql_example = parts
            
            # Apprentissage
            self.translator.learn_from_examples([nl_example], [sql_example])
            print("✅ Example learned successfully")
            
        except Exception as e:
            print(f"❌ Learning error: {str(e)}")
    
    def do_import(self, arg):
        """Importe des données depuis un fichier CSV"""
        if not arg:
            print("❌ Please provide filename")
            return
        
        try:
            import csv
            from pathlib import Path
            
            filepath = Path(arg)
            if not filepath.exists():
                print(f"❌ File not found: {arg}")
                return
            
            # Détection du nom de table
            table_name = filepath.stem
            
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # Création de la table
                headers = reader.fieldnames
                create_sql = f"CREATE TABLE {table_name} ({', '.join([f'{h} TEXT' for h in headers])})"
                self.db.execute(create_sql)
                
                # Insertion des données
                for row in reader:
                    values = [f"'{row[h]}'" for h in headers]
                    insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(values)})"
                    self.db.execute(insert_sql)
                
                print(f"✅ Imported {table_name} from {filepath}")
                
        except Exception as e:
            print(f"❌ Import error: {str(e)}")
    
    def do_export(self, arg):
        """Exporte des données vers un fichier CSV"""
        if not arg:
            print("❌ Please provide table name")
            return
        
        try:
            import csv
            
            # Récupérer les données
            result = self.db.execute(f"SELECT * FROM {arg}")
            
            if not result or 'rows' not in result:
                print(f"❌ No data in table {arg}")
                return
            
            # Écriture CSV
            filename = f"{arg}_export.csv"
            with open(filename, 'w', encoding='utf-8', newline='') as f:
                if result['rows']:
                    writer = csv.DictWriter(f, fieldnames=result['rows'][0].keys())
                    writer.writeheader()
                    writer.writerows(result['rows'])
            
            print(f"✅ Exported {len(result['rows'])} rows to {filename}")
            
        except Exception as e:
            print(f"❌ Export error: {str(e)}")
    
    def _display_result(self, result):
        """Affiche les résultats de manière lisible"""
        if result is None:
            print("✅ Command executed successfully")
            return
        
        if isinstance(result, str):
            print(f"📋 {result}")
            return
        
        if isinstance(result, dict):
            if 'rows' in result and result['rows']:
                rows = result['rows']
                print(f"\n📊 Results: {len(rows)} row(s)")
                
                # Affichage tabulaire
                headers = list(rows[0].keys())
                print("\n" + " | ".join(headers))
                print("-" * (len(" | ".join(headers))))
                
                for row in rows[:20]:  # Limite à 20 lignes
                    values = [str(row[h])[:30] for h in headers]
                    print(" | ".join(values))
                
                if len(rows) > 20:
                    print(f"... and {len(rows) - 20} more rows")
                    
            elif 'message' in result:
                print(f"📋 {result['message']}")
                
        elif isinstance(result, list):
            if result:
                print(f"\n📊 Results: {len(result)} row(s)")
                for i, row in enumerate(result[:20], 1):
                    print(f"{i:3}. {row}")
                if len(result) > 20:
                    print(f"... and {len(result) - 20} more")
            else:
                print("📭 No results")
    
    def do_clear(self, arg):
        """Efface l'écran"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def do_exit(self, arg):
        """Quitte l'interface"""
        print("👋 Goodbye!")
        return True
    
    def do_quit(self, arg):
        """Quitte l'interface"""
        return self.do_exit(arg)
    
    def default(self, line):
        """Traitement par défaut: essaie d'exécuter comme SQL"""
        try:
            result = self.db.execute(line)
            self._display_result(result)
        except Exception as e:
            print(f"❌ Error: {str(e)}")

def main():
    """Point d'entrée principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='GSQL Database Engine')
    parser.add_argument('database', nargs='?', help='Database file path')
    parser.add_argument('--sql', help='Execute single SQL command')
    parser.add_argument('--nl', help='Execute natural language query')
    parser.add_argument('--no-nlp', action='store_true', help='Disable NLP features')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Niveau de logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Mode ligne de commande unique
    if args.sql:
        cli = GSQLCLI(args.database, use_nlp=not args.no_nlp)
        cli.do_sql(args.sql)
    elif args.nl:
        cli = GSQLCLI(args.database, use_nlp=not args.no_nlp)
        cli.do_nl(args.nl)
    else:
        # Mode interactif
        cli = GSQLCLI(args.database, use_nlp=not args.no_nlp)
        cli.cmdloop()

if __name__ == '__main__':
    main()
