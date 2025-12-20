# cli.py
#!/usr/bin/env python3
"""
CLI Interactive pour GSQL
"""

import sys
import os
import cmd
import json
from typing import List, Dict, Any
from pathlib import Path
from gsql import GSQL

class GSQLCLI(cmd.Cmd):
    """Interface en ligne de commande pour GSQL"""
    
    intro = """
    ╔══════════════════════════════════════╗
    ║      GSQL Database v0.1.0            ║
    ║      Simple mais Puissant            ║
    ╚══════════════════════════════════════╝
    
    Tapez 'help' pour l'aide, 'exit' pour quitter.
    """
    
    prompt = "gsql> "
    
    def __init__(self, db_path=None):
        super().__init__()
        self.db_path = db_path
        self.db = None
        self._connect()
    
    def _connect(self):
        """Connecter à la base de données"""
        try:
            self.db = GSQL(self.db_path)
            print(f"✓ Connecté à {self.db_path or 'gsql.db'}")
            print(f"✓ {len(self.db.tables)} tables chargées")
        except Exception as e:
            print(f"✗ Erreur de connexion: {e}")
            sys.exit(1)
    
    def default(self, line: str):
        """Traiter les commandes SQL"""
        if line.strip().lower() in ['quit', 'exit']:
            return self.do_exit('')
        
        try:
            result = self.db.execute(line)
            self._print_result(result)
        except Exception as e:
            print(f"✗ Erreur: {e}")
    
    def do_tables(self, arg):
        """Lister toutes les tables"""
        if not self.db.tables:
            print("Aucune table dans la base de données")
            return
        
        print("\nTables:")
        print("-" * 50)
        for table_name, metadata in self.db.tables.items():
            col_count = len(metadata.get('columns', []))
            row_count = metadata.get('row_count', 0)
            print(f"• {table_name} ({col_count} colonnes, {row_count} lignes)")
        print()
    
    def do_describe(self, table_name):
        """Décrire la structure d'une table"""
        if not table_name:
            print("Usage: describe <table_name>")
            return
        
        if table_name not in self.db.tables:
            print(f"Table '{table_name}' n'existe pas")
            return
        
        table_meta = self.db.tables[table_name]
        print(f"\nStructure de '{table_name}':")
        print("-" * 60)
        
        for col in table_meta.get('columns', []):
            col_name = col.get('name', '?')
            col_type = col.get('type', '?')
            constraints = col.get('constraints', [])
            
            constr_str = ", ".join(constraints) if constraints else ""
            print(f"  {col_name:20} {col_type:15} {constr_str}")
        print()
    
    def do_export(self, args):
        """Exporter la base en JSON: export <fichier.json>"""
        if not args:
            print("Usage: export <fichier.json>")
            return
        
        try:
            self.db.export_json(args.strip())
            print(f"✓ Base exportée vers {args}")
        except Exception as e:
            print(f"✗ Erreur: {e}")
    
    def do_import(self, args):
        """Importer depuis JSON: import <fichier.json>"""
        if not args:
            print("Usage: import <fichier.json>")
            return
        
        try:
            self.db.import_json(args.strip())
            print(f"✓ Base importée depuis {args}")
        except Exception as e:
            print(f"✗ Erreur: {e}")
    
    def do_shell(self, line):
        """Exécuter une commande shell"""
        os.system(line)
    
    def do_exit(self, arg):
        """Quitter GSQL"""
        print("\nAu revoir ! 👋")
        self.db.close()
        return True
    
    def _print_result(self, result: Dict[str, Any]):
        """Afficher le résultat d'une requête"""
        result_type = result.get('type')
        
        if result_type == 'SELECT':
            data = result.get('data', [])
            if not data:
                print("0 lignes")
                return
            
            # Afficher en tableau
            headers = list(data[0].keys())
            
            # Calculer les largeurs de colonnes
            col_widths = []
            for header
