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
            for header in headers:
                max_len = len(str(header))
                for row in data:
                    max_len = max(max_len, len(str(row.get(header, ''))))
                col_widths.append(max_len + 2)
            
            # Ligne de séparation
            separator = "+" + "+".join(["-" * w for w in col_widths]) + "+"
            
            print(separator)
            
            # En-têtes
            header_row = "|"
            for i, header in enumerate(headers):
                header_row += f" {header:<{col_widths[i]-1}}|"
            print(header_row)
            
            print(separator)
            
            # Données
            for row in data:
                row_str = "|"
                for i, header in enumerate(headers):
                    value = str(row.get(header, ''))
                    row_str += f" {value:<{col_widths[i]-1}}|"
                print(row_str)
            
            print(separator)
            print(f"{len(data)} ligne(s)")
            
        elif result_type == 'INSERT':
            rows = result.get('rows_affected', 0)
            print(f"✓ {rows} ligne(s) insérée(s)")
            
        elif result_type == 'CREATE_TABLE':
            table = result.get('table', '')
            print(f"✓ Table '{table}' créée")
            
        elif result_type == 'DELETE':
            rows = result.get('rows_affected', 0)
            print(f"✓ {rows} ligne(s) supprimée(s)")
            
        elif result_type == 'UPDATE':
            rows = result.get('rows_affected', 0)
            print(f"✓ {rows} ligne(s) mise(s) à jour")


def main():
    """Point d'entrée principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='GSQL Database CLI')
    parser.add_argument('database', nargs='?', help='Fichier de base de données')
    parser.add_argument('-e', '--execute', help='Exécuter une commande SQL')
    
    args = parser.parse_args()
    
    if args.execute:
        # Mode non-interactif
        db = GSQL(args.database)
        result = db.execute(args.execute)
        
        if isinstance(result.get('data'), list):
            # Format JSON pour parsing facile
            print(json.dumps(result, indent=2))
        else:
            print(result)
        db.close()
    else:
        # Mode interactif
        cli = GSQLCLI(args.database)
        cli.cmdloop()


if __name__ == "__main__":
    main()
