#!/usr/bin/env python3
"""
FUNCTION.PY CORRIGÉ - Avec gestion des erreurs et nettoyage
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from gsql.database import Database
from gsql.exceptions import SQLExecutionError

print("✅ Import réussi")
print("🚀 Test GSQL avec gestion d'erreurs")

# 1. Créer une base FRÂICHE
db = Database(db_path=":memory:")

print("\n📊 STRUCTURE execute():")
result = db.execute("SELECT 1 as test, 'hello' as message")

print(f"Clés: {list(result.keys())}")
print(f"Type: {result['type']}")
print(f"Success: {result['success']}")
print(f"Colonnes: {result['columns']}")
print(f"Rows: {result['rows']}")

# 2. FONCTION DE CONVERSION UTILE
def rows_to_dicts(result):
    """Convertit les tuples rows en liste de dicts"""
    if not result.get('success') or not result.get('rows'):
        return []
    
    dicts = []
    for row_tuple in result['rows']:
        row_dict = {}
        for i, col_name in enumerate(result.get('columns', [])):
            row_dict[col_name] = row_tuple[i] if i < len(row_tuple) else None
        dicts.append(row_dict)
    
    return dicts

# 3. TEST AVEC GESTION D'ERREURS
print("\n🧪 TEST AVEC GESTION D'ERREURS:")

# Nettoyer d'abord
try:
    db.execute("DROP TABLE IF EXISTS test_table")
    print("✓ Ancienne table nettoyée")
except SQLExecutionError as e:
    print(f"Note: {e}")

# Créer table avec IF NOT EXISTS
try:
    create = db.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)")
    print(f"✓ Table créée: {create['success']}")
except SQLExecutionError as e:
    print(f"❌ Erreur CREATE: {e}")

# Insérer avec gestion d'erreurs
try:
    insert = db.execute("INSERT OR IGNORE INTO test_table (name) VALUES ('Alice'), ('Bob')")
    print(f"✓ Insertion: {insert.get('rows_affected', '?')} lignes")
except SQLExecutionError as e:
    print(f"❌ Erreur INSERT: {e}")

# SELECT avec conversion
try:
    select = db.execute("SELECT * FROM test_table ORDER BY name")
    
    if select['success']:
        print(f"\n📋 Résultats SELECT:")
        print(f"  Colonnes: {select['columns']}")
        print(f"  Nombre: {select['count']}")
        
        # Conversion en dicts
        data = rows_to_dicts(select)
        for item in data:
            print(f"  • ID: {item.get('id')}, Name: {item.get('name')}")
    else:
        print("❌ SELECT échouée")
        
except SQLExecutionError as e:
    print(f"❌ Erreur SELECT: {e}")

# 4. TEST DES DIFFÉRENTS TYPES DE REQUÊTES
print("\n🔧 TEST DES TYPES DE REQUÊTES:")

test_queries = [
    ("CREATE TABLE", "CREATE TABLE IF NOT EXISTS products (id INTEGER, name TEXT, price REAL)"),
    ("INSERT", "INSERT OR IGNORE INTO products VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99)"),
    ("SELECT simple", "SELECT * FROM products"),
    ("SELECT avec calcul", "SELECT name, price, price * 0.9 as discounted FROM products"),
    ("UPDATE", "UPDATE products SET price = price * 0.8 WHERE name = 'Mouse'"),
    ("DELETE", "DELETE FROM products WHERE price > 1000"),
    ("DROP", "DROP TABLE IF EXISTS products"),
]

for query_name, sql in test_queries:
    try:
        result = db.execute(sql)
        status = "✓" if result.get('success') else "✗"
        print(f"  {status} {query_name:20} -> {result.get('type', 'unknown')}")
        
        # Afficher des infos supplémentaires pour SELECT
        if result.get('type') == 'select' and 'rows' in result:
            print(f"      {len(result['rows'])} ligne(s)")
            
    except SQLExecutionError as e:
        print(f"  ✗ {query_name:20} -> ERREUR: {e}")
    except Exception as e:
        print(f"  ✗ {query_name:20} -> Exception: {type(e).__name__}")

# 5. TEST TRANSACTIONS
print("\n💼 TEST DES TRANSACTIONS:")

try:
    # Créer table pour transaction
    db.execute("CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance REAL)")
    db.execute("INSERT OR IGNORE INTO accounts VALUES (1, 1000.0), (2, 500.0)")
    
    print("✓ Table accounts créée")
    
    # Transaction simple
    db.execute("BEGIN TRANSACTION")
    db.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    db.execute("COMMIT")
    
    print("✓ Transaction réussie")
    
    # Vérifier
    result = db.execute("SELECT * FROM accounts ORDER BY id")
    accounts = rows_to_dicts(result)
    for acc in accounts:
        print(f"  Compte {acc['id']}: ${acc['balance']:.2f}")
        
except SQLExecutionError as e:
    print(f"❌ Transaction échouée: {e}")
    try:
        db.execute("ROLLBACK")
        print("✓ Rollback effectué")
    except:
        pass

# 6. FERMETURE PROPRE
db.close()
print("\n✅ Test terminé avec succès")
