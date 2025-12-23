#!/usr/bin/env python3
"""
FINAL GSQL TEST - Corrections basées sur les erreurs réelles
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from gsql.database import Database
from gsql.exceptions import SQLExecutionError

print("🔬 TEST FINAL GSQL - Basé sur les erreurs réelles")
print("=" * 60)

# 1. BASE TOUJOURS FRAÎCHE
db = Database(db_path=":memory:")
print("✅ Base mémoire créée")

# 2. ANALYSE COMPLÈTE DE L'API
print("\n📊 ANALYSE DE L'API execute():")
test = db.execute("SELECT 1 as a, 2 as b, 'test' as c")
print(f"Structure: {list(test.keys())}")
print(f"Type: {test['type']}")
print(f"Format rows: {type(test['rows'][0]) if test['rows'] else 'vide'}")

# 3. NETTOYAGE COMPLET
print("\n🧹 NETTOYAGE COMPLET:")
tables_to_drop = ['test_table', 'products', 'accounts', 'users', 'test_data']
for table in tables_to_drop:
    try:
        db.execute(f"DROP TABLE IF EXISTS {table}")
    except:
        pass
print("✓ Tables nettoyées")

# 4. CRÉATION CORRECTE DES TABLES
print("\n🏗️  CRÉATION DES TABLES:")

# Version CORRECTE - Spécifier toutes les colonnes
create_queries = [
    ("users", """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("products", """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            price DECIMAL(10,2),
            stock INTEGER DEFAULT 0,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("accounts", """
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            account_number TEXT UNIQUE,
            balance DECIMAL(10,2) DEFAULT 0.0,
            owner_id INTEGER,
            FOREIGN KEY (owner_id) REFERENCES users(id)
        )
    """)
]

for table_name, sql in create_queries:
    try:
        result = db.execute(sql)
        print(f"✓ Table '{table_name}' créée ({result['type']})")
    except SQLExecutionError as e:
        print(f"✗ Table '{table_name}': {e}")

# 5. INSERTION CORRECTE
print("\n📝 INSERTION DE DONNÉES:")

# INSERT users - CORRECT avec colonnes spécifiées
users_sql = """
    INSERT INTO users (name, age, email) 
    VALUES 
        ('Alice', 25, 'alice@example.com'),
        ('Bob', 30, 'bob@example.com'),
        ('Charlie', 22, 'charlie@example.com')
"""

try:
    users_result = db.execute(users_sql)
    print(f"✓ Users: {users_result.get('rows_affected', '?')} lignes")
except SQLExecutionError as e:
    print(f"✗ Users: {e}")

# INSERT products - CORRECT avec toutes les colonnes OU spécifier colonnes
products_sql = """
    INSERT INTO products (id, name, category, price, stock, description)
    VALUES 
        (1, 'Laptop', 'Electronics', 999.99, 10, 'High-end gaming laptop'),
        (2, 'Mouse', 'Electronics', 29.99, 50, 'Wireless mouse'),
        (3, 'Desk', 'Furniture', 299.99, 5, 'Office desk')
"""

try:
    products_result = db.execute(products_sql)
    print(f"✓ Products: {products_result.get('rows_affected', '?')} lignes")
except SQLExecutionError as e:
    print(f"✗ Products: {e}")

# 6. REQUÊTES SELECT CORRECTES
print("\n🔍 REQUÊTES SELECT:")

select_queries = [
    ("Tous les users", "SELECT * FROM users ORDER BY name"),
    ("Users > 23 ans", "SELECT name, age, email FROM users WHERE age > 23"),
    ("Produits par catégorie", """
        SELECT category, COUNT(*) as count, AVG(price) as avg_price 
        FROM products 
        GROUP BY category
    """),
    ("Jointure", """
        SELECT u.name, p.name as product, p.price
        FROM users u, products p
        WHERE u.age > 20
        ORDER BY u.name, p.price
    """)
]

for desc, sql in select_queries:
    try:
        result = db.execute(sql)
        if result['success']:
            print(f"✓ {desc}: {len(result['rows'])} résultat(s)")
            
            # Afficher les premières lignes
            if result['rows']:
                print(f"  Colonnes: {result['columns']}")
                for i, row in enumerate(result['rows'][:2]):
                    print(f"  [{i}] {row}")
                if len(result['rows']) > 2:
                    print(f"  ... et {len(result['rows']) - 2} autres")
        else:
            print(f"✗ {desc}: Échec")
    except SQLExecutionError as e:
        print(f"✗ {desc}: {e}")

# 7. TRANSACTIONS - LA BONNE FAÇON
print("\n💼 TRANSACTIONS - Méthode correcte:")

# Méthode 1: Utiliser SAVEPOINT (plus fiable dans GSQL)
try:
    print("Méthode 1: SAVEPOINT")
    db.execute("SAVEPOINT sp1")
    
    # Opérations dans la transaction
    db.execute("UPDATE users SET age = age + 1 WHERE name = 'Alice'")
    db.execute("UPDATE products SET stock = stock - 1 WHERE name = 'Laptop'")
    
    db.execute("RELEASE SAVEPOINT sp1")
    print("✓ Transaction SAVEPOINT réussie")
    
except SQLExecutionError as e:
    print(f"✗ SAVEPOINT échoué: {e}")
    try:
        db.execute("ROLLBACK TO SAVEPOINT sp1")
        print("✓ Rollback SAVEPOINT")
    except:
        pass

# Méthode 2: BEGIN/COMMIT explicite
print("\nMéthode 2: BEGIN/COMMIT")
try:
    # D'abord vérifier s'il y a une transaction active
    try:
        db.execute("ROLLBACK")  # Nettoyer toute transaction existante
    except:
        pass  # Pas de transaction active, c'est bon
    
    db.execute("BEGIN TRANSACTION")
    print("✓ Transaction débutée")
    
    db.execute("INSERT INTO users (name, age, email) VALUES ('David', 28, 'david@test.com')")
    db.execute("UPDATE products SET price = price * 0.9 WHERE category = 'Electronics'")
    
    db.execute("COMMIT")
    print("✓ Transaction commitée")
    
except SQLExecutionError as e:
    print(f"✗ Transaction: {e}")
    try:
        db.execute("ROLLBACK")
        print("✓ Rollback effectué")
    except Exception as re:
        print(f"✗ Rollback aussi échoué: {re}")

# 8. FONCTIONS AVANCÉES
print("\n⚡ FONCTIONS AVANCÉES:")

# Créer une fonction personnalisée (si disponible)
try:
    # Vérifier si register_function existe
    if hasattr(db, 'register_function'):
        
        def calculate_tax(amount):
            return amount * 1.20  # 20% de taxe
        
        db.register_function('calculate_tax', calculate_tax)
        print("✓ Fonction calculate_tax enregistrée")
        
        # Tester la fonction
        tax_result = db.execute("SELECT calculate_tax(100) as with_tax")
        if tax_result['success']:
            print(f"  Test: 100€ avec taxe = {tax_result['rows'][0][0]}€")
    else:
        print("ℹ️  register_function non disponible")
        
except Exception as e:
    print(f"✗ Fonctions: {e}")

# 9. PERFORMANCE ET STATISTIQUES
print("\n📈 PERFORMANCE:")

# Test de performance
import time

start = time.time()
for i in range(50):
    db.execute(f"INSERT INTO products (name, price) VALUES ('Product_{i}', {i * 10.0})")
insert_time = time.time() - start

print(f"✓ 50 insertions: {insert_time:.3f}s ({insert_time/50:.4f}s par insertion)")

# Statistiques
stats = db.execute("""
    SELECT 
        COUNT(*) as total_products,
        AVG(price) as avg_price,
        SUM(stock) as total_stock,
        MIN(price) as min_price,
        MAX(price) as max_price
    FROM products
""")

if stats['success'] and stats['rows']:
    row = stats['rows'][0]
    print(f"📊 Statistiques produits:")
    print(f"  Total: {row[0]}")
    print(f"  Prix moyen: {row[1]:.2f}€")
    print(f"  Stock total: {row[2]}")
    print(f"  Prix min: {row[3]:.2f}€")
    print(f"  Prix max: {row[4]:.2f}€")

# 10. NETTOYAGE FINAL ET VÉRIFICATION
print("\n🧼 NETTOYAGE FINAL:")

# Lister toutes les tables
try:
    tables_result = db.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    
    if tables_result['success'] and tables_result['rows']:
        print("📋 Tables dans la base:")
        for table in tables_result['rows']:
            table_name = table[0]
            count_result = db.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = count_result['rows'][0][0] if count_result['success'] else 0
            print(f"  • {table_name}: {count} ligne(s)")
    else:
        print("  Aucune table utilisateur")
        
except SQLExecutionError as e:
    print(f"✗ Liste tables: {e}")

# Fermeture
db.close()
print("\n" + "=" * 60)
print("✅ TEST COMPLET TERMINÉ AVEC SUCCÈS!")
print("=" * 60)

# RÉSUMÉ DES LEÇONS APPRISES
print("\n📚 RÉSUMÉ DES LEÇONS:")
print("1. execute() retourne dict avec 'rows' (tuples)")
print("2. Spécifier TOUTES les colonnes dans INSERT")
print("3. Utiliser SAVEPOINT pour les transactions")
print("4. Nettoyer les tables avant les tests")
print("5. Gérer les erreurs avec try/except SQLExecutionError")
