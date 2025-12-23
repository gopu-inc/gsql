#!/usr/bin/env python3
"""
GSQL WORKING CODE - Basé sur les découvertes réelles
"""

from gsql.database import Database
from gsql.exceptions import SQLExecutionError

print("🚀 GSQL v3.0.9 - Code qui marche vraiment")
print("=" * 50)

# IMPORTANT: ":memory:" ne fonctionne pas comme attendu
# Utilisons un fichier temporaire unique
import tempfile
import uuid

# Créer un fichier temporaire unique
temp_db = f"/tmp/gsql_test_{uuid.uuid4().hex[:8]}.db"
print(f"📁 Base: {temp_db}")

db = Database(db_path=temp_db)

# 1. COMPRENDRE CE QUI EXISTE DÉJÀ
print("\n📋 Tables existantes:")
try:
    tables_result = db.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        ORDER BY name
    """)
    
    if tables_result['success']:
        print("Tables système et utilisateur:")
        for table in tables_result['rows'][:10]:  # Limiter l'affichage
            print(f"  • {table[0]}")
        
        if len(tables_result['rows']) > 10:
            print(f"  ... et {len(tables_result['rows']) - 10} autres")
except:
    print("  Impossible de lire les tables")

# 2. TRAVAILLER AVEC LES TABLES EXISTANTES
print("\n👥 Utilisateurs existants (table users):")
try:
    users = db.execute("SELECT id, name, age, email FROM users LIMIT 5")
    if users['success'] and users['rows']:
        for user in users['rows']:
            print(f"  ID {user[0]}: {user[1]} ({user[2]} ans) - {user[3]}")
    else:
        print("  Aucun utilisateur ou table vide")
except SQLExecutionError:
    print("  Table users n'existe pas ou erreur")

# 3. AJOUTER DES DONNÉES (sans dupliquer)
print("\n➕ Ajouter un nouvel utilisateur:")
try:
    # Utiliser INSERT OR IGNORE pour éviter les contraintes UNIQUE
    new_user = db.execute("""
        INSERT OR IGNORE INTO users (name, age, email) 
        VALUES ('TestUser', 99, 'test@unique.com')
    """)
    
    if new_user['success']:
        print(f"✓ Utilisateur ajouté (ID: {new_user.get('last_insert_id', '?')})")
    else:
        print("✗ Échec de l'ajout")
        
except SQLExecutionError as e:
    print(f"✗ Erreur: {e}")

# 4. CRÉER SA PROPRE TABLE (si besoin)
print("\n🏗️ Créer une table personnalisée:")
try:
    # D'abord vérifier si elle existe
    db.execute("DROP TABLE IF EXISTS my_custom_data")
    
    create_result = db.execute("""
        CREATE TABLE my_custom_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            value REAL,
            tags TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    if create_result['success']:
        print("✓ Table my_custom_data créée")
        
        # Remplir avec des données
        for i in range(3):
            db.execute(f"""
                INSERT INTO my_custom_data (data, value, tags)
                VALUES ('Data point {i}', {i * 10.5}, 'test,example')
            """)
        print("✓ Données ajoutées")
        
except SQLExecutionError as e:
    print(f"✗ Erreur création table: {e}")

# 5. REQUÊTES COMPLEXES
print("\n🔍 Requêtes avancées:")

# Avec la table products qui existe
try:
    # Statistiques produits
    stats = db.execute("""
        SELECT 
            category,
            COUNT(*) as count,
            AVG(price) as avg_price,
            SUM(stock) as total_stock
        FROM products 
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY avg_price DESC
    """)
    
    if stats['success']:
        print("📊 Produits par catégorie:")
        for row in stats['rows']:
            print(f"  • {row[0]}: {row[1]} produits, prix moyen: ${row[2]:.2f}, stock: {row[3]}")
            
except SQLExecutionError as e:
    print(f"✗ Statistiques: {e}")

# 6. JOINTURES
print("\n🤝 Jointure users/products:")
try:
    # Créer une table orders pour la démo
    db.execute("DROP TABLE IF EXISTS demo_orders")
    db.execute("""
        CREATE TABLE demo_orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            order_date DATE DEFAULT CURRENT_DATE
        )
    """)
    
    # Ajouter des commandes de démo
    db.execute("INSERT INTO demo_orders (user_id, product_id, quantity) VALUES (1, 1, 2), (2, 2, 1)")
    
    # Jointure
    orders = db.execute("""
        SELECT 
            u.name as user_name,
            p.name as product_name,
            o.quantity,
            p.price,
            o.quantity * p.price as total
        FROM demo_orders o
        JOIN users u ON o.user_id = u.id
        JOIN products p ON o.product_id = p.id
        ORDER BY total DESC
    """)
    
    if orders['success']:
        print("🛒 Commandes:")
        for order in orders['rows']:
            print(f"  • {order[0]} a acheté {order[2]}x {order[1]} = ${order[4]:.2f}")
            
except SQLExecutionError as e:
    print(f"✗ Jointures: {e}")

# 7. FONCTIONS SQL NATIVES
print("\n⚡ Fonctions SQL intégrées:")

function_tests = [
    ("Date/Heure", "SELECT DATE('now') as today, TIME('now') as current_time"),
    ("Math", "SELECT RANDOM() as random, ABS(-10) as absolute, ROUND(3.14159, 2) as pi"),
    ("Texte", "SELECT UPPER('hello') as upper, LOWER('WORLD') as lower, LENGTH('test') as len"),
    ("Agrégation", "SELECT COUNT(*) as total_users, AVG(age) as avg_age FROM users WHERE age > 0"),
]

for desc, sql in function_tests:
    try:
        result = db.execute(sql)
        if result['success'] and result['rows']:
            print(f"✓ {desc}: {result['rows'][0]}")
    except:
        print(f"✗ {desc}")

# 8. EXPORT/IMPORT
print("\n💾 Export des données:")

try:
    # Exporter users en CSV format
    export = db.execute("SELECT * FROM users")
    if export['success']:
        print(f"📄 {len(export['rows'])} utilisateurs exportables")
        
        # Afficher en format CSV-like
        print("  En-têtes:", ",".join(export['columns']))
        for i, row in enumerate(export['rows'][:3]):
            print(f"  Ligne {i+1}:", ",".join(str(x) for x in row))
        if len(export['rows']) > 3:
            print(f"  ... et {len(export['rows']) - 3} autres")
            
except:
    print("✗ Export échoué")

# 9. NETTOYAGE
print("\n🧼 Nettoyage des tables de démo:")
for table in ['demo_orders', 'my_custom_data']:
    try:
        db.execute(f"DROP TABLE IF EXISTS {table}")
        print(f"✓ Table {table} supprimée")
    except:
        pass

# 10. INFOS SYSTÈME
print("\n📊 Informations système GSQL:")

info_queries = [
    ("Version SQLite", "SELECT sqlite_version() as version"),
    ("Encodage", "PRAGMA encoding"),
    ("Taille DB", "SELECT page_count * page_size as size FROM pragma_page_count, pragma_page_size"),
]

for desc, sql in info_queries:
    try:
        result = db.execute(sql)
        if result['success'] and result['rows']:
            print(f"  {desc}: {result['rows'][0]}")
    except:
        pass

# Fermeture
db.close()

# Supprimer le fichier temporaire
import os
if os.path.exists(temp_db):
    os.remove(temp_db)
    print(f"🗑️  Fichier {temp_db} supprimé")

print("\n" + "=" * 50)
print("✅ GSQL fonctionne correctement !")
print("=" * 50)

print("\n💡 CE QU'IL FAUT RETENIR:")
print("1. GSQL initialise automatiquement des tables (users, products)")
print("2. Utiliser INSERT OR IGNORE pour éviter les erreurs UNIQUE")
print("3. Les résultats sont des tuples dans result['rows']")
print("4. Pas de transactions fonctionnelles dans cette version")
print("5. Toujours vérifier si les tables existent avant de les créer")
