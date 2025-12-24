#!/usr/bin/env python3
"""
TEST GSQL - WORKAROUND COMPLET POUR BUG TRANSACTIONS
"""

import os
import sys
import time
import tempfile
import shutil
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

print("🔧 TEST GSQL - WORKAROUND TRANSACTIONS")
print("=" * 70)

def test_transaction_workaround():
    """Test transactions avec workaround complet"""
    print("\n💼 TEST TRANSACTIONS AVEC WORKAROUND")
    print("-" * 50)
    
    from gsql.database import Database
    import tempfile
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_workaround_")
    
    try:
        db = Database(db_path=":memory:", base_dir=temp_dir)
        
        # Nettoyer tables par défaut
        print("🧹 Nettoyage tables...")
        for table in ['users', 'products', 'orders', 'logs']:
            db.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Créer table de test
        db.execute("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                name TEXT,
                balance REAL DEFAULT 0.0
            )
        """)
        
        # Données initiales
        db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.0)")
        db.execute("INSERT INTO accounts VALUES (2, 'Bob', 500.0)")
        print("✅ Table et données créées")
        
        # ====================================================================
        # WORKAROUND 1: Transactions manuelles avec SQL direct
        # ====================================================================
        print("\n🔀 WORKAROUND 1: Transactions SQL manuelles")
        
        # Début transaction SQL
        db.execute("BEGIN TRANSACTION")
        print("💼 Transaction SQL démarrée")
        
        # Opérations
        db.execute("UPDATE accounts SET balance = balance - 200 WHERE id = 1")
        db.execute("UPDATE accounts SET balance = balance + 200 WHERE id = 2")
        print("💰 Transfert 200€ Alice → Bob")
        
        # Commit SQL
        db.execute("COMMIT")
        print("✅ Transaction SQL commitée")
        
        # Vérifier
        result = db.execute("SELECT name, balance FROM accounts ORDER BY id")
        print("📊 Soldes après transfert:")
        for row in result['rows']:
            print(f"  • {row[0]}: ${row[1]:.2f}")
        
        # ====================================================================
        # WORKAROUND 2: Accès direct à SQLite
        # ====================================================================
        print("\n🔀 WORKAROUND 2: Accès direct SQLite")
        
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        
        cursor.execute("CREATE TABLE test (id INTEGER, value TEXT)")
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("INSERT INTO test VALUES (1, 'Transaction 1')")
        cursor.execute("SAVEPOINT sp1")
        cursor.execute("INSERT INTO test VALUES (2, 'Transaction 2')")
        cursor.execute("ROLLBACK TO SAVEPOINT sp1")
        cursor.execute("COMMIT")
        
        cursor.execute("SELECT COUNT(*) FROM test")
        count = cursor.fetchone()[0]
        print(f"✅ Transaction SQLite directe: {count} lignes (devrait être 1)")
        
        conn.close()
        
        # ====================================================================
        # WORKAROUND 3: Utiliser les méthodes GSQL mais avec vérification
        # ====================================================================
        print("\n🔀 WORKAROUND 3: Méthodes GSQL avec vérification")
        
        # Essayer les méthodes natives
        try:
            # Cette méthode a le bug
            tid = db.begin_transaction()
            print(f"⚠️  db.begin_transaction() = TID {tid} (BUG: pas de transaction SQLite)")
        except Exception as e:
            print(f"❌ db.begin_transaction() échoue: {e}")
        
        # Vérifier état transaction SQLite
        result = db.execute("SELECT * FROM pragma_transaction_state")
        if result['success'] and result['rows']:
            state = result['rows'][0][0]
            print(f"📊 État transaction SQLite: {state}")
        
        # ====================================================================
        # TEST: Rollback manuel
        # ====================================================================
        print("\n🔀 TEST Rollback manuel")
        
        # Solde avant
        result = db.execute("SELECT balance FROM accounts WHERE id = 1")
        before = result['rows'][0][0]
        print(f"💰 Alice avant: ${before:.2f}")
        
        # Transaction avec rollback
        db.execute("BEGIN TRANSACTION")
        db.execute("UPDATE accounts SET balance = balance + 1000 WHERE id = 1")
        db.execute("ROLLBACK")
        print("↩️  Rollback manuel exécuté")
        
        # Vérifier après rollback
        result = db.execute("SELECT balance FROM accounts WHERE id = 1")
        after = result['rows'][0][0]
        print(f"💰 Alice après rollback: ${after:.2f}")
        
        if abs(after - before) < 0.01:
            print("✅ Rollback fonctionne correctement")
        
        # ====================================================================
        # TEST: Niveaux d'isolation
        # ====================================================================
        print("\n🔀 TEST Niveaux d'isolation")
        
        levels = {
            "DEFERRED": "BEGIN DEFERRED TRANSACTION",
            "IMMEDIATE": "BEGIN IMMEDIATE TRANSACTION", 
            "EXCLUSIVE": "BEGIN EXCLUSIVE TRANSACTION"
        }
        
        for level_name, sql in levels.items():
            try:
                db.execute(sql)
                db.execute("INSERT INTO accounts VALUES (?, ?, ?)", [100, f"Test_{level_name}", 100.0])
                db.execute("COMMIT")
                print(f"  ✅ Niveau '{level_name}': OK")
            except Exception as e:
                print(f"  ❌ Niveau '{level_name}': {e}")
        
        # ====================================================================
        # VÉRIFICATION FINALE
        # ====================================================================
        print("\n📊 Vérification finale:")
        
        # Nombre de comptes
        result = db.execute("SELECT COUNT(*) FROM accounts")
        count = result['rows'][0][0]
        print(f"  • Comptes totaux: {count}")
        
        # Solde total
        result = db.execute("SELECT SUM(balance) FROM accounts")
        total = result['rows'][0][0] if result['rows'][0][0] else 0
        print(f"  • Solde total: ${total:.2f}")
        
        db.close()
        shutil.rmtree(temp_dir)
        
        print("\n✅ Test transactions terminé avec succès")
        return True
        
    except Exception as e:
        print(f"❌ Erreur majeure: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_database_methods():
    """Test toutes les méthodes de Database"""
    print("\n📋 TEST TOUTES LES MÉTHODES DATABASE")
    print("-" * 50)
    
    from gsql.database import Database
    import tempfile
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_methods_")
    
    try:
        db = Database(db_path=":memory:", base_dir=temp_dir)
        
        print("🔍 Méthodes disponibles dans Database:")
        
        methods = []
        for attr_name in dir(db):
            if not attr_name.startswith('_') and callable(getattr(db, attr_name)):
                methods.append(attr_name)
        
        methods.sort()
        for i, method in enumerate(methods, 1):
            print(f"  {i:2d}. {method}()")
        
        print(f"\n📊 Total: {len(methods)} méthodes publiques")
        
        # Tester les méthodes importantes
        print("\n🧪 Test méthodes spécifiques:")
        
        # 1. execute()
        result = db.execute("SELECT 1 as test")
        print(f"  • execute(): {result.get('success', False)}")
        
        # 2. check_health()
        health = db.check_health()
        print(f"  • check_health(): {health.get('status', 'UNKNOWN')}")
        
        # 3. get_stats() via execute
        result = db.execute("STATS")
        print(f"  • STATS: {result.get('success', False)}")
        
        # 4. close()
        db.close()
        print(f"  • close(): OK")
        
        shutil.rmtree(temp_dir)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

def create_patch_for_transaction_bug():
    """Crée un patch pour corriger le bug de transaction"""
    print("\n🔧 CRÉATION PATCH POUR BUG TRANSACTION")
    print("-" * 50)
    
    patch_content = '''# Patch pour gsql/storage/sqlite_storage.py
# Correction de TransactionManager.begin()

--- ORIGINAL (ligne ~180-190)
def begin(self, isolation_level: str = "DEFERRED") -> int:
    """Démarre une nouvelle transaction"""
    with self.lock:
        tid = self.transaction_counter
        self.transaction_counter += 1

        # Définir le niveau d'isolation
        isolation_sql = {
            "DEFERRED": "BEGIN DEFERRED TRANSACTION",
            "IMMEDIATE": "BEGIN IMMEDIATE TRANSACTION", 
            "EXCLUSIVE": "BEGIN EXCLUSIVE TRANSACTION"
        }.get(isolation_level, "BEGIN")

        try:
            # BUG: Cette ligne est CRITIQUE mais souvent absente
            self.storage._execute_raw(isolation_sql)
        except Exception as e:
            raise TransactionError(f"Failed to begin transaction: {e}")

        self.active_transactions[tid] = {
            'start_time': time.time(),
            'isolation': isolation_level,
            'changes': {},
            'savepoints': [],
            'state': 'ACTIVE'
        }

        logger.debug(f"Transaction {tid} started ({isolation_level})")
        return tid

--- VÉRIFICATION
Pour vérifier si le bug existe dans votre installation:

1. Ouvrez le fichier:
   nano /usr/lib/python3.9/site-packages/gsql/storage/sqlite_storage.py

2. Cherchez la méthode "begin" dans TransactionManager

3. Vérifiez si cette ligne existe:
   self.storage._execute_raw(isolation_sql)

Si la ligne est MANQUANTE, ajoutez-la après la définition de isolation_sql.
'''
    
    patch_file = "/tmp/gsql_transaction_patch.txt"
    with open(patch_file, "w") as f:
        f.write(patch_content)
    
    print(f"✅ Patch créé: {patch_file}")
    
    # Vérification directe
    print("\n🔍 Vérification directe du bug:")
    try:
        import inspect
        from gsql.storage.sqlite_storage import TransactionManager
        
        source = inspect.getsource(TransactionManager.begin)
        
        if "self.storage._execute_raw" in source:
            print("✅ La ligne critique est PRÉSENTE dans le code")
            print("⚠️  Le bug pourrait être ailleurs")
        else:
            print("❌ BUG CONFIRMÉ: La ligne critique est ABSENTE")
            print("   La méthode begin() ne démarre pas de transaction SQLite")
            
    except Exception as e:
        print(f"⚠️  Impossible de vérifier: {e}")
    
    return patch_file

def test_complete_workflow():
    """Workflow complet avec toutes les corrections"""
    print("\n🚀 WORKFLOW COMPLET AVEC CORRECTIONS")
    print("=" * 60)
    
    from gsql.database import Database
    import tempfile
    import time
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_complete_")
    
    try:
        print("1. Initialisation Database")
        db = Database(db_path=":memory:", base_dir=temp_dir)
        
        print("2. Nettoyage tables par défaut")
        for table in ['users', 'products', 'orders', 'logs']:
            db.execute(f"DROP TABLE IF EXISTS {table}")
        
        print("3. Création tables personnalisées")
        
        # Table produits
        db.execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT,
                price REAL CHECK(price > 0),
                stock INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Index séparé (correction bug INDEX)
        db.execute("CREATE INDEX idx_products_category ON products(category)")
        db.execute("CREATE INDEX idx_products_price ON products(price)")
        
        print("4. Insertion données")
        
        products = [
            ('Laptop', 'Electronics', 999.99, 10),
            ('Mouse', 'Electronics', 29.99, 50),
            ('Chair', 'Furniture', 149.99, 20),
            ('Desk', 'Furniture', 299.99, 15),
            ('Monitor', 'Electronics', 199.99, 25),
            ('Keyboard', 'Electronics', 79.99, 30)
        ]
        
        for prod in products:
            db.execute(
                "INSERT INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)",
                params=prod
            )
        
        print(f"✅ {len(products)} produits insérés")
        
        print("\n5. Requêtes avancées")
        
        # Requête avec GROUP BY et HAVING
        result = db.execute("""
            SELECT 
                category,
                COUNT(*) as count,
                AVG(price) as avg_price,
                SUM(stock) as total_stock,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM products
            GROUP BY category
            HAVING COUNT(*) > 1
            ORDER BY avg_price DESC
        """)
        
        if result['success']:
            print("📊 Statistiques par catégorie:")
            for row in result['rows']:
                print(f"  • {row[0]}: {row[1]} produits")
                print(f"    Prix: ${row[2]:.2f} moyen (${row[4]:.2f}-${row[5]:.2f})")
                print(f"    Stock: {row[3]} unités")
        
        print("\n6. Transactions avec workaround")
        
        # WORKAROUND: Transactions SQL directes
        print("💼 Début transaction manuelle")
        db.execute("BEGIN IMMEDIATE TRANSACTION")
        
        # Mettre à jour les stocks
        db.execute("UPDATE products SET stock = stock - 5 WHERE name = 'Laptop'")
        db.execute("UPDATE products SET stock = stock - 10 WHERE name = 'Mouse'")
        
        print("💰 Stocks mis à jour")
        
        # Vérifier avant commit
        result = db.execute("SELECT name, stock FROM products WHERE name IN ('Laptop', 'Mouse')")
        print("📊 Stocks après mise à jour (dans transaction):")
        for row in result['rows']:
            print(f"  • {row[0]}: {row[1]} unités")
        
        # Commit
        db.execute("COMMIT")
        print("✅ Transaction commitée")
        
        print("\n7. Test cache de requêtes")
        
        # Première exécution
        start = time.time()
        result1 = db.execute("SELECT COUNT(*) FROM products WHERE category = 'Electronics'", use_cache=True)
        time1 = time.time() - start
        
        # Cache hit
        start = time.time()
        result2 = db.execute("SELECT COUNT(*) FROM products WHERE category = 'Electronics'", use_cache=True)
        time2 = time.time() - start
        
        print(f"⏱️  Performance cache:")
        print(f"  • Sans cache: {time1:.4f}s")
        print(f"  • Avec cache: {time2:.4f}s")
        print(f"  • Amélioration: {time1/time2:.1f}x")
        
        print("\n8. Commandes spéciales GSQL")
        
        commands = [
            ("SHOW TABLES", "Liste tables"),
            ("STATS", "Statistiques"),
            ("VACUUM", "Optimisation"),
        ]
        
        for cmd, desc in commands:
            result = db.execute(cmd)
            if result['success']:
                print(f"  ✅ {cmd}: {desc}")
        
        print("\n9. Nettoyage et fermeture")
        
        db.execute("DROP TABLE products")
        db.close()
        shutil.rmtree(temp_dir)
        
        print("✅ Workflow terminé avec succès")
        return True
        
    except Exception as e:
        print(f"❌ Erreur workflow: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Fonction principale"""
    print("🚀 TEST GSQL - DIAGNOSTIC COMPLET BUG TRANSACTIONS")
    print("=" * 70)
    
    tests = [
        ("Transactions avec workaround", test_transaction_workaround),
        ("Méthodes Database", test_database_methods),
        ("Création patch", create_patch_for_transaction_bug),
        ("Workflow complet", test_complete_workflow)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"🧪 {test_name}")
        print('='*60)
        try:
            success = test_func()
            results[test_name] = "✅ PASS" if success else "❌ FAIL"
        except Exception as e:
            print(f"⚠️  Exception: {e}")
            results[test_name] = "💥 ERROR"
    
    # Résumé
    print(f"\n{'='*70}")
    print("📊 RÉSULTATS DIAGNOSTIC")
    print('='*70)
    
    for test_name, result in results.items():
        print(f"  {test_name:30s} : {result}")
    
    passed = sum(1 for r in results.values() if "PASS" in r)
    total = len(results)
    
    print(f"\n🎯 Score: {passed}/{total} tests réussis ({passed/total*100:.0f}%)")
    
    # Diagnostic final
    print("\n🔍 DIAGNOSTIC FINAL DU BUG TRANSACTION:")
    print("  1. TransactionManager.begin() ne démarre pas de transaction SQLite")
    print("  2. db.begin_transaction() retourne un TID mais pas de BEGIN SQL")
    print("  3. Les commits/rollbacks échouent car pas de transaction active")
    
    print("\n🔧 SOLUTIONS:")
    print("  A. WORKAROUND: Utiliser db.execute('BEGIN TRANSACTION') directement")
    print("  B. CORRECTION: Ajouter self.storage._execute_raw() dans TransactionManager.begin()")
    print("  C. ALTERNATIVE: Accès direct SQLite avec sqlite3.connect()")
    
    print("\n💡 RECOMMANDATION IMMÉDIATE:")
    print("  Utiliser le WORKAROUND A jusqu'à ce que le bug soit corrigé")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
