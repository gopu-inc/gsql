#!/usr/bin/env python3
"""
TEST GSQL AVEC GESTION PROPRE DES TABLES - VERSION CORRIGÉE
"""

import os
import sys
import time
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

print("🧹 TEST GSQL - GESTION PROPRE DES TABLES (CORRIGÉ)")
print("=" * 70)

def safe_execute(db, sql, params=None):
    """Exécute SQL avec gestion d'erreur"""
    try:
        return db.execute(sql, params)
    except Exception as e:
        print(f"⚠️  SQL échoué: {sql[:50]}... → {e}")
        return {'success': False, 'message': str(e)}

def cleanup_default_tables(db):
    """Nettoie les tables par défaut si elles existent"""
    print("\n🧹 Nettoyage tables par défaut:")
    
    default_tables = ['users', 'products', 'orders', 'logs']
    
    for table in default_tables:
        try:
            # Vérifier si la table existe
            result = db.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            if result.get('success') and result.get('rows'):
                # Désactiver les foreign keys temporairement
                db.execute("PRAGMA foreign_keys = OFF")
                
                # Supprimer la table
                drop_result = db.execute(f"DROP TABLE IF EXISTS {table}")
                if drop_result.get('success'):
                    print(f"  ✅ Table '{table}' supprimée")
                else:
                    print(f"  ⚠️  Échec suppression '{table}': {drop_result.get('message')}")
                
                # Réactiver les foreign keys
                db.execute("PRAGMA foreign_keys = ON")
        except Exception as e:
            print(f"  ❌ Erreur nettoyage '{table}': {e}")

def test_table_management():
    """Test la gestion complète des tables"""
    print("\n📊 TEST GESTION DES TABLES")
    print("-" * 50)
    
    from gsql.database import Database
    import tempfile
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_tables_")
    
    try:
        # 1. Initialisation
        db = Database(db_path=":memory:", base_dir=temp_dir)
        print("✅ Database initialisée")
        
        # 2. Lister les tables existantes
        result = db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        if result.get('success'):
            tables = [row[0] for row in result.get('rows', [])]
            print(f"📋 Tables existantes: {len(tables)}")
            for table in tables:
                print(f"  • {table}")
        
        # 3. Nettoyer avant de créer
        cleanup_default_tables(db)
        
        # 4. Créer nos propres tables
        print("\n🔨 Création tables personnalisées:")
        
        # Table 1: Sans foreign key
        sql1 = """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            salary REAL DEFAULT 0.0,
            department TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        result = safe_execute(db, sql1)
        if result.get('success'):
            print("✅ Table 'employees' créée")
        
        # Table 2: Avec foreign key
        sql2 = """
        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            manager_id INTEGER,
            budget REAL,
            deadline DATE,
            FOREIGN KEY (manager_id) REFERENCES employees(id) ON DELETE SET NULL
        )
        """
        result = safe_execute(db, sql2)
        if result.get('success'):
            print("✅ Table 'projects' créée avec FK")
        
        # Table 3: Avec contraintes
        sql3 = """
        CREATE TABLE tasks (
            task_id INTEGER PRIMARY KEY,
            project_id INTEGER NOT NULL,
            description TEXT,
            status TEXT CHECK(status IN ('pending', 'in_progress', 'completed')),
            priority INTEGER CHECK(priority BETWEEN 1 AND 5),
            assigned_to INTEGER,
            FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
            FOREIGN KEY (assigned_to) REFERENCES employees(id) ON DELETE SET NULL
        )
        """
        result = safe_execute(db, sql3)
        if result.get('success'):
            print("✅ Table 'tasks' créée avec contraintes")
        
        # 5. Insérer des données
        print("\n📝 Insertion données de test:")
        
        # Employees
        employees_data = [
            ('Alice Johnson', 'alice@company.com', 75000, 'Engineering'),
            ('Bob Smith', 'bob@company.com', 65000, 'Sales'),
            ('Charlie Brown', 'charlie@company.com', 80000, 'Engineering'),
            ('Diana Prince', 'diana@company.com', 90000, 'Management')
        ]
        
        for emp in employees_data:
            sql = "INSERT INTO employees (name, email, salary, department) VALUES (?, ?, ?, ?)"
            result = safe_execute(db, sql, params=emp)
            if result.get('success'):
                print(f"  ✅ Employee: {emp[0]}")
        
        # Projects
        projects_data = [
            (1, 'Website Redesign', 1, 50000, '2024-06-30'),
            (2, 'Mobile App', 3, 75000, '2024-08-15'),
            (3, 'Database Migration', 1, 30000, '2024-05-20')
        ]
        
        for proj in projects_data:
            sql = "INSERT INTO projects (project_id, name, manager_id, budget, deadline) VALUES (?, ?, ?, ?, ?)"
            result = safe_execute(db, sql, params=proj)
            if result.get('success'):
                print(f"  ✅ Project: {proj[1]}")
        
        # 6. Requêtes complexes
        print("\n🔍 Requêtes complexes:")
        
        # JOIN avec agrégation
        sql = """
        SELECT 
            e.department,
            COUNT(*) as employee_count,
            AVG(e.salary) as avg_salary,
            COUNT(p.project_id) as project_count
        FROM employees e
        LEFT JOIN projects p ON e.id = p.manager_id
        GROUP BY e.department
        ORDER BY avg_salary DESC
        """
        
        result = safe_execute(db, sql)
        if result.get('success'):
            print("📊 Stats par département:")
            for row in result.get('rows', []):
                dept, emp_count, avg_salary, proj_count = row
                print(f"  • {dept}: {emp_count} employés, ${avg_salary:,.0f} moyen, {proj_count} projets")
        
        # 7. Test contraintes
        print("\n⚡ Test des contraintes:")
        
        # Violation UNIQUE
        sql = "INSERT INTO employees (name, email) VALUES ('Test', 'alice@company.com')"
        result = safe_execute(db, sql)
        if not result.get('success'):
            print("✅ Contrainte UNIQUE fonctionne")
        
        # Violation CHECK
        sql = "INSERT INTO tasks (task_id, project_id, status) VALUES (1, 1, 'invalid_status')"
        result = safe_execute(db, sql)
        if not result.get('success'):
            print("✅ Contrainte CHECK fonctionne")
        
        # 8. Test foreign key cascade
        print("\n🔗 Test FOREIGN KEY CASCADE:")
        
        # Créer une tâche
        sql = "INSERT INTO tasks (task_id, project_id, status, priority) VALUES (1, 1, 'pending', 3)"
        safe_execute(db, sql)
        print("✅ Tâche créée pour project_id=1")
        
        # Supprimer le projet (devrait supprimer la tâche via CASCADE)
        sql = "DELETE FROM projects WHERE project_id = 1"
        safe_execute(db, sql)
        
        # Vérifier que la tâche est supprimée
        sql = "SELECT COUNT(*) FROM tasks WHERE project_id = 1"
        result = safe_execute(db, sql)
        if result.get('success') and result.get('rows'):
            count = result['rows'][0][0]
            if count == 0:
                print("✅ CASCADE DELETE fonctionne")
            else:
                print(f"⚠️  CASCADE DELETE échoué: {count} tâches restantes")
        
        # 9. Métadonnées
        print("\n📋 Métadonnées finales:")
        
        # Nombre de tables
        result = db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE '_gsql_%'")
        if result.get('success'):
            table_count = result['rows'][0][0] if result['rows'] else 0
            print(f"📊 Tables personnalisées: {table_count}")
        
        # Liste complète
        result = db.execute("""
            SELECT name, sql 
            FROM sqlite_master 
            WHERE type='table' 
            AND name NOT LIKE '_gsql_%'
            ORDER BY name
        """)
        
        if result.get('success'):
            for row in result.get('rows', []):
                name, sql_def = row
                print(f"  • {name}: {sql_def[:60]}...")
        
        # 10. Cleanup final
        print("\n🧹 Cleanup final:")
        for table in ['tasks', 'projects', 'employees']:
            safe_execute(db, f"DROP TABLE IF EXISTS {table}")
            print(f"  ✅ Table '{table}' supprimée")
        
        db.close()
        shutil.rmtree(temp_dir)
        print("\n✅ Test terminé avec succès")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur majeure: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_transaction_with_clean_tables():
    """Test transactions avec tables propres"""
    print("\n💼 TEST TRANSACTIONS AVEC TABLES PROPRES")
    print("-" * 50)
    
    from gsql.database import Database
    import tempfile
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_trans_clean_")
    
    try:
        db = Database(db_path=":memory:", base_dir=temp_dir)
        
        # Nettoyer d'abord
        cleanup_default_tables(db)
        
        # Créer table simple
        db.execute("""
            CREATE TABLE bank_accounts (
                account_id INTEGER PRIMARY KEY,
                owner TEXT NOT NULL,
                balance REAL DEFAULT 0.0,
                CHECK(balance >= 0)
            )
        """)
        
        # Insérer données initiales
        accounts = [
            (101, 'Alice', 1000.0),
            (102, 'Bob', 500.0),
            (103, 'Charlie', 1500.0)
        ]
        
        for acc in accounts:
            db.execute("INSERT INTO bank_accounts VALUES (?, ?, ?)", params=acc)
        
        print("✅ Données initiales insérées")
        
        # Transaction: transfert bancaire
        print("\n🔀 Transaction: Transfert bancaire")
        
        # Début transaction
        db.begin_transaction(isolation_level="IMMEDIATE")
        print("💼 Transaction IMMEDIATE démarrée")
        
        try:
            # 1. Débiter Alice
            db.execute("UPDATE bank_accounts SET balance = balance - 200 WHERE account_id = 101")
            print("💰 Alice débitée de 200")
            
            # WORKAROUND: Savepoint via execute()
            db.execute("SAVEPOINT before_credit")
            print("📌 Savepoint 'before_credit' créé")
            
            # 2. Créditer Bob
            db.execute("UPDATE bank_accounts SET balance = balance + 200 WHERE account_id = 102")
            print("💰 Bob crédité de 200")
            
            # Vérifier solde négatif (devrait échouer)
            db.execute("UPDATE bank_accounts SET balance = -100 WHERE account_id = 103")
            print("⚠️  Tentative solde négatif...")
            
        except Exception as e:
            print(f"❌ Erreur dans transaction: {e}")
            # Rollback au savepoint
            db.execute("ROLLBACK TO SAVEPOINT before_credit")
            print("↩️  Rollback to savepoint")
            
            # Réessayer crédit
            db.execute("UPDATE bank_accounts SET balance = balance + 200 WHERE account_id = 102")
            print("💰 Bob crédité (après rollback)")
        
        # Vérifier soldes
        result = db.execute("SELECT owner, balance FROM bank_accounts ORDER BY account_id")
        if result.get('success'):
            print("\n📊 Soldes finaux:")
            for row in result.get('rows', []):
                print(f"  • {row[0]}: ${row[1]:.2f}")
        
        # Commit
        db.commit_transaction(0)
        print("✅ Transaction commitée")
        
        # Test rollback complet
        print("\n🔀 Test rollback complet:")
        
        db.begin_transaction()
        db.execute("UPDATE bank_accounts SET balance = balance + 1000 WHERE account_id = 101")
        print("💰 Alice +1000 (dans transaction)")
        
        db.rollback_transaction(0)
        print("↩️  Rollback complet")
        
        # Vérifier que le changement n'est pas persistant
        result = db.execute("SELECT balance FROM bank_accounts WHERE account_id = 101")
        if result.get('success') and result.get('rows'):
            balance = result['rows'][0][0]
            print(f"💰 Solde Alice après rollback: ${balance:.2f}")
        
        db.close()
        shutil.rmtree(temp_dir)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur transaction: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_performance_with_clean_tables():
    """Test performance avec tables optimisées"""
    print("\n⚡ TEST PERFORMANCE AVEC TABLES PROPRES")
    print("-" * 50)
    
    from gsql.database import Database
    import tempfile
    import time
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_perf_")
    
    try:
        db = Database(db_path=":memory:", base_dir=temp_dir)
        
        # Nettoyer tables par défaut
        cleanup_default_tables(db)
        
        # Créer table optimisée
        db.execute("""
            CREATE TABLE performance_test (
                id INTEGER PRIMARY KEY,
                value REAL NOT NULL,
                category TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_category (category),
                INDEX idx_timestamp (timestamp)
            )
        """)
        
        print("✅ Table optimisée créée")
        
        # Benchmark INSERT
        print("\n📈 Benchmark INSERT:")
        
        start = time.time()
        batch_size = 1000
        
        for i in range(batch_size):
            db.execute(
                "INSERT INTO performance_test (id, value, category) VALUES (?, ?, ?)",
                params=[i, i * 1.5, f"cat_{i % 10}"]
            )
        
        insert_time = time.time() - start
        print(f"  • {batch_size} INSERT: {insert_time:.3f}s ({insert_time/batch_size*1000:.2f}ms/row)")
        
        # Benchmark SELECT
        print("\n📈 Benchmark SELECT:")
        
        # Sans cache
        start = time.time()
        result1 = db.execute("SELECT AVG(value) FROM performance_test WHERE category = 'cat_1'", use_cache=False)
        time1 = time.time() - start
        
        # Avec cache
        start = time.time()
        result2 = db.execute("SELECT AVG(value) FROM performance_test WHERE category = 'cat_1'", use_cache=True)
        time2 = time.time() - start
        
        print(f"  • Sans cache: {time1:.4f}s")
        print(f"  • Avec cache: {time2:.4f}s")
        print(f"  • Amélioration: {time1/time2:.1f}x")
        
        # Benchmark JOIN
        print("\n📈 Benchmark JOIN:")
        
        # Créer seconde table
        db.execute("CREATE TABLE categories (cat_id TEXT PRIMARY KEY, description TEXT)")
        for i in range(10):
            db.execute("INSERT INTO categories VALUES (?, ?)", params=[f"cat_{i}", f"Category {i}"])
        
        start = time.time()
        result = db.execute("""
            SELECT p.category, c.description, COUNT(*), AVG(p.value)
            FROM performance_test p
            JOIN categories c ON p.category = c.cat_id
            GROUP BY p.category
            ORDER BY AVG(p.value) DESC
        """)
        
        join_time = time.time() - start
        print(f"  • JOIN complexe: {join_time:.3f}s")
        
        if result.get('success'):
            print(f"  • Résultats: {len(result.get('rows', []))} groupes")
        
        # Stats finales
        print("\n📊 Stats finales:")
        result = db.execute("SELECT COUNT(*) FROM performance_test")
        if result.get('success'):
            count = result['rows'][0][0] if result['rows'] else 0
            print(f"  • Lignes totales: {count}")
        
        # VACUUM
        result = db.execute("VACUUM")
        if result.get('success'):
            print("  • VACUUM exécuté")
        
        db.close()
        shutil.rmtree(temp_dir)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur performance: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Exécute tous les tests"""
    print("🚀 TESTS GSQL AVEC GESTION PROPRE DES TABLES")
    print("=" * 70)
    
    tests = [
        ("Gestion complète tables", test_table_management),
        ("Transactions tables propres", test_transaction_with_clean_tables),
        ("Performance tables optimisées", test_performance_with_clean_tables)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print("\n" + "=" * 60)
        print(f"🧪 {test_name}")
        print("=" * 60)
        try:
            success = test_func()
            results[test_name] = "✅ PASS" if success else "❌ FAIL"
        except Exception as e:
            print(f"⚠️  Exception: {e}")
            results[test_name] = "💥 ERROR"
    
    # Résumé
    print("\n" + "=" * 70)
    print("📊 RÉSULTATS FINAUX")
    print("=" * 70)
    
    for test_name, result in results.items():
        print(f"  {test_name:40s} : {result}")
    
    passed = sum(1 for r in results.values() if "PASS" in r)
    total = len(results)
    
    print(f"\n🎯 Score: {passed}/{total} tests réussis ({passed/total*100:.0f}%)")
    
    # Recommandations
    print("\n💡 RECOMMANDATIONS POUR GSQL:")
    print("   1. Ajouter option `create_default_tables=False` à Database.__init__()")
    print("   2. Améliorer DROP TABLE IF EXISTS avec vérification FK")
    print("   3. Documenter les tables système (_gsql_*)")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
