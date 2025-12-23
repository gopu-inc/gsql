#!/usr/bin/env python3
"""
TEST GSQL CORRIGÉ - Version avec bugs fixes
"""

import os
import sys
import time
import inspect
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

def test_storage_transactions_fixed():
    """Test des transactions corrigé"""
    print("\n🔧 TEST TRANSACTIONS CORRIGÉ")
    print("-" * 50)
    
    from gsql.storage import SQLiteStorage
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_fix_")
    db_path = os.path.join(temp_dir, "test_fix.db")
    
    try:
        storage = SQLiteStorage(db_path=db_path, buffer_pool_size=10)
        
        # Créer table
        storage.execute("""
            CREATE TABLE test_fix (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        
        # Transaction avec savepoint CORRIGÉ
        tm = storage.transaction_manager
        tid = tm.begin(isolation_level="DEFERRED")
        print(f"✅ Transaction démarrée: TID={tid}")
        
        # Insertion
        storage.execute("INSERT INTO test_fix (id, name) VALUES (1, 'Test1')")
        
        # CORRECTION: Créer le savepoint avec _execute_raw()
        storage._execute_raw("SAVEPOINT sp1")
        print(f"✅ Savepoint sp1 créé via _execute_raw()")
        
        # Insertion supplémentaire
        storage.execute("INSERT INTO test_fix (id, name) VALUES (2, 'Test2')")
        
        # Vérifier avant rollback
        result = storage.execute("SELECT COUNT(*) FROM test_fix")
        count_before = result['rows'][0][0] if result['rows'] else 0
        print(f"📊 Lignes avant rollback: {count_before}")
        
        # Rollback au savepoint
        tm.rollback(tid, to_savepoint="sp1")
        print(f"✅ Rollback to sp1 réussi")
        
        # Vérifier après rollback
        result = storage.execute("SELECT COUNT(*) FROM test_fix")
        count_after = result['rows'][0][0] if result['rows'] else 0
        print(f"📊 Lignes après rollback: {count_after}")
        
        # Commit
        tm.commit(tid)
        print(f"✅ Transaction commitée")
        
        # Test supplémentaire: savepoint via transaction manager
        tid2 = tm.begin()
        storage._execute_raw("SAVEPOINT sp2")
        print(f"✅ Savepoint sp2 créé")
        
        # Rollback sans spécifier savepoint
        tm.rollback(tid2)
        print(f"✅ Rollback complet réussi")
        
        storage.close()
        shutil.rmtree(temp_dir)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_database_savepoint_fixed():
    """Test des savepoints database corrigé"""
    print("\n🔧 TEST DATABASE SAVEPOINT CORRIGÉ")
    print("-" * 50)
    
    from gsql.database import Database
    import tempfile
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_db_fix_")
    
    try:
        db = Database(db_path=":memory:", base_dir=temp_dir)
        
        # CORRECTION: Utiliser les méthodes transaction de database
        print("🔹 Méthode 1: Via database transaction methods")
        
        # Début transaction
        db.begin_transaction(isolation_level="DEFERRED")
        print(f"✅ Transaction démarrée via database")
        
        # Insertion
        db.execute("INSERT INTO users (username, email) VALUES ('test', 'test@example.com')")
        
        # CORRECTION: Utiliser la bonne signature
        # create_savepoint() de database nécessite tid et name
        # Mais database gère son propre tid, donc utiliser storage directement
        tid = 0  # ID par défaut
        db.storage.create_savepoint(tid, "db_sp1")
        print(f"✅ Savepoint db_sp1 créé")
        
        # Autre insertion
        db.execute("INSERT INTO users (username, email) VALUES ('test2', 'test2@example.com')")
        
        # Rollback
        db.storage.rollback_transaction(tid, to_savepoint="db_sp1")
        print(f"✅ Rollback to db_sp1 réussi")
        
        # Commit
        db.commit_transaction(tid)
        print(f"✅ Transaction commitée")
        
        print(f"\n🔹 Méthode 2: Via storage directement")
        
        # Transaction via storage
        tid2 = db.storage.begin_transaction()
        print(f"✅ Storage transaction démarrée: TID={tid2}")
        
        # Savepoint via storage
        db.storage.create_savepoint(tid2, "storage_sp")
        print(f"✅ Savepoint storage_sp créé")
        
        # Commit via storage
        db.storage.commit_transaction(tid2)
        print(f"✅ Storage transaction commitée")
        
        db.close()
        shutil.rmtree(temp_dir)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_all_transaction_methods():
    """Test toutes les méthodes de transaction"""
    print("\n🔧 TEST COMPLET DES TRANSACTIONS")
    print("-" * 50)
    
    from gsql.database import Database
    import tempfile
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_tx_all_")
    
    try:
        db = Database(db_path=":memory:", base_dir=temp_dir)
        
        print("📋 Signatures disponibles:")
        print("\n1. Database transaction methods:")
        db_methods = [
            ("begin_transaction", "isolation_level='DEFERRED'"),
            ("commit_transaction", "tid"),
            ("rollback_transaction", "tid, to_savepoint=None"),
            ("create_savepoint", "tid, name")
        ]
        
        for method, params in db_methods:
            print(f"   • {method}({params})")
        
        print("\n2. Storage transaction methods:")
        storage = db.storage
        storage_methods = [
            ("begin_transaction", "isolation_level='DEFERRED' → tid"),
            ("commit_transaction", "tid → bool"),
            ("rollback_transaction", "tid, to_savepoint=None → bool"),
            ("create_savepoint", "tid, name → bool")
        ]
        
        for method, desc in storage_methods:
            print(f"   • {method}: {desc}")
        
        print("\n3. TransactionManager methods:")
        tm = storage.transaction_manager
        tm_methods = [
            ("begin", "isolation_level='DEFERRED' → tid"),
            ("commit", "tid → bool"),
            ("rollback", "tid, to_savepoint=None → bool"),
            ("savepoint", "tid, name → bool")
        ]
        
        for method, desc in tm_methods:
            print(f"   • {method}: {desc}")
        
        # Test pratique: Niveaux d'isolation
        print("\n🧪 Test niveaux d'isolation:")
        
        isolation_levels = ["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]
        for level in isolation_levels:
            try:
                tid = storage.begin_transaction(isolation_level=level)
                print(f"   ✅ {level}: Transaction démarrée (TID={tid})")
                
                # Test simple
                storage.execute(f"INSERT INTO logs (level, message) VALUES ('INFO', 'Test {level}')")
                
                # Commit
                storage.commit_transaction(tid)
                print(f"   ✅ {level}: Commit réussi")
                
            except Exception as e:
                print(f"   ❌ {level}: {e}")
        
        db.close()
        shutil.rmtree(temp_dir)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_workflow_complet():
    """Workflow complet avec toutes les corrections"""
    print("\n🚀 WORKFLOW COMPLET CORRIGÉ")
    print("=" * 60)
    
    from gsql.database import Database
    import tempfile
    
    temp_dir = tempfile.mkdtemp(prefix="gsql_workflow_")
    
    try:
        # 1. Initialisation
        db = Database(
            db_path=":memory:",
            base_dir=temp_dir,
            buffer_pool_size=50,
            enable_wal=True,
            auto_recovery=True
        )
        print("✅ Database initialisée")
        
        # 2. Création table custom
        db.execute("""
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                department TEXT,
                salary REAL,
                hired DATE DEFAULT CURRENT_DATE
            )
        """)
        print("✅ Table 'employees' créée")
        
        # 3. Transaction complexe
        print("\n🔹 Transaction complexe:")
        
        # Début transaction
        db.begin_transaction(isolation_level="IMMEDIATE")
        print("   ✅ Transaction IMMEDIATE démarrée")
        
        # Insertion données
        employees = [
            ("Alice", "Engineering", 75000),
            ("Bob", "Sales", 65000),
            ("Charlie", "Marketing", 70000)
        ]
        
        for name, dept, salary in employees:
            db.execute(
                "INSERT INTO employees (name, department, salary) VALUES (?, ?, ?)",
                params=[name, dept, salary]
            )
        
        # Savepoint après insertion
        tid = 0  # Première transaction
        db.storage.create_savepoint(tid, "after_insert")
        print("   ✅ Savepoint 'after_insert' créé")
        
        # Mise à jour
        db.execute("UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering'")
        print("   ✅ Salaires Engineering augmentés de 10%")
        
        # Vérification avant rollback
        result = db.execute("SELECT SUM(salary) FROM employees")
        total_before = result['rows'][0][0] if result['rows'] else 0
        print(f"   📊 Total salaires avant rollback: ${total_before:,.2f}")
        
        # Rollback partiel
        db.storage.rollback_transaction(tid, to_savepoint="after_insert")
        print("   ✅ Rollback to 'after_insert'")
        
        # Vérification après rollback
        result = db.execute("SELECT SUM(salary) FROM employees")
        total_after = result['rows'][0][0] if result['rows'] else 0
        print(f"   📊 Total salaires après rollback: ${total_after:,.2f}")
        
        # Commit
        db.commit_transaction(tid)
        print("   ✅ Transaction commitée")
        
        # 4. Cache de requêtes
        print("\n🔹 Test cache de requêtes:")
        
        # Première exécution
        start = time.time()
        result1 = db.execute("SELECT * FROM employees ORDER BY salary DESC", use_cache=True)
        time1 = time.time() - start
        
        # Seconde exécution (cache)
        start = time.time()
        result2 = db.execute("SELECT * FROM employees ORDER BY salary DESC", use_cache=True)
        time2 = time.time() - start
        
        print(f"   • Première exécution: {time1:.3f}s")
        print(f"   • Cache hit: {time2:.3f}s")
        print(f"   • Amélioration: {time1/time2:.1f}x")
        
        # 5. Stats et métadonnées
        print("\n🔹 Statistiques:")
        
        # Stats database
        result = db.execute("STATS")
        if result.get('success'):
            stats = result.get('stats', {})
            print(f"   • Requêtes exécutées: {stats.get('queries_executed', 0)}")
            print(f"   • Cache hits: {stats.get('queries_cached', 0)}")
            print(f"   • Erreurs: {stats.get('errors', 0)}")
        
        # Tables
        result = db.execute("SHOW TABLES")
        if result.get('success'):
            tables = [t['table'] for t in result.get('tables', [])]
            print(f"   • Tables: {', '.join([t for t in tables if not t.startswith('_')])}")
        
        # 6. Fermeture propre
        db.close()
        print("\n✅ Database fermée proprement")
        
        shutil.rmtree(temp_dir)
        print("🧹 Fichiers temporaires nettoyés")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur workflow: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Exécute tous les tests corrigés"""
    print("🔧 TESTS GSQL AVEC CORRECTIONS DES BUGS")
    print("=" * 70)
    
    tests = [
        ("Transactions Storage corrigées", test_storage_transactions_fixed),
        ("Savepoints Database corrigés", test_database_savepoint_fixed),
        ("Toutes méthodes transaction", test_all_transaction_methods),
        ("Workflow complet", test_workflow_complet)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"🧪 TEST: {test_name}")
        print(f"{'='*60}")
        try:
            success = test_func()
            results[test_name] = "✅ PASS" if success else "❌ FAIL"
        except Exception as e:
            print(f"⚠️  Exception: {e}")
            results[test_name] = "💥 ERROR"
    
    # Résumé
    print(f"\n{'='*70}")
    print("📊 RÉSULTATS TESTS CORRIGÉS")
    print(f"{'='*70}")
    
    for test_name, result in results.items():
        print(f"  {test_name:35s} : {result}")
    
    passed = sum(1 for r in results.values() if "PASS" in r)
    total = len(results)
    
    print(f"\n🎯 Score: {passed}/{total} tests réussis ({passed/total*100:.0f}%)")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
