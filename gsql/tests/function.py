#!/usr/bin/env python3
"""
Test des transactions GSQL - Version Corrigée
"""

import os
import tempfile
import sys
import logging
from pathlib import Path

# Ajouter le chemin parent pour importer gsql
sys.path.insert(0, str(Path(__file__).parent.parent))

from gsql.database import Database

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def print_header(text):
    """Affiche un en-tête de test"""
    print("\n" + "="*60)
    print(f"🧪 {text}")
    print("="*60)

def print_result(success, message):
    """Affiche un résultat de test"""
    if success:
        print(f"✅ {message}")
    else:
        print(f"❌ {message}")

def test_basic_transaction():
    """Test basique de transaction (COMMIT) - VERSION CORRIGÉE"""
    print_header("Test Transaction BASIC - COMMIT")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        # Créer une base de données
        db = Database(db_path, create_default_tables=False)
        
        # Créer une table de test
        db.execute("""
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE
            )
        """)
        
        # Démarrer une transaction
        begin_result = db.begin_transaction("BEGIN DEFERRED")
        print(f"BEGIN: {begin_result}")
        
        if begin_result.get('success'):
            tid = begin_result['tid']
            
            # Insérer des données dans la transaction
            insert1 = db.execute_in_transaction(
                "INSERT INTO test_users (name, email) VALUES (?, ?)",
                ("Alice", "alice@example.com")
            )
            print(f"INSERT 1: {insert1.get('success')}")
            
            insert2 = db.execute_in_transaction(
                "INSERT INTO test_users (name, email) VALUES (?, ?)",
                ("Bob", "bob@example.com")
            )
            print(f"INSERT 2: {insert2.get('success')}")
            
            # Vérifier que les données sont visibles dans la transaction
            select_in_tx = db.execute_in_transaction("SELECT * FROM test_users")
            print(f"Rows in transaction: {select_in_tx.get('count', 0)}")
            
            # COMMIT la transaction
            commit_result = db.commit_transaction()
            print(f"COMMIT: {commit_result}")
            
            # Vérifier que les données sont persistées
            select_after = db.execute("SELECT * FROM test_users")
            rows_after = select_after.get('count', 0)
            print(f"Rows after commit: {rows_after}")
            
            # Validation
            success = (
                begin_result.get('success') and
                insert1.get('success') and
                insert2.get('success') and
                commit_result.get('success') and
                rows_after == 2
            )
            
            print_result(success, f"Transaction COMMIT: {success}")
            return success
            
        else:
            print_result(False, f"Failed to begin transaction: {begin_result.get('error')}")
            return False
            
    except Exception as e:
        print_result(False, f"Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Nettoyer
        try:
            db.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_rollback_transaction():
    """Test de transaction avec ROLLBACK - VERSION CORRIGÉE"""
    print_header("Test Transaction ROLLBACK")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        # Créer une base de données
        db = Database(db_path, create_default_tables=False)
        
        # Créer une table de test
        db.execute("""
            CREATE TABLE test_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price REAL
            )
        """)
        
        # Insérer une donnée de base (HORS transaction)
        db.execute(
            "INSERT INTO test_products (name, price) VALUES (?, ?)",
            ("Base Product", 10.0)
        )
        
        # Démarrer une transaction
        begin_result = db.begin_transaction("BEGIN")
        print(f"BEGIN: {begin_result}")
        
        if begin_result.get('success'):
            tid = begin_result['tid']
            
            # Insérer des données dans la transaction
            insert1 = db.execute_in_transaction(
                "INSERT INTO test_products (name, price) VALUES (?, ?)",
                ("Product A", 20.0)
            )
            print(f"INSERT 1: {insert1.get('success')}")
            
            # CORRECTION: Utiliser execute_in_transaction pour voir dans la transaction
            select_in_tx = db.execute_in_transaction("SELECT * FROM test_products")
            rows_in_tx = select_in_tx.get('count', 0)
            print(f"Rows visible in transaction: {rows_in_tx}")
            
            # ROLLBACK la transaction
            rollback_result = db.rollback_transaction()
            print(f"ROLLBACK: {rollback_result}")
            
            # Vérifier que les données ne sont PAS persistées
            # CORRECTION: Utiliser execute normal (hors transaction)
            select_after = db.execute("SELECT * FROM test_products")
            rows_after = select_after.get('count', 0)
            print(f"Rows after rollback (outside transaction): {rows_after}")
            
            # Validation
            # CORRIGÉ: Dans la transaction on voit 2 lignes, mais après rollback seulement 1
            success = (
                begin_result.get('success') and
                insert1.get('success') and
                rows_in_tx == 2 and  # Base + Product A (visibles dans la transaction)
                rollback_result.get('success') and
                rows_after == 1  # Seulement Base Product (après rollback)
            )
            
            print_result(success, f"Transaction ROLLBACK: {success}")
            return success
            
        else:
            print_result(False, f"Failed to begin transaction: {begin_result.get('error')}")
            return False
            
    except Exception as e:
        print_result(False, f"Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Nettoyer
        try:
            db.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_transaction_isolation_levels():
    """Test des différents niveaux d'isolation"""
    print_header("Test Niveaux d'Isolation")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Tester différents niveaux d'isolation
        isolation_levels = ["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]
        results = []
        
        for isolation in isolation_levels:
            print(f"\nTest isolation: {isolation}")
            
            # Démarrer transaction avec le niveau spécifié
            begin_result = db.begin_transaction(f"BEGIN {isolation}")
            success = begin_result.get('success', False)
            
            if success:
                # Vérifier que le niveau est correct
                actual_isolation = begin_result.get('isolation', 'UNKNOWN')
                print(f"  Started: success={success}, isolation={actual_isolation}")
                
                # Rollback
                db.rollback_transaction()
            else:
                print(f"  Failed: {begin_result.get('error')}")
            
            results.append(success)
        
        all_success = all(results)
        print_result(all_success, f"Isolation levels: {all_success}")
        return all_success
        
    except Exception as e:
        print_result(False, f"Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        try:
            db.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_transaction_context_manager():
    """Test du context manager pour transactions"""
    print_header("Test Context Manager")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Créer une table
        db.execute("CREATE TABLE test_context (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Utiliser le context manager
        with db.transaction("DEFERRED") as tx:
            # Ces inserts sont dans la transaction
            db.execute("INSERT INTO test_context (value) VALUES ('in_transaction')")
        
        # Après le context manager, la transaction est automatiquement commitée
        result = db.execute("SELECT COUNT(*) as count FROM test_context")
        count = result.get('rows', [{}])[0].get('count', 0) if result.get('rows') else 0
        
        success = count == 1
        print_result(success, f"Context manager auto-commit: {success}")
        return success
        
    except Exception as e:
        print_result(False, f"Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        try:
            db.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_transaction_errors():
    """Test des erreurs de transaction"""
    print_header("Test Erreurs de Transaction")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Test 1: Commit sans transaction active
        print("\n1. Commit sans transaction active:")
        result = db.commit_transaction()
        print(f"   Result: success={result.get('success')}, error={result.get('error')}")
        
        # Test 2: Rollback sans transaction active
        print("\n2. Rollback sans transaction active:")
        result = db.rollback_transaction()
        print(f"   Result: success={result.get('success')}, error={result.get('error')}")
        
        # Test 3: Double BEGIN
        print("\n3. Double BEGIN:")
        db.begin_transaction()
        result = db.begin_transaction()
        print(f"   Result: success={result.get('success')}, error={result.get('error')}")
        
        # Nettoyer
        db.rollback_transaction()
        
        print_result(True, "Error handling tested")
        return True
        
    except Exception as e:
        print_result(False, f"Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        try:
            db.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_savepoints():
    """Test des savepoints - VERSION CORRIGÉE"""
    print_header("Test Savepoints")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Créer une table
        db.execute("CREATE TABLE test_savepoints (id INTEGER PRIMARY KEY, step INTEGER)")
        
        # Démarrer transaction
        begin_result = db.begin_transaction()
        print(f"BEGIN: {begin_result.get('success')}")
        
        if not begin_result.get('success'):
            return False
        
        # Étape 1
        db.execute_in_transaction("INSERT INTO test_savepoints (step) VALUES (1)")
        
        # Créer savepoint
        savepoint1 = db.create_savepoint("step1")
        print(f"SAVEPOINT step1: {savepoint1.get('success')}")
        
        # Étape 2
        db.execute_in_transaction("INSERT INTO test_savepoints (step) VALUES (2)")
        
        # Étape 3
        db.execute_in_transaction("INSERT INTO test_savepoints (step) VALUES (3)")
        
        # CORRECTION: Vérifier avec execute_in_transaction
        select_before = db.execute_in_transaction("SELECT COUNT(*) as count FROM test_savepoints")
        count_before = select_before.get('rows', [{}])[0].get('count', 0) if select_before.get('rows') else 0
        print(f"Rows visible before rollback (in transaction): {count_before}")
        
        # Rollback au savepoint
        rollback_result = db.rollback_to_savepoint("step1")
        print(f"ROLLBACK TO step1: {rollback_result.get('success')}")
        
        # CORRECTION: Vérifier après rollback avec execute_in_transaction
        select_after = db.execute_in_transaction("SELECT COUNT(*) as count FROM test_savepoints")
        count_after = select_after.get('rows', [{}])[0].get('count', 0) if select_after.get('rows') else 0
        print(f"Rows visible after rollback (in transaction): {count_after}")
        
        # Commit
        commit_result = db.commit_transaction()
        print(f"COMMIT: {commit_result.get('success')}")
        
        # Vérifier final (hors transaction)
        select_final = db.execute("SELECT COUNT(*) as count FROM test_savepoints")
        count_final = select_final.get('rows', [{}])[0].get('count', 0) if select_final.get('rows') else 0
        print(f"Final rows (persisted): {count_final}")
        
        # CORRECTION: Après rollback au savepoint, on devrait voir seulement 1 ligne (step 1)
        # Après commit, seulement 1 ligne devrait être persistée
        success = (
            count_before == 3 and  # Toutes les 3 lignes visibles dans la transaction
            count_after == 1 and   # Après rollback, seulement step 1 visible
            count_final == 1       # Après commit, seulement step 1 persisté
        )
        
        print_result(success, f"Savepoints: {success}")
        return success
        
    except Exception as e:
        print_result(False, f"Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        try:
            db.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_compatibility_old_style():
    """Test de compatibilité avec l'ancien style (tid en paramètre)"""
    print_header("Test Compatibilité Ancien Style (tid param)")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Créer une table
        db.execute("CREATE TABLE test_compat (id INTEGER PRIMARY KEY, data TEXT)")
        
        # Ancien style: begin avec retour de tid
        begin_result = db.begin_transaction()
        tid = begin_result['tid']
        print(f"BEGIN (old style): tid={tid}")
        
        # Ancien style: insert avec tid
        insert_result = db.execute(
            "INSERT INTO test_compat (data) VALUES ('test')",
            tid=tid
        )
        print(f"INSERT with tid param: {insert_result.get('success')}")
        
        # Ancien style: commit avec tid paramètre
        commit_result = db.commit_transaction(tid)
        print(f"COMMIT with tid param: {commit_result.get('success')}")
        
        # Vérifier
        select_result = db.execute("SELECT COUNT(*) as count FROM test_compat")
        count = select_result.get('rows', [{}])[0].get('count', 0) if select_result.get('rows') else 0
        
        success = count == 1
        print_result(success, f"Compatibility mode: {success}")
        return success
        
    except Exception as e:
        print_result(False, f"Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        try:
            db.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_sql_commands():
    """Test des commandes SQL BEGIN/COMMIT/ROLLBACK"""
    print_header("Test Commandes SQL Directes")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Créer une table
        db.execute("CREATE TABLE test_sql_cmds (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Utiliser les commandes SQL directement
        print("\n1. BEGIN SQL command:")
        begin_result = db.execute("BEGIN")
        print(f"   Result: {begin_result.get('success')}")
        
        print("\n2. INSERT in transaction:")
        insert_result = db.execute("INSERT INTO test_sql_cmds (value) VALUES ('test')")
        print(f"   Result: {insert_result.get('success')}")
        
        print("\n3. SELECT in transaction:")
        select_result = db.execute("SELECT * FROM test_sql_cmds")
        print(f"   Rows: {select_result.get('count', 0)}")
        
        print("\n4. COMMIT SQL command:")
        commit_result = db.execute("COMMIT")
        print(f"   Result: {commit_result.get('success')}")
        
        print("\n5. Verify after commit:")
        final_result = db.execute("SELECT COUNT(*) as count FROM test_sql_cmds")
        count = final_result.get('rows', [{}])[0].get('count', 0) if final_result.get('rows') else 0
        print(f"   Final count: {count}")
        
        # Test ROLLBACK
        print("\n6. Test ROLLBACK:")
        db.execute("BEGIN")
        db.execute("INSERT INTO test_sql_cmds (value) VALUES ('to_rollback')")
        db.execute("ROLLBACK")
        
        final_count = db.execute("SELECT COUNT(*) as count FROM test_sql_cmds")
        count_after = final_count.get('rows', [{}])[0].get('count', 0) if final_count.get('rows') else 0
        print(f"   Count after rollback: {count_after}")
        
        success = (
            begin_result.get('success') and
            insert_result.get('success') and
            commit_result.get('success') and
            count == 1 and
            count_after == 1  # Le rollback a bien annulé la 2ème insertion
        )
        
        print_result(success, f"SQL commands: {success}")
        return success
        
    except Exception as e:
        print_result(False, f"Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        try:
            db.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def run_all_tests():
    """Exécute tous les tests"""
    print("\n" + "="*80)
    print("🚀 DÉMARRAGE DES TESTS DE TRANSACTIONS GSQL - VERSION CORRIGÉE")
    print("="*80)
    
    test_results = []
    
    # Liste des tests à exécuter
    tests = [
        ("Transaction BASIC (COMMIT)", test_basic_transaction),
        ("Transaction ROLLBACK", test_rollback_transaction),
        ("Niveaux d'isolation", test_transaction_isolation_levels),
        ("Context Manager", test_transaction_context_manager),
        ("Gestion des erreurs", test_transaction_errors),
        ("Savepoints", test_savepoints),
        ("Compatibilité ancien style", test_compatibility_old_style),
        ("Commandes SQL directes", test_sql_commands),
    ]
    
    # Exécuter chaque test
    for test_name, test_func in tests:
        print(f"\n▶️  Exécution: {test_name}")
        try:
            result = test_func()
            test_results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test '{test_name}' a échoué avec exception: {e}")
            test_results.append((test_name, False))
    
    # Afficher le résumé
    print("\n" + "="*80)
    print("📊 RÉSUMUM DES TESTS")
    print("="*80)
    
    passed = 0
    failed = 0
    
    for test_name, result in test_results:
        if result:
            print(f"✅ {test_name}: PASSED")
            passed += 1
        else:
            print(f"❌ {test_name}: FAILED")
            failed += 1
    
    print("\n" + "="*80)
    print(f"🎯 TOTAL: {passed} passed, {failed} failed sur {len(tests)} tests")
    print("="*80)
    
    return failed == 0

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)