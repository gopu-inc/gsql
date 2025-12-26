#!/usr/bin/env python3
"""
Test de diagnostic des transactions GSQL
"""

import os
import tempfile
import sys
import logging
from pathlib import Path

# Ajouter le chemin parent pour importer gsql
sys.path.insert(0, str(Path(__file__).parent.parent))

from gsql.database import Database
from gsql.storage import SQLiteStorage

# Configuration du logging
logging.basicConfig(
    level=logging.DEBUG,  # DEBUG pour plus de détails
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_direct_sqlite():
    """Test direct avec SQLite pour vérifier si le problème vient de SQLite lui-même"""
    print("\n" + "="*60)
    print("🧪 Test DIRECT SQLite (sans GSQL)")
    print("="*60)
    
    import sqlite3
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Créer une table
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Insérer une ligne hors transaction
        cursor.execute("INSERT INTO test (value) VALUES ('initial')")
        conn.commit()
        
        print("\n1. Avant transaction:")
        cursor.execute("SELECT COUNT(*) FROM test")
        print(f"   Lignes: {cursor.fetchone()[0]}")
        
        # Démarrer transaction
        print("\n2. Début transaction:")
        cursor.execute("BEGIN")
        
        # Insérer dans la transaction
        cursor.execute("INSERT INTO test (value) VALUES ('in_transaction')")
        
        print("\n3. Dans transaction (avant rollback):")
        cursor.execute("SELECT COUNT(*) FROM test")
        print(f"   Lignes visibles: {cursor.fetchone()[0]}")
        
        # Rollback
        print("\n4. ROLLBACK:")
        cursor.execute("ROLLBACK")
        
        print("\n5. Après ROLLBACK:")
        cursor.execute("SELECT COUNT(*) FROM test")
        print(f"   Lignes: {cursor.fetchone()[0]}")
        
        # Vérifier
        cursor.execute("SELECT value FROM test ORDER BY id")
        rows = cursor.fetchall()
        print(f"   Contenu: {rows}")
        
        expected_rows = 1
        actual_rows = len(rows)
        
        success = actual_rows == expected_rows
        print(f"\n✅ SQLite ROLLBACK fonctionne: {success} (attendu: {expected_rows}, obtenu: {actual_rows})")
        
        return success
        
    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        try:
            conn.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_storage_rollback():
    """Test direct du storage GSQL"""
    print("\n" + "="*60)
    print("🧪 Test STORAGE GSQL Direct")
    print("="*60)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        storage = SQLiteStorage(db_path)
        
        # Créer une table
        storage.execute("CREATE TABLE test_storage (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Insérer une ligne hors transaction
        storage.execute("INSERT INTO test_storage (value) VALUES ('initial')")
        
        print("\n1. Avant transaction:")
        result = storage.execute("SELECT COUNT(*) as count FROM test_storage")
        print(f"   Lignes: {result.get('rows', [{}])[0].get('count', 0)}")
        
        # Démarrer transaction
        print("\n2. Début transaction:")
        tx_result = storage.begin_transaction()
        tid = tx_result.get('tid')
        print(f"   TID: {tid}, Success: {tx_result.get('success')}")
        
        # Insérer dans la transaction
        print("\n3. Insert dans transaction:")
        insert_result = storage.execute_in_transaction(tid, 
            "INSERT INTO test_storage (value) VALUES ('in_transaction')")
        print(f"   Insert success: {insert_result.get('success')}")
        
        # Vérifier dans la transaction
        print("\n4. Dans transaction (avant rollback):")
        select_in_tx = storage.execute_in_transaction(tid, 
            "SELECT COUNT(*) as count FROM test_storage")
        print(f"   Lignes visibles dans tx: {select_in_tx.get('rows', [{}])[0].get('count', 0)}")
        
        # Rollback
        print("\n5. ROLLBACK:")
        rollback_result = storage.rollback_transaction(tid)
        print(f"   Rollback success: {rollback_result.get('success')}")
        print(f"   Rollback error: {rollback_result.get('error')}")
        
        # Vérifier après rollback (hors transaction)
        print("\n6. Après ROLLBACK (hors transaction):")
        select_after = storage.execute("SELECT COUNT(*) as count FROM test_storage")
        print(f"   Lignes: {select_after.get('rows', [{}])[0].get('count', 0)}")
        
        # Vérifier le contenu
        content_result = storage.execute("SELECT value FROM test_storage ORDER BY id")
        rows = content_result.get('rows', [])
        print(f"   Contenu: {rows}")
        
        expected_rows = 1
        actual_rows = len(rows)
        
        success = actual_rows == expected_rows
        print(f"\n✅ Storage ROLLBACK fonctionne: {success} (attendu: {expected_rows}, obtenu: {actual_rows})")
        
        return success
        
    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        try:
            storage.close()
        except:
            pass
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_database_rollback_detailed():
    """Test détaillé du rollback avec Database"""
    print("\n" + "="*60)
    print("🧪 Test DATABASE Rollback Détaillé")
    print("="*60)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Créer une table
        db.execute("CREATE TABLE test_db (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Insérer une ligne hors transaction
        db.execute("INSERT INTO test_db (value) VALUES ('initial')")
        
        print("\n1. Avant transaction:")
        result = db.execute("SELECT COUNT(*) as count FROM test_db")
        print(f"   Lignes: {result.get('rows', [{}])[0].get('count', 0)}")
        
        # Démarrer transaction
        print("\n2. Début transaction:")
        begin_result = db.begin_transaction()
        tid = begin_result.get('tid')
        print(f"   TID: {tid}, Success: {begin_result.get('success')}")
        
        # Insérer dans la transaction
        print("\n3. Insert dans transaction:")
        insert_result = db.execute_in_transaction(
            "INSERT INTO test_db (value) VALUES ('in_transaction')")
        print(f"   Insert success: {insert_result.get('success')}")
        
        # Vérifier AVANT rollback
        print("\n4. AVANT rollback:")
        print("   4a. Dans transaction (avec execute_in_transaction):")
        select_in_tx = db.execute_in_transaction("SELECT COUNT(*) as count FROM test_db")
        print(f"       Lignes: {select_in_tx.get('rows', [{}])[0].get('count', 0)}")
        
        print("\n   4b. Hors transaction (avec execute normal):")
        select_outside = db.execute("SELECT COUNT(*) as count FROM test_db")
        print(f"       Lignes: {select_outside.get('rows', [{}])[0].get('count', 0)}")
        
        # Rollback
        print("\n5. ROLLBACK:")
        rollback_result = db.rollback_transaction()
        print(f"   Rollback success: {rollback_result.get('success')}")
        print(f"   Rollback error: {rollback_result.get('error')}")
        
        # Vérifier APRÈS rollback
        print("\n6. APRÈS rollback:")
        select_after = db.execute("SELECT COUNT(*) as count FROM test_db")
        rows_count = select_after.get('rows', [{}])[0].get('count', 0)
        print(f"   Lignes: {rows_count}")
        
        # Vérifier le contenu
        content_result = db.execute("SELECT value FROM test_db ORDER BY id")
        rows = content_result.get('rows', [])
        print(f"   Contenu: {rows}")
        
        expected_rows = 1
        actual_rows = rows_count
        
        success = actual_rows == expected_rows
        print(f"\n✅ Database ROLLBACK fonctionne: {success} (attendu: {expected_rows}, obtenu: {actual_rows})")
        
        if not success:
            print(f"\n⚠️  DEBUG: Le rollback n'a pas fonctionné!")
            print(f"   - Transaction active après rollback: {db.active_transaction}")
            print(f"   - Auto-commit mode: {db.auto_commit_mode}")
            
        return success
        
    except Exception as e:
        print(f"❌ Exception: {e}")
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

def test_manual_sql_commands():
    """Test avec commandes SQL manuelles"""
    print("\n" + "="*60)
    print("🧪 Test Commandes SQL Manuelles")
    print("="*60)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Créer une table
        db.execute("CREATE TABLE test_manual (id INTEGER PRIMARY KEY, value TEXT)")
        
        print("\n1. Insert initial:")
        db.execute("INSERT INTO test_manual (value) VALUES ('initial')")
        
        print("\n2. Utiliser BEGIN SQL direct:")
        result = db.execute("BEGIN")
        print(f"   Result: {result}")
        
        print("\n3. Insert dans transaction:")
        result = db.execute("INSERT INTO test_manual (value) VALUES ('in_transaction')")
        print(f"   Result: {result.get('success')}")
        
        print("\n4. Vérifier dans transaction:")
        result = db.execute("SELECT COUNT(*) as count FROM test_manual")
        print(f"   Lignes: {result.get('rows', [{}])[0].get('count', 0)}")
        
        print("\n5. ROLLBACK SQL direct:")
        result = db.execute("ROLLBACK")
        print(f"   Result: {result}")
        
        print("\n6. Vérifier après ROLLBACK:")
        result = db.execute("SELECT COUNT(*) as count FROM test_manual")
        rows = result.get('rows', [{}])[0].get('count', 0)
        print(f"   Lignes: {rows}")
        
        success = rows == 1
        print(f"\n✅ Commandes SQL directes: {success}")
        
        return success
        
    except Exception as e:
        print(f"❌ Exception: {e}")
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

def main():
    """Exécute tous les tests de diagnostic"""
    print("\n" + "="*80)
    print("🔍 DIAGNOSTIC DES TRANSACTIONS GSQL")
    print("="*80)
    
    tests = [
        ("SQLite Direct", test_direct_sqlite),
        ("Storage GSQL", test_storage_rollback),
        ("Database Rollback", test_database_rollback_detailed),
        ("Commandes SQL", test_manual_sql_commands),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n▶️  {test_name}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Exception: {e}")
            results.append((test_name, False))
    
    # Résumé
    print("\n" + "="*80)
    print("📊 RÉSULTATS DU DIAGNOSTIC")
    print("="*80)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print("\n" + "="*80)
    
    # Analyser les résultats
    sqlite_works = results[0][1] if len(results) > 0 else False
    storage_works = results[1][1] if len(results) > 1 else False
    database_works = results[2][1] if len(results) > 2 else False
    sql_cmds_work = results[3][1] if len(results) > 3 else False
    
    print("\n🔍 ANALYSE:")
    
    if not sqlite_works:
        print("❌ SQLite lui-même ne fonctionne pas - problème système")
    elif not storage_works:
        print("❌ Le problème est dans storage.py")
        print("   → Vérifiez les méthodes begin/commit/rollback dans SQLiteStorage")
    elif not database_works:
        print("❌ Le problème est dans database.py")
        print("   → Vérifiez comment Database gère les transactions")
    elif not sql_cmds_work:
        print("❌ Les commandes SQL directes ne fonctionnent pas")
        print("   → Vérifiez la méthode execute() dans Database")
    else:
        print("✅ Tous les tests passent - le problème est dans les tests originaux")
    
    return all(result for _, result in results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)