#!/usr/bin/env python3
"""
Test simple de transactions GSQL
"""

import os
import tempfile
import sys
from pathlib import Path

# Ajouter le chemin parent pour importer gsql
sys.path.insert(0, str(Path(__file__).parent.parent))

from gsql.database import Database

def test_simple_transaction():
    """Test transaction simple avec COMMIT"""
    print("🧪 Test transaction simple")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        # Créer base de données
        db = Database(db_path, create_default_tables=False)
        
        # Créer table
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Test 1: INSERT sans transaction (auto-commit)
        print("1. INSERT sans transaction:")
        result = db.execute("INSERT INTO test (value) VALUES ('test1')")
        print(f"   Success: {result.get('success')}")
        
        # Test 2: Transaction avec COMMIT
        print("\n2. Transaction avec COMMIT:")
        print("   a) BEGIN:")
        result = db.execute("BEGIN")
        print(f"      Success: {result.get('success')}, TID: {result.get('tid')}")
        
        print("   b) INSERT dans transaction:")
        result = db.execute("INSERT INTO test (value) VALUES ('test2')")
        print(f"      Success: {result.get('success')}")
        
        print("   c) SELECT dans transaction:")
        result = db.execute("SELECT COUNT(*) as count FROM test")
        count_in_tx = result.get('rows', [{}])[0].get('count', 0)
        print(f"      Count in transaction: {count_in_tx}")
        
        print("   d) COMMIT:")
        result = db.execute("COMMIT")
        print(f"      Success: {result.get('success')}")
        
        print("   e) SELECT après COMMIT:")
        result = db.execute("SELECT COUNT(*) as count FROM test")
        count_after = result.get('rows', [{}])[0].get('count', 0)
        print(f"      Count after commit: {count_after}")
        
        # Test 3: Transaction avec ROLLBACK
        print("\n3. Transaction avec ROLLBACK:")
        print("   a) BEGIN:")
        result = db.execute("BEGIN")
        print(f"      Success: {result.get('success')}")
        
        print("   b) INSERT dans transaction:")
        result = db.execute("INSERT INTO test (value) VALUES ('to_rollback')")
        print(f"      Success: {result.get('success')}")
        
        print("   c) SELECT avant ROLLBACK:")
        result = db.execute("SELECT COUNT(*) as count FROM test")
        count_before = result.get('rows', [{}])[0].get('count', 0)
        print(f"      Count before rollback: {count_before}")
        
        print("   d) ROLLBACK:")
        result = db.execute("ROLLBACK")
        print(f"      Success: {result.get('success')}")
        
        print("   e) SELECT après ROLLBACK:")
        result = db.execute("SELECT COUNT(*) as count FROM test", use_cache=False)
        count_after = result.get('rows', [{}])[0].get('count', 0)
        print(f"      Count after rollback: {count_after}")
        
        # Vérification finale
        result = db.execute("SELECT value FROM test ORDER BY id")
        values = [row.get('value') for row in result.get('rows', [])]
        print(f"\n   Final values: {values}")
        
        success = count_after == 2  # test1 + test2 (to_rollback a été annulé)
        print(f"\n✅ Test {'PASSED' if success else 'FAILED'}")
        
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

def test_api_transaction():
    """Test transaction avec API (sans SQL direct)"""
    print("\n🧪 Test transaction avec API")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Créer table
        db.execute("CREATE TABLE test_api (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Test avec API
        print("1. Début transaction avec API:")
        result = db.begin_transaction()
        print(f"   Success: {result.get('success')}, TID: {result.get('tid')}")
        
        print("2. INSERT avec execute_in_transaction:")
        result = db.execute_in_transaction("INSERT INTO test_api (value) VALUES ('test_api')")
        print(f"   Success: {result.get('success')}")
        
        print("3. Commit avec API:")
        result = db.commit_transaction()
        print(f"   Success: {result.get('success')}")
        
        print("4. Vérifier après commit:")
        result = db.execute("SELECT COUNT(*) as count FROM test_api")
        count = result.get('rows', [{}])[0].get('count', 0)
        print(f"   Count: {count}")
        
        success = count == 1
        print(f"\n✅ Test API {'PASSED' if success else 'FAILED'}")
        
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

if __name__ == "__main__":
    print("="*60)
    print("🚀 TEST TRANSACTIONS GSQL")
    print("="*60)
    
    test1 = test_simple_transaction()
    test2 = test_api_transaction()
    
    print("\n" + "="*60)
    print("📊 RÉSULTATS")
    print("="*60)
    print(f"Test SQL direct: {'✅ PASS' if test1 else '❌ FAIL'}")
    print(f"Test API: {'✅ PASS' if test2 else '❌ FAIL'}")
    
    if test1 and test2:
        print("\n🎉 TOUS LES TESTS ONT RÉUSSI!")
    else:
        print("\n⚠️  CERTAINS TESTS ONT ÉCHOUÉ")
    
    sys.exit(0 if (test1 and test2) else 1)