#!/usr/bin/env python3
"""
Test simple des transactions GSQL
"""

import os
import sys
import tempfile

# Le module est dans le même dossier parent
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_basic_transaction():
    """Teste une transaction simple"""
    print("🧪 Test transaction BEGIN/COMMIT")
    
    from gsql.database import create_database
    
    # Créer une base temporaire
    db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False).name
    
    try:
        # 1. Créer la base
        db = create_database(db_file)
        print("✅ Base créée")
        
        # 2. Créer une table
        result = db.execute("""
            CREATE TABLE test_tx (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """)
        
        if not result['success']:
            print(f"❌ Erreur création table: {result.get('error')}")
            return False
        
        print("✅ Table créée")
        
        # 3. Transaction BEGIN
        tx_result = db.begin_transaction()
        print(f"BEGIN: {tx_result}")
        
        if not tx_result['success']:
            print(f"❌ Erreur BEGIN: {tx_result.get('error')}")
            return False
        
        tid = tx_result['tid']
        
        # 4. INSERT
        result = db.execute(
            "INSERT INTO test_tx (name, value) VALUES (?, ?)",
            ("test", 100)
        )
        print(f"INSERT: {result['success']}")
        
        # 5. COMMIT
        commit_result = db.commit_transaction(tid)
        print(f"COMMIT: {commit_result}")
        
        # 6. Vérifier
        check = db.execute("SELECT COUNT(*) as count FROM test_tx")
        if check['success'] and check['rows'][0]['count'] == 1:
            print("✅ Transaction réussie!")
            return True
        else:
            print("❌ Données non persistées")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        try:
            if 'db' in locals():
                db.close()
            os.unlink(db_file)
        except:
            pass

def test_rollback():
    """Teste ROLLBACK"""
    print("\n🧪 Test ROLLBACK")
    
    from gsql.database import create_database
    
    db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False).name
    
    try:
        db = create_database(db_file)
        db.execute("CREATE TABLE test_rb (id INT, data TEXT)")
        
        # Compter avant
        before = db.execute("SELECT COUNT(*) as count FROM test_rb")
        count_before = before['rows'][0]['count']
        
        # Transaction avec rollback
        tx_result = db.begin_transaction()
        tid = tx_result['tid']
        
        db.execute("INSERT INTO test_rb VALUES (1, 'test')")
        
        rollback_result = db.rollback_transaction(tid)
        print(f"ROLLBACK: {rollback_result}")
        
        # Vérifier après
        after = db.execute("SELECT COUNT(*) as count FROM test_rb")
        count_after = after['rows'][0]['count']
        
        if count_before == count_after:
            print("✅ Rollback réussi")
            return True
        else:
            print(f"❌ Rollback échoué: {count_before} -> {count_after}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False
    finally:
        try:
            if 'db' in locals():
                db.close()
            os.unlink(db_file)
        except:
            pass

if __name__ == "__main__":
    print("=" * 60)
    print("Test Transactions GSQL")
    print("=" * 60)
    
    success1 = test_basic_transaction()
    success2 = test_rollback()
    
    print("\n" + "=" * 60)
