def test_manual_sql_commands():
    """Test avec commandes SQL manuelles - CORRIGÉ"""
    print("\n" + "="*60)
    print("🧪 Test Commandes SQL Manuelles CORRIGÉ")
    print("="*60)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path, create_default_tables=False)
        
        # Créer une table
        db.execute("CREATE TABLE test_manual (id INTEGER PRIMARY KEY, value TEXT)")
        
        print("\n1. Insert initial (hors transaction):")
        result = db.execute("INSERT INTO test_manual (value) VALUES ('initial')")
        print(f"   Result: {result.get('success')}")
        
        print("\n2. Utiliser BEGIN SQL direct:")
        result = db.execute("BEGIN")
        print(f"   Result: {result}")
        
        print("\n3. Insert dans transaction:")
        result = db.execute("INSERT INTO test_manual (value) VALUES ('in_transaction')")
        print(f"   Result: {result.get('success')}")
        
        print("\n4. Vérifier dans transaction (avec execute_in_transaction):")
        # Utiliser execute_in_transaction pour voir dans la transaction
        result = db.execute_in_transaction("SELECT COUNT(*) as count FROM test_manual")
        rows_in_tx = result.get('rows', [{}])[0].get('count', 0) if result.get('rows') else 0
        print(f"   Lignes dans transaction: {rows_in_tx}")
        
        print("\n5. Vérifier hors transaction (avec execute normal):")
        result = db.execute("SELECT COUNT(*) as count FROM test_manual")
        rows_outside = result.get('rows', [{}])[0].get('count', 0) if result.get('rows') else 0
        print(f"   Lignes hors transaction (devrait être 1): {rows_outside}")
        
        print("\n6. ROLLBACK SQL direct:")
        result = db.execute("ROLLBACK")
        print(f"   Result: {result}")
        
        print("\n7. Vérifier après ROLLBACK (hors transaction):")
        # Désactiver le cache pour cette requête
        result = db.execute("SELECT COUNT(*) as count FROM test_manual", use_cache=False)
        rows_after = result.get('rows', [{}])[0].get('count', 0) if result.get('rows') else 0
        print(f"   Lignes: {rows_after}")
        
        print("\n8. Vérifier le contenu:")
        result = db.execute("SELECT value FROM test_manual ORDER BY id")
        rows = result.get('rows', [])
        print(f"   Contenu: {rows}")
        
        success = rows_after == 1 and len(rows) == 1 and rows[0].get('value') == 'initial'
        print(f"\n✅ Commandes SQL directes: {success} (lignes: {rows_after})")
        
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