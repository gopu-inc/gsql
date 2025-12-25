from gsql.storage.sqlite_storage import create_storage

# Test rapide
storage = create_storage()

# 1. Créer une table
result = storage.execute("CREATE TABLE test_fix (id INT PRIMARY KEY, name TEXT)")
print("CREATE:", result['success'])

# 2. Insérer des données  
result = storage.execute("INSERT INTO test_fix VALUES (1, 'Test'), (2, 'Demo')")
print("INSERT:", result['success'], "ID:", result.get('lastrowid'))

# 3. Sélectionner
result = storage.execute("SELECT * FROM test_fix")
print("SELECT:", result['success'], "Rows:", result.get('count'))

# 4. Décrire la table (BUG FIXÉ)
schema = storage.get_table_schema("test_fix")
print("DESCRIBE: Columns:", len(schema['columns']) if schema else 0)

# 5. Transactions
tid = storage.begin_transaction()
print("BEGIN TX:", tid)

result = storage.execute("UPDATE test_fix SET name = 'Updated' WHERE id = 1")
print("UPDATE in TX:", result['success'])

storage.commit_transaction(tid)
print("COMMIT TX:", tid)

storage.close()
