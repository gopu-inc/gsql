#!/usr/bin/env python3
"""
APP.PY - À exécuter depuis /root/gsql
"""

# IMPORT LOCAL - depuis le répertoire courant
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

try:
    from gsql.database import Database
    print("✅ Import réussi")
except ImportError as e:
    print(f"❌ Import échoué: {e}")
    print("Assure-toi d'être dans /root/gsql")
    sys.exit(1)

# MAIN CODE
print("🚀 Test GSQL depuis /root/gsql")

# Création simple
db = Database(db_path=":memory:")

# Test execute() - RETOURNE UN DICT!
result = db.execute("SELECT 1 as test, 'hello' as message")

print(f"\n📊 Résultat execute():")
print(f"  Type: {type(result)}")
print(f"  Clés: {list(result.keys()) if isinstance(result, dict) else 'N/A'}")

# Accès correct AU DICT
if isinstance(result, dict):
    if 'rows' in result:
        print(f"\n📋 Lignes trouvées: {len(result['rows'])}")
        for row in result['rows']:
            print(f"  • {row}")
    else:
        print(f"\n🔍 Contenu du dict:")
        for key, value in result.items():
            print(f"  {key}: {value}")

# Test complet
print("\n🧪 Test complet:")

# 1. CREATE
create = db.execute("CREATE TABLE test_table (id INT, name TEXT)")
print(f"CREATE: {create}")

# 2. INSERT  
insert = db.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")
print(f"INSERT: {insert}")

# 3. SELECT
select = db.execute("SELECT * FROM test_table")
print(f"SELECT type: {type(select)}")

if isinstance(select, dict) and 'rows' in select:
    print(f"  {len(select['rows'])} lignes:")
    for row in select['rows']:
        print(f"    ID: {row.get('id')}, Name: {row.get('name')}")

db.close()
print("\n✅ Test terminé")
