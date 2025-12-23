#!/usr/bin/env python3
"""
FUNCTION.PY CORRIGÉ - Comprend la structure réelle de GSQL
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from gsql.database import Database

print("✅ Import réussi")
print("🚀 Test GSQL avec structure correcte")

db = Database(db_path=":memory:")

# 1. TEST STRUCTURE
print("\n📊 STRUCTURE execute():")
result = db.execute("SELECT 1 as test, 'hello' as message")

print(f"Clés: {list(result.keys())}")
print(f"Type: {result['type']}")
print(f"Success: {result['success']}")
print(f"Colonnes: {result['columns']}")  # ['test', 'message']
print(f"Rows (tuples): {result['rows']}")  # [(1, 'hello')]

# 2. ACCÈS CORRECT AUX DONNÉES
print("\n🎯 ACCÈS AUX DONNÉES:")
if result['success'] and result['rows']:
    # Les rows sont des TUPLES, utiliser les indices
    for row in result['rows']:
        # row[0] = première colonne, row[1] = deuxième, etc.
        print(f"  Tuple: {row}")
        print(f"    test={row[0]}, message={row[1]}")
        
        # OU utiliser zip avec les noms de colonnes
        for col_name, value in zip(result['columns'], row):
            print(f"    {col_name}: {value}")

# 3. FONCTION UTILE POUR CONVERTIR EN DICT
def rows_to_dicts(result):
    """Convertit les tuples rows en liste de dicts"""
    if not result['success'] or not result['rows']:
        return []
    
    dicts = []
    for row_tuple in result['rows']:
        row_dict = {}
        for i, col_name in enumerate(result['columns']):
            row_dict[col_name] = row_tuple[i]
        dicts.append(row_dict)
    
    return dicts

# 4. TEST COMPLET AVEC LA FONCTION
print("\n🧪 TEST COMPLET AVEC CONVERSION:")

# CREATE
create = db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
print(f"CREATE: {create['success']}")

# INSERT
insert = db.execute("INSERT INTO users (name, age) VALUES ('Alice', 25), ('Bob', 30)")
print(f"INSERT: {insert['success']}, {insert['rows_affected']} lignes")

# SELECT avec conversion
select = db.execute("SELECT * FROM users ORDER BY name")
print(f"\nSELECT réussie: {select['success']}")
print(f"Colonnes: {select['columns']}")
print(f"Nombre de lignes: {select['count']}")

# Conversion en dicts
users = rows_to_dicts(select)
print(f"\n👥 Utilisateurs (format dict):")
for user in users:
    print(f"  • ID: {user.get('id')}, Nom: {user.get('name')}, Âge: {user.get('age')}")

# 5. AUTRE FAÇON: Parcourir directement
print("\n🔧 PARCOURS DIRECT DES TUPLES:")
for row_tuple in select['rows']:
    # row_tuple[0] = id, row_tuple[1] = name, row_tuple[2] = age
    print(f"  Tuple: id={row_tuple[0]}, name='{row_tuple[1]}', age={row_tuple[2]}")

# 6. FONCTION POUR AFFICHAGE TABULAIRE
def print_table(result, max_rows=10):
    """Affiche les résultats en tableau"""
    if not result['success']:
        print("❌ Requête échouée")
        return
    
    print(f"\n📋 {result['type'].upper()} - {result['count']} ligne(s)")
    
    # En-têtes
    headers = result['columns']
    print(" | ".join(headers))
    print("-" * (len(headers) * 10))
    
    # Données
    for i, row in enumerate(result['rows'][:max_rows]):
        print(" | ".join(str(cell) for cell in row))
    
    if result['count'] > max_rows:
        print(f"... et {result['count'] - max_rows} lignes supplémentaires")

# 7. TEST AVEC PLUS DE DONNÉES
db.execute("INSERT INTO users (name, age) VALUES ('Charlie', 22), ('Diana', 35), ('Eve', 28)")

select_all = db.execute("SELECT id, name, age FROM users ORDER BY age DESC")
print_table(select_all)

# 8. REQUÊTES COMPLEXES
print("\n📊 STATISTIQUES:")
stats = db.execute("""
    SELECT 
        COUNT(*) as total,
        AVG(age) as moyenne_age,
        MIN(age) as age_min,
        MAX(age) as age_max
    FROM users
""")

stats_dicts = rows_to_dicts(stats)
if stats_dicts:
    stats_row = stats_dicts[0]
    print(f"  Total: {stats_row['total']} utilisateurs")
    print(f"  Âge moyen: {stats_row['moyenne_age']:.1f} ans")
    print(f"  Âge min: {stats_row['age_min']} ans")
    print(f"  Âge max: {stats_row['age_max']} ans")

db.close()
print("\n✅ Test terminé avec succès")
