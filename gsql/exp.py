# Utilisation basique
from gsql import GSQL

# Connexion à une base
db = GSQL("ma_base.db")

# Via SQL
db.execute("CREATE TABLE users (id INT, name TEXT, age INT)")
db.execute("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)")
results = db.query("SELECT * FROM users WHERE age > 20")

# Via API Python
db.create_table("products", [
    {"name": "id", "type": "INTEGER", "constraints": ["PRIMARY_KEY"]},
    {"name": "name", "type": "TEXT"},
    {"name": "price", "type": "FLOAT"}
])

db.insert("products", {"id": 1, "name": "Laptop", "price": 999.99})

# Opérations administratives
db.backup("sauvegarde_avant_migration")
db.vacuum()  # Optimisation
stats = db.get_stats()
integrity = db.check_integrity()

# Fermeture propre
db.close()

# Avec context manager
with GSQL("test.db") as db:
    db.execute("SELECT * FROM users")
    # Auto-fermeture à la sortie du bloc
