<img width="1024" height="1024" alt="IMG_7686" src="https://github.com/user-attachments/assets/9cf47e59-c2f3-49d9-a7c2-82771d5363bd" />

> **GSQL - Système de Base de Données SQL Complet 🚀**
> **powered by gopu.inc,**
[![GSQL-Datastor](https://img.shields.io/badge/GSQL-Datastor-356fa5?style=for-the-badge)](https://github.com/gopu-inc/gsql)
[![PyPI](https://img.shields.io/pypi/v/gsql?style=flat-square&logo=pypi&color=006dad)](https://pypi.org/project/gsql/)
[![Python](https://img.shields.io/pypi/pyversions/gsql?style=flat-square&logo=python&color=3776ab)](https://pypi.org/project/gsql/)
[![Conda Version](https://img.shields.io/conda/v/gopu-inc/gsql?logo=anaconda&color=44a833&style=flat-square)](https://anaconda.org/gopu-inc/gsql)
![Dl](https://anaconda.org/gopu-inc/gsql/badges/downloads.svg)
[![Docker](https://img.shields.io/docker/pulls/ceoseshell/gsql?style=flat-square&logo=docker&color=2496ed)](https://hub.docker.com/r/ceoseshell/gsql)
[![GitHub](https://img.shields.io/github/stars/gopu-inc/gsql?style=flat-square&logo=github&color=f0db4f)](https://github.com/gopu-inc/gsql)
[![License](https://img.shields.io/github/license/gopu-inc/gsql?style=flat-square&logo=opensourceinitiative&color=6cc24a)](LICENSE)

---

## 📋 Table des Matières

1. [🚀 Vue d'Ensemble](#-vue-densemble)
2. [🎯 Fonctionnalités Avancées](#-fonctionnalités-avancées)
3. [📦 Architecture Technique](#-architecture-technique)
4. [⚡ Installation Rapide](#-installation-rapide)
5. [🔧 Utilisation de Base](#-utilisation-de-base)
6. [🤖 Intégration IA & NLP](#-intégration-ia--nlp)
7. [💾 Stockage Multi-Backend](#-stockage-multi-backend)
8. [🔍 Système d'Indexation](#-système-dindexation)
9. [🔧 API Python](#-api-python)
10. [📊 Commandes Référence](#-commandes-référence)
11. [🧪 Tests & Validation](#-tests--validation)
12. [🚀 Déploiement](#-déploiement)
13. [🤝 Contribution](#-contribution)
14. [📄 Licence](#-licence)

---

## 🚀 Vue d'Ensemble

**GSQL** est un système de gestion de base de données relationnelle écrit entièrement en Python. Il combine la simplicité de SQLite avec des fonctionnalités avancées d'intelligence artificielle, de traitement du langage naturel (NLP) et de stockage multi-backend.

> **Notre philosophie :** La puissance du SQL, la simplicité de Python, l'intelligence de l'IA.

### Caractéristiques principales

*   🔹 **Moteur SQL complet** avec support des transactions ACID.
*   🔹 **Shell interactif** avec auto-complétion et coloration syntaxique.
*   🔹 **Traduction naturelle** de langage vers SQL (NLP).
*   🔹 **Stockage flexible** (SQLite, YAML, Mémoire).
*   🔹 **Système d'indexation avancé** (B+Tree).
*   🔹 **Extensibilité** via fonctions Python personnalisées.

| Information | Détail |
| :--- | :--- |
| **Version** | `3.0.0` |
| **Statut** | Production Ready |
| **Base de Données** | SQLite avec extensions GSQL |
| **Langage** | Python 3.8+ |

---

## 🎯 Fonctionnalités Avancées

### ✅ Fonctionnalités Principales
*   **Moteur SQL complet** : Support `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`.
*   **Transactions ACID** : Avec isolation des niveaux pour garantir l'intégrité des données.
*   **Cache intelligent** : Optimisation des requêtes et mise en cache des résultats.
*   **Shell interactif** : Historique des commandes et auto-complétion intuitive.
*   **Gestion des erreurs** : Messages détaillés avec suggestions de correction automatique.
*   **Support multi-backend** : SQLite, YAML, Mémoire.

### 🔧 Extensions GSQL
*   **Fonctions Python** : Exécutez du code Python directement dans vos requêtes SQL.
*   **Indexation B+Tree** : Performances optimisées pour les grands volumes de données.
*   **NLP intégré** : Traduction automatique du langage naturel vers SQL.
*   **Migration automatique** : Outils pour migrer entre différents backends.
*   **Journalisation** : Logs avancés avec niveaux configurables.

### 🧠 Intelligence Intégrée
*   **Traducteur NLP** : *"Montre-moi les 10 meilleurs clients"* → Devient une requête SQL valide.
*   **Détection d'intention** : Comprend le but de la requête utilisateur.
*   **Suggestions** : Basées sur le schéma de la base de données.
*   **Optimisation** : Réécriture automatique des requêtes complexes.

---

## 📦 Architecture Technique

La structure du projet est modulaire et maintenable :

```text
gsql/
├── 📁 core/
│   ├── database.py           # Classe Database principale
│   ├── executor.py           # Exécuteur de requêtes
│   ├── parser.py             # Parseur SQL avancé
│   └── index.py              # Gestionnaire d'index
│
├── 📁 storage/               # Moteurs de stockage
│   ├── storage.py            # Interface de stockage
│   ├── sqlite_storage.py     # Backend SQLite
│   ├── yaml_storage.py       # Backend YAML
│   ├── buffer_pool.py        # Cache de pages
│   └── exceptions.py         # Exceptions spécifiques
│
├── 📁 index/                 # Système d'indexation
│   ├── btree.py              # Implémentation B+Tree
│   └── base_index.py         # Interface d'index
│
├── 📁 nlp/                   # Traitement langage naturel
│   ├── translator.py         # Traducteur NL → SQL
│   └── intent_detector.py    # Détection d'intention
│
├── 📁 functions/             # Fonctions SQL étendues
│   ├── user_functions.py     # Fonctions utilisateur
│   └── builtin_functions.py  # Fonctions intégrées
│
├── 📁 cli/                   # Interface ligne de commande
│   ├── shell.py              # Shell interactif
│   └── commands.py           # Commandes système
│
├── 📁 utils/                 # Utilitaires
│   └── logger.py             # Système de journalisation
│
├── 📁 exceptions/            # Gestion des erreurs
│   └── exceptions.py         # Exceptions personnalisées
│
├── __init__.py               # Initialisation du module
├── __main__.py               # Point d'entrée principal
└── requirements.txt          # Dépendances
```

### Composants Clés

#### 1. Core Database 
📘 GSQL Database - Documentation Technique

🚀 Vue d'ensemble

GSQL v3.0.9 est une couche Python moderne au-dessus de SQLite, conçue pour simplifier l'interaction avec les bases de données tout en ajoutant des fonctionnalités avancées comme le NLP et la gestion automatique des schémas.

✨ Fonctionnalités

✅ Fonctionnalités Stables

· SQL complet : Support de SELECT, INSERT, UPDATE, DELETE, JOIN, etc.
· Interface interactive : Shell avec auto-complétion, historique, couleurs
· Cache intelligent : Cache LRU avec amélioration 10-20x des performances
· Index avancés : Support des B+Tree et Hash Index
· Commandes spéciales : .tables, .stats, .schema, .help
· Contraintes SQL : FOREIGN KEY, CHECK, UNIQUE, NOT NULL
· Fonctions intégrées : 14+ fonctions mathématiques et de texte
· Auto-récupération : Mécanisme de récupération automatique en cas d'erreur

⚠️ Bugs Connus (Workarounds Disponibles)

· Transactions natives : Utiliser db.execute("BEGIN/COMMIT") directement
· Savepoints : Utiliser db.execute("SAVEPOINT/ROLLBACK TO") directement
· DROP TABLE : Éviter sur tables par défaut (users, products, etc.)
· INDEX dans CREATE TABLE : Créer les index séparément

📦 Installation

Installation via pip (recommandé)

```bash
pip install gsql
```

Installation depuis source

```bash
git clone https://github.com/gopu-inc/gsql.git
cd gsql
pip install -e .
```

Dépendances

· Python 3.8+
· SQLite 3.8+
· NLTK (optionnel pour NLP)

🚀 Utilisation Rapide

Mode Interactif

```bash
gsql
# ou
python -m gsql
```

Mode Script

```python
from gsql.database import Database

# Initialisation
db = Database(db_path=":memory:")

# Créer table
db.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
    )
""")

# Insérer données
db.execute("INSERT INTO users (name, email) VALUES (?, ?)", ["Alice", "alice@example.com"])

# Requêter
result = db.execute("SELECT * FROM users", use_cache=True)
print(f"Résultats: {result['rows']}")

# Fermer
db.close()
```

📚 Guide Complet

1. Initialisation

```python
from gsql.database import Database

# Base en mémoire (recommandé pour les tests)
db = Database(db_path=":memory:")

# Base fichier avec options
db = Database(
    db_path="/chemin/vers/ma_base.db",
    base_dir="~/.gsql",          # Répertoire de configuration
    buffer_pool_size=100,        # Taille du cache
    enable_wal=True,             # Mode WAL pour meilleures performances
    auto_recovery=True           # Récupération automatique
)
```

2. Commandes SQL Standards

```python
# CREATE TABLE avec contraintes
db.execute("""
    CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        price REAL CHECK(price > 0),
        category TEXT,
        stock INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# INSERT avec paramètres
db.execute(
    "INSERT INTO products (name, price, category) VALUES (?, ?, ?)",
    ["Laptop", 999.99, "Electronics"]
)

# SELECT avec JOIN
result = db.execute("""
    SELECT p.name, p.price, c.description
    FROM products p
    JOIN categories c ON p.category = c.id
    WHERE p.stock > 0
    ORDER BY p.price DESC
    LIMIT 10
""")

# UPDATE
db.execute("UPDATE products SET price = price * 0.9 WHERE category = 'Electronics'")

# DELETE
db.execute("DELETE FROM products WHERE stock = 0")
```

3. Commandes Spéciales GSQL

```sql
-- Dans le shell interactif ou via execute()
SHOW TABLES;      -- Liste toutes les tables
DESCRIBE users;   -- Affiche le schéma d'une table
STATS;            -- Affiche les statistiques système
VACUUM;           -- Optimise la base de données
HELP;             -- Affiche l'aide
```

4. Transactions (Workaround Nécessaire)

```python
# ❌ NE PAS UTILISER (bug connu)
# db.begin_transaction()
# db.commit_transaction(tid)

# ✅ UTILISER CE WORKAROUND
# Début transaction
db.execute("BEGIN IMMEDIATE TRANSACTION")

try:
    # Opérations transactionnelles
    db.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    
    # Savepoint (workaround)
    db.execute("SAVEPOINT before_validation")
    
    # Validation
    db.execute("INSERT INTO audit_log (action) VALUES ('transfer')")
    
    # Commit
    db.execute("COMMIT")
    print("Transaction réussie")
    
except Exception as e:
    # Rollback en cas d'erreur
    db.execute("ROLLBACK")
    print(f"Transaction échouée: {e}")
```

5. Cache et Performance

```python
# Activation du cache (défaut: True)
result = db.execute("SELECT * FROM large_table WHERE category = ?", 
                    params=["books"], 
                    use_cache=True)

# Performance typique:
# - Première exécution: 100ms
# - Cache hit: 5ms (20x amélioration)

# Désactiver le cache pour requêtes uniques
result = db.execute("DELETE FROM temp_table", use_cache=False)
```

6. Index Avancés

```python
# Création d'index (doit être séparé de CREATE TABLE)
db.execute("CREATE INDEX idx_products_category ON products(category)")
db.execute("CREATE INDEX idx_products_price ON products(price)")

# Index composite
db.execute("CREATE INDEX idx_products_category_price ON products(category, price)")

# Vérifier les index existants
result = db.execute("SELECT name FROM sqlite_master WHERE type='index'")
```

7. Fonctions Intégrées

```python
# Fonctions mathématiques
result = db.execute("SELECT pow(2, 3), sqrt(16), ceil(4.3), floor(4.7)")

# Fonctions texte
result = db.execute("SELECT trim('  hello  '), substr('hello', 2, 3), instr('hello', 'll')")

# Fonctions date
result = db.execute("SELECT date(), time(), datetime(), julianday('now')")
```

🔧 Shell Interactif

Le shell GSQL offre une expérience riche :

```bash
$ gsql
GSQL Interactive Shell v3.0.9
Type 'help' for commands, 'exit' to quit

gsql> .tables
_users     products   orders     logs

gsql> SELECT * FROM users LIMIT 3;
 id | name  | email
----|-------|-----------------
 1  | Alice | alice@test.com
 2  | Bob   | bob@test.com

gsql> .stats
Queries executed: 42
Cache hits: 38 (90.5%)
Active transactions: 0

gsql> .help
Available commands:
  .tables     - List all tables
  .schema     - Show table schema
  .stats      - Show database statistics
  .vacuum     - Optimize database
  .backup     - Create backup
  .help       - Show this help
  .exit/.quit - Exit shell
```

Fonctionnalités du Shell

· Auto-complétion : Tables, colonnes, mots-clés SQL
· Historique : Commandes persistantes entre sessions
· Couleurs : Sortie colorée pour meilleure lisibilité
· Mode multiligne : Support des requêtes multi-lignes
· Export : Résultats formatés en tableau

📊 Performance et Benchmark

Résultats des Tests

Opération Performance Notes
INSERT 3,000 rows/sec En mémoire, batch de 100
SELECT avec cache 5ms 20x plus rapide que sans cache
JOIN complexe < 100ms 1,000 rows, 3 tables
Index lookup < 1ms B+Tree avec 10,000 entries

Optimisations Recommandées

```python
# 1. Utiliser le cache pour requêtes répétitives
db.execute("SELECT * FROM config", use_cache=True)

# 2. Créer des index sur les colonnes fréquemment interrogées
db.execute("CREATE INDEX idx_users_email ON users(email)")

# 3. Utiliser des transactions pour INSERT multiples
db.execute("BEGIN TRANSACTION")
for i in range(1000):
    db.execute("INSERT INTO data VALUES (?, ?)", [i, f"value_{i}"])
db.execute("COMMIT")

# 4. Activer WAL pour meilleure concurrence
db = Database(enable_wal=True)
```

🐛 Bugs Connus et Solutions

Bug 1: Transactions Natives

Problème: db.begin_transaction() ne démarre pas de transaction SQLite
Solution:

```python
# Workaround
db.execute("BEGIN TRANSACTION")
# ... opérations ...
db.execute("COMMIT")
```

Bug 2: Savepoints

Problème: Savepoints créés mais non reconnus par SQLite
Solution:

```python
# Workaround
db.execute("SAVEPOINT sp1")
# ... opérations ...
db.execute("ROLLBACK TO SAVEPOINT sp1")
```

Bug 3: DROP TABLE sur Tables par Défaut

Problème: DROP TABLE IF EXISTS users échoue même si table existe
Solution: Éviter de supprimer les tables par défaut

Bug 4: INDEX dans CREATE TABLE

Problème: Syntaxe CREATE TABLE ... INDEX non supportée
Solution: Créer les index séparément

```python
# ❌ Incorrect
CREATE TABLE t (id INT, INDEX idx(id))

# ✅ Correct
CREATE TABLE t (id INT)
CREATE INDEX idx ON t(id)
```

🔮 Améliorations Futures

Court Terme (v3.1)

· Correction des bugs de transaction
· Support complet des savepoints
· Amélioration de DROP TABLE
· Documentation API complète
· Tests unitaires étendus

Moyen Terme (v3.5)

· Support PostgreSQL en plus de SQLite
· Réplication maître-esclave
· Backup/restore automatisés
· Monitoring intégré
· Interface web admin

Long Terme (v4.0)

· Support du clustering
· Sharding automatique
· Compression des données
· Chiffrement transparent
· Machine Learning intégré

🧪 Tests et Qualité

Exécuter les Tests

```bash
# Tests unitaires
python -m pytest tests/

# Tests de performance
python tests/performance.py

# Tests de charge
python tests/stress_test.py

# Couverture de code
coverage run -m pytest tests/
coverage report
```

Standards de Code

```bash
# Vérification PEP8
flake8 gsql/

# Vérification types
mypy gsql/

# Tests de sécurité
bandit -r gsql/
```

🤝 Contribution

Les contributions sont les bienvenues ! Voici comment contribuer :

1. Signaler un bug : Ouvrir une issue avec un cas de test reproductible
2. Proposer une fonctionnalité : Décrire le cas d'usage et l'API proposée
3. Soumettre un correctif : Pull request avec tests et documentation
4. Améliorer la documentation : Corrections, traductions, exemples

Structure du Projet

```
gsql/
├── __init__.py              # Point d'entrée
├── database.py              # Classe Database principale
├── storage.py               # Moteur de stockage SQLite
├── executor.py              # Exécuteur de requêtes
├── parser.py                # Parser SQL
├── exceptions.py            # Exceptions personnalisées
├── index.py                 # Index B+Tree et Hash
├── btree.py                 # Implémentation B+Tree
├── functions/               # Fonctions utilisateur
├── nlp/                     # Traitement langage naturel
├── storage/                 # Modules de stockage
├── tests/                   # Tests
└── cli.py                   Interface en ligne de commande
```

🙏 Remerciements

· SQLite : Pour le moteur de base de données robuste
· NLTK : Pour le traitement du langage naturel
· La communauté Python : Pour les outils et bibliothèques

---

GSQL - La puissance de SQL, la simplicité du langage naturel

Dernière mise à jour : v3.0.9 - Décembre 2025

---

## 🤝 Contribution

Nous accueillons avec plaisir les contributions !

### Guide de Contribution
1.  **Fork** le dépôt.
2.  **Clone** votre fork : `git clone https://github.com/votre-username/gsql.git`
3.  **Branche** : `git checkout -b feature/ma-fonctionnalité`
4.  **Code & Test** : `pytest tests/`
5.  **Commit** : `git commit -m "Ajout de ma fonctionnalité"`
6.  **Push** & **Pull Request**.

### Normes de Code
*   **PEP 8** : Respectez les conventions Python.
*   **Docstrings** : Format Google.
*   **Typing** : Type hints Python 3.8+ requis.

---


## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright © 2025 Gopu Inc. All rights reserved.


### 📞 Support & Contact

*   **Documentation**
*   [Docs pages](https://gopu-inc.github.io/gsql/)
*   **Issues** : [GitHub Issues](https://github.com/gopu-inc/gsql/issues)
*   **Email** : support@gopu-inc.com

---

### 🌟 Étoilez-nous !

Si GSQL vous est utile, n'hésitez pas à donner une étoile ⭐ sur GitHub !

```bash
git clone https://github.com/gopu-inc/gsql.git
```

**GSQL - La puissance de SQL avec la simplicité de Python et l'intelligence de l'IA.** 🚀
```
