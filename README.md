<img width="280" height="280" alt="GSQL Logo" src="https://github.com/user-attachments/assets/9cf47e59-c2f3-49d9-a7c2-82771d5363bd" />

# GSQL - Une interface Python avancée pour SQLite 🔧

> **Développé par gopu.inc | Statut : Bêta Active - En développement**

[![PyPI Version](https://img.shields.io/pypi/v/gsql?style=flat-square&logo=pypi&color=006dad)](https://pypi.org/project/gsql/)
[![Python Versions](https://img.shields.io/pypi/pyversions/gsql?style=flat-square&logo=python&color=3776ab)](https://pypi.org/project/gsql/)
[![Conda Version](https://img.shields.io/conda/v/gopu-inc/gsql?logo=anaconda&color=44a833&style=flat-square)](https://anaconda.org/gopu-inc/gsql)
[![Downloads](https://static.pepy.tech/personalized-badge/gsql?period=total&units=international_system&left_color=black&right_color=blue&left_text=PyPI%20Downloads)](https://pepy.tech/project/gsql)
[![Docker Pulls](https://img.shields.io/docker/pulls/ceoseshell/gsql?style=flat-square&logo=docker&color=2496ed)](https://hub.docker.com/r/ceoseshell/gsql)
[![License](https://img.shields.io/github/license/gopu-inc/gsql?style=flat-square&logo=opensourceinitiative&color=6cc24a)](LICENSE)

## 🚨 État du Projet & Transparence

**GSQL est un projet en développement actif (version bêta).** Il n'est pas encore prêt pour une utilisation en production critique.

**Ce que GSQL est VRAIMENT :**
- ✅ **Une surcouche Python puissante pour SQLite** avec un shell interactif, un cache et des outils de productivité.
- ✅ Un projet open-source qui évolue rapidement grâce à la communauté.

**Bugs & Limitations Actuelles (à connaître avant d'utiliser) :**
- 🔸 **Transactions** : L'API transactionnelle native (`db.begin_transaction()`) a des bugs. **Il faut utiliser les commandes SQL brutes `BEGIN`/`COMMIT`** (un workaround est fourni ci-dessous).
- 🔸 **Guillemets** : Certains caractères dans les chaînes peuvent causer des problèmes de parsing dans le shell interactif.
- 🔸 **Fonctionnalités expérimentales** : Les modules NLP (`gsql.nlp`) et les backends de stockage alternatifs (YAML, mémoire) sont en prototype et non stabilisés.

**Notre philosophie :** Apporter la productivité du Python et la clarté d'une interface moderne à la robustesse de SQLite.

---

## 📦 Installation Rapide

### Via pip (recommandé pour tester)
```bash
pip install gsql
```

Via Conda (à partir du canal gopu-inc)

```bash
conda install -c gopu-inc gsql
```

Depuis les sources (pour les contributeurs)

```bash
git clone https://github.com/gopu-inc/gsql.git
cd gsql
pip install -e .
```

Avec Docker

```bash
docker pull ceoseshell/gsql:latest
docker run -it ceoseshell/gsql --help
```

---

🚀 Utilisation en 30 secondes

1. Lancer le Shell Interactif (CLI)

C'est le moyen le plus simple de découvrir GSQL.

```bash
gsql
# > Bienvenue dans le shell GSQL. Tapez 'help' pour les commandes.
# gsql> .tables
# gsql> SELECT * FROM sqlite_master;
```

2. Utilisation dans un Script Python

Voici comment intégrer GSQL correctement dans votre code aujourd'hui.

```python
from gsql.database import Database

# 1. INITIALISATION : Créez une instance de la base de données.
#    Pour les tests, utilisez ':memory:'. Pour un fichier, donnez un chemin.
db = Database(db_path=":memory:", enable_wal=True, auto_recovery=True)

# 2. EXÉCUTION DE REQUÊTES : Utilisez la méthode .execute()
# Créer une table
db.execute("""
    CREATE IF NOT EXISTS EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT
    )
""")

# Insérer des données (toujours avec des paramètres pour la sécurité)
db.execute(
    "INSERT INTO users (username, email) VALUES (?, ?)",
    ["jdoe", "john.doe@example.com"]
)

# Sélectionner des données
result = db.execute("SELECT * FROM users", use_cache=True)
print(f"Trouvé {result['count']} utilisateur(s).")
for row in result['rows']:
    print(f"- {row['id']}: {row['username']}")

# 3. GESTION DES TRANSACTIONS : UTILISEZ CE WORKAROUND
# ⚠️ N'utilisez PAS db.begin_transaction(). Utilisez les commandes SQL directes.
try:
    # Début de la transaction
    db.execute("BEGIN IMMEDIATE TRANSACTION")

    # Vos opérations atomiques
    db.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")

    # Si tout est bon, validez
    db.execute("COMMIT")
    print("Virement effectué avec succès.")

except Exception as e:
    # En cas d'erreur, annulez tout
    db.execute("ROLLBACK")
    print(f"Échec du virement : {e}")

# 4. UTILISER LES COMMANDES SPÉCIALES GSQL
# Ces commandes fonctionnent à la fois dans le shell ET via .execute().
stats = db.execute("STATS")  # Récupère des statistiques d'utilisation
print(stats.get('message'))

# 5. FERMETURE PROPRE
db.close()
```

---

🛠️ Fonctionnalités Stables et Prêtes à l'Emploi

✅ Fonctionnalités Principales Totalement Opérationnelles

· Shell Interactif : Auto-complétion, historique, coloration syntaxique, affichage tabulaire.
· Cache Intelligent (LRU) : Accélère les requêtes SELECT répétitives jusqu'à 20x.
· Commande Spéciales Intégrées :
  ```sql
  .tables                 -- Liste les tables
  .schema <table>         -- Montre la structure d'une table
  STATS;                  -- Affiche les stats de performance et de cache
  VACUUM;                 -- Optimise la base de données
  HELP;                   -- Affiche l'aide
  ```
· Support SQL Complet : Tout ce que SQLite supporte (SELECT, INSERT, JOIN, etc.) passe par GSQL.
· Gestion des Erreurs : Messages d'erreur détaillés avec suivi de pile.

🔧 Fonctionnalités en Développement/Expérimentales

· Module NLP (gsql.nlp) : Traduction du langage naturel en SQL. Instable.
· Autres Backends : Stockage YAML ou en mémoire. Non recommandé pour les données importantes.

---

📁 Structure du Projet (Pour Contributeurs)

```
gsql/
├── gsql/
│   ├── __init__.py              # Point d'entrée principal
│   ├── database.py              # CLASSE PRINCIPALE `Database`
│   ├── storage.py               # Abstraction du stockage (SQLite)
│   ├── executor.py              # Exécuteur et cache des requêtes
│   ├── cli.py                   # Interface du Shell Interactif
│   └── exceptions.py            # Exceptions personnalisées
├── tests/                       # Suite de tests
├── meta.yaml                    # Recette de construction Conda
├── setup.py                     # Configuration pour pip
├── Dockerfile                   # Configuration pour le conteneur
└── README.md                    # Ce fichier
```

Classe Principale : gsql.database.Database
Point d'Entrée CLI : gsql.cli.main() (accessible via la commande gsql)

---

🧪 Exécuter les Tests et Contribuer

Nous avons besoin de votre aide pour stabiliser le projet !

```bash
# 1. Clonez et installez en mode développement
git clone https://github.com/gopu-inc/gsql.git
cd gsql
pip install -e .[dev]  # Installe les dépendances de test

# 2. Exécutez la suite de tests existante
pytest tests/ -v

# 3. Vérifiez la couverture de code et le style
coverage run -m pytest tests/
coverage report
flake8 gsql/  # Vérification du style PEP8
```

Comment contribuer ?

1. Signaler un bug : Ouvrez une issue sur GitHub en décrivant précisément le problème, la version de GSQL, et un exemple de code minimal pour le reproduire.
2. Proposer une amélioration : Discutez-en d'abord dans une issue.
3. Soumettre une correction (PR) : Fork, branche, code, tests, pull request.

---

❓ FAQ & Dépannage

Q : db.begin_transaction() ne marche pas, que faire ?
R : C'est un bug connu. Utilisez toujours db.execute("BEGIN TRANSACTION") et db.execute("COMMIT") ou db.execute("ROLLBACK"). Voir l'exemple de code plus haut.

Q : Le shell plante avec une erreur de parsing ?
R : Évitez les guillemets complexes ou les caractères spéciaux dans les chaînes en mode interactif pour l'instant. Utilisez un script Python pour les requêtes complexes.

Q : Comment migrer de SQLite brut vers GSQL ?
R : Pointez simplement le paramètre db_path vers votre fichier .db SQLite existant. GSQL le lira directement.

---

📄 Licence

Ce projet est publié sous la licence MIT. Voir le fichier LICENSE pour plus de détails.

GSQL © 2025 Gopu Inc. | Apportons une interface moderne à SQLite.
