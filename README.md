<img width="280" height="280" alt="GSQL Logo" src="https://github.com/user-attachments/assets/9cf47e59-c2f3-49d9-a7c2-82771d5363bd" />

# GSQL - Une interface Python avancée pour SQLite 🔧

> **Développé par gopu.inc | Statut : Bêta Active - En développement**
<!-- Badge animé type GitHub -->
[![New Release](https://img.shields.io/badge/🎉_New_Release_v3.9.7-FF4081?style=for-the-badge&logo=gift&logoColor=white&labelColor=1a1a1a&color=FF4081)](https://gopu-inc.github.io)
[![GSQL Powered](https://img.shields.io/badge/🛠️_GSQL_Powered-4169E1?style=for-the-badge&logo=database&logoColor=white&labelColor=0A2540&color=4169E1)](https://gopu-inc.github.io/gsql)
[![Open Source](https://img.shields.io/badge/GP_Open_Source-6F42C1?style=for-the-badge&logo=opensourceinitiative&logoColor=white&labelColor=1a1a1a&color=6F42C1)](https://github.com/gopu-inc)
[![Stars](https://img.shields.io/badge/⭐_Stars-100+-FFD700?style=for-the-badge&logo=github&logoColor=black&labelColor=1a1a1a&color=FFD700&animation=glow)](https://github.com/gopu-inc/gsql)
[![GOPU.inc](https://img.shields.io/badge/GP_GOPU.inc-0A2540?style=for-the-badge&logo=starship&logoColor=white&labelColor=0A2540&color=FF6B35)](https://gopu-inc.github.io)
[![WhatsApp](https://img.shields.io/badge/Whatsapp-chain-25D366?logo=whatsapp&logoColor=white)](https://chat.whatsapp.com/F7NGsDVYDevEISVKTqpGZ1)
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
GSQL - Une Interface Python Moderne pour SQLite

🎯 Introduction

GSQL est une surcouche Python avancée pour SQLite qui transforme l'expérience de travail avec les bases de données SQLite. Elle ajoute des fonctionnalités modernes tout en conservant la robustesse et la simplicité de SQLite.

Pourquoi GSQL existe ?
Parce que SQLite est incroyablement puissant, mais son interface en Python manque parfois de fonctionnalités modernes. GSQL comble ce vide en ajoutant :

· Un shell interactif avec auto-complétion
· Un cache intelligent pour les performances
· Des commandes spéciales pour la gestion quotidienne
· Une meilleure gestion des erreurs

📊 Statut Actuel du Projet

Version 3.9.7 (Beta Active)

⚠️ Important : GSQL est en développement actif et présente encore des bugs connus. Il n'est pas recommandé pour les environnements de production critiques.

Bugs Connus et Workarounds

1. API Transactionnelle : db.begin_transaction() ne fonctionne pas correctement
   · Solution : Utiliser les commandes SQL natives : db.execute("BEGIN TRANSACTION")
2. Parsing des guillemets : Problèmes avec les caractères spéciaux dans le shell
   · Solution : Préférer les scripts Python pour les requêtes complexes
3. Backends expérimentaux : Les modules NLP et stockage alternatif sont instables
   · Solution : S'en tenir au backend SQLite principal

🚀 Fonctionnalités Clés

✅ Fonctionnalités Stables

· Shell interactif : Auto-complétion, historique, coloration syntaxique
· Cache LRU : Améliore les performances jusqu'à 20x pour les requêtes répétitives
· Commandes spéciales : .tables, .schema, STATS, VACUUM, HELP
· Compatibilité totale : Utilisez vos bases SQLite existantes sans modification
· Gestion d'erreurs avancée : Messages clairs avec suggestions

🔧 Fonctionnalités Expérimentales (Beta)

· Module NLP : Traduction langage naturel → SQL
· Backends alternatifs : YAML, mémoire (non recommandés pour production)
· Migration automatique entre backends

🛠️ Architecture Technique

```
gsql/
├── database.py          # Classe Database principale
├── storage.py           # Abstraction du stockage SQLite
├── executor.py          # Exécuteur et cache des requêtes
├── cli.py               # Interface en ligne de commande
├── parser.py            # Parseur SQL amélioré
├── exceptions.py        # Exceptions personnalisées
├── functions/           # Fonctions utilisateur
├── nlp/                 # Traitement langage naturel (beta)
└── tests/               # Suite de tests complète
```

📦 Installation Rapide

```bash
# Installation standard
pip install gsql

# Mode shell interactif
gsql

# Dans un script Python
from gsql.database import Database
db = Database(db_path=":memory:")
```

💡 Cas d'Utilisation

Pour les Développeurs

· Prototypage rapide avec base en mémoire
· Interface CLI pour explorer les données
· Gestion simplifiée des schémas

Pour les Administrateurs

· Monitoring avec commande STATS
· Optimisation automatique avec VACUUM
· Sauvegarde/restauration intégrées

Pour les Projets en Production

· Cache intelligent pour les performances
· Gestion robuste des erreurs
· Compatibilité descendante avec SQLite

🔍 Comparaison avec SQLite Brut

Fonctionnalité SQLite Brut GSQL
Shell interactif Basique Avancé avec auto-complétion
Cache de requêtes Manuel Automatique (LRU)
Gestion des erreurs Messages techniques Messages clairs avec solutions
Commandes spéciales Non Oui (.tables, STATS, etc.)
Performance SELECT Standard Jusqu'à 20x plus rapide (cache)
Courbe d'apprentissage Raide Progressive

🚧 Limitations Actuelles

1. Pas de transactions natives (utilisation des commandes SQL brutes requise)
2. Parser limité pour les requêtes complexes dans le shell
3. Modules NLP encore expérimentaux
4. Documentation en cours d'amélioration

🌟 Feuille de Route

Court Terme (v3.10)

· Correction des bugs transactionnels
· Amélioration du parser SQL
· Documentation complète

Moyen Terme (v4.0)

· Support PostgreSQL
· Interface web d'administration
· Réplication simple

🤝 Contribuer

GSQL est un projet open source qui a besoin de votre aide !

Bugs prioritaires à corriger :

1. API transactionnelle (begin_transaction())
2. Parser des guillemets dans le shell
3. Problèmes de cache après DROP TABLE

Comment contribuer :

```bash
git clone https://github.com/gopu-inc/gsql.git
cd gsql
pip install -e .[dev]
pytest tests/  # Exécuter les tests
```

📚 Ressources

· Documentation : GitHub Wiki
· Issues : GitHub Issues
· Code Source : GitHub Repository
· Package : PyPI

💬 Discussion

Questions fréquentes :

Q : Puis-je utiliser GSQL en production ?
R : Pas encore pour les cas critiques. Utilisez-le pour le développement et les tests.

Q : Comment gérer les transactions ?
R : Utilisez le workaround : db.execute("BEGIN TRANSACTION") au lieu de db.begin_transaction()

Q : GSQL remplace-t-il SQLite ?
R : Non, GSQL s'appuie sur SQLite et l'améliore avec des fonctionnalités supplémentaires.

[![Documentation](https://img.shields.io/badge/docs-gsql-blue)](https://gopu-inc.github.io/gsql/#home)
[![WhatsApp](https://img.shields.io/badge/WhatsApp-GOPU.inc-25D366?logo=whatsapp&logoColor=white)](https://chat.whatsapp.com/F7NGsDVYDevEISVKTqpGZ1)


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
