<img width="1024" height="1024" alt="IMG_7686" src="https://github.com/user-attachments/assets/9cf47e59-c2f3-49d9-a7c2-82771d5363bd" />

> **GSQL - Système de Base de Données SQL Complet 🚀**
> **powered by gopu.inc,**


[![PyPI](https://img.shields.io/pypi/v/gsql?style=flat-square&logo=pypi&color=006dad)](https://pypi.org/project/gsql/)
[![Python](https://img.shields.io/pypi/pyversions/gsql?style=flat-square&logo=python&color=3776ab)](https://pypi.org/project/gsql/)
[![Conda](https://img.shields.io/conda/v/conda-forge/gsql?style=flat-square&logo=anaconda&color=44a833)](https://anaconda.org/conda-forge/gsql)
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

#### 1. Core Database (`database.py`)
Le chef d'orchestre du système.

```python
class Database:
    """Point d'entrée principal du système GSQL"""
    
    def __init__(self, storage_backend='sqlite', config=None):
        self.storage = self._init_storage(storage_backend, config)
        self.executor = QueryExecutor(self.storage)
        self.parser = SQLParser()
        self.index_manager = IndexManager()
        self.cache = QueryCache()
        self.logger = GLogger()
    
    def execute(self, query: str, params=None, many=False):
        """Exécute une requête SQL"""
        # 1. Parsing
        parsed = self.parser.parse(query)
        
        # 2. Vérification du cache
        if cached := self.cache.get(query, params):
            return cached
        
        # 3. Exécution
        result = self.executor.execute(parsed, params, many)
        
        # 4. Mise en cache
        self.cache.set(query, params, result)
        
        return result
```

#### 2. SQL Parser (`parser.py`)
*   Parseur SQL récursif descendant.
*   Support des clauses complexes (JOIN, GROUP BY, HAVING).
*   Validation syntaxique et sémantique.
*   Génération d'AST (Abstract Syntax Tree).

#### 3. Query Executor (`executor.py`)

```python
class QueryExecutor:
    """Exécuteur de requêtes optimisé"""
    
    def execute(self, parsed_query, params=None, many=False):
        query_type = parsed_query['type']
        
        if query_type == 'select':
            return self._execute_select(parsed_query, params)
        elif query_type == 'insert':
            return self._execute_insert(parsed_query, params, many)
        elif query_type == 'update':
            return self._execute_update(parsed_query, params)
        elif query_type == 'delete':
            return self._execute_delete(parsed_query, params)
        elif query_type == 'create_table':
            return self._execute_create_table(parsed_query)
```

#### 4. Storage System (`storage/`)
*   **Interface unifiée** : Abstraction commune pour tous les backends.
*   **SQLiteStorage** : Backend SQLite haute performance.
*   **YAMLStorage** : Stockage lisible pour développement et tests.
*   **BufferPool** : Cache LRU pour les pages de données.

#### 5. Index System (`index.py`, `btree.py`)

```python
class BTree:
    """Implémentation B+Tree pour indexation rapide"""
    
    def __init__(self, order=100):
        self.order = order
        self.root = BTreeNode(is_leaf=True)
    
    def insert(self, key, value):
        """Insertion optimisée avec rééquilibrage automatique"""
        if self.root.is_full():
            new_root = BTreeNode()
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
        self._insert_non_full(self.root, key, value)
```

#### 6. NLP Translator (`nlp/translator.py`)

```python
class NLTranslator:
    """Traducteur de langage naturel vers SQL"""
    
    def translate(self, natural_language: str) -> str:
        """
        Traduit une phrase en langage naturel en requête SQL
        
        Exemple:
        "Montre-moi les 10 meilleurs clients" →
        "SELECT * FROM clients ORDER BY score DESC LIMIT 10"
        """
        intent = self.detector.detect_intent(natural_language)
        entities = self.extractor.extract_entities(natural_language)
        sql = self.generator.generate_sql(intent, entities)
        return sql
```

---

## ⚡ Installation Rapide

### Méthode 1: Installation via pip

```bash
# Clonez le dépôt
git clone https://github.com/gopu-inc/gsql.git
cd gsql

# Installation des dépendances
pip install -r requirements.txt

# Installation en mode développement
pip install -e .

# Vérification
gsql --version
```

### Méthode 2: Installation directe

```bash
# Installation depuis GitHub
pip install git+https://github.com/gopu-inc/gsql.git

# Ou avec spécification de version
pip install git+https://github.com/gopu-inc/gsql.git@main
```

### Méthode 3: Docker

```bash
# Construction de l'image
docker build -t gsql .

# Exécution avec volume persistant
docker run -it -v $(pwd)/data:/data gsql

# Exécution avec configuration personnalisée
docker run -it -v $(pwd)/config:/config gsql --config /config/gsql.yaml
```

### Dépendances (`requirements.txt`)
*   `sqlite3>=3.35.0`
*   `PyYAML>=6.0`
*   `click>=8.0.0`
*   `colorama>=0.4.4`
*   `prompt-toolkit>=3.0.0`
*   `nltk>=3.6.0`
*   `pandas>=1.3.0`
*   `numpy>=1.21.0`

---

## 🔧 Utilisation de Base

### 1. Mode Shell Interactif

```bash
# Lancement du shell
gsql

# Ou avec fichier de base spécifique
gsql --database ma_base.db

# Mode verbose pour débogage
gsql --verbose
```

**Dans le shell GSQL :**

```sql
-- Création d'une table
CREATE TABLE clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL,
    email TEXT UNIQUE,
    solde DECIMAL(10, 2),
    actif BOOLEAN DEFAULT TRUE,
    inscription DATE DEFAULT CURRENT_DATE
);

-- Insertion de données
INSERT INTO clients (nom, email, solde) 
VALUES 
    ('Alice Dupont', 'alice@example.com', 1500.50),
    ('Bob Martin', 'bob@example.com', 2300.75);

-- Requête avec NLP (Commande magique NL>)
NL> Montre-moi les clients avec plus de 2000 euros
-- Résultat automatique :
-- SELECT * FROM clients WHERE solde > 2000;

-- Indexation
CREATE INDEX idx_solde ON clients(solde);

-- Vérification des performances
EXPLAIN SELECT * FROM clients WHERE solde > 2000;
```

### 2. Utilisation Programmatique

```python
from gsql import Database, GSQLException

# Connexion à une base
db = Database('sqlite', {'path': 'ma_base.db'})

# Exécution de requêtes
try:
    # Création de table
    db.execute("""
        CREATE TABLE produits (
            id INTEGER PRIMARY KEY,
            nom TEXT NOT NULL,
            prix DECIMAL(10, 2),
            stock INTEGER
        )
    """)
    
    # Insertion multiple
    produits = [
        ('Laptop', 999.99, 50),
        ('Smartphone', 699.99, 150),
        ('Tablette', 399.99, 75)
    ]
    
    db.execute(
        "INSERT INTO produits (nom, prix, stock) VALUES (?, ?, ?)",
        produits,
        many=True
    )
    
    # Requête avec jointure
    result = db.execute("""
        SELECT p.nom, p.prix, c.nom as categorie
        FROM produits p
        JOIN categories c ON p.categorie_id = c.id
        WHERE p.prix > 500
        ORDER BY p.prix DESC
        LIMIT 10
    """)
    
    print(f"Résultats: {result['rows']}")
    print(f"Colonnes: {result['columns']}")
    print(f"Temps d'exécution: {result['execution_time_ms']}ms")
    
except GSQLException as e:
    print(f"Erreur GSQL: {e}")
    print(f"Suggestion: {e.suggestion}")
```

### 3. Utilisation avec NLP (Python)

```python
from gsql.nlp.translator import NLTranslator

translator = NLTranslator()

queries = [
    "Combien de clients avons-nous ?",
    "Montre les 5 produits les plus chers",
    "Quels clients n'ont pas acheté depuis 30 jours ?",
    "Donne-moi le total des ventes par mois"
]

for nl_query in queries:
    sql = translator.translate(nl_query)
    print(f"NL: {nl_query}")
    print(f"SQL: {sql}\n")
```

---

## 🤖 Intégration IA & NLP

### Traducteur NLP Avancé

Le module `nlp/translator.py` offre des capacités avancées de compréhension du langage naturel.

```python
class NLTranslator:
    """Traducteur NL→SQL avec apprentissage contextuel"""
    
    def __init__(self):
        self.intent_detector = IntentDetector()
        self.entity_extractor = EntityExtractor()
        self.sql_generator = SQLGenerator()
        self.context_manager = ContextManager()
        
    def translate(self, text: str, context=None) -> Dict:
        """
        Traduit un texte en langage naturel en requête SQL
        """
        # 1. Analyse contextuelle
        context = self.context_manager.update_context(text, context)
        
        # 2. Détection d'intention
        intent = self.intent_detector.detect(text)
        
        # 3. Extraction d'entités
        entities = self.entity_extractor.extract(text, intent)
        
        # 4. Génération SQL
        sql, confidence = self.sql_generator.generate(intent, entities, context)
        
        # 5. Validation sémantique
        if confidence > 0.7:
            sql = self._validate_sql(sql)
        
        return {
            'sql': sql,
            'confidence': confidence,
            'entities': entities,
            'intent': intent,
            'explanation': self._generate_explanation(intent, entities)
        }
```

### Exemples de Traduction

| Langage Naturel | SQL Généré | Confiance |
| :--- | :--- | :--- |
| "Affiche les 10 premiers clients" | `SELECT * FROM clients LIMIT 10` | 95% |
| "Combien de commandes en attente ?" | `SELECT COUNT(*) FROM commandes WHERE statut = 'en_attente'` | 88% |
| "Revenu total du mois dernier" | `SELECT SUM(montant) FROM ventes WHERE date >= date('now', '-1 month')` | 92% |
| "Clients sans achat depuis 30 jours" | `SELECT * FROM clients WHERE dernier_achat < date('now', '-30 days')` | 85% |

### Configuration NLP (`config/nlp_config.yaml`)

```yaml
nlp:
  models:
    intent: "fr_core_news_sm"
    entity: "custom_entity_model"
  vocabulary:
    tables:
      - clients
      - produits
    synonyms:
      "montre": ["affiche", "liste", "donne"]
      "combien": ["nombre", "quantité", "total"]
  min_confidence: 0.6
  max_suggestions: 3
  language: "fr"
```

---

## 💾 Stockage Multi-Backend

### 1. SQLite Storage (`storage/sqlite_storage.py`)
Backend haute performance utilisant SQLite avec des optimisations spécifiques (WAL, Cache, etc.).

```python
class SQLiteStorage(StorageBackend):
    """Backend SQLite avec optimisations GSQL"""
    # ... (implémentation optimisée avec BufferPool)
```

### 2. YAML Storage (`storage/yaml_storage.py`)
Backend léger, idéal pour le développement et les tests unitaires.

```python
class YAMLStorage(StorageBackend):
    """Stockage YAML pour tests et prototypes"""
    # ... (chargement et sauvegarde en format lisible)
```

### 3. Migration entre Backends

```python
from gsql import Database, MigrationTool

# Migration de YAML vers SQLite
source = Database('yaml', {'path': 'data.yaml'})
target = Database('sqlite', {'path': 'production.db'})

migrator = MigrationTool(source, target)
migrator.migrate_all()
```

---

## 🔍 Système d'Indexation

### B+Tree Implementation (`btree.py`)
GSQL implémente son propre arbre B+ pour une indexation ultra-rapide indépendante du backend de stockage sous-jacent.

```python
class BTree:
    """Implémentation B+Tree optimisée pour les bases de données"""
    
    def __init__(self, order=100, unique=False):
        self.order = order  # Ordre de l'arbre
        self.unique = unique
        self.root = BTreeNode(is_leaf=True)
        # ...
```

### Utilisation des Index

```python
from gsql import Database

db = Database()
# Création d'index
db.create_index(
    name='idx_titre',
    table='documents',
    columns=['titre'],
    index_type='btree',
    unique=True
)

# Vérification de l'utilisation des index
explanation = db.explain("""
    SELECT * FROM documents 
    WHERE titre LIKE 'Introduction%'
""")
print(f"Index utilisés: {explanation['indexes_used']}")
```

---

## 🔧 API Python

### Client Complet (`GSQLClient`)

Voici un exemple d'implémentation d'un client riche incluant l'export vers Pandas et des backups.

```python
from gsql import Database, GSQLException, connect
from gsql.functions import register_function
import pandas as pd

class GSQLClient:
    """Client GSQL complet avec fonctionnalités avancées"""
    
    def __init__(self, connection_string=None, **kwargs):
        self.db = connect(connection_string, **kwargs)
        self._setup_custom_functions()
    
    def _setup_custom_functions(self):
        # Enregistrement de fonctions custom (ex: formatage, calculs taxes)
        register_function(self.db, 'format_currency', lambda amount: f"${amount:,.2f}" if amount else None)
    
    def query_to_dataframe(self, query: str, params=None) -> pd.DataFrame:
        """Exécute une requête et retourne un DataFrame pandas"""
        result = self.db.execute(query, params)
        if not result['rows']:
            return pd.DataFrame()
        return pd.DataFrame(result['rows'], columns=result['columns'])
    
    def backup(self, backup_path: str):
        """Sauvegarde complète de la base"""
        # ... (Logique de sauvegarde atomique)
```

---

## 📊 Commandes Référence

### Commandes SQL Supportées

```sql
-- CRÉATION
CREATE TABLE table (id INTEGER, col TYPE);
CREATE INDEX idx ON table(col);

-- MANIPULATION
INSERT INTO table VALUES (v1, v2);
SELECT * FROM table WHERE cond;
UPDATE table SET col=val WHERE cond;
DELETE FROM table WHERE cond;

-- AVANCÉ
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
SELECT col FROM t WHERE col IN (SELECT col FROM t2);
```

### Commandes Spécifiques GSQL

*   `NL> [phrase]` : Traduction et exécution de langage naturel.
*   `EXPLAIN SELECT ...` : Affiche le plan d'exécution.
*   `SHOW STATS` : Affiche les métriques de performance.
*   `CACHE STATS` : Statistiques du cache.
*   `BACKUP TO 'path'` : Effectue une sauvegarde à chaud.

### Commandes CLI

```bash
gsql --database DB_PATH      # Connexion
gsql --file script.sql       # Exécution de script
gsql --export csv            # Export de données
gsql --migrate               # Outil de migration
```

---

## 🧪 Tests & Validation

La suite de tests est structurée pour couvrir les unités, l'intégration, les fonctionnalités et les performances.

### Structure

*   `tests/unit/`: Tests unitaires des composants (Parser, Executor).
*   `tests/integration/`: Tests entre modules (Backend, Migration).
*   `tests/functional/`: Tests bout en bout (CLI).
*   `tests/benchmarks/`: Tests de charge et performance.

### Exemple de Test Unitaire

```python
import unittest
from gsql import Database

class TestDatabase(unittest.TestCase):
    def test_nlp_translation(self):
        """Test traduction NLP"""
        self.db.execute("CREATE TABLE products (id INTEGER, name TEXT)")
        
        # Traduction NLP
        from gsql.nlp.translator import NLTranslator
        translator = NLTranslator()
        
        nl_query = "Montre les produits"
        translated = translator.translate(nl_query, context={'tables': ['products']})
        
        self.assertIn('SELECT', translated['sql'])
```

### Benchmarks de Performance

Le projet inclut des scripts (`benchmark_insert`, `benchmark_select_with_index`) pour valider les performances avant mise en production.

---

## 🚀 Déploiement

### 1. Configuration de Production (`config/production.yaml`)

```yaml
database:
  backend: sqlite
  path: /var/lib/gsql/production.db
  options:
    journal_mode: WAL
    cache_size: -10000

performance:
  buffer_pool_size: 5000
  max_connections: 50

monitoring:
  enabled: true
  metrics_port: 9090
  alerting:
    email: admin@example.com
```

### 2. Docker Compose

```yaml
version: '3.8'
services:
  gsql:
    build: .
    ports:
      - "8080:8080"  # API
      - "9090:9090"  # Metrics
    volumes:
      - gsql_data:/var/lib/gsql
    environment:
      - GSQL_CONFIG=/config/production.yaml
```

### 3. Kubernetes

Des manifestes complets (`Deployment`, `Service`, `ConfigMap`, `PVC`) sont fournis pour un déploiement sur cluster Kubernetes.

### 4. Scripts d'automatisation
Un script complet `deploy.sh` et un script de vérification de santé `health_check.py` sont inclus pour automatiser le cycle de vie de l'application.

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

*   **Documentation** : [https://gsql.readthedocs.io](https://gsql.readthedocs.io)
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
