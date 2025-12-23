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

```python
from gsql.database import Database

# Initialisation simple
db = Database(db_path="./data/myapp.db")
```

📊 Structure des données

Tables par défaut

GSQL initialise automatiquement 4 tables principales :

Table Description Structure
users Utilisateurs système (id, username, email, full_name, age, city, created_at, updated_at)
products Catalogue produits (id, name, category, price, stock, description, created_at)
orders Commandes (id, user_id, product_id, quantity, total, status, order_date)
logs Logs système (id, level, message, context, created_at)

Tables système

· _gsql_metadata : Métadonnées GSQL
· _gsql_schemas : Schémas de tables
· _gsql_statistics : Statistiques d'utilisation
· _gsql_transactions_log : Log des transactions

🔧 API Principale

Initialisation

```python
# Options d'initialisation
db = Database(
    db_path="./data/app.db",      # Chemin fichier ou ":memory:"
    base_dir="/root/.gsql",       # Répertoire base
    buffer_pool_size=100,         # Taille cache (KB)
    enable_wal=True,              # Write-Ahead Logging
    auto_recovery=True            # Récupération automatique
)
```

Format des résultats

db.execute() retourne toujours un dictionnaire :

```python
{
    'success': True/False,
    'execution_time': float,
    'type': 'select'|'insert'|'update'|'create'|'delete',
    'count': int,
    'columns': ['col1', 'col2', ...],
    'rows': [(val1, val2, ...), ...],  # TUPLES
    'timestamp': 'ISO-8601'
}
```

Helper pour convertir en dicts

```python
def rows_to_dicts(result):
    """Convertit result['rows'] (tuples) en liste de dicts"""
    if not result.get('success'):
        return []
    
    dicts = []
    for row_tuple in result.get('rows', []):
        row_dict = {}
        for i, col_name in enumerate(result.get('columns', [])):
            row_dict[col_name] = row_tuple[i] if i < len(row_tuple) else None
        dicts.append(row_dict)
    
    return dicts
```

🛠️ Intégration avec LangChain

GSQL Agent pour LangChain

```python
from langchain.agents import Tool, AgentExecutor, create_react_agent
from langchain.memory import ConversationBufferMemory
from langchain.prompts import PromptTemplate
from gsql.database import Database

class GSQLTool:
    """Outil GSQL pour LangChain"""
    
    def __init__(self, db_path="./data/chat.db"):
        self.db = Database(db_path=db_path)
        self._init_schema()
    
    def _init_schema(self):
        """Initialise le schéma pour les conversations IA"""
        schema = """
        CREATE TABLE IF NOT EXISTS chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            role TEXT CHECK(role IN ('user', 'assistant', 'system')),
            content TEXT,
            metadata TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS chat_sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT,
            context TEXT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_chat_session ON chat_messages(session_id);
        CREATE INDEX idx_chat_timestamp ON chat_messages(created_at);
        """
        
        for statement in schema.split(';'):
            if statement.strip():
                self.db.execute(statement)
    
    def execute_query(self, query: str) -> str:
        """Exécute une requête SQL et retourne les résultats formatés"""
        try:
            result = self.db.execute(query)
            
            if not result.get('success'):
                return f"❌ Erreur: {result}"
            
            if result.get('type') == 'select' and result.get('rows'):
                # Convertir en format lisible
                dicts = self._format_results(result)
                return self._results_to_string(dicts)
            
            return f"✅ Opération {result['type']} réussie"
            
        except Exception as e:
            return f"❌ Exception: {e}"
    
    def _format_results(self, result):
        """Formate les résultats pour l'affichage"""
        return rows_to_dicts(result)
    
    def _results_to_string(self, results, limit=10):
        """Convertit les résultats en chaîne lisible"""
        if not results:
            return "Aucun résultat"
        
        output = []
        for i, row in enumerate(results[:limit]):
            row_str = ", ".join([f"{k}: {v}" for k, v in row.items()])
            output.append(f"{i+1}. {row_str}")
        
        if len(results) > limit:
            output.append(f"... et {len(results) - limit} lignes supplémentaires")
        
        return "\n".join(output)
    
    def save_conversation(self, session_id: str, role: str, content: str, metadata: dict = None):
        """Sauvegarde un message de conversation"""
        import json
        
        query = """
            INSERT INTO chat_messages (session_id, role, content, metadata)
            VALUES (?, ?, ?, ?)
        """
        
        # Mettre à jour la session
        self.db.execute("""
            INSERT OR REPLACE INTO chat_sessions (session_id, last_active)
            VALUES (?, CURRENT_TIMESTAMP)
        """, [session_id])
        
        # Sauvegarder le message
        self.db.execute(query, [
            session_id,
            role,
            content,
            json.dumps(metadata or {})
        ])
        
        return {"success": True, "message": "Conversation sauvegardée"}
    
    def get_conversation_history(self, session_id: str, limit: int = 20):
        """Récupère l'historique d'une conversation"""
        result = self.db.execute("""
            SELECT role, content, created_at
            FROM chat_messages
            WHERE session_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, [session_id, limit])
        
        if result['success']:
            history = []
            for row in reversed(result['rows']):  # Inverser pour ordre chronologique
                history.append({
                    "role": row[0],
                    "content": row[1],
                    "timestamp": row[2]
                })
            return history
        
        return []

# Intégration LangChain
gsql_tool = Tool(
    name="GSQL Database",
    func=GSQLTool().execute_query,
    description="""
    Utilisez cet outil pour interagir avec la base de données GSQL.
    Formatez vos requêtes en SQL standard.
    Exemples:
    - "SELECT * FROM users WHERE age > 25"
    - "INSERT INTO products (name, price) VALUES ('Laptop', 999.99)"
    - "UPDATE orders SET status = 'completed' WHERE id = 123"
    """
)
```

Prompt Template pour agent GSQL

```python
GSQL_AGENT_PROMPT = PromptTemplate.from_template("""
Vous êtes un assistant IA spécialisé dans les bases de données GSQL.

Règles importantes:
1. N'exécutez que des requêtes SQL valides
2. Validez les données avant insertion/mise à jour
3. Utilisez des transactions pour les opérations multiples
4. Gérez proprement les erreurs SQL

Contexte:
{context}

Historique de conversation:
{history}

Requête utilisateur: {input}

Format de réponse attendu:
- Si c'est une requête SELECT: affichez les résultats en tableau
- Si c'est une modification: confirmez l'opération avec détails
- En cas d'erreur: expliquez le problème et suggérez une solution

Réponse:
""")
```

📈 Patterns d'Intégration

1. Application Web avec Flask

```python
from flask import Flask, jsonify, request
from gsql.database import Database
import os

app = Flask(__name__)

# Configuration
DB_PATH = os.getenv("GSQL_DB_PATH", "./data/webapp.db")
db = Database(db_path=DB_PATH)

@app.route('/api/query', methods=['POST'])
def execute_query():
    """Endpoint pour exécuter des requêtes SQL"""
    data = request.json
    query = data.get('query')
    params = data.get('params', [])
    
    try:
        result = db.execute(query, params)
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/data/<table>', methods=['GET'])
def get_table_data(table):
    """Récupère les données d'une table avec pagination"""
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 100, type=int)
    offset = (page - 1) * limit
    
    query = f"SELECT * FROM {table} LIMIT ? OFFSET ?"
    result = db.execute(query, [limit, offset])
    
    return jsonify({
        'table': table,
        'page': page,
        'limit': limit,
        'data': rows_to_dicts(result),
        'total': result.get('count', 0)
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
```

2. Analyse de données avec Pandas

```python
import pandas as pd
from gsql.database import Database

class GSQLDataAnalyzer:
    """Analyseur de données GSQL avec Pandas"""
    
    def __init__(self, db_path):
        self.db = Database(db_path=db_path)
    
    def query_to_dataframe(self, query, params=None):
        """Exécute une requête et retourne un DataFrame"""
        result = self.db.execute(query, params)
        
        if not result['success']:
            raise Exception(f"Query failed: {result}")
        
        # Convertir en DataFrame
        df = pd.DataFrame(
            result['rows'],
            columns=result['columns']
        )
        
        return df
    
    def analyze_sales(self, start_date, end_date):
        """Analyse les ventes sur une période"""
        query = """
            SELECT 
                DATE(order_date) as date,
                COUNT(*) as orders_count,
                SUM(total) as revenue,
                AVG(total) as avg_order_value,
                COUNT(DISTINCT user_id) as unique_customers
            FROM orders
            WHERE order_date BETWEEN ? AND ?
            GROUP BY DATE(order_date)
            ORDER BY date
        """
        
        df = self.query_to_dataframe(query, [start_date, end_date])
        
        # Analyses supplémentaires
        summary = {
            'total_orders': df['orders_count'].sum(),
            'total_revenue': df['revenue'].sum(),
            'avg_daily_revenue': df['revenue'].mean(),
            'peak_day': df.loc[df['revenue'].idxmax(), 'date'] if not df.empty else None
        }
        
        return df, summary
    
    def export_to_csv(self, table_name, output_path):
        """Exporte une table en CSV"""
        df = self.query_to_dataframe(f"SELECT * FROM {table_name}")
        df.to_csv(output_path, index=False)
        return output_path
```

3. Cache distribué avec Redis

```python
import redis
import json
from gsql.database import Database
from functools import lru_cache

class CachedGSQL:
    """GSQL avec cache Redis"""
    
    def __init__(self, db_path, redis_url="redis://localhost:6379/0"):
        self.db = Database(db_path=db_path)
        self.redis = redis.from_url(redis_url)
        self.cache_ttl = 300  # 5 minutes
    
    def execute_with_cache(self, query, params=None, cache_key=None):
        """Exécute avec cache Redis"""
        if cache_key is None:
            cache_key = f"gsql:{hash(f'{query}{params}')}"
        
        # Vérifier le cache
        cached = self.redis.get(cache_key)
        if cached:
            return json.loads(cached)
        
        # Exécuter la requête
        result = self.db.execute(query, params)
        
        # Mettre en cache si c'est un SELECT réussi
        if result.get('success') and result.get('type') == 'select':
            self.redis.setex(
                cache_key,
                self.cache_ttl,
                json.dumps(result)
            )
        
        return result
    
    def invalidate_cache(self, pattern="gsql:*"):
        """Invalide le cache pour un pattern"""
        keys = self.redis.keys(pattern)
        if keys:
            self.redis.delete(*keys)
        return len(keys)
```

🚨 Bonnes pratiques

1. Gestion des connexions

```python
from contextlib import contextmanager

@contextmanager
def gsql_session(db_path=None):
    """Context manager pour les sessions GSQL"""
    db = Database(db_path=db_path or ":memory:")
    try:
        yield db
    finally:
        db.close()

# Utilisation
with gsql_session("./data/app.db") as db:
    result = db.execute("SELECT * FROM users")
    # La connexion se ferme automatiquement
```

2. Validation des requêtes

```python
def validate_sql_query(query):
    """Valide une requête SQL avant exécution"""
    forbidden_keywords = ['DROP DATABASE', 'TRUNCATE', 'ALTER SYSTEM']
    
    query_upper = query.upper()
    
    # Vérifier les mots-clés interdits
    for keyword in forbidden_keywords:
        if keyword in query_upper:
            return False, f"Keyword '{keyword}' not allowed"
    
    # Vérifier la syntaxe basique
    if 'SELECT' in query_upper and 'FROM' not in query_upper:
        return False, "SELECT without FROM clause"
    
    return True, "Query is valid"
```

3. Logging et monitoring

```python
import logging
from datetime import datetime

class MonitoredGSQL:
    """GSQL avec monitoring"""
    
    def __init__(self, db_path):
        self.db = Database(db_path=db_path)
        self.logger = logging.getLogger('gsql.monitor')
        self.query_log = []
    
    def execute_monitored(self, query, params=None):
        """Exécute avec monitoring"""
        start_time = datetime.now()
        
        try:
            result = self.db.execute(query, params)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Log
            log_entry = {
                'query': query,
                'params': params,
                'execution_time': execution_time,
                'success': result.get('success'),
                'timestamp': start_time.isoformat()
            }
            
            self.query_log.append(log_entry)
            
            # Alert si trop lent
            if execution_time > 1.0:  # > 1 seconde
                self.logger.warning(f"Slow query: {query[:100]} took {execution_time:.3f}s")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Query failed: {query[:100]} - Error: {e}")
            raise
```

📦 Déploiement

Dockerfile

```dockerfile
FROM python:3.9-slim

WORKDIR /app

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier l'application
COPY . .

# Variables d'environnement
ENV GSQL_DB_PATH=/data/app.db
ENV PYTHONPATH=/app

# Créer le volume de données
VOLUME /data

# Exécuter l'application
CMD ["python", "app/main.py"]
```

docker-compose.yml

```yaml
version: '3.8'

services:
  gsql-app:
    build: .
    ports:
      - "8000:8000"
    volumes:
      - gsql-data:/data
    environment:
      - GSQL_DB_PATH=/data/app.db
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - redis
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
  
  monitor:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana

volumes:
  gsql-data:
  grafana-data:
```

🔍 Dépannage

Problèmes courants et solutions

1. "table already exists"
   ```python
   # Utiliser IF NOT EXISTS
   db.execute("CREATE TABLE IF NOT EXISTS users (...)")
   ```
2. "no such column"
   ```python
   # Vérifier la structure de la table
   result = db.execute("PRAGMA table_info(users)")
   print(f"Colonnes: {result['rows']}")
   ```
3. Transactions non fonctionnelles
   ```python
   # Utiliser des SAVEPOINTs à la place
   db.execute("SAVEPOINT my_transaction")
   # ... opérations ...
   db.execute("RELEASE SAVEPOINT my_transaction")
   ```
4. Performance lente
   ```python
   # Activer le WAL et optimiser
   db = Database(enable_wal=True, buffer_pool_size=500)
   db.execute("PRAGMA journal_mode = WAL")
   db.execute("PRAGMA synchronous = NORMAL")
   ```

📊 Benchmarks

```python
import timeit

def benchmark_gsql():
    """Benchmark des performances GSQL"""
    
    setup = """
from gsql.database import Database
db = Database(db_path=":memory:")
db.execute("CREATE TABLE test (id INTEGER, value REAL, text TEXT)")
    """
    
    stmt = """
for i in range(100):
    db.execute(f"INSERT INTO test VALUES ({i}, {i*1.5}, 'text_{i}')")
    """
    
    time = timeit.timeit(stmt, setup=setup, number=1)
    print(f"100 INSERT: {time:.3f}s ({time/100:.4f}s par insertion)")
```

---

📚 Ressources

· Documentation officielle : python -c "import gsql; help(gsql.database.Database)"
· Code source : https://github.com/gopu-inc/gsql
· Exemples complets : Voir le dossier /gsql/tests/
· Support : Issues GitHub ou communauté Discord

---

GSQL v3.0.9 est prêt pour la production avec une API simple, des performances SQLite natives, et une intégration facile avec les écosystèmes Python modernes. 🚀
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
