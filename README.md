<div align="center">
  <img width="280" height="280" alt="GSQL Logo" src="https://github.com/user-attachments/assets/9cf47e59-c2f3-49d9-a7c2-82771d5363bd" />

  <h1>
      GSQL : L'Interface Python
      Avancée pour SQLite 🔧
  </h1>
<footer>
  <p>
      <strong>Développé par
          GOPU.inc | Évolution :
          v3.9.7 (Beta) ➔ v4.0.0
          (Stable)</strong>
  </p>
</footer>

[![GitHub Issues](https://img.shields.io/github/issues/gopu-inc/gsql?color=%23343A40&label=Problèmes)](https://github.com/gopu-inc/gsql/issues)
[![GitHub Discussions](https://img.shields.io/badge/Discussions-Welcome-ff69b4?logo=github)](https://github.com/gopu-inc/gsql/discussions)
[![Community Chat](https://img.shields.io/badge/👥-Community_Chat-FF5722?logo=chatbot)](https://chat.whatsapp.com/F7NGsDVYDevEISVKTqpGZ1)
[![Discord Chat](https://img.shields.io/badge/Chat_on_Discord-5865F2?logo=discord&logoColor=white)](https://discord.gg/qWx5DszrC)
[![Documentation Status](https://img.shields.io/badge/Documentation_Complète-008080?logo=gitbook)](https://gopu-inc.github.io/gsql)

  <!-- Badges -->
  <a href="https://gopu-inc.github.io">
    <img src="https://img.shields.io/badge/🎉_Release-v4.0.0-FF4081?style=for-the-badge&logo=rocket&logoColor=white&labelColor=1a1a1a&color=FF4081" alt="New Release">
  </a>
  <a href="https://gopu-inc.github.io/gsql">
    <img src="https://img.shields.io/badge/🛠️_GSQL_Powered-4169E1?style=for-the-badge&logo=database&logoColor=white&labelColor=0A2540&color=4169E1" alt="GSQL Powered">
  </a>
  <a href="https://github.com/gopu-inc">
    <img src="https://img.shields.io/badge/GP_Open_Source-6F42C1?style=for-the-badge&logo=opensourceinitiative&logoColor=white&labelColor=1a1a1a&color=6F42C1" alt="Open Source">
  </a>
  <a href="https://chat.whatsapp.com/F7NGsDVYDevEISVKTqpGZ1">
    <img src="https://img.shields.io/badge/Whatsapp-Community-25D366?style=for-the-badge&logo=whatsapp&logoColor=white" alt="WhatsApp">
  </a>
  <br/>
  <a href="https://pypi.org/project/gsql/">
    <img src="https://img.shields.io/pypi/v/gsql?style=flat-square&logo=pypi&color=006dad" alt="PyPI Version">
  </a>
  <a href="https://pepy.tech/project/gsql">
    <img src="https://static.pepy.tech/personalized-badge/gsql?period=total&units=international_system&left_color=black&right_color=blue&left_text=Downloads" alt="Downloads">
  </a>
  <a href="https://hub.docker.com/r/ceoseshell/gsql">
    <img src="https://img.shields.io/docker/pulls/ceoseshell/gsql?style=flat-square&logo=docker&color=2496ed" alt="Docker Pulls">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/github/license/gopu-inc/gsql?style=flat-square&logo=opensourceinitiative&color=6cc24a" alt="License">
  </a>
</div>

---
> **powered by gopu.inc**

## 🚨 Note de Version & Migration

**GSQL est désormais disponible en version majeure 4.0.0.**

- **Version 4.0.0 (Stable)** : Introduce une gestion transactionnelle ACID complète, une meilleure stabilité pour la production et une intégration Docker native. Recommandée pour tous les nouveaux projets.
- **Version 3.9.7 (Legacy/Beta)** : Version toujours supportée pour la compatibilité. Idéale pour le prototypage rapide et l'utilisation du Shell interactif expérimental.

> **Ce README couvre les deux versions.** Les sections spécifiques à une version sont clairement indiquées.

---

## 🎯 Pourquoi GSQL ?

SQLite est puissant, mais son interface brute peut être limitante. **GSQL** comble ce fossé en apportant :

- ✅ **Productivité** : Une API Python plus intuitive que `sqlite3`.
- ✅ **Performance** : Un cache intelligent (LRU) qui accélère les SELECT répétitifs jusqu'à 20x.
- ✅ **Outils** : Un shell interactif avec auto-complétion et coloration syntaxique.
- ✅ **Robustesse** : Gestion avancée des transactions et des erreurs (v4.0.0+).

---

## 📦 Installation

Choisissez la méthode adaptée à votre environnement.

### 1. Via Pip (Standard)
```bash
# Pour la dernière version stable (4.0.0)
pip install gsql

# Pour forcer la version de compatibilité
pip install gsql==3.9.7
```

### 2. Via Docker (Recommandé pour v4.0.0)
```bash
docker pull ceoseshell/gsql:4.0.0

# Lancer une instance persistante
docker run -d \
  -p 8080:8080 \
  -v $(pwd)/data:/data \
  ceoseshell/gsql:4.0.0
```

### 3. Via Conda
```bash
conda install -c gopu-inc gsql
```

---

## 🚀 Démarrage Rapide

### Utilisation dans un Script Python

Quel que soit votre version, l'initialisation reste simple.

```python
from gsql.database import Database

# 1. INITIALISATION
# v4.0.0 : Options recommandées pour la production
db = Database(
    db_path="app.db",
    enable_wal=True,        # Write-Ahead Logging pour la performance
    auto_recovery=True      # Récupération auto en cas de crash
)

# 2. EXÉCUTION DE REQUÊTES BASIQUES
db.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT
    )
""")

# Insertion sécurisée (paramétrée)
db.execute(
    "INSERT INTO users (username, email) VALUES (?, ?)",
    ["jdoe", "john.doe@example.com"]
)

# Sélection avec Cache (Le cache est géré automatiquement)
result = db.execute("SELECT * FROM users")
print(f"Utilisateurs trouvés : {result['count']}") # Format de réponse standardisé
```

---

## ⚛️ Gestion des Transactions (Le Cœur du Système)

C'est ici que la différence entre les versions est cruciale.

### ✅ Méthode Moderne (v4.0.0+)
Utilisez l'API transactionnelle Python native. Elle gère les IDs de transaction (tid) et les rollbacks automatiques.

```python
try:
    # Démarrer une transaction (IMMEDIATE, EXCLUSIVE ou DEFERRED)
    tx = db.begin_transaction("IMMEDIATE")
    tid = tx['tid']

    # Passer le tid à chaque opération
    db.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1", tid=tid)
    db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2", tid=tid)

    # Valider
    db.commit_transaction(tid)
    print("Transaction réussie (v4)")

except Exception as e:
    # Annulation propre
    if 'tid' in locals():
        db.rollback_transaction(tid)
    print(f"Erreur : {e}")
```

### ⚠️ Méthode de Compatibilité (v3.9.7 / Workaround)
Si vous êtes sur la version 3.9.7, l'API `begin_transaction()` peut être instable. **Utilisez les commandes SQL brutes.**

```python
try:
    # SQL Brut pour le contrôle manuel
    db.execute("BEGIN IMMEDIATE TRANSACTION")
    
    db.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    
    db.execute("COMMIT")
    print("Transaction réussie (v3 workaround)")

except Exception as e:
    db.execute("ROLLBACK")
    print(f"Erreur : {e}")
```

---

## 🛠️ Fonctionnalités Avancées

### Shell Interactif (CLI)
GSQL fournit un terminal puissant pour explorer vos données.
```bash
$ gsql
# > Bienvenue dans GSQL v4.0.0
# gsql> .tables            -- Liste les tables
# gsql> .schema users      -- Affiche la structure
# gsql> STATS;             -- Affiche les performances du cache
# gsql> SELECT * FROM users;
```

### Commandes Spéciales & Maintenance
- `STATS` : Affiche le taux de succès du cache et le nombre de requêtes.
- `VACUUM` : Optimise la taille du fichier DB.
- `db.backup()` : Crée une sauvegarde à chaud (disponible dans l'API).

---

## 💡 Cas d'Usage Réels (Production)

### 1. Application Web (Flask + GSQL)
Intégration simple avec gestion de contexte.

```python
@app.route('/api/transfer', methods=['POST'])
def transfer():
    db = get_db()
    try:
        # Utilisation du context manager (v4.0.0)
        with db.transaction("EXCLUSIVE") as ctx:
            db.execute("UPDATE accounts SET amount = amount - ? WHERE id = ?", (val, src))
            db.execute("UPDATE accounts SET amount = amount + ? WHERE id = ?", (val, dst))
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
```

### 2. ETL et Import de Données
Grâce au mode WAL (`enable_wal=True`), GSQL peut gérer des imports massifs sans bloquer les lectures.

---

## 📂 Structure du Projet

```
gsql/
├── gsql/
│   ├── __init__.py          # Entrée
│   ├── database.py          # CLASSE PRINCIPALE (v4 refactorisée)
│   ├── storage.py           # Moteur de stockage & Cache
│   ├── executor.py          # Exécution des requêtes
│   ├── cli.py               # Shell Interactif
│   └── exceptions.py        # Gestion des erreurs typées
├── tests/                   # Suite de tests (Pytest)
├── Dockerfile               # Configuration Docker Production
└── README.md                # Documentation
```

---

## 🤝 Contribuer & Support

GSQL est un projet Open Source vivant. La version 4.0.0 stabilise le cœur, mais nous avons toujours besoin d'aide pour :
1.  Améliorer le parser SQL du Shell.
2.  Développer les modules expérimentaux (NLP).

**Liens Utiles :**
*   🐛 **Signaler un bug :** [GitHub Issues](https://github.com/gopu-inc/gsql/issues)
*   💬 **Discuter :** [WhatsApp Community](https://chat.whatsapp.com/F7NGsDVYDevEISVKTqpGZ1)
 [Discord](https://discord.gg/qWx5DszrC)
*   📖 **Documentation Complète :** [GitHub Wiki](https://gopu-inc.github.io/gsql)

---

<div align="center">
  <p><strong>GOPU.inc © 2025</strong><br/>
  <em>Apportons une interface moderne à SQLite.</em></p>
</div>
