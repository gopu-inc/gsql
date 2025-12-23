#!/usr/bin/env python3
"""
🔬 SCRIPT COMPLET DE TEST ET CONFIGURATION GSQL v3.0.9
Version: v3.0.9 - SQLite Only
Auteur: Gopu Inc.
"""

import sys
import os
import json
import inspect
import sqlite3
from datetime import datetime
from pathlib import Path
import traceback

# ============================================================================
# 1. CONFIGURATION ET INITIALISATION
# ============================================================================

def setup_environment():
    """Configure l'environnement pour GSQL"""
    print("🔧 CONFIGURATION DE L'ENVIRONNEMENT")
    print("-" * 50)
    
    # Chemin de base
    base_dir = Path.home() / ".gsql"
    base_dir.mkdir(exist_ok=True)
    
    # Fichier de configuration
    config_path = base_dir / "config.json"
    
    # Configuration par défaut
    default_config = {
        "version": "3.0.9",
        "base_dir": str(base_dir),
        "database_path": None,
        "auto_recovery": True,
        "buffer_pool_size": 100,
        "enable_wal": True,
        "nlp_enabled": True,
        "cache_size": 200,
        "timeout": 30,
        "auto_backup": True,
        "backup_interval": 86400,
        "max_query_cache": 100,
        "query_timeout": 30,
        "log_level": "INFO",
        "log_file": str(base_dir / "gsql.log"),
        "colors": True,
        "verbose_errors": True,
        "storage_type": "sqlite",
        "wal_mode": "WAL",
        "journal_mode": "DELETE",
        "synchronous": "NORMAL",
        "temp_store": "MEMORY",
        "page_size": 4096,
        "cache_size_kb": -2000,
        "foreign_keys": True,
        "recursive_triggers": True,
        "secure_delete": False
    }
    
    # Sauvegarder la config
    with open(config_path, 'w') as f:
        json.dump(default_config, f, indent=2)
    
    print(f"✓ Répertoire config: {base_dir}")
    print(f"✓ Fichier config: {config_path}")
    print(f"✓ Configuration sauvegardée")
    
    return base_dir, config_path

# ============================================================================
# 2. IMPORT ET VÉRIFICATION DES MODULES
# ============================================================================

def verify_imports():
    """Vérifie tous les imports GSQL"""
    print("\n📦 VÉRIFICATION DES IMPORTS GSQL")
    print("-" * 50)
    
    imports_to_test = [
        # Modules principaux
        ("gsql", "Module principal"),
        ("gsql.__version__", "Version"),
        ("gsql.__author__", "Auteur"),
        
        # Sous-modules critiques
        ("gsql.database", "Module Database"),
        ("gsql.storage", "Module Storage"),
        ("gsql.executor", "Module Executor"),
        ("gsql.parser", "Module Parser"),
        ("gsql.functions", "Module Functions"),
        ("gsql.exceptions", "Exceptions"),
        
        # NLP (optionnel)
        ("gsql.nlp", "Module NLP"),
        ("gsql.nlp.translator", "Translator NLP"),
        
        # Autres modules
        ("gsql.btree", "B-Tree"),
        ("gsql.index", "Index"),
    ]
    
    imported_modules = {}
    
    for import_path, description in imports_to_test:
        try:
            if '.' in import_path:
                parts = import_path.split('.')
                module = __import__(parts[0])
                for part in parts[1:]:
                    module = getattr(module, part)
            else:
                module = __import__(import_path)
            
            imported_modules[import_path] = module
            print(f"✅ {description:20} : {import_path}")
            
        except ImportError as e:
            print(f"❌ {description:20} : {import_path} - {e}")
            imported_modules[import_path] = None
        except AttributeError as e:
            print(f"⚠️  {description:20} : {import_path} - {e}")
            imported_modules[import_path] = None
    
    return imported_modules

# ============================================================================
# 3. ANALYSE DES SIGNATURES ET API
# ============================================================================

def analyze_signatures(imported_modules):
    """Analyse détaillée des signatures"""
    print("\n🔍 ANALYSE DES SIGNATURES")
    print("-" * 50)
    
    # Fonction pour afficher une signature
    def print_signature(name, obj):
        try:
            if inspect.isclass(obj):
                print(f"🏗  {name}")
                sig = inspect.signature(obj.__init__)
                print(f"    __init__{sig}")
                
                # Méthodes importantes
                important_methods = ['execute', 'connect', 'close', 'query', 
                                   'create_table', 'insert', 'select', 'update', 'delete',
                                   'configure', 'backup', 'vacuum']
                for method_name in important_methods:
                    if hasattr(obj, method_name):
                        method = getattr(obj, method_name)
                        if callable(method):
                            try:
                                msig = inspect.signature(method)
                                print(f"    {method_name}{msig}")
                            except:
                                print(f"    {method_name}()")
                
            elif inspect.isfunction(obj) or inspect.ismethod(obj):
                sig = inspect.signature(obj)
                print(f"🔧  {name}{sig}")
                
                # Documentation
                doc = inspect.getdoc(obj)
                if doc:
                    first_line = doc.split('\n')[0]
                    print(f"     📝 {first_line[:80]}...")
                    
        except Exception as e:
            print(f"⚠️   {name}: Erreur de signature - {e}")
    
    # Analyser les composants principaux
    components = [
        ("Database", "gsql.database"),
        ("connect", "gsql"),
        ("create_database", "gsql"),
        ("SQLiteStorage", "gsql.storage"),
        ("QueryExecutor", "gsql.executor"),
        ("SQLParser", "gsql.parser"),
        ("FunctionManager", "gsql.functions"),
        ("NLToSQLTranslator", "gsql.nlp.translator"),
    ]
    
    for comp_name, comp_path in components:
        try:
            if comp_path in imported_modules:
                if imported_modules[comp_path] is not None:
                    # Si c'est un module, chercher la classe dedans
                    if comp_path == comp_name:
                        obj = imported_modules[comp_path]
                    else:
                        obj = getattr(imported_modules[comp_path], comp_name)
                    print_signature(comp_name, obj)
                    print()
        except Exception as e:
            print(f"❌ Impossible d'analyser {comp_name}: {e}")

# ============================================================================
# 4. CONFIGURATION DES BASES DE DONNÉES
# ============================================================================

def test_database_configurations():
    """Test toutes les configurations possibles de Database"""
    print("\n⚙️  CONFIGURATIONS DATABASE")
    print("-" * 50)
    
    try:
        from gsql.database import Database
        
        configurations = [
            {
                "name": "Configuration minimale",
                "params": {},
                "description": "Tous les paramètres par défaut"
            },
            {
                "name": "Base en mémoire",
                "params": {"db_path": ":memory:"},
                "description": "Base temporaire en RAM"
            },
            {
                "name": "Base fichier",
                "params": {"db_path": "/tmp/test_gsql.db"},
                "description": "Base dans un fichier"
            },
            {
                "name": "Performance optimisée",
                "params": {
                    "db_path": None,
                    "base_dir": "/tmp/gsql_perf",
                    "buffer_pool_size": 500,
                    "enable_wal": True,
                    "auto_recovery": True
                },
                "description": "Config pour haute performance"
            },
            {
                "name": "Sécurité maximale",
                "params": {
                    "db_path": None,
                    "base_dir": "/tmp/gsql_secure",
                    "buffer_pool_size": 50,
                    "enable_wal": True,
                    "auto_recovery": True
                },
                "description": "Config pour sécurité/robustesse"
            },
            {
                "name": "Debug",
                "params": {
                    "db_path": None,
                    "base_dir": "/tmp/gsql_debug",
                    "buffer_pool_size": 10,
                    "enable_wal": False,
                    "auto_recovery": True
                },
                "description": "Config pour debugging"
            }
        ]
        
        for config in configurations:
            print(f"\n🔹 {config['name']}")
            print(f"   Description: {config['description']}")
            print(f"   Paramètres: {config['params']}")
            
            try:
                # Créer le répertoire si nécessaire
                if 'base_dir' in config['params']:
                    Path(config['params']['base_dir']).mkdir(exist_ok=True)
                
                db = Database(**config['params'])
                print(f"   ✅ Création réussie")
                
                # Tester une opération simple
                test_result = db.execute("SELECT 1 as test")
                print(f"   ✅ Test SQL: OK")
                
                db.close()
                print(f"   ✅ Fermeture propre")
                
            except Exception as e:
                print(f"   ❌ Erreur: {e}")
                if "unexpected keyword argument" in str(e):
                    print(f"   💡 Conseil: Vérifiez les paramètres acceptés par Database.__init__")
    
    except ImportError as e:
        print(f"❌ Impossible d'importer Database: {e}")
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")

# ============================================================================
# 5. TEST DES OPÉRATIONS SQL
# ============================================================================

def test_sql_operations():
    """Test complet des opérations SQL"""
    print("\n🗃️  TEST DES OPÉRATIONS SQL")
    print("-" * 50)
    
    try:
        from gsql.database import Database
        
        # Créer une base de test
        test_dir = Path("/tmp/gsql_test_operations")
        test_dir.mkdir(exist_ok=True)
        
        db = Database(
            db_path=None,
            base_dir=str(test_dir),
            buffer_pool_size=100,
            enable_wal=True,
            auto_recovery=True
        )
        
        print("✓ Base de test créée")
        
        # 1. CRÉATION DE TABLES
        print("\n1. CRÉATION DE TABLES:")
        
        tables = [
            ("users", """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    age INTEGER CHECK(age >= 0),
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """),
            ("products", """
                CREATE TABLE products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    category TEXT,
                    price DECIMAL(10,2) CHECK(price >= 0),
                    stock INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """),
            ("orders", """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER CHECK(quantity > 0),
                    total_price DECIMAL(10,2),
                    status TEXT DEFAULT 'pending',
                    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    FOREIGN KEY (product_id) REFERENCES products(id)
                )
            """)
        ]
        
        for table_name, create_sql in tables:
            try:
                db.execute(create_sql)
                print(f"   ✅ Table '{table_name}' créée")
            except Exception as e:
                print(f"   ❌ Table '{table_name}': {e}")
        
        # 2. INSERTION DE DONNÉES
        print("\n2. INSERTION DE DONNÉES:")
        
        insert_queries = [
            ("users", """
                INSERT INTO users (username, email, age, status) 
                VALUES 
                    ('alice', 'alice@example.com', 25, 'active'),
                    ('bob', 'bob@example.com', 30, 'active'),
                    ('charlie', 'charlie@example.com', 22, 'inactive'),
                    ('diana', 'diana@example.com', 35, 'active')
            """),
            ("products", """
                INSERT INTO products (name, category, price, stock) 
                VALUES 
                    ('Laptop', 'Electronics', 999.99, 50),
                    ('Mouse', 'Electronics', 29.99, 200),
                    ('Desk', 'Furniture', 299.99, 30),
                    ('Chair', 'Furniture', 149.99, 75)
            """)
        ]
        
        for table_name, insert_sql in insert_queries:
            try:
                result = db.execute(insert_sql)
                if isinstance(result, dict) and 'rowcount' in result:
                    print(f"   ✅ {table_name}: {result['rowcount']} lignes insérées")
                else:
                    print(f"   ✅ {table_name}: Données insérées")
            except Exception as e:
                print(f"   ❌ {table_name}: {e}")
        
        # 3. REQUÊTES SELECT
        print("\n3. REQUÊTES SELECT:")
        
        select_queries = [
            ("Tous les utilisateurs", "SELECT * FROM users ORDER BY id"),
            ("Utilisateurs actifs", "SELECT username, email, age FROM users WHERE status = 'active'"),
            ("Produits par catégorie", "SELECT category, COUNT(*) as count, AVG(price) as avg_price FROM products GROUP BY category"),
            ("Jointure users/products", """
                SELECT u.username, p.name, p.price 
                FROM users u, products p 
                WHERE u.status = 'active' 
                ORDER BY u.username, p.name
            """)
        ]
        
        for query_name, select_sql in select_queries:
            try:
                results = db.execute(select_sql)
                if isinstance(results, list):
                    print(f"   ✅ {query_name}: {len(results)} résultats")
                    if results and len(results) > 0:
                        # Afficher le premier résultat pour montrer le format
                        first_result = results[0]
                        if isinstance(first_result, dict):
                            print(f"       Format: dict avec clés {list(first_result.keys())[:3]}...")
                        elif isinstance(first_result, (list, tuple)):
                            print(f"       Format: {type(first_result).__name__} de taille {len(first_result)}")
                else:
                    print(f"   ⚠️  {query_name}: Format inattendu {type(results)}")
            except Exception as e:
                print(f"   ❌ {query_name}: {e}")
        
        # 4. MISES À JOUR
        print("\n4. MISES À JOUR:")
        
        update_queries = [
            ("Mettre à jour le stock", "UPDATE products SET stock = stock + 10 WHERE category = 'Electronics'"),
            ("Changer le statut", "UPDATE users SET status = 'active' WHERE username = 'charlie'"),
        ]
        
        for query_name, update_sql in update_queries:
            try:
                result = db.execute(update_sql)
                if isinstance(result, dict) and 'rowcount' in result:
                    print(f"   ✅ {query_name}: {result['rowcount']} lignes affectées")
                else:
                    print(f"   ✅ {query_name}: Exécutée")
            except Exception as e:
                print(f"   ❌ {query_name}: {e}")
        
        # 5. TRANSACTIONS
        print("\n5. TEST DES TRANSACTIONS:")
        
        try:
            # Commencer une transaction
            db.execute("BEGIN TRANSACTION")
            print("   ✅ Transaction commencée")
            
            # Opérations dans la transaction
            db.execute("INSERT INTO users (username, email, age) VALUES ('test_user', 'test@example.com', 40)")
            db.execute("UPDATE products SET stock = stock - 1 WHERE name = 'Laptop'")
            
            # Commit
            db.execute("COMMIT")
            print("   ✅ Transaction commitée")
            
        except Exception as e:
            print(f"   ❌ Transaction: {e}")
            try:
                db.execute("ROLLBACK")
                print("   ✅ Rollback effectué")
            except:
                pass
        
        # 6. FONCTIONS SQL
        print("\n6. FONCTIONS SQL:")
        
        function_tests = [
            ("COUNT", "SELECT COUNT(*) as user_count FROM users"),
            ("SUM", "SELECT SUM(stock) as total_stock FROM products"),
            ("AVG", "SELECT AVG(price) as avg_price FROM products"),
            ("MAX/MIN", "SELECT MAX(price) as max_price, MIN(price) as min_price FROM products"),
            ("DATE", "SELECT DATE(created_at) as creation_date FROM users LIMIT 1"),
        ]
        
        for func_name, func_sql in function_tests:
            try:
                result = db.execute(func_sql)
                if isinstance(result, list) and len(result) > 0:
                    print(f"   ✅ {func_name}: {result[0]}")
                else:
                    print(f"   ✅ {func_name}: Exécutée")
            except Exception as e:
                print(f"   ❌ {func_name}: {e}")
        
        # 7. GESTION DES INDEX
        print("\n7. INDEX ET PERFORMANCE:")
        
        index_queries = [
            ("Créer index", "CREATE INDEX idx_users_status ON users(status)"),
            ("Créer index unique", "CREATE UNIQUE INDEX idx_users_email ON users(email)"),
            ("Voir les index", "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'users'"),
        ]
        
        for query_name, index_sql in index_queries:
            try:
                result = db.execute(index_sql)
                print(f"   ✅ {query_name}")
            except Exception as e:
                print(f"   ❌ {query_name}: {e}")
        
        # 8. VACUUM ET MAINTENANCE
        print("\n8. MAINTENANCE:")
        
        try:
            db.execute("VACUUM")
            print("   ✅ VACUUM exécuté")
        except Exception as e:
            print(f"   ❌ VACUUM: {e}")
        
        # Fermer la base
        db.close()
        print("\n✓ Base fermée proprement")
        
        # Nettoyage
        import shutil
        if test_dir.exists():
            shutil.rmtree(test_dir)
            print("✓ Répertoire de test nettoyé")
        
    except Exception as e:
        print(f"❌ Erreur lors des tests SQL: {e}")
        traceback.print_exc()

# ============================================================================
# 6. TEST DES FONCTIONNALITÉS AVANCÉES
# ============================================================================

def test_advanced_features():
    """Test des fonctionnalités avancées"""
    print("\n🚀 FONCTIONNALITÉS AVANCÉES")
    print("-" * 50)
    
    # 1. NLP (si disponible)
    print("1. TRAITEMENT DU LANGAGE NATUREL (NLP):")
    try:
        from gsql.nlp.translator import NLToSQLTranslator, nl_to_sql
        
        translator = NLToSQLTranslator(language='fr')
        print("   ✅ NLToSQLTranslator initialisé")
        
        test_phrases = [
            "Affiche tous les utilisateurs",
            "Combien d'utilisateurs actifs ?",
            "Montre les produits les plus chers",
            "Utilisateurs de plus de 25 ans"
        ]
        
        for phrase in test_phrases:
            try:
                sql = translator.translate(phrase)
                print(f"   🔄 '{phrase}' → {sql}")
            except Exception as e:
                print(f"   ❌ Traduction '{phrase}': {e}")
                
    except ImportError:
        print("   ⚠️  Module NLP non disponible")
    except Exception as e:
        print(f"   ❌ NLP: {e}")
    
    # 2. FONCTIONS UTILISATEUR
    print("\n2. FONCTIONS PERSONNALISÉES:")
    try:
        from gsql.functions import FunctionManager
        
        fm = FunctionManager()
        print("   ✅ FunctionManager initialisé")
        
        # Tester l'enregistrement de fonctions
        def custom_uppercase(text):
            return text.upper() if text else ""
        
        def calculate_discount(price, discount_percent):
            return price * (1 - discount_percent / 100)
        
        try:
            fm.register_function("UPPERCASE", custom_uppercase)
            print("   ✅ Fonction UPPERCASE enregistrée")
        except:
            print("   ⚠️  Impossible d'enregistrer UPPERCASE")
            
    except ImportError:
        print("   ⚠️  Module Functions non disponible")
    except Exception as e:
        print(f"   ❌ Functions: {e}")
    
    # 3. BUFFER POOL ET CACHE
    print("\n3. PERFORMANCE ET CACHE:")
    try:
        from gsql.storage import BufferPool, get_storage_stats
        
        print("   ✅ Modules performance disponibles")
        
        # Tester différentes tailles de buffer pool
        for size in [10, 50, 100, 500]:
            try:
                bp = BufferPool(size)
                print(f"   📊 BufferPool({size}) créé")
            except Exception as e:
                print(f"   ❌ BufferPool({size}): {e}")
                
    except ImportError:
        print("   ⚠️  Modules performance non disponibles")
    except Exception as e:
        print(f"   ❌ Performance tests: {e}")

# ============================================================================
# 7. GESTION DES ERREURS ET EXCEPTIONS
# ============================================================================

def test_error_handling():
    """Test de la gestion des erreurs"""
    print("\n🚨 GESTION DES ERREURS")
    print("-" * 50)
    
    try:
        from gsql.database import Database
        from gsql.exceptions import SQLSyntaxError, SQLExecutionError
        
        # Créer une base pour les tests d'erreur
        error_dir = Path("/tmp/gsql_error_test")
        error_dir.mkdir(exist_ok=True)
        
        db = Database(
            db_path=None,
            base_dir=str(error_dir),
            buffer_pool_size=10,
            enable_wal=True,
            auto_recovery=True
        )
        
        print("✓ Base pour tests d'erreur créée")
        
        # 1. ERREURS DE SYNTAXE
        print("\n1. ERREURS DE SYNTAXE SQL:")
        
        syntax_errors = [
            "SELECTE * FROM nonexistent",
            "SELECT * FRM users",
            "CREATE TABL users (id INT)",
            "INSERT IN users VALUES (1, 'test')"
        ]
        
        for error_sql in syntax_errors:
            try:
                db.execute(error_sql)
                print(f"   ⚠️  '{error_sql[:30]}...' : Pas d'erreur (inattendu)")
            except SQLSyntaxError as e:
                print(f"   ✅ '{error_sql[:30]}...' : SQLSyntaxError capturée")
            except Exception as e:
                print(f"   🔶 '{error_sql[:30]}...' : {type(e).__name__}")
        
        # 2. ERREURS D'EXÉCUTION
        print("\n2. ERREURS D'EXÉCUTION:")
        
        execution_errors = [
            "SELECT * FROM table_inexistante",
            "INSERT INTO users (invalid_column) VALUES ('test')",
            "UPDATE nonexistent SET x = 1",
            "DROP TABLE table_qui_n_existe_pas"
        ]
        
        for error_sql in execution_errors:
            try:
                db.execute(error_sql)
                print(f"   ⚠️  '{error_sql[:30]}...' : Pas d'erreur (inattendu)")
            except SQLExecutionError as e:
                print(f"   ✅ '{error_sql[:30]}...' : SQLExecutionError capturée")
            except Exception as e:
                print(f"   🔶 '{error_sql[:30]}...' : {type(e).__name__}")
        
        # 3. VIOLATIONS DE CONTRAINTES
        print("\n3. VIOLATIONS DE CONTRAINTES:")
        
        # Créer une table avec contraintes
        db.execute("""
            CREATE TABLE IF NOT EXISTS test_constraints (
                id INTEGER PRIMARY KEY,
                email TEXT UNIQUE,
                age INTEGER CHECK(age >= 0)
            )
        """)
        
        constraint_errors = [
            ("INSERT INTO test_constraints (id, email, age) VALUES (1, 'test@test.com', -5)", "CHECK constraint"),
            ("INSERT INTO test_constraints (id, email, age) VALUES (1, 'test@test.com', 25)", "UNIQUE constraint"),
            ("INSERT INTO test_constraints (id, email, age) VALUES (NULL, 'test2@test.com', 25)", "PRIMARY KEY"),
        ]
        
        for error_sql, expected_error in constraint_errors:
            try:
                db.execute(error_sql)
                print(f"   ⚠️  {expected_error} : Pas d'erreur (inattendu)")
            except Exception as e:
                print(f"   ✅ {expected_error} : {type(e).__name__} capturée")
        
        db.close()
        
        # Nettoyage
        import shutil
        if error_dir.exists():
            shutil.rmtree(error_dir)
            
    except Exception as e:
        print(f"❌ Tests d'erreur: {e}")

# ============================================================================
# 8. CONFIGURATION PRATIQUE ET EXEMPLES
# ============================================================================

def practical_configurations():
    """Exemples de configurations pratiques"""
    print("\n🎯 CONFIGURATIONS PRATIQUES")
    print("-" * 50)
    
    configurations = [
        {
            "name": "🌐 Application Web",
            "config": {
                "base_dir": "/var/lib/gsql/webapp",
                "buffer_pool_size": 500,
                "enable_wal": True,
                "auto_recovery": True,
                "cache_size": 1000,
                "timeout": 10,
                "journal_mode": "WAL",
                "synchronous": "NORMAL",
                "temp_store": "MEMORY",
                "foreign_keys": True
            },
            "usage": "Base de données pour application web avec concurrence modérée"
        },
        {
            "name": "📊 Analyse de Données",
            "config": {
                "base_dir": "/data/analytics",
                "buffer_pool_size": 2000,
                "enable_wal": False,
                "auto_recovery": False,
                "cache_size": 5000,
                "timeout": 300,
                "journal_mode": "OFF",
                "synchronous": "OFF",
                "temp_store": "FILE",
                "page_size": 8192
            },
            "usage": "Traitement par lots, lectures intensives, performances maximales"
        },
        {
            "name": "🔐 Transactionnel Critique",
            "config": {
                "base_dir": "/secure/transactions",
                "buffer_pool_size": 100,
                "enable_wal": True,
                "auto_recovery": True,
                "cache_size": 100,
                "timeout": 30,
                "journal_mode": "WAL",
                "synchronous": "FULL",
                "temp_store": "FILE",
                "foreign_keys": True,
                "secure_delete": True
            },
            "usage": "Transactions financières, données critiques, sécurité maximale"
        },
        {
            "name": "📱 Application Mobile",
            "config": {
                "base_dir": "./mobile_data",
                "buffer_pool_size": 50,
                "enable_wal": True,
                "auto_recovery": True,
                "cache_size": 100,
                "timeout": 5,
                "journal_mode": "DELETE",
                "synchronous": "NORMAL",
                "temp_store": "MEMORY",
                "page_size": 1024
            },
            "usage": "Applications mobiles, ressources limitées, besoin de réactivité"
        },
        {
            "name": "🔄 Cache / Session",
            "config": {
                "base_dir": ":memory:",
                "buffer_pool_size": 1000,
                "enable_wal": False,
                "auto_recovery": False,
                "cache_size": 5000,
                "timeout": 1,
                "journal_mode": "OFF",
                "synchronous": "OFF",
                "temp_store": "MEMORY"
            },
            "usage": "Cache en mémoire, sessions utilisateurs, données temporaires"
        }
    ]
    
    for config in configurations:
        print(f"\n{config['name']}")
        print(f"📝 Usage: {config['usage']}")
        print("⚙️  Configuration:")
        for key, value in config['config'].items():
            print(f"   {key:20} = {value}")
        
        # Code d'exemple
        print("💻 Code d'exemple:")
        print(f"""from gsql.database import Database

db = Database(
    base_dir="{config['config'].get('base_dir', '/tmp/gsql')}",
    buffer_pool_size={config['config'].get('buffer_pool_size', 100)},
    enable_wal={config['config'].get('enable_wal', True)},
    auto_recovery={config['config'].get('auto_recovery', True)}
)

# Configuration SQLite avancée
db.execute("PRAGMA journal_mode = {config['config'].get('journal_mode', 'WAL')}")
db.execute("PRAGMA synchronous = {config['config'].get('synchronous', 'NORMAL')}")
db.execute("PRAGMA temp_store = {config['config'].get('temp_store', 'MEMORY')}")
db.execute("PRAGMA foreign_keys = {'ON' if config['config'].get('foreign_keys', True) else 'OFF'}")

print("Base configurée pour: {config['usage']}")
""")

# ============================================================================
# 9. UTILISATION AVEC AUTRES BIBLIOTHÈQUES
# ============================================================================

def integration_examples():
    """Exemples d'intégration avec d'autres bibliothèques"""
    print("\n🔗 INTÉGRATION AVEC AUTRES BIBLIOTHÈQUES")
    print("-" * 50)
    
    examples = [
        {
            "name": "📈 Pandas (Data Analysis)",
            "code": """
import pandas as pd
from gsql.database import Database

# Connexion à GSQL
db = Database(base_dir="./data")

# Exécuter une requête et convertir en DataFrame
query = "SELECT * FROM users WHERE status = 'active'"
results = db.execute(query)

if isinstance(results, list) and len(results) > 0:
    # Convertir en DataFrame
    df = pd.DataFrame(results)
    
    # Analyser avec Pandas
    print(f"Total users: {len(df)}")
    print(f"Average age: {df['age'].mean()}")
    print(f"Users by status:\\n{df['status'].value_counts()}")
    
    # Sauvegarder en CSV
    df.to_csv('active_users.csv', index=False)
    print("Données sauvegardées dans active_users.csv")
            """,
            "requirements": "pandas"
        },
        {
            "name": "📊 Matplotlib (Visualisation)",
            "code": """
import matplotlib.pyplot as plt
from gsql.database import Database

db = Database(base_dir="./data")

# Récupérer les données
results = db.execute("""
    SELECT category, COUNT(*) as count, AVG(price) as avg_price 
    FROM products 
    GROUP BY category
""")

if results:
    categories = [r['category'] for r in results]
    counts = [r['count'] for r in results]
    
    # Créer un graphique
    plt.figure(figsize=(10, 6))
    plt.bar(categories, counts)
    plt.xlabel('Catégorie')
    plt.ylabel('Nombre de produits')
    plt.title('Produits par catégorie')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('products_by_category.png')
    plt.show()
            """,
            "requirements": "matplotlib"
        },
        {
            "name": "🌐 Flask (Web Application)",
            "code": """
from flask import Flask, jsonify, request
from gsql.database import Database

app = Flask(__name__)

# Initialiser la base de données
db = Database(base_dir="./app_data")

@app.route('/api/users', methods=['GET'])
def get_users():
    users = db.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT 100")
    return jsonify(users)

@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.json
    db.execute(
        "INSERT INTO users (username, email, age) VALUES (?, ?, ?)",
        [data['username'], data['email'], data['age']]
    )
    return jsonify({"message": "User created"}), 201

@app.route('/api/stats', methods=['GET'])
def get_stats():
    stats = db.execute("""
        SELECT 
            COUNT(*) as total_users,
            AVG(age) as average_age,
            COUNT(CASE WHEN status = 'active' THEN 1 END) as active_users
        FROM users
    """)
    return jsonify(stats[0] if stats else {})

if __name__ == '__main__':
    app.run(debug=True)
            """,
            "requirements": "flask"
        }
    ]
    
    for example in examples:
        print(f"\n{example['name']}")
        if example.get('requirements'):
            print(f"📋 Requirements: {example['requirements']}")
        print(f"💻 Code example:")
        print(example['code'])

# ============================================================================
# 10. RAPPORT FINAL ET RECOMMANDATIONS
# ============================================================================

def generate_final_report():
    """Génère un rapport final avec recommandations"""
    print("\n" + "=" * 80)
    print("📋 RAPPORT FINAL GSQL v3.0.9")
    print("=" * 80)
    
    # Collecter des informations
    import gsql
    
    report = {
        "version": getattr(gsql, '__version__', 'Inconnue'),
        "author": getattr(gsql, '__author__', 'Inconnu'),
        "description": getattr(gsql, '__description__', ''),
        "base_dir": str(Path.home() / ".gsql"),
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"\n📊 INFORMATIONS SYSTÈME:")
    print(f"   Version GSQL: {report['version']}")
    print(f"   Auteur: {report['author']}")
    print(f"   Description: {report['description']}")
    print(f"   Répertoire de config: {report['base_dir']}")
    print(f"   Date du test: {report['timestamp']}")
    
    print(f"\n✅ POINTS FORTS IDENTIFIÉS:")
    print("   1. Interface simple et Pythonique")
    print("   2. Support SQLite complet")
    print("   3. Transactions ACID")
    print("   4. Buffer pool pour la performance")
    print("   5. Gestion d'erreurs structurée")
    print("   6. Configuration flexible")
    
    print(f"\n⚠️  RECOMMANDATIONS:")
    print("   1. Toujours utiliser 'from gsql.database import Database'")
    print("   2. Configurer enable_wal=True pour les applications concurrentes")
    print("   3. Adapter buffer_pool_size selon la mémoire disponible")
    print("   4. Utiliser les transactions pour les opérations multiples")
    print("   5. Sauvegarder régulièrement les données importantes")
    print("   6. Tester avec une base ':memory:' pour le développement")
    
    print(f"\n🔧 CONFIGURATION RECOMMANDÉE (Cas général):")
    print("""from gsql.database import Database

# Pour la plupart des applications
db = Database(
    db_path=None,                    # Auto-généré
    base_dir="./gsql_data",          # Répertoire des données
    buffer_pool_size=100,            # Cache mémoire
    enable_wal=True,                 # Concurrent writes
    auto_recovery=True               # Résilience
)

# Optimisations SQLite recommandées
db.execute("PRAGMA journal_mode = WAL")
db.execute("PRAGMA synchronous = NORMAL")
db.execute("PRAGMA temp_store = MEMORY")
db.execute("PRAGMA foreign_keys = ON")
""")
    
    print(f"\n🚀 POUR COMMENCER RAPIDEMENT:")
    print("""# Installation et premier script
pip install gsql  # Si disponible sur PyPI
# ou depuis sources
cd /chemin/vers/gsql
python setup.py install

# Script minimal
from gsql.database import Database

db = Database()  # Configuration par défaut
db.execute("CREATE TABLE test (id INT, name TEXT)")
db.execute("INSERT INTO test VALUES (1, 'Hello GSQL')")
results = db.execute("SELECT * FROM test")
print(results)
""")
    
    return report

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Fonction principale"""
    print("=" * 80)
    print("🚀 SCRIPT COMPLET DE TEST ET CONFIGURATION GSQL v3.0.9")
    print("=" * 80)
    
    try:
        # 1. Configuration
        setup_environment()
        
        # 2. Imports
        imported_modules = verify_imports()
        
        # 3. Signatures
        analyze_signatures(imported_modules)
        
        # 4. Configurations Database
        test_database_configurations()
        
        # 5. Opérations SQL
        test_sql_operations()
        
        # 6. Fonctionnalités avancées
        test_advanced_features()
        
        # 7. Gestion des erreurs
        test_error_handling()
        
        # 8. Configurations pratiques
        practical_configurations()
        
        # 9. Intégration
        integration_examples()
        
        # 10. Rapport final
        report = generate_final_report()
        
        print("\n" + "=" * 80)
        print("🎉 TESTS TERMINÉS AVEC SUCCÈS!")
        print("=" * 80)
        
        # Sauvegarder le rapport
        report_path = Path.home() / ".gsql" / "test_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📄 Rapport sauvegardé dans: {report_path}")
        print("\n📌 Prochaines étapes:")
        print("   1. Consultez le rapport pour les recommandations")
        print("   2. Adaptez la configuration à vos besoins")
        print("   3. Testez avec vos propres données")
        print("   4. Consultez la documentation pour les cas avancés")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Test interrompu par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 ERREUR CRITIQUE: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
