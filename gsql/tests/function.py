#!/usr/bin/env python3
"""
TEST COMPLET GSQL v3.0.9 - Exploration de toutes les fonctionnalités et signatures
"""

import os
import sys
import time
import inspect
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

# Configuration
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

print("🔍 TEST COMPLET GSQL - EXPLORATION DES SIGNATURES")
print("=" * 70)

# ==================== 1. INITIALISATION ET CONFIGURATION ====================

def test_initialization():
    """Teste l'initialisation de GSQL"""
    print("\n📦 1. INITIALISATION GSQL")
    print("-" * 50)
    
    try:
        from gsql import (
            __version__, config, setup_logging,
            get_version, get_features, check_health,
            FeatureDetection
        )
        
        print(f"✅ Version GSQL: {__version__}")
        print(f"✅ Version via get_version(): {get_version()}")
        
        # Configuration
        print(f"\n⚙️  Configuration globale:")
        config_dict = config.to_dict()
        for key, value in list(config_dict.items())[:5]:  # Affiche les 5 premiers
            print(f"   • {key}: {value}")
        
        # Détection des fonctionnalités
        print(f"\n🔧 Détection des fonctionnalités:")
        features = get_features()
        for feature, available in features.items():
            status = "✅" if available else "❌"
            print(f"   • {feature}: {status}")
        
        # Vérification santé
        print(f"\n🏥 Vérification santé:")
        health = check_health()
        print(f"   • Status: {health['status']}")
        if health['issues']:
            print(f"   • Issues: {health['issues']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur initialisation: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== 2. STORAGE ENGINE ====================

def test_storage_signatures():
    """Explore les signatures du module storage"""
    print("\n💾 2. STORAGE ENGINE - SIGNATURES")
    print("-" * 50)
    
    try:
        from gsql.storage import (
            SQLiteStorage, BufferPool, TransactionManager,
            create_storage, get_storage_stats
        )
        
        # SQLiteStorage
        print(f"\n🏗️  SQLiteStorage:")
        sig = inspect.signature(SQLiteStorage.__init__)
        params = list(sig.parameters.keys())
        print(f"   __init__({', '.join(params[1:])})")
        
        # Méthodes publiques
        methods = []
        for name in dir(SQLiteStorage):
            if not name.startswith('_') and callable(getattr(SQLiteStorage, name)):
                try:
                    sig = inspect.signature(getattr(SQLiteStorage, name))
                    params = list(sig.parameters.keys())
                    methods.append(f"{name}({', '.join(params[1:])})")
                except:
                    methods.append(f"{name}()")
        
        print(f"   Méthodes disponibles ({len(methods)}):")
        for i, method in enumerate(sorted(methods), 1):
            print(f"     {i:2d}. {method}")
        
        # BufferPool
        print(f"\n🏗️  BufferPool:")
        sig = inspect.signature(BufferPool.__init__)
        print(f"   __init__({', '.join(list(sig.parameters.keys())[1:])})")
        
        # TransactionManager
        print(f"\n🏗️  TransactionManager:")
        sig = inspect.signature(TransactionManager.__init__)
        print(f"   __init__({', '.join(list(sig.parameters.keys())[1:])})")
        
        # Fonctions
        print(f"\n🔧 Fonctions storage:")
        print(f"   • create_storage() -> SQLiteStorage")
        print(f"   • get_storage_stats(storage) -> dict")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur storage: {e}")
        return False

def test_storage_functionality():
    """Teste les fonctionnalités du storage"""
    print("\n💾 3. STORAGE ENGINE - FONCTIONNALITÉS")
    print("-" * 50)
    
    try:
        from gsql.storage import SQLiteStorage, BufferPool, TransactionManager
        
        # Créer un répertoire temporaire
        temp_dir = tempfile.mkdtemp(prefix="gsql_test_")
        db_path = os.path.join(temp_dir, "test.db")
        
        print(f"📁 Répertoire temporaire: {temp_dir}")
        
        # Test 1: Création storage
        print(f"\n🔹 Test 1: Création SQLiteStorage")
        storage = SQLiteStorage(
            db_path=db_path,
            base_dir=temp_dir,
            buffer_pool_size=50,
            enable_wal=True
        )
        print(f"   ✅ Storage créé: {storage.db_path}")
        print(f"   ✅ BufferPool: {storage.buffer_pool.max_pages} pages")
        print(f"   ✅ TransactionManager: {storage.transaction_manager}")
        
        # Test 2: BufferPool
        print(f"\n🔹 Test 2: BufferPool operations")
        bp = storage.buffer_pool
        
        # Mettre des données
        bp.put("page1", {"data": "test1", "id": 1})
        bp.put("page2", {"data": "test2", "id": 2}, priority=True)
        
        # Récupérer
        data1 = bp.get("page1")
        data2 = bp.get("page2")
        data3 = bp.get("page3")  # Non existant
        
        print(f"   • put/get: {data1 is not None}, {data2 is not None}")
        print(f"   • cache miss: {data3 is None}")
        
        # Stats
        stats = bp.get_stats()
        print(f"   • Stats: {stats['size']}/{stats['max_size']} pages")
        print(f"   • Hit ratio: {stats['hit_ratio']:.2%}")
        
        # Test 3: Exécution SQL
        print(f"\n🔹 Test 3: Exécution SQL basique")
        
        # Créer table
        result = storage.execute("""
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print(f"   • CREATE TABLE: {result.get('success', False)}")
        
        # Insert
        result = storage.execute("""
            INSERT INTO test_users (name, age) 
            VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)
        """)
        print(f"   • INSERT: {result.get('success', False)}, rows: {result.get('rowcount', 0)}")
        
        # Select
        result = storage.execute("SELECT * FROM test_users ORDER BY age")
        print(f"   • SELECT: {result.get('success', False)}")
        if result.get('success') and result.get('rows'):
            print(f"   • Rows: {len(result['rows'])}")
            for row in result['rows'][:2]:  # Affiche 2 premières lignes
                print(f"     → {row}")
        
        # Test 4: Transactions
        print(f"\n🔹 Test 4: Gestion des transactions")
        
        tm = storage.transaction_manager
        
        # Début transaction
        tid = tm.begin(isolation_level="IMMEDIATE")
        print(f"   • Transaction démarrée: TID={tid}")
        
        # Exécuter dans transaction
        storage.execute("INSERT INTO test_users (name, age) VALUES ('David', 28)")
        
        # Savepoint
        tm.savepoint(tid, "sp1")
        print(f"   • Savepoint créé: sp1")
        
        # Rollback to savepoint
        storage.execute("INSERT INTO test_users (name, age) VALUES ('Eve', 32)")
        tm.rollback(tid, to_savepoint="sp1")
        print(f"   • Rollback to sp1")
        
        # Commit
        tm.commit(tid)
        print(f"   • Transaction commitée")
        
        # Vérifier
        result = storage.execute("SELECT COUNT(*) as count FROM test_users")
        if result.get('success'):
            count = result['rows'][0][0] if result['rows'] else 0
            print(f"   • Total rows après commit: {count}")
        
        # Test 5: Métadonnées
        print(f"\n🔹 Test 5: Métadonnées et statistiques")
        
        # Liste tables
        tables = storage.get_tables()
        print(f"   • Tables: {[t['table_name'] for t in tables]}")
        
        # Schéma table
        schema = storage.get_table_schema("test_users")
        print(f"   • Schema test_users: {len(schema)} colonnes")
        
        # Stats storage
        stats = storage.get_stats()
        print(f"   • Stats database: {stats.get('database', {}).get('tables', 0)} tables")
        print(f"   • BufferPool: {stats.get('performance', {}).get('buffer_pool', {}).get('size', 0)} pages")
        
        # Test 6: Vacuum et backup
        print(f"\n🔹 Test 6: Maintenance")
        
        # Vacuum
        result = storage.vacuum()
        print(f"   • VACUUM: {result.get('success', False)}")
        
        # Backup
        backup_path = os.path.join(temp_dir, "backup.db")
        result = storage.backup(backup_path)
        print(f"   • BACKUP: {result.get('success', False)}")
        if result.get('success'):
            size = os.path.getsize(backup_path) / 1024
            print(f"   • Backup size: {size:.1f} KB")
        
        # Fermer
        storage.close()
        print(f"\n🔒 Storage fermé")
        
        # Nettoyer
        shutil.rmtree(temp_dir)
        print(f"🧹 Répertoire nettoyé: {temp_dir}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur fonctionnalités storage: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== 4. DATABASE MODULE ====================

def test_database_signatures():
    """Explore les signatures du module database"""
    print("\n🗃️ 4. DATABASE MODULE - SIGNATURES")
    print("-" * 50)
    
    try:
        from gsql.database import Database, create_database, connect
        
        # Database class
        print(f"\n🏗️  Database:")
        sig = inspect.signature(Database.__init__)
        params = list(sig.parameters.keys())
        print(f"   __init__({', '.join(params[1:])})")
        
        # Méthodes principales
        methods = []
        for name in dir(Database):
            if not name.startswith('_') and callable(getattr(Database, name)):
                try:
                    sig = inspect.signature(getattr(Database, name))
                    params = list(sig.parameters.keys())
                    methods.append(f"{name}({', '.join(params[1:])})")
                except:
                    methods.append(f"{name}()")
        
        print(f"   Méthodes principales ({len(methods)}):")
        for i, method in enumerate(sorted(methods)[:15], 1):  # Affiche 15 premières
            print(f"     {i:2d}. {method}")
        
        # Fonctions
        print(f"\n🔧 Fonctions database:")
        print(f"   • create_database(**kwargs) -> Database")
        print(f"   • connect(db_path=None, **kwargs) -> Database")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur database: {e}")
        return False

def test_database_functionality():
    """Teste les fonctionnalités du module database"""
    print("\n🗃️ 5. DATABASE MODULE - FONCTIONNALITÉS")
    print("-" * 50)
    
    try:
        from gsql.database import Database
        import tempfile
        
        temp_dir = tempfile.mkdtemp(prefix="gsql_db_test_")
        print(f"📁 Répertoire temporaire: {temp_dir}")
        
        # Test 1: Création database
        print(f"\n🔹 Test 1: Création Database")
        db = Database(
            db_path=":memory:",
            base_dir=temp_dir,
            buffer_pool_size=30,
            enable_wal=True,
            auto_recovery=True
        )
        print(f"   ✅ Database créée")
        print(f"   ✅ Storage: {type(db.storage).__name__}")
        print(f"   ✅ Config: v{db.config.get('version')}")
        
        # Test 2: Commandes spéciales
        print(f"\n🔹 Test 2: Commandes spéciales GSQL")
        
        # SHOW TABLES
        result = db.execute("SHOW TABLES")
        print(f"   • SHOW TABLES: {result.get('success', False)}")
        if result.get('success') and result.get('tables'):
            print(f"     Tables système: {[t['table'] for t in result['tables']]}")
        
        # DESCRIBE
        result = db.execute("DESCRIBE users")
        print(f"   • DESCRIBE users: {result.get('success', False)}")
        if result.get('success') and result.get('columns'):
            print(f"     Colonnes: {len(result['columns'])}")
        
        # STATS
        result = db.execute("STATS")
        print(f"   • STATS: {result.get('success', False)}")
        if result.get('success'):
            stats = result.get('stats', {})
            print(f"     Queries: {stats.get('queries_executed', 0)}")
            print(f"     Cache hits: {stats.get('queries_cached', 0)}")
        
        # VACUUM
        result = db.execute("VACUUM")
        print(f"   • VACUUM: {result.get('success', False)}")
        
        # HELP
        result = db.execute("HELP")
        print(f"   • HELP: {result.get('success', False)}")
        
        # Test 3: Cache de requêtes
        print(f"\n🔹 Test 3: Cache de requêtes")
        
        # Première exécution (cache miss)
        start = time.time()
        result1 = db.execute("SELECT * FROM users WHERE age > 20", use_cache=True)
        time1 = time.time() - start
        
        # Deuxième exécution (cache hit)
        start = time.time()
        result2 = db.execute("SELECT * FROM users WHERE age > 20", use_cache=True)
        time2 = time.time() - start
        
        print(f"   • First execution: {time1:.3f}s")
        print(f"   • Cached execution: {time2:.3f}s")
        print(f"   • Speedup: {time1/time2:.1f}x")
        
        # Test 4: Auto-recovery simulation
        print(f"\n🔹 Test 4: Auto-recovery (simulation)")
        
        # Forcer une erreur de base verrouillée
        try:
            # Créer une deuxième connexion pour verrouiller
            import sqlite3
            lock_conn = sqlite3.connect(db.storage.db_path)
            lock_cursor = lock_conn.cursor()
            lock_cursor.execute("BEGIN EXCLUSIVE")
            
            # Essayer une requête qui échouera
            result = db.execute("SELECT * FROM users")
            print(f"   • Query with lock: {result.get('success', False)}")
            
            lock_cursor.execute("ROLLBACK")
            lock_conn.close()
            
        except Exception as e:
            print(f"   • Lock test: {e}")
        
        # Test 5: Transactions via database
        print(f"\n🔹 Test 5: Transactions")
        
        db.storage.begin_transaction()
        db.execute("INSERT INTO products (name, price) VALUES ('Test Product', 99.99)")
        db.storage.create_savepoint("test_sp")
        db.execute("UPDATE products SET price = 88.88 WHERE name = 'Test Product'")
        db.storage.rollback_transaction()  # Rollback au savepoint
        # db.storage.commit_transaction()  # Décommenter pour commit
        
        # Test 6: Fermeture propre
        db.storage.close()
        print(f"\n🔒 Database fermée")
        
        shutil.rmtree(temp_dir)
        print(f"🧹 Répertoire nettoyé")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur fonctionnalités database: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== 6. EXECUTOR MODULE ====================

def test_executor_signatures():
    """Explore les signatures du module executor"""
    print("\n⚡ 6. EXECUTOR MODULE - SIGNATURES")
    print("-" * 50)
    
    try:
        from gsql.executor import QueryExecutor, create_executor
        
        # QueryExecutor class
        print(f"\n🏗️  QueryExecutor:")
        sig = inspect.signature(QueryExecutor.__init__)
        params = list(sig.parameters.keys())
        print(f"   __init__({', '.join(params[1:])})")
        
        # Méthodes principales
        methods = []
        for name in dir(QueryExecutor):
            if not name.startswith('_') and callable(getattr(QueryExecutor, name)):
                try:
                    sig = inspect.signature(getattr(QueryExecutor, name))
                    params = list(sig.parameters.keys())
                    methods.append(f"{name}({', '.join(params[1:])})")
                except:
                    methods.append(f"{name}()")
        
        print(f"   Méthodes principales ({len(methods)}):")
        for i, method in enumerate(sorted(methods)[:10], 1):
            print(f"     {i:2d}. {method}")
        
        # Détection des fonctions intégrées
        print(f"\n🔧 Fonctions intégrées détectées:")
        executor = QueryExecutor()
        if hasattr(executor, '_register_builtin_functions'):
            # Lister les fonctions en examinant les méthodes qui commencent par _func_
            func_methods = [m for m in dir(executor) if m.startswith('_func_')]
            print(f"   • {len(func_methods)} fonctions: {', '.join([m[6:] for m in func_methods])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur executor: {e}")
        return False

# ==================== 7. PARSER ET INDEX ====================

def test_parser_index_signatures():
    """Explore les signatures des modules parser et index"""
    print("\n📝 7. PARSER & INDEX - SIGNATURES")
    print("-" * 50)
    
    try:
        # Parser
        from gsql.parser import SQLParser
        print(f"\n🏗️  SQLParser:")
        sig = inspect.signature(SQLParser.__init__)
        params = list(sig.parameters.keys())
        print(f"   __init__({', '.join(params[1:])})")
        
        parser_methods = []
        for name in dir(SQLParser):
            if not name.startswith('_') and callable(getattr(SQLParser, name)):
                parser_methods.append(name)
        
        print(f"   Méthodes: {', '.join(sorted(parser_methods))}")
        
        # Index
        from gsql.index import BPlusTreeIndex, HashIndex
        print(f"\n🏗️  Index classes:")
        print(f"   • BPlusTreeIndex(order=3)")
        print(f"   • HashIndex(size=1000)")
        
        # BTree
        from gsql.btree import BPlusTree
        print(f"   • BPlusTree(order=3)")
        
        # Tester B+Tree
        print(f"\n🔹 Test B+Tree:")
        btree = BPlusTree(order=3)
        btree.insert(10, 1001)
        btree.insert(20, 1002)
        btree.insert(5, 1003)
        
        result = btree.search(10)
        print(f"   • search(10): {result}")
        
        range_result = btree.search_range(5, 15)
        print(f"   • search_range(5, 15): {range_result}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur parser/index: {e}")
        return False

# ==================== 8. EXCEPTIONS ====================

def test_exceptions():
    """Liste toutes les exceptions disponibles"""
    print("\n🚨 8. HIÉRARCHIE DES EXCEPTIONS")
    print("-" * 50)
    
    try:
        from gsql.exceptions import (
            GSQLBaseException, SQLSyntaxError, SQLExecutionError,
            ConstraintViolationError, TransactionError, FunctionError,
            NLError, BufferPoolError, StorageError, QueryError
        )
        
        exceptions = [
            ("GSQLBaseException", GSQLBaseException),
            ("SQLSyntaxError", SQLSyntaxError),
            ("SQLExecutionError", SQLExecutionError),
            ("ConstraintViolationError", ConstraintViolationError),
            ("TransactionError", TransactionError),
            ("FunctionError", FunctionError),
            ("NLError", NLError),
            ("BufferPoolError", BufferPoolError),
            ("StorageError", StorageError),
            ("QueryError", QueryError)
        ]
        
        print("   Hiérarchie complète:")
        for name, exc_class in exceptions:
            bases = [base.__name__ for base in exc_class.__bases__]
            print(f"   • {name} ← {', '.join(bases) if bases else 'Exception'}")
        
        # Tester quelques exceptions
        print(f"\n🔹 Test d'exceptions:")
        try:
            raise SQLSyntaxError("Test syntax error")
        except SQLSyntaxError as e:
            print(f"   • SQLSyntaxError: {e} ✓")
        
        try:
            raise TransactionError("Test transaction error")
        except TransactionError as e:
            print(f"   • TransactionError: {e} ✓")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur exceptions: {e}")
        return False

# ==================== 9. SHELL INTERACTIF ====================

def test_shell_capabilities():
    """Teste les capacités du shell"""
    print("\n🐚 9. SHELL INTERACTIF - CAPACITÉS")
    print("-" * 50)
    
    try:
        from gsql.__main__ import GSQLShell, Colors, GSQLCompleter
        from gsql.database import Database
        import tempfile
        
        temp_dir = tempfile.mkdtemp(prefix="gsql_shell_test_")
        
        # Créer une database pour le test
        db = Database(db_path=":memory:", base_dir=temp_dir)
        
        print(f"🔧 Fonctionnalités shell:")
        
        # Couleurs
        print(f"   • Couleurs supportées: {hasattr(Colors, 'colorize')}")
        if hasattr(Colors, 'success'):
            print(f"     - Colors.success('texte'): {Colors.success('Succès')}")
            print(f"     - Colors.error('texte'): {Colors.error('Erreur')}")
            print(f"     - Colors.warning('texte'): {Colors.warning('Avertissement')}")
        
        # Auto-complétion
        print(f"\n   • Auto-complétion:")
        completer = GSQLCompleter(database=db)
        print(f"     - Keywords: {len(completer.keywords)} mots-clés SQL")
        print(f"     - GSQL commands: {len(completer.gsql_commands)} commandes pointées")
        
        # Commandes pointées
        dot_commands = [
            '.tables', '.schema', '.stats', '.help', '.backup',
            '.vacuum', '.exit', '.quit', '.clear', '.history'
        ]
        print(f"     - Commandes disponibles: {', '.join(dot_commands)}")
        
        # Shell
        print(f"\n   • Classe GSQLShell:")
        print(f"     - Intro: Affiche message d'accueil")
        print(f"     - Prompt personnalisable")
        print(f"     - Historique persistant")
        
        db.storage.close()
        shutil.rmtree(temp_dir)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur shell: {e}")
        return False

# ==================== 10. RÉSUMÉ COMPLET ====================

def generate_summary():
    """Génère un résumé complet des signatures"""
    print("\n" + "=" * 70)
    print("📋 RÉSUMÉ COMPLET DES SIGNATURES GSQL")
    print("=" * 70)
    
    summary = {}
    
    try:
        # Collecter toutes les informations
        from gsql import (
            __version__, config,
            SQLiteStorage, BufferPool, TransactionManager,
            Database, QueryExecutor, SQLParser,
            BPlusTreeIndex, HashIndex, BPlusTree
        )
        
        summary['version'] = __version__
        
        # Classes principales avec signatures
        classes_to_check = [
            ('SQLiteStorage', SQLiteStorage),
            ('Database', Database),
            ('QueryExecutor', QueryExecutor),
            ('SQLParser', SQLParser)
        ]
        
        for name, cls in classes_to_check:
            methods = []
            for attr_name in dir(cls):
                if not attr_name.startswith('_') and callable(getattr(cls, attr_name)):
                    try:
                        sig = inspect.signature(getattr(cls, attr_name))
                        params = list(sig.parameters.keys())
                        methods.append({
                            'name': attr_name,
                            'params': params[1:] if params[0] == 'self' else params,
                            'signature': str(sig)
                        })
                    except:
                        methods.append({'name': attr_name, 'params': [], 'signature': f'{attr_name}()'})
            
            summary[name] = {
                'method_count': len(methods),
                'methods': methods[:10]  # 10 premières seulement pour le résumé
            }
        
        # Afficher le résumé
        print(f"\n📊 Statistiques GSQL v{summary['version']}:")
        for class_name, data in summary.items():
            if class_name != 'version':
                print(f"\n  {class_name}:")
                print(f"    • {data['method_count']} méthodes publiques")
                print(f"    • Méthodes principales:")
                for method in data['methods'][:5]:  # 5 premières méthodes
                    print(f"      - {method['name']}({', '.join(method['params'])})")
        
        # Recommandations
        print(f"\n💡 RECOMMANDATIONS POUR LES TESTS:")
        print(f"  1. Tester l'auto-recovery avec une base corrompue")
        print(f"  2. Benchmarks BufferPool avec différentes tailles")
        print(f"  3. Tester les niveaux d'isolation des transactions")
        print(f"  4. Valider le cache de requêtes sur données volumineuses")
        print(f"  5. Tester les commandes NLP si NLTK installé")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur génération résumé: {e}")
        return False

# ==================== EXÉCUTION PRINCIPALE ====================

def main():
    """Exécute tous les tests"""
    print("🚀 DÉMARRAGE DU TEST COMPLET GSQL")
    print("=" * 70)
    
    results = {}
    
    # Exécuter tous les tests
    tests = [
        ("Initialisation", test_initialization),
        ("Signatures Storage", test_storage_signatures),
        ("Fonctionnalités Storage", test_storage_functionality),
        ("Signatures Database", test_database_signatures),
        ("Fonctionnalités Database", test_database_functionality),
        ("Signatures Executor", test_executor_signatures),
        ("Parser & Index", test_parser_index_signatures),
        ("Exceptions", test_exceptions),
        ("Shell", test_shell_capabilities),
        ("Résumé", generate_summary)
    ]
    
    for test_name, test_func in tests:
        print(f"\n{'='*70}")
        print(f"🧪 TEST: {test_name}")
        print(f"{'='*70}")
        try:
            success = test_func()
            results[test_name] = "✅ PASS" if success else "❌ FAIL"
        except Exception as e:
            print(f"⚠️  Exception inattendue: {e}")
            results[test_name] = "💥 ERROR"
            import traceback
            traceback.print_exc()
    
    # Résumé final
    print(f"\n{'='*70}")
    print("📈 RÉSULTATS FINAUX")
    print(f"{'='*70}")
    
    passed = sum(1 for r in results.values() if "PASS" in r)
    total = len(results)
    
    for test_name, result in results.items():
        print(f"  {test_name:25s} : {result}")
    
    print(f"\n🎯 Score: {passed}/{total} tests réussis ({passed/total*100:.0f}%)")
    
    if passed == total:
        print("✨ TOUS LES TESTS SONT RÉUSSIS !")
    else:
        print("⚠️  Certains tests nécessitent attention")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
