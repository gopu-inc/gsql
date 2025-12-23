#!/usr/bin/env python3
"""
TEST COMPLET DU MODULE STORAGE GSQL
Analyse de gsql/storage.py et ses composants
"""

import sys
import os
import inspect
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

print("🔬 TEST COMPLET DU STORAGE GSQL")
print("=" * 60)

# 1. IMPORT ET ANALYSE DES COMPOSANTS
try:
    from gsql.storage import (
        SQLiteStorage,
        BufferPool,
        TransactionManager,
        create_storage,
        get_storage_stats
    )
    
    print("✅ Modules storage importés avec succès")
    
    # Analyse des classes
    print("\n📦 CLASSES DISPONIBLES:")
    
    # SQLiteStorage
    print(f"\n🏗️  SQLiteStorage:")
    sig = inspect.signature(SQLiteStorage.__init__)
    print(f"   Signature: __init__{sig}")
    
    # Voir les méthodes principales
    methods = [m for m in dir(SQLiteStorage) if not m.startswith('_') and callable(getattr(SQLiteStorage, m))]
    print(f"   Méthodes ({len(methods)}): {', '.join(sorted(methods))}")
    
    # BufferPool
    print(f"\n🏗️  BufferPool:")
    if BufferPool:
        sig = inspect.signature(BufferPool.__init__)
        print(f"   Signature: __init__{sig}")
    
    # TransactionManager  
    print(f"\n🏗️  TransactionManager:")
    if TransactionManager:
        sig = inspect.signature(TransactionManager.__init__)
        print(f"   Signature: __init__{sig}")
    
    # Fonctions
    print(f"\n🔧 FONCTIONS:")
    print(f"   • create_storage: {create_storage}")
    print(f"   • get_storage_stats: {get_storage_stats}")
    
except ImportError as e:
    print(f"❌ Erreur d'import: {e}")
    sys.exit(1)

# 2. TEST PRATIQUE DE SQLiteStorage
print("\n" + "=" * 60)
print("🧪 TEST PRATIQUE SQLiteStorage")
print("=" * 60)

try:
    # Créer une instance SQLiteStorage
    print("\n1. Création SQLiteStorage:")
    
    # Test avec différentes configurations
    configs = [
        {
            "name": "Base mémoire simple",
            "params": {"db_path": ":memory:", "buffer_pool_size": 50}
        },
        {
            "name": "Base fichier avec WAL",
            "params": {"db_path": "/tmp/test_storage.db", "enable_wal": True, "buffer_pool_size": 100}
        },
        {
            "name": "Base avec auto-recovery",
            "params": {"db_path": "/tmp/test_recovery.db", "auto_recovery": True}
        }
    ]
    
    for config in configs:
        print(f"\n🔹 {config['name']}:")
        print(f"   Paramètres: {config['params']}")
        
        try:
            storage = SQLiteStorage(**config['params'])
            print(f"   ✅ Création réussie")
            
            # Tester les méthodes basiques
            print(f"   🧪 Test des méthodes:")
            
            # execute()
            try:
                result = storage.execute("SELECT 1 as test")
                print(f"     • execute(): {result.get('type', 'unknown')}")
            except Exception as e:
                print(f"     • execute(): ❌ {e}")
            
            # begin_transaction()
            if hasattr(storage, 'begin_transaction'):
                try:
                    storage.begin_transaction()
                    print(f"     • begin_transaction(): ✅")
                except Exception as e:
                    print(f"     • begin_transaction(): ❌ {e}")
            
            # commit()
            if hasattr(storage, 'commit'):
                try:
                    storage.commit()
                    print(f"     • commit(): ✅")
                except Exception as e:
                    print(f"     • commit(): ❌ {e}")
            
            # get_stats()
            if hasattr(storage, 'get_stats'):
                try:
                    stats = storage.get_stats()
                    print(f"     • get_stats(): {stats}")
                except Exception as e:
                    print(f"     • get_stats(): ❌ {e}")
            
            # Fermer proprement
            if hasattr(storage, 'close'):
                storage.close()
                print(f"   🔒 Storage fermé")
            
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
    
except Exception as e:
    print(f"❌ Erreur test SQLiteStorage: {e}")
    import traceback
    traceback.print_exc()

# 3. TEST BUFFERPOOL
print("\n" + "=" * 60)
print("🧠 TEST BUFFERPOOL")
print("=" * 60)

try:
    if BufferPool:
        print("\n1. Création BufferPool:")
        
        sizes = [10, 50, 100, 500]
        for size in sizes:
            try:
                bp = BufferPool(size)
                print(f"   ✅ BufferPool({size}) créé")
                
                # Tester les méthodes
                methods_to_test = ['get', 'put', 'clear', 'size', 'get_stats']
                for method in methods_to_test:
                    if hasattr(bp, method):
                        print(f"     • {method}() disponible")
                
            except Exception as e:
                print(f"   ❌ BufferPool({size}): {e}")
    
    else:
        print("⚠️  BufferPool non disponible")
        
except Exception as e:
    print(f"❌ Erreur BufferPool: {e}")

# 4. TEST TRANSACTION MANAGER
print("\n" + "=" * 60)
print("💼 TEST TRANSACTION MANAGER")
print("=" * 60)

try:
    if TransactionManager:
        print("\n1. Création TransactionManager:")
        
        try:
            tm = TransactionManager()
            print(f"   ✅ TransactionManager créé")
            
            # Tester les méthodes
            tx_methods = ['begin', 'commit', 'rollback', 'savepoint', 'rollback_to_savepoint']
            for method in tx_methods:
                if hasattr(tm, method):
                    print(f"     • {method}() disponible")
            
        except Exception as e:
            print(f"   ❌ TransactionManager: {e}")
    
    else:
        print("⚠️  TransactionManager non disponible")
        
except Exception as e:
    print(f"❌ Erreur TransactionManager: {e}")

# 5. TEST CREATE_STORAGE
print("\n" + "=" * 60)
print("🏭 TEST CREATE_STORAGE")
print("=" * 60)

try:
    if create_storage:
        print("\n1. Fonction create_storage:")
        
        # Tester avec différents backends
        backends = ['sqlite', 'memory']  # À ajuster selon ce qui est disponible
        
        for backend in backends:
            try:
                storage = create_storage(backend=backend, db_path=":memory:")
                if storage:
                    print(f"   ✅ create_storage('{backend}'): {type(storage).__name__}")
                    
                    # Tester une opération basique
                    result = storage.execute("SELECT 1")
                    print(f"     • Test execute: {result.get('success', False)}")
                    
                else:
                    print(f"   ❌ create_storage('{backend}'): retourné None")
                    
            except Exception as e:
                print(f"   ❌ create_storage('{backend}'): {e}")
    
    else:
        print("⚠️  create_storage non disponible")
        
except Exception as e:
    print(f"❌ Erreur create_storage: {e}")

# 6. BENCHMARK DE PERFORMANCE
print("\n" + "=" * 60)
print("📊 BENCHMARK DE PERFORMANCE")
print("=" * 60)

import time

def benchmark_storage():
    """Benchmark du storage"""
    
    try:
        storage = SQLiteStorage(db_path=":memory:", buffer_pool_size=100)
        
        # Créer table de test
        storage.execute("CREATE TABLE benchmark (id INTEGER, data TEXT, value REAL)")
        
        print("\n1. Benchmark INSERT:")
        
        # Test INSERT
        start = time.time()
        for i in range(1000):
            storage.execute(f"INSERT INTO benchmark VALUES ({i}, 'data_{i}', {i * 1.5})")
        insert_time = time.time() - start
        print(f"   ✅ 1000 INSERT: {insert_time:.3f}s ({insert_time/1000:.5f}s par ligne)")
        
        # Test SELECT
        print("\n2. Benchmark SELECT:")
        
        start = time.time()
        result = storage.execute("SELECT COUNT(*) as count, AVG(value) as avg FROM benchmark")
        select_time = time.time() - start
        print(f"   ✅ SELECT agrégat: {select_time:.4f}s")
        
        if result.get('success') and result.get('rows'):
            print(f"   📊 Résultats: {result['rows'][0]}")
        
        # Test BufferPool (si disponible)
        print("\n3. Test cache (si disponible):")
        if hasattr(storage, 'buffer_pool'):
            bp = storage.buffer_pool
            if bp and hasattr(bp, 'get_stats'):
                stats = bp.get_stats()
                print(f"   📈 Stats BufferPool: {stats}")
        
        storage.close()
        
    except Exception as e:
        print(f"❌ Benchmark: {e}")

benchmark_storage()

# 7. TEST YAML_STORAGE (si disponible)
print("\n" + "=" * 60)
print("📁 TEST YAML_STORAGE")
print("=" * 60)

try:
    # Essayer d'importer yaml_storage
    import importlib.util
    
    # Vérifier si le fichier existe
    yaml_path = os.path.join(os.path.dirname(__file__), '..', 'stockage', 'yaml_storage.py')
    
    if os.path.exists(yaml_path):
        print(f"✅ Fichier yaml_storage.py trouvé: {yaml_path}")
        
        # Essayer l'import dynamique
        spec = importlib.util.spec_from_file_location("yaml_storage", yaml_path)
        yaml_module = importlib.util.module_from_spec(spec)
        
        try:
            spec.loader.exec_module(yaml_module)
            print("✅ Module yaml_storage importé dynamiquement")
            
            # Chercher la classe YAMLStorage
            if hasattr(yaml_module, 'YAMLStorage'):
                YAMLStorage = yaml_module.YAMLStorage
                print(f"✅ Classe YAMLStorage trouvée")
                
                # Tester
                try:
                    yaml_storage = YAMLStorage()
                    print(f"✅ Instance YAMLStorage créée")
                    
                    # Tester les méthodes
                    test_methods = ['save', 'load', 'delete', 'list']
                    for method in test_methods:
                        if hasattr(yaml_storage, method):
                            print(f"   • {method}() disponible")
                            
                except Exception as e:
                    print(f"❌ Instance YAMLStorage: {e}")
                    
            else:
                print("⚠️  Classe YAMLStorage non trouvée dans le module")
                
        except Exception as e:
            print(f"❌ Import yaml_storage: {e}")
            
    else:
        print("⚠️  Fichier yaml_storage.py non trouvé")
        
except Exception as e:
    print(f"❌ Test yaml_storage: {e}")

print("\n" + "=" * 60)
print("✅ TEST STORAGE TERMINÉ")
print("=" * 60)
