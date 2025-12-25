#!/usr/bin/env python3
"""
Test Complet GSQL - Teste toutes les fonctionnalités
Usage: python test_gsql.py
"""

import os
import sys
import tempfile
import time
from pathlib import Path

# Ajouter le chemin au module GSQL
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gsql.database import create_database
from gsql.storage import create_storage
from gsql import __version__

# ==================== CONFIGURATION ====================

TEST_DB = tempfile.NamedTemporaryFile(suffix='.db', delete=False).name

# Couleurs pour l'affichage
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

# ==================== UTILITAIRES ====================

def print_header(text):
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{text:^80}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.RESET}")

def print_section(text):
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'-'*80}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}▶ {text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'-'*80}{Colors.RESET}")

def print_test(name, success=True, details=""):
    status = f"{Colors.GREEN}✓{Colors.RESET}" if success else f"{Colors.RED}✗{Colors.RESET}"
    print(f"  {status} {name}")
    if details and not success:
        print(f"    {Colors.YELLOW}{details}{Colors.RESET}")
    return success

def run_test(test_func, *args):
    """Exécute un test avec gestion d'erreur"""
    try:
        return test_func(*args)
    except Exception as e:
        import traceback
        print(f"{Colors.RED}Erreur: {e}{Colors.RESET}")
        if "debug" in sys.argv:
            traceback.print_exc()
        return False

# ==================== TESTS DE BASE ====================

def test_initialization():
    """Teste l'initialisation de la base"""
    print_section("Test d'initialisation")
    
    success_count = 0
    
    # Test 1: Création de la base
    try:
        db = create_database(TEST_DB)
        print_test("Création de la base de données", True)
        success_count += 1
    except Exception as e:
        print_test("Création de la base de données", False, str(e))
        return False
    
    # Test 2: Vérification des tables système
    try:
        result = db.execute("SHOW TABLES")
        if result.get('success'):
            tables = [t['table'] for t in result.get('tables', [])]
            if any('_gsql_' in t for t in tables):
                print_test("Tables système créées", True)
                success_count += 1
            else:
                print_test("Tables système créées", False, "Tables système non trouvées")
        else:
            print_test("Tables système créées", False, result.get('message'))
    except Exception as e:
        print_test("Tables système créées", False, str(e))
    
    # Test 3: Fermeture propre
    try:
        db.close()
        print_test("Fermeture de la base", True)
        success_count += 1
    except Exception as e:
        print_test("Fermeture de la base", False, str(e))
    
    return success_count >= 2

def test_basic_operations():
    """Teste les opérations SQL de base"""
    print_section("Test des opérations SQL de base")
    
    db = create_database(TEST_DB)
    success_count = 0
    
    # Test 1: CREATE TABLE
    try:
        result = db.execute("""
            CREATE TABLE IF NOT EXISTS test_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        if result.get('success'):
            print_test("CREATE TABLE", True)
            success_count += 1
        else:
            print_test("CREATE TABLE", False, result.get('message'))
    except Exception as e:
        print_test("CREATE TABLE", False, str(e))
    
    # Test 2: INSERT
    try:
        result = db.execute("""
            INSERT INTO test_users (username, email, age) 
            VALUES ('test_user1', 'test1@example.com', 25)
        """)
        if result.get('success'):
            print_test("INSERT", True, f"ID: {result.get('lastrowid')}")
            success_count += 1
        else:
            print_test("INSERT", False, result.get('message'))
    except Exception as e:
        print_test("INSERT", False, str(e))
    
    # Test 3: SELECT
    try:
        result = db.execute("SELECT * FROM test_users WHERE username = 'test_user1'")
        if result.get('success') and result.get('count') == 1:
            print_test("SELECT", True, f"{result.get('count')} ligne(s) trouvée(s)")
            success_count += 1
        else:
            print_test("SELECT", False, result.get('message'))
    except Exception as e:
        print_test("SELECT", False, str(e))
    
    # Test 4: UPDATE
    try:
        result = db.execute("UPDATE test_users SET age = 26 WHERE username = 'test_user1'")
        if result.get('success') and result.get('rows_affected') == 1:
            print_test("UPDATE", True, f"{result.get('rows_affected')} ligne(s) modifiée(s)")
            success_count += 1
        else:
            print_test("UPDATE", False, result.get('message'))
    except Exception as e:
        print_test("UPDATE", False, str(e))
    
    # Test 5: DELETE
    try:
        result = db.execute("DELETE FROM test_users WHERE username = 'test_user1'")
        if result.get('success') and result.get('rows_affected') == 1:
            print_test("DELETE", True, f"{result.get('rows_affected')} ligne(s) supprimée(s)")
            success_count += 1
        else:
            print_test("DELETE", False, result.get('message'))
    except Exception as e:
        print_test("DELETE", False, str(e))
    
    # Test 6: DROP TABLE
    try:
        result = db.execute("DROP TABLE test_users")
        if result.get('success'):
            print_test("DROP TABLE", True)
            success_count += 1
        else:
            print_test("DROP TABLE", False, result.get('message'))
    except Exception as e:
        print_test("DROP TABLE", False, str(e))
    
    db.close()
    return success_count >= 5

def test_transactions():
    """Teste les transactions"""
    print_section("Test des transactions")
    
    db = create_database(TEST_DB)
    success_count = 0
    
    # Créer une table de test
    db.execute("""
        CREATE TABLE IF NOT EXISTS test_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            balance REAL NOT NULL DEFAULT 0.0
        )
    """)
    
    # Test 1: Transaction simple avec COMMIT
    try:
        # Début de transaction
        tx_result = db.begin_transaction()
        tid = tx_result.get('tid')
        print_test("BEGIN TRANSACTION", True, f"TID: {tid}")
        success_count += 1
        
        # Opérations dans la transaction
        db.execute("INSERT INTO test_accounts (name, balance) VALUES ('Alice', 1000.0)")
        db.execute("INSERT INTO test_accounts (name, balance) VALUES ('Bob', 500.0)")
        
        # Commit
        db.commit_transaction(tid)
        print_test("COMMIT TRANSACTION", True)
        success_count += 1
        
        # Vérifier
        result = db.execute("SELECT COUNT(*) as count FROM test_accounts")
        if result.get('success') and result['rows'][0]['count'] == 2:
            print_test("Vérification après COMMIT", True, "2 comptes créés")
            success_count += 1
        else:
            print_test("Vérification après COMMIT", False, "Données non persistées")
    
    except Exception as e:
        print_test("Transaction simple", False, str(e))
    
    # Test 2: Transaction avec ROLLBACK
    try:
        # Début de transaction
        tx_result = db.begin_transaction()
        tid = tx_result.get('tid')
        
        # Opération
        db.execute("INSERT INTO test_accounts (name, balance) VALUES ('Charlie', 750.0)")
        
        # Rollback
        db.rollback_transaction(tid)
        print_test("ROLLBACK TRANSACTION", True)
        success_count += 1
        
        # Vérifier que Charlie n'existe pas
        result = db.execute("SELECT * FROM test_accounts WHERE name = 'Charlie'")
        if result.get('success') and result.get('count') == 0:
            print_test("Vérification après ROLLBACK", True, "Charlie n'existe pas")
            success_count += 1
        else:
            print_test("Vérification après ROLLBACK", False, "Rollback non effectif")
    
    except Exception as e:
        print_test("Transaction avec ROLLBACK", False, str(e))
    
    # Test 3: Savepoints
    try:
        tx_result = db.begin_transaction()
        tid = tx_result.get('tid')
        
        # Insertion initiale
        db.execute("INSERT INTO test_accounts (name, balance) VALUES ('David', 300.0)")
        
        # Savepoint 1
        db.create_savepoint(tid, 'sp1')
        print_test("SAVEPOINT", True, "Savepoint 'sp1' créé")
        success_count += 1
        
        # Modification après savepoint
        db.execute("UPDATE test_accounts SET balance = 400.0 WHERE name = 'David'")
        
        # Rollback au savepoint
        db.rollback_transaction(tid, to_savepoint='sp1')
        print_test("ROLLBACK TO SAVEPOINT", True, "Rollback à sp1")
        success_count += 1
        
        # Vérifier que la modification a été annulée
        result = db.execute("SELECT balance FROM test_accounts WHERE name = 'David'")
        if result.get('success') and result['rows'][0]['balance'] == 300.0:
            print_test("Vérification savepoint", True, "Balance restaurée à 300")
            success_count += 1
        
        # Commit
        db.commit_transaction(tid)
    
    except Exception as e:
        print_test("Savepoints", False, str(e))
    
    # Nettoyage
    db.execute("DROP TABLE test_accounts")
    db.close()
    
    return success_count >= 7

def test_special_commands():
    """Teste les commandes spéciales GSQL"""
    print_section("Test des commandes spéciales GSQL")
    
    db = create_database(TEST_DB)
    success_count = 0
    
    # Créer des tables de test
    db.execute("CREATE TABLE test1 (id INT, name TEXT)")
    db.execute("CREATE TABLE test2 (id INT, value TEXT)")
    db.execute("INSERT INTO test1 VALUES (1, 'Test'), (2, 'Demo')")
    
    # Test 1: SHOW TABLES
    try:
        result = db.execute("SHOW TABLES")
        if result.get('success'):
            tables = [t['table'] for t in result.get('tables', [])]
            if 'test1' in tables and 'test2' in tables:
                print_test("SHOW TABLES", True, f"{len(tables)} tables trouvées")
                success_count += 1
            else:
                print_test("SHOW TABLES", False, "Tables de test non trouvées")
        else:
            print_test("SHOW TABLES", False, result.get('message'))
    except Exception as e:
        print_test("SHOW TABLES", False, str(e))
    
    # Test 2: DESCRIBE
    try:
        result = db.execute("DESCRIBE test1")
        if result.get('success'):
            columns = result.get('columns', [])
            if len(columns) == 2:
                print_test("DESCRIBE", True, f"{len(columns)} colonnes trouvées")
                success_count += 1
            else:
                print_test("DESCRIBE", False, f"Attendu 2 colonnes, trouvé {len(columns)}")
        else:
            print_test("DESCRIBE", False, result.get('message'))
    except Exception as e:
        print_test("DESCRIBE", False, str(e))
    
    # Test 3: STATS
    try:
        result = db.execute("STATS")
        if result.get('success'):
            stats = result.get('database', {})
            if 'queries_executed' in stats:
                print_test("STATS", True, "Statistiques récupérées")
                success_count += 1
            else:
                print_test("STATS", False, "Statistiques incomplètes")
        else:
            print_test("STATS", False, result.get('message'))
    except Exception as e:
        print_test("STATS", False, str(e))
    
    # Test 4: VACUUM
    try:
        result = db.execute("VACUUM")
        if result.get('success'):
            print_test("VACUUM", True, "Base optimisée")
            success_count += 1
        else:
            print_test("VACUUM", False, result.get('message'))
    except Exception as e:
        print_test("VACUUM", False, str(e))
    
    # Test 5: BACKUP
    try:
        result = db.execute("BACKUP")
        if result.get('success'):
            backup_file = result.get('backup_file')
            if os.path.exists(backup_file):
                print_test("BACKUP", True, f"Sauvegarde créée: {backup_file}")
                success_count += 1
                # Nettoyer la sauvegarde
                os.remove(backup_file)
            else:
                print_test("BACKUP", False, "Fichier de sauvegarde non créé")
        else:
            print_test("BACKUP", False, result.get('message'))
    except Exception as e:
        print_test("BACKUP", False, str(e))
    
    # Nettoyage
    db.execute("DROP TABLE test1")
    db.execute("DROP TABLE test2")
    db.close()
    
    return success_count >= 4

def test_error_handling():
    """Teste la gestion des erreurs"""
    print_section("Test de la gestion des erreurs")
    
    db = create_database(TEST_DB)
    success_count = 0
    
    # Test 1: Requête SQL invalide
    try:
        result = db.execute("SELECT * FROM table_inexistante")
        if not result.get('success'):
            print_test("Erreur SQL (table inexistante)", True, f"Message: {result.get('error')}")
            success_count += 1
        else:
            print_test("Erreur SQL (table inexistante)", False, "Devrait échouer")
    except Exception as e:
        print_test("Erreur SQL (table inexistante)", False, str(e))
    
    # Test 2: Syntaxe SQL invalide
    try:
        result = db.execute("SELCT * FROM invalid_syntax")
        if not result.get('success'):
            print_test("Erreur de syntaxe SQL", True, f"Message: {result.get('error')}")
            success_count += 1
        else:
            print_test("Erreur de syntaxe SQL", False, "Devrait échouer")
    except Exception as e:
        print_test("Erreur de syntaxe SQL", False, str(e))
    
    # Test 3: Violation de contrainte UNIQUE
    db.execute("CREATE TABLE test_unique (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
    db.execute("INSERT INTO test_unique VALUES (1, 'ABC123')")
    
    try:
        result = db.execute("INSERT INTO test_unique VALUES (2, 'ABC123')")
        if not result.get('success'):
            print_test("Violation UNIQUE constraint", True, f"Message: {result.get('error')}")
            success_count += 1
        else:
            print_test("Violation UNIQUE constraint", False, "Devrait échouer")
    except Exception as e:
        print_test("Violation UNIQUE constraint", False, str(e))
    
    # Test 4: Transaction déjà terminée
    try:
        tx_result = db.begin_transaction()
        tid = tx_result.get('tid')
        db.commit_transaction(tid)
        # Essayer de commit à nouveau
        result = db.commit_transaction(tid)
        if not result:  # Devrait retourner False ou lever une exception
            print_test("Transaction déjà terminée", True, "Commit échoue sur transaction terminée")
            success_count += 1
        else:
            print_test("Transaction déjà terminée", False, "Devrait échouer")
    except Exception as e:
        print_test("Transaction déjà terminée", True, f"Exception attendue: {e}")
        success_count += 1
    
    # Nettoyage
    db.execute("DROP TABLE test_unique")
    db.close()
    
    return success_count >= 3

def test_performance():
    """Teste les performances"""
    print_section("Test de performance")
    
    db = create_database(TEST_DB)
    success_count = 0
    
    # Créer une table de test volumineuse
    db.execute("""
        CREATE TABLE IF NOT EXISTS benchmark (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            value REAL,
            category TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Test 1: Insertion de masse
    try:
        start_time = time.time()
        
        # Insérer 1000 lignes
        for i in range(1000):
            db.execute(
                "INSERT INTO benchmark (name, value, category) VALUES (?, ?, ?)",
                (f"Item_{i}", i * 1.5, f"Category_{i % 10}")
            )
        
        elapsed = time.time() - start_time
        print_test("Insertion de masse (1000 lignes)", True, f"Temps: {elapsed:.2f}s ({elapsed/1000:.4f}s/ligne)")
        success_count += 1
        
    except Exception as e:
        print_test("Insertion de masse", False, str(e))
    
    # Test 2: Sélection avec WHERE
    try:
        start_time = time.time()
        result = db.execute("SELECT COUNT(*) as count FROM benchmark WHERE category = 'Category_5'")
        elapsed = time.time() - start_time
        
        if result.get('success'):
            count = result['rows'][0]['count']
            print_test("SELECT avec WHERE", True, f"{count} lignes en {elapsed:.4f}s")
            success_count += 1
        else:
            print_test("SELECT avec WHERE", False, result.get('message'))
    
    except Exception as e:
        print_test("SELECT avec WHERE", False, str(e))
    
    # Test 3: UPDATE de masse
    try:
        start_time = time.time()
        result = db.execute("UPDATE benchmark SET value = value * 1.1 WHERE category LIKE 'Category_%'")
        elapsed = time.time() - start_time
        
        if result.get('success'):
            rows = result.get('rows_affected', 0)
            print_test("UPDATE de masse", True, f"{rows} lignes en {elapsed:.4f}s")
            success_count += 1
        else:
            print_test("UPDATE de masse", False, result.get('message'))
    
    except Exception as e:
        print_test("UPDATE de masse", False, str(e))
    
    # Test 4: VACUUM performance
    try:
        start_time = time.time()
        result = db.execute("VACUUM")
        elapsed = time.time() - start_time
        
        if result.get('success'):
            print_test("VACUUM", True, f"Temps: {elapsed:.2f}s")
            success_count += 1
        else:
            print_test("VACUUM", False, result.get('message'))
    
    except Exception as e:
        print_test("VACUUM", False, str(e))
    
    # Nettoyage
    db.execute("DROP TABLE benchmark")
    db.close()
    
    return success_count >= 3

def test_concurrency():
    """Teste la concurrence (basique)"""
    print_section("Test de concurrence")
    
    success_count = 0
    
    # Test 1: Connexions multiples
    try:
        # Créer deux connexions indépendantes
        db1 = create_database(TEST_DB)
        db2 = create_database(TEST_DB)
        
        db1.execute("CREATE TABLE IF NOT EXISTS concurrent_test (id INT, data TEXT)")
        
        # Écrire depuis db1
        db1.execute("INSERT INTO concurrent_test VALUES (1, 'From DB1')")
        
        # Lire depuis db2
        result = db2.execute("SELECT * FROM concurrent_test WHERE id = 1")
        
        if result.get('success') and result.get('count') == 1:
            print_test("Connexions multiples", True, "Données partagées entre connexions")
            success_count += 1
        else:
            print_test("Connexions multiples", False, "Pas de partage de données")
        
        # Nettoyage
        db1.execute("DROP TABLE concurrent_test")
        db1.close()
        db2.close()
    
    except Exception as e:
        print_test("Connexions multiples", False, str(e))
    
    # Test 2: Transactions concurrentes (basique)
    try:
        db = create_database(TEST_DB)
        db.execute("CREATE TABLE IF NOT EXISTS tx_test (id INT PRIMARY KEY, balance REAL)")
        db.execute("INSERT INTO tx_test VALUES (1, 1000.0)")
        
        # Transaction 1
        tx1 = db.begin_transaction()
        db.execute("UPDATE tx_test SET balance = balance - 100 WHERE id = 1")
        
        # Transaction 2 (doit attendre)
        try:
            # Essayer IMMEDIATE pour éviter les deadlocks
            tx2 = db.begin_transaction("IMMEDIATE")
            db.execute("SELECT balance FROM tx_test WHERE id = 1")
            db.commit_transaction(tx2)
            print_test("Transactions concurrentes", True, "IMMEDIATE transaction réussie")
            success_count += 1
        except Exception as e:
            print_test("Transactions concurrentes", False, f"Concurrence: {e}")
        
        # Terminer transaction 1
        db.commit_transaction(tx1)
        
        db.execute("DROP TABLE tx_test")
        db.close()
    
    except Exception as e:
        print_test("Transactions concurrentes", False, str(e))
    
    return success_count >= 1

def test_storage_api():
    """Teste l'API de stockage directement"""
    print_section("Test de l'API Storage")
    
    success_count = 0
    
    try:
        # Créer un storage directement
        storage = create_storage(TEST_DB + "_storage")
        
        # Test 1: Exécution SQL
        result = storage.execute("CREATE TABLE storage_test (id INT, name TEXT)")
        if result.get('success'):
            print_test("Storage.execute() - CREATE", True)
            success_count += 1
        else:
            print_test("Storage.execute() - CREATE", False, result.get('error'))
        
        # Test 2: Insertion
        result = storage.execute("INSERT INTO storage_test VALUES (1, 'Test Storage')")
        if result.get('success'):
            print_test("Storage.execute() - INSERT", True, f"ID: {result.get('lastrowid')}")
            success_count += 1
        else:
            print_test("Storage.execute() - INSERT", False, result.get('error'))
        
        # Test 3: Sélection
        result = storage.execute("SELECT * FROM storage_test")
        if result.get('success') and result.get('count') == 1:
            print_test("Storage.execute() - SELECT", True, f"{result.get('count')} ligne(s)")
            success_count += 1
        else:
            print_test("Storage.execute() - SELECT", False, result.get('error'))
        
        # Test 4: get_tables()
        tables = storage.get_tables()
        if any(t['table_name'] == 'storage_test' for t in tables):
            print_test("Storage.get_tables()", True, f"{len(tables)} tables")
            success_count += 1
        else:
            print_test("Storage.get_tables()", False, "Table non trouvée")
        
        # Test 5: get_table_schema()
        schema = storage.get_table_schema('storage_test')
        if schema and len(schema.get('columns', [])) == 2:
            print_test("Storage.get_table_schema()", True, f"{len(schema['columns'])} colonnes")
            success_count += 1
        else:
            print_test("Storage.get_table_schema()", False, "Schéma non trouvé")
        
        # Test 6: Backup
        backup_file = storage.backup("test_backup.db")
        if os.path.exists(backup_file):
            print_test("Storage.backup()", True, f"Backup: {backup_file}")
            success_count += 1
            os.remove(backup_file)
        else:
            print_test("Storage.backup()", False, "Fichier non créé")
        
        # Test 7: Vacuum
        if storage.vacuum():
            print_test("Storage.vacuum()", True, "Optimisation réussie")
            success_count += 1
        else:
            print_test("Storage.vacuum()", False, "Échec d'optimisation")
        
        # Test 8: get_stats()
        stats = storage.get_stats()
        if stats and 'database' in stats:
            print_test("Storage.get_stats()", True, "Statistiques récupérées")
            success_count += 1
        else:
            print_test("Storage.get_stats()", False, "Pas de statistiques")
        
        # Nettoyage
        storage.execute("DROP TABLE storage_test")
        storage.close()
        
        # Supprimer le fichier de test
        os.remove(TEST_DB + "_storage")
    
    except Exception as e:
        print_test("API Storage", False, str(e))
    
    return success_count >= 5

def test_shell_commands():
    """Teste les commandes du shell (simulées)"""
    print_section("Test des commandes Shell")
    
    db = create_database(TEST_DB)
    success_count = 0
    
    # Créer des tables de test
    db.execute("CREATE TABLE shell_test (id INT, cmd TEXT)")
    db.execute("INSERT INTO shell_test VALUES (1, 'test'), (2, 'demo')")
    
    # Simuler les commandes shell
    commands = [
        ("SHOW TABLES", "doit retourner des tables"),
        ("DESCRIBE shell_test", "doit décrire la table"),
        ("SELECT * FROM shell_test", "doit retourner 2 lignes"),
        ("STATS", "doit retourner des statistiques"),
        ("VACUUM", "doit optimiser la base"),
    ]
    
    for cmd, desc in commands:
        try:
            result = db.execute(cmd)
            if result.get('success'):
                print_test(f"Commande: {cmd.split()[0]}", True, desc)
                success_count += 1
            else:
                print_test(f"Commande: {cmd.split()[0]}", False, f"{desc}: {result.get('error')}")
        except Exception as e:
            print_test(f"Commande: {cmd.split()[0]}", False, f"{desc}: {e}")
    
    # Nettoyage
    db.execute("DROP TABLE shell_test")
    db.close()
    
    return success_count >= 4

# ==================== MAIN ====================

def main():
    """Fonction principale"""
    
    print_header(f"Test Complet GSQL v{__version__}")
    print(f"Base de test: {TEST_DB}")
    print(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    tests = [
        ("Initialisation", test_initialization),
        ("Opérations de base", test_basic_operations),
        ("Transactions", test_transactions),
        ("Commandes spéciales", test_special_commands),
        ("Gestion des erreurs", test_error_handling),
        ("Performance", test_performance),
        ("Concurrence", test_concurrency),
        ("API Storage", test_storage_api),
        ("Commandes Shell", test_shell_commands),
    ]
    
    results = []
    total_tests = 0
    passed_tests = 0
    
    for test_name, test_func in tests:
        print(f"\n{Colors.BOLD}{Colors.BLUE}▶▶ Exécution: {test_name}{Colors.RESET}")
        success = run_test(test_func)
        results.append((test_name, success))
        
        if success:
            passed_tests += 1
            print(f"{Colors.GREEN}✓ {test_name}: PASSÉ{Colors.RESET}")
        else:
            print(f"{Colors.RED}✗ {test_name}: ÉCHOUÉ{Colors.RESET}")
        
        total_tests += 1
    
    # ==================== RÉSULTATS ====================
    
    print_header("RÉSULTATS DU TEST")
    
    print(f"\n{Colors.BOLD}Résumé:{Colors.RESET}")
    for test_name, success in results:
        status = f"{Colors.GREEN}PASSÉ{Colors.RESET}" if success else f"{Colors.RED}ÉCHOUÉ{Colors.RESET}"
        print(f"  {test_name:30} {status}")
    
    success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
    
    print(f"\n{Colors.BOLD}Statistiques:{Colors.RESET}")
    print(f"  Tests exécutés: {total_tests}")
    print(f"  Tests réussis: {passed_tests}")
    print(f"  Tests échoués: {total_tests - passed_tests}")
    print(f"  Taux de réussite: {success_rate:.1f}%")
    
    if success_rate >= 80:
        print(f"\n{Colors.BOLD}{Colors.GREEN}✅ SUCCÈS: GSQL est fonctionnel !{Colors.RESET}")
    elif success_rate >= 60:
        print(f"\n{Colors.BOLD}{Colors.YELLOW}⚠️  AVERTISSEMENT: GSQL a des problèmes mineurs{Colors.RESET}")
    else:
        print(f"\n{Colors.BOLD}{Colors.RED}❌ ÉCHEC: GSQL a des problèmes majeurs{Colors.RESET}")
    
    # ==================== NETTOYAGE ====================
    
    try:
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
            print(f"\n{Colors.CYAN}Fichier de test supprimé: {TEST_DB}{Colors.RESET}")
    except:
        pass
    
    # ==================== RECOMMANDATIONS ====================
    
    print_header("RECOMMANDATIONS")
    
    if passed_tests < total_tests:
        print(f"\n{Colors.YELLOW}Tests échoués:{Colors.RESET}")
        for i, (test_name, success) in enumerate(results):
            if not success:
                print(f"  • {test_name}")
        
        print(f"\n{Colors.YELLOW}Actions recommandées:{Colors.RESET}")
        print("  1. Vérifier les logs pour les erreurs détaillées")
        print("  2. Tester manuellement les fonctionnalités échouées")
        print("  3. Vérifier les dépendances (sqlite3, etc.)")
        print("  4. Consulter la documentation pour les tests spécifiques")
    
    print(f"\n{Colors.GREEN}Test terminé à {time.strftime('%H:%M:%S')}{Colors.RESET}")
    
    return passed_tests == total_tests

# ==================== EXÉCUTION ====================

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Test interrompu par l'utilisateur{Colors.RESET}")
        sys.exit(130)
    except Exception as e:
        print(f"\n{Colors.RED}Erreur fatale: {e}{Colors.RESET}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
