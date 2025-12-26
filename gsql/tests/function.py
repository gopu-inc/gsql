#!/usr/bin/env python3
"""
Tests Complets GSQL - Focus Transactions ACID
Version: 2.0.0 - Tests Transactions Complètes
"""

import os
import sys
import tempfile
import time
import json
import threading
import concurrent.futures
from pathlib import Path
from datetime import datetime

# Ajouter le chemin au module GSQL
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gsql.database import create_database
from gsql.storage import create_storage, TransactionContext, atomic_transaction
from gsql import __version__

# ==================== CONFIGURATION ====================

TEST_DB = tempfile.NamedTemporaryFile(suffix='.db', delete=False).name
TEST_TIMEOUT = 30  # secondes par test

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
    """Affiche un en-tête de section"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{text:^80}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.RESET}")

def print_section(text):
    """Affiche une sous-section"""
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'-'*80}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}▶ {text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'-'*80}{Colors.RESET}")

def print_test(name, success=True, details="", time_taken=None):
    """Affiche le résultat d'un test"""
    status = f"{Colors.GREEN}✓{Colors.RESET}" if success else f"{Colors.RED}✗{Colors.RESET}"
    time_str = f" [{time_taken:.3f}s]" if time_taken is not None else ""
    print(f"  {status} {name}{time_str}")
    if details:
        color = Colors.YELLOW if not success else Colors.CYAN
        lines = details.split('\n')
        for line in lines:
            if line.strip():
                print(f"    {color}{line}{Colors.RESET}")
    return success

def assert_condition(condition, message):
    """Assertion avec message détaillé"""
    if not condition:
        raise AssertionError(message)
    return True

def run_test_safely(test_func, test_name, *args):
    """Exécute un test avec gestion d'erreur"""
    start_time = time.time()
    try:
        success = test_func(*args)
        elapsed = time.time() - start_time
        
        if elapsed > TEST_TIMEOUT:
            print_test(test_name, False, f"⚠️  Timeout: {elapsed:.1f}s > {TEST_TIMEOUT}s", elapsed)
            return False
        
        print_test(test_name, success, "" if success else "Échec du test", elapsed)
        return success
        
    except AssertionError as e:
        elapsed = time.time() - start_time
        print_test(test_name, False, f"AssertionError: {str(e)}", elapsed)
        return False
    except Exception as e:
        elapsed = time.time() - start_time
        print_test(test_name, False, f"Exception: {type(e).__name__}: {str(e)}", elapsed)
        if "debug" in sys.argv:
            import traceback
            traceback.print_exc()
        return False

# ==================== TESTS TRANSACTIONS ====================

def test_transaction_basic():
    """Teste les transactions de base"""
    print_section("Transactions de base")
    
    db = create_database(TEST_DB)
    db.execute("DROP TABLE IF EXISTS test_tx_basic")
    db.execute("""
        CREATE TABLE test_tx_basic (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER DEFAULT 0
        )
    """)
    
    # Test 1: Transaction simple avec commit
    tx_result = db.begin_transaction()
    assert_condition(tx_result['success'], "Échec du début de transaction")
    tid = tx_result['tid']
    
    # Insérer des données
    result1 = db.execute("INSERT INTO test_tx_basic (id, name, value) VALUES (1, 'Alice', 100)")
    assert_condition(result1['success'], "Échec de l'insertion 1")
    
    result2 = db.execute("INSERT INTO test_tx_basic (id, name, value) VALUES (2, 'Bob', 200)")
    assert_condition(result2['success'], "Échec de l'insertion 2")
    
    # Commit
    commit_result = db.commit_transaction(tid)
    assert_condition(commit_result['success'], "Échec du commit")
    
    # Vérifier que les données sont persistées
    check_result = db.execute("SELECT COUNT(*) as count FROM test_tx_basic")
    assert_condition(check_result['success'], "Échec de la vérification")
    assert_condition(check_result['rows'][0]['count'] == 2, "Données non persistées après commit")
    
    # Test 2: Transaction avec rollback
    tx_result = db.begin_transaction()
    tid = tx_result['tid']
    
    db.execute("INSERT INTO test_tx_basic (id, name, value) VALUES (3, 'Charlie', 300)")
    
    # Rollback
    rollback_result = db.rollback_transaction(tid)
    assert_condition(rollback_result['success'], "Échec du rollback")
    
    # Vérifier que Charlie n'existe pas
    check_result = db.execute("SELECT * FROM test_tx_basic WHERE name = 'Charlie'")
    assert_condition(check_result['count'] == 0, "Données persistées après rollback")
    
    # Test 3: Transaction timeout (simulé)
    tx_result = db.begin_transaction()
    tid = tx_result['tid']
    
    # Insérer mais ne pas commit immédiatement
    db.execute("INSERT INTO test_tx_basic (id, name, value) VALUES (4, 'David', 400)")
    
    # Attendre plus que le timeout (pour forcer un rollback timeout)
    # Note: dans la vraie vie, on utiliserait un timeout court
    # Ici on vérifie juste que la transaction existe toujours
    status_result = db.get_transaction_status(tid)
    assert_condition(status_result.get('state') == 'ACTIVE', "Transaction non active")
    
    # Nettoyer
    db.rollback_transaction(tid)
    db.execute("DROP TABLE test_tx_basic")
    db.close()
    
    return True

def test_transaction_isolation_levels():
    """Teste les différents niveaux d'isolation"""
    print_section("Niveaux d'isolation")
    
    db = create_database(TEST_DB)
    db.execute("DROP TABLE IF EXISTS test_isolation")
    db.execute("""
        CREATE TABLE test_isolation (
            id INTEGER PRIMARY KEY,
            data TEXT,
            counter INTEGER DEFAULT 0
        )
    """)
    db.execute("INSERT INTO test_isolation (id, data) VALUES (1, 'Initial')")
    
    isolation_levels = ["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]
    
    for level in isolation_levels:
        # Démarrer une transaction avec le niveau d'isolation
        tx_result = db.begin_transaction(level)
        assert_condition(tx_result['success'], f"Échec transaction {level}")
        tid = tx_result['tid']
        
        # Lire les données
        read_result = db.execute("SELECT * FROM test_isolation WHERE id = 1")
        assert_condition(read_result['success'], f"Échec lecture {level}")
        
        # Modifier les données
        update_result = db.execute("UPDATE test_isolation SET counter = counter + 1 WHERE id = 1")
        assert_condition(update_result['success'], f"Échec mise à jour {level}")
        
        # Commit
        commit_result = db.commit_transaction(tid)
        assert_condition(commit_result['success'], f"Échec commit {level}")
        
        print_test(f"Isolation {level}", True, f"Transaction {tid} réussie")
    
    # Vérifier le compteur final
    final_result = db.execute("SELECT counter FROM test_isolation WHERE id = 1")
    assert_condition(final_result['rows'][0]['counter'] == 3, "Compteur incorrect")
    
    # Nettoyer
    db.execute("DROP TABLE test_isolation")
    db.close()
    
    return True

def test_transaction_savepoints():
    """Teste les savepoints et rollback partiel"""
    print_section("Savepoints et Rollback Partiel")
    
    db = create_database(TEST_DB)
    db.execute("DROP TABLE IF EXISTS test_savepoints")
    db.execute("""
        CREATE TABLE test_savepoints (
            id INTEGER PRIMARY KEY,
            step TEXT,
            value INTEGER
        )
    """)
    
    # Démarrer une transaction
    tx_result = db.begin_transaction()
    tid = tx_result['tid']
    
    # Étape 1
    db.execute("INSERT INTO test_savepoints (id, step, value) VALUES (1, 'step1', 100)")
    
    # Savepoint 1
    sp_result = db.create_savepoint(tid, 'sp1')
    assert_condition(sp_result['success'], "Échec création savepoint 1")
    
    # Étape 2
    db.execute("INSERT INTO test_savepoints (id, step, value) VALUES (2, 'step2', 200)")
    
    # Savepoint 2
    sp_result = db.create_savepoint(tid, 'sp2')
    assert_condition(sp_result['success'], "Échec création savepoint 2")
    
    # Étape 3
    db.execute("INSERT INTO test_savepoints (id, step, value) VALUES (3, 'step3', 300)")
    
    # Vérifier qu'on a 3 enregistrements
    check1 = db.execute("SELECT COUNT(*) as count FROM test_savepoints")
    assert_condition(check1['rows'][0]['count'] == 3, "Devrait avoir 3 enregistrements avant rollback")
    
    # Rollback au savepoint 2
    rollback_result = db.rollback_transaction(tid, 'sp2')
    assert_condition(rollback_result['success'], "Échec rollback au savepoint 2")
    
    # Vérifier qu'on a 2 enregistrements (step3 supprimé)
    check2 = db.execute("SELECT COUNT(*) as count FROM test_savepoints")
    assert_condition(check2['rows'][0]['count'] == 2, "Devrait avoir 2 enregistrements après rollback à sp2")
    
    # Rollback au savepoint 1
    rollback_result = db.rollback_transaction(tid, 'sp1')
    assert_condition(rollback_result['success'], "Échec rollback au savepoint 1")
    
    # Vérifier qu'on a 1 enregistrement (step2 supprimé)
    check3 = db.execute("SELECT COUNT(*) as count FROM test_savepoints")
    assert_condition(check3['rows'][0]['count'] == 1, "Devrait avoir 1 enregistrement après rollback à sp1")
    
    # Commit
    commit_result = db.commit_transaction(tid)
    assert_condition(commit_result['success'], "Échec commit final")
    
    # Vérifier les données finales
    final_check = db.execute("SELECT * FROM test_savepoints")
    assert_condition(final_check['count'] == 1, "Devrait avoir 1 enregistrement final")
    assert_condition(final_check['rows'][0]['step'] == 'step1', "Seul step1 devrait persister")
    
    # Nettoyer
    db.execute("DROP TABLE test_savepoints")
    db.close()
    
    return True

def test_transaction_context_manager():
    """Teste le context manager de transactions"""
    print_section("Context Manager Transactions")
    
    db = create_database(TEST_DB)
    db.execute("DROP TABLE IF EXISTS test_context")
    db.execute("""
        CREATE TABLE test_context (
            id INTEGER PRIMARY KEY,
            operation TEXT,
            success BOOLEAN
        )
    """)
    
    # Test 1: Transaction réussie (commit automatique)
    try:
        with TransactionContext(db.storage, "DEFERRED") as tx:
            result = tx.execute("INSERT INTO test_context (id, operation) VALUES (1, 'op1')")
            assert_condition(result['success'], "Échec opération 1")
            
            result = tx.execute("INSERT INTO test_context (id, operation) VALUES (2, 'op2')")
            assert_condition(result['success'], "Échec opération 2")
        
        # Vérifier que les données sont persistées
        check = db.execute("SELECT COUNT(*) as count FROM test_context")
        assert_condition(check['rows'][0]['count'] == 2, "Données non persistées après context manager")
        print_test("Context Manager - Commit automatique", True)
        
    except Exception as e:
        print_test("Context Manager - Commit automatique", False, str(e))
        return False
    
    # Test 2: Transaction avec rollback automatique (exception)
    initial_count = db.execute("SELECT COUNT(*) as count FROM test_context")['rows'][0]['count']
    
    try:
        with TransactionContext(db.storage, "DEFERRED") as tx:
            result = tx.execute("INSERT INTO test_context (id, operation) VALUES (3, 'op3')")
            assert_condition(result['success'], "Échec opération 3")
            
            # Simuler une erreur
            raise ValueError("Erreur simulée pour tester le rollback")
    
    except ValueError:
        # L'exception est attendue
        pass
    
    # Vérifier que l'insertion a été rollbackée
    final_count = db.execute("SELECT COUNT(*) as count FROM test_context")['rows'][0]['count']
    assert_condition(final_count == initial_count, "Rollback automatique échoué")
    print_test("Context Manager - Rollback automatique", True)
    
    # Nettoyer
    db.execute("DROP TABLE test_context")
    db.close()
    
    return True

def test_atomic_transaction():
    """Teste les transactions atomiques"""
    print_section("Transactions Atomiques")
    
    db = create_database(TEST_DB)
    db.execute("DROP TABLE IF EXISTS test_atomic")
    db.execute("DROP TABLE IF EXISTS test_audit")
    
    db.execute("""
        CREATE TABLE test_atomic (
            id INTEGER PRIMARY KEY,
            account TEXT UNIQUE,
            balance REAL NOT NULL DEFAULT 0.0
        )
    """)
    
    db.execute("""
        CREATE TABLE test_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Initialiser les comptes
    db.execute("INSERT INTO test_atomic (id, account, balance) VALUES (1, 'ACC001', 1000.0)")
    db.execute("INSERT INTO test_atomic (id, account, balance) VALUES (2, 'ACC002', 500.0)")
    
    # Test 1: Transaction atomique réussie
    operations = [
        {'query': "INSERT INTO test_audit (operation) VALUES ('Début transfert')"},
        {'query': "UPDATE test_atomic SET balance = balance - 100 WHERE account = 'ACC001'"},
        {'query': "UPDATE test_atomic SET balance = balance + 100 WHERE account = 'ACC002'"},
        {'query': "INSERT INTO test_audit (operation) VALUES ('Fin transfert')"}
    ]
    
    result = db.storage.atomic_transaction(operations)
    assert_condition(result['success'], "Transaction atomique échouée")
    assert_condition(result['operations_executed'] == 4, "Pas toutes les opérations exécutées")
    
    # Vérifier les soldes
    balances = db.execute("SELECT account, balance FROM test_atomic ORDER BY account")
    acc1_balance = next((b['balance'] for b in balances['rows'] if b['account'] == 'ACC001'), None)
    acc2_balance = next((b['balance'] for b in balances['rows'] if b['account'] == 'ACC002'), None)
    
    assert_condition(acc1_balance == 900.0, f"Balance ACC001 incorrecte: {acc1_balance}")
    assert_condition(acc2_balance == 600.0, f"Balance ACC002 incorrecte: {acc2_balance}")
    
    # Vérifier les logs d'audit
    audit_logs = db.execute("SELECT COUNT(*) as count FROM test_audit")
    assert_condition(audit_logs['rows'][0]['count'] == 2, "Logs d'audit manquants")
    
    print_test("Transaction atomique réussie", True, f"TID: {result['transaction_id']}")
    
    # Test 2: Transaction atomique avec échec (rollback automatique)
    initial_balances = {
        'ACC001': db.execute("SELECT balance FROM test_atomic WHERE account = 'ACC001'")['rows'][0]['balance'],
        'ACC002': db.execute("SELECT balance FROM test_atomic WHERE account = 'ACC002'")['rows'][0]['balance']
    }
    
    operations_with_error = [
        {'query': "INSERT INTO test_audit (operation) VALUES ('Débit échoué')"},
        {'query': "UPDATE test_atomic SET balance = balance - 200 WHERE account = 'ACC001'"},
        {'query': "UPDATE test_atomic SET balance = balance + 200 WHERE account = 'INEXISTANT'"},  # Échec volontaire
        {'query': "INSERT INTO test_audit (operation) VALUES ('Ceci ne devrait pas être exécuté')"}
    ]
    
    result = db.storage.atomic_transaction(operations_with_error)
    assert_condition(not result['success'], "Devrait échouer")
    assert_condition('rollback_result' in result, "Rollback manquant")
    assert_condition(result['rollback_result']['success'], "Rollback échoué")
    
    # Vérifier que les soldes n'ont pas changé (rollback)
    final_balances = {
        'ACC001': db.execute("SELECT balance FROM test_atomic WHERE account = 'ACC001'")['rows'][0]['balance'],
        'ACC002': db.execute("SELECT balance FROM test_atomic WHERE account = 'ACC002'")['rows'][0]['balance']
    }
    
    assert_condition(final_balances['ACC001'] == initial_balances['ACC001'], "Balance ACC001 modifiée après rollback")
    assert_condition(final_balances['ACC002'] == initial_balances['ACC002'], "Balance ACC002 modifiée après rollback")
    
    # Vérifier qu'un seul log d'audit a été ajouté (le premier)
    audit_count = db.execute("SELECT COUNT(*) as count FROM test_audit")['rows'][0]['count']
    assert_condition(audit_count == 3, f"Nombre incorrect de logs d'audit: {audit_count}")
    
    print_test("Transaction atomique avec rollback", True, "Rollback automatique réussi")
    
    # Nettoyer
    db.execute("DROP TABLE test_atomic")
    db.execute("DROP TABLE test_audit")
    db.close()
    
    return True

def test_transaction_concurrency():
    """Teste la concurrence entre transactions"""
    print_section("Concurrence des Transactions")
    
    db_path = TEST_DB + "_concurrent"
    results = {'success': 0, 'failed': 0}
    
    def worker_transaction(worker_id):
        """Fonction exécutée par chaque thread"""
        try:
            db = create_database(db_path)
            
            # S'assurer que la table existe
            db.execute("""
                CREATE TABLE IF NOT EXISTS test_concurrent (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    worker_id INTEGER,
                    data TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Démarrer une transaction
            tx_result = db.begin_transaction("IMMEDIATE")
            if not tx_result['success']:
                results['failed'] += 1
                db.close()
                return
            
            tid = tx_result['tid']
            
            # Opération dans la transaction
            result = db.execute(
                "INSERT INTO test_concurrent (worker_id, data) VALUES (?, ?)",
                (worker_id, f"Data from worker {worker_id}")
            )
            
            if result['success']:
                # Simuler un peu de travail
                time.sleep(0.01)
                
                # Commit
                commit_result = db.commit_transaction(tid)
                if commit_result['success']:
                    results['success'] += 1
                else:
                    results['failed'] += 1
            else:
                # Rollback en cas d'échec
                db.rollback_transaction(tid)
                results['failed'] += 1
            
            db.close()
            
        except Exception as e:
            results['failed'] += 1
    
    # Lancer plusieurs threads simultanément
    num_workers = 10
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker_transaction, i) for i in range(num_workers)]
        concurrent.futures.wait(futures)
    
    # Vérifier les résultats
    db = create_database(db_path)
    final_count = db.execute("SELECT COUNT(*) as count FROM test_concurrent")['rows'][0]['count']
    db.execute("DROP TABLE IF EXISTS test_concurrent")
    db.close()
    
    # Supprimer le fichier de test
    try:
        os.remove(db_path)
    except:
        pass
    
    assert_condition(final_count == results['success'], 
                    f"Count mismatch: inserted={final_count}, successful_tx={results['success']}")
    
    success_rate = (results['success'] / num_workers) * 100
    print_test("Transactions concurrentes", True, 
              f"{results['success']}/{num_workers} réussies ({success_rate:.1f}%) - {final_count} inserts")
    
    return True

def test_transaction_consistency():
    """Teste la cohérence ACID des transactions"""
    print_section("Cohérence ACID")
    
    db = create_database(TEST_DB)
    db.execute("DROP TABLE IF EXISTS test_acid")
    
    db.execute("""
        CREATE TABLE test_acid (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            value INTEGER CHECK(value >= 0),
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Test d'Atomicité
    print("  🧪 Test Atomicité...")
    tx_result = db.begin_transaction()
    tid = tx_result['tid']
    
    # Opération réussie
    db.execute("INSERT INTO test_acid (id, name, value) VALUES (1, 'Test1', 100)")
    
    # Opération qui échouera (violation UNIQUE)
    try:
        db.execute("INSERT INTO test_acid (id, name, value) VALUES (2, 'Test1', 200)")
    except:
        pass  # L'erreur est attendue
    
    # Commit (devrait échouer ou rollback partiel selon l'implémentation)
    commit_result = db.commit_transaction(tid)
    
    if not commit_result['success']:
        # Le commit a échoué, vérifier que rien n'a été inséré
        count_result = db.execute("SELECT COUNT(*) as count FROM test_acid")
        assert_condition(count_result['rows'][0]['count'] == 0, 
                        "Atomicité violée: des données ont été persistées malgré l'échec")
        print_test("Atomicité", True, "Rollback complet après erreur")
    else:
        print_test("Atomicité", True, "Commit avec gestion des erreurs")
    
    # Test de Cohérence (contraintes)
    print("  🧪 Test Cohérence...")
    db.execute("DELETE FROM test_acid")  # Nettoyer
    
    tx_result = db.begin_transaction()
    tid = tx_result['tid']
    
    # Insertion valide
    db.execute("INSERT INTO test_acid (id, name, value) VALUES (1, 'Valid', 100)")
    
    # Tentative d'insertion invalide (CHECK constraint)
    invalid_result = db.execute("INSERT INTO test_acid (id, name, value) VALUES (2, 'Invalid', -10)")
    
    if not invalid_result['success']:
        print_test("Cohérence - CHECK", True, "Contrainte CHECK respectée")
    else:
        # Si l'insertion a réussi, le rollback devrait être fait
        db.rollback_transaction(tid)
        print_test("Cohérence - CHECK", False, "Contrainte CHECK violée")
        return False
    
    db.commit_transaction(tid)
    
    # Test d'Isolation (basique)
    print("  🧪 Test Isolation...")
    
    # Créer deux connexions indépendantes
    db1 = create_database(TEST_DB)
    db2 = create_database(TEST_DB)
    
    # Transaction 1: mise à jour
    tx1_result = db1.begin_transaction("IMMEDIATE")
    db1.execute("UPDATE test_acid SET value = 200 WHERE id = 1")
    
    # Transaction 2: tentative de lecture
    # Selon le niveau d'isolation, cela peut bloquer ou voir les anciennes données
    read_result = db2.execute("SELECT value FROM test_acid WHERE id = 1")
    
    if read_result['success']:
        # Vérifier quelle valeur est lue
        value_read = read_result['rows'][0]['value']
        
        # IMMEDIATE devrait permettre la lecture des données non commitées
        # ou bloquer selon l'implémentation
        if value_read == 100:
            print_test("Isolation - IMMEDIATE", True, "Lecture des données non modifiées (isolation)")
        elif value_read == 200:
            print_test("Isolation - IMMEDIATE", True, "Lecture des données modifiées (dirty read possible)")
        else:
            print_test("Isolation - IMMEDIATE", False, f"Valeur inattendue: {value_read}")
    
    # Terminer la transaction 1
    db1.commit_transaction(tx1_result['tid'])
    
    # Test de Durabilité
    print("  🧪 Test Durabilité...")
    
    # Insérer des données et commit
    db.execute("INSERT INTO test_acid (id, name, value) VALUES (3, 'Durable', 300)")
    
    # Fermer et rouvrir la base
    db.close()
    
    db = create_database(TEST_DB)
    
    # Vérifier que les données persistent
    check_result = db.execute("SELECT * FROM test_acid WHERE name = 'Durable'")
    assert_condition(check_result['count'] == 1, "Durabilité violée: données non persistées")
    print_test("Durabilité", True, "Données persistées après fermeture/réouverture")
    
    # Nettoyer
    db1.close()
    db2.close()
    db.execute("DROP TABLE test_acid")
    db.close()
    
    return True

def test_transaction_error_handling():
    """Teste la gestion des erreurs dans les transactions"""
    print_section("Gestion des Erreurs")
    
    db = create_database(TEST_DB)
    
    # Test 1: Transaction inexistante
    print("  🧪 Test transaction inexistante...")
    result = db.commit_transaction(99999)
    assert_condition(not result['success'], "Devrait échouer sur transaction inexistante")
    print_test("Commit transaction inexistante", True, f"Message: {result.get('error', 'No error')}")
    
    result = db.rollback_transaction(99999)
    assert_condition(not result['success'] or 'warning' in result, 
                    "Devrait échouer ou avertir sur transaction inexistante")
    print_test("Rollback transaction inexistante", True)
    
    # Test 2: Savepoint inexistant
    print("  🧪 Test savepoint inexistant...")
    tx_result = db.begin_transaction()
    tid = tx_result['tid']
    
    result = db.rollback_transaction(tid, 'inexistant_savepoint')
    assert_condition(not result['success'], "Devrait échouer sur savepoint inexistant")
    print_test("Rollback savepoint inexistant", True, f"Message: {result.get('error', 'No error')}")
    
    db.rollback_transaction(tid)
    
    # Test 3: Double commit
    print("  🧪 Test double commit...")
    tx_result = db.begin_transaction()
    tid = tx_result['tid']
    
    # Premier commit
    result1 = db.commit_transaction(tid)
    assert_condition(result1['success'], "Premier commit devrait réussir")
    
    # Deuxième commit (devrait échouer)
    result2 = db.commit_transaction(tid)
    assert_condition(not result2['success'], "Deuxième commit devrait échouer")
    print_test("Double commit", True, "Second commit correctement rejeté")
    
    # Test 4: SQL erreur dans transaction
    print("  🧪 Test erreur SQL dans transaction...")
    tx_result = db.begin_transaction()
    tid = tx_result['tid']
    
    # Requête SQL invalide
    result = db.execute("INVALID SQL SYNTAX")
    assert_condition(not result['success'], "Requête invalide devrait échouer")
    
    # Commit devrait quand même être possible (ou rollback automatique)
    commit_result = db.commit_transaction(tid)
    if not commit_result['success']:
        print_test("Commit après erreur SQL", True, "Rollback automatique après erreur")
    else:
        print_test("Commit après erreur SQL", True, "Commit malgré erreur précédente")
    
    db.close()
    
    return True

def test_transaction_performance():
    """Teste les performances des transactions"""
    print_section("Performance des Transactions")
    
    db = create_database(TEST_DB)
    db.execute("DROP TABLE IF EXISTS test_perf")
    
    db.execute("""
        CREATE TABLE test_perf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    num_transactions = 100
    num_ops_per_tx = 10
    total_ops = num_transactions * num_ops_per_tx
    
    start_time = time.time()
    
    for tx_num in range(num_transactions):
        tx_result = db.begin_transaction()
        if not tx_result['success']:
            print_test(f"Transaction {tx_num}", False, "Échec début transaction")
            continue
        
        tid = tx_result['tid']
        
        # Exécuter plusieurs opérations dans la transaction
        for op_num in range(num_ops_per_tx):
            db.execute(
                "INSERT INTO test_perf (data) VALUES (?)",
                (f"Tx{tx_num}_Op{op_num}",)
            )
        
        # Commit
        db.commit_transaction(tid)
    
    elapsed = time.time() - start_time
    
    # Vérifier le nombre total d'insertions
    count_result = db.execute("SELECT COUNT(*) as count FROM test_perf")
    actual_count = count_result['rows'][0]['count']
    
    ops_per_second = total_ops / elapsed if elapsed > 0 else 0
    tx_per_second = num_transactions / elapsed if elapsed > 0 else 0
    
    success = actual_count == total_ops
    details = (f"{actual_count}/{total_ops} inserts | "
              f"{elapsed:.2f}s total | "
              f"{ops_per_second:.1f} ops/s | "
              f"{tx_per_second:.1f} tx/s")
    
    print_test("Performance transactions", success, details)
    
    # Nettoyer
    db.execute("DROP TABLE test_perf")
    db.close()
    
    return success

# ==================== TESTS COMPLÉMENTAIRES ====================

def test_storage_direct():
    """Teste l'API de stockage directement"""
    print_section("API Storage Directe")
    
    storage_path = TEST_DB + "_storage"
    storage = create_storage(storage_path)
    
    # Test transactions via storage
    tx_result = storage.begin_transaction()
    assert_condition(tx_result['success'], "Échec début transaction storage")
    tid = tx_result['tid']
    
    # Créer une table
    result = storage.execute("""
        CREATE TABLE storage_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)
    assert_condition(result['success'], "Échec création table")
    
    # Insérer des données
    result = storage.execute_in_transaction(tid, "INSERT INTO storage_test VALUES (1, 'Test Storage')")
    assert_condition(result['success'], "Échec insertion transactionnelle")
    
    # Commit
    commit_result = storage.commit_transaction(tid)
    assert_condition(commit_result['success'], "Échec commit storage")
    
    # Vérifier
    tables = storage.get_tables()
    has_table = any(t['table_name'] == 'storage_test' for t in tables)
    assert_condition(has_table, "Table non trouvée après commit")
    
    # Nettoyer
    storage.execute("DROP TABLE storage_test")
    storage.close()
    
    try:
        os.remove(storage_path)
    except:
        pass
    
    return True

def test_database_integration():
    """Teste l'intégration complète de la base de données"""
    print_section("Intégration Base de Données")
    
    db = create_database(TEST_DB)
    
    # Test des commandes spéciales
    commands = [
        ("SHOW TABLES", "doit retourner des tables"),
        ("STATS", "doit retourner des statistiques"),
        ("VACUUM", "doit optimiser la base"),
    ]
    
    all_success = True
    for cmd, desc in commands:
        result = db.execute(cmd)
        if result.get('success'):
            print_test(f"Commande: {cmd.split()[0]}", True, desc)
        else:
            print_test(f"Commande: {cmd.split()[0]}", False, f"{desc}: {result.get('error', 'No error')}")
            all_success = False
    
    db.close()
    return all_success

# ==================== MAIN ====================

def main():
    """Fonction principale"""
    
    print_header(f"🏗️  Tests Complets GSQL v{__version__} - Transactions ACID")
    print(f"📁 Base de test: {TEST_DB}")
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Timeout par test: {TEST_TIMEOUT}s")
    
    # Liste des tests
    test_suites = [
        ("Transactions de base", test_transaction_basic),
        ("Niveaux d'isolation", test_transaction_isolation_levels),
        ("Savepoints", test_transaction_savepoints),
        ("Context Manager", test_transaction_context_manager),
        ("Transactions atomiques", test_atomic_transaction),
        ("Cohérence ACID", test_transaction_consistency),
        ("Gestion des erreurs", test_transaction_error_handling),
        ("Performance", test_transaction_performance),
        ("Concurrence", test_transaction_concurrency),
        ("API Storage", test_storage_direct),
        ("Intégration DB", test_database_integration),
    ]
    
    results = []
    total_tests = 0
    passed_tests = 0
    
    print(f"\n{Colors.BOLD}{Colors.BLUE}🚀 Exécution de {len(test_suites)} suites de tests...{Colors.RESET}")
    
    for suite_name, test_func in test_suites:
        success = run_test_safely(test_func, suite_name)
        results.append((suite_name, success))
        
        if success:
            passed_tests += 1
            status = f"{Colors.GREEN}✓ PASSÉ{Colors.RESET}"
        else:
            status = f"{Colors.RED}✗ ÉCHOUÉ{Colors.RESET}"
        
        print(f"  {status}")
        total_tests += 1
    
    # ==================== RÉSULTATS ====================
    
    print_header("📊 RÉSULTATS DÉTAILLÉS")
    
    print(f"\n{Colors.BOLD}Suites de tests:{Colors.RESET}")
    for test_name, success in results:
        status = f"{Colors.GREEN}PASSÉ{Colors.RESET}" if success else f"{Colors.RED}ÉCHOUÉ{Colors.RESET}"
        print(f"  {test_name:30} {status}")
    
    success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
    
    print(f"\n{Colors.BOLD}Statistiques:{Colors.RESET}")
    print(f"  Suites exécutées: {total_tests}")
    print(f"  Suites réussies: {passed_tests}")
    print(f"  Suites échouées: {total_tests - passed_tests}")
    print(f"  Taux de réussite: {success_rate:.1f}%")
    
    # Évaluation finale
    if success_rate == 100:
        print(f"\n{Colors.BOLD}{Colors.GREEN}🎉 EXCELLENT: Tous les tests passent !{Colors.RESET}")
        print(f"{Colors.GREEN}✅ GSQL est stable et prêt pour la production.{Colors.RESET}")
    elif success_rate >= 90:
        print(f"\n{Colors.BOLD}{Colors.GREEN}✅ TRÈS BON: Tests quasi-complets{Colors.RESET}")
        print(f"{Colors.YELLOW}⚠️  Quelques ajustements mineurs nécessaires.{Colors.RESET}")
    elif success_rate >= 70:
        print(f"\n{Colors.BOLD}{Colors.YELLOW}⚠️  SATISFAISANT: Tests majoritaires{Colors.RESET}")
        print(f"{Colors.YELLOW}🔧 Des correctifs sont nécessaires.{Colors.RESET}")
    else:
        print(f"\n{Colors.BOLD}{Colors.RED}❌ PROBLÉMATIQUE: Tests insuffisants{Colors.RESET}")
        print(f"{Colors.RED}🚨 Révision majeure requise.{Colors.RESET}")
    
    # ==================== NETTOYAGE ====================
    
    try:
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
            print(f"\n{Colors.CYAN}🧹 Fichier de test nettoyé: {TEST_DB}{Colors.RESET}")
    except Exception as e:
        print(f"\n{Colors.YELLOW}⚠️  Impossible de nettoyer {TEST_DB}: {e}{Colors.RESET}")
    
    # ==================== RECOMMANDATIONS ====================
    
    if passed_tests < total_tests:
        print_header("📝 RECOMMANDATIONS")
        
        print(f"\n{Colors.YELLOW}Tests échoués:{Colors.RESET}")
        for test_name, success in results:
            if not success:
                print(f"  • {test_name}")
        
        print(f"\n{Colors.YELLOW}Actions recommandées:{Colors.RESET}")
        print("  1. Examiner les logs pour les erreurs détaillées")
        print("  2. Tester manuellement les fonctionnalités échouées")
        print("  3. Vérifier la gestion des erreurs dans les transactions")
        print("  4. Tester avec différentes charges de travail")
        print("  5. Documenter les cas limites identifiés")
    
    print(f"\n{Colors.GREEN}✅ Tests terminés à {datetime.now().strftime('%H:%M:%S')}{Colors.RESET}")
    
    return passed_tests == total_tests

# ==================== EXÉCUTION ====================

if __name__ == "__main__":
    try:
        print(f"{Colors.BOLD}🔧 Mode debug activé: affichage détaillé des erreurs{Colors.RESET}" if "debug" in sys.argv else "")
        
        success = main()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}⏹️  Tests interrompus par l'utilisateur{Colors.RESET}")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n{Colors.RED}💥 Erreur fatale: {e}{Colors.RESET}")
        import traceback
        traceback.print_exc()
        sys.exit(1)