import unittest
import tempfile
import json
from pathlib import Path
from gsql.nlp.translator import NLToSQLTranslator, nl_to_sql
from gsql.exceptions import NLError

class TestNLToSQLTranslator(unittest.TestCase):
    
    def setUp(self):
        self.translator = NLToSQLTranslator()
        
        # Créer un fichier de patterns temporaire
        self.temp_dir = tempfile.mkdtemp()
        self.patterns_file = Path(self.temp_dir) / 'test_patterns.json'
        
        patterns = {
            "select_patterns": [
                {
                    "pattern": r"affiche (?:les|tous les) (\w+)",
                    "template": "SELECT * FROM {table}"
                }
            ],
            "column_mapping": {
                "clients": ["nom", "email"],
                "produits": ["nom", "prix"]
            }
        }
        
        with open(self.patterns_file, 'w', encoding='utf-8') as f:
            json.dump(patterns, f)
        
        self.translator_with_file = NLToSQLTranslator(str(self.patterns_file))
    
    def test_basic_translation(self):
        """Test de traduction basique"""
        tests = [
            ("montre les clients", "SELECT * FROM {table}"),
            ("combien de produits", "SELECT COUNT(*) FROM {table}"),
        ]
        
        for nl, expected_pattern in tests:
            with self.subTest(nl=nl):
                result = self.translator.translate(nl)
                self.assertIsInstance(result, str)
                self.assertTrue(len(result) > 0)
    
    def test_with_patterns_file(self):
        """Test avec fichier de patterns personnalisé"""
        result = self.translator_with_file.translate("affiche les clients")
        self.assertIn("SELECT", result)
    
    def test_nl_to_sql_helper(self):
        """Test de la fonction helper"""
        result = nl_to_sql("montre les produits", self.translator)
        self.assertIsInstance(result, str)
        self.assertIn("SELECT", result)
    
    def test_invalid_input(self):
        """Test avec entrée invalide"""
        with self.assertRaises(NLError):
            self.translator.translate("")
        
        with self.assertRaises(NLError):
            self.translator.translate(None)
    
    def test_learning(self):
        """Test de l'apprentissage"""
        examples = ["liste les utilisateurs actifs"]
        sqls = ["SELECT * FROM users WHERE status = 'active'"]
        
        self.translator.learn_from_examples(examples, sqls)
        
        # Vérifier que le pattern a été ajouté
        self.assertGreater(len(self.translator.patterns["select_patterns"]), 0)
    
    def tearDown(self):
        """Nettoyage après les tests"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

if __name__ == '__main__':
    unittest.main()
