import json
import re
import sqlite3
from pathlib import Path
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

class NLToSQLTranslator:
    """Traducteur de langage naturel vers SQL avec NLTK"""
    
    def __init__(self, patterns_file=None):
        self.lemmatizer = WordNetLemmatizer()
        self.stop_words = set(stopwords.words('french') + stopwords.words('english'))
        
        # Charger les patterns de traduction
        if patterns_file and Path(patterns_file).exists():
            with open(patterns_file, 'r', encoding='utf-8') as f:
                self.patterns = json.load(f)
        else:
            self.patterns = self._get_default_patterns()
        
        # Étiqueteur grammatical
        try:
            nltk.data.find('taggers/averaged_perceptron_tagger')
        except LookupError:
            nltk.download('averaged_perceptron_tagger', quiet=True)
    
    def _get_default_patterns(self):
        """Patterns par défaut pour la traduction"""
        return {
            "select_patterns": [
                {
                    "pattern": r"(montre|affiche|donne|sélectionne)\s+(.+?)\s+(de|depuis|from)\s+(\w+)",
                    "template": "SELECT {columns} FROM {table}"
                },
                {
                    "pattern": r"(combien|nombre|count)\s+(de|d')\s+(\w+)\s+(dans|in|from)\s+(\w+)",
                    "template": "SELECT COUNT(*) FROM {table}"
                }
            ],
            "where_patterns": [
                {
                    "pattern": r"(où|where)\s+(.+?)\s+(est|égale|=\s*|>\s*|<\s*)(.+?)$",
                    "template": "WHERE {column} {operator} {value}"
                }
            ],
            "column_mapping": {
                "clients": ["nom", "email", "ville", "âge", "salaire"],
                "produits": ["nom", "prix", "catégorie", "stock"],
                "commandes": ["id", "date", "montant", "statut"]
            }
        }
    
    def translate(self, nl_query):
        """
        Traduit une requête en langage naturel en SQL
        
        Args:
            nl_query (str): Requête en langage naturel
            
        Returns:
            str: Requête SQL
            
        Raises:
            NLError: Si la traduction échoue
        """
        nl_query = nl_query.lower().strip()
        
        # 1. Tokenisation et nettoyage
        tokens = word_tokenize(nl_query, language='french')
        tokens = [t for t in tokens if t not in self.stop_words and t.isalnum()]
        
        # 2. Lemmatisation
        lemmas = [self.lemmatizer.lemmatize(t) for t in tokens]
        
        # 3. Détection du type de requête
        sql_query = self._detect_query_type(nl_query, lemmas)
        
        # 4. Extraction des tables et colonnes
        sql_query = self._extract_entities(sql_query, lemmas)
        
        return sql_query
    
    def _detect_query_type(self, query, lemmas):
        """Détecte le type de requête SQL"""
        first_words = ' '.join(lemmas[:3])
        
        # SELECT
        for pattern in self.patterns["select_patterns"]:
            match = re.search(pattern["pattern"], query, re.IGNORECASE)
            if match:
                return pattern["template"]
        
        # INSERT (simplifié)
        if any(word in lemmas for word in ['ajoute', 'insère', 'nouveau']):
            return "INSERT INTO {table} VALUES ({values})"
        
        # DELETE
        if any(word in lemmas for word in ['supprime', 'efface', 'enlève']):
            return "DELETE FROM {table} WHERE {condition}"
        
        # UPDATE
        if any(word in lemmas for word in ['modifie', 'mets à jour', 'change']):
            return "UPDATE {table} SET {set_clause} WHERE {condition}"
        
        # Par défaut, c'est un SELECT
        return "SELECT * FROM {table}"
    
    def _extract_entities(self, sql_template, lemmas):
        """Extrait les tables, colonnes et valeurs"""
        # Cherche des noms de table dans les patterns
        for table, columns in self.patterns["column_mapping"].items():
            if table in lemmas or table[:-1] in lemmas:  # Gère le pluriel
                sql_template = sql_template.replace("{table}", table)
                
                # Cherche des colonnes spécifiques
                for col in columns:
                    if col in lemmas:
                        sql_template = sql_template.replace("{columns}", col)
                        break
                else:
                    sql_template = sql_template.replace("{columns}", "*")
                
                break
        
        return sql_template
    
    def learn_from_examples(self, nl_examples, sql_examples):
        """Apprend de nouveaux patterns à partir d'exemples"""
        for nl, sql in zip(nl_examples, sql_examples):
            # Extraction simple de patterns (version basique)
            if "SELECT" in sql:
                self.patterns["select_patterns"].append({
                    "pattern": self._create_pattern_from_example(nl),
                    "template": sql
                })
        
        # Sauvegarde des patterns
        self._save_patterns()
    
    def _create_pattern_from_example(self, nl_example):
        """Crée un pattern regex à partir d'un exemple"""
        # Conversion basique
        pattern = nl_example.lower()
        pattern = re.sub(r'\b(le|la|les|un|une|des)\b', '', pattern)
        pattern = re.sub(r'\b(\w+)\b', r'(\w+)', pattern)  # Capture les mots
        return f"^{pattern}$"
    
    def _save_patterns(self, filepath="patterns.json"):
        """Sauvegarde les patterns appris"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.patterns, f, ensure_ascii=False, indent=2)

# Interface simple pour le CLI
def nl_to_sql(nl_query, translator=None):
    """Fonction utilitaire pour convertir NL en SQL"""
    if translator is None:
        translator = NLToSQLTranslator()
    
    try:
        return translator.translate(nl_query)
    except Exception as e:
        raise NLError(f"Échec de la traduction: {str(e)}")
