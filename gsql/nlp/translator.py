#!/usr/bin/env python3
"""
Traducteur de langage naturel vers SQL pour GSQL
"""

import json
import re
from pathlib import Path
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

class NLToSQLTranslator:
    """Traducteur de langage naturel vers SQL avec NLTK"""
    
    def __init__(self, patterns_file=None):
        self.lemmatizer = WordNetLemmatizer()
        
        # Initialiser les stopwords
        self.stop_words = self._load_stopwords()
        
        # Charger les patterns de traduction
        if patterns_file and Path(patterns_file).exists():
            with open(patterns_file, 'r', encoding='utf-8') as f:
                self.patterns = json.load(f)
        else:
            self.patterns = self._get_default_patterns()
        
        # Vérifier les données NLTK
        self._ensure_nltk_data()
    
    def _load_stopwords(self):
        """Charger les stopwords avec fallback"""
        try:
            nltk.data.find('corpora/stopwords')
            french_stopwords = set(stopwords.words('french'))
            english_stopwords = set(stopwords.words('english'))
            return french_stopwords.union(english_stopwords)
        except LookupError:
            # Liste de fallback
            return {
                'le', 'la', 'les', 'un', 'une', 'des', 'de', 'du', 'au', 'aux',
                'à', 'avec', 'et', 'ou', 'où', 'qui', 'que', 'quoi', 'dont',
                'dans', 'en', 'sur', 'sous', 'pour', 'par', 'chez', 'entre',
                'mais', 'ou', 'donc', 'car', 'ni', 'or', 'ne', 'pas', 'plus',
                'moins', 'très', 'trop', 'peu', 'beaucoup', 'tout', 'tous',
                'cette', 'ce', 'cet', 'ces', 'mon', 'ton', 'son', 'notre',
                'votre', 'leur', 'même', 'autre', 'quel', 'quelle', 'quels',
                'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
                'for', 'of', 'with', 'by', 'as', 'is', 'are', 'was', 'were',
                'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did'
            }
    
    def _ensure_nltk_data(self):
        """S'assurer que les données NLTK nécessaires sont disponibles"""
        required = [
            ('punkt', 'tokenizers/punkt'),
            ('stopwords', 'corpora/stopwords'),
            ('wordnet', 'corpora/wordnet')
        ]
        
        for package, path in required:
            try:
                nltk.data.find(path)
            except LookupError:
                try:
                    nltk.download(package, quiet=True)
                except:
                    pass
    
    def _get_default_patterns(self):
        """Patterns par défaut pour la traduction"""
        return {
            "patterns": [
                {
                    "keywords": ["table", "tables", "tableau", "tableaux"],
                    "action": "SHOW_TABLES",
                    "sql": "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                },
                {
                    "keywords": ["fonction", "fonctions", "function", "functions"],
                    "action": "SHOW_FUNCTIONS",
                    "sql": "SELECT 'UPPER(text)', 'LOWER(text)', 'LENGTH(text)' as functions"
                },
                {
                    "keywords": ["aide", "help", "commande", "commandes"],
                    "action": "HELP",
                    "sql": "SELECT 'SHOW TABLES - List tables' as help UNION SELECT 'SHOW FUNCTIONS - List functions' UNION SELECT 'CREATE FUNCTION - Create custom function'"
                },
                {
                    "keywords": ["crée", "créer", "create", "nouvelle", "nouveau"],
                    "patterns": [
                        (r"crée(?:r)?\s+(?:une\s+)?(?:table|tableau)\s+(\w+)", "CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
                        (r"crée(?:r)?\s+(?:une\s+)?fonction\s+(\w+)", "CREATE FUNCTION {name}(text) RETURNS text AS $$ RETURN $1 $$ LANGUAGE plpython")
                    ]
                },
                {
                    "keywords": ["affiche", "montre", "donne", "liste", "show", "select"],
                    "patterns": [
                        (r"(affiche|montre|donne|liste)\s+(?:les\s+)?(\w+)", "SELECT * FROM {table}"),
                        (r"(affiche|montre|donne|liste)\s+(?:tous\s+les\s+)?(\w+)", "SELECT * FROM {table}"),
                        (r"(\w+)\s+(?:de|dans)\s+(\w+)", "SELECT {column} FROM {table}"),
                        (r"combien\s+de\s+(\w+)", "SELECT COUNT(*) FROM {table}"),
                        (r"(\w+)\s+avec\s+(\w+)\s*=\s*['\"]?(.+?)['\"]?$", "SELECT * FROM {table} WHERE {column} = '{value}'")
                    ]
                },
                {
                    "keywords": ["ajoute", "insère", "insert", "add"],
                    "patterns": [
                        (r"ajoute(?:r)?\s+(?:un|une)?\s*(\w+)\s+(.+)", "INSERT INTO {table} VALUES ({values})"),
                        (r"insère(?:r)?\s+(?:dans\s+)?(\w+)\s+(.+)", "INSERT INTO {table} VALUES ({values})")
                    ]
                },
                {
                    "keywords": ["supprime", "efface", "delete", "remove"],
                    "patterns": [
                        (r"supprime(?:r)?\s+(?:les\s+)?(\w+)", "DELETE FROM {table}"),
                        (r"efface(?:r)?\s+(?:la\s+)?table\s+(\w+)", "DROP TABLE {table}")
                    ]
                }
            ],
            "table_mapping": {
                "utilisateurs": "users",
                "usagers": "users",
                "clients": "clients",
                "client": "clients",
                "produits": "products",
                "produit": "products",
                "commandes": "orders",
                "commande": "orders",
                "orders": "orders",
                "employés": "employees",
                "employes": "employees",
                "employé": "employees",
                "user": "users",
                "users": "users",
                "client": "clients",
                "product": "products",
                "products": "products"
            },
            "column_mapping": {
                "users": ["id", "name", "email", "age", "created_at"],
                "clients": ["id", "nom", "email", "ville", "telephone"],
                "products": ["id", "nom", "prix", "quantite", "categorie"],
                "orders": ["id", "client_id", "date", "montant", "statut"]
            }
        }
    
    def translate(self, nl_query):
        """
        Traduit une requête en langage naturel en SQL
        
        Args:
            nl_query (str): Requête en langage naturel
            
        Returns:
            str: Requête SQL
        """
        nl_query = nl_query.lower().strip()
        original_query = nl_query
        
        # 1. Nettoyer la requête
        nl_query = self._clean_query(nl_query)
        
        # 2. Vérifier les patterns par mots-clés d'abord
        for pattern_group in self.patterns.get("patterns", []):
            # Vérifier si un mot-clé de ce groupe est dans la requête
            keywords = pattern_group.get("keywords", [])
            if any(keyword in original_query for keyword in keywords):
                
                # Vérifier l'action directe
                if "action" in pattern_group:
                    if pattern_group["action"] == "SHOW_TABLES":
                        return "SHOW TABLES"
                    elif pattern_group["action"] == "SHOW_FUNCTIONS":
                        return "SHOW FUNCTIONS"
                    elif pattern_group["action"] == "HELP":
                        return "HELP"
                    elif "sql" in pattern_group:
                        return pattern_group["sql"]
                
                # Vérifier les patterns spécifiques
                if "patterns" in pattern_group:
                    for pattern, template in pattern_group["patterns"]:
                        match = re.search(pattern, original_query, re.IGNORECASE)
                        if match:
                            sql = self._apply_template(template, match, original_query)
                            if sql:
                                return sql
        
        # 3. Chercher des noms de table directement
        table_mapping = self.patterns.get("table_mapping", {})
        for fr_word, en_table in table_mapping.items():
            if fr_word in original_query:
                return f"SELECT * FROM {en_table}"
        
        # 4. Tokenisation avancée si disponible
        try:
            tokens = word_tokenize(original_query, language='french')
            tokens = [t for t in tokens if t.lower() not in self.stop_words and t.isalnum()]
            
            # Chercher des mots qui pourraient être des tables
            for token in tokens:
                if token in table_mapping:
                    return f"SELECT * FROM {table_mapping[token]}"
                
                # Vérifier aussi en anglais
                if token in table_mapping.values():
                    return f"SELECT * FROM {token}"
        except:
            # Fallback à la recherche simple
            pass
        
        # 5. Pattern par défaut - table + mot
        words = original_query.split()
        if len(words) >= 2:
            # Essayer différentes combinaisons
            for i in range(len(words)):
                potential_table = words[i]
                if potential_table in table_mapping:
                    return f"SELECT * FROM {table_mapping[potential_table]}"
                elif i > 0:
                    # Combinaison de mots
                    combined = words[i-1] + words[i]
                    if combined in table_mapping:
                        return f"SELECT * FROM {table_mapping[combined]}"
        
        # 6. Si "table" est mentionné
        if "table" in original_query:
            return "SHOW TABLES"
        
        # 7. Par défaut, retourner de l'aide
        return "HELP"
    
    def _clean_query(self, query):
        """Nettoyer la requête NL"""
        # Supprimer la ponctuation excessive
        query = re.sub(r'[^\w\s\']', ' ', query)
        # Remplacer multiples espaces
        query = re.sub(r'\s+', ' ', query)
        return query.strip()
    
    def _apply_template(self, template, match, original_query):
        """Appliquer un template avec les groupes de capture"""
        try:
            sql = template
            
            # Remplacer {table}
            if '{table}' in template and match.lastindex >= 1:
                table_name = match.group(1).lower()
                table_mapping = self.patterns.get("table_mapping", {})
                actual_table = table_mapping.get(table_name, table_name)
                sql = sql.replace('{table}', actual_table)
            
            # Remplacer {column}
            if '{column}' in template and match.lastindex >= 2:
                column = match.group(2)
                sql = sql.replace('{column}', column)
            
            # Remplacer {value}
            if '{value}' in template and match.lastindex >= 3:
                value = match.group(3).strip("'\"")
                sql = sql.replace('{value}', value)
            
            # Remplacer {name}
            if '{name}' in template and match.lastindex >= 1:
                name = match.group(1)
                sql = sql.replace('{name}', name)
            
            # Remplacer {values}
            if '{values}' in template and match.lastindex >= 2:
                values_text = match.group(2)
                # Séparer les valeurs par espaces/virgules
                values = re.split(r'[\s,]+', values_text)
                quoted_values = [f"'{v}'" for v in values if v]
                sql = sql.replace('{values}', ', '.join(quoted_values))
            
            return sql
        except Exception as e:
            return None
    
    def learn(self, nl_example, sql_example):
        """Apprend un nouvel exemple"""
        # Pour l'instant, simple log
        print(f"Learned: '{nl_example}' -> '{sql_example}'")
        return "Pattern learned (in memory only)"
    
    def save_patterns(self, filepath):
        """Sauvegarder les patterns"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.patterns, f, ensure_ascii=False, indent=2)
            return True
        except:
            return False

# Fonction helper
def nl_to_sql(nl_query, translator=None):
    """Convertir NL en SQL"""
    if translator is None:
        translator = NLToSQLTranslator()
    
    try:
        return translator.translate(nl_query)
    except Exception as e:
        # Fallback simple
        nl_lower = nl_query.lower()
        
        if "table" in nl_lower and "show" not in nl_lower:
            # Extraire le nom de table
            words = nl_lower.split()
            for word in words:
                if word != "table" and len(word) > 2:
                    return f"SELECT * FROM {word}"
        
        if "table" in nl_lower:
            return "SHOW TABLES"
        elif "fonction" in nl_lower:
            return "SHOW FUNCTIONS"
        elif "aide" in nl_lower or "help" in nl_lower:
            return "HELP"
        else:
            return "SELECT 'Try: show tables, show functions, table [name]' as suggestion"
