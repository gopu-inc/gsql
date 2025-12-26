#!/usr/bin/env python3
"""
Traducteur de langage naturel vers SQL pour GSQL
Version avancée avec apprentissage et compréhension contextuelle
"""

import json
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import hashlib
import pickle

# Import NLTK avec fallback
try:
    import nltk
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer
    from nltk import pos_tag
    from nltk.chunk import ne_chunk
    from nltk.tree import Tree
    
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False
    print("NLTK non disponible - certaines fonctionnalités NLP seront limitées")


class NLPattern:
    """Représente un pattern de traduction NL->SQL"""
    
    def __init__(self, pattern_type: str, nl_pattern: str, sql_template: str, 
                 examples: List[str] = None, confidence: float = 1.0):
        self.pattern_type = pattern_type  # 'select', 'insert', 'update', etc.
        self.nl_pattern = nl_pattern
        self.sql_template = sql_template
        self.examples = examples or []
        self.confidence = confidence
        self.usage_count = 0
        self.last_used = None
        
    def match(self, query: str) -> Optional[Dict]:
        """Vérifie si le pattern correspond à la requête"""
        # Convertir en regex
        pattern_regex = self.nl_pattern
        pattern_regex = re.sub(r'\{(\w+)\}', r'(?P<\1>[\\w\\s]+)', pattern_regex)
        pattern_regex = f"^{pattern_regex}$"
        
        match = re.match(pattern_regex, query, re.IGNORECASE)
        if match:
            self.usage_count += 1
            self.last_used = datetime.now()
            return match.groupdict()
        return None
    
    def to_dict(self) -> Dict:
        """Convertir en dictionnaire"""
        return {
            'pattern_type': self.pattern_type,
            'nl_pattern': self.nl_pattern,
            'sql_template': self.sql_template,
            'examples': self.examples,
            'confidence': self.confidence,
            'usage_count': self.usage_count,
            'last_used': self.last_used.isoformat() if self.last_used else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'NLPattern':
        """Créer depuis un dictionnaire"""
        pattern = cls(
            data['pattern_type'],
            data['nl_pattern'],
            data['sql_template'],
            data.get('examples', [])
        )
        pattern.confidence = data.get('confidence', 1.0)
        pattern.usage_count = data.get('usage_count', 0)
        if data.get('last_used'):
            pattern.last_used = datetime.fromisoformat(data['last_used'])
        return pattern


class DatabaseContext:
    """Gère le contexte de la base de données pour une meilleure compréhension"""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path
        self.schema = {}
        self.table_info = {}
        self.column_stats = {}
        self.common_queries = {}
        
        if db_path and Path(db_path).exists():
            self.load_schema()
    
    def load_schema(self) -> None:
        """Charge le schéma de la base de données"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Récupérer les tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                # Informations sur la table
                cursor.execute(f"PRAGMA table_info({table})")
                columns = []
                for col in cursor.fetchall():
                    columns.append({
                        'name': col[1],
                        'type': col[2],
                        'nullable': not col[3],
                        'default': col[4],
                        'pk': col[5]
                    })
                
                self.schema[table] = columns
                
                # Statistiques sur les colonnes (valeurs uniques)
                self.column_stats[table] = {}
                for col in columns:
                    try:
                        cursor.execute(f"SELECT COUNT(DISTINCT {col['name']}) FROM {table}")
                        unique_count = cursor.fetchone()[0]
                        
                        cursor.execute(f"SELECT {col['name']} FROM {table} LIMIT 10")
                        sample_values = [row[0] for row in cursor.fetchall() if row[0] is not None]
                        
                        self.column_stats[table][col['name']] = {
                            'unique_count': unique_count,
                            'sample_values': sample_values[:5]
                        }
                    except:
                        continue
            
            conn.close()
            
        except Exception as e:
            print(f"Erreur lors du chargement du schéma: {e}")
    def ensure_patterns_file(patterns_path: str = None) -> str:
    """
    Ensure patterns file exists, create default if not
    
    Returns:
        str: Path to patterns file
    """
    if patterns_path is None:
        # Default path
        default_dir = Path.home() / '.gsql' / 'nlp'
        default_dir.mkdir(exist_ok=True, parents=True)
        patterns_path = str(default_dir / 'patterns.json')
    
    path = Path(patterns_path)
    
    if not path.exists():
        # Create default patterns
        default_patterns = {
            "patterns": {
                # ... [INSÉRER ICI LES PATTERNS PAR DÉFAUT COMPLETS] ...
            },
            "synonyms": {
                # ... [INSÉRER ICI LES SYNONYMES] ...
            },
            "metadata": {
                "created": datetime.now().isoformat(),
                "version": "1.0",
                "description": "Auto-generated NLP patterns for GSQL"
            }
        }
        
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(default_patterns, f, ensure_ascii=False, indent=2)
            logger.info(f"Created default NLP patterns file: {path}")
        except Exception as e:
            logger.error(f"Failed to create patterns file: {e}")
    
    return patterns_path
    def suggest_columns(self, table: str, context: str = "") -> List[str]:
        """Suggère des colonnes basées sur le contexte"""
        if table not in self.schema:
            return []
        
        columns = [col['name'] for col in self.schema[table]]
        
        # Filtrer basé sur le contexte si disponible
        if context:
            context_lower = context.lower()
            relevant_cols = []
            for col in columns:
                col_lower = col.lower()
                # Vérifier si le nom de colonne ou son type correspond au contexte
                if (context_lower in col_lower or 
                    col_lower in context_lower):
                    relevant_cols.append(col)
            
            if relevant_cols:
                return relevant_cols
        
        return columns
    
    def get_table_for_context(self, context_words: List[str]) -> Optional[str]:
        """Trouve la table la plus pertinente basée sur des mots de contexte"""
        best_match = None
        best_score = 0
        
        for table in self.schema.keys():
            table_lower = table.lower()
            score = 0
            
            for word in context_words:
                word_lower = word.lower()
                if word_lower in table_lower:
                    score += 2
                # Vérifier la similarité
                if any(table_word.startswith(word_lower[:3]) for table_word in table_lower.split('_')):
                    score += 1
            
            if score > best_score:
                best_score = score
                best_match = table
        
        return best_match


class NLToSQLTranslator:
    """Traducteur avancé de langage naturel vers SQL"""
    
    def __init__(self, patterns_file: Optional[str] = None, db_path: Optional[str] = None):
        # Initialisation NLTK
        self.nltk_available = NLTK_AVAILABLE
        if self.nltk_available:
            self._init_nltk()
        
        # Contexte de base de données
        self.db_context = DatabaseContext(db_path)
        
        # Patterns de traduction
        self.patterns: Dict[str, List[NLPattern]] = {}
        self._load_default_patterns()
        
        # Charger les patterns personnalisés
        self.patterns_file = patterns_file
        if patterns_file and Path(patterns_file).exists():
            self.load_patterns(patterns_file)
        
        # Cache pour les traductions fréquentes
        self.translation_cache: Dict[str, Tuple[str, float]] = {}
        self.cache_size = 100
        
        # Historique d'apprentissage
        self.learning_history = []
        
        # Dictionnaires de synonymes
        self.synonyms = self._load_synonyms()
    
    def _init_nltk(self) -> None:
        """Initialiser NLTK avec les données nécessaires"""
        try:
            # Télécharger les données NLTK si nécessaire
            nltk_data = [
                ('punkt', 'tokenizers/punkt'),
                ('stopwords', 'corpora/stopwords'),
                ('wordnet', 'corpora/wordnet'),
                ('averaged_perceptron_tagger', 'taggers/averaged_perceptron_tagger'),
                ('maxent_ne_chunker', 'chunkers/maxent_ne_chunker_tab'),
                ('words', 'corpora/words')
            ]
            
            for package, path in nltk_data:
                try:
                    nltk.data.find(path)
                except LookupError:
                    nltk.download(package, quiet=True)
            
            self.lemmatizer = WordNetLemmatizer()
            self.stop_words = set(stopwords.words('english')).union(set(stopwords.words('french')))
            
        except Exception as e:
            print(f"NLTK initialization warning: {e}")
            self.nltk_available = False
    
    def _load_synonyms(self) -> Dict[str, List[str]]:
        """Charger les synonymes"""
        return {
            'show': ['afficher', 'montrer', 'voir', 'lister', 'donner'],
            'select': ['choisir', 'sélectionner', 'extraire', 'obtenir'],
            'insert': ['ajouter', 'insérer', 'créer', 'nouveau'],
            'update': ['modifier', 'changer', 'mettre à jour', 'éditer'],
            'delete': ['supprimer', 'effacer', 'enlever', 'retirer'],
            'count': ['compter', 'dénombrer', 'total'],
            'average': ['moyenne', 'moyen'],
            'sum': ['somme', 'total'],
            'max': ['maximum', 'plus grand', 'plus haut'],
            'min': ['minimum', 'plus petit', 'plus bas'],
            'where': ['où', 'dans lequel', 'pour lequel'],
            'order_by': ['trier par', 'ordonner par', 'classer par'],
            'group_by': ['grouper par', 'regrouper par'],
            'limit': ['limiter à', 'seulement', 'premier'],
            'join': ['joindre', 'combiner', 'relier']
        }
    
    def _load_default_patterns(self) -> None:
        """Charger les patterns par défaut"""
        default_patterns = [
            # Patterns SELECT
            NLPattern('select', 'show tables', 'SHOW TABLES', 
                     ['montre les tables', 'liste les tables', 'affiche les tables']),
            
            NLPattern('select', 'show columns from {table}', 
                     'DESCRIBE {table}', 
                     ['montre les colonnes de {table}', 'décris {table}']),
            
            NLPattern('select', 'select all from {table}', 
                     'SELECT * FROM {table}', 
                     ['tous les {table}', 'tout de {table}', 'affiche {table}']),
            
            NLPattern('select', 'select {columns} from {table}', 
                     'SELECT {columns} FROM {table}', 
                     ['montre {columns} de {table}', 'donne {columns} depuis {table}']),
            
            NLPattern('select', 'count {table}', 
                     'SELECT COUNT(*) FROM {table}', 
                     ['combien de {table}', 'nombre de {table}', 'total {table}']),
            
            NLPattern('select', 'select from {table} where {condition}', 
                     'SELECT * FROM {table} WHERE {condition}', 
                     ['{table} où {condition}', 'trouve {table} avec {condition}']),
            
            # Patterns INSERT
            NLPattern('insert', 'insert into {table} values {values}', 
                     'INSERT INTO {table} VALUES ({values})', 
                     ['ajoute à {table} valeurs {values}', 'nouveau dans {table} {values}']),
            
            # Patterns DELETE
            NLPattern('delete', 'delete from {table} where {condition}', 
                     'DELETE FROM {table} WHERE {condition}', 
                     ['supprime de {table} où {condition}', 'enlève {table} avec {condition}']),
            
            # Patterns UPDATE
            NLPattern('update', 'update {table} set {set_clause} where {condition}', 
                     'UPDATE {table} SET {set_clause} WHERE {condition}', 
                     ['modifie {table} mettre {set_clause} où {condition}']),
            
            # Patterns CREATE
            NLPattern('create', 'create table {table} with {columns}', 
                     'CREATE TABLE {table} ({columns})', 
                     ['crée table {table} avec {columns}', 'nouvelle table {table} colonnes {columns}']),
            
            # Patterns DROP
            NLPattern('drop', 'drop table {table}', 
                     'DROP TABLE {table}', 
                     ['supprime table {table}', 'efface table {table}']),
            
            # Patterns avec agrégation
            NLPattern('select', 'average {column} from {table}', 
                     'SELECT AVG({column}) FROM {table}', 
                     ['moyenne de {column} dans {table}', 'moyenne {column} {table}']),
            
            NLPattern('select', 'sum {column} from {table}', 
                     'SELECT SUM({column}) FROM {table}', 
                     ['total de {column} dans {table}', 'somme {column} {table}']),
            
            NLPattern('select', 'max {column} from {table}', 
                     'SELECT MAX({column}) FROM {table}', 
                     ['maximum de {column} dans {table}', 'plus grand {column} {table}']),
            
            NLPattern('select', 'min {column} from {table}', 
                     'SELECT MIN({column}) FROM {table}', 
                     ['minimum de {column} dans {table}', 'plus petit {column} {table}']),
        ]
        
        # Organiser par type
        for pattern in default_patterns:
            if pattern.pattern_type not in self.patterns:
                self.patterns[pattern.pattern_type] = []
            self.patterns[pattern.pattern_type].append(pattern)
    
    def preprocess_query(self, query: str) -> str:
        """Prétraiter la requête NL"""
        # Nettoyage de base
        query = query.lower().strip()
        
        # Remplacer les synonymes
        for standard, syn_list in self.synonyms.items():
            for syn in syn_list:
                if f" {syn} " in f" {query} ":
                    query = query.replace(syn, standard)
        
        # Standardiser la ponctuation
        query = re.sub(r'[^\w\s\?]', ' ', query)
        query = re.sub(r'\s+', ' ', query)
        
        # Lemmatisation si NLTK disponible
        if self.nltk_available:
            tokens = word_tokenize(query)
            tokens = [self.lemmatizer.lemmatize(token) for token in tokens 
                     if token not in self.stop_words]
            query = ' '.join(tokens)
        
        return query.strip()
    
    def extract_entities(self, query: str) -> Dict[str, Any]:
        """Extraire les entités de la requête"""
        entities = {
            'tables': [],
            'columns': [],
            'values': [],
            'conditions': [],
            'operators': [],
            'aggregations': []
        }
        
        # Détecter les agrégations
        agg_keywords = ['count', 'sum', 'avg', 'average', 'max', 'min', 'total']
        for agg in agg_keywords:
            if agg in query:
                entities['aggregations'].append(agg)
        
        # Détecter les opérateurs
        operators = ['=', '>', '<', '>=', '<=', '!=', 'like', 'in', 'between']
        for op in operators:
            if f" {op} " in f" {query} ":
                entities['operators'].append(op)
        
        # Si on a un contexte de base de données, essayer d'identifier les tables et colonnes
        if self.db_context.schema:
            words = query.split()
            for word in words:
                # Vérifier si c'est une table
                if word in self.db_context.schema:
                    entities['tables'].append(word)
                # Vérifier si c'est une colonne
                for table in self.db_context.schema:
                    if word in [col['name'] for col in self.db_context.schema[table]]:
                        entities['columns'].append(word)
        
        return entities
    
    def translate(self, nl_query: str, confidence_threshold: float = 0.3) -> Dict[str, Any]:
        """
        Traduire une requête NL en SQL
        
        Returns:
            Dict avec 'sql', 'confidence', 'entities', 'explanation'
        """
        # Vérifier le cache
        query_hash = hashlib.md5(nl_query.encode()).hexdigest()
        if query_hash in self.translation_cache:
            sql, conf = self.translation_cache[query_hash]
            return {
                'sql': sql,
                'confidence': conf,
                'cached': True,
                'explanation': f"From cache (confidence: {conf:.2f})"
            }
        
        # Prétraitement
        processed_query = self.preprocess_query(nl_query)
        
        # Extraction d'entités
        entities = self.extract_entities(processed_query)
        
        # Essayer les patterns
        best_match = None
        best_confidence = 0
        best_sql = ""
        matched_pattern = None
        
        for pattern_type, patterns in self.patterns.items():
            for pattern in patterns:
                match = pattern.match(processed_query)
                if match:
                    confidence = pattern.confidence * (1 + pattern.usage_count * 0.01)
                    
                    if confidence > best_confidence:
                        best_confidence = confidence
                        best_match = match
                        matched_pattern = pattern
                        
                        # Construire la requête SQL
                        sql = pattern.sql_template
                        for key, value in match.items():
                            sql = sql.replace(f"{{{key}}}", self._format_sql_value(key, value, entities))
                        
                        best_sql = sql
        
        # Si pas de pattern exact, essayer une heuristique
        if not best_match and best_confidence < confidence_threshold:
            best_sql, best_confidence = self._heuristic_translation(processed_query, entities)
        
        # Post-traitement SQL
        if best_sql:
            best_sql = self._postprocess_sql(best_sql, entities)
        
        # Construire la réponse
        result = {
            'sql': best_sql or "HELP",
            'confidence': best_confidence,
            'entities': entities,
            'explanation': self._generate_explanation(best_sql, best_confidence, matched_pattern),
            'suggestions': self._generate_suggestions(nl_query, best_sql)
        }
        
        # Mettre en cache si confiance suffisante
        if best_confidence > 0.5:
            self.translation_cache[query_hash] = (best_sql, best_confidence)
            if len(self.translation_cache) > self.cache_size:
                # Retirer le plus ancien
                oldest_key = next(iter(self.translation_cache))
                del self.translation_cache[oldest_key]
        
        # Enregistrer dans l'historique
        self.learning_history.append({
            'timestamp': datetime.now(),
            'nl_query': nl_query,
            'sql_query': best_sql,
            'confidence': best_confidence,
            'entities': entities
        })
        
        return result
    
    def _heuristic_translation(self, query: str, entities: Dict) -> Tuple[str, float]:
        """Traduction heuristique quand aucun pattern ne correspond"""
        words = query.split()
        confidence = 0.3
        
        # Détecter l'intention
        if any(word in ['show', 'list', 'display'] for word in words):
            if 'tables' in words:
                return "SHOW TABLES", 0.8
            elif any(word in entities.get('tables', []) for word in words):
                table = next((w for w in words if w in entities.get('tables', [])), None)
                if table:
                    return f"SELECT * FROM {table}", 0.6
        
        # Chercher un nom de table
        if self.db_context.schema:
            for word in words:
                if word in self.db_context.schema:
                    return f"SELECT * FROM {word}", 0.5
        
        # Intention de comptage
        if any(word in ['count', 'how many', 'number'] for word in words):
            for word in words:
                if word in self.db_context.schema:
                    return f"SELECT COUNT(*) FROM {word}", 0.7
        
        # Par défaut, chercher une table basée sur le contexte
        suggested_table = self.db_context.get_table_for_context(words)
        if suggested_table:
            return f"SELECT * FROM {suggestied_table}", 0.4
        
        return "HELP", 0.1
    
    def _format_sql_value(self, key: str, value: str, entities: Dict) -> str:
        """Formater une valeur pour SQL"""
        if key in ['column', 'columns']:
            # Séparer les colonnes multiples
            if ',' in value:
                columns = [col.strip() for col in value.split(',')]
                return ', '.join(columns)
            return value
        
        elif key in ['value', 'values']:
            # Détecter si c'est numérique ou chaîne
            try:
                float(value)
                return value
            except ValueError:
                return f"'{value}'"
        
        elif key == 'condition':
            # Essayer de parser la condition
            if '=' in value:
                left, right = value.split('=', 1)
                right_formatted = self._format_sql_value('value', right.strip(), entities)
                return f"{left.strip()} = {right_formatted}"
            return value
        
        elif key == 'set_clause':
            # Formatage pour SET clause
            if '=' in value:
                parts = value.split('=', 1)
                right_formatted = self._format_sql_value('value', parts[1].strip(), entities)
                return f"{parts[0].strip()} = {right_formatted}"
            return value
        
        return value
    
    def _postprocess_sql(self, sql: str, entities: Dict) -> str:
        """Post-traiter la requête SQL"""
        # Ajouter LIMIT si SELECT sans limite et retourne beaucoup de lignes
        if sql.upper().startswith('SELECT') and 'LIMIT' not in sql.upper():
            if not any(op in sql.upper() for op in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']):
                sql += " LIMIT 100"
        
        # Standardiser les noms de table si nécessaire
        if self.db_context.schema:
            for table in self.db_context.schema:
                pattern = rf'\b{table}\b'
                sql = re.sub(pattern, table, sql, flags=re.IGNORECASE)
        
        return sql
    
    def _generate_explanation(self, sql: str, confidence: float, pattern: Optional[NLPattern]) -> str:
        """Générer une explication de la traduction"""
        if not sql or sql == "HELP":
            return "Je n'ai pas compris votre requête. Essayez de formuler autrement."
        
        explanations = {
            'SHOW TABLES': "Cette commande affiche la liste de toutes les tables disponibles dans la base de données.",
            'DESCRIBE': "Cette commande montre la structure d'une table, y compris toutes ses colonnes et leurs types.",
            'SELECT': "Cette requête récupère des données d'une ou plusieurs tables.",
            'INSERT': "Cette commande ajoute de nouvelles données dans une table.",
            'UPDATE': "Cette commande modifie des données existantes dans une table.",
            'DELETE': "Cette commande supprime des données d'une table.",
            'HELP': "Commande d'aide pour connaître les fonctionnalités disponibles."
        }
        
        command = sql.split()[0].upper()
        explanation = explanations.get(command, f"Commande {command} exécutée.")
        
        if pattern:
            explanation += f" (Pattern: {pattern.nl_pattern})"
        
        if confidence < 0.5:
            explanation += f" ⚠️ Faible confiance ({confidence:.2f}) - vérifiez le résultat."
        
        return explanation
    
    def _generate_suggestions(self, nl_query: str, sql: str) -> List[str]:
        """Générer des suggestions alternatives"""
        suggestions = []
        
        if sql == "HELP":
            suggestions = [
                "Essayez: 'montre les tables' pour voir les tables disponibles",
                "Essayez: 'sélectionne tout de [table]' pour voir les données",
                "Essayez: 'combien de [table]' pour compter les enregistrements"
            ]
        elif "SELECT * FROM" in sql:
            table = sql.replace("SELECT * FROM", "").strip().split()[0]
            suggestions = [
                f"Pour compter: 'combien de {table}'",
                f"Pour les colonnes: 'montre les colonnes de {table}'",
                f"Pour filtrer: '{table} où [condition]'"
            ]
        
        return suggestions
    
    def learn_from_example(self, nl_query: str, sql_query: str, feedback_score: float = 1.0) -> bool:
        """
        Apprendre d'un exemple fourni par l'utilisateur
        
        Args:
            nl_query: Requête en langage naturel
            sql_query: Requête SQL correspondante
            feedback_score: Score de feedback (0.0 à 1.0)
        
        Returns:
            bool: Succès de l'apprentissage
        """
        try:
            # Prétraiter la requête NL
            processed_nl = self.preprocess_query(nl_query)
            
            # Analyser la requête SQL pour en déduire un pattern
            sql_upper = sql_query.upper()
            
            if sql_upper.startswith('SELECT'):
                if 'COUNT(*)' in sql_upper:
                    # Pattern de comptage
                    table = sql_query.split('FROM')[1].split()[0].strip()
                    pattern_str = f"count {table}"
                elif '*' in sql_query:
                    # SELECT * FROM table
                    table = sql_query.split('FROM')[1].split()[0].strip()
                    pattern_str = f"select all from {table}"
                else:
                    # SELECT columns FROM table
                    columns_part = sql_query.split('SELECT')[1].split('FROM')[0].strip()
                    table = sql_query.split('FROM')[1].split()[0].strip()
                    pattern_str = f"select {columns_part} from {table}"
            
            elif sql_upper.startswith('INSERT'):
                table = sql_query.split('INTO')[1].split()[0].strip()
                values_match = re.search(r'VALUES\s*\((.*?)\)', sql_query, re.IGNORECASE)
                if values_match:
                    values = values_match.group(1)
                    pattern_str = f"insert into {table} values {values}"
            
            elif sql_upper.startswith('SHOW TABLES'):
                pattern_str = "show tables"
            
            else:
                pattern_str = processed_nl
            
            # Créer un nouveau pattern
            pattern_type = self._detect_query_type(sql_query)
            new_pattern = NLPattern(
                pattern_type=pattern_type,
                nl_pattern=pattern_str,
                sql_template=sql_query,
                examples=[nl_query],
                confidence=feedback_score
            )
            
            # Ajouter au dictionnaire de patterns
            if pattern_type not in self.patterns:
                self.patterns[pattern_type] = []
            
            self.patterns[pattern_type].append(new_pattern)
            
            # Enregistrer l'apprentissage
            self.learning_history.append({
                'timestamp': datetime.now(),
                'type': 'learned',
                'nl_query': nl_query,
                'sql_query': sql_query,
                'pattern': pattern_str,
                'feedback': feedback_score
            })
            
            # Sauvegarder si un fichier de patterns est configuré
            if self.patterns_file:
                self.save_patterns(self.patterns_file)
            
            return True
            
        except Exception as e:
            print(f"Erreur lors de l'apprentissage: {e}")
            return False
    
    def _detect_query_type(self, sql: str) -> str:
        """Détecter le type de requête SQL"""
        sql_upper = sql.upper()
        
        if sql_upper.startswith('SELECT'):
            return 'select'
        elif sql_upper.startswith('INSERT'):
            return 'insert'
        elif sql_upper.startswith('UPDATE'):
            return 'update'
        elif sql_upper.startswith('DELETE'):
            return 'delete'
        elif sql_upper.startswith('CREATE'):
            return 'create'
        elif sql_upper.startswith('DROP'):
            return 'drop'
        elif sql_upper.startswith('SHOW'):
            return 'show'
        elif sql_upper.startswith('DESCRIBE'):
            return 'describe'
        else:
            return 'unknown'
    
    def load_patterns(self, filepath: str) -> bool:
        """Charger les patterns depuis un fichier"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convertir les données JSON en objets NLPattern
            self.patterns.clear()
            for pattern_type, patterns_data in data.get('patterns', {}).items():
                self.patterns[pattern_type] = [
                    NLPattern.from_dict(pattern_data) 
                    for pattern_data in patterns_data
                ]
            
            # Charger les synonymes si présents
            if 'synonyms' in data:
                self.synonyms.update(data['synonyms'])
            
            return True
            
        except Exception as e:
            print(f"Erreur lors du chargement des patterns: {e}")
            return False
    
    def save_patterns(self, filepath: str) -> bool:
        """Sauvegarder les patterns dans un fichier"""
        try:
            # Convertir les patterns en dictionnaire
            patterns_dict = {}
            for pattern_type, patterns in self.patterns.items():
                patterns_dict[pattern_type] = [p.to_dict() for p in patterns]
            
            data = {
                'patterns': patterns_dict,
                'synonyms': self.synonyms,
                'metadata': {
                    'last_saved': datetime.now().isoformat(),
                    'total_patterns': sum(len(p) for p in self.patterns.values()),
                    'learning_history_count': len(self.learning_history)
                }
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception as e:
            print(f"Erreur lors de la sauvegarde des patterns: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Obtenir des statistiques sur le traducteur"""
        total_patterns = sum(len(patterns) for patterns in self.patterns.values())
        total_translations = len(self.learning_history)
        
        # Patterns les plus utilisés
        all_patterns = []
        for patterns in self.patterns.values():
            all_patterns.extend(patterns)
        
        top_patterns = sorted(all_patterns, key=lambda p: p.usage_count, reverse=True)[:5]
        
        return {
            'total_patterns': total_patterns,
            'total_translations': total_translations,
            'cache_size': len(self.translation_cache),
            'nltk_available': self.nltk_available,
            'db_context_loaded': bool(self.db_context.schema),
            'top_patterns': [{
                'pattern': p.nl_pattern,
                'usage_count': p.usage_count,
                'confidence': p.confidence
            } for p in top_patterns],
            'recent_translations': self.learning_history[-5:] if self.learning_history else []
        }


# Fonctions utilitaires
def create_translator(db_path: Optional[str] = None, patterns_path: Optional[str] = None) -> NLToSQLTranslator:
    """Créer un traducteur avec les paramètres donnés"""
    if patterns_path is None:
        # Chemin par défaut
        default_path = Path.home() / '.gsql' / 'nlp_patterns.json'
        patterns_path = str(default_path)
        default_path.parent.mkdir(exist_ok=True)
    
    return NLToSQLTranslator(patterns_path, db_path)


def nl_to_sql(nl_query: str, translator: Optional[NLToSQLTranslator] = None, 
              db_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Interface simplifiée pour la traduction NL->SQL
    
    Args:
        nl_query: Requête en langage naturel
        translator: Traducteur existant (optionnel)
        db_path: Chemin vers la base de données (optionnel)
    
    Returns:
        Dict avec le résultat de la traduction
    """
    if translator is None:
        translator = create_translator(db_path)
    
    return translator.translate(nl_query)


# Test
if __name__ == "__main__":
    # Créer un traducteur de test
    translator = NLToSQLTranslator()
    
    # Tests
    test_queries = [
        "montre les tables",
        "sélectionne tout de utilisateurs",
        "combien de clients",
        "moyenne d'âge des utilisateurs",
        "ajoute un nouvel utilisateur"
    ]
    
    print("Tests de traduction NL->SQL:")
    print("=" * 50)
    
    for query in test_queries:
        result = translator.translate(query)
        print(f"\nNL: {query}")
        print(f"SQL: {result['sql']}")
        print(f"Confiance: {result['confidence']:.2f}")
        print(f"Explication: {result['explanation']}")
        print("-" * 30)
    
    # Afficher les statistiques
    stats = translator.get_statistics()
    print(f"\nStatistiques du traducteur:")
    print(f"Patterns chargés: {stats['total_patterns']}")
    print(f"Traductions effectuées: {stats['total_translations']}")
    print(f"NLTK disponible: {stats['nltk_available']}")
