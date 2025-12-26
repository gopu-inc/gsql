"""
Fonctions utilisateur pour GSQL
"""

import math
import re
from datetime import datetime
from typing import Any, List, Dict, Callable, Optional

# ==================== CLASSE FunctionManager ====================

class FunctionManager:
    """Gestionnaire des fonctions utilisateur pour GSQL"""
    
    def __init__(self):
        self.functions = {}
        self._register_builtins()
    
    def _register_builtins(self):
        """Enregistre toutes les fonctions intégrées"""
        # String functions
        self.register('UPPER', UPPER)
        self.register('LOWER', LOWER)
        self.register('LENGTH', LENGTH)
        self.register('CONCAT', CONCAT)
        self.register('SUBSTR', SUBSTR)
        self.register('TRIM', TRIM)
        
        # Math functions
        self.register('ABS', ABS)
        self.register('ROUND', ROUND)
        self.register('SQRT', SQRT)
        self.register('POWER', POWER)
        self.register('MOD', MOD)
        
        # Date/Time functions
        self.register('NOW', NOW)
        self.register('DATE', DATE)
        self.register('TIME', TIME)
        self.register('YEAR', YEAR)
        self.register('MONTH', MONTH)
        self.register('DAY', DAY)
        
        # Aggregation functions
        self.register('SUM', SUM)
        self.register('AVG', AVG)
        self.register('COUNT', COUNT)
        self.register('MAX', MAX)
        self.register('MIN', MIN)
        
        # Validation functions
        self.register('IS_EMAIL', IS_EMAIL)
        self.register('IS_NUMBER', IS_NUMBER)
        self.register('IS_DATE', IS_DATE)
    
    def register(self, name: str, func: Callable) -> None:
        """Enregistre une nouvelle fonction"""
        self.functions[name.upper()] = func
    
    def get(self, name: str) -> Optional[Callable]:
        """Récupère une fonction par son nom"""
        return self.functions.get(name.upper())
    
    def execute(self, name: str, *args) -> Any:
        """Exécute une fonction avec ses arguments"""
        func = self.get(name)
        if func:
            try:
                return func(*args)
            except Exception as e:
                raise Exception(f"Error executing function {name}: {str(e)}")
        raise Exception(f"Function {name} not found")
    
    def list_functions(self) -> List[str]:
        """Liste toutes les fonctions disponibles"""
        return list(self.functions.keys())
    
    def has_function(self, name: str) -> bool:
        """Vérifie si une fonction existe"""
        return name.upper() in self.functions

# ==================== FONCTIONS STRING ====================

def UPPER(text: str) -> str:
    """Convertit en majuscules"""
    return text.upper() if text else ''

def LOWER(text: str) -> str:
    """Convertit en minuscules"""
    return text.lower() if text else ''

def LENGTH(text: str) -> int:
    """Longueur d'une chaîne"""
    return len(text) if text else 0

def CONCAT(*args) -> str:
    """Concatène des chaînes"""
    return ''.join(str(arg) for arg in args if arg is not None)

def SUBSTR(text: str, start: int, length: int = None) -> str:
    """Sous-chaîne"""
    if not text:
        return ''
    if length:
        return text[start:start+length]
    return text[start:]

def TRIM(text: str) -> str:
    """Supprime les espaces"""
    return text.strip() if text else ''

# ==================== FONCTIONS MATHÉMATIQUES ====================

def ABS(number: float) -> float:
    """Valeur absolue"""
    try:
        return abs(float(number))
    except:
        return 0.0

def ROUND(number: float, decimals: int = 0) -> float:
    """Arrondi"""
    try:
        return round(float(number), int(decimals))
    except:
        return float(number) if number else 0.0

def SQRT(number: float) -> float:
    """Racine carrée"""
    try:
        return math.sqrt(float(number))
    except:
        return 0.0

def POWER(base: float, exponent: float) -> float:
    """Puissance"""
    try:
        return float(base) ** float(exponent)
    except:
        return 0.0

def MOD(dividend: float, divisor: float) -> float:
    """Modulo"""
    try:
        return float(dividend) % float(divisor)
    except:
        return 0.0

# ==================== FONCTIONS DATE/HEURE ====================

def NOW() -> str:
    """Date et heure actuelles"""
    return datetime.now().isoformat()

def DATE() -> str:
    """Date actuelle"""
    return datetime.now().strftime("%Y-%m-%d")

def TIME() -> str:
    """Heure actuelle"""
    return datetime.now().strftime("%H:%M:%S")

def YEAR(date_str: str = None) -> int:
    """Année d'une date"""
    if date_str:
        try:
            return datetime.fromisoformat(date_str.replace('Z', '')).year
        except:
            pass
    return datetime.now().year

def MONTH(date_str: str = None) -> int:
    """Mois d'une date"""
    if date_str:
        try:
            return datetime.fromisoformat(date_str.replace('Z', '')).month
        except:
            pass
    return datetime.now().month

def DAY(date_str: str = None) -> int:
    """Jour d'une date"""
    if date_str:
        try:
            return datetime.fromisoformat(date_str.replace('Z', '')).day
        except:
            pass
    return datetime.now().day

# ==================== FONCTIONS D'AGRÉGATION ====================

def SUM(*args) -> float:
    """Somme de valeurs"""
    try:
        return sum(float(arg) for arg in args if arg is not None)
    except:
        return 0.0

def AVG(*args) -> float:
    """Moyenne de valeurs"""
    try:
        values = [float(arg) for arg in args if arg is not None]
        return sum(values) / len(values) if values else 0.0
    except:
        return 0.0

def COUNT(*args) -> int:
    """Compte les valeurs non-null"""
    return sum(1 for arg in args if arg is not None)

def MAX(*args) -> float:
    """Maximum"""
    try:
        values = [float(arg) for arg in args if arg is not None]
        return max(values) if values else 0.0
    except:
        return 0.0

def MIN(*args) -> float:
    """Minimum"""
    try:
        values = [float(arg) for arg in args if arg is not None]
        return min(values) if values else 0.0
    except:
        return 0.0

# ==================== FONCTIONS DE VALIDATION ====================

def IS_EMAIL(text: str) -> bool:
    """Vérifie si c'est un email valide"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, text)) if text else False

def IS_NUMBER(text: str) -> bool:
    """Vérifie si c'est un nombre"""
    try:
        float(text)
        return True
    except:
        return False

def IS_DATE(text: str) -> bool:
    """Vérifie si c'est une date valide"""
    try:
        datetime.fromisoformat(text.replace('Z', ''))
        return True
    except:
        return False

# ==================== FONCTION D'ENREGISTREMENT ====================

def register_function(name: str, func: Callable) -> None:
    """Fonction utilitaire pour enregistrer une nouvelle fonction"""
    # Cette fonction sera utilisée par le code externe
    # pour enregistrer des fonctions personnalisées
    # Le manager sera initialisé dans __init__.py
    pass

# ==================== EXPORT ====================

__all__ = [
    # Classe principale
    'FunctionManager',
    
    # String
    'UPPER', 'LOWER', 'LENGTH', 'CONCAT', 'SUBSTR', 'TRIM',
    
    # Math
    'ABS', 'ROUND', 'SQRT', 'POWER', 'MOD',
    
    # Date/Heure
    'NOW', 'DATE', 'TIME', 'YEAR', 'MONTH', 'DAY',
    
    # Agrégation
    'SUM', 'AVG', 'COUNT', 'MAX', 'MIN',
    
    # Validation
    'IS_EMAIL', 'IS_NUMBER', 'IS_DATE',
    
    # Utilitaire
    'register_function'
]