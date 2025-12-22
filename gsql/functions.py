#!/usr/bin/env python3
"""
GSQL Function Manager - Gestion des fonctions personnalisées
"""

import json
import logging
import inspect
from typing import Dict, List, Any, Callable, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class FunctionError(Exception):
    """Exception pour les erreurs de fonction"""
    pass

class FunctionManager:
    """Gestionnaire de fonctions personnalisées GSQL"""
    
    def __init__(self):
        self.user_functions = {}  # name -> function
        self.function_metadata = {}  # name -> metadata
        self.function_stats = {}  # name -> usage stats
        
        # Enregistrer les fonctions intégrées par défaut
        self._register_builtin_functions()
    
    def _register_builtin_functions(self):
        """Enregistre les fonctions intégrées"""
        builtins = {
            'UPPER': self._builtin_upper,
            'LOWER': self._builtin_lower,
            'LENGTH': self._builtin_length,
            'ABS': self._builtin_abs,
            'ROUND': self._builtin_round,
            'CONCAT': self._builtin_concat,
            'NOW': self._builtin_now,
            'DATE': self._builtin_date,
            'SUM': self._builtin_sum,
            'AVG': self._builtin_avg,
            'COUNT': self._builtin_count,
            'MAX': self._builtin_max,
            'MIN': self._builtin_min
        }
        
        for name, func in builtins.items():
            self.register_function(name, func, num_params=-1, is_builtin=True)
    
    # ==================== FONCTIONS INTÉGRÉES ====================
    
    def _builtin_upper(self, text: str) -> str:
        """Convertit en majuscules"""
        return text.upper() if text else ''
    
    def _builtin_lower(self, text: str) -> str:
        """Convertit en minuscules"""
        return text.lower() if text else ''
    
    def _builtin_length(self, text: str) -> int:
        """Longueur d'une chaîne"""
        return len(text) if text else 0
    
    def _builtin_abs(self, number: float) -> float:
        """Valeur absolue"""
        try:
            return abs(float(number))
        except:
            return 0.0
    
    def _builtin_round(self, number: float, decimals: int = 0) -> float:
        """Arrondi"""
        try:
            return round(float(number), int(decimals))
        except:
            return float(number) if number else 0.0
    
    def _builtin_concat(self, *args) -> str:
        """Concatène des chaînes"""
        return ''.join(str(arg) for arg in args if arg is not None)
    
    def _builtin_now(self) -> str:
        """Date et heure actuelles"""
        return datetime.now().isoformat()
    
    def _builtin_date(self) -> str:
        """Date actuelle"""
        return datetime.now().strftime("%Y-%m-%d")
    
    def _builtin_sum(self, *args) -> float:
        """Somme de valeurs"""
        try:
            return sum(float(arg) for arg in args if arg is not None)
        except:
            return 0.0
    
    def _builtin_avg(self, *args) -> float:
        """Moyenne de valeurs"""
        try:
            values = [float(arg) for arg in args if arg is not None]
            return sum(values) / len(values) if values else 0.0
        except:
            return 0.0
    
    def _builtin_count(self, *args) -> int:
        """Compte les valeurs non-null"""
        return sum(1 for arg in args if arg is not None)
    
    def _builtin_max(self, *args) -> float:
        """Maximum"""
        try:
            values = [float(arg) for arg in args if arg is not None]
            return max(values) if values else 0.0
        except:
            return 0.0
    
    def _builtin_min(self, *args) -> float:
        """Minimum"""
        try:
            values = [float(arg) for arg in args if arg is not None]
            return min(values) if values else 0.0
        except:
            return 0.0
    
    # ==================== GESTION DES FONCTIONS ====================
    
    def register_function(self, name: str, func: Callable, 
                         num_params: int = -1, 
                         description: str = "",
                         is_builtin: bool = False) -> bool:
        """
        Enregistre une fonction personnalisée
        
        Args:
            name: Nom de la fonction
            func: Fonction Python
            num_params: Nombre de paramètres (-1 pour variable)
            description: Description de la fonction
            is_builtin: Si c'est une fonction intégrée
        
        Returns:
            bool: True si enregistrée avec succès
        """
        try:
            # Valider le nom
            if not name or not name.isidentifier():
                raise FunctionError(f"Nom de fonction invalide: '{name}'")
            
            # Obtenir les informations de la fonction
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            
            # Si num_params n'est pas spécifié, utiliser le nombre réel
            if num_params == -1:
                num_params = len(params)
            
            metadata = {
                'name': name,
                'function': func,
                'num_params': num_params,
                'params': params,
                'description': description or func.__doc__ or '',
                'is_builtin': is_builtin,
                'registered_at': datetime.now().isoformat(),
                'call_count': 0
            }
            
            self.user_functions[name] = func
            self.function_metadata[name] = metadata
            self.function_stats[name] = {'calls': 0, 'errors': 0}
            
            logger.info(f"Fonction '{name}' enregistrée ({num_params} paramètres)")
            return True
            
        except Exception as e:
            logger.error(f"Échec de l'enregistrement de la fonction '{name}': {e}")
            raise FunctionError(f"Impossible d'enregistrer la fonction '{name}': {e}")
    
    def unregister_function(self, name: str) -> bool:
        """Supprime une fonction enregistrée"""
        if name in self.user_functions:
            del self.user_functions[name]
            del self.function_metadata[name]
            del self.function_stats[name]
            logger.info(f"Fonction '{name}' supprimée")
            return True
        return False
    
    def execute_function(self, name: str, *args) -> Any:
        """
        Exécute une fonction enregistrée
        
        Args:
            name: Nom de la fonction
            *args: Arguments à passer
        
        Returns:
            Résultat de la fonction
        """
        if name not in self.user_functions:
            raise FunctionError(f"Fonction '{name}' non trouvée")
        
        try:
            # Mettre à jour les statistiques
            self.function_stats[name]['calls'] += 1
            
            # Exécuter la fonction
            result = self.user_functions[name](*args)
            
            return result
            
        except Exception as e:
            self.function_stats[name]['errors'] += 1
            logger.error(f"Erreur d'exécution de la fonction '{name}': {e}")
            raise FunctionError(f"Erreur dans la fonction '{name}': {e}")
    
    def get_function_info(self, name: str) -> Optional[Dict]:
        """Récupère les informations d'une fonction"""
        return self.function_metadata.get(name)
    
    def list_functions(self) -> List[Dict]:
        """Liste toutes les fonctions disponibles"""
        functions = []
        for name, metadata in self.function_metadata.items():
            functions.append({
                'name': name,
                'num_params': metadata['num_params'],
                'description': metadata['description'],
                'is_builtin': metadata['is_builtin'],
                'call_count': metadata['call_count'],
                'registered_at': metadata['registered_at']
            })
        
        # Trier par nom
        functions.sort(key=lambda x: x['name'])
        return functions
    
    def get_stats(self) -> Dict:
        """Récupère les statistiques d'utilisation"""
        total_calls = sum(stats['calls'] for stats in self.function_stats.values())
        total_errors = sum(stats['errors'] for stats in self.function_stats.values())
        
        return {
            'total_functions': len(self.user_functions),
            'builtin_functions': sum(1 for m in self.function_metadata.values() if m['is_builtin']),
            'user_functions': sum(1 for m in self.function_metadata.values() if not m['is_builtin']),
            'total_calls': total_calls,
            'total_errors': total_errors,
            'functions': self.function_stats.copy()
        }
    
    def save_to_file(self, filepath: str):
        """Sauvegarde les fonctions dans un fichier"""
        try:
            data = {
                'metadata': self.function_metadata,
                'stats': self.function_stats,
                'saved_at': datetime.now().isoformat()
            }
            
            # Convertir les fonctions en références (ne pas sérialiser le code)
            for name in data['metadata']:
                if 'function' in data['metadata'][name]:
                    data['metadata'][name]['function'] = 'python_function'
            
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Fonctions sauvegardées dans {filepath}")
            
        except Exception as e:
            logger.error(f"Échec de la sauvegarde: {e}")
            raise
    
    def load_from_file(self, filepath: str):
        """Charge les fonctions depuis un fichier"""
        # Note: Les fonctions Python ne peuvent pas être sérialisées
        # Cette méthode ne chargera que les métadonnées
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Réinitialiser
            self.user_functions.clear()
            self.function_metadata.clear()
            self.function_stats.clear()
            
            # Recharger les fonctions intégrées
            self._register_builtin_functions()
            
            logger.info(f"Fonctions chargées depuis {filepath}")
            
        except Exception as e:
            logger.error(f"Échec du chargement: {e}")
            raise

# ==================== FONCTIONS UTILITAIRES ====================

def create_function_manager() -> FunctionManager:
    """Crée une instance de FunctionManager"""
    return FunctionManager()

def get_default_function_manager() -> FunctionManager:
    """Récupère le gestionnaire de fonctions par défaut"""
    if not hasattr(get_default_function_manager, '_instance'):
        get_default_function_manager._instance = FunctionManager()
    return get_default_function_manager._instance

# ==================== FONCTIONS D'EXEMPLE ====================

def example_add(a: float, b: float) -> float:
    """Additionne deux nombres"""
    return float(a) + float(b)

def example_multiply(a: float, b: float) -> float:
    """Multiplie deux nombres"""
    return float(a) * float(b)

def example_greet(name: str) -> str:
    """Saluer une personne"""
    return f"Bonjour, {name}!"

def example_calculate_age(birth_year: int) -> int:
    """Calcule l'âge à partir de l'année de naissance"""
    current_year = datetime.now().year
    return current_year - int(birth_year)

# ==================== ENREGISTREMENT DES FONCTIONS D'EXEMPLE ====================

def register_example_functions(manager: FunctionManager = None):
    """Enregistre les fonctions d'exemple"""
    if manager is None:
        manager = get_default_function_manager()
    
    examples = [
        (example_add, 'ADD', 2, "Additionne deux nombres"),
        (example_multiply, 'MULTIPLY', 2, "Multiplie deux nombres"),
        (example_greet, 'GREET', 1, "Saluer une personne"),
        (example_calculate_age, 'CALCULATE_AGE', 1, "Calcule l'âge")
    ]
    
    for func, name, num_params, description in examples:
        try:
            manager.register_function(
                name=name,
                func=func,
                num_params=num_params,
                description=description
            )
        except Exception as e:
            logger.warning(f"Impossible d'enregistrer la fonction d'exemple {name}: {e}")

# ==================== EXPORT ====================

__all__ = [
    'FunctionManager',
    'FunctionError',
    'create_function_manager',
    'get_default_function_manager',
    'register_example_functions',
    'example_add',
    'example_multiply',
    'example_greet',
    'example_calculate_age'
]
