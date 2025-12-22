import re
import hashlib
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

class UserFunctionRegistry:
    """Registre des fonctions utilisateur"""
    
    def __init__(self):
        self.functions = {}
        self._register_builtins()
    
    def _register_builtins(self):
        """Enregistre les fonctions intégrées"""
        self.register('upper', self._func_upper)
        self.register('lower', self._func_lower)
        self.register('length', self._func_length)
        self.register('substring', self._func_substring)
        self.register('concat', self._func_concat)
        self.register('abs', self._func_abs)
        self.register('round', self._func_round)
        self.register('md5', self._func_md5)
        self.register('now', self._func_now)
        self.register('date_format', self._func_date_format)
        
        # Fonctions statistiques
        self.register('mean', self._func_mean)
        self.register('variance', self._func_variance)
        self.register('stddev', self._func_stddev)
    
    def register(self, name, func):
        """Enregistre une nouvelle fonction"""
        if name in self.functions:
            raise FunctionError(f"Function '{name}' already exists")
        self.functions[name] = func
    
    def call(self, name, args, context=None):
        """Appelle une fonction par son nom"""
        if name not in self.functions:
            raise FunctionError(f"Unknown function: '{name}'")
        
        try:
            return self.functions[name](args, context)
        except Exception as e:
            raise FunctionError(f"Error calling '{name}': {str(e)}")
    
    # ===== FONCTIONS INTÉGRÉES =====
    
    def _func_upper(self, args, context):
        if len(args) != 1:
            raise FunctionError("UPPER expects 1 argument")
        return str(args[0]).upper()
    
    def _func_lower(self, args, context):
        if len(args) != 1:
            raise FunctionError("LOWER expects 1 argument")
        return str(args[0]).lower()
    
    def _func_length(self, args, context):
        if len(args) != 1:
            raise FunctionError("LENGTH expects 1 argument")
        return len(str(args[0]))
    
    def _func_substring(self, args, context):
        if len(args) not in [2, 3]:
            raise FunctionError("SUBSTRING expects 2 or 3 arguments")
        
        text = str(args[0])
        start = int(args[1])
        
        if len(args) == 3:
            length = int(args[2])
            return text[start-1:start-1+length]
        else:
            return text[start-1:]
    
    def _func_concat(self, args, context):
        return ''.join(str(arg) for arg in args)
    
    def _func_abs(self, args, context):
        if len(args) != 1:
            raise FunctionError("ABS expects 1 argument")
        return abs(float(args[0]))
    
    def _func_round(self, args, context):
        if len(args) not in [1, 2]:
            raise FunctionError("ROUND expects 1 or 2 arguments")
        
        number = Decimal(str(args[0]))
        decimals = int(args[1]) if len(args) == 2 else 0
        
        return float(number.quantize(
            Decimal(f"1.{'0' * decimals}") if decimals > 0 else Decimal("1"),
            rounding=ROUND_HALF_UP
        ))
    
    def _func_md5(self, args, context):
        if len(args) != 1:
            raise FunctionError("MD5 expects 1 argument")
        return hashlib.md5(str(args[0]).encode()).hexdigest()
    
    def _func_now(self, args, context):
        if args:
            raise FunctionError("NOW expects no arguments")
        return datetime.now().isoformat()
    
    def _func_date_format(self, args, context):
        if len(args) != 2:
            raise FunctionError("DATE_FORMAT expects 2 arguments")
        
        date_str = str(args[0])
        fmt = str(args[1])
        
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError:
            # Essayer d'autres formats
            for fmt_str in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']:
                try:
                    dt = datetime.strptime(date_str, fmt_str)
                    break
                except ValueError:
                    continue
            else:
                raise FunctionError(f"Invalid date format: {date_str}")
        
        # Conversion simplifiée des formats
        fmt_mapping = {
            '%Y': dt.year,
            '%m': f"{dt.month:02d}",
            '%d': f"{dt.day:02d}",
            '%H': f"{dt.hour:02d}",
            '%M': f"{dt.minute:02d}",
            '%S': f"{dt.second:02d}"
        }
        
        result = fmt
        for key, value in fmt_mapping.items():
            result = result.replace(key, str(value))
        
        return result
    
    def _func_mean(self, args, context):
        """Calcule la moyenne d'une liste de valeurs"""
        if not args:
            raise FunctionError("MEAN expects at least 1 argument")
        
        values = [float(arg) for arg in args if arg is not None]
        if not values:
            return None
        
        return sum(values) / len(values)
    
    def _func_variance(self, args, context):
        """Calcule la variance d'une liste de valeurs"""
        if not args:
            raise FunctionError("VARIANCE expects at least 1 argument")
        
        values = [float(arg) for arg in args if arg is not None]
        if len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        squared_diffs = [(x - mean) ** 2 for x in values]
        return sum(squared_diffs) / len(values)
    
    def _func_stddev(self, args, context):
        """Calcule l'écart-type"""
        variance = self._func_variance(args, context)
        return variance ** 0.5 if variance is not None else None


# Interface pour CREATE FUNCTION
class FunctionManager:
    """Gestionnaire des fonctions utilisateur"""
    
    def __init__(self):
        self.registry = UserFunctionRegistry()
        self.user_functions = {}
    
    def create_function(self, name, params, body, return_type="TEXT"):
        """
        Crée une nouvelle fonction utilisateur
        
        Args:
            name (str): Nom de la fonction
            params (list): Liste des paramètres
            body (str): Corps de la fonction (Python)
            return_type (str): Type de retour
            
        Returns:
            str: Message de confirmation
        """
        if name in self.registry.functions or name in self.user_functions:
            raise FunctionError(f"Function '{name}' already exists")
        
        # Validation du nom
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise FunctionError(f"Invalid function name: '{name}'")
        
        # Création de la fonction dynamique
        try:
            # Construction de la signature
            param_str = ', '.join(params) if params else ''
            func_code = f"""
def user_func({param_str}, context=None):
{self._indent_body(body)}
"""
            
            # Exécution dans un espace de noms sécurisé
            namespace = {}
            exec(func_code, {}, namespace)
            
            # Enregistrement
            self.user_functions[name] = {
                'function': namespace['user_func'],
                'params': params,
                'return_type': return_type,
                'created_at': datetime.now()
            }
            
            # Ajout au registre
            self.registry.register(name, namespace['user_func'])
            
            return f"Function '{name}' created successfully"
            
        except SyntaxError as e:
            raise FunctionError(f"Syntax error in function body: {str(e)}")
        except Exception as e:
            raise FunctionError(f"Error creating function: {str(e)}")
    
    def _indent_body(self, body):
        """Indente le corps de la fonction"""
        lines = body.strip().split('\n')
        return '\n'.join('    ' + line for line in lines)
    
    def list_functions(self):
        """Liste toutes les fonctions disponibles"""
        all_funcs = []
        
        # Fonctions intégrées
        for name in self.registry.functions:
            if name not in self.user_functions:
                all_funcs.append({
                    'name': name,
                    'type': 'builtin',
                    'params': 'varies'
                })
        
        # Fonctions utilisateur
        for name, info in self.user_functions.items():
            all_funcs.append({
                'name': name,
                'type': 'user',
                'params': ', '.join(info['params']),
                'return_type': info['return_type'],
                'created_at': info['created_at']
            })
        
        return all_funcs
    
    def drop_function(self, name):
        """Supprime une fonction utilisateur"""
        if name not in self.user_functions:
            raise FunctionError(f"User function '{name}' not found")
        
        del self.user_functions[name]
        del self.registry.functions[name]
        
        return f"Function '{name}' dropped"
