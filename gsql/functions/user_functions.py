#!/usr/bin/env python3
"""
User-defined functions for GSQL
"""

import math
import re
import json
import hashlib
from datetime import datetime
from typing import Any, List, Dict, Optional

class FunctionManager:
    """Manager for user-defined functions"""
    
    def __init__(self):
        self.functions = {}
        self._register_builtin_functions()
    
    def _register_builtin_functions(self):
        """Register built-in functions"""
        # Math functions
        self.register_function('ABS', lambda x: abs(x) if x is not None else None)
        self.register_function('ROUND', lambda x, d=0: round(x, d) if x is not None else None)
        self.register_function('CEIL', lambda x: math.ceil(x) if x is not None else None)
        self.register_function('FLOOR', lambda x: math.floor(x) if x is not None else None)
        self.register_function('SQRT', lambda x: math.sqrt(x) if x is not None else None)
        self.register_function('POWER', lambda x, y: math.pow(x, y) if x is not None and y is not None else None)
        self.register_function('LOG', lambda x: math.log(x) if x is not None else None)
        self.register_function('LOG10', lambda x: math.log10(x) if x is not None else None)
        self.register_function('EXP', lambda x: math.exp(x) if x is not None else None)
        
        # String functions
        self.register_function('UPPER', lambda x: x.upper() if x is not None else None)
        self.register_function('LOWER', lambda x: x.lower() if x is not None else None)
        self.register_function('LENGTH', lambda x: len(x) if x is not None else 0)
        self.register_function('TRIM', lambda x: x.strip() if x is not None else None)
        self.register_function('LTRIM', lambda x: x.lstrip() if x is not None else None)
        self.register_function('RTRIM', lambda x: x.rstrip() if x is not None else None)
        self.register_function('SUBSTR', lambda x, start, length=None: x[start-1:start-1+length] if x is not None and length else x[start-1:] if x is not None else None)
        self.register_function('REPLACE', lambda x, old, new: x.replace(old, new) if x is not None else None)
        self.register_function('CONCAT', lambda *args: ''.join(str(arg) for arg in args if arg is not None))
        
        # Date functions
        self.register_function('NOW', lambda: datetime.now().isoformat())
        self.register_function('DATE', lambda: datetime.now().strftime('%Y-%m-%d'))
        self.register_function('TIME', lambda: datetime.now().strftime('%H:%M:%S'))
        
        # Type conversion
        self.register_function('INT', lambda x: int(x) if x is not None else None)
        self.register_function('FLOAT', lambda x: float(x) if x is not None else None)
        self.register_function('STR', lambda x: str(x) if x is not None else None)
        
        # Conditional
        self.register_function('IF', lambda condition, true_val, false_val: true_val if condition else false_val)
        self.register_function('COALESCE', lambda *args: next((arg for arg in args if arg is not None), None))
        
        # Hash functions
        self.register_function('MD5', lambda x: hashlib.md5(str(x).encode()).hexdigest() if x is not None else None)
        self.register_function('SHA1', lambda x: hashlib.sha1(str(x).encode()).hexdigest() if x is not None else None)
        
        # JSON functions
        self.register_function('JSON_EXTRACT', lambda json_str, path: self._json_extract(json_str, path))
        self.register_function('JSON_VALID', lambda json_str: self._json_valid(json_str))
    
    def _json_extract(self, json_str: str, path: str) -> Optional[Any]:
        """Extract value from JSON string"""
        try:
            data = json.loads(json_str)
            # Simple path extraction (supports dot notation)
            keys = path.split('.')
            for key in keys:
                if key.isdigit():
                    key = int(key)
                data = data[key]
            return data
        except:
            return None
    
    def _json_valid(self, json_str: str) -> bool:
        """Check if string is valid JSON"""
        try:
            json.loads(json_str)
            return True
        except:
            return False
    
    def register_function(self, name: str, func):
        """Register a new function"""
        self.functions[name.upper()] = func
    
    def execute_function(self, name: str, args: List[Any]) -> Any:
        """Execute a registered function"""
        func = self.functions.get(name.upper())
        if not func:
            raise ValueError(f"Function '{name}' not found")
        
        try:
            return func(*args)
        except Exception as e:
            raise ValueError(f"Error executing function '{name}': {e}")
    
    def get_functions(self) -> Dict[str, Any]:
        """Get all registered functions"""
        return {
            name: {
                'name': name,
                'type': 'builtin',
                'description': f'{name} function'
            }
            for name in self.functions.keys()
        }
    
    def clear(self):
        """Clear all functions (except builtin)"""
        # Keep only builtin functions
        self.functions = {}
        self._register_builtin_functions()
