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
        
        # String functions
        self.register_function('UPPER', lambda x: x.upper() if x is not None else None)
        self.register_function('LOWER', lambda x: x.lower() if x is not None else None)
        self.register_function('LENGTH', lambda x: len(x) if x is not None else 0)
        self.register_function('TRIM', lambda x: x.strip() if x is not None else None)
        self.register_function('SUBSTR', lambda x, start, length=None: x[start-1:start-1+length] if x is not None and length else x[start-1:] if x is not None else None)
        self.register_function('REPLACE', lambda x, old, new: x.replace(old, new) if x is not None else None)
        
        # Date functions
        self.register_function('NOW', lambda: datetime.now().isoformat())
        self.register_function('DATE', lambda: datetime.now().strftime('%Y-%m-%d'))
        self.register_function('TIME', lambda: datetime.now().strftime('%H:%M:%S'))
        
        # Type conversion
        self.register_function('INT', lambda x: int(x) if x is not None else None)
        self.register_function('FLOAT', lambda x: float(x) if x is not None else None)
        self.register_function('STR', lambda x: str(x) if x is not None else None)
        
        # Aggregation functions (simplified)
        self.register_function('COUNT', lambda *args: len([a for a in args if a is not None]))
        self.register_function('SUM', lambda *args: sum([a for a in args if isinstance(a, (int, float))]))
        self.register_function('AVG', lambda *args: sum([a for a in args if isinstance(a, (int, float))]) / len([a for a in args if isinstance(a, (int, float))]) if args else None)
        self.register_function('MAX', lambda *args: max([a for a in args if isinstance(a, (int, float))]) if args else None)
        self.register_function('MIN', lambda *args: min([a for a in args if isinstance(a, (int, float))]) if args else None)
    
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