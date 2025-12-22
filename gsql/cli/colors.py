#!/usr/bin/env python3
"""
Module de couleurs pour GSQL CLI
"""

try:
    from colorama import init, Fore, Back, Style, Cursor
    init(autoreset=True)
    
    # Couleurs de base
    RED = Fore.RED
    GREEN = Fore.GREEN
    YELLOW = Fore.YELLOW
    BLUE = Fore.BLUE
    MAGENTA = Fore.MAGENTA
    CYAN = Fore.CYAN
    WHITE = Fore.WHITE
    BLACK = Fore.BLACK
    
    # Styles
    BRIGHT = Style.BRIGHT
    DIM = Style.DIM
    NORMAL = Style.NORMAL
    RESET = Style.RESET_ALL
    
    # Backgrounds
    BG_RED = Back.RED
    BG_GREEN = Back.GREEN
    BG_YELLOW = Back.YELLOW
    BG_BLUE = Back.BLUE
    BG_MAGENTA = Back.MAGENTA
    BG_CYAN = Back.CYAN
    BG_WHITE = Back.WHITE
    
    COLORAMA_AVAILABLE = True
    
except ImportError:
    # Fallback sans colorama - définir TOUS les attributs
    RED = GREEN = YELLOW = BLUE = MAGENTA = CYAN = WHITE = BLACK = ''
    BRIGHT = DIM = NORMAL = RESET = ''
    BG_RED = BG_GREEN = BG_YELLOW = BG_BLUE = BG_MAGENTA = BG_CYAN = BG_WHITE = ''
    COLORAMA_AVAILABLE = False


class Colors:
    """Classe utilitaire pour les couleurs GSQL"""
    
    # Définir TOUS les attributs statiques
    TITLE = f"{BRIGHT}{CYAN}"
    HEADER = f"{BRIGHT}{BLUE}"
    PROMPT = f"{BRIGHT}{GREEN}"
    
    # Messages
    SUCCESS = f"{BRIGHT}{GREEN}"
    ERROR = f"{BRIGHT}{RED}"
    WARNING = f"{BRIGHT}{YELLOW}"
    INFO = f"{BRIGHT}{CYAN}"
    HELP = f"{DIM}{WHITE}"
    
    # Données
    TABLE = f"{BRIGHT}{WHITE}"
    COLUMN = f"{CYAN}"
    ROW = f"{WHITE}"
    ROW_ALT = f"{DIM}{WHITE}"
    
    # SQL - IMPORTANT: définir TOUS ces attributs
    SQL_KEYWORD = f"{BRIGHT}{YELLOW}"
    SQL_FUNCTION = f"{BRIGHT}{MAGENTA}"
    SQL_STRING = f"{GREEN}"
    SQL_NUMBER = f"{YELLOW}"
    SQL_COMMENT = f"{DIM}{GREEN}"
    SQL_TABLE = f"{BRIGHT}{CYAN}"
    SQL_COLUMN = f"{CYAN}"
    
    # NLP
    NLP_QUESTION = f"{BRIGHT}{CYAN}"
    NLP_SQL = f"{BRIGHT}{MAGENTA}"
    
    # Types de données
    TYPE_STRING = f"{GREEN}"
    TYPE_NUMBER = f"{YELLOW}"
    TYPE_BOOL = f"{MAGENTA}"
    TYPE_NULL = f"{DIM}{WHITE}"
    TYPE_DATE = f"{BLUE}"
    
    # Autres
    RESET = RESET
    
    @staticmethod
    def colorize_sql(sql: str) -> str:
        """Colorise le code SQL"""
        if not COLORAMA_AVAILABLE or not sql:
            return sql
        
        try:
            import re
            
            # Appliquer les couleurs
            colored = sql
            
            # 1. Commentaires
            colored = re.sub(r'--.*$', f"{Colors.SQL_COMMENT}\\g<0>{Colors.RESET}", colored, flags=re.MULTILINE)
            
            # 2. Chaînes de caractères
            colored = re.sub(r"'[^']*'", f"{Colors.SQL_STRING}\\g<0>{Colors.RESET}", colored)
            colored = re.sub(r'"[^"]*"', f"{Colors.SQL_STRING}\\g<0>{Colors.RESET}", colored)
            
            # 3. Nombres
            colored = re.sub(r'\b\d+(\.\d+)?\b', f"{Colors.SQL_NUMBER}\\g<0>{Colors.RESET}", colored)
            
            # 4. Mots-clés SQL principaux
            keywords = [
                'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
                'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
                'ALTER', 'JOIN', 'GROUP BY', 'ORDER BY', 'LIMIT',
                'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL',
                'TRUE', 'FALSE', 'ASC', 'DESC', 'DISTINCT'
            ]
            
            for keyword in keywords:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                colored = re.sub(pattern, f"{Colors.SQL_KEYWORD}{keyword}{Colors.RESET}", colored, flags=re.IGNORECASE)
            
            # 5. Fonctions SQL
            functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'UPPER', 'LOWER']
            for func in functions:
                pattern = r'\b' + re.escape(func) + r'\s*\('
                colored = re.sub(pattern, f"{Colors.SQL_FUNCTION}{func}{Colors.RESET}(", colored, flags=re.IGNORECASE)
            
            return colored
            
        except Exception:
            return sql  # Retourner le SQL original en cas d'erreur
    
    @staticmethod
    def print_table(headers, rows, max_rows=50):
        """Affiche un tableau avec des couleurs"""
        if not rows:
            print(f"{Colors.WARNING}📭 No data to display{Colors.RESET}")
            return
        
        try:
            from tabulate import tabulate
            
            # Si ce sont des dictionnaires
            if isinstance(rows[0], dict):
                colored_rows = []
                for i, row in enumerate(rows[:max_rows]):
                    colored_row = {}
                    for key, value in row.items():
                        # Coloriser la clé
                        colored_key = f"{Colors.COLUMN}{key}{Colors.RESET}"
                        
                        # Coloriser la valeur selon son type
                        if value is None:
                            colored_value = f"{Colors.TYPE_NULL}NULL{Colors.RESET}"
                        elif isinstance(value, bool):
                            colored_value = f"{Colors.TYPE_BOOL}{value}{Colors.RESET}"
                        elif isinstance(value, (int, float)):
                            colored_value = f"{Colors.TYPE_NUMBER}{value}{Colors.RESET}"
                        elif isinstance(value, str):
                            # Vérifier si c'est une date
                            if any(x in key.lower() for x in ['date', 'time', 'created', 'updated']):
                                colored_value = f"{Colors.TYPE_DATE}{value}{Colors.RESET}"
                            else:
                                colored_value = f"{Colors.TYPE_STRING}{value}{Colors.RESET}"
                        else:
                            colored_value = str(value)
                        
                        colored_row[colored_key] = colored_value
                    colored_rows.append(colored_row)
                
                table = tabulate(colored_rows, headers="keys", tablefmt="grid")
                print(f"{Colors.TABLE}{table}{Colors.RESET}")
            
            else:
                # Simple liste
                table = tabulate(rows, headers=headers, tablefmt="grid")
                print(f"{Colors.TABLE}{table}{Colors.RESET}")
            
            if len(rows) > max_rows:
                remaining = len(rows) - max_rows
                print(f"{Colors.INFO}... and {remaining} more rows{Colors.RESET}")
                
        except ImportError:
            # Fallback simple
            Colors._print_simple_table(headers, rows, max_rows)
    
    @staticmethod
    def _print_simple_table(headers, rows, max_rows):
        """Affiche un tableau simple avec couleurs"""
        if not rows:
            return
        
        # Déterminer les en-têtes
        if isinstance(rows[0], dict):
            headers = list(rows[0].keys())
        
        # Calculer les largeurs
        col_widths = {}
        for header in headers:
            col_widths[header] = len(str(header))
            for row in rows[:max_rows]:
                if isinstance(row, dict):
                    value = str(row.get(header, ''))
                else:
                    value = str(row)
                col_widths[header] = max(col_widths[header], len(value))
        
        # Afficher l'en-tête
        header_line = " │ ".join([f"{Colors.COLUMN}{str(h).ljust(col_widths[h])}{Colors.RESET}" for h in headers])
        print(header_line)
        print("─┼─".join(["─" * col_widths[h] for h in headers]))
        
        # Afficher les lignes
        for i, row in enumerate(rows[:max_rows]):
            if isinstance(row, dict):
                values = [str(row.get(h, '')) for h in headers]
            else:
                values = [str(row)]
            
            # Alterner les couleurs
            if i % 2 == 0:
                row_line = " │ ".join([f"{Colors.ROW}{v.ljust(col_widths[h])}{Colors.RESET}" for v, h in zip(values, headers)])
            else:
                row_line = " │ ".join([f"{Colors.ROW_ALT}{v.ljust(col_widths[h])}{Colors.RESET}" for v, h in zip(values, headers)])
            
            print(row_line)
        
        if len(rows) > max_rows:
            remaining = len(rows) - max_rows
            print(f"{Colors.INFO}... and {remaining} more rows{Colors.RESET}")
