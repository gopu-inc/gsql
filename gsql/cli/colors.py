#!/usr/bin/env python3
"""
Module de couleurs pour GSQL CLI
"""

try:
    from colorama import init, Fore, Back, Style, Cursor
    init(autoreset=True)  # Réinitialise les couleurs après chaque print
    
    # Couleurs principales
    RED = Fore.RED
    GREEN = Fore.GREEN
    YELLOW = Fore.YELLOW
    BLUE = Fore.BLUE
    MAGENTA = Fore.MAGENTA
    CYAN = Fore.CYAN
    WHITE = Fore.WHITE
    
    # Styles
    BRIGHT = Style.BRIGHT
    DIM = Style.DIM
    RESET = Style.RESET_ALL
    
    # Backgrounds
    BG_RED = Back.RED
    BG_GREEN = Back.GREEN
    BG_YELLOW = Back.YELLOW
    BG_BLUE = Back.BLUE
    
    # Utilitaires
    UP = Cursor.UP
    CLEAR_LINE = '\033[2K'
    
    COLORAMA_AVAILABLE = True
    
except ImportError:
    # Fallback sans colorama
    RED = GREEN = YELLOW = BLUE = MAGENTA = CYAN = WHITE = ''
    BRIGHT = DIM = RESET = ''
    BG_RED = BG_GREEN = BG_YELLOW = BG_BLUE = ''
    UP = ''
    CLEAR_LINE = ''
    COLORAMA_AVAILABLE = False


class Colors:
    """Classe utilitaire pour les couleurs GSQL"""
    
    # Titres et en-têtes
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
    ROW_ALT = f"{DIM}{WHITE}"  # Pour lignes alternées
    
    # Syntaxe SQL
    SQL_KEYWORD = f"{BRIGHT}{YELLOW}"
    SQL_FUNCTION = f"{BRIGHT}{MAGENTA}"
    SQL_STRING = f"{GREEN}"
    SQL_NUMBER = f"{YELLOW}"
    SQL_COMMENT = f"{DIM}{GREEN}"
    
    # NLP
    NLP_QUESTION = f"{BRIGHT}{CYAN}"
    NLP_SQL = f"{BRIGHT}{MAGENTA}"
    
    # Types
    TYPE_STRING = f"{GREEN}"
    TYPE_NUMBER = f"{YELLOW}"
    TYPE_BOOL = f"{MAGENTA}"
    TYPE_NULL = f"{DIM}{WHITE}"
    
    @staticmethod
    def colorize_sql(sql: str) -> str:
        """Colorise le code SQL"""
        if not COLORAMA_AVAILABLE:
            return sql
        
        import re
        
        # Mots-clés SQL
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
            'ALTER', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
            'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET',
            'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL',
            'TRUE', 'FALSE', 'ASC', 'DESC', 'DISTINCT', 'UNIQUE',
            'PRIMARY KEY', 'FOREIGN KEY', 'REFERENCES', 'CONSTRAINT',
            'INDEX', 'VIEW', 'TRIGGER', 'PROCEDURE', 'FUNCTION',
            'BEGIN', 'END', 'COMMIT', 'ROLLBACK', 'TRANSACTION',
            'GRANT', 'REVOKE', 'EXPLAIN', 'ANALYZE', 'VACUUM'
        ]
        
        # Fonctions SQL
        functions = [
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'UPPER', 'LOWER',
            'LENGTH', 'SUBSTR', 'TRIM', 'COALESCE', 'NULLIF',
            'DATE', 'TIME', 'DATETIME', 'JULIANDAY', 'STRFTIME',
            'ABS', 'ROUND', 'RANDOM', 'TYPEOF', 'CHANGES', 'LAST_INSERT_ROWID'
        ]
        
        # Appliquer les couleurs
        colored = sql
        
        # Commentaires
        colored = re.sub(r'--.*$', f"{Colors.SQL_COMMENT}\\g<0>{RESET}", colored, flags=re.MULTILINE)
        colored = re.sub(r'/\*.*?\*/', f"{Colors.SQL_COMMENT}\\g<0>{RESET}", colored, flags=re.DOTALL)
        
        # Chaînes de caractères
        colored = re.sub(r"'[^']*'", f"{Colors.SQL_STRING}\\g<0>{RESET}", colored)
        colored = re.sub(r'"[^"]*"', f"{Colors.SQL_STRING}\\g<0>{RESET}", colored)
        
        # Nombres
        colored = re.sub(r'\b\d+(\.\d+)?\b', f"{Colors.SQL_NUMBER}\\g<0>{RESET}", colored)
        
        # Mots-clés SQL (doit être après les chaînes pour éviter de coloriser à l'intérieur)
        for keyword in keywords:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            colored = re.sub(pattern, f"{Colors.SQL_KEYWORD}{keyword}{RESET}", colored, flags=re.IGNORECASE)
        
        # Fonctions SQL
        for func in functions:
            pattern = r'\b' + re.escape(func) + r'\('
            colored = re.sub(pattern, f"{Colors.SQL_FUNCTION}{func}{RESET}(", colored, flags=re.IGNORECASE)
        
        return colored
    
    @staticmethod
    def print_table(headers, rows, max_rows=50):
        """Affiche un tableau avec des couleurs"""
        if not rows:
            print(f"{Colors.WARNING}📭 No data to display{RESET}")
            return
        
        try:
            from tabulate import tabulate
            
            # Préparer les données avec couleurs
            colored_rows = []
            for i, row in enumerate(rows[:max_rows]):
                if isinstance(row, dict):
                    colored_row = {}
                    for key, value in row.items():
                        colored_key = f"{Colors.COLUMN}{key}{RESET}"
                        # Coloriser la valeur selon son type
                        if value is None:
                            colored_value = f"{Colors.TYPE_NULL}NULL{RESET}"
                        elif isinstance(value, (int, float)):
                            colored_value = f"{Colors.TYPE_NUMBER}{value}{RESET}"
                        elif isinstance(value, bool):
                            colored_value = f"{Colors.TYPE_BOOL}{value}{RESET}"
                        elif isinstance(value, str):
                            colored_value = f"{Colors.TYPE_STRING}{value}{RESET}"
                        else:
                            colored_value = str(value)
                        
                        colored_row[colored_key] = colored_value
                    colored_rows.append(colored_row)
                else:
                    colored_rows.append(row)
            
            # Afficher le tableau
            table = tabulate(colored_rows, headers="keys", tablefmt="grid")
            print(f"{Colors.TABLE}{table}{RESET}")
            
            if len(rows) > max_rows:
                remaining = len(rows) - max_rows
                print(f"{Colors.INFO}... and {remaining} more rows{RESET}")
                
        except ImportError:
            # Fallback sans tabulate
            Colors._print_simple_table(headers, rows, max_rows)
    
    @staticmethod
    def _print_simple_table(headers, rows, max_rows):
        """Affiche un tableau simple avec couleurs"""
        if isinstance(rows[0], dict):
            headers = list(rows[0].keys())
        
        # Calculer les largeurs de colonnes
        col_widths = {}
        for header in headers:
            col_widths[header] = len(str(header))
            for row in rows[:max_rows]:
                if isinstance(row, dict):
                    value = str(row.get(header, ''))
                else:
                    value = str(row)
                col_widths[header] = max(col_widths[header], len(value))
        
        # En-tête
        header_line = " │ ".join([f"{Colors.COLUMN}{str(h).ljust(col_widths[h])}{RESET}" for h in headers])
        separator = "─┼─".join(["─" * col_widths[h] for h in headers])
        
        print(f"{Colors.HEADER}{header_line}{RESET}")
        print(f"{Colors.HEADER}{separator}{RESET}")
        
        # Lignes
        for i, row in enumerate(rows[:max_rows]):
            if isinstance(row, dict):
                row_values = [str(row.get(h, '')) for h in headers]
            else:
                row_values = [str(row)]
            
            # Alterner les couleurs de ligne
            row_color = Colors.ROW if i % 2 == 0 else Colors.ROW_ALT
            row_line = " │ ".join([f"{row_color}{v.ljust(col_widths[h])}{RESET}" for v, h in zip(row_values, headers)])
            print(row_line)
        
        if len(rows) > max_rows:
            remaining = len(rows) - max_rows
            print(f"{Colors.INFO}... and {remaining} more rows{RESET}")
    
    @staticmethod
    def progress_bar(iteration, total, prefix='', suffix='', length=50, fill='█'):
        """Affiche une barre de progression"""
        if not COLORAMA_AVAILABLE:
            return
        
        percent = ("{0:.1f}").format(100 * (iteration / float(total)))
        filled_length = int(length * iteration // total)
        bar = fill * filled_length + '░' * (length - filled_length)
        
        # Changer la couleur selon le pourcentage
        if percent >= 100:
            color = Colors.SUCCESS
        elif percent >= 70:
            color = Colors.INFO
        elif percent >= 40:
            color = Colors.WARNING
        else:
            color = Colors.ERROR
        
        print(f'\r{color}{prefix} |{bar}| {percent}% {suffix}{RESET}', end='\r')
        
        if iteration == total:
            print()
