#!/usr/bin/env python3
"""
Formateur de sortie pour GSQL CLI
"""

from .colors import Colors
import json
from typing import Any, Dict, List

class OutputFormatter:
    """Formate la sortie du CLI"""
    
    @staticmethod
    def format_result(result: Any) -> str:
        """Formate un résultat de requête"""
        if result is None:
            return f"{Colors.SUCCESS}✅ Command executed successfully{RESET}"
        
        if isinstance(result, str):
            return f"{Colors.INFO}📋 {result}{RESET}"
        
        if isinstance(result, dict):
            return OutputFormatter._format_dict_result(result)
        
        if isinstance(result, list):
            return OutputFormatter._format_list_result(result)
        
        return f"{Colors.INFO}📋 {result}{RESET}"
    
    @staticmethod
    def _format_dict_result(result: Dict) -> str:
        """Formate un résultat dictionnaire"""
        output_lines = []
        
        # HELP messages
        if result.get('type') == 'help' and 'message' in result:
            return f"{Colors.HELP}{result['message']}{RESET}"
        
        # SHOW TABLES
        if result.get('type') in ['show_tables', 'tables'] and 'rows' in result:
            rows = result['rows']
            if rows:
                message = result.get('message', f'Found {len(rows)} table(s):')
                output_lines.append(f"{Colors.INFO}📊 {message}{RESET}")
                Colors.print_table(None, rows)
            else:
                output_lines.append(f"{Colors.WARNING}📭 No tables found{RESET}")
            return '\n'.join(output_lines)
        
        # SHOW FUNCTIONS
        if result.get('type') == 'show_functions' and 'rows' in result:
            rows = result['rows']
            if rows:
                message = result.get('message', f'Found {len(rows)} function(s):')
                output_lines.append(f"{Colors.INFO}🔧 {message}{RESET}")
                
                for row in rows:
                    if isinstance(row, dict):
                        func_name = row.get('name', 'unknown')
                        func_type = row.get('type', 'unknown')
                        desc = row.get('description', '')
                        
                        if func_type == 'builtin':
                            output_lines.append(f"  {Colors.SQL_FUNCTION}📦 {func_name}{RESET} - {desc}")
                        else:
                            created = row.get('created_at', '')
                            if hasattr(created, 'strftime'):
                                created = created.strftime('%Y-%m-%d')
                            output_lines.append(f"  {Colors.SUCCESS}👤 {func_name}{RESET} - User function ({Colors.DIM}{created}{RESET})")
                    else:
                        output_lines.append(f"  {row}")
            else:
                output_lines.append(f"{Colors.WARNING}📭 No functions found{RESET}")
            return '\n'.join(output_lines)
        
        # SELECT results
        if 'rows' in result and result['rows']:
            rows = result['rows']
            count = result.get('count', len(rows))
            
            output_lines.append(f"{Colors.INFO}📊 Results: {count} row(s){RESET}")
            Colors.print_table(None, rows)
            return '\n'.join(output_lines)
        
        # Messages simples
        if 'message' in result:
            msg_type = result.get('type', 'info')
            if msg_type == 'error':
                return f"{Colors.ERROR}❌ {result['message']}{RESET}"
            elif msg_type == 'warning':
                return f"{Colors.WARNING}⚠ {result['message']}{RESET}"
            elif msg_type == 'success':
                return f"{Colors.SUCCESS}✅ {result['message']}{RESET}"
            else:
                return f"{Colors.INFO}📋 {result['message']}{RESET}"
        
        # Erreur
        if 'error' in result:
            return f"{Colors.ERROR}❌ {result['error']}{RESET}"
        
        # JSON format
        return f"{Colors.INFO}📋 {json.dumps(result, indent=2, default=str)}{RESET}"
    
    @staticmethod
    def _format_list_result(result: List) -> str:
        """Formate un résultat liste"""
        if not result:
            return f"{Colors.WARNING}📭 No results{RESET}"
        
        output_lines = [f"{Colors.INFO}📊 Results: {len(result)} row(s){RESET}"]
        
        for i, row in enumerate(result[:50], 1):
            if isinstance(row, dict):
                row_str = json.dumps(row, default=str)
            else:
                row_str = str(row)
            
            # Alterner les couleurs
            row_color = Colors.ROW if i % 2 == 0 else Colors.ROW_ALT
            output_lines.append(f"{row_color}{i:3}. {row_str}{RESET}")
        
        if len(result) > 50:
            remaining = len(result) - 50
            output_lines.append(f"{Colors.INFO}... and {remaining} more rows{RESET}")
        
        return '\n'.join(output_lines)
    
    @staticmethod
    def format_sql(sql: str) -> str:
        """Formate et colorise le SQL"""
        return Colors.colorize_sql(sql)
    
    @staticmethod
    def format_nlp_question(question: str) -> str:
        """Formate une question NLP"""
        return f"{Colors.NLP_QUESTION}🔍 Question: {question}{RESET}"
    
    @staticmethod
    def format_nlp_sql(sql: str) -> str:
        """Formate le SQL généré par NLP"""
        colored_sql = Colors.colorize_sql(sql)
        return f"{Colors.NLP_SQL}📊 SQL généré: {colored_sql}{RESET}"
