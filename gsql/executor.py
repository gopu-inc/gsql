# gsql/gsql/executor.py
"""
Query executor
"""

class QueryExecutor:
    """Execute SQL queries"""
    
    def __init__(self, storage):
        self.storage = storage
    
    def execute(self, ast):
        """Execute AST"""
        query_type = ast.get('type')
        
        if query_type == 'CREATE_TABLE':
            return self._execute_create(ast)
        elif query_type == 'INSERT':
            return self._execute_insert(ast)
        elif query_type == 'SELECT':
            return self._execute_select(ast)
        elif query_type == 'DELETE':
            return self._execute_delete(ast)
        else:
            from .exceptions import GQLExecutionError
            raise GQLExecutionError(f"Unsupported query type: {query_type}")
    
    def _execute_create(self, ast):
        """Execute CREATE TABLE"""
        table_name = ast['table']
        columns = ast['columns']
        
        self.storage.create_table(table_name, columns)
        
        return {
            'type': 'CREATE_TABLE',
            'table': table_name,
            'columns': len(columns)
        }
    
    def _execute_insert(self, ast):
        """Execute INSERT"""
        table_name = ast['table']
        columns = ast.get('columns', [])
        values = ast['values']
        
        # Convert to list of dicts
        rows = []
        for row_values in values:
            row = {}
            
            if columns:
                # With column names
                for i, col in enumerate(columns):
                    if i < len(row_values):
                        row[col] = row_values[i]
            else:
                # Without column names (assume all columns)
                for i, val in enumerate(row_values):
                    row[f'col_{i}'] = val
            
            rows.append(row)
        
        rows_affected = self.storage.insert(table_name, rows)
        
        return {
            'type': 'INSERT',
            'table': table_name,
            'rows_affected': rows_affected
        }
    
    def _execute_select(self, ast):
        """Execute SELECT"""
        table_name = ast['table']
        columns = ast['columns']
        where_ast = ast.get('where')
        
        # Convert WHERE AST to dict
        where = None
        if where_ast:
            where = {}
            for cond in where_ast.get('conditions', []):
                if cond['operator'] == '=':
                    where[cond['column']] = cond['value']
        
        data = self.storage.select(table_name, where, columns)
        
        return {
            'type': 'SELECT',
            'table': table_name,
            'data': data,
            'row_count': len(data)
        }
    
    def _execute_delete(self, ast):
        """Execute DELETE"""
        table_name = ast['table']
        where_ast = ast.get('where')
        
        where = None
        if where_ast:
            where = {}
            for cond in where_ast.get('conditions', []):
                if cond['operator'] == '=':
                    where[cond['column']] = cond['value']
        
        rows_affected = self.storage.delete(table_name, where)
        
        return {
            'type': 'DELETE',
            'table': table_name,
            'rows_affected': rows_affected
        }
