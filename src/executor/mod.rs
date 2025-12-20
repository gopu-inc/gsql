// src/executor/mod.rs
mod planner;
mod evaluator;

use crate::parser::ast::*;
use crate::storage::StorageEngine;
use crate::error::GSQLError;
use crate::Value;

pub struct QueryExecutor {
    planner: planner::QueryPlanner,
    evaluator: evaluator::ExpressionEvaluator,
}

impl QueryExecutor {
    pub fn new() -> Self {
        QueryExecutor {
            planner: planner::QueryPlanner::new(),
            evaluator: evaluator::ExpressionEvaluator::new(),
        }
    }
    
    pub fn execute(
        &self, 
        stmt: &Statement, 
        storage: &mut StorageEngine
    ) -> Result<QueryResult, GSQLError> {
        match stmt {
            Statement::CreateTable(stmt) => {
                storage.create_table(stmt)?;
                Ok(QueryResult::Create { table: stmt.table_name.clone() })
            }
            
            Statement::Insert(stmt) => {
                let rows_affected = storage.insert(stmt)?;
                Ok(QueryResult::Insert { rows_affected })
            }
            
            Statement::Select(stmt) => {
                // 1. Planifier l'exécution
                let plan = self.planner.plan_select(stmt, storage)?;
                
                // 2. Exécuter le plan
                let rows = plan.execute()?;
                
                // 3. Formater le résultat
                let columns = stmt.columns.iter()
                    .map(|c| match c {
                        SelectItem::Column(name) => name.clone(),
                        SelectItem::All => "*".to_string(),
                    })
                    .collect();
                
                Ok(QueryResult::Select { columns, rows })
            }
            
            Statement::Delete(stmt) => {
                // Implémenter plus tard
                Err(GSQLError::NotImplemented("DELETE"))
            }
        }
    }
}

// src/executor/planner.rs
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn new() -> Self {
        QueryPlanner
    }
    
    pub fn plan_select(
        &self, 
        stmt: &SelectStmt, 
        storage: &mut StorageEngine
    ) -> Result<ExecutionPlan, GSQLError> {
        // MVP: Plan simple - scan séquentiel
        // Phase 2: Ajouter l'optimisation
        Ok(ExecutionPlan::SeqScan(SeqScanPlan {
            table_name: match &stmt.from {
                TableRef::Table(name) => name.clone(),
                _ => return Err(GSQLError::NotImplemented("Joins")),
            },
            filter: stmt.where_clause.clone(),
        }))
    }
}

pub enum ExecutionPlan {
    SeqScan(SeqScanPlan),
    IndexScan(IndexScanPlan),
    // Plus tard: Join, Sort, Aggregate
}

pub struct SeqScanPlan {
    pub table_name: String,
    pub filter: Option<Expression>,
}
