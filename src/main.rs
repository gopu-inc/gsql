// src/main.rs
mod storage;
mod parser;
mod executor;
mod error;

use error::GSQLError;

#[derive(Debug)]
pub struct GSQL {
    storage: storage::StorageEngine,
    parser: parser::SQLParser,
    executor: executor::QueryExecutor,
}

impl GSQL {
    // Initialisation - comme __init__
    pub fn new(path: Option<&str>) -> Result<Self, GSQLError> {
        let storage = storage::StorageEngine::new(path)?;
        let parser = parser::SQLParser::new();
        let executor = executor::QueryExecutor::new();
        
        Ok(GSQL {
            storage,
            parser,
            executor,
        })
    }
    
    // Fonction principale pour exécuter les requêtes
    pub fn query(&mut self, sql: &str) -> Result<QueryResult, GSQLError> {
        // 1. Parser la requête
        let ast = self.parser.parse(sql)?;
        
        // 2. Exécuter via l'executor
        let result = self.executor.execute(&ast, &mut self.storage)?;
        
        Ok(result)
    }
    
    // Version asynchrone pour plus tard
    pub async fn query_async(&mut self, sql: &str) -> Result<QueryResult, GSQLError> {
        // À implémenter avec Tokio
        self.query(sql)
    }
}

// Résultat de requête
#[derive(Debug)]
pub enum QueryResult {
    Select { columns: Vec<String>, rows: Vec<Vec<Value>> },
    Insert { rows_affected: u64 },
    Create { table: String },
    Delete { rows_affected: u64 },
    Empty,
}

// Valeurs supportées
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
}
