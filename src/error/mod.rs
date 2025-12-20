// src/error/mod.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GSQLError {
    #[error("Syntax error: {0}")]
    SyntaxError(String),
    
    #[error("Table {0} already exists")]
    TableExists(String),
    
    #[error("Table {0} not found")]
    TableNotFound(String),
    
    #[error("Column {0} not found")]
    ColumnNotFound(String),
    
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Not implemented: {0}")]
    NotImplemented(String),
    
    #[error("Unknown error: {0}")]
    Other(String),
}
