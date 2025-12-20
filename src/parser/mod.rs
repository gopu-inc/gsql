// src/parser/mod.rs
mod ast;
mod tokenizer;
mod grammar;

use crate::error::GSQLError;
use ast::*;

pub struct SQLParser;

impl SQLParser {
    pub fn new() -> Self {
        SQLParser
    }
    
    pub fn parse(&self, sql: &str) -> Result<Statement, GSQLError> {
        // 1. Tokenization
        let tokens = tokenizer::tokenize(sql)?;
        
        // 2. Parsing en AST
        grammar::parse(tokens)
    }
}

// src/parser/ast.rs
#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
    Delete(DeleteStmt),
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Int,
    BigInt,
    Float,
    Double,
    Text,
    Boolean,
    Varchar(usize),
}

#[derive(Debug, Clone)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<SelectItem>,
    pub from: TableRef,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expression>>,
}
