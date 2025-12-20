// src/storage/mod.rs
mod page;
mod buffer;
mod wal;
mod btree;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write, Read};
use crate::error::GSQLError;

pub struct StorageEngine {
    file: File,
    tables: HashMap<String, TableMetadata>,
    page_cache: buffer::BufferPool,
    wal: wal::WriteAheadLog,
}

impl StorageEngine {
    pub fn new(path: Option<&str>) -> Result<Self, GSQLError> {
        let path = path.unwrap_or("gsql.db");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
            
        Ok(StorageEngine {
            file,
            tables: HashMap::new(),
            page_cache: buffer::BufferPool::new(100), // 100 pages en cache
            wal: wal::WriteAheadLog::new("gsql.wal")?,
        })
    }
    
    pub fn create_table(&mut self, stmt: &CreateTableStmt) -> Result<(), GSQLError> {
        // 1. Créer les métadonnées
        let metadata = TableMetadata::from(stmt);
        
        // 2. Créer le fichier de données
        let table_file = format!("{}.tbl", stmt.table_name);
        File::create(&table_file)?;
        
        // 3. Créer l'index primaire (B+Tree)
        let index = btree::BTree::new(&format!("{}.idx", stmt.table_name))?;
        
        // 4. Stocker en mémoire
        self.tables.insert(stmt.table_name.clone(), metadata);
        
        Ok(())
    }
    
    pub fn insert(&mut self, stmt: &InsertStmt) -> Result<u64, GSQLError> {
        let mut rows_affected = 0;
        
        for row_values in &stmt.values {
            // 1. Écrire dans le WAL (Write-Ahead Log)
            self.wal.log_insert(&stmt.table_name, row_values)?;
            
            // 2. Serialiser la ligne
            let row_data = self.serialize_row(&stmt.table_name, row_values)?;
            
            // 3. Écrire dans le fichier de table
            let page_id = self.allocate_page()?;
            self.write_page(page_id, &row_data)?;
            
            // 4. Mettre à jour l'index
            if let Some(primary_key) = self.get_primary_key(&stmt.table_name) {
                let key = &row_values[primary_key.index];
                self.update_index(&stmt.table_name, key, page_id)?;
            }
            
            rows_affected += 1;
        }
        
        // 5. Flush le WAL
        self.wal.flush()?;
        
        Ok(rows_affected)
    }
    
    pub fn select(&mut self, stmt: &SelectStmt) -> Result<Vec<Row>, GSQLError> {
        let mut results = Vec::new();
        
        // Pour MVP: scan séquentiel simple
        // Plus tard: utiliser les indexes
        let table_name = match &stmt.from {
            TableRef::Table(name) => name,
            _ => return Err(GSQLError::NotImplemented("Joins")),
        };
        
        if let Some(metadata) = self.tables.get(table_name) {
            // Lire toutes les pages de la table
            for page_id in 0..metadata.page_count {
                if let Ok(page_data) = self.read_page(page_id) {
                    let rows = self.deserialize_page(&page_data, metadata)?;
                    
                    // Filtrer avec WHERE clause
                    for row in rows {
                        if self.evaluate_where(&row, &stmt.where_clause)? {
                            results.push(row);
                        }
                    }
                }
            }
        }
        
        Ok(results)
    }
}

// src/storage/page.rs
pub const PAGE_SIZE: usize = 8192; // 8KB par page

#[derive(Debug)]
pub struct Page {
    pub id: u64,
    pub data: [u8; PAGE_SIZE],
    pub dirty: bool,
}

impl Page {
    pub fn new(id: u64) -> Self {
        Page {
            id,
            data: [0; PAGE_SIZE],
            dirty: false,
        }
    }
}
