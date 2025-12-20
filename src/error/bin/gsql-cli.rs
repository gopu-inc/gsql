// src/bin/gsql-cli.rs
use gsql::GSQL;
use rustyline::Editor;
use rustyline::error::ReadlineError;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("GSQL v0.1.0 - Simple & Powerful SQL");
    println!("Type '.help' for help, '.exit' to quit\n");
    
    let mut rl = Editor::<()>::new()?;
    let mut db = GSQL::new(None)?;
    
    loop {
        let readline = rl.readline("gsql> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(&line);
                
                // Commandes spéciales
                if line.starts_with('.') {
                    match line.trim() {
                        ".exit" | ".quit" => break,
                        ".help" => print_help(),
                        ".tables" => list_tables(&db),
                        _ => println!("Unknown command: {}", line),
                    }
                    continue;
                }
                
                // Requête SQL
                if !line.trim().is_empty() {
                    match db.query(&line) {
                        Ok(result) => print_result(result),
                        Err(e) => println!("Error: {}", e),
                    }
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
            },
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    
    Ok(())
}

fn print_result(result: QueryResult) {
    match result {
        QueryResult::Select { columns, rows } => {
            // Afficher en tableau
            println!("+{}+", "-".repeat(columns.len() * 15));
            println!("| {}", columns.join(" | "));
            println!("+{}+", "-".repeat(columns.len() * 15));
            for row in rows {
                let values: Vec<String> = row.iter()
                    .map(|v| format!("{:?}", v))
                    .collect();
                println!("| {}", values.join(" | "));
            }
            println!("+{}+", "-".repeat(columns.len() * 15));
            println!("{} rows", rows.len());
        }
        QueryResult::Insert { rows_affected } => {
            println!("Inserted {} rows", rows_affected);
        }
        QueryResult::Create { table } => {
            println!("Table '{}' created", table);
        }
        _ => println!("OK"),
    }
}
