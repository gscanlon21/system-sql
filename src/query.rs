
use sqlparser::{ast::*, dialect::MsSqlDialect, parser::Parser, test_utils};
use std::fs::{self, DirEntry};
use crate::core::*;
use crate::display::*;

pub fn parse_sql(sql: &str) -> Result<(), CoreError> {
    let dialect = MsSqlDialect {}; // Supports [bracketed] table names which makes it easier to read from directories: "SELECT * FROM [./]"

    let parse_result = Parser::parse_sql(&dialect, &sql);

    match parse_result {
        Ok(statements) => {
            println!("Raw SQL:\n'{}'", sql);

            println!("Parse results:\n{:#?}", statements);

            for statement in statements {
                consume_statement(statement)?;
            }

            Ok(())
        }
        Err(e) => {
            println!("Error during parsing: {:?}", e);

            Err(CoreError::from(e))
        }
    }
}

pub fn consume_statement(statement: Statement) -> Result<(), CoreError> {
    return match statement {
        Statement::Query(query) => {
            consume_query(*query)?;

            return  Ok(());
        }
        Statement::Insert { table_name, columns, source } => {
            let files = consume_query(*source)?;

            if let Some(table_name) = table_name.0.first() {
                match table_name.value.chars().rev().take_while(|c| *c != '.').collect::<String>().as_str() {
                    "vsc" => { write_csv(files, &table_name.value) }
                    "nosj" => { write_json(files, &table_name.value) }
                    _ => { println!("Files:\n{:#?}", files) }
                }
            };

            return Ok(());
        }
        Statement::Copy { table_name, columns, values } => { Err(CoreError::from("Unimplemented")) }
        Statement::Update { table_name, assignments, selection } => { Err(CoreError::from("Unimplemented")) }
        Statement::Delete { table_name, selection } => { Err(CoreError::from("Unimplemented")) }
        Statement::Drop { object_type, if_exists, names, cascade } => { Err(CoreError::from("Unimplemented")) }
        Statement::SetVariable { local, variable, value } => { Err(CoreError::from("Unimplemented")) }
        Statement::ShowVariable { variable } => { Err(CoreError::from("Unimplemented")) }
        Statement::ShowColumns { extended, full, table_name, filter } => { 
            println!("Columns: {}", Column::iterator().map(|c| format!(", {}", c)).collect::<String>());

            Ok(())
        }
        Statement::StartTransaction { modes } => { Err(CoreError::from("Unimplemented")) }
        Statement::SetTransaction { modes } => { Err(CoreError::from("Unimplemented")) }
        Statement::Commit { chain } => { Err(CoreError::from("Unimplemented")) }
        Statement::Rollback { chain } => { Err(CoreError::from("Unimplemented")) }
        Statement::Assert { condition, message } => { Err(CoreError::from("Unimplemented")) }
        _ => { Err(CoreError::from("Not implemented")) }
    }
}


pub fn consume_query(query: Query) -> Result<Vec<CoreFile>, CoreError> {
    match query.body {
        SetExpr::Select(select) => { 
            let files = consume_select(*select)?;
            
            println!("\nOk! Here are the results:");

            for file in &files {
                println!("File: {:#?}", file);
            }

            return Ok(files);
        }
        SetExpr::Query(_) => { Err(CoreError::from("Unimplemented")) }
        SetExpr::SetOperation { op, all, left, right } => { Err(CoreError::from("Unimplemented")) }
        SetExpr::Values(_) => { Err(CoreError::from("Unimplemented")) }
    }
}

pub fn consume_select(select: Select) -> Result<Vec<CoreFile>, CoreError> {
    for table_with_join in select.from {
        let mut files = consume_relation(table_with_join.relation)?;

        for join in table_with_join.joins {
            let join_files = consume_relation(join.relation)?;
            match join.join_operator {
                JoinOperator::Inner(constraint) => {
                    match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => {}
                        sqlparser::ast::JoinConstraint::Using(_) => {}
                        sqlparser::ast::JoinConstraint::Natural => {}
                    }
                }
                JoinOperator::LeftOuter(_) => {}
                JoinOperator::RightOuter(_) => {}
                JoinOperator::FullOuter(_) => {}
                JoinOperator::CrossJoin => {}
                JoinOperator::CrossApply => {}
                JoinOperator::OuterApply => {}
            }
        }

        if select.distinct {
            files.sort();
            files.dedup()
        }

        if let Some(ref top) = select.top  {
            if let Expr::Value(ref value) = top.quantity.as_ref().unwrap() {
                let quantity: Option<usize> = match value {
                    Value::Number(str) => { str.parse().ok() }
                    Value::SingleQuotedString(str) => { str.parse().ok() }
                    Value::NationalStringLiteral(str) => { str.parse().ok() }
                    Value::HexStringLiteral(str) => { str.parse().ok() }
                    Value::Boolean(b) => { Some(*b as usize) }
                    Value::Interval { value, leading_field, leading_precision, last_field, fractional_seconds_precision } => { None }
                    Value::Null => { None }
                };

                files = if let Some(quantity) = quantity {
                    files.into_iter().take(quantity).collect()
                } else {
                    files
                }
            }
        }

        return Ok(files)
    }


    for group in select.group_by {

    }

    if let Some(seelction) = select.selection {

    }

    for projection in select.projection {

    }

    if let Some(having) = select.having {

    }

    return Err(CoreError::from("There's nothing here!"));
}

fn consume_relation(relation: TableFactor) -> Result<Vec<CoreFile>, CoreError> {
    let mut files: Vec<CoreFile> = Vec::new();
    match relation {
        TableFactor::Table { name, alias, args, with_hints } => {
            if let Some(table_name) = name.0.first() {
                let paths = fs::read_dir(table_name.value.clone())?;
                for path in paths {
                    if let Ok(path) = path {
                        // let metadata = fs::metadata(&path)?;
                        files.push(CoreFile::from(path));
                    }                        
                }
            }
        }
        TableFactor::Derived { lateral, subquery, alias } => {}
        TableFactor::NestedJoin(_) => {}
    }

    return Ok(files);
}


#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn debup_files() {
    //     let temp1 = fs::File::create("./temp/temp1").unwrap();
    //     let temp2 = fs::File::create("./temp/temp2").unwrap();

    //     let mut dir = fs::read_dir("./temp/")
    //                 .expect("")
    //                 .into_iter()
    //                 .map(|f| 
    //                     f.unwrap()
    //                 );
    //     let first = dir.nth(0).unwrap();
    //     let second = dir.nth(0).unwrap();
    //     let mut files = vec![
    //         first,
    //         second,
    //     ];
    //     files.push(files[1]);

    //     dedup_files(&mut files);
    // }
    
    #[test]
    fn consume_query() {
        // let ast = parse_sql("SELECT * FROM [./]");

        // let query = Query { 
        //     body: SetExpr::Query(Box::new(Query { 
        //         ctes: Vec::new(), 
        //         order_by: Vec::new(),
        //         limit: None,
        //         offset: None,
        //         fetch: None,
        //         body: SetExpr::Select(Box::new(Select { 
        //             distinct: false,
        //             group_by: Vec::new(),
        //             selection: None,
        //             having: None,
        //             projection: Vec::new(),
        //             from: Vec::new(),
        //             top: None
        //         })) 
        //     })), 
        //     ctes: Vec::new(), 
        //     order_by: Vec::new(), 
        //     limit: None, 
        //     offset: None, 
        //     fetch: None 
        // };
        // File::create(&outputPath).expect("Failure writing temp file");
        // let files = fs::read_dir(env::current_dir().expect("Failure reading current directory"))
        //                         .expect("Failure reading current directory")
        //                         .into_iter().map(|f|
        //                             f.unwrap()
        //                         ).collect();
        
        // super::write_csv(files, &outputPath.to_string());

        // let csv = fs::metadata(outputPath).expect("Output file should exist");
        // assert_eq!(csv.len() > 8, true); 
    }
}


