
use sqlparser::{ast::*, dialect::MsSqlDialect, parser::Parser, test_utils};
use std::{collections::HashMap, fmt::{self, Display}, fs::{self, DirEntry}, str::FromStr};
use crate::{core::{column::*, file::*, dialect, error::CoreError, expr_result::ExprResult}, enumerable};
use crate::display::*;
use strum::IntoEnumIterator;

pub fn parse_sql(sql: &str, dialect: dialect::CoreDialect) -> Result<(), CoreError> {
    let dialect = dialect.dialect; // Supports [bracketed] table names which makes it easier to read from directories: "SELECT * FROM [./]"

    let parse_result = Parser::parse_sql(&*dialect, &sql);

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
    match statement {
        // SELECT columns FROM table_name ...
        Statement::Query(query) => {
            let query = consume_query(*query)?;
            println!("{:#?}", query);

            Ok(())
        }
        // INSERT INTO name ...
        Statement::Insert { table_name, columns, source } => {
            let files = consume_query(*source)?;

            let columns = columns.into_iter().filter_map(|c|
                FileColumn::from_str(c.value.as_str()).ok()
            ).collect();

            if let Some(table_name) = table_name.0.first() {
                match table_name.value.chars().rev().take_while(|c| *c != '.').collect::<String>().as_str() {
                    "vsc" => { write_csv(columns, files, &table_name.value) }
                    "nosj" => { write_json(columns, files, &table_name.value) }
                    _ => { println!("Files:\n{:#?}", files) }
                }
            };

            Ok(())
        }
        // UPDATE table_name SET column = value ...
        Statement::Update { table_name, assignments, selection } => { 
            let table_name = &table_name.0[0].value;
            let files = consume_table_name(table_name)?;

            if let Some(expr) = selection {
                let mut hash_map: HashMap<String, Vec<CoreFile>> = [(table_name.clone(), files)].iter().cloned().collect();
                let result = consume_expr(expr.clone(), Some(&mut hash_map))?;
            }

            for assignment in &assignments {
                
            }

            Ok(())
        }
        // SHOW COLUMNS FROM table_name
        Statement::ShowColumns { extended, full, table_name, filter } => { 
            let columns = FileColumn::iterator().map(|c| c.to_string()).collect::<Vec<String>>();
            println!("Columns: {}", columns.join(", "));

            Ok(())
        }
        _ => { unimplemented!() }
    }
}


pub fn consume_query(query: Query) -> Result<Vec<Vec<FileColumn>>, CoreError> {
    match query.body {
        SetExpr::Select(select) => { 
            let files = consume_select(*select)?;

            return Ok(files);
        }
        _ => { unimplemented!() }
    }
}

fn consume_select(select: Select) -> Result<Vec<Vec<FileColumn>>, CoreError> {
    for table_with_join in select.from {
        let (table_name, files) = consume_relation(table_with_join.relation)?;
        let mut hash_map: HashMap<String, Vec<CoreFile>> = [(table_name, files.clone())].iter().cloned().collect();
        let mut result_columns: Vec<Vec<FileColumn>> = hash_map.iter().map(|f| 
            f.1.clone().iter().map(|cf| cf.name()).collect()
        ).collect();

        for join in table_with_join.joins {
            let (join_table_name, join_files) = consume_relation(join.relation.clone())?;
            match join.join_operator {
                JoinOperator::Inner(constraint) => {
                    match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => {
                            hash_map.insert(join_table_name, join_files.clone());

                            let expr_result = consume_expr(expr, Some(&mut hash_map))?;
                            /*
                            BinaryOp {
                                left: CompoundIdentifier(
                                    [
                                        Ident { value: "Files2", quote_style: None, },
                                        Ident { value: "Type", quote_style: None, },
                                    ],
                                ),
                                op: Eq,
                                right: CompoundIdentifier(
                                    [
                                        Ident { value: "Files", quote_style: None, },
                                        Ident { value: "Type", quote_style: None, },
                                    ],
                                ),
                            },
                            */
                            println!("expr_result: {:#?}", expr_result);

                            match expr_result {
                                ExprResult::BinaryOp((left_table_name, left), op, (right_table_name, right)) => {
                                    let join_result = enumerable::inner_join(files.clone(), join_files, left, right, Box::new(|f: CoreFile, jf: CoreFile| vec![f, jf]));
                                    hash_map.remove(&left_table_name);
                                    hash_map.remove(&right_table_name);

                                    result_columns = join_result.iter().map(|f| 
                                        f.clone().iter().map(|cf| cf.name()).collect()
                                    ).collect();
                                }
                                _ => { unimplemented!() }
                            }
                        }
                        _ => { unimplemented!() }
                    }
                }
                _ => { unimplemented!() }
            }
        }

        if select.distinct {
            //result_columns.sort();
            result_columns.dedup()
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

                result_columns = if let Some(quantity) = quantity {
                    result_columns.into_iter().take(quantity).collect()
                } else {
                    result_columns
                }
            }
        }

        for projection in select.projection {
            //consume_select_item(projction);
        }

        return Ok(result_columns)
    }


    for group in select.group_by {

    }

    if let Some(seelction) = select.selection {

    }

    if let Some(having) = select.having {

    }

    return Err(CoreError::from("There's nothing here!"));
}

fn consume_expr(expr: Expr, tables: Option<&mut HashMap<String, Vec<CoreFile>>>) -> Result<ExprResult, CoreError> {
    match expr {
        Expr::Identifier(ident) => { consume_expr_ident(ident) }
        Expr::CompoundIdentifier(mut idents) => { 
            let column = idents.pop().unwrap();
            let column_select = match consume_expr_ident(column) {
                Ok(ExprResult::Select(select)) => { Ok(select) }
                _ => { Err("No match arm provided in column_select") }
            }?;

            let table = idents.pop().unwrap();

            Ok(ExprResult::CompoundSelect(table.value, column_select))
        }
        Expr::BinaryOp { left, op, right } => { 
            let left_result = consume_expr(*left, None)?;
            let right_result = consume_expr(*right, None)?;

            if let Some(tables) = tables {
                match (left_result, right_result) {
                    (ExprResult::Select(left), ExprResult::Select(right)) => {
                        Ok(ExprResult::BinaryOp(("".to_owned(), left), op, ("".to_owned(), right)))
                    }
                    (ExprResult::CompoundSelect(left_table_name, left), ExprResult::CompoundSelect(right_table_name, right)) => {
                        Ok(ExprResult::BinaryOp((left_table_name, left), op, (right_table_name, right)))
                    }
                    _ => { unimplemented!() }
                }
            } else {
                panic!()
            }
        }
        Expr::Value(value) => { Ok(ExprResult::Value(value)) }
        _ => { unimplemented!( )}
    }
}

fn consume_expr_ident(ident: Ident) -> Result<ExprResult, CoreError> {    
    match FileColumn::from_str(ident.value.as_str()) {
        Ok(column) => { Ok(ExprResult::Select(Box::new(move |c| c.column(&column)))) }
        Err(e) => { Err(CoreError::from(e)) }
    }
}

fn consume_op(left: Value, op: &BinaryOperator, right: Value) -> Result<Value, CoreError> {
    println!("op: {{ left: {:?}, right: {:?} }}", left, right);
    match op {
        BinaryOperator::Plus => { 
            match (left, right) {
                (Value::Number(a), Value::Number(b)) => { Ok(Value::Number((a.parse::<i32>().unwrap() + b.parse::<i32>().unwrap()).to_string())) }
                (Value::Number(a), Value::Null) => { Ok(Value::Number(a)) }
                (Value::SingleQuotedString(mut a), Value::SingleQuotedString(b)) => { a.push_str(b.as_str()); Ok(Value::SingleQuotedString(a)) }
                (Value::SingleQuotedString(a), Value::Null) => { Ok(Value::SingleQuotedString(a)) }
                (Value::Boolean(a), Value::Null) => { Ok(Value::Boolean(a)) }
                (Value::Null, Value::Number(b)) => { Ok(Value::Number(b)) }
                (Value::Null, Value::SingleQuotedString(b)) => { Ok(Value::SingleQuotedString(b)) }
                (Value::Null, Value::Boolean(b)) => { Ok(Value::Boolean(b)) }
                (Value::Null, Value::Null) => { Ok(Value::Null) }
                _ => { Err(CoreError::GeneralError("The data types are invalid for the specified operator".to_owned())) }
            }
        }
        BinaryOperator::Minus => {
            match (left, right) {
                (Value::Number(a), Value::Number(b)) => { Ok(Value::Number((a.parse::<i32>().unwrap() - b.parse::<i32>().unwrap()).to_string())) }
                (Value::Number(a), Value::Null) => { Ok(Value::Number(a)) }
                (Value::SingleQuotedString(a), Value::Null) => { Ok(Value::SingleQuotedString(a)) }
                (Value::Boolean(a), Value::Null) => { Ok(Value::Boolean(a)) }
                (Value::Null, Value::Number(b)) => { Ok(Value::Number(b)) }
                (Value::Null, Value::SingleQuotedString(b)) => { Ok(Value::SingleQuotedString(b)) }
                (Value::Null, Value::Boolean(b)) => { Ok(Value::Boolean(b)) }
                (Value::Null, Value::Null) => { Ok(Value::Null) }
                _ => { Err(CoreError::GeneralError("The data types are invalid for the specified operator".to_owned())) }
            }
        }
        BinaryOperator::Eq => { Ok(Value::Boolean(left == right)) }
        _ => { unimplemented!() }
    }
}

fn consume_relation(relation: TableFactor) -> Result<(String, Vec<CoreFile>), CoreError> {
    match relation {
        TableFactor::Table { name, alias, args, with_hints } => {
            if let Some(table_name) = name.0.first() {
                let table_alias = if alias.is_some() { alias.unwrap().name.value } else { table_name.value.clone() };
                return Ok((table_alias, consume_table_name(table_name.value.as_str())?));
            }
            panic!()
        }
        _ => { unimplemented!() }
    }
}

fn consume_table_name(table_name: &str) -> Result<Vec<CoreFile>, CoreError> {
    let mut files: Vec<CoreFile> = Vec::new();
    let paths = fs::read_dir(table_name)?;
    for path in paths {
        if let Ok(path) = path {
            files.push(CoreFile::from(path));
        }                        
    }

    Ok(files)
}


#[cfg(test)]
mod tests {
    use super::*;

    const PATH_TO_TEST_DIR: &str = "./test/";

    #[test]
    fn consume_op_add_number_number() {
        let left_val = Value::Number("2".to_owned());
        let right_val = Value::Number("2".to_owned());
        let result = consume_op(left_val, &BinaryOperator::Plus, right_val);

        assert_eq!(match result {
            Ok(Value::Number(string)) => { string }
            _ => { panic!("Incorrect enum variant expected") }
        }, "4".to_owned());
    }

    #[test]
    fn consume_op_add_string_string() {
        let left_val = Value::SingleQuotedString("2".to_owned());
        let right_val = Value::SingleQuotedString("2".to_owned());
        let result = consume_op(left_val, &BinaryOperator::Plus, right_val);

        assert_eq!(match result {
            Ok(Value::SingleQuotedString(string)) => { string }
            _ => { panic!("Incorrect enum variant expected") }
        }, "22".to_owned());
    }

    #[test]
    fn consume_op_subtract_string_string() {
        let left_val = Value::SingleQuotedString("2".to_owned());
        let right_val = Value::SingleQuotedString("2".to_owned());
        let result = consume_op(left_val, &BinaryOperator::Minus, right_val);

        assert!(match result {
            Err(CoreError::GeneralError(_)) => { true }
            _ => { panic!("Incorrect enum variant expected") }
        });
    }

    #[test]
    fn consume_op_subtract_number_number() {
        let left_val = Value::Number("-2".to_owned());
        let right_val = Value::Number("-2".to_owned());
        let result = consume_op(left_val, &BinaryOperator::Minus, right_val);

        assert_eq!(match result {
            Ok(Value::Number(string)) => { string }
            _ => { panic!("Incorrect enum variant expected") }
        }, "0".to_owned());
    }

    #[test]
    fn consume_table_name() {
        let files = super::consume_table_name(PATH_TO_TEST_DIR);

        assert_eq!(files.expect("Directory is readable").len(), 1);
    }

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


