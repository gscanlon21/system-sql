
use sqlparser::{ast::*, dialect::MsSqlDialect, parser::Parser, test_utils};
use std::{collections::HashMap, fmt::{self, Display}, fs::{self, DirEntry}, str::FromStr};
use crate::core::{column::Column, column::FileColumn, dialect::CoreDialect, error::CoreError, file::ColumnDisplay, expr_result::ExprResult, file::{ColumnValue, CoreFile}};
use crate::display::*;
use strum::IntoEnumIterator;

pub fn parse_sql(sql: &str, dialect: CoreDialect) -> Result<(), CoreError> {
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
        Statement::Query(query) => {
            consume_query(*query)?;

            Ok(())
        }
        Statement::Insert { table_name, columns, source } => {
            let files = consume_query(*source)?;

            let columns = columns.into_iter().filter_map(|c|
                Column::from_str(c.value.as_str()).ok()
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
        Statement::Update { table_name, assignments, selection } => { 
            let table_name = &table_name.0[0].value;
            let files = consume_table_name(table_name).unwrap();

            if let Some(expr) = selection {
                let mut hash_map: HashMap<String, Vec<CoreFile>> = [(table_name.clone(), files)].iter().cloned().collect();
                let result = consume_expr(expr.clone(), Some(&mut hash_map));
            }

            for assignment in &assignments {
                
            }

            Ok(())
        }
        Statement::ShowColumns { extended, full, table_name, filter } => { 
            let columns = Column::iterator().map(|c| c.to_string()).collect::<Vec<String>>();
            println!("Columns: {}", columns.join(", "));

            Ok(())
        }
        _ => { unimplemented!() }
    }
}


pub fn consume_query(query: Query) -> Result<Vec<Vec<CoreFile>>, CoreError> {
    match query.body {
        SetExpr::Select(select) => { 
            let files = consume_select(*select)?;

            return Ok(files);
        }
        _ => { unimplemented!() }
    }
}

fn inner_join<TLeft, TRight, TKey, TResult>(left: Vec<TLeft>, right: Vec<TRight>, left_key_selector: Box<dyn Fn(&TLeft) -> TKey>, right_key_selector: Box<dyn Fn(&TRight) -> TKey>, result_selector: Box<dyn Fn(TRight, TLeft) -> TResult>) -> Vec<TResult> 
    where TKey: std::cmp::Eq + std::hash::Hash + std::fmt::Debug, TRight: Clone + std::fmt::Debug, TLeft: Clone, TResult: std::fmt::Debug {
    let mut right_iter = right.iter();

    let mut results = Vec::new();
    let mut lookup: HashMap<TKey, Vec<TLeft>> = HashMap::new();
    for item in left
    {
        let key = left_key_selector(&item);
        let values = lookup.entry(key).or_insert(Vec::new());
        values.push(item);
    }
    while let Some(right_item) = right_iter.next() {
        let g: Option<&Vec<TLeft>> = lookup.get(&right_key_selector(&right_item));
        if let Some(g) = g {
            for i in g
            {
                results.push(result_selector(right_item.clone(), i.clone()));
            }
        }
    }

    results
}

fn consume_select(select: Select) -> Result<Vec<Vec<CoreFile>>, CoreError> {
    for table_with_join in select.from {
        let (table_name, files) = consume_relation(table_with_join.relation)?;
        let mut hash_map: HashMap<String, Vec<CoreFile>> = [(table_name, files.clone())].iter().cloned().collect();
        let mut result_files: Vec<Vec<CoreFile>> = hash_map.iter().map(|f| f.1.clone()).collect();

        for join in table_with_join.joins {
            let (join_table_name, join_files) = consume_relation(join.relation.clone())?;
            match join.join_operator {
                JoinOperator::Inner(constraint) => {
                    match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => {
                            hash_map.insert(join_table_name, join_files.clone());

                            let expr_result = consume_expr(expr, Some(&mut hash_map))?;
                            println!("expr_result: {:#?}", expr_result);

                            match expr_result {
                                ExprResult::BinaryOp((left_table_name, left), op, (right_table_name, right)) => {
                                    let join_result = inner_join(files.clone(), join_files, left, right, Box::new(|jf: CoreFile, f: CoreFile| vec![f, jf]));
                                    hash_map.remove(&left_table_name);
                                    hash_map.remove(&right_table_name);

                                    result_files = join_result;
                                    //*hash_map.entry(right_table_name).or_default() = join_result.iter().map(|f| f.1.clone()).collect();
                                }
                                _ => { unimplemented!() }
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        if select.distinct {
            result_files.sort();
            result_files.dedup()
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

                result_files = if let Some(quantity) = quantity {
                    result_files.into_iter().take(quantity).collect()
                } else {
                    result_files
                }
            }
        }

        for projection in select.projection {
            //consume_select_item(projction);
        }

        return Ok(result_files)
    }


    for group in select.group_by {

    }

    if let Some(seelction) = select.selection {

    }

    if let Some(having) = select.having {

    }

    return Err(CoreError::from("There's nothing here!"));
}

fn consume_select_item(select_item: SelectItem) {
    match select_item {
        SelectItem::UnnamedExpr(expr) => {
            consume_expr(expr, None);
        }
        SelectItem::ExprWithAlias { expr, alias } => {}
        SelectItem::QualifiedWildcard(_) => {}
        SelectItem::Wildcard => {}
    }
}

fn consume_expr(expr: Expr, tables: Option<&mut HashMap<String, Vec<CoreFile>>>) -> Result<ExprResult, CoreError> {
    match expr {
        Expr::Identifier(ident) => { consume_expr_ident(ident) }
        Expr::CompoundIdentifier(mut idents) => { 
            let column = idents.pop().unwrap();
            let column_select = match consume_expr_ident(column) {
                Ok(ExprResult::Select(select)) => { Ok(select) }
                _ => { Err(()) }
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

            // if let Some(tables) = tables {
            //     match (left_result, right_result) {
            //         (ExprResult::CompoundSelect(left_table, left), ExprResult::CompoundSelect(right_table, right)) => {
            //             let left_files = tables.iter().filter_map(|t| if t.0.to_owned() == left_table { Some(t.1) } else { None } ).collect();
            //             let right_files = tables.iter().filter_map(|t| if t.0.to_owned() == right_table { Some(t.1) } else { None } ).collect();
            //         }
            //         (ExprResult::CompoundSelect(_, _), ExprResult::Select(_)) => { unimplemented!() }
            //         (ExprResult::CompoundSelect(_, _), ExprResult::Value(_)) => { unimplemented!() }
            //         (ExprResult::Select(_), ExprResult::CompoundSelect(_, _)) => { unimplemented!() }
            //         (ExprResult::Select(left), ExprResult::Select(right)) => {
            //             tables.iter().flat_map(|a| a.1).filter_map(|f| match consume_op(left(f), &op, right(f)) { // left and right should return an option if the column does not exist on the table
            //                 Ok(Value::Boolean(b)) => { 
            //                     if b {
            //                         Some(f)
            //                     } else {
            //                         None
            //                     }
            //                 }
            //                 _ => { panic!() }
            //             });
            //         }
            //         (ExprResult::Select(_), ExprResult::Value(_)) => { unimplemented!() }
            //         (ExprResult::Value(_), ExprResult::CompoundSelect(_, _)) => { unimplemented!() }
            //         (ExprResult::Value(_), ExprResult::Select(_)) => { unimplemented!() }
            //         (ExprResult::Value(left), ExprResult::Value(right)) => {
            //             match consume_op(left, &op, right) {
            //                 Ok(Value::Boolean(b)) => { 
            //                     if b == false { *tables = HashMap::new() }
            //                  }
            //                 _ => { panic!() }
            //             }
            //         }
            //         _ => { panic!() }
            //     }
            // }

           
            //     let mut results: HashMap<String, Vec<CoreFile>> = HashMap::new();

            //     let mut table_iter = tables.iter();
            //     while let Some((table_name, files)) = table_iter.next() {
            //         let mut iter = files.iter();
            //         while let Some(file) = iter.next() {
            //             let left = match &left_result {
            //                 ExprResult::Select(select) => {
            //                     Some(select(file))
            //                 }
            //                 ExprResult::Value(literal) => { Some(literal.clone()) }
            //                 _ => { None }
            //             }.unwrap();
            
            //             let right = match &right_result {
            //                 ExprResult::Select(select) => {
            //                     Some(select(file))
            //                 }
            //                 ExprResult::Value(literal) => { Some(literal.clone()) }
            //                 _ => { None }
            //             }.unwrap();    
        
            //             println!("binary_op: {{ left: {:?}, right: {:?} }}", left, right);
        
            //             match consume_op(left, &op, right) {
            //                 Ok(Value::Boolean(b)) => { 
            //                     if b {
            //                         results.entry(table_name.clone()).or_insert(Vec::new()).push(file.clone());
            //                     }
            //                  }
            //                 _ => { panic!() }
            //             }
            //         }
            //     }
            //     tables = results;
        }
        Expr::Value(value) => { Ok(ExprResult::Value(value)) }
        _ => { unimplemented!( )}
    }
}

fn consume_expr_ident(ident: Ident) -> Result<ExprResult, CoreError> {    
    match Column::from_str(ident.value.as_str()) {
        Ok(column) => { Ok(ExprResult::Select(Box::new(move |c| Value::SingleQuotedString(c.value(&column))))) }
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
                return Ok((table_name.value.clone(), consume_table_name(table_name.value.as_str())?));
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


