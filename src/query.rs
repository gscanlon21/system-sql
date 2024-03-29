
use sqlparser::{ast::*, dialect::MsSqlDialect, parser::Parser, test_utils};
use std::{collections::HashMap, ffi::OsString, fmt::{self, Display}, fs::{self, DirEntry}, str::FromStr, result};
use crate::{core::{column::*, file::*, dialect, error::CoreError, expr_result::ExprResult}, enumerable};
use crate::display::*;
use strum::IntoEnumIterator;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/**
 * Parses and evaluates SQL string
 * 
 * The SQL will be executed against the system's files and directories 
**/
pub fn parse_sql(sql: &str, dialect: dialect::CoreDialect) -> Result<(), CoreError> {
    let dialect = dialect.dialect;

    let parse_result = Parser::parse_sql(&*dialect, &sql)?;

    //println!("Raw SQL:\n'{}'", sql);
    //println!("Parse results:\n{:#?}", parse_result);

    for statement in parse_result {
        consume_statement(statement)?;
    }

    Ok(())
}

/**
 * Consumes and executes a SQL statement
**/
pub fn consume_statement(statement: Statement) -> Result<(), CoreError> {
    match statement {
        // SELECT columns FROM table_name ...
        Statement::Query(query) => {
            let query = consume_query(*query)?;
            println!("{:#?}", query);

            Ok(())
        }
        // INSERT INTO name ...
        // table_name is the file being inserted into
        // columns are the columns to insert defined in paranthesis after the table_name (simple idents)
        // source is the query to pull data for the insert from
        Statement::Insert { table_name, columns, source } => {
            println!("Source: {:#?}", source);
            
            let mut files = consume_query(*source)?;

            println!("Columns are {:#?}", columns);
            println!("Files:\n{:#?}", files);

            //let columns: Vec<String> = if columns.len() > 0 {
            //    columns.into_iter().map(|c|
            //        FileColumn::from_str(c.value.as_str()).expect(format!("No such column {} was found", c.value).as_str())
            //    ).collect()
            //} else {
            //    FileColumn::iter().collect()
            //};

            //let column_cmp = |a: &String, b: &String| std::mem::discriminant(a) == std::mem::discriminant(b);
            //files = files.into_iter().map(|row| row.into_iter().filter(|column|
            //    columns.iter().any(|c| column_cmp(c, column))).collect::<Vec<String>>()
            //).collect::<Vec<Vec<String>>>();
            
            //if let Some(table_name) = table_name.0.first() {
            //    match table_name.value.chars().rev().take_while(|c| *c != '.').collect::<String>().as_str() {
                    // ...mmh
            //        "vsc" => { write_csv(columns, files, &table_name.value)? }
            //        "nosj" => { write_json(columns, files, &table_name.value)? }
            //        _ => { println!("Files:\n{:#?}", files); }
            //    }
            //};

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

/**
 * Consumes and executes a SQL query
**/
pub fn consume_query(query: Query) -> Result<Vec<(String, String, String)>, CoreError> {
    match query.body {
        SetExpr::Select(select) => { 
            let files = consume_select(*select)?;

            return Ok(files);
        }
        _ => { unimplemented!() }
    }
}

/**
 * Consumes and executes a SQL select statement
**/
fn consume_select(select: Select) -> Result<Vec<(String, String, String)>, CoreError> {
    let mut hash_map: HashMap</*table name*/ String, /*row*/ Vec<CoreFile>> = HashMap::new();
    let mut result_columns: /*rows*/ Vec</*columns*/ Vec<FileColumn>> = Vec::new();
    let select_projection = select.projection;

    if !select.from.is_empty() {
        for table_with_join in select.from {
            // Load the table's files into memory
            let (table_name, files) = consume_relation(table_with_join.relation)?;

            hash_map.insert(table_name, files.clone());
            //println!("hash map{:#?}", hash_map);

            result_columns = hash_map.clone().into_iter().flat_map(|f| 
                f.1.clone().iter().map(|cf| cf.columns()).collect::<Vec<Vec<FileColumn>>>()
            ).collect::<Vec<Vec<FileColumn>>>();
            //println!("{:#?}", result_columns);

            for join in table_with_join.joins {
                let (join_table_name, join_files) = consume_relation(join.relation.clone())?;
                match join.join_operator {
                    JoinOperator::Inner(constraint) => {
                        match constraint {
                            sqlparser::ast::JoinConstraint::On(expr) => {
                                hash_map.insert(join_table_name, join_files.clone());

                                let expr_result = consume_expr(expr, Some(&mut hash_map))?;

                                //println!("expr_result: {:#?}", expr_result);

                                match expr_result {
                                    ExprResult::BinaryOp((left_table_name, left), op, (right_table_name, right)) => {
                                        let join_result = enumerable::inner_join(files.clone(), join_files, left, right, Box::new(|f: CoreFile, jf: CoreFile| vec![f, jf]));
                                        hash_map.remove(&left_table_name);
                                        hash_map.remove(&right_table_name);

                                        result_columns = join_result.iter().map(|f| 
                                            f.clone().iter().flat_map(|cf| cf.columns()).collect()
                                        ).collect();
                                    }
                                    _ => { unimplemented!() }
                                }
                            }
                            _ => { unimplemented!() }
                        }
                    }
                    JoinOperator::LeftOuter(constraint) => {
                        match constraint {
                            sqlparser::ast::JoinConstraint::On(expr) => {
                                hash_map.insert(join_table_name, join_files.clone());

                                let expr_result = consume_expr(expr, Some(&mut hash_map))?;

                                //println!("expr_result: {:#?}", expr_result);

                                match expr_result {
                                    ExprResult::BinaryOp((left_table_name, left), op, (right_table_name, right)) => {
                                        let join_result = enumerable::left_join(files.clone(), join_files, left, right, Box::new(|f: CoreFile, jf: Option<CoreFile>| vec![Some(f), jf]));
                                        
                                        hash_map.remove(&left_table_name);
                                        hash_map.remove(&right_table_name);

                                        result_columns = join_result.iter().map(|f| 
                                            f.iter().flat_map(|cf|
                                                match cf {
                                                    Some(cf) => { cf.columns() }
                                                    None => { vec![FileColumn::Null] }
                                                } 
                                            ).collect()
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
                todo!()
                //result_columns.sort();
                //result_columns.dedup()
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

            println!("results: {:#?}", result_columns);
            println!("projections: {:#?}", select_projection);

            // This is a very quick and dirty implmentation down for limiting the data set to our selected columns
            // It will fail on joined tables or compound identifiers
            return Ok(select_projection.iter().flat_map(|select_expr|
                result_columns.iter().filter_map(|row|
                    // col = Name(Some("Cargo.lock",),)

                    match_select_item(row.clone(), select_expr)
                ).flatten().collect::<Vec<(String, String, String)>>()
                
                // row = [Name(Some("Cargo.lock",),),Path(Some("./Cargo.lock",),),Type(Some(File,),),FileExtension(Some("lock",),),Size(Some(5500,),),Path(Some("./Cargo.lock",),),Created(Some(SystemTime{intervals: 132534735133613130,},),),],
                //row.into_iter().filter_map(|col| 
                    // col = Name(Some("Cargo.lock",),)
                    //select_projection.iter().any(|p | { match_select_item(col.clone(), p) })
                //).collect::<Vec<String>>()
            ).collect::<Vec<(String, String, String)>>());        
        }
    } else {
        //select_projection.
    }

    for group in select.group_by {
        todo!()
    }

    if let Some(seelction) = select.selection {
        todo!()
    }

    if let Some(having) = select.having {
        todo!()
    }

    return Ok(result_columns.into_iter().flat_map(|row| 
        row.into_iter().map(|items|
            (calculate_hash(&items).to_string(), "".to_owned(), items.to_string())
        ).collect::<Vec<(String, String, String)>>()
    ).collect::<Vec<(String, String, String)>>());
}

fn unbox<T>(value: Box<T>) -> T {
    *value
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn match_select_item(row: Vec<FileColumn> /* The file's column that is checked if it is contained in the SELECT list */, projection: &SelectItem /* One item following SELECT (sa. files.name) */) -> Option<Vec<(String, String, String)>> {
    match projection {
        SelectItem::UnnamedExpr(expr) => { 
            match expr {
                Expr::Identifier(ident) => { 
                    Some(vec![(calculate_hash(&ident).to_string(), "".to_owned(), ident.value.clone())])
                }
                Expr::Wildcard => { 
                    Some(row.into_iter().map(|row| 
                        (calculate_hash(&row).to_string(), "".to_owned(), row.to_string())
                    ).collect::<Vec<(String, String, String)>>())
                }
                Expr::QualifiedWildcard(idents) => { todo!() }
                Expr::CompoundIdentifier(idents) => { todo!() }
                Expr::Case { operand, conditions, results, else_result } => { 
                    //println!("column: {:#?}", column);
                    //println!("projection: {:#?}", projection);
                    //println!("operand: {:#?}", operand);
                    //println!("conditions: {:#?}", conditions);
                    //println!("results: {:#?}", results);
                    //println!("else: {:#?}", else_result);

                    //let expr = unbox(operand.unwrap());
                    //let mut hash_map: HashMap<String, Vec<CoreFile>> = [(table_name.clone(), files)].iter().cloned().collect();
                    //let result = consume_expr(expr.clone(), Some(&mut hash_map))?;

                    Some(vec![(calculate_hash(&projection).to_string(), "".to_owned(), "TODO".to_owned())])
                }
                _ => unimplemented!() 
            }    
        }
        SelectItem::ExprWithAlias { expr, alias } => { todo!() }
        SelectItem::QualifiedWildcard(_) => { todo!() }
        SelectItem::Wildcard => { 
            Some(row.into_iter().map(|row| 
                (calculate_hash(&row).to_string(), "".to_owned(), row.to_string())
            ).collect::<Vec<(String, String, String)>>())
         }
    }
}

/**
 * Consumes and executes a SQL expression
**/
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
                    (ExprResult::Value(left), ExprResult::Value(right)) => {
                        Ok(ExprResult::BinaryOp(
                            ("".to_owned(), Box::new(move |f| FileColumn::Name(Some(OsString::from(left.to_string()))))), 
                            op, 
                            ("".to_owned(), Box::new(move |f| FileColumn::Name(Some(OsString::from(right.to_string())))))
                        ))
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
    let file_column = FileColumn::from_str(ident.value.as_str())?;
    Ok(ExprResult::Select(Box::new(move |c| c.column(&file_column))))
}

/**
 * Consumes and executes a SQL operation
**/
fn consume_op(left: Value, op: &BinaryOperator, right: Value) -> Result<Value, CoreError> {
    //println!("op: {{ left: {:?}, right: {:?} }}", left, right);
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

        assert_eq!(files.expect("Directory is readable").len(), 3);
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


