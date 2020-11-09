
use sqlparser::{ast::*, dialect::MsSqlDialect, parser::Parser, test_utils};
use std::{fmt::{self, Display}, fs::{self, DirEntry}, str::FromStr};
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
    return match statement {
        Statement::Query(query) => {
            consume_query(*query)?;

            return  Ok(());
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

            return Ok(());
        }
        Statement::Copy { table_name, columns, values } => { unimplemented!() }
        Statement::Update { table_name, assignments, selection } => { 
            let files = consume_table_name(&table_name.0[0].value);

            if let Some(expr) = selection {
                for file in &files.unwrap() {
                    println!("{:#?}", expr.clone());
                    let result = consume_expr(expr.clone(), Some(file));
                    println!("{:#?}", result);
                }
            }

            for assignment in &assignments {
                
            }

            Ok(())
        }
        Statement::Delete { table_name, selection } => { unimplemented!() }
        Statement::Drop { object_type, if_exists, names, cascade } => { unimplemented!() }
        Statement::SetVariable { local, variable, value } => { unimplemented!() }
        Statement::ShowVariable { variable } => { unimplemented!() }
        Statement::ShowColumns { extended, full, table_name, filter } => { 
            let columns = Column::iterator().map(|c| c.to_string()).collect::<Vec<String>>();
            println!("Columns: {}", columns.join(", "));

            Ok(())
        }
        Statement::StartTransaction { modes } => { unimplemented!() }
        Statement::SetTransaction { modes } => { unimplemented!() }
        Statement::Commit { chain } => { unimplemented!() }
        Statement::Rollback { chain } => { unimplemented!() }
        Statement::Assert { condition, message } => { unimplemented!() }
        _ => { unimplemented!() }
    }
}


pub fn consume_query(query: Query) -> Result<Vec<CoreFile>, CoreError> {
    match query.body {
        SetExpr::Select(select) => { 
            let files = consume_select(*select)?;
            
            for file in &files {
                println!("{}", file);
            }

            return Ok(files);
        }
        SetExpr::Query(_) => { unimplemented!() }
        SetExpr::SetOperation { op, all, left, right } => { unimplemented!() }
        SetExpr::Values(_) => { unimplemented!() }
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
                        sqlparser::ast::JoinConstraint::On(expr) => {
                            for file in &join_files {
                                let expr_result = consume_expr(expr, Some(file))?;
                                match expr_result {
                                    ExprResult::Select(_) => {}
                                    ExprResult::Assignment(_) => {}
                                    ExprResult::Value(_) => {}
                                    ExprResult::Filter(bool) => {
                                        if bool {
                                            files.push(file)
                                        }
                                    }
                                    ExprResult::Expr(_) => {}
                                }
                            }
                        }
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

        for projection in select.projection {
            //consume_select_item(projction);
        }

        return Ok(files)
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

fn consume_expr(expr: Expr) -> Result<ExprResult, CoreError> {
    match expr {
        Expr::Identifier(ident) => { consume_expr_ident(ident) }
        Expr::CompoundIdentifier(mut idents) => { 
            let column = idents.pop().unwrap();
            let table = idents.pop().unwrap();
            
            //Ok(ExprResult::Select())

            unimplemented!()
        }
        Expr::BinaryOp { left, op, right } =>{ 
            let left = match consume_expr(*left) {
                Ok(expr) => {
                    match expr {
                        ExprResult::Select(select) => {
                            Some(select(core_file.unwrap()))
                        }
                        ExprResult::Value(literal) => { Some(literal) }
                        _ => { None }
                    }
                }
                Err(_) => { None }
            }.unwrap();

            let right = match consume_expr(*right) {
                Ok(expr) => {
                    match expr {
                        ExprResult::Select(select) => {
                            Some(select(core_file.unwrap()))
                        }
                        ExprResult::Value(literal) => { Some(literal) }
                        _ => { None }
                    }
                }
                Err(_) => { None }
            }.unwrap();

            println!("binary_op: {{ left: {:?}, right: {:?} }}", left, right);

            Ok(ExprResult::Value(consume_op(left, op, right)))
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

fn consume_op2(left: ExprResult, op: BinaryOperator, right: ExprResult) -> ExprResult {
    println!("op: {{ left: {:?}, right: {:?} }}", left, right);
    match op {
        BinaryOperator::Plus => { 
            match (left, right) {
                (ExprResult::Select(a), ExprResult::Select(b)) => {
                    ExprResult::Filter2(Box::new(|c| consume_op(a(c), op, b(c))))
                }
                _ => { panic!() }
            }
         }
        //BinaryOperator::Eq => { Value::Boolean(left == right) }
        _ => { unimplemented!() }
    }
}

fn consume_op(left: Value, op: BinaryOperator, right: Value) -> Value {
    println!("op: {{ left: {:?}, right: {:?} }}", left, right);
    match op {
        BinaryOperator::Plus => { 
            match (left, right) {
                (Value::Number(a), Value::Number(b)) => { Value::Number((a.parse::<i32>().unwrap() + b.parse::<i32>().unwrap()).to_string()) }
                (Value::Number(a), Value::Null) => { Value::Number(a) }
                (Value::SingleQuotedString(mut a), Value::SingleQuotedString(b)) => { a.push_str(b.as_str()); Value::SingleQuotedString(a) }
                (Value::SingleQuotedString(a), Value::Null) => { Value::SingleQuotedString(a) }
                (Value::Boolean(a), Value::Null) => { Value::Boolean(a) }
                (Value::Null, Value::Number(b)) => { Value::Number(b) }
                (Value::Null, Value::SingleQuotedString(b)) => { Value::SingleQuotedString(b) }
                (Value::Null, Value::Boolean(b)) => { Value::Boolean(b) }
                (Value::Null, Value::Null) => { Value::Null }
                _ => { panic!() }
            }
         }
        BinaryOperator::Eq => { Value::Boolean(left == right) }
        _ => { unimplemented!() }
    }
}

fn consume_relation(relation: TableFactor) -> Result<Vec<CoreFile>, CoreError> {
    let mut files: Vec<CoreFile> = Vec::new();
    match relation {
        TableFactor::Table { name, alias, args, with_hints } => {
            if let Some(table_name) = name.0.first() {
                files = consume_table_name(table_name.value.as_str())?;
            }
        }
        TableFactor::Derived { lateral, subquery, alias } => {}
        TableFactor::NestedJoin(_) => {}
    }

    return Ok(files);
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


