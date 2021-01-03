use assert_cmd::prelude::*; // Add methods on commands
use predicates::prelude::*; // Used for writing assertions
use std::process::Command; // Run programs

const PATH_TO_TEST_DIR: &str = "./test/";
const PROGRAM_NAME: &str = "systemsql";

#[test]
fn select_name() -> Result<(), Box<dyn std::error::Error>> {
    let sql = format!("SELECT Name FROM [{test_dir}]", test_dir = PATH_TO_TEST_DIR).to_owned();

    let mut cmd = Command::cargo_bin(PROGRAM_NAME)?;

    cmd.arg(sql);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("3.md").and(predicate::str::contains("one")).and(predicate::str::contains("two")));

    Ok(())
}