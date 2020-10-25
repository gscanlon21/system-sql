## Usage

Query current directory
```bash
cargo run "SELECT * FROM [./]"
```

Query column names
```bash
cargo run "SELECT TOP 0 * FROM [./]"
```


Save results to a CSV
```bash
 cargo run "INSERT INTO [temp.csv] SELECT * FROM [./]"
```

Save results as JSON
```bash
 cargo run "INSERT INTO [temp.json] SELECT * FROM [./]"
```


Pipe results as JSON
```bash
 cargo run "INSERT INTO [json] SELECT * FROM [./]"
```

