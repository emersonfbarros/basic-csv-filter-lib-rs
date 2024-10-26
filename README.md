# CSV Processing Library

A lightweight, dependency-free Rust library for processing CSV data with column selection and row filtering capabilities. The library provides a C-compatible interface, making it suitable for integration with other programming languages and systems.

## Features

- Process CSV data from both strings and files
- Select specific columns for output
- Apply multiple filter conditions to rows
- Support for various comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`)
- Zero external dependencies
- C-compatible interface
- Comprehensive test coverage

## Installation

Since this is a Rust library that compiles to a C-compatible library, you'll need to have Rust installed on your system. The library can be built using Cargo, Rust's package manager.

1. Add the library to your Cargo.toml:

```toml
[lib]
name = "csv"
crate-type = ["cdylib"]
```

2. Build the library:

```bash
cargo build --release
```

This will create a shared library in the target/release directory.

## Usage

### C Interface

The library provides two main functions:

```c
void processCsv(const char[] csv_data, const char[] selected_columns, const char[] row_filter_definitions);
void processCsvFile(const char[] csv_file_path, const char[] selected_columns, const char[] row_filter_definitions);
```

#### Parameters

- `csv_data`/`csv_file_path`: The CSV data as a string or file path
- `selected_columns`: Comma-separated list of column headers to include in the output
- `row_filter_definitions`: Newline-separated list of filter conditions

#### Filter syntax

Filters support the following operators:

- `=` (equals)
- `!=` (not equals)
- `<` (less than)
- `>` (greater than)
- `<=` (less than or equal)
- `>=` (greater than or equal)

Multiple filters for the same column are treated as OR conditions, while filters across different columns are treated as AND conditions.

#### Example

```c
// Processing CSV data directly
const char* csv_data = "name,age,salary\nJohn,30,50000\nJane,25,60000";
const char* selected_columns = "name,salary";
const char* filters = "age>20\nsalary>=55000";

processCsv(csv_data, selected_columns, filters);

// Processing CSV from a file
const char* file_path = "employees.csv";
processCsvFile(file_path, selected_columns, filters);
```

#### Output Format

The output is printed to stdout in CSV format, with the selected columns in the order specified in the selected_columns parameter. For example:

```
name,salary
Jane,60000
```

### Error Handling

The library handles various error conditions and provides appropriate error messages:

- Invalid CSV format
- Missing headers
- Invalid filter syntax
- File access errors
- Character encoding errors

Errors are printed to `stderr`, and the program exits with status code 1 when an error occurs.

## Testing

The library includes a comprehensive test suite covering various scenarios:

- Basic column selection
- Different filter combinations
- Edge cases
- Error conditions
- Double quote handling in headers
- Multiple filter conditions per column

Run the tests using:

```bash
cargo test -- --test-threads=1
```

Important Note: The flag `-- --test-threads=1` is required when running tests. This is because some tests intentionally trigger `process::exit` and `panic` conditions to verify error handling. Running tests in a single thread prevents these termination conditions from interfering with other test executions.

## Limitations

- Assumes CSV data is comma-delimited
- Doesn't support quoted fields containing commas
- All comparisons are performed lexicographically
- Filter values must be exact matches (no partial matches or regular expressions)
- Headers must be unique

## Performance Considerations

- The library processes CSV data line by line
- No additional memory allocation for intermediate results
- Filters are evaluated lazily
- Column selection is performed during output generation

## Thread Safety

The library is not explicitly thread-safe. When using in a multi-threaded environment, ensure that calls to the processing functions are properly synchronized.
