use core::ffi::{c_char, CStr};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    process,
};

#[no_mangle]
pub extern "C" fn processCsv(
    csv_data: *const c_char,
    selected_columns: *const c_char,
    row_filter_definitions: *const c_char,
) {
    unsafe {
        let csv_data = match CStr::from_ptr(csv_data).to_str() {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Failed to convert csv_data: {}", e);
                process::exit(1)
            }
        };

        let selected_columns = match CStr::from_ptr(selected_columns).to_str() {
            Ok(columns) => columns,
            Err(e) => {
                eprintln!("Failed to convert selected_columns: {}", e);
                process::exit(1)
            }
        };

        let row_filter_definitions = match CStr::from_ptr(row_filter_definitions).to_str() {
            Ok(filter) => filter,
            Err(e) => {
                eprintln!("Failed to convert row_filter_definitions: {}", e);
                process::exit(1)
            }
        };

        let final_result = process_csv_impl(csv_data, selected_columns, row_filter_definitions);
        print_result(final_result);
    }
}

#[no_mangle]
pub extern "C" fn processCsvFile(
    csv_file_path: *const c_char,
    selected_columns: *const c_char,
    row_filter_definitions: *const c_char,
) {
    unsafe {
        let csv_file_path = match CStr::from_ptr(csv_file_path).to_str() {
            Ok(path) => path,
            Err(e) => {
                eprintln!("Failed to convert csv_file_path: {}", e);
                process::exit(1)
            }
        };

        let selected_columns = match CStr::from_ptr(selected_columns).to_str() {
            Ok(columns) => columns,
            Err(e) => {
                eprintln!("Failed to convert selected_columns: {}", e);
                process::exit(1)
            }
        };

        let row_filter_definitions = match CStr::from_ptr(row_filter_definitions).to_str() {
            Ok(filter) => filter,
            Err(e) => {
                eprintln!("Failed to convert row_filter_definitions: {}", e);
                process::exit(1)
            }
        };

        let final_result =
            process_csv_file_impl(csv_file_path, selected_columns, row_filter_definitions);
        print_result(final_result);
    }
}

fn exit_if_no_header(col: &str) {
    eprintln!("Header '{}' not found in CSV file/string", col);
    panic!("Execution ended.");
}

fn split_filter(filter: &str) -> Option<Vec<&str>> {
    let filter_parts: Vec<&str> = if filter.contains("<=") {
        filter.split("<=").collect()
    } else if filter.contains(">=") {
        filter.split(">=").collect()
    } else if filter.contains("!=") {
        filter.split("!=").collect()
    } else if filter.contains('=') {
        filter.split('=').collect()
    } else if filter.contains('<') {
        filter.split('<').collect()
    } else if filter.contains('>') {
        filter.split('>').collect()
    } else if filter.is_empty() {
        return None;
    } else {
        eprintln!("Invalid filter: {}", filter);
        return None;
    };
    if filter_parts.len() != 2 {
        eprintln!("Invalid filter: {}", filter);
        return None;
    }
    Some(filter_parts)
}

fn remove_invalid_filters(filters: &str) -> Vec<&str> {
    filters
        .split('\n')
        .filter_map(|filter| {
            if split_filter(filter).is_some() {
                Some(filter)
            } else {
                None
            }
        })
        .collect::<Vec<&str>>()
}

fn print_result(final_result: String) {
    println!("{}", final_result);
}

fn process_csv_impl(
    csv_data: &str,
    selected_columns: &str,
    row_filter_definitions: &str,
) -> String {
    let mut lines = csv_data.lines();

    let headers = match lines.next() {
        Some(line) => line
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>(),
        None => {
            eprintln!("Invalid CSV: no headers found");
            process::exit(1);
        }
    };
    let header_indices: HashMap<&str, usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| (h.as_str(), i))
        .collect();

    let selected_cols: Vec<&str> = selected_columns.split(',').map(|s| s.trim()).collect();
    let filters: Vec<&str> = remove_invalid_filters(row_filter_definitions);

    let selected_indices: Vec<usize> = headers
        .iter()
        .filter_map(|header| {
            if selected_cols.contains(&header.as_str()) {
                header_indices.get(header.as_str()).cloned()
            } else {
                None
            }
        })
        .collect();

    for &col in &selected_cols {
        if !headers.contains(&col.to_string()) {
            exit_if_no_header(col);
        }
    }

    let reordered_selected_columns: Vec<String> = selected_indices
        .iter()
        .map(|&index| headers[index].clone())
        .collect();

    let mut result_rows = String::new();
    result_rows.push_str(&reordered_selected_columns.join(","));
    for line in lines {
        let record: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if apply_filters(&record, &filters, &header_indices) {
            let selected_values: Vec<&str> = selected_indices
                .iter()
                .map(|&index| record[index])
                .collect();

            result_rows.push_str("\n");
            result_rows.push_str(&selected_values.join(","));
        }
    }

    result_rows
}

fn process_csv_file_impl(
    csv_path: &str,
    selected_columns: &str,
    row_filter_definitions: &str,
) -> String {
    let file = match File::open(csv_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open file {}: {}", csv_path, e);
            process::exit(1);
        }
    };

    let reader = BufReader::new(file);
    let mut csv_data = String::new();
    for line in reader.lines() {
        match line {
            Ok(l) => csv_data.push_str(&format!("{}\n", l)),
            Err(e) => {
                eprintln!("Failed to read file {}: {}", csv_path, e);
                process::exit(1);
            }
        }
    }

    process_csv_impl(&csv_data, selected_columns, row_filter_definitions)
}
fn apply_filters(record: &[&str], filters: &[&str], header_indices: &HashMap<&str, usize>) -> bool {
    let mut grouped_filters: HashMap<&str, Vec<&str>> = HashMap::new();
    for &filter in filters {
        let filter_parts = split_filter(filter).unwrap();
        let col_name = filter_parts[0].trim();
        grouped_filters
            .entry(col_name)
            .or_insert(Vec::new())
            .push(filter);
    }

    for (col_name, col_filters) in grouped_filters {
        let mut column_passes = false;

        if let Some(&index) = header_indices.get(col_name) {
            for &filter in &col_filters {
                let filter_parts: Vec<&str> = split_filter(filter).unwrap();
                let col_value = filter_parts[1].trim();
                let record_value = record[index];
                let comparison = record_value.cmp(col_value);

                let filter_valid = (filter.contains('=')
                    && !filter.contains("<=")
                    && !filter.contains(">=")
                    && !filter.contains("!=")
                    && comparison == Ordering::Equal)
                    || (filter.contains('<')
                        && !filter.contains('=')
                        && comparison == Ordering::Less)
                    || (filter.contains('>')
                        && !filter.contains('=')
                        && comparison == Ordering::Greater)
                    || (filter.contains("<=") && comparison != Ordering::Greater)
                    || (filter.contains(">=") && comparison != Ordering::Less)
                    || (filter.contains("!=") && comparison != Ordering::Equal);

                if filter_valid {
                    column_passes = true;
                    break;
                }
            }

            if !column_passes {
                return false;
            }
        } else {
            exit_if_no_header(col_name);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
