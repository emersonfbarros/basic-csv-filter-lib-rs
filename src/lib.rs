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
mod libcsv_tests {
    use super::*;
    use std::{collections::HashMap, ffi::CString};

    #[test]
    fn test_exit_if_no_header() {
        let col = "nonexistent";
        let result = std::panic::catch_unwind(|| exit_if_no_header(col));
        assert!(result.is_err());
    }

    #[test]
    fn test_split_filter() {
        assert_eq!(split_filter("age>25"), Some(vec!["age", "25"]));
        assert_eq!(split_filter("age<25"), Some(vec!["age", "25"]));
        assert_eq!(split_filter("age>=25"), Some(vec!["age", "25"]));
        assert_eq!(split_filter("age=25"), Some(vec!["age", "25"]));
        assert_eq!(split_filter("age<=25"), Some(vec!["age", "25"]));
        assert_eq!(split_filter("age!=25"), Some(vec!["age", "25"]));
        assert!(split_filter("").is_none());
        assert!(split_filter("age=25=40").is_none());
        assert!(split_filter("invalidfilter").is_none());
    }

    #[test]
    fn test_remove_invalid_filters() {
        let filters = "age>25\ninvalidfilter\nage<30";
        let valid_filters = remove_invalid_filters(filters);
        assert_eq!(valid_filters, vec!["age>25", "age<30"]);
    }

    const CSV_DATA: &str = "header1,header2,header3\n1,2,3\n4,5,6\n7,8,9";

    fn setup_test_csv() -> HashMap<&'static str, (&'static str, &'static str, &'static str)> {
        let mut cases = HashMap::new();

        cases.insert(
            "all_columns_no_filters",
            (
                "header1,header2,header3",
                "",
                "header1,header2,header3\n1,2,3\n4,5,6\n7,8,9",
            ),
        );

        cases.insert(
            "two_columns_no_filter",
            ("header2,header3", "", "header2,header3\n2,3\n5,6\n8,9"),
        );

        cases.insert(
            "all_columns_no_filters_unordered",
            (
                "header3,header1,header2",
                "",
                "header1,header2,header3\n1,2,3\n4,5,6\n7,8,9",
            ),
        );

        cases.insert(
            "two_columns_equal_filter",
            ("header3,header1", "header2=2", "header1,header3\n1,3"),
        );

        cases.insert(
            "two_columns_less_filter",
            ("header2,header1", "header1<2", "header1,header2\n1,2"),
        );

        cases.insert(
            "two_columns_greater_filter",
            (
                "header1,header2,header3",
                "header3>7",
                "header1,header2,header3\n7,8,9",
            ),
        );

        cases.insert(
            "two_columns_different_filter",
            ("header3,header1", "header1!=1", "header1,header3\n4,6\n7,9"),
        );

        cases.insert(
            "two_columns_geater_or_equal_filter",
            ("header2,header1", "header2>=5", "header1,header2\n4,5\n7,8"),
        );

        cases.insert(
            "two_columns_less_or_equal_filter",
            ("header3,header2", "header1<=6", "header2,header3\n2,3\n5,6"),
        );

        cases.insert(
            "all_columns_all_simple_filters",
            (
                "header2,header3,header1",
                "header1>6\nheader2<9\nheader3=9",
                "header1,header2,header3\n7,8,9",
            ),
        );

        cases.insert(
            "all_columns_all_composite_filters",
            (
                "header3,header1,header2",
                "header1!=2\nheader2>=5\nheader3<=6",
                "header1,header2,header3\n4,5,6",
            ),
        );

        cases.insert(
            "all_columns_mixed_filters",
            (
                "header3,header1,header2",
                "header1!=2\nheader2=5\nheader3<=7",
                "header1,header2,header3\n4,5,6",
            ),
        );

        cases.insert(
            "all_columns_or_equal_filters",
            (
                "header3,header1,header2",
                "header1=10\nheader1=7",
                "header1,header2,header3\n7,8,9",
            ),
        );

        cases.insert(
            "all_columns_or_less_filters",
            (
                "header3,header1,header2",
                "header3<2\nheader3<7",
                "header1,header2,header3\n1,2,3\n4,5,6",
            ),
        );

        cases.insert(
            "all_columns_or_grater_filter",
            (
                "header3,header1,header2",
                "header2>9\nheader2>7",
                "header1,header2,header3\n7,8,9",
            ),
        );

        cases.insert(
            "all_columns_or_mixed_simple_filter",
            (
                "header3,header1,header2",
                "header1=1\nheader1=4\nheader2>3\nheader3>4",
                "header1,header2,header3\n4,5,6",
            ),
        );

        cases.insert(
            "all_columns_or_mixed_composite_filter",
            (
                "header3,header1,header2",
                "header1!=1\nheader1=8\nheader2>=3\nheader3=4\nheader3<=9",
                "header1,header2,header3\n4,5,6\n7,8,9",
            ),
        );

        cases.insert(
            "two_columns_invalid_filter",
            (
                "header3,header2",
                "header1=7\nheader2-8",
                "header2,header3\n8,9",
            ),
        );

        cases
    }

    #[test]
    fn test_process_csv_impl_multiple_cases() {
        let cases = setup_test_csv();

        for (case_name, (selected_columns, row_filter_definitions, expected_result)) in cases {
            let result = process_csv_impl(CSV_DATA, selected_columns, row_filter_definitions);
            assert_eq!(
                &result, expected_result,
                "\n\nTest case '{}' failed: \nExpected '{}'; \nGot '{}'\n",
                case_name, expected_result, result
            );
        }
    }

    #[test]
    fn test_process_csv_impl_multiple_with_double_quote_as_header_char() {
        let csv_data: &str = "header1,header2,heade\"r3\n1,2,3\n4,5,6\n7,8,9";
        let selected_columns = "heade\"r3,header1,header2";
        let row_filter_definitions = "header1!=1\nheader1=8\nheader2>=3\nheade\"r3=4\nheade\"r3<=9";
        let expected_result = "header1,header2,heade\"r3\n4,5,6\n7,8,9";

        let result = process_csv_impl(csv_data, selected_columns, row_filter_definitions);
        assert_eq!(&result, expected_result,);
    }

    #[test]
    fn test_process_csv_impl_missing_selected_header() {
        let selected_columns = "header1,nonexistent";
        let filters = "";
        let result =
            std::panic::catch_unwind(|| process_csv_impl(CSV_DATA, selected_columns, filters));
        assert!(result.is_err());
    }

    #[test]
    fn test_process_csv_impl_missing_selected_in_filters() {
        let selected_columns = "header1,header2,header3";
        let filters = "header1=7\nnonexistent=8";
        let result =
            std::panic::catch_unwind(|| process_csv_impl(CSV_DATA, selected_columns, filters));
        assert!(result.is_err());
    }

    const FILE_PATH: &str = "/tmp/test.csv";

    #[test]
    fn test_process_csv_file_impl_multiple_with_double_quote_as_header_char() {
        let csv_data: &str = "header1,header2,heade\"r3\n1,2,3\n4,5,6\n7,8,9";
        std::fs::write(FILE_PATH, csv_data).unwrap();

        let selected_columns = "heade\"r3,header1,header2";
        let row_filter_definitions = "header1!=1\nheader1=8\nheader2>=3\nheade\"r3=4\nheade\"r3<=9";
        let expected_result = "header1,header2,heade\"r3\n4,5,6\n7,8,9";

        let result = process_csv_file_impl(FILE_PATH, selected_columns, row_filter_definitions);
        assert_eq!(&result, expected_result,);
    }

    #[test]
    fn test_process_csv_file_impl_multiple_cases() {
        let cases = setup_test_csv();

        for (case_name, (selected_columns, row_filter_definitions, expected_result)) in cases {
            std::fs::write(FILE_PATH, CSV_DATA).unwrap();
            let result = process_csv_file_impl(FILE_PATH, selected_columns, row_filter_definitions);
            assert_eq!(
                &result, expected_result,
                "\n\nTest case '{}' failed: \nExpected '{}'; \nGot '{}'\n",
                case_name, expected_result, result
            );
        }
    }

    #[test]
    fn test_process_csv_file_impl_missing_selected_header() {
        let selected_columns = "header1,nonexistent";
        let filters = "";
        std::fs::write(FILE_PATH, CSV_DATA).unwrap();
        let result = std::panic::catch_unwind(|| {
            process_csv_file_impl(FILE_PATH, selected_columns, filters)
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_process_csv_file_impl_missing_selected_in_filters() {
        let selected_columns = "header1,header2,header3";
        let filters = "header1=7\nnonexistent=8";
        std::fs::write(FILE_PATH, CSV_DATA).unwrap();
        let result = std::panic::catch_unwind(|| {
            process_csv_file_impl(FILE_PATH, selected_columns, filters)
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_inputs_on_process_csv_c_function() {
        let csv_data = CString::new(CSV_DATA).unwrap();
        let selected_columns = CString::new("header1,header2,header3").unwrap();
        let row_filter_definitions =
            CString::new("header1!=1\nheader1=8\nheader2>=3\nheader3=4\nheader3<=9").unwrap();

        processCsv(
            csv_data.as_ptr(),
            selected_columns.as_ptr(),
            row_filter_definitions.as_ptr(),
        );
    }

    #[test]
    fn test_inputs_on_process_csv_file_c_function() {
        std::fs::write(FILE_PATH, CSV_DATA).unwrap();

        let csv_file_path = CString::new(FILE_PATH).unwrap();
        let selected_columns = CString::new("header1,header2,header3").unwrap();
        let row_filter_definitions =
            CString::new("header1!=1\nheader1=8\nheader2>=3\nheader3=4\nheader3<=9").unwrap();

        processCsvFile(
            csv_file_path.as_ptr(),
            selected_columns.as_ptr(),
            row_filter_definitions.as_ptr(),
        );
    }
}
