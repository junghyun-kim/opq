use anyhow::{anyhow, Result};

/// Represents a sort specification for a single column
#[derive(Debug, Clone)]
pub struct SortSpec {
    pub column: String,
    pub ascending: bool,
}

/// Parse sort specifications from command line arguments
/// Supports formats:
/// - "column" -> (column, true)
/// - "column+" -> (column, true)  
/// - "column-" -> (column, false)
/// - "column:asc" -> (column, true)
/// - "column:desc" -> (column, false)
pub fn parse_sort_specs(sort_args: &[String]) -> Result<Vec<SortSpec>> {
    sort_args
        .iter()
        .map(|spec| parse_single_sort_spec(spec))
        .collect()
}

/// Parse a single sort specification string
fn parse_single_sort_spec(spec: &str) -> Result<SortSpec> {
    let spec = spec.trim();
    
    if spec.is_empty() {
        return Err(anyhow!("Empty sort specification"));
    }
    
    // Handle +/- suffix format
    if spec.ends_with('+') {
        return Ok(SortSpec {
            column: spec[..spec.len() - 1].to_string(),
            ascending: true,
        });
    }
    
    if spec.ends_with('-') {
        return Ok(SortSpec {
            column: spec[..spec.len() - 1].to_string(),
            ascending: false,
        });
    }
    
    // Handle :asc/:desc format
    if let Some((column, order)) = spec.split_once(':') {
        let ascending = match order.to_lowercase().as_str() {
            "asc" | "ascending" => true,
            "desc" | "descending" => false,
            _ => return Err(anyhow!("Invalid sort order '{}'. Use 'asc' or 'desc'", order)),
        };
        
        return Ok(SortSpec {
            column: column.to_string(),
            ascending,
        });
    }
    
    // Default: column name only, ascending
    Ok(SortSpec {
        column: spec.to_string(),
        ascending: true,
    })
}

/// Validate that all sort columns are present in the selected columns
pub fn validate_sort_columns(
    sort_specs: &[SortSpec],
    selected_columns: Option<&[String]>,
    all_columns: &[String],
) -> Result<()> {
    for spec in sort_specs {
        // Check if column exists in the file
        if !all_columns.contains(&spec.column) {
            return Err(anyhow!("Sort column '{}' does not exist in the file", spec.column));
        }
        
        // Check if column is selected (if column selection is used)
        if let Some(cols) = selected_columns {
            if !cols.contains(&spec.column) {
                return Err(anyhow!(
                    "Sort column '{}' must be included in --columns selection", 
                    spec.column
                ));
            }
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_column() {
        let result = parse_single_sort_spec("name").unwrap();
        assert_eq!(result.column, "name");
        assert_eq!(result.ascending, true);
    }

    #[test]
    fn test_parse_plus_suffix() {
        let result = parse_single_sort_spec("name+").unwrap();
        assert_eq!(result.column, "name");
        assert_eq!(result.ascending, true);
    }

    #[test]
    fn test_parse_minus_suffix() {
        let result = parse_single_sort_spec("age-").unwrap();
        assert_eq!(result.column, "age");
        assert_eq!(result.ascending, false);
    }

    #[test]
    fn test_parse_asc_suffix() {
        let result = parse_single_sort_spec("salary:asc").unwrap();
        assert_eq!(result.column, "salary");
        assert_eq!(result.ascending, true);
    }

    #[test]
    fn test_parse_desc_suffix() {
        let result = parse_single_sort_spec("date:desc").unwrap();
        assert_eq!(result.column, "date");
        assert_eq!(result.ascending, false);
    }

    #[test]
    fn test_parse_multiple_specs() {
        let specs = vec![
            "name".to_string(),
            "age-".to_string(), 
            "salary:desc".to_string(),
        ];
        let results = parse_sort_specs(&specs).unwrap();
        
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].column, "name");
        assert_eq!(results[0].ascending, true);
        assert_eq!(results[1].column, "age");
        assert_eq!(results[1].ascending, false);
        assert_eq!(results[2].column, "salary");
        assert_eq!(results[2].ascending, false);
    }

    #[test]
    fn test_invalid_order() {
        let result = parse_single_sort_spec("name:invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_sort_columns() {
        let sort_specs = vec![
            SortSpec { column: "name".to_string(), ascending: true },
            SortSpec { column: "age".to_string(), ascending: false },
        ];
        
        let all_columns = vec!["name".to_string(), "age".to_string(), "salary".to_string()];
        let selected_columns = vec!["name".to_string(), "age".to_string()];
        
        // Should pass validation
        let result = validate_sort_columns(&sort_specs, Some(&selected_columns), &all_columns);
        assert!(result.is_ok());
        
        // Should fail when sort column not in selection
        let limited_selection = vec!["name".to_string()];
        let result = validate_sort_columns(&sort_specs, Some(&limited_selection), &all_columns);
        assert!(result.is_err());
        
        // Should fail when sort column doesn't exist
        let invalid_spec = vec![SortSpec { column: "nonexistent".to_string(), ascending: true }];
        let result = validate_sort_columns(&invalid_spec, None, &all_columns);
        assert!(result.is_err());
    }
}