use anyhow::{Error, Result};
use arrow::array::Array;
use arrow::json::writer::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use clap::ValueEnum;
use comfy_table::{Table, Cell};

#[derive(ValueEnum, Clone, Debug)]
pub enum OutputFormat {
    Table,
    Vertical,
    Ndjson,
}

fn print_vertical_format(batches: &[RecordBatch]) -> Result<()> {
    for (batch_idx, batch) in batches.iter().enumerate() {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            if batch_idx > 0 || row_idx > 0 {
                println!(); // Empty line between records
            }

            println!(
                "*************************** {} ***************************",
                row_idx + 1
            );

            for (col_idx, column) in batch.columns().iter().enumerate() {
                let field = schema.field(col_idx);
                let field_name = field.name();
                let value = format_array_value(column.as_ref(), row_idx);

                println!("{:>20}: {}", field_name, value);
            }
        }
    }
    Ok(())
}

fn format_array_value(array: &dyn Array, row_idx: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(row_idx) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Struct(_) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut parts = Vec::new();

            for (i, field) in struct_array.fields().iter().enumerate() {
                let child_array = struct_array.column(i);
                let child_value = format_array_value(child_array.as_ref(), row_idx);
                parts.push(format!("{}: {}", field.name(), child_value));
            }

            format!("{{{}}}", parts.join(", "))
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let list_value = list_array.value(row_idx);
            let mut items = Vec::new();

            for i in 0..list_value.len() {
                items.push(format_array_value(list_value.as_ref(), i));
            }

            format!("[{}]", items.join(", "))
        }
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            string_array.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            int_array.value(row_idx).to_string()
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            float_array.value(row_idx).to_string()
        }
        DataType::Float32 => {
            let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            float_array.value(row_idx).to_string()
        }
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            int_array.value(row_idx).to_string()
        }
        DataType::Boolean => {
            let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            bool_array.value(row_idx).to_string()
        }
        _ => {
            // Fallback for other types
            format!("<{:?}>", array.data_type())
        }
    }
}

pub fn print_arrow_batches(
    batch_iterator: impl Iterator<Item = Result<RecordBatch>>,
    limit: usize,
    format: &OutputFormat,
    truncate: usize,
) -> Result<()> {
    match format {
        OutputFormat::Table => {
            let mut all_batches = Vec::new();
            let mut total_rows = 0;

            for batch_result in batch_iterator {
                let batch = batch_result?;

                let rows_to_take = std::cmp::min(batch.num_rows(), limit - total_rows);

                if rows_to_take > 0 {
                    let limited_batch = batch.slice(0, rows_to_take);
                    all_batches.push(limited_batch);
                    total_rows += rows_to_take;
                }

                if total_rows >= limit {
                    break;
                }
            }

            if !all_batches.is_empty() {
                print_table_with_truncate(&all_batches, truncate)?;
            }
        }
        OutputFormat::Vertical => {
            let mut all_batches = Vec::new();
            let mut total_rows = 0;

            for batch_result in batch_iterator {
                let batch = batch_result?;

                let rows_to_take = std::cmp::min(batch.num_rows(), limit - total_rows);

                if rows_to_take > 0 {
                    let limited_batch = batch.slice(0, rows_to_take);
                    all_batches.push(limited_batch);
                    total_rows += rows_to_take;
                }

                if total_rows >= limit {
                    break;
                }
            }

            if !all_batches.is_empty() {
                print_vertical_format(&all_batches)?;
            }
        }
        OutputFormat::Ndjson => {
            let mut total_rows = 0;

            for batch_result in batch_iterator {
                let batch = batch_result?;

                let rows_to_take = std::cmp::min(batch.num_rows(), limit - total_rows);

                if rows_to_take > 0 {
                    let limited_batch = batch.slice(0, rows_to_take);

                    let mut buffer = Vec::new();
                    {
                        let mut writer = LineDelimitedWriter::new(&mut buffer);
                        writer
                            .write_batches(&[&limited_batch])
                            .map_err(Error::new)?;
                        writer.finish().map_err(Error::new)?;
                    }

                    let json_str = String::from_utf8(buffer).map_err(Error::new)?;
                    print!("{}", json_str);

                    total_rows += rows_to_take;
                }

                if total_rows >= limit {
                    break;
                }
            }
        }
    }

    Ok(())
}

fn print_table_with_truncate(batches: &[RecordBatch], truncate_len: usize) -> Result<()> {
    let mut table = Table::new();

    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();
    
    // Add header row
    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_header(header);

    // Add data rows
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for (col_idx, _column) in batch.columns().iter().enumerate() {
                let column = batch.column(col_idx);
                let value = format_array_value(column.as_ref(), row_idx);
                let truncated_value = if truncate_len > 0 && value.len() > truncate_len {
                    format!("{}...", &value[..truncate_len])
                } else {
                    value
                };
                row.push(Cell::new(truncated_value));
            }
            table.add_row(row);
        }
    }

    println!("{table}");
    Ok(())
}
