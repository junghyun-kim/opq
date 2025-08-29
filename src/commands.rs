use crate::cli::{OutputFormat, SchemaFormat};
use crate::output::print_arrow_batches;
use crate::reader::{
    FileType, get_file_type, get_orc_metadata, get_parquet_metadata,
    read_orc_to_arrow_with_projection, read_parquet_to_arrow_with_projection,
};
use crate::schema::show_schema;
use anyhow::Result;

/// Handle schema command - display schema information for files
pub fn handle_schema_command(files: &[String], format: &SchemaFormat) -> Result<()> {
    for file in files {
        show_schema(file, format)?;
        println!(); // 파일 간 구분을 위한 빈 줄
    }
    Ok(())
}

/// Handle metadata command - display metadata for files
pub fn handle_metadata_command(files: &[String]) -> Result<()> {
    for file in files {
        println!("=== {} ===", file);
        let file_type = get_file_type(file)?;
        match file_type {
            FileType::Parquet => get_parquet_metadata(file)?,
            FileType::Orc => get_orc_metadata(file)?,
        };
        println!(); // 파일 간 구분을 위한 빈 줄
    }
    Ok(())
}

/// Handle view command - display file contents with optional column selection and row limit
pub fn handle_view_command(
    file: &str,
    columns: &Option<Vec<String>>,
    limit: usize,
    format: &OutputFormat,
    truncate: usize,
) -> Result<()> {
    let file_type = get_file_type(file)?;
    let batches = match file_type {
        FileType::Parquet => read_parquet_to_arrow_with_projection(file, columns.clone())?,
        FileType::Orc => read_orc_to_arrow_with_projection(file, columns.clone())?,
    };

    let output_format = match format {
        OutputFormat::Table => crate::output::OutputFormat::Table,
        OutputFormat::Ndjson => crate::output::OutputFormat::Ndjson,
    };

    print_arrow_batches(batches, limit, &output_format, truncate)?;
    Ok(())
}
