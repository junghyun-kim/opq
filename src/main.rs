mod output;
mod reader;

use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use orc_rust::arrow_reader::ArrowReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;

use output::{OutputFormat, print_arrow_batches};
use reader::{FileType, get_file_type, read_orc_to_arrow, read_parquet_to_arrow};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// View the schema of a file
    Schema {
        /// Path to the file
        #[arg(short, long)]
        file: String,
    },
    /// View the metadata of a file
    Meta {
        /// Path to the file
        #[arg(short, long)]
        file: String,
    },
    /// View the content of a file
    View {
        /// Path to the file
        #[arg(short, long)]
        file: String,
        /// Number of rows to display
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
        /// Output format
        #[arg(short = 'o', long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
}

fn print_parquet_schema(file_path: &str) -> Result<()> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let file = File::open(&path).map_err(Error::new)?;
    let reader = SerializedFileReader::new(file).map_err(Error::new)?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema();

    println!("{:#?}", schema);

    Ok(())
}

fn print_orc_schema(file_path: &str) -> Result<()> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let file = File::open(&path).map_err(Error::new)?;
    let reader_builder = ArrowReaderBuilder::try_new(file).map_err(Error::new)?;
    let schema = reader_builder.schema();

    println!("{:#?}", schema);

    Ok(())
}

fn print_parquet_meta(file_path: &str) -> Result<()> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let file = File::open(&path).map_err(Error::new)?;
    let reader = SerializedFileReader::new(file).map_err(Error::new)?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();

    println!("Number of row groups: {}", metadata.num_row_groups());
    println!("Number of rows: {}", file_metadata.num_rows());
    println!(
        "Created by: {}",
        file_metadata.created_by().unwrap_or("N/A")
    );

    if let Some(key_value_metadata) = file_metadata.key_value_metadata() {
        println!("Key-value metadata:");
        for kv in key_value_metadata {
            println!("  {}: {}", kv.key, kv.value.as_deref().unwrap_or(""));
        }
    }

    Ok(())
}

fn print_orc_meta(file_path: &str) -> Result<()> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let file = File::open(&path).map_err(Error::new)?;
    let reader_builder = ArrowReaderBuilder::try_new(file).map_err(Error::new)?;
    let metadata = reader_builder.file_metadata();

    println!("Number of rows: {}", metadata.number_of_rows());

    if !metadata.user_custom_metadata().is_empty() {
        println!("User metadata:");
        for (key, value) in metadata.user_custom_metadata() {
            println!("  {}: {:?}", key, value);
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Schema { file } => {
            let file_type = get_file_type(file)?;
            let result = match file_type {
                FileType::Parquet => print_parquet_schema(file),
                FileType::Orc => print_orc_schema(file),
            };
            result
        }
        Commands::Meta { file } => {
            let file_type = get_file_type(file)?;
            let result = match file_type {
                FileType::Parquet => print_parquet_meta(file),
                FileType::Orc => print_orc_meta(file),
            };
            result
        }
        Commands::View {
            file,
            limit,
            format,
        } => {
            let file_type = get_file_type(file)?;
            let batch_iterator = match file_type {
                FileType::Parquet => read_parquet_to_arrow(file)?,
                FileType::Orc => read_orc_to_arrow(file)?,
            };
            print_arrow_batches(batch_iterator, *limit, format)
        }
    }
}
