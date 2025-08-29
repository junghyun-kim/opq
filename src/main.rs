mod output;
mod reader;

use anyhow::Result;
use clap::{Parser, Subcommand};

use output::{OutputFormat, print_arrow_batches};
use reader::{
    FileType, get_file_type, get_orc_metadata, get_orc_schema, get_parquet_metadata,
    get_parquet_schema, read_orc_to_arrow, read_parquet_to_arrow,
};

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

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Schema { file } => {
            let file_type = get_file_type(file)?;
            let result = match file_type {
                FileType::Parquet => get_parquet_schema(file),
                FileType::Orc => get_orc_schema(file),
            };
            result
        }
        Commands::Meta { file } => {
            let file_type = get_file_type(file)?;
            let result = match file_type {
                FileType::Parquet => get_parquet_metadata(file),
                FileType::Orc => get_orc_metadata(file),
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
