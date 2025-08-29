mod cli;
mod commands;
mod output;
mod reader;
mod schema;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use commands::{handle_metadata_command, handle_query_command, handle_schema_command};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Schema { files, format } => handle_schema_command(files, format),
        Commands::Metadata { files } => handle_metadata_command(files),
        Commands::Query { file, columns, limit } => {
            handle_query_command(file, columns, *limit)
        }
    }
}
