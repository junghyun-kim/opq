mod cli;
mod commands;
mod output;
mod reader;
mod schema;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use commands::{handle_metadata_command, handle_schema_command, handle_view_command};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Schema { files, format } => handle_schema_command(files, format),
        Commands::Metadata { files } => handle_metadata_command(files),
        Commands::View {
            file,
            columns,
            limit,
            format,
            truncate,
        } => handle_view_command(file, columns, *limit, format, *truncate),
    }
}
