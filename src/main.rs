mod output;
mod reader;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use reader::{
    FileType, get_file_type, get_orc_metadata, get_orc_schema, get_parquet_metadata,
    get_parquet_schema, read_orc_to_arrow_with_projection, read_parquet_to_arrow_with_projection,
};
use output::print_arrow_batches;

#[derive(ValueEnum, Clone, Debug)]
pub enum SchemaFormat {
    Raw,
    Tree,
}

#[derive(ValueEnum, Clone, Debug)]
pub enum OutputFormat {
    Table,
    Json,
}

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
        /// Schema output format
        #[arg(short = 'o', long, value_enum, default_value_t = SchemaFormat::Raw)]
        format: SchemaFormat,
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
        /// Select specific fields (comma-separated)
        #[arg(long, value_delimiter = ',')]
        fields: Option<Vec<String>>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Schema { file, format } => {
            show_schema(file, format)
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
            fields,
        } => {
            let file_type = get_file_type(file)?;
            let batch_iterator = match file_type {
                FileType::Parquet => read_parquet_to_arrow_with_projection(file, fields.clone())?,
                FileType::Orc => read_orc_to_arrow_with_projection(file, fields.clone())?,
            };
            let output_format = match format {
                OutputFormat::Table => output::OutputFormat::Table,
                OutputFormat::Json => output::OutputFormat::Ndjson,
            };
            print_arrow_batches(batch_iterator, *limit, &output_format)
        }
    }
}

fn show_schema(file: &str, format: &SchemaFormat) -> Result<()> {
    let file_type = get_file_type(file)?;
    
    match file_type {
        FileType::Parquet => {
            let schema = get_parquet_schema(file)?;
            match format {
                SchemaFormat::Raw => {
                    println!("{}", schema);
                }
                SchemaFormat::Tree => {
                    print_schema_tree(&schema, "parquet");
                }
            }
        }
        FileType::Orc => {
            let schema = get_orc_schema(file)?;
            match format {
                SchemaFormat::Raw => {
                    println!("{}", schema);
                }
                SchemaFormat::Tree => {
                    print_schema_tree(&schema, "orc");
                }
            }
        }
    }
    
    Ok(())
}

fn print_schema_tree(schema: &str, file_type: &str) {
    println!("Schema Tree ({}):", file_type);
    
    if file_type == "parquet" {
        print_parquet_schema_tree(schema);
    } else {
        print_orc_schema_tree(schema);
    }
}

fn print_parquet_schema_tree(schema: &str) {
    let lines: Vec<&str> = schema.lines().collect();
    let mut root_name = "root";
    
    // GroupType에서 root 이름 찾기
    for line in &lines {
        if line.trim().starts_with("name:") {
            if let Some(name_part) = line.split('"').nth(1) {
                root_name = name_part;
                break;
            }
        }
    }
    
    println!("└── {}", root_name);
    
    // fields 섹션 찾기
    let mut in_fields = false;
    let mut brace_depth = 0;
    let mut current_field = String::new();
    let mut field_type = String::new();
    
    for line in &lines {
        let trimmed = line.trim();
        
        if trimmed == "fields: [" {
            in_fields = true;
            continue;
        }
        
        if !in_fields {
            continue;
        }
        
        // 중괄호 깊이 추적
        brace_depth += trimmed.chars().filter(|&c| c == '{').count() as i32;
        brace_depth -= trimmed.chars().filter(|&c| c == '}').count() as i32;
        
        // 필드 이름 추출
        if trimmed.starts_with("name:") && current_field.is_empty() {
            if let Some(name_part) = trimmed.split('"').nth(1) {
                current_field = name_part.to_string();
            }
        }
        
        // 필드 타입 추출
        if trimmed.starts_with("physical_type:") && !field_type.is_empty() == false {
            if let Some(type_part) = trimmed.split(':').nth(1) {
                field_type = type_part.trim().trim_end_matches(',').to_string();
            }
        } else if trimmed.starts_with("converted_type:") && field_type.is_empty() {
            if let Some(type_part) = trimmed.split(':').nth(1) {
                let type_str = type_part.trim().trim_end_matches(',');
                if type_str != "NONE" {
                    field_type = type_str.to_string();
                }
            }
        }
        
        // 필드 완료 시 출력
        if brace_depth == 0 && !current_field.is_empty() {
            let type_display = if field_type.is_empty() { 
                "UNKNOWN".to_string() 
            } else { 
                field_type.clone() 
            };
            println!("    ├── {} ({})", current_field, type_display);
            current_field.clear();
            field_type.clear();
        }
        
        // fields 배열 끝
        if trimmed == "]," && brace_depth <= 0 {
            break;
        }
    }
}

fn print_orc_schema_tree(schema: &str) {
    println!("└── root");
    
    // Arrow Schema의 경우 Field 구조 파싱
    let lines: Vec<&str> = schema.lines().collect();
    let mut in_fields = false;
    let mut current_field = String::new();
    let mut current_type = String::new();
    let mut in_field_block = false;
    
    for line in &lines {
        let trimmed = line.trim();
        
        if trimmed == "fields: [" {
            in_fields = true;
            continue;
        }
        
        if !in_fields {
            continue;
        }
        
        if trimmed == "]," {
            break;
        }
        
        // Field 블록 시작
        if trimmed.starts_with("Field {") {
            in_field_block = true;
            current_field.clear();
            current_type.clear();
            continue;
        }
        
        if in_field_block {
            // 필드 이름 추출
            if trimmed.starts_with("name:") {
                if let Some(name_part) = trimmed.split('"').nth(1) {
                    current_field = name_part.to_string();
                }
            }
            
            // 데이터 타입 추출
            if trimmed.starts_with("data_type:") {
                if let Some(type_part) = trimmed.split(':').nth(1) {
                    current_type = type_part.trim().trim_end_matches(',').to_string();
                }
            }
            
            // 필드 블록 끝
            if trimmed == "}," {
                if !current_field.is_empty() {
                    println!("    ├── {} ({})", current_field, current_type);
                }
                in_field_block = false;
            }
        }
    }
}


