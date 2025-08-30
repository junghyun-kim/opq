# opq.rs (ORC Parquet Query built with Rust)
A fast and efficient command-line tool for viewing, inspecting, and querying ORC and Parquet files with comprehensive compression support.

## Features

### File Format Support
- **ORC** - Optimized Row Columnar format
- **Parquet** - Apache Parquet columnar storage format

### Compression Support
- **gzip** (`.gz`)
- **zstd** (`.zst`, `.zstd`)
- **snappy** (`.snappy`, `.sz`)
- **zlib** (`.zlib`, `.z`)
- **Uncompressed** files

### Output Formats
- **Table** - Pretty-printed table format (default)
- **NDJSON** - Newline-delimited JSON format

Note: Vertical format has been removed. Use table format for structured output or NDJSON for data export.

### Commands
- **schema** - View file schema and structure with tree visualization support
- **metadata** - View file metadata and statistics 
- **view** - View file contents with customizable output formats and column selection

### Advanced Features
- **Tree Schema Visualization** - Hierarchical display of complex nested structures
- **Multiple File Support** - Process multiple files with schema and metadata commands
- **Column Selection** - Choose specific columns for better performance and focused analysis
  - Supports top-level columns only (e.g., `name`, `age`, `address`)
  - Nested field selection (e.g., `address.city`) is not currently supported
  - Complex nested structures are returned as complete objects
- **Streaming Processing** - Memory-efficient handling of large files
- **Auto-detection** - Automatic file type and compression format detection

## Installation

### From Source
```bash
git clone <repository-url>
cd opq.rs
cargo build --release
```

The binary will be available at `target/release/opq`.

## Usage

### Basic Commands

#### View Schema
```bash
# View schema of an ORC file
opq schema data.orc

# View schema of a compressed Parquet file in tree format
opq schema data.parquet.gz --format tree

# View schema of multiple files
opq schema data1.parquet data2.orc samples/*.parquet

# Tree format shows hierarchical structure for nested data
opq schema nested_data.parquet --format tree
```

#### View Metadata
```bash
# View metadata of a Parquet file
opq metadata data.parquet

# View metadata of multiple files
opq metadata data.parquet data.orc samples/*.parquet

# View metadata of a compressed ORC file
opq metadata data.orc.zst
```

#### View Data Content
```bash
# View first 10 rows in table format (default)
opq view data.parquet

# View first 20 rows
opq view data.orc --limit 20

# Export to NDJSON format
opq view data.parquet.gz --format ndjson --limit 100

# Select specific columns for better performance
opq view data.parquet --columns "id,name,email" --limit 10

# Single column selection
opq view data.parquet --columns "name" --limit 5

# Select nested structures as complete objects
opq view nested_data.parquet --columns "id,address,metadata" --limit 5

# Note: view command processes one file at a time
# For multiple files, use separate commands:
# opq view file1.parquet --limit 5
# opq view file2.orc --limit 5
```

### Output Format Examples

#### Table Format (Default)
```
+-------------+----------+--------+-------------------------+
| PassengerId | Survived | Pclass | Name                    |
+-------------+----------+--------+-------------------------+
| 1           | 0        | 3      | Braund, Mr. Owen Harris |
| 2           | 1        | 1      | Cumings, Mrs. John...   |
+-------------+----------+--------+-------------------------+
```

#### Tree Schema Format
```
Schema Tree (parquet):
└── root
    ├── id: INT64
    ├── name: UTF8
    ├── address: STRUCT
    │   ├── street: UTF8
    │   ├── city: UTF8
    │   └── coordinates: STRUCT
    │       ├── lat: FLOAT64
    │       └── lng: FLOAT64
    └── metadata: STRUCT
        ├── created_at: UTF8
        └── preferences: STRUCT
            ├── theme: UTF8
            └── language: UTF8
```

#### NDJSON Format
```json
{"PassengerId":1,"Survived":0,"Pclass":3,"Name":"Braund, Mr. Owen Harris","Sex":"male","Age":22.0}
{"PassengerId":2,"Survived":1,"Pclass":1,"Name":"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","Sex":"female","Age":38.0}
```

## Compression Support

OPQ automatically detects compression based on file extensions:

| Compression | Extensions | Example |
|-------------|------------|---------|
| gzip | `.gz` | `data.parquet.gz`, `data.orc.gz` |
| zstd | `.zst`, `.zstd` | `data.parquet.zst`, `data.orc.zstd` |
| snappy | `.snappy`, `.sz` | `data.parquet.snappy`, `data.orc.sz` |
| zlib | `.zlib`, `.z` | `data.parquet.zlib`, `data.orc.z` |

## Examples

### Working with Compressed Files
```bash
# Schema of gzip-compressed Parquet with tree view
opq schema sales_data.parquet.gz --format tree

# Metadata of zstd-compressed ORC
opq metadata logs.orc.zst

# View snappy-compressed data
opq view user_events.parquet.snappy --limit 5
```

### Data Exploration Workflow
```bash
# 1. Check the schema first with tree visualization
opq schema dataset.parquet --format tree

# 2. View metadata for size info
opq metadata dataset.parquet

# 3. Preview data in table format
opq view dataset.parquet --limit 10

# 4. Select specific columns for analysis
opq view dataset.parquet --columns "user_id,timestamp,event_type" --limit 20

# 5. Export sample to JSON for further processing
opq view dataset.parquet --columns "id,name,email" --format ndjson --limit 1000 > sample.jsonl
```

### Working with Multiple Files
```bash
# View schema of multiple files
opq schema data/*.parquet --format tree

# Check metadata for all ORC files in a directory
opq metadata logs/*.orc

# Process multiple files with the same command
opq schema samples/sample_parquet_1/titanic.parquet samples/sample_orc_2/iris.orc

# Note: view command only supports one file at a time
# For viewing multiple files, run separate commands:
for file in data/*.parquet; do
  echo "=== Processing $file ==="
  opq view "$file" --limit 5
done
```

## Supported Data Types

OPQ handles complex nested data structures including:
- Primitive types (integers, floats, strings, booleans)
- Nested structs with hierarchical tree visualization
- Arrays and lists
- Maps and dictionaries
- Timestamp and date types

### Schema Visualization
- **Raw format**: Shows native schema representation
- **Tree format**: Hierarchical visualization perfect for understanding nested structures

### Column Selection Limitations
- **Supported**: Top-level column selection (`id`, `name`, `address`)
- **Not supported**: Nested field paths (`address.city`, `metadata.preferences.theme`)
- **Workaround**: Select the entire nested structure and use post-processing tools

## Performance

- **Fast startup** - Optimized for quick data inspection
- **Memory efficient** - Streams data without loading entire files into memory
- **Large file support** - Handles multi-GB files efficiently
- **Compression aware** - Automatic decompression without temporary files
- **Arrow-based** - Uses Apache Arrow for unified data processing

### Large File Handling

OPQ is designed to handle large files efficiently:

#### ✅ **Uncompressed Files (Recommended for large datasets)**
- **Streaming processing** - Only loads data as needed
- **Memory usage** - Constant memory usage regardless of file size
- **5GB+ files** - No memory issues, processes in batches

#### ⚠️ **Compressed Files (Memory considerations)**
- **Memory requirement** - Decompressed file size + processing overhead
- **5GB compressed file** - May require 6-8GB available memory
- **Recommendation** - For very large files, use uncompressed format when possible

#### 💡 **Best Practices for Large Files**
```bash
# Check file size and metadata first
opq metadata large_dataset.parquet

# Preview small sample before processing large amounts
opq view large_dataset.parquet --limit 10

# View schema with tree format to understand structure
opq schema large_dataset.parquet --format tree

# For very large files, consider using limit parameter
opq view huge_file.parquet --limit 1000 --format ndjson > sample.jsonl
```

## Requirements

- Rust 1.89.0 or later
- macOS, Linux, or Windows

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request


## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Changelog

### v0.2.0
- **Breaking Changes**: Simplified CLI interface - removed `--file` flag, files are now positional arguments
- **New Feature**: Tree schema visualization with `--format tree`
- **New Feature**: Multiple file support for schema and metadata commands
- **Enhancement**: Arrow-based unified schema processing for consistent output
- **Enhancement**: Improved nested structure support with proper hierarchical display
- **Removed**: Vertical output format (use table format instead)
- **Fixed**: Command renamed from `meta` to `metadata` for clarity

### v0.1.0
- Initial release
- Support for ORC and Parquet files
- Multiple output formats (table, vertical, ndjson)
- Comprehensive compression support (gzip, zstd, snappy, zlib)
- Schema and metadata inspection
- Auto-detection of file types and compression formats
