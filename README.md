# OPQ (ORC Parquet Query)

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
- **Vertical** - MySQL-style vertical format (great for wide tables)
- **NDJSON** - Newline-delimited JSON format

### Commands
- **schema** - View file schema and structure
- **meta** - View file metadata and statistics
- **view** - View file contents with customizable output formats and column selection

### Advanced Features
- **Column Selection** - Choose specific columns for better performance and focused analysis
- **Streaming Processing** - Memory-efficient handling of large files
- **Auto-detection** - Automatic file type and compression format detection

## Installation

### From Source
```bash
git clone <repository-url>
cd opq
cargo build --release
```

The binary will be available at `target/release/opq`.

## Usage

### Basic Commands

#### View Schema
```bash
# View schema of an ORC file
opq schema --file data.orc

# View schema of a compressed Parquet file
opq schema --file data.parquet.gz
```

#### View Metadata
```bash
# View metadata of a Parquet file
opq meta --file data.parquet

# View metadata of a compressed ORC file
opq meta --file data.orc.zst
```

#### View Data Content
```bash
# View first 10 rows in table format (default)
opq view --file data.parquet

# View first 20 rows in vertical format
opq view --file data.orc --limit 20 --format vertical

# Export to NDJSON format
opq view --file data.parquet.gz --format ndjson --limit 100

# Select specific columns for better performance
opq view --file data.parquet --fields "id,name,email" --limit 10

# Single column selection
opq view --file data.parquet --fields "name" --format vertical --limit 5
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

#### Vertical Format
```
*************************** 1 ***************************
PassengerId: 1
   Survived: 0
     Pclass: 3
       Name: Braund, Mr. Owen Harris
        Sex: male
        Age: 22.0
*************************** 2 ***************************
PassengerId: 2
   Survived: 1
     Pclass: 1
       Name: Cumings, Mrs. John Bradley (Florence Briggs Thayer)
        Sex: female
        Age: 38.0
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
# Schema of gzip-compressed Parquet
opq schema --file sales_data.parquet.gz

# Metadata of zstd-compressed ORC
opq meta --file logs.orc.zst

# View snappy-compressed data in vertical format
opq view --file user_events.parquet.snappy --format vertical --limit 5
```

### Data Exploration Workflow
```bash
# 1. Check the schema first
opq schema --file dataset.parquet

# 2. View metadata for size info
opq meta --file dataset.parquet

# 3. Preview data in table format
opq view --file dataset.parquet --limit 10

# 4. Select specific columns for analysis
opq view --file dataset.parquet --fields "user_id,timestamp,event_type" --limit 20

# 5. For wide tables, use vertical format
opq view --file dataset.parquet --format vertical --limit 3

# 6. Export sample to JSON for further processing
opq view --file dataset.parquet --fields "id,name,email" --format ndjson --limit 1000 > sample.jsonl
```

## Supported Data Types

OPQ handles complex nested data structures including:
- Primitive types (integers, floats, strings, booleans)
- Nested structs
- Arrays and lists
- Maps and dictionaries
- Timestamp and date types

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
opq meta --file large_dataset.parquet

# Preview small sample before processing large amounts
opq view --file large_dataset.parquet --limit 10

# For very large files, consider using limit parameter
opq view --file huge_file.parquet --limit 1000 --format ndjson > sample.jsonl
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

[Add your license information here]

## Changelog

### v0.1.0
- Initial release
- Support for ORC and Parquet files
- Multiple output formats (table, vertical, ndjson)
- Comprehensive compression support (gzip, zstd, snappy, zlib)
- Schema and metadata inspection
- Auto-detection of file types and compression formats
