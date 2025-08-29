use anyhow::{Error, Result};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use flate2::read::{GzDecoder, ZlibDecoder};
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask as OrcProjectionMask;
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use snap::read::FrameDecoder as SnappyDecoder;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use zstd::stream::read::Decoder as ZstdDecoder;

pub enum FileType {
    Parquet,
    Orc,
}

#[derive(Debug, Clone)]
pub enum CompressionType {
    None,
    Gzip,
    Zlib,
    Snappy,
    Zstd,
}

pub fn detect_compression_from_extension(file_path: &str) -> CompressionType {
    let path = Path::new(file_path);
    let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

    if file_name.ends_with(".gz") {
        CompressionType::Gzip
    } else if file_name.ends_with(".zlib") || file_name.ends_with(".z") {
        CompressionType::Zlib
    } else if file_name.ends_with(".snappy") || file_name.ends_with(".sz") {
        CompressionType::Snappy
    } else if file_name.ends_with(".zst") || file_name.ends_with(".zstd") {
        CompressionType::Zstd
    } else {
        CompressionType::None
    }
}

pub fn get_file_type(file_path: &str) -> Result<FileType> {
    let path = Path::new(file_path);
    let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

    // Remove compression extensions to get the actual file type
    let mut core_name = file_name;

    // Remove each compression extension one by one
    if let Some(name) = core_name.strip_suffix(".gz") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".zlib") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".z") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".snappy") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".sz") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".zst") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".zstd") {
        core_name = name;
    }

    if core_name.ends_with(".parquet") {
        Ok(FileType::Parquet)
    } else if core_name.ends_with(".orc") {
        Ok(FileType::Orc)
    } else {
        Err(anyhow::Error::msg(format!(
            "Unsupported file type for file: {}",
            file_path
        )))
    }
}

pub struct ArrowBatchIterator {
    reader: Box<dyn Iterator<Item = Result<RecordBatch>>>,
}

impl ArrowBatchIterator {
    pub fn new_from_batches(batches: Vec<RecordBatch>) -> Self {
        let iter = batches.into_iter().map(Ok);
        Self {
            reader: Box::new(iter),
        }
    }

    pub fn new_from_parquet_reader(
        reader: impl Iterator<Item = arrow::error::Result<RecordBatch>> + 'static,
    ) -> Self {
        let iter = reader.map(|batch| batch.map_err(Error::new));
        Self {
            reader: Box::new(iter),
        }
    }
}

impl Iterator for ArrowBatchIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

fn create_decompressed_reader(file_path: &str) -> Result<Box<dyn Read>> {
    let file = File::open(file_path).map_err(Error::new)?;
    let compression = detect_compression_from_extension(file_path);

    match compression {
        CompressionType::None => Ok(Box::new(BufReader::new(file))),
        CompressionType::Gzip => Ok(Box::new(GzDecoder::new(BufReader::new(file)))),
        CompressionType::Zlib => Ok(Box::new(ZlibDecoder::new(BufReader::new(file)))),
        CompressionType::Snappy => Ok(Box::new(SnappyDecoder::new(BufReader::new(file)))),
        CompressionType::Zstd => Ok(Box::new(
            ZstdDecoder::new(BufReader::new(file)).map_err(Error::new)?,
        )),
    }
}

pub fn read_parquet_to_arrow_with_projection(
    file_path: &str,
    columns: Option<Vec<String>>,
) -> Result<ArrowBatchIterator> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let compression = detect_compression_from_extension(file_path);

    match compression {
        CompressionType::None => {
            // Read uncompressed file directly - streaming
            let file = File::open(&path).map_err(Error::new)?;
            let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(Error::new)?;

            // Apply column projection if specified
            if let Some(column_names) = columns {
                let schema = builder.schema();
                let mut column_indices = Vec::new();

                for column_name in &column_names {
                    if let Some(index) =
                        schema.fields().iter().position(|f| f.name() == column_name)
                    {
                        column_indices.push(index);
                    } else {
                        return Err(anyhow::Error::msg(format!(
                            "Column '{}' not found in schema",
                            column_name
                        )));
                    }
                }

                let projection = ProjectionMask::roots(builder.parquet_schema(), column_indices);
                builder = builder.with_projection(projection);
            }

            let reader = builder.build().map_err(Error::new)?;
            Ok(ArrowBatchIterator::new_from_parquet_reader(reader))
        }
        _ => {
            // For compressed files, we still need to decompress to memory
            // This is a limitation of current Parquet/ORC libraries
            let mut decompressed_reader = create_decompressed_reader(file_path)?;
            let mut buffer = Vec::new();
            decompressed_reader
                .read_to_end(&mut buffer)
                .map_err(Error::new)?;

            let bytes = Bytes::from(buffer);
            let mut builder =
                ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(Error::new)?;

            // Apply column projection if specified
            if let Some(column_names) = columns {
                let schema = builder.schema();
                let mut column_indices = Vec::new();

                for column_name in &column_names {
                    if let Some(index) =
                        schema.fields().iter().position(|f| f.name() == column_name)
                    {
                        column_indices.push(index);
                    } else {
                        return Err(anyhow::Error::msg(format!(
                            "Column '{}' not found in schema",
                            column_name
                        )));
                    }
                }

                let projection = ProjectionMask::roots(builder.parquet_schema(), column_indices);
                builder = builder.with_projection(projection);
            }

            let reader = builder.build().map_err(Error::new)?;
            Ok(ArrowBatchIterator::new_from_parquet_reader(reader))
        }
    }
}

pub fn read_orc_to_arrow_with_projection(
    file_path: &str,
    columns: Option<Vec<String>>,
) -> Result<ArrowBatchIterator> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let compression = detect_compression_from_extension(file_path);

    match compression {
        CompressionType::None => {
            // Read uncompressed file directly
            let file = File::open(&path).map_err(Error::new)?;
            let reader_builder = ArrowReaderBuilder::try_new(file).map_err(Error::new)?;

            // Apply column projection if specified
            let reader_builder = if let Some(column_names) = columns {
                let file_metadata = reader_builder.file_metadata();
                let root_data_type = file_metadata.root_data_type();
                let projection = OrcProjectionMask::named_roots(root_data_type, &column_names);
                reader_builder.with_projection(projection)
            } else {
                reader_builder
            };

            let mut reader = reader_builder.build();

            let mut batches = Vec::new();
            loop {
                let batch_result = reader.next();
                let batch = match batch_result {
                    Some(b) => b.map_err(Error::new)?,
                    None => break,
                };
                batches.push(batch);
            }

            Ok(ArrowBatchIterator::new_from_batches(batches))
        }
        _ => {
            // For compressed files, we need to decompress first and create a temporary buffer
            let mut decompressed_reader = create_decompressed_reader(file_path)?;
            let mut buffer = Vec::new();
            decompressed_reader
                .read_to_end(&mut buffer)
                .map_err(Error::new)?;

            let bytes = Bytes::from(buffer);
            let reader_builder = ArrowReaderBuilder::try_new(bytes).map_err(Error::new)?;

            // Apply column projection if specified
            let reader_builder = if let Some(column_names) = columns {
                let file_metadata = reader_builder.file_metadata();
                let root_data_type = file_metadata.root_data_type();
                let projection = OrcProjectionMask::named_roots(root_data_type, &column_names);
                reader_builder.with_projection(projection)
            } else {
                reader_builder
            };

            let mut reader = reader_builder.build();

            let mut batches = Vec::new();
            loop {
                let batch_result = reader.next();
                let batch = match batch_result {
                    Some(b) => b.map_err(Error::new)?,
                    None => break,
                };
                batches.push(batch);
            }

            Ok(ArrowBatchIterator::new_from_batches(batches))
        }
    }
}

use parquet::file::reader::{FileReader, SerializedFileReader};

pub fn get_parquet_schema(file_path: &str) -> Result<String> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let compression = detect_compression_from_extension(file_path);

    match compression {
        CompressionType::None => {
            // Read uncompressed file directly
            let file = File::open(&path).map_err(Error::new)?;
            let reader = SerializedFileReader::new(file).map_err(Error::new)?;
            let metadata = reader.metadata();
            let schema = metadata.file_metadata().schema();
            Ok(format!("{:#?}", schema))
        }
        _ => {
            // For compressed files, decompress first
            let mut decompressed_reader = create_decompressed_reader(file_path)?;
            let mut buffer = Vec::new();
            decompressed_reader
                .read_to_end(&mut buffer)
                .map_err(Error::new)?;

            let bytes = Bytes::from(buffer);
            let reader = SerializedFileReader::new(bytes).map_err(Error::new)?;
            let metadata = reader.metadata();
            let schema = metadata.file_metadata().schema();
            Ok(format!("{:#?}", schema))
        }
    }
}

pub fn get_orc_schema(file_path: &str) -> Result<String> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let compression = detect_compression_from_extension(file_path);

    match compression {
        CompressionType::None => {
            // Read uncompressed file directly
            let file = File::open(&path).map_err(Error::new)?;
            let reader_builder = ArrowReaderBuilder::try_new(file).map_err(Error::new)?;
            let schema = reader_builder.schema();
            Ok(format!("{:#?}", schema))
        }
        _ => {
            // For compressed files, decompress first
            let mut decompressed_reader = create_decompressed_reader(file_path)?;
            let mut buffer = Vec::new();
            decompressed_reader
                .read_to_end(&mut buffer)
                .map_err(Error::new)?;

            let bytes = Bytes::from(buffer);
            let reader_builder = ArrowReaderBuilder::try_new(bytes).map_err(Error::new)?;
            let schema = reader_builder.schema();
            Ok(format!("{:#?}", schema))
        }
    }
}

pub fn get_parquet_metadata(file_path: &str) -> Result<()> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let compression = detect_compression_from_extension(file_path);

    match compression {
        CompressionType::None => {
            // Read uncompressed file directly
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
        _ => {
            // For compressed files, decompress first
            let mut decompressed_reader = create_decompressed_reader(file_path)?;
            let mut buffer = Vec::new();
            decompressed_reader
                .read_to_end(&mut buffer)
                .map_err(Error::new)?;

            let bytes = Bytes::from(buffer);
            let reader = SerializedFileReader::new(bytes).map_err(Error::new)?;
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
    }
}

pub fn get_orc_metadata(file_path: &str) -> Result<()> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let compression = detect_compression_from_extension(file_path);

    match compression {
        CompressionType::None => {
            // Read uncompressed file directly
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
        _ => {
            // For compressed files, decompress first
            let mut decompressed_reader = create_decompressed_reader(file_path)?;
            let mut buffer = Vec::new();
            decompressed_reader
                .read_to_end(&mut buffer)
                .map_err(Error::new)?;

            let bytes = Bytes::from(buffer);
            let reader_builder = ArrowReaderBuilder::try_new(bytes).map_err(Error::new)?;
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
    }
}
