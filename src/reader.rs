use anyhow::{Error, Result};
use arrow::record_batch::RecordBatch;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;

pub enum FileType {
    Parquet,
    Orc,
}

pub fn get_file_type(file_path: &str) -> Result<FileType> {
    let path = Path::new(file_path);
    let extension = path.extension().and_then(|s| s.to_str());

    match extension {
        Some("parquet") => Ok(FileType::Parquet),
        Some("orc") => Ok(FileType::Orc),
        _ => Err(anyhow::Error::msg(format!(
            "Unsupported file type for file: {}",
            file_path
        ))),
    }
}

pub struct ArrowBatchIterator {
    batches: Vec<RecordBatch>,
    current_index: usize,
}

impl ArrowBatchIterator {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            current_index: 0,
        }
    }
}

impl Iterator for ArrowBatchIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index < self.batches.len() {
            let batch = self.batches[self.current_index].clone();
            self.current_index += 1;
            Some(Ok(batch))
        } else {
            None
        }
    }
}

pub fn read_parquet_to_arrow(file_path: &str) -> Result<ArrowBatchIterator> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let file = File::open(&path).map_err(Error::new)?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(Error::new)?;
    let reader = builder.build().map_err(Error::new)?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result.map_err(Error::new)?);
    }

    Ok(ArrowBatchIterator::new(batches))
}

pub fn read_orc_to_arrow(file_path: &str) -> Result<ArrowBatchIterator> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::Error::msg(format!("File not found: {}", file_path)));
    }

    let file = File::open(&path).map_err(Error::new)?;
    let reader_builder = ArrowReaderBuilder::try_new(file).map_err(Error::new)?;
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

    Ok(ArrowBatchIterator::new(batches))
}
