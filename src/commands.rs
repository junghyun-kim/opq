use crate::cli::{OutputFormat, SchemaFormat};
use crate::output::print_arrow_batches;
use crate::reader::{
    ArrowBatchIterator, FileType, get_file_type, get_orc_metadata, get_parquet_metadata,
    read_orc_to_arrow_with_projection, read_parquet_to_arrow_with_projection,
};
use crate::schema::show_schema;
use crate::sort::{parse_sort_specs, validate_sort_columns};
use anyhow::Result;
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{DataFrame, SessionContext, col};
use futures::StreamExt;

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
pub async fn handle_view_command(
    file: &str,
    columns: &Option<Vec<String>>,
    limit: usize,
    format: &OutputFormat,
    truncate: usize,
    sort: &Option<Vec<String>>,
) -> Result<()> {
    // Apply sorting if specified - use adaptive strategy
    if let Some(sort_args) = sort {
        if !sort_args.is_empty() {
            return handle_sorted_view(file, columns, limit, format, truncate, sort_args).await;
        }
    }

    // No sorting - use original implementation
    let file_type = get_file_type(file)?;
    let batch_iterator = match file_type {
        FileType::Parquet => read_parquet_to_arrow_with_projection(file, columns.clone())?,
        FileType::Orc => read_orc_to_arrow_with_projection(file, columns.clone())?,
    };

    let output_format = match format {
        OutputFormat::Table => crate::output::OutputFormat::Table,
        OutputFormat::Ndjson => crate::output::OutputFormat::Ndjson,
    };

    print_arrow_batches(batch_iterator, limit, &output_format, truncate)?;
    Ok(())
}

/// Handle sorted view with adaptive strategy (TopK vs External Sort)
async fn handle_sorted_view(
    file: &str,
    columns: &Option<Vec<String>>,
    limit: usize,
    format: &OutputFormat,
    truncate: usize,
    sort_args: &[String],
) -> Result<()> {
    // Parse sort specifications
    let sort_specs = parse_sort_specs(sort_args)?;

    // Determine strategy based on limit and file characteristics
    let use_topk = should_use_topk(limit, &sort_specs);

    if use_topk {
        handle_topk_sort(file, columns, limit, format, truncate, sort_args).await
    } else {
        handle_external_sort(file, columns, limit, format, truncate, sort_args).await
    }
}

/// Determine whether to use TopK optimization
fn should_use_topk(limit: usize, sort_specs: &[crate::sort::SortSpec]) -> bool {
    // Use TopK if:
    // 1. Limit is relatively small (< 1000)
    // 2. Single column sort (TopK works best with single column)
    limit < 1000 && sort_specs.len() == 1
}

/// Handle TopK sorting for small limits
async fn handle_topk_sort(
    file: &str,
    columns: &Option<Vec<String>>,
    limit: usize,
    format: &OutputFormat,
    truncate: usize,
    sort_args: &[String],
) -> Result<()> {
    let file_type = get_file_type(file)?;
    let batch_iterator = match file_type {
        FileType::Parquet => read_parquet_to_arrow_with_projection(file, columns.clone())?,
        FileType::Orc => read_orc_to_arrow_with_projection(file, columns.clone())?,
    };

    // Convert iterator to vector for TopK sorting
    let batches: Vec<RecordBatch> = batch_iterator.collect::<Result<Vec<_>, _>>()?;

    // Apply TopK sorting
    let final_batches = apply_topk_sorting(batches, sort_args, columns.as_deref(), limit)?;

    let output_format = match format {
        OutputFormat::Table => crate::output::OutputFormat::Table,
        OutputFormat::Ndjson => crate::output::OutputFormat::Ndjson,
    };

    // Convert back to iterator for output
    let result_iterator = ArrowBatchIterator::new_from_batches(final_batches);
    print_arrow_batches(result_iterator, limit, &output_format, truncate)?;
    Ok(())
}

/// Handle external sorting using DataFusion for large files (both Parquet and ORC)
async fn handle_external_sort(
    file: &str,
    columns: &Option<Vec<String>>,
    limit: usize,
    format: &OutputFormat,
    truncate: usize,
    sort_args: &[String],
) -> Result<()> {
    use datafusion::prelude::*;

    let ctx = SessionContext::new();

    // Create DataFrame based on file type
    let mut df = match get_file_type(file)? {
        FileType::Parquet => {
            ctx.read_parquet(file, ParquetReadOptions::default())
                .await?
        }
        FileType::Orc => {
            // For ORC files, use streaming approach to avoid loading all data into memory
            return handle_orc_external_sort_streaming(
                file, columns, limit, format, truncate, sort_args,
            )
            .await;
        }
    };

    // Apply column selection if specified
    if let Some(cols) = columns {
        let col_exprs: Vec<Expr> = cols.iter().map(|c| col(c)).collect();
        df = df.select(col_exprs)?;
    }

    // Apply sorting using DataFrame API
    let sort_specs = parse_sort_specs(sort_args)?;
    if !sort_specs.is_empty() {
        let sort_exprs = sort_specs
            .iter()
            .map(|spec| {
                col(&spec.column).sort(spec.ascending, false) // ascending, nulls_first
            })
            .collect();
        df = df.sort(sort_exprs)?;
    }

    // Apply limit if specified
    if limit > 0 {
        df = df.limit(0, Some(limit))?;
    }

    // Use streaming execution instead of collect() to avoid loading all data into memory
    let output_format = match format {
        OutputFormat::Table => crate::output::OutputFormat::Table,
        OutputFormat::Ndjson => crate::output::OutputFormat::Ndjson,
    };

    // Stream processing for memory efficiency
    execute_dataframe_streaming(&ctx, df, limit, &output_format, truncate).await?;
    Ok(())
}

/// Execute DataFrame using streaming to avoid loading all data into memory
async fn execute_dataframe_streaming(
    ctx: &SessionContext,
    df: DataFrame,
    limit: usize,
    output_format: &crate::output::OutputFormat,
    truncate: usize,
) -> Result<()> {
    // Create execution plan
    let execution_plan = df.create_physical_plan().await?;
    let task_ctx = ctx.task_ctx();

    // Execute streaming
    let mut stream = execution_plan.execute(0, task_ctx)?;

    let mut processed_rows = 0;
    let mut batches = Vec::new();

    // Process batches in streaming fashion
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        let batch_rows = batch.num_rows();

        // Check if we need to limit the batch
        if limit > 0 && processed_rows + batch_rows > limit {
            let remaining = limit - processed_rows;
            if remaining > 0 {
                // Take only the needed rows from this batch
                let indices: Vec<u32> = (0..remaining).map(|i| i as u32).collect();
                let indices_array = arrow::array::UInt32Array::from(indices);

                let limited_columns: Result<Vec<_>, _> = batch
                    .columns()
                    .iter()
                    .map(|column| arrow::compute::take(column.as_ref(), &indices_array, None))
                    .collect();

                let limited_batch =
                    arrow::record_batch::RecordBatch::try_new(batch.schema(), limited_columns?)?;
                batches.push(limited_batch);
            }
            break;
        }

        batches.push(batch);
        processed_rows += batch_rows;

        // For very large results, we could process and output batches immediately
        // instead of collecting them all. For now, we collect for compatibility
        // with the existing print_arrow_batches function.

        if limit > 0 && processed_rows >= limit {
            break;
        }
    }

    // Convert to iterator and print
    let result_iterator = ArrowBatchIterator::new_from_batches(batches);
    print_arrow_batches(result_iterator, limit, output_format, truncate)?;

    Ok(())
}

/// Apply TopK sorting to record batches
fn apply_topk_sorting(
    batches: Vec<RecordBatch>,
    sort_args: &[String],
    selected_columns: Option<&[String]>,
    k: usize,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }

    // Parse sort specifications
    let sort_specs = parse_sort_specs(sort_args)?;

    // Validate sort columns
    let all_columns: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    validate_sort_columns(&sort_specs, selected_columns, &all_columns)?;

    // For TopK, we use the first sort column only
    let spec = &sort_specs[0];
    let mut sorted_batches = Vec::new();

    for batch in batches {
        let column_index = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == &spec.column)
            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found", spec.column))?;

        let sort_column = batch.column(column_index);
        let sort_options = SortOptions {
            descending: !spec.ascending,
            nulls_first: false,
        };

        // TopK optimization not available in this Arrow version
        // Use regular sorting and take first k elements

        // Note: Arrow's top_k function might not be available in this version
        // Fall back to full sort and take first k elements
        let indices = lexsort_to_indices(
            &[SortColumn {
                values: sort_column.clone(),
                options: Some(sort_options),
            }],
            None,
        )?;

        // Take only first k elements
        let k_to_take = std::cmp::min(k, indices.len());
        let limited_indices = indices.slice(0, k_to_take);

        // Apply sorting to all columns
        let sorted_columns: Result<Vec<_>, _> = batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &limited_indices, None))
            .collect();

        let sorted_columns = sorted_columns?;
        let sorted_batch = RecordBatch::try_new(batch.schema(), sorted_columns)?;
        sorted_batches.push(sorted_batch);
    }

    Ok(sorted_batches)
}

/// Handle ORC external sorting using streaming approach to avoid memory issues
/// Since DataFusion doesn't natively support ORC streaming, we implement
/// a chunked processing strategy with temporary DataFrames
async fn handle_orc_external_sort_streaming(
    file: &str,
    columns: &Option<Vec<String>>,
    limit: usize,
    format: &OutputFormat,
    truncate: usize,
    sort_args: &[String],
) -> Result<()> {
    use datafusion::prelude::*;

    let sort_specs = parse_sort_specs(sort_args)?;
    let ctx = SessionContext::new();

    // Strategy: Process ORC file in chunks to avoid loading everything into memory
    let batch_iterator = read_orc_to_arrow_with_projection(file, None)?;

    let output_format = match format {
        OutputFormat::Table => crate::output::OutputFormat::Table,
        OutputFormat::Ndjson => crate::output::OutputFormat::Ndjson,
    };

    // For very large ORC files, we need to implement chunk-based external sorting
    const CHUNK_SIZE: usize = 100_000; // Process 100k rows at a time

    let mut chunk_results = Vec::new();
    let mut current_chunk = Vec::new();
    let mut current_chunk_size = 0;

    // Process ORC file in chunks
    for batch_result in batch_iterator {
        let batch = batch_result?;
        let batch_rows = batch.num_rows();

        current_chunk.push(batch);
        current_chunk_size += batch_rows;

        // When chunk is large enough, process it
        if current_chunk_size >= CHUNK_SIZE {
            let chunk_result =
                process_orc_chunk(&ctx, current_chunk, columns, &sort_specs, limit).await?;

            if let Some(result) = chunk_result {
                chunk_results.push(result);
            }

            current_chunk = Vec::new();
            current_chunk_size = 0;

            // Early termination if we have enough results
            if limit > 0
                && chunk_results
                    .iter()
                    .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
                    .sum::<usize>()
                    >= limit
            {
                break;
            }
        }
    }

    // Process remaining chunk
    if !current_chunk.is_empty() {
        let chunk_result =
            process_orc_chunk(&ctx, current_chunk, columns, &sort_specs, limit).await?;

        if let Some(result) = chunk_result {
            chunk_results.push(result);
        }
    }

    // Merge sorted chunks and apply final sorting
    if chunk_results.is_empty() {
        return Ok(());
    }

    // Flatten all chunk results
    let all_batches: Vec<RecordBatch> = chunk_results.into_iter().flatten().collect();

    if all_batches.is_empty() {
        return Ok(());
    }

    // Create final DataFrame from all chunks and apply final sort
    let final_df = ctx.read_batches(all_batches)?;

    // Apply final sorting
    let mut sorted_df = final_df;
    if !sort_specs.is_empty() {
        let sort_exprs = sort_specs
            .iter()
            .map(|spec| col(&spec.column).sort(spec.ascending, false))
            .collect();
        sorted_df = sorted_df.sort(sort_exprs)?;
    }

    // Apply final limit
    if limit > 0 {
        sorted_df = sorted_df.limit(0, Some(limit))?;
    }

    // Execute final result with streaming
    execute_dataframe_streaming(&ctx, sorted_df, limit, &output_format, truncate).await?;

    Ok(())
}

/// Process a chunk of ORC data
async fn process_orc_chunk(
    ctx: &SessionContext,
    chunk_batches: Vec<RecordBatch>,
    columns: &Option<Vec<String>>,
    sort_specs: &[crate::sort::SortSpec],
    limit: usize,
) -> Result<Option<Vec<RecordBatch>>> {
    if chunk_batches.is_empty() {
        return Ok(None);
    }

    // Create DataFrame from chunk
    let mut chunk_df = ctx.read_batches(chunk_batches)?;

    // Apply column selection
    if let Some(cols) = columns {
        let col_exprs: Vec<datafusion::logical_expr::Expr> = cols.iter().map(|c| col(c)).collect();
        chunk_df = chunk_df.select(col_exprs)?;
    }

    // Apply sorting to chunk
    if !sort_specs.is_empty() {
        let sort_exprs = sort_specs
            .iter()
            .map(|spec| col(&spec.column).sort(spec.ascending, false))
            .collect();
        chunk_df = chunk_df.sort(sort_exprs)?;
    }

    // Apply limit to chunk (take more than needed for final merge)
    // We fetch twice the requested limit per chunk to ensure that, after sorting and merging,
    // we have enough records to satisfy the final limit. This helps account for possible
    // overlaps or ordering changes across chunks during the merge phase.
    let chunk_limit = if limit > 0 { limit * 2 } else { 0 };
    if chunk_limit > 0 {
        chunk_df = chunk_df.limit(0, Some(chunk_limit))?;
    }

    // Execute chunk with streaming
    let execution_plan = chunk_df.create_physical_plan().await?;
    let task_ctx = ctx.task_ctx();
    let mut stream = execution_plan.execute(0, task_ctx)?;

    let mut result_batches = Vec::new();
    use futures::StreamExt;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        result_batches.push(batch);
    }

    if result_batches.is_empty() {
        Ok(None)
    } else {
        Ok(Some(result_batches))
    }
}
