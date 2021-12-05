//! Example demonstrating how to read from parquet in parallel using rayon
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::SystemTime;

use rayon::prelude::*;

use arrow2::{
    error::Result, io::parquet::read, io::parquet::read::MutStreamingIterator,
    record_batch::RecordBatch,
};

fn parallel_read(path: &str, row_group: usize) -> Result<RecordBatch> {
    let mut file = BufReader::new(File::open(path)?);
    let file_metadata = read::read_metadata(&mut file)?;
    let arrow_schema = Arc::new(read::get_schema(&file_metadata)?);

    // IO-bounded
    let columns = file_metadata
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(field_i, field)| {
            let start = SystemTime::now();
            println!("read start - field: {}", field_i);
            let mut columns = read::get_column_iterator(
                &mut file,
                &file_metadata,
                row_group,
                field_i,
                None,
                vec![],
            );

            let mut column_chunks = vec![];
            while let read::State::Some(mut new_iter) = columns.advance().unwrap() {
                if let Some((pages, metadata)) = new_iter.get() {
                    let pages = pages.collect::<Vec<_>>();

                    column_chunks.push((pages, metadata.clone()));
                }
                columns = new_iter;
            }
            println!(
                "read end - {:?}: {} {}",
                start.elapsed().unwrap(),
                field_i,
                row_group
            );
            (field_i, field.clone(), column_chunks)
        })
        .collect::<Vec<_>>();

    // CPU-bounded
    let columns = columns
        .into_par_iter()
        .map(|(field_i, parquet_field, column_chunks)| {
            let columns = read::ReadColumnIterator::new(parquet_field, column_chunks);
            let field = &arrow_schema.fields()[field_i];

            read::column_iter_to_array(columns, field, vec![]).map(|x| x.0.into())
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(arrow_schema, columns)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let row_group = args[2].parse::<usize>().unwrap();

    let start = SystemTime::now();
    let batch = parallel_read(file_path, row_group)?;
    assert!(batch.num_rows() > 0);
    println!("took: {} ms", start.elapsed().unwrap().as_millis());

    Ok(())
}
