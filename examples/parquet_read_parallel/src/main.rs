//! Example demonstrating how to read from parquet in parallel using rayon
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::SystemTime;

use rayon::prelude::*;

use arrow2::{array::Array, chunk::Chunk, error::Result, io::parquet::read};

fn parallel_read(path: &str, row_group: usize) -> Result<Chunk<Arc<dyn Array>>> {
    let mut file = BufReader::new(File::open(path)?);
    let metadata = read::read_metadata(&mut file)?;
    let schema = read::infer_schema(&metadata)?;

    // read (IO-bounded) all columns into memory (use a subset of the fields to project)
    let columns = read::read_columns(
        &mut file,
        &metadata.row_groups[row_group],
        schema.fields,
        None,
    )?;

    // CPU-bounded
    let columns = columns
        .into_par_iter()
        .map(|mut iter| {
            // when chunk_size != None, `iter` must be iterated multiple times to get all the chunks
            // see the implementation of `arrow2::io::parquet::read::RowGroupDeserializer::next`
            // to see how this can be done.
            iter.next().unwrap()
        })
        .collect::<Result<Vec<_>>>()?;

    Chunk::try_new(columns)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let row_group = args[2].parse::<usize>().unwrap();

    let start = SystemTime::now();
    let batch = parallel_read(file_path, row_group)?;
    assert!(!batch.is_empty());
    println!("took: {} ms", start.elapsed().unwrap().as_millis());

    Ok(())
}
