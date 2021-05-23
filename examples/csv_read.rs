use arrow2::error::Result;
use arrow2::io::csv::read;
use arrow2::record_batch::RecordBatch;

fn read_path(path: &str, projection: Option<&[usize]>) -> Result<RecordBatch> {
    // Create a CSV reader. This is typically created on the thread that reads the file and
    // thus owns the read head.
    let mut reader = read::ReaderBuilder::new().from_path(path)?;

    // Infers the schema using the default inferer. The inferer is just a function that maps a string
    // to a `DataType`.
    let schema = read::infer_schema(&mut reader, None, true, &read::infer)?;

    // skip 0 (excluding the header) and read up to 100 rows.
    // this is IO-intensive and performs minimal CPU work. In particular,
    // no deserialization is performed.
    let rows = read::read_rows(&mut reader, 0, 100)?;

    // parse the batches into a `RecordBatch`. This is CPU-intensive, has no IO,
    // and can be performed on a different thread by passing `rows` through a channel.
    read::deserialize_batch(
        &rows,
        schema.fields(),
        projection,
        0,
        read::deserialize_column,
    )
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let batch = read_path(&file_path, None)?;
    println!("{:?}", batch);
    Ok(())
}
