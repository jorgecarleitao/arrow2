use std::fs::File;
use std::time::SystemTime;

use arrow2::error::Error;
use arrow2::io::parquet::read;

fn main() -> Result<(), Error> {
    // say we have a file
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let mut reader = File::open(file_path)?;

    // we can read its metadata:
    let metadata = read::read_metadata(&mut reader)?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = read::infer_schema(&metadata)?;

    // we can filter the columns we need (here we select all)
    let schema = schema.filter(|_index, _field| true);

    // we can read the statistics of all parquet's row groups (here for the first field)
    let statistics = read::statistics::deserialize(&schema.fields[0], &metadata.row_groups)?;

    println!("{:#?}", statistics);

    // and create an iterator of
    let reader = read::FileReader::new(reader, metadata, schema, Some(1024 * 8 * 8), None, None);

    let start = SystemTime::now();
    for maybe_chunk in reader {
        let chunk = maybe_chunk?;
        assert!(!chunk.is_empty());
    }
    println!("took: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())
}
