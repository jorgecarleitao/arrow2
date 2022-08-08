use std::fs::File;
use std::time::SystemTime;

use arrow2::error::Result;
use arrow2::io::parquet::read;

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let mut reader = File::open(file_path)?;

    let metadata = read::read_metadata(&mut reader)?;
    let reader = read::FileReader::try_new(reader, metadata, None, Some(1024 * 8 * 8), None, None)?;

    println!("{:#?}", reader.schema());

    // say we want to evaluate if the we can skip some row groups based on a field's value
    let field = &reader.schema().fields[0];

    // we can deserialize the parquet statistics from this field
    let statistics = read::statistics::deserialize(field, &reader.metadata().row_groups)?;

    println!("{:#?}", statistics);

    let start = SystemTime::now();
    for maybe_chunk in reader {
        let chunk = maybe_chunk?;
        assert!(!chunk.is_empty());
    }
    println!("took: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())
}
