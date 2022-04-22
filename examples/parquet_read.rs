use std::fs::File;
use std::time::SystemTime;

use arrow2::error::Result;
use arrow2::io::parquet::read;

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let reader = File::open(file_path)?;
    let reader = read::FileReader::try_new(reader, Some(&[8]), None, None, None)?;

    println!("{:#?}", reader.metadata());

    let start = SystemTime::now();
    for maybe_chunk in reader {
        let columns = maybe_chunk?;
        assert!(!columns.is_empty());
    }
    println!("took: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())
}
