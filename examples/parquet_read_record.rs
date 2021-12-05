use std::fs::File;
use std::time::SystemTime;

use arrow2::error::Result;
use arrow2::io::parquet::read;

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let reader = File::open(file_path)?;
    let reader = read::RecordReader::try_new(reader, None, None, None, None)?;

    let start = SystemTime::now();
    for maybe_batch in reader {
        let batch = maybe_batch?;
        assert!(batch.num_rows() > 0);
    }
    println!("took: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())
}
