use std::fs::File;

use arrow2::error::Result;
use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use arrow2::record_batch::RecordBatch;

fn read_batches(path: &str) -> Result<Vec<RecordBatch>> {
    let mut file = File::open(path)?;

    // read the files' metadata. At this point, we can distribute the read whatever we like.
    let metadata = read_file_metadata(&mut file)?;

    // Simplest way: use the reader, an iterator over batches.
    let reader = FileReader::new(&mut file, metadata);

    reader.collect()
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let batches = read_batches(file_path)?;
    println!("{:?}", batches);
    Ok(())
}
