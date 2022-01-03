use std::fs::File;
use std::sync::Arc;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use arrow2::io::print;

fn read_batches(path: &str) -> Result<(Schema, Vec<Chunk<Arc<dyn Array>>>)> {
    let mut file = File::open(path)?;

    // read the files' metadata. At this point, we can distribute the read whatever we like.
    let metadata = read_file_metadata(&mut file)?;

    let schema = metadata.schema.clone();

    // Simplest way: use the reader, an iterator over batches.
    let reader = FileReader::new(file, metadata, None);

    let columns = reader.collect::<Result<Vec<_>>>()?;
    Ok((schema, columns))
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let (schema, batches) = read_batches(file_path)?;
    let names = schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>();
    println!("{}", print::write(&batches, &names));
    Ok(())
}
