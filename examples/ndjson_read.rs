use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::error::Result;
use arrow2::io::json::read;

fn read_path(path: &str, projection: Option<Vec<&str>>) -> Result<Chunk<Arc<dyn Array>>> {
    // Example of reading a NDJSON file.
    let mut reader = BufReader::new(File::open(path)?);

    let fields = read::infer_and_reset(&mut reader, None)?;

    let fields = if let Some(projection) = projection {
        fields
            .into_iter()
            .filter(|field| projection.contains(&field.name.as_ref()))
            .collect()
    } else {
        fields
    };

    // at most 1024 rows. This container can be re-used across batches.
    let mut rows = vec![String::default(); 1024];

    // Reads up to 1024 rows.
    // this is IO-intensive and performs minimal CPU work. In particular,
    // no deserialization is performed.
    let read = read::read_rows(&mut reader, &mut rows)?;
    let rows = &rows[..read];

    // deserialize `rows` into `Chunk`. This is CPU-intensive, has no IO,
    // and can be performed on a different thread pool via a channel.
    read::deserialize(rows, &fields)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let batch = read_path(file_path, None)?;
    println!("{:#?}", batch);
    Ok(())
}
