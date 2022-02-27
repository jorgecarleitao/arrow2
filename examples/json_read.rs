use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow2::array::Array;
use arrow2::error::Result;
use arrow2::io::json::read;

fn read_path(path: &str) -> Result<Arc<dyn Array>> {
    // Example of reading a JSON file.
    let reader = BufReader::new(File::open(path)?);
    let json = serde_json::from_reader(reader)?;

    let data_type = read::infer(&json)?;

    read::deserialize(&json, data_type)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let batch = read_path(file_path)?;
    println!("{:#?}", batch);
    Ok(())
}
