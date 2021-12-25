use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow2::error::Result;
use arrow2::io::avro::read;

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let path = &args[1];

    let file = &mut BufReader::new(File::open(path)?);

    let (avro_schema, schema, codec, file_marker) = read::read_metadata(file)?;

    println!("{:#?}", avro_schema);

    let reader = read::Reader::new(
        read::Decompressor::new(read::BlockStreamIterator::new(file, file_marker), codec),
        avro_schema,
        Arc::new(schema),
    );

    for batch in reader {
        let batch = batch?;
        assert!(batch.len() > 0);
    }
    Ok(())
}
