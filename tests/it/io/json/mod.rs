mod read;
mod write;

use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::json::write as json_write;

fn write_batch<A: AsRef<dyn Array>>(
    batch: Chunk<A>,
    names: Vec<String>,
    format: json_write::Format,
) -> Result<Vec<u8>> {
    let batches = vec![Ok(batch)].into_iter();

    let blocks = json_write::Serializer::new(batches, names, vec![], format);

    let mut buf = Vec::new();
    json_write::write(&mut buf, format, blocks)?;
    Ok(buf)
}
