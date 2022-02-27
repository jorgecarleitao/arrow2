mod read;
mod write;

use std::sync::Arc;

use arrow2::array::*;
use arrow2::error::Result;
use arrow2::io::json::write as json_write;

fn write_batch(array: Box<dyn Array>) -> Result<Vec<u8>> {
    let mut serializer = json_write::Serializer::new(vec![Ok(array)].into_iter(), vec![]);

    let mut buf = vec![];
    json_write::write(&mut buf, &mut serializer);

    Ok(buf)
}
