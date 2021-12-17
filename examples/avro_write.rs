use std::{fs::File, sync::Arc};

use arrow2::{
    array::{Array, Int32Array},
    datatypes::{Field, Schema},
    error::Result,
    io::avro::write,
    record_batch::RecordBatch,
};

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let path = &args[1];

    let array = Int32Array::from(&[
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]);
    let field = Field::new("c1", array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);

    let avro_schema = write::to_avro_schema(&schema)?;

    let mut file = File::create(path)?;

    let compression = None;

    write::write_metadata(&mut file, &avro_schema, compression)?;

    let serializer = write::new_serializer(&array, avro_schema.fields()[0]);
    let mut block = write::Block::new(array.len(), vec![]);

    write::serialize(&mut vec![serializer], &mut block)?;

    let mut compressed_block = write::CompressedBlock::default();

    if let Some(compression) = compression {
        write::compress(&block, &mut compressed_block, compression)?;
    } else {
        compressed_block.number_of_rows = block.number_of_rows;
        std::mem::swap(&mut compressed_block.data, &mut block.data);
    }

    write::write_block(&mut file, &compressed_block)?;

    Ok(())
}
