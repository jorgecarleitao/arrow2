use std::fs::File;

use arrow2::{
    array::{Array, Int32Array},
    datatypes::{Field, Schema},
    error::Result,
    io::avro::write,
};

fn write_avro<W: std::io::Write>(
    file: &mut W,
    arrays: &[&dyn Array],
    schema: &Schema,
    compression: Option<write::Compression>,
) -> Result<()> {
    let avro_fields = write::to_avro_schema(schema)?;

    let mut serializers = arrays
        .iter()
        .zip(avro_fields.iter())
        .map(|(array, field)| write::new_serializer(*array, &field.schema))
        .collect::<Vec<_>>();
    let mut block = write::Block::new(arrays[0].len(), vec![]);

    write::serialize(&mut serializers, &mut block);

    let mut compressed_block = write::CompressedBlock::default();

    let _was_compressed = write::compress(&mut block, &mut compressed_block, compression)?;

    write::write_metadata(file, avro_fields.clone(), compression)?;

    write::write_block(file, &compressed_block)?;

    Ok(())
}

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

    let mut file = File::create(path)?;
    write_avro(&mut file, &[(&array) as &dyn Array], &schema, None)?;

    Ok(())
}
