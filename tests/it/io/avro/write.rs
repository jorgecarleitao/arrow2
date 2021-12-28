use std::sync::Arc;

use arrow2::array::*;
use arrow2::columns::Columns;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::avro::write;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
        Field::new("date", DataType::Date32, false),
        Field::new("d", DataType::Binary, false),
        Field::new("e", DataType::Float64, false),
        Field::new("f", DataType::Boolean, false),
        Field::new("g", DataType::Utf8, true),
    ])
}

fn data() -> Columns<Arc<dyn Array>> {
    let columns = vec![
        Arc::new(Int64Array::from_slice([27, 47])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from_slice(["foo", "bar"])) as Arc<dyn Array>,
        Arc::new(Int32Array::from_slice([1, 1])) as Arc<dyn Array>,
        Arc::new(Int32Array::from_slice([1, 2]).to(DataType::Date32)) as Arc<dyn Array>,
        Arc::new(BinaryArray::<i32>::from_slice([b"foo", b"bar"])) as Arc<dyn Array>,
        Arc::new(PrimitiveArray::<f64>::from_slice([1.0, 2.0])) as Arc<dyn Array>,
        Arc::new(BooleanArray::from_slice([true, false])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from([Some("foo"), None])) as Arc<dyn Array>,
    ];

    Columns::try_new(columns).unwrap()
}

use super::read::read_avro;

fn write_avro<R: AsRef<dyn Array>>(
    columns: &Columns<R>,
    schema: &Schema,
    compression: Option<write::Compression>,
) -> Result<Vec<u8>> {
    let avro_fields = write::to_avro_schema(schema)?;

    let mut serializers = columns
        .arrays()
        .iter()
        .map(|x| x.as_ref())
        .zip(avro_fields.iter())
        .map(|(array, field)| write::new_serializer(array, &field.schema))
        .collect::<Vec<_>>();
    let mut block = write::Block::new(columns.len(), vec![]);

    write::serialize(&mut serializers, &mut block);

    let mut compressed_block = write::CompressedBlock::default();

    write::compress(&mut block, &mut compressed_block, compression)?;

    let mut file = vec![];

    write::write_metadata(&mut file, avro_fields.clone(), compression)?;

    write::write_block(&mut file, &compressed_block)?;

    Ok(file)
}

fn roundtrip(compression: Option<write::Compression>) -> Result<()> {
    let expected = data();
    let expected_schema = schema();

    let data = write_avro(&expected, &expected_schema, compression)?;

    let (result, read_schema) = read_avro(&data)?;

    assert_eq!(expected_schema, read_schema);
    for (c1, c2) in result.columns().iter().zip(expected.columns().iter()) {
        assert_eq!(c1.as_ref(), c2.as_ref());
    }
    Ok(())
}

#[test]
fn no_compression() -> Result<()> {
    roundtrip(None)
}

#[test]
fn snappy() -> Result<()> {
    roundtrip(Some(write::Compression::Snappy))
}

#[test]
fn deflate() -> Result<()> {
    roundtrip(Some(write::Compression::Deflate))
}
