use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::avro::{write, CompressedBlock};
use arrow2::types::months_days_ns;

use super::read::read_avro;

pub(super) fn schema() -> Schema {
    Schema::from(vec![
        Field::new("int64", DataType::Int64, false),
        Field::new("int64 nullable", DataType::Int64, true),
        Field::new("utf8", DataType::Utf8, false),
        Field::new("utf8 nullable", DataType::Utf8, true),
        Field::new("int32", DataType::Int32, false),
        Field::new("int32 nullable", DataType::Int32, true),
        Field::new("date", DataType::Date32, false),
        Field::new("date nullable", DataType::Date32, true),
        Field::new("binary", DataType::Binary, false),
        Field::new("binary nullable", DataType::Binary, true),
        Field::new("float32", DataType::Float32, false),
        Field::new("float32 nullable", DataType::Float32, true),
        Field::new("float64", DataType::Float64, false),
        Field::new("float64 nullable", DataType::Float64, true),
        Field::new("boolean", DataType::Boolean, false),
        Field::new("boolean nullable", DataType::Boolean, true),
        Field::new(
            "interval",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
        Field::new(
            "interval nullable",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
    ])
}

pub(super) fn data() -> Chunk<Box<dyn Array>> {
    let columns = vec![
        Box::new(Int64Array::from_slice([27, 47])) as Box<dyn Array>,
        Box::new(Int64Array::from([Some(27), None])),
        Box::new(Utf8Array::<i32>::from_slice(["foo", "bar"])),
        Box::new(Utf8Array::<i32>::from([Some("foo"), None])),
        Box::new(Int32Array::from_slice([1, 1])),
        Box::new(Int32Array::from([Some(1), None])),
        Box::new(Int32Array::from_slice([1, 2]).to(DataType::Date32)),
        Box::new(Int32Array::from([Some(1), None]).to(DataType::Date32)),
        Box::new(BinaryArray::<i32>::from_slice([b"foo", b"bar"])),
        Box::new(BinaryArray::<i32>::from([Some(b"foo"), None])),
        Box::new(PrimitiveArray::<f32>::from_slice([1.0, 2.0])),
        Box::new(PrimitiveArray::<f32>::from([Some(1.0), None])),
        Box::new(PrimitiveArray::<f64>::from_slice([1.0, 2.0])),
        Box::new(PrimitiveArray::<f64>::from([Some(1.0), None])),
        Box::new(BooleanArray::from_slice([true, false])),
        Box::new(BooleanArray::from([Some(true), None])),
        Box::new(PrimitiveArray::<months_days_ns>::from_slice([
            months_days_ns::new(1, 1, 10 * 1_000_000), // 10 millis
            months_days_ns::new(2, 2, 20 * 1_000_000), // 20 millis
        ])),
        Box::new(PrimitiveArray::<months_days_ns>::from([
            Some(months_days_ns::new(1, 1, 10 * 1_000_000)), // 10 millis
            None,
        ])),
    ];

    Chunk::new(columns)
}

pub(super) fn serialize_to_block<R: AsRef<dyn Array>>(
    columns: &Chunk<R>,
    schema: &Schema,
    compression: Option<write::Compression>,
) -> Result<CompressedBlock> {
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

    Ok(compressed_block)
}

fn write_avro<R: AsRef<dyn Array>>(
    columns: &Chunk<R>,
    schema: &Schema,
    compression: Option<write::Compression>,
) -> Result<Vec<u8>> {
    let compressed_block = serialize_to_block(columns, schema, compression)?;

    let avro_fields = write::to_avro_schema(schema)?;
    let mut file = vec![];

    write::write_metadata(&mut file, avro_fields, compression)?;

    write::write_block(&mut file, &compressed_block)?;

    Ok(file)
}

fn roundtrip(compression: Option<write::Compression>) -> Result<()> {
    let expected = data();
    let expected_schema = schema();

    let data = write_avro(&expected, &expected_schema, compression)?;

    let (result, read_schema) = read_avro(&data, None)?;

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
