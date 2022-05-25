use std::io::Cursor;

use arrow2::error::Result;
use arrow2::io::parquet::write::*;

use super::*;

fn round_trip(
    column: &str,
    file: &str,
    version: Version,
    compression: CompressionOptions,
    encodings: Vec<Encoding>,
) -> Result<()> {
    let (array, statistics) = match file {
        "nested" => (
            pyarrow_nested_nullable(column),
            pyarrow_nested_nullable_statistics(column),
        ),
        "nullable" => (
            pyarrow_nullable(column),
            pyarrow_nullable_statistics(column),
        ),
        "required" => (
            pyarrow_required(column),
            pyarrow_required_statistics(column),
        ),
        "struct" => (pyarrow_struct(column), pyarrow_struct_statistics(column)),
        _ => unreachable!(),
    };
    let array: Arc<dyn Array> = array.into();

    let field = Field::new("a1", array.data_type().clone(), true);
    let schema = Schema::from(vec![field]);

    let options = WriteOptions {
        write_statistics: true,
        compression,
        version,
    };

    let iter = vec![Chunk::try_new(vec![array.clone()])];

    let row_groups =
        RowGroupIterator::try_new(iter.into_iter(), &schema, options, vec![encodings])?;

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::try_new(writer, schema, options)?;

    writer.start()?;
    for group in row_groups {
        writer.write(group?)?;
    }
    writer.end(None)?;

    let data = writer.into_inner().into_inner();

    let (result, stats) = read_column(&mut Cursor::new(data), "a1")?;
    assert_eq!(array.as_ref(), result.as_ref());
    assert_eq!(statistics, stats);
    Ok(())
}

#[test]
fn int64_optional_v1() -> Result<()> {
    round_trip(
        "int64",
        "nullable",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn int64_required_v1() -> Result<()> {
    round_trip(
        "int64",
        "required",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn int64_optional_v2() -> Result<()> {
    round_trip(
        "int64",
        "nullable",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[cfg(feature = "io_parquet_compression")]
#[test]
fn int64_optional_v2_compressed() -> Result<()> {
    round_trip(
        "int64",
        "nullable",
        Version::V2,
        CompressionOptions::Snappy,
        vec![Encoding::Plain],
    )
}

#[test]
fn utf8_optional_v1() -> Result<()> {
    round_trip(
        "string",
        "nullable",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn utf8_required_v1() -> Result<()> {
    round_trip(
        "string",
        "required",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn utf8_optional_v2() -> Result<()> {
    round_trip(
        "string",
        "nullable",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn utf8_required_v2() -> Result<()> {
    round_trip(
        "string",
        "required",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[cfg(feature = "io_parquet_compression")]
#[test]
fn utf8_optional_v2_compressed() -> Result<()> {
    round_trip(
        "string",
        "nullable",
        Version::V2,
        CompressionOptions::Snappy,
        vec![Encoding::Plain],
    )
}

#[cfg(feature = "io_parquet_compression")]
#[test]
fn utf8_required_v2_compressed() -> Result<()> {
    round_trip(
        "string",
        "required",
        Version::V2,
        CompressionOptions::Snappy,
        vec![Encoding::Plain],
    )
}

#[test]
fn bool_optional_v1() -> Result<()> {
    round_trip(
        "bool",
        "nullable",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn bool_required_v1() -> Result<()> {
    round_trip(
        "bool",
        "required",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn bool_optional_v2_uncompressed() -> Result<()> {
    round_trip(
        "bool",
        "nullable",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn bool_required_v2_uncompressed() -> Result<()> {
    round_trip(
        "bool",
        "required",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[cfg(feature = "io_parquet_compression")]
#[test]
fn bool_required_v2_compressed() -> Result<()> {
    round_trip(
        "bool",
        "required",
        Version::V2,
        CompressionOptions::Snappy,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_int64_optional_v2() -> Result<()> {
    round_trip(
        "list_int64",
        "nested",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_int64_optional_v1() -> Result<()> {
    round_trip(
        "list_int64",
        "nested",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_int64_required_required_v1() -> Result<()> {
    round_trip(
        "list_int64_required_required",
        "nested",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_int64_required_required_v2() -> Result<()> {
    round_trip(
        "list_int64_required_required",
        "nested",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_bool_optional_v2() -> Result<()> {
    round_trip(
        "list_bool",
        "nested",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_bool_optional_v1() -> Result<()> {
    round_trip(
        "list_bool",
        "nested",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_utf8_optional_v2() -> Result<()> {
    round_trip(
        "list_utf8",
        "nested",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_utf8_optional_v1() -> Result<()> {
    round_trip(
        "list_utf8",
        "nested",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_large_binary_optional_v2() -> Result<()> {
    round_trip(
        "list_large_binary",
        "nested",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn list_large_binary_optional_v1() -> Result<()> {
    round_trip(
        "list_large_binary",
        "nested",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
#[ignore]
fn utf8_optional_v2_delta() -> Result<()> {
    round_trip(
        "string",
        "nullable",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::DeltaLengthByteArray],
    )
}

#[test]
fn i32_optional_v2_dict() -> Result<()> {
    round_trip(
        "int32_dict",
        "nullable",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::RleDictionary],
    )
}

#[cfg(feature = "io_parquet_compression")]
#[test]
fn i32_optional_v2_dict_compressed() -> Result<()> {
    round_trip(
        "int32_dict",
        "nullable",
        Version::V2,
        CompressionOptions::Snappy,
        vec![Encoding::RleDictionary],
    )
}

// Decimal Testing
#[test]
fn decimal_9_optional_v1() -> Result<()> {
    round_trip(
        "decimal_9",
        "nullable",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_9_required_v1() -> Result<()> {
    round_trip(
        "decimal_9",
        "required",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_18_optional_v1() -> Result<()> {
    round_trip(
        "decimal_18",
        "nullable",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_18_required_v1() -> Result<()> {
    round_trip(
        "decimal_18",
        "required",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_26_optional_v1() -> Result<()> {
    round_trip(
        "decimal_26",
        "nullable",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_26_required_v1() -> Result<()> {
    round_trip(
        "decimal_26",
        "required",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_9_optional_v2() -> Result<()> {
    round_trip(
        "decimal_9",
        "nullable",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_9_required_v2() -> Result<()> {
    round_trip(
        "decimal_9",
        "required",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_18_optional_v2() -> Result<()> {
    round_trip(
        "decimal_18",
        "nullable",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_18_required_v2() -> Result<()> {
    round_trip(
        "decimal_18",
        "required",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_26_optional_v2() -> Result<()> {
    round_trip(
        "decimal_26",
        "nullable",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn decimal_26_required_v2() -> Result<()> {
    round_trip(
        "decimal_26",
        "required",
        Version::V2,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain],
    )
}

#[test]
fn struct_() -> Result<()> {
    round_trip(
        "struct",
        "struct",
        Version::V1,
        CompressionOptions::Uncompressed,
        vec![Encoding::Plain, Encoding::Plain],
    )
}
