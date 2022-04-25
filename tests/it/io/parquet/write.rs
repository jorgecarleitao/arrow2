use std::io::Cursor;

use arrow2::error::Result;
use arrow2::io::parquet::write::*;

use super::*;

fn round_trip(
    column: &str,
    nullable: bool,
    nested: bool,
    version: Version,
    compression: CompressionOptions,
    encoding: Encoding,
) -> Result<()> {
    let (array, statistics) = if nested {
        (
            pyarrow_nested_nullable(column),
            pyarrow_nested_nullable_statistics(column),
        )
    } else if nullable {
        (
            pyarrow_nullable(column),
            pyarrow_nullable_statistics(column),
        )
    } else {
        (
            pyarrow_required(column),
            pyarrow_required_statistics(column),
        )
    };
    let array: Arc<dyn Array> = array.into();

    let field = Field::new("a1", array.data_type().clone(), nullable);
    let schema = Schema::from(vec![field]);

    let options = WriteOptions {
        write_statistics: true,
        compression,
        version,
    };

    let iter = vec![Chunk::try_new(vec![array.clone()])];

    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, vec![encoding])?;

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
        true,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn int64_required_v1() -> Result<()> {
    round_trip(
        "int64",
        false,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn int64_optional_v2() -> Result<()> {
    round_trip(
        "int64",
        true,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn int64_optional_v2_compressed() -> Result<()> {
    round_trip(
        "int64",
        true,
        false,
        Version::V2,
        CompressionOptions::Snappy,
        Encoding::Plain,
    )
}

#[test]
fn utf8_optional_v1() -> Result<()> {
    round_trip(
        "string",
        true,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn utf8_required_v1() -> Result<()> {
    round_trip(
        "string",
        false,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn utf8_optional_v2() -> Result<()> {
    round_trip(
        "string",
        true,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn utf8_required_v2() -> Result<()> {
    round_trip(
        "string",
        false,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn utf8_optional_v2_compressed() -> Result<()> {
    round_trip(
        "string",
        true,
        false,
        Version::V2,
        CompressionOptions::Snappy,
        Encoding::Plain,
    )
}

#[test]
fn utf8_required_v2_compressed() -> Result<()> {
    round_trip(
        "string",
        false,
        false,
        Version::V2,
        CompressionOptions::Snappy,
        Encoding::Plain,
    )
}

#[test]
fn bool_optional_v1() -> Result<()> {
    round_trip(
        "bool",
        true,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn bool_required_v1() -> Result<()> {
    round_trip(
        "bool",
        false,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn bool_optional_v2_uncompressed() -> Result<()> {
    round_trip(
        "bool",
        true,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn bool_required_v2_uncompressed() -> Result<()> {
    round_trip(
        "bool",
        false,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn bool_required_v2_compressed() -> Result<()> {
    round_trip(
        "bool",
        false,
        false,
        Version::V2,
        CompressionOptions::Snappy,
        Encoding::Plain,
    )
}

#[test]
fn list_int64_optional_v2() -> Result<()> {
    round_trip(
        "list_int64",
        true,
        true,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn list_int64_optional_v1() -> Result<()> {
    round_trip(
        "list_int64",
        true,
        true,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn list_bool_optional_v2() -> Result<()> {
    round_trip(
        "list_bool",
        true,
        true,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn list_bool_optional_v1() -> Result<()> {
    round_trip(
        "list_bool",
        true,
        true,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn list_utf8_optional_v2() -> Result<()> {
    round_trip(
        "list_utf8",
        true,
        true,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn list_utf8_optional_v1() -> Result<()> {
    round_trip(
        "list_utf8",
        true,
        true,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn list_large_binary_optional_v2() -> Result<()> {
    round_trip(
        "list_large_binary",
        true,
        true,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn list_large_binary_optional_v1() -> Result<()> {
    round_trip(
        "list_large_binary",
        true,
        true,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
#[ignore]
fn utf8_optional_v2_delta() -> Result<()> {
    round_trip(
        "string",
        true,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::DeltaLengthByteArray,
    )
}

#[test]
fn i32_optional_v2_dict() -> Result<()> {
    round_trip(
        "int32_dict",
        true,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::RleDictionary,
    )
}

#[test]
fn i32_optional_v2_dict_compressed() -> Result<()> {
    round_trip(
        "int32_dict",
        true,
        false,
        Version::V2,
        CompressionOptions::Snappy,
        Encoding::RleDictionary,
    )
}

// Decimal Testing
#[test]
fn decimal_9_optional_v1() -> Result<()> {
    round_trip(
        "decimal_9",
        true,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_9_required_v1() -> Result<()> {
    round_trip(
        "decimal_9",
        false,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_18_optional_v1() -> Result<()> {
    round_trip(
        "decimal_18",
        true,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_18_required_v1() -> Result<()> {
    round_trip(
        "decimal_18",
        false,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_26_optional_v1() -> Result<()> {
    round_trip(
        "decimal_26",
        true,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_26_required_v1() -> Result<()> {
    round_trip(
        "decimal_26",
        false,
        false,
        Version::V1,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_9_optional_v2() -> Result<()> {
    round_trip(
        "decimal_9",
        true,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_9_required_v2() -> Result<()> {
    round_trip(
        "decimal_9",
        false,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_18_optional_v2() -> Result<()> {
    round_trip(
        "decimal_18",
        true,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_18_required_v2() -> Result<()> {
    round_trip(
        "decimal_18",
        false,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_26_optional_v2() -> Result<()> {
    round_trip(
        "decimal_26",
        true,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn decimal_26_required_v2() -> Result<()> {
    round_trip(
        "decimal_26",
        false,
        false,
        Version::V2,
        CompressionOptions::Uncompressed,
        Encoding::Plain,
    )
}
