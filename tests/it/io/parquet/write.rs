use std::io::Cursor;

use arrow2::io::parquet::write::*;
use arrow2::{error::Result, record_batch::RecordBatch};

use super::*;

fn round_trip(
    column: usize,
    nullable: bool,
    nested: bool,
    version: Version,
    compression: Compression,
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
    let schema = Schema::new(vec![field]);

    let options = WriteOptions {
        write_statistics: true,
        compression,
        version,
    };

    let parquet_schema = to_parquet_schema(&schema)?;

    let iter = vec![RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![array.clone()],
    )];

    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, vec![encoding])?;

    let mut writer = Cursor::new(vec![]);
    write_file(
        &mut writer,
        row_groups,
        &schema,
        parquet_schema,
        options,
        None,
    )?;

    let data = writer.into_inner();

    let (result, stats) = read_column(&mut Cursor::new(data), 0, 0)?;
    assert_eq!(array.as_ref(), result.as_ref());
    assert_eq!(statistics.as_ref(), stats.as_ref());
    Ok(())
}

#[test]
fn test_int64_optional_v1() -> Result<()> {
    round_trip(
        0,
        true,
        false,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_int64_required_v1() -> Result<()> {
    round_trip(
        0,
        false,
        false,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_int64_optional_v2() -> Result<()> {
    round_trip(
        0,
        true,
        false,
        Version::V2,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_int64_optional_v2_compressed() -> Result<()> {
    round_trip(
        0,
        true,
        false,
        Version::V2,
        Compression::Snappy,
        Encoding::Plain,
    )
}

#[test]
fn test_utf8_optional_v1() -> Result<()> {
    round_trip(
        2,
        true,
        false,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_utf8_required_v1() -> Result<()> {
    round_trip(
        2,
        false,
        false,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_utf8_optional_v2() -> Result<()> {
    round_trip(
        2,
        true,
        false,
        Version::V2,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_utf8_required_v2() -> Result<()> {
    round_trip(
        2,
        false,
        false,
        Version::V2,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_utf8_optional_v2_compressed() -> Result<()> {
    round_trip(
        2,
        true,
        false,
        Version::V2,
        Compression::Snappy,
        Encoding::Plain,
    )
}

#[test]
fn test_utf8_required_v2_compressed() -> Result<()> {
    round_trip(
        2,
        false,
        false,
        Version::V2,
        Compression::Snappy,
        Encoding::Plain,
    )
}

#[test]
fn test_bool_optional_v1() -> Result<()> {
    round_trip(
        3,
        true,
        false,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_bool_required_v1() -> Result<()> {
    round_trip(
        3,
        false,
        false,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_bool_optional_v2_uncompressed() -> Result<()> {
    round_trip(
        3,
        true,
        false,
        Version::V2,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_bool_required_v2_uncompressed() -> Result<()> {
    round_trip(
        3,
        false,
        false,
        Version::V2,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_bool_required_v2_compressed() -> Result<()> {
    round_trip(
        3,
        false,
        false,
        Version::V2,
        Compression::Snappy,
        Encoding::Plain,
    )
}

#[test]
fn test_list_int64_optional_v2() -> Result<()> {
    round_trip(
        0,
        true,
        true,
        Version::V2,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_list_int64_optional_v1() -> Result<()> {
    round_trip(
        0,
        true,
        true,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_list_bool_optional_v2() -> Result<()> {
    round_trip(
        4,
        true,
        true,
        Version::V2,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_list_bool_optional_v1() -> Result<()> {
    round_trip(
        4,
        true,
        true,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_list_utf8_optional_v2() -> Result<()> {
    round_trip(
        5,
        true,
        true,
        Version::V2,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_list_utf8_optional_v1() -> Result<()> {
    round_trip(
        5,
        true,
        true,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_list_large_binary_optional_v2() -> Result<()> {
    round_trip(
        6,
        true,
        true,
        Version::V2,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_list_large_binary_optional_v1() -> Result<()> {
    round_trip(
        6,
        true,
        true,
        Version::V1,
        Compression::Uncompressed,
        Encoding::Plain,
    )
}

#[test]
fn test_utf8_optional_v2_delta() -> Result<()> {
    round_trip(
        2,
        true,
        false,
        Version::V2,
        Compression::Uncompressed,
        Encoding::DeltaLengthByteArray,
    )
}

#[test]
fn test_i32_optional_v2_dict() -> Result<()> {
    round_trip(
        6,
        true,
        false,
        Version::V2,
        Compression::Uncompressed,
        Encoding::RleDictionary,
    )
}
