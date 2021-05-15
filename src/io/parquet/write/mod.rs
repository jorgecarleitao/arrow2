mod binary;
mod boolean;
mod fixed_len_bytes;
mod primitive;
mod schema;
mod utf8;
mod utils;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::{array::*, io::parquet::read::is_type_nullable};

pub use parquet2::{
    compression::CompressionCodec, read::CompressedPage, schema::types::ParquetType,
};
use parquet2::{
    metadata::SchemaDescriptor, schema::KeyValue, write::write_file as parquet_write_file,
};
use schema::schema_to_metadata_key;
pub use schema::to_parquet_type;

/// Writes
pub fn write_file<
    W,
    I,   // iterator over pages
    II,  // iterator over columns
    III, // iterator over row groups
>(
    writer: &mut W,
    row_groups: III,
    schema: &Schema,
    codec: CompressionCodec,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<()>
where
    W: std::io::Write + std::io::Seek,
    I: Iterator<Item = Result<CompressedPage>>,
    II: Iterator<Item = Result<I>>,
    III: Iterator<Item = Result<II>>,
{
    let key_value_metadata = key_value_metadata
        .map(|mut x| {
            x.push(schema_to_metadata_key(&schema));
            x
        })
        .or_else(|| Some(vec![schema_to_metadata_key(&schema)]));

    let fields = schema
        .fields()
        .iter()
        .map(to_parquet_type)
        .collect::<Result<Vec<_>>>()?;
    let schema = SchemaDescriptor::new("root".to_string(), fields);

    let created_by = Some("Arrow2 - Native Rust implementation of Arrow".to_string());
    Ok(parquet_write_file(
        writer,
        row_groups,
        schema,
        codec,
        created_by,
        key_value_metadata,
    )?)
}

pub fn array_to_page(
    array: &dyn Array,
    type_: &ParquetType,
    compression: CompressionCodec,
) -> Result<CompressedPage> {
    // using plain encoding format
    let is_optional = is_type_nullable(type_);
    match array.data_type() {
        DataType::Boolean => boolean::array_to_page_v1(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        // casts below MUST match the casts done at the metadata (field -> parquet type).
        DataType::UInt8 => primitive::array_to_page_v1::<u8, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::UInt16 => primitive::array_to_page_v1::<u16, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::UInt32 => primitive::array_to_page_v1::<u32, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::UInt64 => primitive::array_to_page_v1::<u64, i64>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::Int8 => primitive::array_to_page_v1::<i8, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::Int16 => primitive::array_to_page_v1::<i16, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            primitive::array_to_page_v1::<i32, i32>(
                array.as_any().downcast_ref().unwrap(),
                compression,
                is_optional,
            )
        }
        DataType::Int64 | DataType::Date64 | DataType::Time64(_) | DataType::Timestamp(_, _) => {
            primitive::array_to_page_v1::<i64, i64>(
                array.as_any().downcast_ref().unwrap(),
                compression,
                is_optional,
            )
        }
        DataType::Float32 => primitive::array_to_page_v1::<f32, f32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::Float64 => primitive::array_to_page_v1::<f64, f64>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::Utf8 => utf8::array_to_page_v1::<i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::LargeUtf8 => utf8::array_to_page_v1::<i64>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::Binary => binary::array_to_page_v1::<i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::LargeBinary => binary::array_to_page_v1::<i64>(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        DataType::Null => {
            let array = Int32Array::new_null(DataType::Int32, array.len());
            primitive::array_to_page_v1::<i32, i32>(&array, compression, is_optional)
        }
        DataType::FixedSizeBinary(_) => fixed_len_bytes::array_to_page_v1(
            array.as_any().downcast_ref().unwrap(),
            compression,
            is_optional,
        ),
        other => Err(ArrowError::NotYetImplemented(format!(
            "Writing the data type {:?} is not yet implemented",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::Result;
    use crate::io::parquet::read::{get_page_iterator, page_iter_to_array, read_metadata};
    use std::io::{Cursor, Read, Seek};

    use super::super::tests::*;

    fn read_column<R: Read + Seek>(
        reader: &mut R,
        row_group: usize,
        column: usize,
    ) -> Result<Box<dyn Array>> {
        let metadata = read_metadata(reader)?;
        let iter = get_page_iterator(&metadata, row_group, column, reader)?;

        let descriptor = iter.descriptor().clone();

        page_iter_to_array(iter, &descriptor)
    }

    fn round_trip(column: usize, nullable: bool) -> Result<()> {
        let array = if nullable {
            pyarrow_nullable(column)
        } else {
            pyarrow_required(column)
        };
        let field = Field::new("a1", array.data_type().clone(), nullable);
        let schema = Schema::new(vec![field]);

        let compression = CompressionCodec::Uncompressed;

        let parquet_types = schema
            .fields()
            .iter()
            .map(to_parquet_type)
            .collect::<Result<Vec<_>>>()?;

        // one row group
        // one column chunk
        // one page
        let row_groups = std::iter::once(Result::Ok(std::iter::once(Ok(std::iter::once(
            array.as_ref(),
        )
        .zip(parquet_types.iter())
        .map(|(array, type_)| array_to_page(array, type_, compression))))));

        let mut writer = Cursor::new(vec![]);
        write_file(&mut writer, row_groups, &schema, compression, None)?;

        let data = writer.into_inner();

        let result = read_column(&mut Cursor::new(data), 0, 0)?;
        assert_eq!(array.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn test_int32_optional() -> Result<()> {
        round_trip(0, true)
    }

    #[test]
    fn test_utf8_optional() -> Result<()> {
        round_trip(2, true)
    }

    #[test]
    fn test_bool_optional() -> Result<()> {
        round_trip(3, true)
    }

    #[test]
    fn test_int64_required() -> Result<()> {
        round_trip(0, false)
    }

    #[test]
    fn test_utf8_required() -> Result<()> {
        round_trip(2, false)
    }

    #[test]
    fn test_bool_required() -> Result<()> {
        round_trip(3, false)
    }
}
