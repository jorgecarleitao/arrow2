mod binary;
mod boolean;
mod fixed_len_bytes;
mod primitive;
mod schema;
mod utf8;
mod utils;

pub mod stream;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use parquet2::metadata::ColumnDescriptor;
pub use parquet2::{
    compression::CompressionCodec,
    read::CompressedPage,
    schema::types::ParquetType,
    write::WriteOptions,
    write::{DynIter, RowGroupIter},
};
use parquet2::{
    metadata::SchemaDescriptor, schema::KeyValue, write::write_file as parquet_write_file,
};
use schema::schema_to_metadata_key;
pub use schema::to_parquet_type;

/// Creates a parquet [`SchemaDescriptor`] from a [`Schema`].
pub fn to_parquet_schema(schema: &Schema) -> Result<SchemaDescriptor> {
    let parquet_types = schema
        .fields()
        .iter()
        .map(to_parquet_type)
        .collect::<Result<Vec<_>>>()?;
    Ok(SchemaDescriptor::new("root".to_string(), parquet_types))
}

/// Writes
pub fn write_file<'a, W, I>(
    writer: &mut W,
    row_groups: I,
    schema: &Schema,
    parquet_schema: SchemaDescriptor,
    options: WriteOptions,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<()>
where
    W: std::io::Write + std::io::Seek,
    I: Iterator<Item = Result<RowGroupIter<'a, ArrowError>>>,
{
    let key_value_metadata = key_value_metadata
        .map(|mut x| {
            x.push(schema_to_metadata_key(schema));
            x
        })
        .or_else(|| Some(vec![schema_to_metadata_key(schema)]));

    let created_by = Some("Arrow2 - Native Rust implementation of Arrow".to_string());
    Ok(parquet_write_file(
        writer,
        row_groups,
        parquet_schema,
        options,
        created_by,
        key_value_metadata,
    )?)
}

pub fn array_to_page(
    array: &dyn Array,
    descriptor: ColumnDescriptor,
    options: WriteOptions,
) -> Result<CompressedPage> {
    // using plain encoding format
    match array.data_type() {
        DataType::Boolean => {
            boolean::array_to_page_v1(array.as_any().downcast_ref().unwrap(), options, descriptor)
        }
        // casts below MUST match the casts done at the metadata (field -> parquet type).
        DataType::UInt8 => primitive::array_to_page_v1::<u8, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::UInt16 => primitive::array_to_page_v1::<u16, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::UInt32 => primitive::array_to_page_v1::<u32, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::UInt64 => primitive::array_to_page_v1::<u64, i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Int8 => primitive::array_to_page_v1::<i8, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Int16 => primitive::array_to_page_v1::<i16, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            primitive::array_to_page_v1::<i32, i32>(
                array.as_any().downcast_ref().unwrap(),
                options,
                descriptor,
            )
        }
        DataType::Int64 | DataType::Date64 | DataType::Time64(_) | DataType::Timestamp(_, _) => {
            primitive::array_to_page_v1::<i64, i64>(
                array.as_any().downcast_ref().unwrap(),
                options,
                descriptor,
            )
        }
        DataType::Float32 => primitive::array_to_page_v1::<f32, f32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Float64 => primitive::array_to_page_v1::<f64, f64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Utf8 => utf8::array_to_page_v1::<i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::LargeUtf8 => utf8::array_to_page_v1::<i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Binary => binary::array_to_page_v1::<i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::LargeBinary => binary::array_to_page_v1::<i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Null => {
            let array = Int32Array::new_null(DataType::Int32, array.len());
            primitive::array_to_page_v1::<i32, i32>(&array, options, descriptor)
        }
        DataType::FixedSizeBinary(_) => fixed_len_bytes::array_to_page_v1(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
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
    use std::io::Cursor;

    use super::super::tests::*;

    fn round_trip(column: usize, nullable: bool) -> Result<()> {
        let array = if nullable {
            pyarrow_nullable(column)
        } else {
            pyarrow_required(column)
        };
        let statistics = if nullable {
            pyarrow_nullable_statistics(column)
        } else {
            pyarrow_required_statistics(column)
        };

        let field = Field::new("a1", array.data_type().clone(), nullable);
        let schema = Schema::new(vec![field]);

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionCodec::Uncompressed,
        };

        let parquet_schema = to_parquet_schema(&schema)?;

        // one row group
        // one column chunk
        // one page
        let row_groups =
            std::iter::once(Result::Ok(DynIter::new(std::iter::once(Ok(DynIter::new(
                std::iter::once(array.as_ref())
                    .zip(parquet_schema.columns().to_vec().into_iter())
                    .map(|(array, descriptor)| array_to_page(array, descriptor, options)),
            ))))));

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
        assert_eq!(statistics.as_ref(), stats.unwrap().as_ref());
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
