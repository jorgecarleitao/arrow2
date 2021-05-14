mod boolean;
mod primitive;
mod schema;
mod utf8;
mod utils;

use crate::datatypes::*;
use crate::error::Result;
use crate::{array::*, io::parquet::read::is_type_nullable};

pub use parquet2::{
    compression::CompressionCodec,
    metadata::SchemaDescriptor,
    read::CompressedPage,
    read::{get_page_iterator, read_metadata},
    schema::types::ParquetType,
    write::write_file,
};
pub use schema::to_parquet_type;

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
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::Result;
    use crate::io::parquet::read::page_iter_to_array;
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

        let parquet_type = to_parquet_type(&field)?;
        let schema = SchemaDescriptor::new("root".to_string(), vec![parquet_type.clone()]);

        let row_groups = std::iter::once(Result::Ok(std::iter::once(Ok(std::iter::once(
            array_to_page(
                array.as_ref(),
                &parquet_type,
                CompressionCodec::Uncompressed,
            ),
        )))));

        let mut writer = Cursor::new(vec![]);
        write_file(
            &mut writer,
            schema,
            CompressionCodec::Uncompressed,
            row_groups,
        )?;

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
