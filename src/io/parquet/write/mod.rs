mod boolean;
mod primitive;
mod schema;
mod utf8;

use crate::array::*;
use crate::datatypes::*;
use crate::error::Result;

pub use parquet2::{
    compression::CompressionCodec,
    metadata::SchemaDescriptor,
    read::CompressedPage,
    read::{get_page_iterator, read_metadata},
    write::write_file,
};
pub use schema::to_parquet_type;

pub fn array_to_page(array: &dyn Array) -> Result<CompressedPage> {
    // using plain encoding format
    let compression = CompressionCodec::Uncompressed;
    match array.data_type() {
        DataType::Boolean => {
            boolean::array_to_page_v1(array.as_any().downcast_ref().unwrap(), compression)
        }
        // casts below MUST match the casts done at the metadata (field -> parquet type).
        DataType::UInt8 => primitive::array_to_page_v1::<u8, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
        ),
        DataType::UInt16 => primitive::array_to_page_v1::<u16, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
        ),
        DataType::UInt32 => primitive::array_to_page_v1::<u32, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
        ),
        DataType::UInt64 => primitive::array_to_page_v1::<u64, i64>(
            array.as_any().downcast_ref().unwrap(),
            compression,
        ),
        DataType::Int8 => primitive::array_to_page_v1::<i8, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
        ),
        DataType::Int16 => primitive::array_to_page_v1::<i16, i32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
        ),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            primitive::array_to_page_v1::<i32, i32>(
                array.as_any().downcast_ref().unwrap(),
                compression,
            )
        }
        DataType::Int64 | DataType::Date64 | DataType::Time64(_) | DataType::Timestamp(_, _) => {
            primitive::array_to_page_v1::<i64, i64>(
                array.as_any().downcast_ref().unwrap(),
                compression,
            )
        }
        DataType::Float32 => primitive::array_to_page_v1::<f32, f32>(
            array.as_any().downcast_ref().unwrap(),
            compression,
        ),
        DataType::Float64 => primitive::array_to_page_v1::<f64, f64>(
            array.as_any().downcast_ref().unwrap(),
            compression,
        ),
        DataType::Utf8 => {
            utf8::array_to_page_v1::<i32>(array.as_any().downcast_ref().unwrap(), compression)
        }
        DataType::LargeUtf8 => {
            utf8::array_to_page_v1::<i64>(array.as_any().downcast_ref().unwrap(), compression)
        }
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

    fn round_trip_optional(column: usize) -> Result<()> {
        let array = pyarrow_nullable(column);
        let field = Field::new("a1", array.data_type().clone(), true);

        let parquet_type = to_parquet_type(&field)?;
        let schema = SchemaDescriptor::new("root".to_string(), vec![parquet_type]);

        let row_groups = std::iter::once(Result::Ok(std::iter::once(Ok(std::iter::once(
            array_to_page(array.as_ref()),
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
        round_trip_optional(0)
    }

    #[test]
    fn test_utf8_optional() -> Result<()> {
        round_trip_optional(2)
    }

    #[test]
    fn test_bool_optional() -> Result<()> {
        round_trip_optional(3)
    }
}
