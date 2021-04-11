mod boolean;
mod primitive;
mod utf8;

use parquet2::{compression::CompressionCodec, read::CompressedPage};

use crate::array::*;
use crate::datatypes::DataType;
use crate::error::Result;

pub fn array_to_page(array: &dyn Array) -> Result<CompressedPage> {
    // using plain encoding format
    let compression = CompressionCodec::Uncompressed;
    match array.data_type() {
        DataType::Boolean => {
            boolean::array_to_page_v1(array.as_any().downcast_ref().unwrap(), compression)
        }
        DataType::Int32 => {
            primitive::array_to_page_v1::<i32>(array.as_any().downcast_ref().unwrap(), compression)
        }
        DataType::Int64 => {
            primitive::array_to_page_v1::<i64>(array.as_any().downcast_ref().unwrap(), compression)
        }
        DataType::Float32 => {
            primitive::array_to_page_v1::<f32>(array.as_any().downcast_ref().unwrap(), compression)
        }
        DataType::Float64 => {
            primitive::array_to_page_v1::<f64>(array.as_any().downcast_ref().unwrap(), compression)
        }
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
    use std::io::{Cursor, Read, Seek};

    use super::*;

    use parquet2::{
        metadata::SchemaDescriptor,
        read::{get_page_iterator, read_metadata},
        schema::io_message::from_message,
        write::write_file,
    };

    use crate::error::Result;
    use crate::io::parquet::read::page_iter_to_array;

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

        let row_groups = std::iter::once(Result::Ok(std::iter::once(Ok(std::iter::once(
            array_to_page(array.as_ref()),
        )))));

        // prepare schema
        let (physical, converted) = match array.data_type() {
            DataType::Int32 => ("INT32", ""),
            DataType::Int64 => ("INT64", ""),
            DataType::Float32 => ("FLOAT", ""),
            DataType::Float64 => ("DOUBLE", ""),
            DataType::Boolean => ("BOOLEAN", ""),
            DataType::Utf8 => ("BYTE_ARRAY", "(UTF8)"),
            _ => todo!(),
        };
        let schema = SchemaDescriptor::new(from_message(&format!(
            "message schema {{ OPTIONAL {} col {}; }}",
            physical, converted
        ))?);

        let mut writer = Cursor::new(vec![]);
        write_file(
            &mut writer,
            &schema,
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
