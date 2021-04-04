mod boolean;
mod primitive;
mod utf8;
mod utils;

use crate::{
    array::Array,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use parquet2::{
    error::ParquetError,
    metadata::ColumnDescriptor,
    read::Page,
    schema::types::{LogicalType, ParquetType, PhysicalType, PrimitiveConvertedType},
};

pub fn page_iter_to_array<I: Iterator<Item = std::result::Result<Page, ParquetError>>>(
    iter: I,
    descriptor: &ColumnDescriptor,
) -> Result<Box<dyn Array>> {
    match descriptor.type_() {
        ParquetType::PrimitiveType {
            physical_type,
            converted_type,
            logical_type,
            ..
        } => match (physical_type, converted_type, logical_type) {
            // todo: apply conversion rules and the like
            (PhysicalType::Int32, None, None) => Ok(Box::new(
                primitive::iter_to_array::<i32, _, _>(iter, descriptor, DataType::Int32)?,
            )),
            (PhysicalType::Int64, None, None) => Ok(Box::new(
                primitive::iter_to_array::<i64, _, _>(iter, descriptor, DataType::Int64)?,
            )),
            (PhysicalType::Float, None, None) => Ok(Box::new(
                primitive::iter_to_array::<f32, _, _>(iter, descriptor, DataType::Float32)?,
            )),
            (PhysicalType::Double, None, None) => {
                Ok(Box::new(primitive::iter_to_array::<f64, _, _>(
                    iter,
                    descriptor,
                    DataType::Float64,
                )?))
            }
            (PhysicalType::Boolean, None, None) => {
                Ok(Box::new(boolean::iter_to_array(iter, descriptor)?))
            }
            (
                PhysicalType::ByteArray,
                Some(PrimitiveConvertedType::Utf8),
                Some(LogicalType::STRING(_)),
            ) => Ok(Box::new(utf8::iter_to_array::<i32, _, _>(
                iter, descriptor,
            )?)),
            (p, c, l) => Err(ArrowError::NotYetImplemented(format!(
                "The conversion of ({:?}, {:?}, {:?}) to arrow still not implemented",
                p, c, l
            ))),
        },
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use parquet2::{
        read::{get_page_iterator, read_metadata},
    };

    use crate::array::*;

    use super::*;

    fn pyarrow_integration(column: usize) -> Box<dyn Array> {
        match column {
            0 => Box::new(
                Primitive::<i64>::from(&[
                    Some(0),
                    Some(1),
                    None,
                    Some(3),
                    None,
                    Some(5),
                    Some(6),
                    Some(7),
                    None,
                    Some(9),
                ])
                .to(DataType::Int64),
            ),
            1 => Box::new(
                Primitive::<f64>::from(&[
                    Some(0.0),
                    Some(1.0),
                    None,
                    Some(3.0),
                    None,
                    Some(5.0),
                    Some(6.0),
                    Some(7.0),
                    None,
                    Some(9.0),
                ])
                .to(DataType::Float64),
            ),
            2 => Box::new(Utf8Array::<i32>::from(&vec![
                Some("Hello".to_string()),
                None,
                Some("aa".to_string()),
                Some("".to_string()),
                None,
                Some("abc".to_string()),
                None,
                None,
                Some("def".to_string()),
                Some("aaa".to_string()),
            ])),
            3 => Box::new(BooleanArray::from(&[
                Some(true),
                None,
                Some(false),
                Some(false),
                None,
                Some(true),
                None,
                None,
                Some(true),
                Some(true),
            ])),
            _ => unreachable!(),
        }
    }

    fn get_column(path: &str, row_group: usize, column: usize) -> Result<Box<dyn Array>> {
        let mut file = File::open(path).unwrap();

        let metadata = read_metadata(&mut file).map_err(ArrowError::from_external_error)?;
        let iter = get_page_iterator(&metadata, row_group, column, &mut file)
            .map_err(ArrowError::from_external_error)?;

        let descriptor = &iter.descriptor().clone();

        page_iter_to_array(iter, descriptor)
    }

    #[test]
    fn pyarrow_integration_int64() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() { return Ok(()); }
        let column = 0;
        let path = "fixtures/pyarrow3/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_float64() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() { return Ok(()); }
        let column = 1;
        let path = "fixtures/pyarrow3/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_string() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() { return Ok(()); }
        let column = 2;
        let path = "fixtures/pyarrow3/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_boolean() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() { return Ok(()); }
        let column = 3;
        let path = "fixtures/pyarrow3/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }
}
