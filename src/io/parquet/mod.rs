use crate::error::ArrowError;

pub mod read;
pub mod write;

const ARROW_SCHEMA_META_KEY: &str = "ARROW:schema";

impl From<parquet2::error::ParquetError> for ArrowError {
    fn from(error: parquet2::error::ParquetError) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use crate::array::*;
    use crate::datatypes::*;

    pub fn pyarrow_nullable(column: usize) -> Box<dyn Array> {
        let i64_values = &[
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
        ];

        match column {
            0 => Box::new(Primitive::<i64>::from(i64_values).to(DataType::Int64)),
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
            4 => Box::new(
                Primitive::<i64>::from(i64_values)
                    .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
            ),
            5 => {
                let values = i64_values
                    .iter()
                    .map(|x| x.map(|x| x as u32))
                    .collect::<Vec<_>>();
                Box::new(Primitive::<u32>::from(values).to(DataType::UInt32))
            }
            _ => unreachable!(),
        }
    }

    // these values match the values in `integration`
    pub fn pyarrow_required(column: usize) -> Box<dyn Array> {
        let i64_values = &[
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ];

        match column {
            0 => Box::new(Primitive::<i64>::from(i64_values).to(DataType::Int64)),
            3 => Box::new(BooleanArray::from_slice(&[
                true, true, false, false, false, true, true, true, true, true,
            ])),
            2 => Box::new(Utf8Array::<i32>::from_slice(&[
                "Hello", "bbb", "aa", "", "bbb", "abc", "bbb", "bbb", "def", "aaa",
            ])),
            _ => unreachable!(),
        }
    }
}

/// Round-trip with parquet using the same integration files used for IPC integration tests.
#[cfg(test)]
mod tests_integration {
    use std::sync::Arc;

    use super::write::CompressionCodec;
    use crate::array::Array;
    use crate::datatypes::*;
    use crate::record_batch::*;

    use crate::error::Result;
    use crate::io::ipc::common::tests::read_gzip_json;
    use crate::io::parquet::read;
    use crate::io::parquet::write::*;
    use std::io::Cursor;

    fn integration_write(schema: &Schema, batches: &[RecordBatch]) -> Result<Vec<u8>> {
        let codec = CompressionCodec::Uncompressed;

        let parquet_types = schema
            .fields()
            .iter()
            .map(to_parquet_type)
            .collect::<Result<Vec<_>>>()?;

        let row_groups = batches.iter().map(|batch| {
            let iterator =
                batch
                    .columns()
                    .iter()
                    .zip(parquet_types.iter())
                    .map(|(array, type_)| {
                        Ok(std::iter::once(array_to_page(array.as_ref(), type_, codec)))
                    });
            Ok(iterator)
        });

        let mut writer = Cursor::new(vec![]);

        write_file(&mut writer, row_groups, schema, codec, None)?;

        Ok(writer.into_inner())
    }

    fn integration_read(data: &[u8]) -> Result<(Schema, Vec<RecordBatch>)> {
        let mut reader = Cursor::new(data);

        let file_metadata = read::read_metadata(&mut reader)?;

        let schema = read::get_schema(&file_metadata)?;
        let schema1 = Arc::new(schema.clone());

        let batches = file_metadata
            .row_groups
            .iter()
            .enumerate()
            .map(|(row_group, group)| {
                let columns = group
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(column, column_meta)| {
                        let pages = read::get_page_iterator(
                            &file_metadata,
                            row_group,
                            column,
                            &mut reader,
                        )?;
                        read::page_iter_to_array(pages, column_meta).map(|x| x.into())
                    })
                    .collect::<Result<Vec<Arc<dyn Array>>>>()?;
                RecordBatch::try_new(schema1.clone(), columns)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((schema, batches))
    }

    fn test_file(version: &str, file_name: &str) -> Result<()> {
        let (schema, batches) = read_gzip_json(version, file_name);

        let data = integration_write(&schema, &batches)?;

        let (read_schema, read_batches) = integration_read(&data)?;

        assert_eq!(schema, read_schema);
        assert_eq!(batches, read_batches);

        Ok(())
    }

    #[test]
    fn roundtrip_100_primitive() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_primitive")?;
        test_file("1.0.0-bigendian", "generated_primitive")
    }
}
