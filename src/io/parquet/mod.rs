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
    use crate::bitmap::Bitmap;
    use crate::buffer::Buffer;
    use crate::datatypes::*;

    use crate::error::Result;
    use crate::io::parquet::read;
    use crate::io::parquet::read::statistics::*;
    use std::io::{Read, Seek};
    use std::sync::Arc;

    type ArrayStats = (
        Arc<dyn Array>,
        Option<Box<dyn read::statistics::Statistics>>,
    );

    pub fn read_column<R: Read + Seek>(
        mut reader: R,
        row_group: usize,
        column: usize,
    ) -> Result<ArrayStats> {
        let metadata = read::read_metadata(&mut reader)?;

        let mut reader =
            read::RecordReader::try_new(reader, Some(vec![column]), None, Arc::new(|_, _| true))?;

        let statistics = metadata.row_groups[row_group]
            .column(column)
            .statistics()
            .map(|x| read::statistics::deserialize_statistics(x?.as_ref()))
            .transpose()?;

        Ok((reader.next().unwrap()?.columns()[0].clone(), statistics))
    }

    pub fn pyarrow_nested_nullable(column: usize) -> Box<dyn Array> {
        let offsets = Buffer::<i32>::from([0, 2, 2, 5, 8, 8, 11, 11, 12]);

        let values = match column {
            0 => {
                // [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
                Arc::new(PrimitiveArray::<i64>::from(&[
                    Some(0),
                    Some(1),
                    Some(2),
                    None,
                    Some(3),
                    Some(4),
                    Some(5),
                    Some(6),
                    Some(7),
                    Some(8),
                    Some(9),
                    Some(10),
                ])) as Arc<dyn Array>
            }
            1 | 2 => {
                // [[0, 1], None, [2, 0, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
                Arc::new(PrimitiveArray::<i64>::from(&[
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(0),
                    Some(3),
                    Some(4),
                    Some(5),
                    Some(6),
                    Some(7),
                    Some(8),
                    Some(9),
                    Some(10),
                ])) as Arc<dyn Array>
            }
            3 => Arc::new(PrimitiveArray::<i16>::from(&[
                Some(0),
                Some(1),
                Some(2),
                None,
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(10),
            ])) as Arc<dyn Array>,
            4 => Arc::new(BooleanArray::from(&[
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(true),
            ])) as Arc<dyn Array>,
            /*
                string = [
                    ["Hello", "bbb"],
                    None,
                    ["aa", None, ""],
                    ["bbb", "aa", "ccc"],
                    [],
                    ["abc", "bbb", "bbb"],
                    None,
                    [""],
                ]
            */
            5 => Arc::new(Utf8Array::<i32>::from(&[
                Some("Hello".to_string()),
                Some("bbb".to_string()),
                Some("aa".to_string()),
                None,
                Some("".to_string()),
                Some("bbb".to_string()),
                Some("aa".to_string()),
                Some("ccc".to_string()),
                Some("abc".to_string()),
                Some("bbb".to_string()),
                Some("bbb".to_string()),
                Some("".to_string()),
            ])),
            6 => Arc::new(BinaryArray::<i64>::from(&[
                Some(b"Hello".to_vec()),
                Some(b"bbb".to_vec()),
                Some(b"aa".to_vec()),
                None,
                Some(b"".to_vec()),
                Some(b"bbb".to_vec()),
                Some(b"aa".to_vec()),
                Some(b"ccc".to_vec()),
                Some(b"abc".to_vec()),
                Some(b"bbb".to_vec()),
                Some(b"bbb".to_vec()),
                Some(b"".to_vec()),
            ])),
            _ => unreachable!(),
        };

        match column {
            0 | 1 | 3 | 4 | 5 | 6 => {
                let field = match column {
                    0 => Field::new("item", DataType::Int64, true),
                    1 => Field::new("item", DataType::Int64, false),
                    3 => Field::new("item", DataType::Int16, true),
                    4 => Field::new("item", DataType::Boolean, true),
                    5 => Field::new("item", DataType::Utf8, true),
                    6 => Field::new("item", DataType::LargeBinary, true),
                    _ => unreachable!(),
                };

                let validity = Some(Bitmap::from([
                    true, false, true, true, true, true, false, true,
                ]));
                let data_type = DataType::List(Box::new(field));
                Box::new(ListArray::<i32>::from_data(
                    data_type, offsets, values, validity,
                ))
            }
            2 => {
                // [[0, 1], [], [2, None, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
                let data_type =
                    DataType::List(Box::new(Field::new("item", DataType::Int64, false)));
                Box::new(ListArray::<i32>::from_data(
                    data_type, offsets, values, None,
                ))
            }
            _ => unreachable!(),
        }
    }

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
            0 => Box::new(PrimitiveArray::<i64>::from(i64_values)),
            1 => Box::new(PrimitiveArray::<f64>::from(&[
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
            ])),
            2 => Box::new(Utf8Array::<i32>::from(&[
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
                PrimitiveArray::<i64>::from(i64_values)
                    .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
            ),
            5 => {
                let values = i64_values
                    .iter()
                    .map(|x| x.map(|x| x as u32))
                    .collect::<Vec<_>>();
                Box::new(PrimitiveArray::<u32>::from(values))
            }
            6 => {
                let keys = PrimitiveArray::<i32>::from([Some(0), Some(1), None, Some(1)]);
                let values = Arc::new(PrimitiveArray::<i32>::from_slice([10, 200]));
                Box::new(DictionaryArray::<i32>::from_data(keys, values))
            }
            _ => unreachable!(),
        }
    }

    pub fn pyarrow_nullable_statistics(column: usize) -> Option<Box<dyn Statistics>> {
        Some(match column {
            0 => Box::new(PrimitiveStatistics::<i64> {
                data_type: DataType::Int64,
                distinct_count: None,
                null_count: Some(3),
                min_value: Some(0),
                max_value: Some(9),
            }),
            1 => Box::new(PrimitiveStatistics::<f64> {
                data_type: DataType::Float64,
                distinct_count: None,
                null_count: Some(3),
                min_value: Some(0.0),
                max_value: Some(9.0),
            }),
            2 => Box::new(Utf8Statistics {
                null_count: Some(4),
                distinct_count: None,
                min_value: Some("".to_string()),
                max_value: Some("def".to_string()),
            }),
            3 => Box::new(BooleanStatistics {
                null_count: Some(4),
                distinct_count: None,

                min_value: Some(false),
                max_value: Some(true),
            }),
            4 => Box::new(PrimitiveStatistics::<i64> {
                data_type: DataType::Timestamp(TimeUnit::Millisecond, None),
                distinct_count: None,
                null_count: Some(3),
                min_value: Some(0),
                max_value: Some(9),
            }),
            5 => Box::new(PrimitiveStatistics::<u32> {
                data_type: DataType::UInt32,
                null_count: Some(3),
                distinct_count: None,

                min_value: Some(0),
                max_value: Some(9),
            }),
            6 => return None,
            _ => unreachable!(),
        })
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
            0 => Box::new(PrimitiveArray::<i64>::from(i64_values).to(DataType::Int64)),
            3 => Box::new(BooleanArray::from_slice(&[
                true, true, false, false, false, true, true, true, true, true,
            ])),
            2 => Box::new(Utf8Array::<i32>::from_slice(&[
                "Hello", "bbb", "aa", "", "bbb", "abc", "bbb", "bbb", "def", "aaa",
            ])),
            _ => unreachable!(),
        }
    }

    pub fn pyarrow_required_statistics(column: usize) -> Option<Box<dyn Statistics>> {
        Some(match column {
            0 => Box::new(PrimitiveStatistics::<i64> {
                data_type: DataType::Int64,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(0),
                max_value: Some(9),
            }),
            3 => Box::new(BooleanStatistics {
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(false),
                max_value: Some(true),
            }),
            2 => Box::new(Utf8Statistics {
                null_count: Some(0),
                distinct_count: None,
                min_value: Some("".to_string()),
                max_value: Some("def".to_string()),
            }),
            _ => unreachable!(),
        })
    }

    pub fn pyarrow_nested_nullable_statistics(column: usize) -> Option<Box<dyn Statistics>> {
        Some(match column {
            3 => Box::new(PrimitiveStatistics::<i16> {
                data_type: DataType::Int16,
                distinct_count: None,
                null_count: Some(1),
                min_value: Some(0),
                max_value: Some(10),
            }),
            4 => Box::new(BooleanStatistics {
                distinct_count: None,
                null_count: Some(1),
                min_value: Some(false),
                max_value: Some(true),
            }),
            5 => Box::new(Utf8Statistics {
                distinct_count: None,
                null_count: Some(1),
                min_value: Some("".to_string()),
                max_value: Some("def".to_string()),
            }),
            6 => Box::new(BinaryStatistics {
                distinct_count: None,
                null_count: Some(1),
                min_value: Some(b"".to_vec()),
                max_value: Some(b"def".to_vec()),
            }),
            _ => Box::new(PrimitiveStatistics::<i64> {
                data_type: DataType::Int64,
                distinct_count: None,
                null_count: Some(3),
                min_value: Some(0),
                max_value: Some(9),
            }),
        })
    }
}

/// Round-trip with parquet using the same integration files used for IPC integration tests.
#[cfg(test)]
mod tests_integration {
    use std::sync::Arc;

    use super::write::CompressionCodec;
    use crate::array::{Array, PrimitiveArray, Utf8Array};
    use crate::datatypes::DataType;
    use crate::datatypes::TimeUnit;
    use crate::datatypes::*;
    use crate::record_batch::*;

    use crate::error::Result;
    use crate::io::ipc::common::tests::read_gzip_json;
    use crate::io::parquet::read;
    use crate::io::parquet::write::*;
    use std::io::Cursor;

    fn integration_write(schema: &Schema, batches: &[RecordBatch]) -> Result<Vec<u8>> {
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionCodec::Uncompressed,
            version: Version::V1,
        };

        let parquet_schema = to_parquet_schema(schema)?;
        let descritors = parquet_schema.columns().to_vec().into_iter();

        let row_groups = batches.iter().map(|batch| {
            let iterator = DynIter::new(batch.columns().iter().zip(descritors.clone()).map(
                |(array, type_)| {
                    Ok(DynIter::new(std::iter::once(array_to_page(
                        array.as_ref(),
                        type_,
                        options,
                        Encoding::Plain,
                    ))))
                },
            ));
            Ok(iterator)
        });

        let mut writer = Cursor::new(vec![]);

        write_file(
            &mut writer,
            row_groups,
            schema,
            parquet_schema,
            options,
            None,
        )?;

        Ok(writer.into_inner())
    }

    fn integration_read(data: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
        let reader = Cursor::new(data);
        let reader = read::RecordReader::try_new(reader, None, None, Arc::new(|_, _| true))?;
        let schema = reader.schema().clone();
        let batches = reader.collect::<Result<Vec<_>>>()?;

        Ok((schema, batches))
    }

    fn test_file(version: &str, file_name: &str) -> Result<()> {
        let (schema, batches) = read_gzip_json(version, file_name);

        let data = integration_write(&schema, &batches)?;

        let (read_schema, read_batches) = integration_read(&data)?;

        assert_eq!(&schema, read_schema.as_ref());
        assert_eq!(batches, read_batches);

        Ok(())
    }

    #[test]
    fn roundtrip_100_primitive() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_primitive")?;
        test_file("1.0.0-bigendian", "generated_primitive")
    }

    /// Tests that when arrow-specific types (Duration and LargeUtf8) are written to parquet, we can rountrip its
    /// logical types.
    #[test]
    fn test_arrow_type() -> Result<()> {
        let dt1 = DataType::Duration(TimeUnit::Second);
        let array = PrimitiveArray::<i64>::from([Some(1), None, Some(2)]).to(dt1.clone());
        let array2 = Utf8Array::<i64>::from([Some("a"), None, Some("bb")]);
        let schema = Schema::new(vec![
            Field::new("a1", dt1, true),
            Field::new("a2", array2.data_type().clone(), true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(array), Arc::new(array2)],
        )?;

        let r = integration_write(&schema, &[batch.clone()])?;

        let (new_schema, new_batches) = integration_read(&r)?;

        assert_eq!(new_schema.as_ref(), &schema);
        assert_eq!(new_batches, vec![batch]);
        Ok(())
    }
}
