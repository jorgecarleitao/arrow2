use std::io::{Cursor, Read, Seek};
use std::sync::Arc;

use arrow2::{
    array::*, bitmap::Bitmap, buffer::Buffer, chunk::Chunk, datatypes::*, error::Result,
    io::parquet::read::statistics::*, io::parquet::read::*, io::parquet::write::*,
};

use crate::io::ipc::read_gzip_json;

mod read;
mod read_indexes;
mod write;
mod write_async;

type ArrayStats = (Arc<dyn Array>, Option<Box<dyn Statistics>>);

pub fn read_column<R: Read + Seek>(
    mut reader: R,
    row_group: usize,
    column: &str,
) -> Result<ArrayStats> {
    let metadata = read_metadata(&mut reader)?;
    let schema = infer_schema(&metadata)?;

    // verify that we can read indexes
    let _indexes = read_columns_indexes(
        &mut reader,
        metadata.row_groups[0].columns(),
        &schema.fields,
    )?;

    let column = schema
        .fields
        .iter()
        .enumerate()
        .find_map(|(i, f)| if f.name == column { Some(i) } else { None })
        .unwrap();

    let mut reader = FileReader::try_new(reader, Some(&[column]), None, None, None)?;

    let field = &schema.fields[column];

    let mut statistics = deserialize_statistics(field, metadata.row_groups[row_group].columns())?;

    Ok((
        reader.next().unwrap()?.into_arrays().pop().unwrap(),
        statistics.pop().unwrap(),
    ))
}

pub fn pyarrow_nested_edge(column: &str) -> Box<dyn Array> {
    match column {
        "simple" => {
            // [[0, 1]]
            let data = [Some(vec![Some(0), Some(1)])];
            let mut a = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        "null" => {
            // [None]
            let data = [None::<Vec<Option<i64>>>];
            let mut a = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        _ => todo!(),
    }
}

pub fn pyarrow_nested_nullable(column: &str) -> Box<dyn Array> {
    let offsets = Buffer::from_slice([0, 2, 2, 5, 8, 8, 11, 11, 12]);

    let values = match column {
        "list_int64" => {
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
        "list_int64_required" | "list_int64_required_required" => {
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
        "list_int16" => Arc::new(PrimitiveArray::<i16>::from(&[
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
        "list_bool" => Arc::new(BooleanArray::from(&[
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
        "list_utf8" => Arc::new(Utf8Array::<i32>::from(&[
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
        "list_large_binary" => Arc::new(BinaryArray::<i64>::from(&[
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
        "list_nested_i64"
        | "list_nested_inner_required_i64"
        | "list_nested_inner_required_required_i64" => {
            Arc::new(NullArray::from_data(DataType::Null, 1))
        }
        _ => unreachable!(),
    };

    match column {
        "list_int64_required_required" => {
            // [[0, 1], [], [2, None, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
            let data_type = DataType::List(Box::new(Field::new("item", DataType::Int64, false)));
            Box::new(ListArray::<i32>::from_data(
                data_type, offsets, values, None,
            ))
        }
        "list_nested_i64" => {
            // [[0, 1]], None, [[2, None], [3]], [[4, 5], [6]], [], [[7], None, [9]], [[], [None], None], [[10]]
            let data = [
                Some(vec![Some(vec![Some(0), Some(1)])]),
                None,
                Some(vec![Some(vec![Some(2), None]), Some(vec![Some(3)])]),
                Some(vec![Some(vec![Some(4), Some(5)]), Some(vec![Some(6)])]),
                Some(vec![]),
                Some(vec![Some(vec![Some(7)]), None, Some(vec![Some(9)])]),
                Some(vec![Some(vec![]), Some(vec![None]), None]),
                Some(vec![Some(vec![Some(10)])]),
            ];
            let mut a =
                MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i64>>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        "list_nested_inner_required_i64" => {
            let data = [
                Some(vec![Some(vec![Some(0), Some(1)])]),
                None,
                Some(vec![Some(vec![Some(2), Some(3)]), Some(vec![Some(3)])]),
                Some(vec![Some(vec![Some(4), Some(5)]), Some(vec![Some(6)])]),
                Some(vec![]),
                Some(vec![Some(vec![Some(7)]), None, Some(vec![Some(9)])]),
                None,
                Some(vec![Some(vec![Some(10)])]),
            ];
            let mut a =
                MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i64>>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        "list_nested_inner_required_required_i64" => {
            let data = [
                Some(vec![Some(vec![Some(0), Some(1)])]),
                None,
                Some(vec![Some(vec![Some(2), Some(3)]), Some(vec![Some(3)])]),
                Some(vec![Some(vec![Some(4), Some(5)]), Some(vec![Some(6)])]),
                Some(vec![]),
                Some(vec![
                    Some(vec![Some(7)]),
                    Some(vec![Some(8)]),
                    Some(vec![Some(9)]),
                ]),
                None,
                Some(vec![Some(vec![Some(10)])]),
            ];
            let mut a =
                MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i64>>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        _ => {
            let field = match column {
                "list_int64" => Field::new("item", DataType::Int64, true),
                "list_int64_required" => Field::new("item", DataType::Int64, false),
                "list_int16" => Field::new("item", DataType::Int16, true),
                "list_bool" => Field::new("item", DataType::Boolean, true),
                "list_utf8" => Field::new("item", DataType::Utf8, true),
                "list_large_binary" => Field::new("item", DataType::LargeBinary, true),
                _ => unreachable!(),
            };

            let validity = Some(Bitmap::from([
                true, false, true, true, true, true, false, true,
            ]));
            // [0, 2, 2, 5, 8, 8, 11, 11, 12]
            // [[a1, a2], None, [a3, a4, a5], [a6, a7, a8], [], [a9, a10, a11], None, [a12]]
            let data_type = DataType::List(Box::new(field));
            Box::new(ListArray::<i32>::from_data(
                data_type, offsets, values, validity,
            ))
        }
    }
}

pub fn pyarrow_nullable(column: &str) -> Box<dyn Array> {
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
        "int64" => Box::new(PrimitiveArray::<i64>::from(i64_values)),
        "float64" => Box::new(PrimitiveArray::<f64>::from(&[
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
        "string" => Box::new(Utf8Array::<i32>::from(&[
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
        "bool" => Box::new(BooleanArray::from(&[
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
        "date" => Box::new(
            PrimitiveArray::<i64>::from(i64_values)
                .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
        ),
        "uint32" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as u32))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<u32>::from(values))
        }
        "int32_dict" => {
            let keys = PrimitiveArray::<i32>::from([Some(0), Some(1), None, Some(1)]);
            let values = Arc::new(PrimitiveArray::<i32>::from_slice([10, 200]));
            Box::new(DictionaryArray::<i32>::from_data(keys, values))
        }
        "decimal_9" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(9, 0)))
        }
        "decimal_18" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(18, 0)))
        }
        "decimal_26" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(26, 0)))
        }
        "timestamp_us" => Box::new(
            PrimitiveArray::<i64>::from(i64_values)
                .to(DataType::Timestamp(TimeUnit::Microsecond, None)),
        ),
        "timestamp_s" => Box::new(
            PrimitiveArray::<i64>::from(i64_values).to(DataType::Timestamp(TimeUnit::Second, None)),
        ),
        "timestamp_s_utc" => Box::new(PrimitiveArray::<i64>::from(i64_values).to(
            DataType::Timestamp(TimeUnit::Second, Some("UTC".to_string())),
        )),
        _ => unreachable!(),
    }
}

pub fn pyarrow_nullable_statistics(column: &str) -> Option<Box<dyn Statistics>> {
    Some(match column {
        "int64" => Box::new(PrimitiveStatistics::<i64> {
            data_type: DataType::Int64,
            distinct_count: None,
            null_count: Some(3),
            min_value: Some(0),
            max_value: Some(9),
        }),
        "float64" => Box::new(PrimitiveStatistics::<f64> {
            data_type: DataType::Float64,
            distinct_count: None,
            null_count: Some(3),
            min_value: Some(0.0),
            max_value: Some(9.0),
        }),
        "string" => Box::new(Utf8Statistics {
            null_count: Some(4),
            distinct_count: None,
            min_value: Some("".to_string()),
            max_value: Some("def".to_string()),
        }),
        "bool" => Box::new(BooleanStatistics {
            null_count: Some(4),
            distinct_count: None,

            min_value: Some(false),
            max_value: Some(true),
        }),
        "date" => Box::new(PrimitiveStatistics::<i64> {
            data_type: DataType::Timestamp(TimeUnit::Millisecond, None),
            distinct_count: None,
            null_count: Some(3),
            min_value: Some(0),
            max_value: Some(9),
        }),
        "uint32" => Box::new(PrimitiveStatistics::<u32> {
            data_type: DataType::UInt32,
            null_count: Some(3),
            distinct_count: None,

            min_value: Some(0),
            max_value: Some(9),
        }),
        "int32_dict" => Box::new(PrimitiveStatistics {
            data_type: DataType::Dictionary(IntegerType::Int32, Box::new(DataType::Int32), false),
            null_count: Some(0),
            distinct_count: None,
            min_value: Some(10),
            max_value: Some(200),
        }),
        "decimal_9" => Box::new(PrimitiveStatistics::<i128> {
            distinct_count: None,
            null_count: Some(3),
            min_value: Some(0i128),
            max_value: Some(9i128),
            data_type: DataType::Decimal(9, 0),
        }),
        "decimal_18" => Box::new(PrimitiveStatistics::<i128> {
            distinct_count: None,
            null_count: Some(3),
            min_value: Some(0i128),
            max_value: Some(9i128),
            data_type: DataType::Decimal(18, 0),
        }),
        "decimal_26" => Box::new(PrimitiveStatistics::<i128> {
            distinct_count: None,
            null_count: Some(3),
            min_value: Some(0i128),
            max_value: Some(9i128),
            data_type: DataType::Decimal(26, 0),
        }),
        "timestamp_us" => Box::new(PrimitiveStatistics::<i64> {
            data_type: DataType::Timestamp(TimeUnit::Microsecond, None),
            distinct_count: None,
            null_count: Some(3),
            min_value: Some(0),
            max_value: Some(9),
        }),
        "timestamp_s" => Box::new(PrimitiveStatistics::<i64> {
            data_type: DataType::Timestamp(TimeUnit::Second, None),
            distinct_count: None,
            null_count: Some(3),
            min_value: Some(0),
            max_value: Some(9),
        }),
        "timestamp_s_utc" => Box::new(PrimitiveStatistics::<i64> {
            data_type: DataType::Timestamp(TimeUnit::Second, Some("UTC".to_string())),
            distinct_count: None,
            null_count: Some(3),
            min_value: Some(0),
            max_value: Some(9),
        }),
        _ => unreachable!(),
    })
}

// these values match the values in `integration`
pub fn pyarrow_required(column: &str) -> Box<dyn Array> {
    let i64_values = &[
        Some(-256),
        Some(-1),
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
    ];

    match column {
        "int64" => Box::new(PrimitiveArray::<i64>::from(i64_values)),
        "bool" => Box::new(BooleanArray::from_slice(&[
            true, true, false, false, false, true, true, true, true, true,
        ])),
        "string" => Box::new(Utf8Array::<i32>::from_slice(&[
            "Hello", "bbb", "aa", "", "bbb", "abc", "bbb", "bbb", "def", "aaa",
        ])),
        "decimal_9" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(9, 0)))
        }
        "decimal_18" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(18, 0)))
        }
        "decimal_26" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(26, 0)))
        }
        _ => unreachable!(),
    }
}

pub fn pyarrow_required_statistics(column: &str) -> Option<Box<dyn Statistics>> {
    Some(match column {
        "int64" => Box::new(PrimitiveStatistics::<i64> {
            data_type: DataType::Int64,
            null_count: Some(0),
            distinct_count: None,
            min_value: Some(0),
            max_value: Some(9),
        }),
        "bool" => Box::new(BooleanStatistics {
            null_count: Some(0),
            distinct_count: None,
            min_value: Some(false),
            max_value: Some(true),
        }),
        "string" => Box::new(Utf8Statistics {
            null_count: Some(0),
            distinct_count: None,
            min_value: Some("".to_string()),
            max_value: Some("def".to_string()),
        }),
        "decimal_9" => Box::new(PrimitiveStatistics::<i128> {
            distinct_count: None,
            null_count: Some(0),
            min_value: Some(0i128),
            max_value: Some(9i128),
            data_type: DataType::Decimal(9, 0),
        }),
        "decimal_18" => Box::new(PrimitiveStatistics::<i128> {
            distinct_count: None,
            null_count: Some(0),
            min_value: Some(0i128),
            max_value: Some(9i128),
            data_type: DataType::Decimal(18, 0),
        }),
        "decimal_26" => Box::new(PrimitiveStatistics::<i128> {
            distinct_count: None,
            null_count: Some(0),
            min_value: Some(0i128),
            max_value: Some(9i128),
            data_type: DataType::Decimal(26, 0),
        }),
        _ => unreachable!(),
    })
}

pub fn pyarrow_nested_nullable_statistics(column: &str) -> Option<Box<dyn Statistics>> {
    Some(match column {
        "list_int16" => Box::new(PrimitiveStatistics::<i16> {
            data_type: DataType::Int16,
            distinct_count: None,
            null_count: Some(1),
            min_value: Some(0),
            max_value: Some(10),
        }),
        "list_bool" => Box::new(BooleanStatistics {
            distinct_count: None,
            null_count: Some(1),
            min_value: Some(false),
            max_value: Some(true),
        }),
        "list_utf8" => Box::new(Utf8Statistics {
            distinct_count: None,
            null_count: Some(1),
            min_value: Some("".to_string()),
            max_value: Some("def".to_string()),
        }),
        "list_large_binary" => Box::new(BinaryStatistics {
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

pub fn pyarrow_nested_edge_statistics(column: &str) -> Option<Box<dyn Statistics>> {
    Some(match column {
        "simple" => Box::new(PrimitiveStatistics::<i64> {
            data_type: DataType::Int64,
            distinct_count: None,
            null_count: Some(0),
            min_value: Some(0),
            max_value: Some(1),
        }),
        "null" => Box::new(PrimitiveStatistics::<i64> {
            data_type: DataType::Int64,
            distinct_count: None,
            null_count: Some(0),
            min_value: None,
            max_value: None,
        }),
        _ => unreachable!(),
    })
}

pub fn pyarrow_struct(column: &str) -> Box<dyn Array> {
    let boolean = [
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
    ];
    let boolean = Arc::new(BooleanArray::from(boolean)) as Arc<dyn Array>;
    let fields = vec![
        Field::new("f1", DataType::Utf8, true),
        Field::new("f2", DataType::Boolean, true),
    ];
    match column {
        "struct" => {
            let string = [
                Some("Hello"),
                None,
                Some("aa"),
                Some(""),
                None,
                Some("abc"),
                None,
                None,
                Some("def"),
                Some("aaa"),
            ];
            let values = vec![
                Arc::new(Utf8Array::<i32>::from(string)) as Arc<dyn Array>,
                boolean,
            ];
            Box::new(StructArray::from_data(
                DataType::Struct(fields),
                values,
                None,
            ))
        }
        "struct_struct" => {
            let struct_ = pyarrow_struct("struct").into();
            let values = vec![struct_, boolean];
            Box::new(StructArray::from_data(
                DataType::Struct(vec![
                    Field::new("f1", DataType::Struct(fields), true),
                    Field::new("f2", DataType::Boolean, true),
                ]),
                values,
                None,
            ))
        }
        _ => todo!(),
    }
}

pub fn pyarrow_struct_statistics(column: &str) -> Option<Box<dyn Statistics>> {
    match column {
        "struct" => Some(Box::new(BooleanStatistics {
            distinct_count: None,
            null_count: Some(4),
            min_value: Some(false),
            max_value: Some(true),
        })),
        "struct_struct" => Some(Box::new(BooleanStatistics {
            distinct_count: None,
            null_count: Some(1),
            min_value: Some(false),
            max_value: Some(true),
        })),
        _ => todo!(),
    }
}

/// Round-trip with parquet using the same integration files used for IPC integration tests.
fn integration_write(schema: &Schema, batches: &[Chunk<Arc<dyn Array>>]) -> Result<Vec<u8>> {
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V1,
    };

    let encodings = schema
        .fields
        .iter()
        .map(|x| {
            if let DataType::Dictionary(..) = x.data_type() {
                Encoding::RleDictionary
            } else {
                Encoding::Plain
            }
        })
        .collect();

    let row_groups =
        RowGroupIterator::try_new(batches.iter().cloned().map(Ok), schema, options, encodings)?;

    let writer = Cursor::new(vec![]);

    let mut writer = FileWriter::try_new(writer, schema.clone(), options)?;

    writer.start()?;
    for group in row_groups {
        writer.write(group?)?;
    }
    writer.end(None)?;

    Ok(writer.into_inner().into_inner())
}

type IntegrationRead = (Schema, Vec<Chunk<Arc<dyn Array>>>);

fn integration_read(data: &[u8]) -> Result<IntegrationRead> {
    let reader = Cursor::new(data);
    let reader = FileReader::try_new(reader, None, None, None, None)?;
    let schema = reader.schema().clone();
    let batches = reader.collect::<Result<Vec<_>>>()?;

    Ok((schema, batches))
}

fn test_file(version: &str, file_name: &str) -> Result<()> {
    let (schema, _, batches) = read_gzip_json(version, file_name)?;

    // empty batches are not written/read from parquet and can be ignored
    let batches = batches
        .into_iter()
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();

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

#[test]
fn roundtrip_100_dict() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_dictionary")?;
    test_file("1.0.0-bigendian", "generated_dictionary")
}

#[test]
fn roundtrip_100_extension() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_extension")?;
    test_file("1.0.0-bigendian", "generated_extension")
}

/// Tests that when arrow-specific types (Duration and LargeUtf8) are written to parquet, we can rountrip its
/// logical types.
#[test]
fn arrow_type() -> Result<()> {
    let dt1 = DataType::Duration(TimeUnit::Second);
    let array = PrimitiveArray::<i64>::from([Some(1), None, Some(2)]).to(dt1.clone());
    let array2 = Utf8Array::<i64>::from([Some("a"), None, Some("bb")]);

    let indices = PrimitiveArray::from_values((0..3u64).map(|x| x % 2));
    let values = PrimitiveArray::from_slice([1.0f32, 3.0]);
    let array3 = DictionaryArray::from_data(indices.clone(), std::sync::Arc::new(values));

    let values = BinaryArray::<i32>::from_slice([b"ab", b"ac"]);
    let array4 = DictionaryArray::from_data(indices.clone(), std::sync::Arc::new(values));

    let values = FixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(2),
        vec![b'a', b'b', b'a', b'c'].into(),
        None,
    );
    let array5 = DictionaryArray::from_data(indices, std::sync::Arc::new(values));

    let schema = Schema::from(vec![
        Field::new("a1", dt1, true),
        Field::new("a2", array2.data_type().clone(), true),
        Field::new("a3", array3.data_type().clone(), true),
        Field::new("a4", array4.data_type().clone(), true),
        Field::new("a5", array5.data_type().clone(), true),
        Field::new("a6", array5.data_type().clone(), false),
    ]);
    let batch = Chunk::try_new(vec![
        Arc::new(array) as Arc<dyn Array>,
        Arc::new(array2),
        Arc::new(array3),
        Arc::new(array4),
        Arc::new(array5.clone()),
        Arc::new(array5),
    ])?;

    let r = integration_write(&schema, &[batch.clone()])?;

    let (new_schema, new_batches) = integration_read(&r)?;

    assert_eq!(new_schema, schema);
    assert_eq!(new_batches, vec![batch]);
    Ok(())
}
