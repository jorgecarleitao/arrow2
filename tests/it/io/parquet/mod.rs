use std::io::{Cursor, Read, Seek};

use arrow2::{
    array::*,
    bitmap::Bitmap,
    buffer::Buffer,
    chunk::Chunk,
    datatypes::*,
    error::Result,
    io::parquet::read::statistics::*,
    io::parquet::read::*,
    io::parquet::write::*,
    types::{days_ms, NativeType},
};

#[cfg(feature = "io_json_integration")]
mod integration;
mod read;
mod read_indexes;
mod write;
mod write_async;

type ArrayStats = (Box<dyn Array>, Statistics);

pub fn read_column<R: Read + Seek>(mut reader: R, column: &str) -> Result<ArrayStats> {
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

    let statistics = deserialize(field, &metadata.row_groups)?;

    Ok((
        reader.next().unwrap()?.into_arrays().pop().unwrap(),
        statistics,
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
    let offsets = Buffer::from(vec![0, 2, 2, 5, 8, 8, 11, 11, 12]);

    let values = match column {
        "list_int64" => {
            // [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
            PrimitiveArray::<i64>::from(&[
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
            ])
            .boxed()
        }
        "list_int64_required" | "list_int64_optional_required" | "list_int64_required_required" => {
            // [[0, 1], None, [2, 0, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
            PrimitiveArray::<i64>::from(&[
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
            ])
            .boxed()
        }
        "list_int16" => PrimitiveArray::<i16>::from(&[
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
        ])
        .boxed(),
        "list_bool" => BooleanArray::from(&[
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
        ])
        .boxed(),
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
        "list_utf8" => Box::new(Utf8Array::<i32>::from(&[
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
        "list_large_binary" => Box::new(BinaryArray::<i64>::from(&[
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
            Box::new(NullArray::from_data(DataType::Null, 1))
        }
        other => unreachable!("{}", other),
    };

    match column {
        "list_int64_required_required" => {
            // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
            let data_type = DataType::List(Box::new(Field::new("item", DataType::Int64, false)));
            Box::new(ListArray::<i32>::from_data(
                data_type, offsets, values, None,
            ))
        }
        "list_int64_optional_required" => {
            // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
            let data_type = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
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
                other => unreachable!("{}", other),
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
        Some(-256),
        Some(-1),
        None,
        Some(3),
        None,
        Some(5),
        Some(6),
        Some(7),
        None,
        Some(9),
    ];
    let u32_values = &[
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
        "timestamp_ms" => Box::new(
            PrimitiveArray::<i64>::from_iter(u32_values.iter().map(|x| x.map(|x| x as i64)))
                .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
        ),
        "uint32" => Box::new(PrimitiveArray::<u32>::from(u32_values)),
        "int32_dict" => {
            let keys = PrimitiveArray::<i32>::from([Some(0), Some(1), None, Some(1)]);
            let values = Box::new(PrimitiveArray::<i32>::from_slice([10, 200]));
            Box::new(DictionaryArray::try_from_keys(keys, values).unwrap())
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

pub fn pyarrow_nullable_statistics(column: &str) -> Statistics {
    match column {
        "int64" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(Int64Array::from_slice([-256])),
            max_value: Box::new(Int64Array::from_slice([9])),
        },
        "float64" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(Float64Array::from_slice([0.0])),
            max_value: Box::new(Float64Array::from_slice([9.0])),
        },
        "string" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(4)])),
            min_value: Box::new(Utf8Array::<i32>::from_slice([""])),
            max_value: Box::new(Utf8Array::<i32>::from_slice(["def"])),
        },
        "bool" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(4)])),
            min_value: Box::new(BooleanArray::from_slice([false])),
            max_value: Box::new(BooleanArray::from_slice([true])),
        },
        "timestamp_ms" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(
                Int64Array::from_slice([0]).to(DataType::Timestamp(TimeUnit::Millisecond, None)),
            ),
            max_value: Box::new(
                Int64Array::from_slice([9]).to(DataType::Timestamp(TimeUnit::Millisecond, None)),
            ),
        },
        "uint32" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(UInt32Array::from_slice([0])),
            max_value: Box::new(UInt32Array::from_slice([9])),
        },
        "int32_dict" => {
            let new_dict = |array: Box<dyn Array>| -> Box<dyn Array> {
                Box::new(DictionaryArray::try_from_keys(vec![Some(0)].into(), array).unwrap())
            };

            Statistics {
                distinct_count: Count::Single(UInt64Array::from([None])),
                null_count: Count::Single(UInt64Array::from([Some(0)])),
                min_value: new_dict(Box::new(Int32Array::from_slice([10]))),
                max_value: new_dict(Box::new(Int32Array::from_slice([200]))),
            }
        }
        "decimal_9" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(Int128Array::from_slice([-256]).to(DataType::Decimal(9, 0))),
            max_value: Box::new(Int128Array::from_slice([9]).to(DataType::Decimal(9, 0))),
        },
        "decimal_18" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(Int128Array::from_slice([-256]).to(DataType::Decimal(18, 0))),
            max_value: Box::new(Int128Array::from_slice([9]).to(DataType::Decimal(18, 0))),
        },
        "decimal_26" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(Int128Array::from_slice([-256]).to(DataType::Decimal(26, 0))),
            max_value: Box::new(Int128Array::from_slice([9]).to(DataType::Decimal(26, 0))),
        },
        "timestamp_us" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(
                Int64Array::from_slice([-256]).to(DataType::Timestamp(TimeUnit::Microsecond, None)),
            ),
            max_value: Box::new(
                Int64Array::from_slice([9]).to(DataType::Timestamp(TimeUnit::Microsecond, None)),
            ),
        },
        "timestamp_s" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(
                Int64Array::from_slice([-256]).to(DataType::Timestamp(TimeUnit::Second, None)),
            ),
            max_value: Box::new(
                Int64Array::from_slice([9]).to(DataType::Timestamp(TimeUnit::Second, None)),
            ),
        },
        "timestamp_s_utc" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(3)])),
            min_value: Box::new(Int64Array::from_slice([-256]).to(DataType::Timestamp(
                TimeUnit::Second,
                Some("UTC".to_string()),
            ))),
            max_value: Box::new(Int64Array::from_slice([9]).to(DataType::Timestamp(
                TimeUnit::Second,
                Some("UTC".to_string()),
            ))),
        },
        _ => unreachable!(),
    }
}

// these values match the values in `integration`
pub fn pyarrow_required(column: &str) -> Box<dyn Array> {
    let i64_values = &[
        Some(-256),
        Some(-1),
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

pub fn pyarrow_required_statistics(column: &str) -> Statistics {
    let mut s = pyarrow_nullable_statistics(column);
    s.null_count = Count::Single(UInt64Array::from([Some(0)]));
    s
}

pub fn pyarrow_nested_nullable_statistics(column: &str) -> Statistics {
    let new_list = |array: Box<dyn Array>, nullable: bool| {
        Box::new(ListArray::<i32>::new(
            DataType::List(Box::new(Field::new(
                "item",
                array.data_type().clone(),
                nullable,
            ))),
            vec![0, array.len() as i32].into(),
            array,
            None,
        )) as Box<dyn Array>
    };

    match column {
        "list_int16" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(1)])),
            min_value: new_list(Box::new(Int16Array::from_slice([0])), true),
            max_value: new_list(Box::new(Int16Array::from_slice([10])), true),
        },
        "list_bool" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(1)])),
            min_value: new_list(Box::new(BooleanArray::from_slice([false])), true),
            max_value: new_list(Box::new(BooleanArray::from_slice([true])), true),
        },
        "list_utf8" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single([Some(1)].into()),
            min_value: new_list(Box::new(Utf8Array::<i32>::from_slice([""])), true),
            max_value: new_list(Box::new(Utf8Array::<i32>::from_slice(["ccc"])), true),
        },
        "list_large_binary" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single([Some(1)].into()),
            min_value: new_list(Box::new(BinaryArray::<i64>::from_slice([b""])), true),
            max_value: new_list(Box::new(BinaryArray::<i64>::from_slice([b"ccc"])), true),
        },
        "list_int64" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single([Some(1)].into()),
            min_value: new_list(Box::new(Int64Array::from_slice([0])), true),
            max_value: new_list(Box::new(Int64Array::from_slice([10])), true),
        },
        "list_int64_required" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single([Some(1)].into()),
            min_value: new_list(Box::new(Int64Array::from_slice([0])), false),
            max_value: new_list(Box::new(Int64Array::from_slice([10])), false),
        },
        "list_int64_required_required" | "list_int64_optional_required" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single([Some(0)].into()),
            min_value: new_list(Box::new(Int64Array::from_slice([0])), false),
            max_value: new_list(Box::new(Int64Array::from_slice([10])), false),
        },
        "list_nested_i64" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single([Some(2)].into()),
            min_value: new_list(new_list(Box::new(Int64Array::from_slice([0])), true), true),
            max_value: new_list(new_list(Box::new(Int64Array::from_slice([10])), true), true),
        },
        "list_nested_inner_required_required_i64" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single([Some(0)].into()),
            min_value: new_list(new_list(Box::new(Int64Array::from_slice([0])), true), true),
            max_value: new_list(new_list(Box::new(Int64Array::from_slice([10])), true), true),
        },
        "list_nested_inner_required_i64" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single([Some(0)].into()),
            min_value: new_list(new_list(Box::new(Int64Array::from_slice([0])), true), true),
            max_value: new_list(new_list(Box::new(Int64Array::from_slice([10])), true), true),
        },
        other => todo!("{}", other),
    }
}

pub fn pyarrow_nested_edge_statistics(column: &str) -> Statistics {
    let new_list = |array: Box<dyn Array>| {
        Box::new(ListArray::<i32>::new(
            DataType::List(Box::new(Field::new(
                "item",
                array.data_type().clone(),
                true,
            ))),
            vec![0, array.len() as i32].into(),
            array,
            None,
        ))
    };

    match column {
        "simple" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(0)])),
            min_value: new_list(Box::new(Int64Array::from([Some(0)]))),
            max_value: new_list(Box::new(Int64Array::from([Some(1)]))),
        },
        "null" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(1)])),
            min_value: new_list(Box::new(Int64Array::from([None]))),
            max_value: new_list(Box::new(Int64Array::from([None]))),
        },
        _ => unreachable!(),
    }
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
    let boolean = BooleanArray::from(boolean).boxed();
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
            let values = vec![Utf8Array::<i32>::from(string).boxed(), boolean];
            StructArray::from_data(DataType::Struct(fields), values, None).boxed()
        }
        "struct_struct" => {
            let struct_ = pyarrow_struct("struct");
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

pub fn pyarrow_struct_statistics(column: &str) -> Statistics {
    let new_struct = |arrays: Vec<Box<dyn Array>>, names: Vec<String>| {
        let fields = names
            .into_iter()
            .zip(arrays.iter())
            .map(|(n, a)| Field::new(n, a.data_type().clone(), true))
            .collect();
        StructArray::new(DataType::Struct(fields), arrays, None)
    };

    let names = vec!["f1".to_string(), "f2".to_string()];

    match column {
        "struct" => Statistics {
            distinct_count: Count::Struct(new_struct(
                vec![
                    Box::new(UInt64Array::from([None])),
                    Box::new(UInt64Array::from([None])),
                ],
                names.clone(),
            )),
            null_count: Count::Struct(new_struct(
                vec![
                    Box::new(UInt64Array::from([Some(4)])),
                    Box::new(UInt64Array::from([Some(4)])),
                ],
                names.clone(),
            )),
            min_value: Box::new(new_struct(
                vec![
                    Box::new(Utf8Array::<i32>::from_slice([""])),
                    Box::new(BooleanArray::from_slice([false])),
                ],
                names.clone(),
            )),
            max_value: Box::new(new_struct(
                vec![
                    Box::new(Utf8Array::<i32>::from_slice(["def"])),
                    Box::new(BooleanArray::from_slice([true])),
                ],
                names,
            )),
        },
        "struct_struct" => Statistics {
            distinct_count: Count::Single(UInt64Array::from([None])),
            null_count: Count::Single(UInt64Array::from([Some(1)])),
            min_value: Box::new(BooleanArray::from_slice([false])),
            max_value: Box::new(BooleanArray::from_slice([true])),
        },
        _ => todo!(),
    }
}

pub fn pyarrow_map(column: &str) -> Box<dyn Array> {
    match column {
        "map" => {
            let s1 = [Some("a1"), Some("a2")];
            let s2 = [Some("b1"), Some("b2")];
            let dt = DataType::Struct(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ]);
            MapArray::try_new(
                DataType::Map(Box::new(Field::new("entries", dt.clone(), false)), false),
                vec![0, 2].into(),
                StructArray::try_new(
                    dt,
                    vec![
                        Utf8Array::<i32>::from(s1).boxed(),
                        Utf8Array::<i32>::from(s2).boxed(),
                    ],
                    None,
                )
                .unwrap()
                .boxed(),
                None,
            )
            .unwrap()
            .boxed()
        }
        "map_nullable" => {
            let s1 = [Some("a1"), Some("a2")];
            let s2 = [Some("b1"), None];
            let dt = DataType::Struct(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ]);
            MapArray::try_new(
                DataType::Map(Box::new(Field::new("entries", dt.clone(), false)), false),
                vec![0, 2].into(),
                StructArray::try_new(
                    dt,
                    vec![
                        Utf8Array::<i32>::from(s1).boxed(),
                        Utf8Array::<i32>::from(s2).boxed(),
                    ],
                    None,
                )
                .unwrap()
                .boxed(),
                None,
            )
            .unwrap()
            .boxed()
        }
        _ => unreachable!(),
    }
}

pub fn pyarrow_map_statistics(column: &str) -> Statistics {
    let new_map = |arrays: Vec<Box<dyn Array>>, names: Vec<String>| {
        let fields = names
            .into_iter()
            .zip(arrays.iter())
            .map(|(n, a)| Field::new(n, a.data_type().clone(), true))
            .collect::<Vec<_>>();
        MapArray::new(
            DataType::Map(
                Box::new(Field::new("items", DataType::Struct(fields.clone()), false)),
                false,
            ),
            vec![0, arrays[0].len() as i32].into(),
            StructArray::new(DataType::Struct(fields), arrays, None).boxed(),
            None,
        )
    };

    let names = vec!["key".to_string(), "value".to_string()];

    match column {
        "map" => Statistics {
            distinct_count: Count::Map(new_map(
                vec![
                    UInt64Array::from([None]).boxed(),
                    UInt64Array::from([None]).boxed(),
                ],
                names.clone(),
            )),
            null_count: Count::Map(new_map(
                vec![
                    UInt64Array::from([Some(0)]).boxed(),
                    UInt64Array::from([Some(0)]).boxed(),
                ],
                names.clone(),
            )),
            min_value: Box::new(new_map(
                vec![
                    Utf8Array::<i32>::from_slice(["a1"]).boxed(),
                    Utf8Array::<i32>::from_slice(["b1"]).boxed(),
                ],
                names.clone(),
            )),
            max_value: Box::new(new_map(
                vec![
                    Utf8Array::<i32>::from_slice(["a2"]).boxed(),
                    Utf8Array::<i32>::from_slice(["b2"]).boxed(),
                ],
                names,
            )),
        },
        "map_nullable" => Statistics {
            distinct_count: Count::Map(new_map(
                vec![
                    UInt64Array::from([None]).boxed(),
                    UInt64Array::from([None]).boxed(),
                ],
                names.clone(),
            )),
            null_count: Count::Map(new_map(
                vec![
                    UInt64Array::from([Some(0)]).boxed(),
                    UInt64Array::from([Some(1)]).boxed(),
                ],
                names.clone(),
            )),
            min_value: Box::new(new_map(
                vec![
                    Utf8Array::<i32>::from_slice(["a1"]).boxed(),
                    Utf8Array::<i32>::from_slice(["b1"]).boxed(),
                ],
                names.clone(),
            )),
            max_value: Box::new(new_map(
                vec![
                    Utf8Array::<i32>::from_slice(["a2"]).boxed(),
                    Utf8Array::<i32>::from_slice(["b1"]).boxed(),
                ],
                names,
            )),
        },
        _ => unreachable!(),
    }
}

fn integration_write(schema: &Schema, chunks: &[Chunk<Box<dyn Array>>]) -> Result<Vec<u8>> {
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V1,
    };

    let encodings = schema
        .fields
        .iter()
        .map(|x| {
            vec![if let DataType::Dictionary(..) = x.data_type() {
                Encoding::RleDictionary
            } else {
                Encoding::Plain
            }]
        })
        .collect();

    let row_groups =
        RowGroupIterator::try_new(chunks.iter().cloned().map(Ok), schema, options, encodings)?;

    let writer = Cursor::new(vec![]);

    let mut writer = FileWriter::try_new(writer, schema.clone(), options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    writer.end(None)?;

    Ok(writer.into_inner().into_inner())
}

type IntegrationRead = (Schema, Vec<Chunk<Box<dyn Array>>>);

fn integration_read(data: &[u8]) -> Result<IntegrationRead> {
    let reader = Cursor::new(data);
    let reader = FileReader::try_new(reader, None, None, None, None)?;
    let schema = reader.schema().clone();

    for field in &schema.fields {
        let mut _statistics = deserialize(field, &reader.metadata().row_groups)?;
    }

    let batches = reader.collect::<Result<Vec<_>>>()?;

    Ok((schema, batches))
}

/// Tests that when arrow-specific types (Duration and LargeUtf8) are written to parquet, we can rountrip its
/// logical types.
#[test]
fn arrow_type() -> Result<()> {
    let array1 = PrimitiveArray::<i64>::from([Some(1), None, Some(2)])
        .to(DataType::Duration(TimeUnit::Second));
    let array2 = Utf8Array::<i64>::from([Some("a"), None, Some("bb")]);

    let indices = PrimitiveArray::from_values((0..3u64).map(|x| x % 2));
    let values = PrimitiveArray::from_slice([1.0f32, 3.0]).boxed();
    let array3 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = BinaryArray::<i32>::from_slice([b"ab", b"ac"]).boxed();
    let array4 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = FixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(2),
        vec![b'a', b'b', b'a', b'c'].into(),
        None,
    )
    .boxed();
    let array5 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1i16, 3]).boxed();
    let array6 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1i64, 3])
        .to(DataType::Timestamp(
            TimeUnit::Millisecond,
            Some("UTC".to_string()),
        ))
        .boxed();
    let array7 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1.0f64, 3.0]).boxed();
    let array8 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1u8, 3]).boxed();
    let array9 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1u16, 3]).boxed();
    let array10 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1u32, 3]).boxed();
    let array11 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1u64, 3]).boxed();
    let array12 = DictionaryArray::try_from_keys(indices, values).unwrap();

    let array13 = PrimitiveArray::<i32>::from_slice([1, 2, 3])
        .to(DataType::Interval(IntervalUnit::YearMonth));

    let array14 =
        PrimitiveArray::<days_ms>::from_slice([days_ms(1, 1), days_ms(2, 2), days_ms(3, 3)])
            .to(DataType::Interval(IntervalUnit::DayTime));

    let schema = Schema::from(vec![
        Field::new("a1", array1.data_type().clone(), true),
        Field::new("a2", array2.data_type().clone(), true),
        Field::new("a3", array3.data_type().clone(), true),
        Field::new("a4", array4.data_type().clone(), true),
        Field::new("a5", array5.data_type().clone(), true),
        Field::new("a5a", array5.data_type().clone(), false),
        Field::new("a6", array6.data_type().clone(), true),
        Field::new("a7", array7.data_type().clone(), true),
        Field::new("a8", array8.data_type().clone(), true),
        Field::new("a9", array9.data_type().clone(), true),
        Field::new("a10", array10.data_type().clone(), true),
        Field::new("a11", array11.data_type().clone(), true),
        Field::new("a12", array12.data_type().clone(), true),
        Field::new("a13", array13.data_type().clone(), true),
        Field::new("a14", array14.data_type().clone(), true),
    ]);
    let chunk = Chunk::try_new(vec![
        array1.boxed(),
        array2.boxed(),
        array3.boxed(),
        array4.boxed(),
        array5.clone().boxed(),
        array5.boxed(),
        array6.boxed(),
        array7.boxed(),
        array8.boxed(),
        array9.boxed(),
        array10.boxed(),
        array11.boxed(),
        array12.boxed(),
        array13.boxed(),
        array14.boxed(),
    ])?;

    let r = integration_write(&schema, &[chunk.clone()])?;

    let (new_schema, new_chunks) = integration_read(&r)?;

    assert_eq!(new_schema, schema);
    assert_eq!(new_chunks, vec![chunk]);
    Ok(())
}

fn data<T: NativeType, I: Iterator<Item = T>>(
    mut iter: I,
    inner_is_nullable: bool,
) -> ListArray<i32> {
    // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
    let data = vec![
        Some(vec![Some(iter.next().unwrap()), Some(iter.next().unwrap())]),
        Some(vec![]),
        Some(vec![
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
        ]),
        Some(vec![
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
        ]),
        Some(vec![]),
        Some(vec![
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
        ]),
        Some(vec![]),
        Some(vec![Some(iter.next().unwrap())]),
    ];
    let mut array = MutableListArray::<i32, _>::new_with_field(
        MutablePrimitiveArray::<T>::new(),
        "item",
        inner_is_nullable,
    );
    array.try_extend(data).unwrap();
    array.into()
}

fn list_array_generic<O: Offset>(is_nullable: bool, array: ListArray<O>) -> Result<()> {
    let schema = Schema::from(vec![Field::new(
        "a1",
        array.data_type().clone(),
        is_nullable,
    )]);
    let chunk = Chunk::try_new(vec![array.boxed()])?;

    let r = integration_write(&schema, &[chunk.clone()])?;

    let (new_schema, new_chunks) = integration_read(&r)?;

    assert_eq!(new_schema, schema);
    assert_eq!(new_chunks, vec![chunk]);
    Ok(())
}

#[test]
fn list_array_required_required() -> Result<()> {
    list_array_generic(false, data(0..12i8, false))?;
    list_array_generic(false, data(0..12i16, false))?;
    list_array_generic(false, data(0..12i32, false))?;
    list_array_generic(false, data(0..12i64, false))?;
    list_array_generic(false, data(0..12u8, false))?;
    list_array_generic(false, data(0..12u16, false))?;
    list_array_generic(false, data(0..12u32, false))?;
    list_array_generic(false, data(0..12u64, false))?;
    list_array_generic(false, data((0..12).map(|x| (x as f32) * 1.0), false))?;
    list_array_generic(false, data((0..12).map(|x| (x as f64) * 1.0f64), false))
}

#[test]
fn list_array_optional_optional() -> Result<()> {
    list_array_generic(true, data(0..12, true))
}

#[test]
fn list_array_required_optional() -> Result<()> {
    list_array_generic(true, data(0..12, false))
}

#[test]
fn list_array_optional_required() -> Result<()> {
    list_array_generic(false, data(0..12, true))
}

#[test]
fn list_utf8() -> Result<()> {
    let data = vec![
        Some(vec![Some("a".to_string())]),
        Some(vec![]),
        Some(vec![Some("b".to_string())]),
    ];
    let mut array =
        MutableListArray::<i32, _>::new_with_field(MutableUtf8Array::<i32>::new(), "item", true);
    array.try_extend(data).unwrap();
    list_array_generic(false, array.into())
}

#[test]
fn list_large_utf8() -> Result<()> {
    let data = vec![
        Some(vec![Some("a".to_string())]),
        Some(vec![]),
        Some(vec![Some("b".to_string())]),
    ];
    let mut array =
        MutableListArray::<i32, _>::new_with_field(MutableUtf8Array::<i64>::new(), "item", true);
    array.try_extend(data).unwrap();
    list_array_generic(false, array.into())
}

#[test]
fn list_binary() -> Result<()> {
    let data = vec![
        Some(vec![Some(b"a".to_vec())]),
        Some(vec![]),
        Some(vec![Some(b"b".to_vec())]),
    ];
    let mut array =
        MutableListArray::<i32, _>::new_with_field(MutableBinaryArray::<i32>::new(), "item", true);
    array.try_extend(data).unwrap();
    list_array_generic(false, array.into())
}

#[test]
fn large_list_large_binary() -> Result<()> {
    let data = vec![
        Some(vec![Some(b"a".to_vec())]),
        Some(vec![]),
        Some(vec![Some(b"b".to_vec())]),
    ];
    let mut array =
        MutableListArray::<i64, _>::new_with_field(MutableBinaryArray::<i64>::new(), "item", true);
    array.try_extend(data).unwrap();
    list_array_generic(false, array.into())
}
