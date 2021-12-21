use std::sync::Arc;

use arrow2::{
    array::*, bitmap::Bitmap, buffer::Buffer, datatypes::*, error::Result, io::print::*,
    record_batch::RecordBatch,
};

#[test]
fn write_basics() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Int32, true),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Utf8Array::<i32>::from(vec![
                Some("a"),
                Some("b"),
                None,
                Some("d"),
            ])),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(10), Some(100)])),
        ],
    )?;

    let table = write(&[batch]);

    let expected = vec![
        "+---+-----+",
        "| a | b   |",
        "+---+-----+",
        "| a | 1   |",
        "| b |     |",
        "|   | 10  |",
        "| d | 100 |",
        "+---+-----+",
    ];

    let actual: Vec<&str> = table.lines().collect();

    assert_eq!(expected, actual, "Actual result:\n{}", table);

    Ok(())
}

#[test]
fn write_null() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Null, true),
    ]));

    let num_rows = 4;
    let arrays = schema
        .fields()
        .iter()
        .map(|f| new_null_array(f.data_type().clone(), num_rows).into())
        .collect();

    // define data (null)
    let batch = RecordBatch::try_new(schema, arrays)?;

    let table = write(&[batch]);

    let expected = vec![
        "+---+---+---+",
        "| a | b | c |",
        "+---+---+---+",
        "|   |   |   |",
        "|   |   |   |",
        "|   |   |   |",
        "|   |   |   |",
        "+---+---+---+",
    ];

    let actual: Vec<&str> = table.lines().collect();

    assert_eq!(expected, actual, "Actual result:\n{:#?}", table);
    Ok(())
}

#[test]
fn write_dictionary() -> Result<()> {
    // define a schema.
    let field_type = DataType::Dictionary(i32::KEY_TYPE, Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![Field::new("d1", field_type, true)]));

    let mut array = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();

    array.try_extend(vec![Some("one"), None, Some("three")])?;
    let array = array.into_arc();

    let batch = RecordBatch::try_new(schema, vec![array])?;

    let table = write(&[batch]);

    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| one   |",
        "|       |",
        "| three |",
        "+-------+",
    ];

    let actual: Vec<&str> = table.lines().collect();

    assert_eq!(expected, actual, "Actual result:\n{}", table);

    Ok(())
}

#[test]
fn dictionary_validities() -> Result<()> {
    // define a schema.
    let field_type = DataType::Dictionary(i32::KEY_TYPE, Box::new(DataType::Int32));
    let schema = Arc::new(Schema::new(vec![Field::new("d1", field_type, true)]));

    let keys = PrimitiveArray::<i32>::from([Some(1), None, Some(0)]);
    let values = PrimitiveArray::<i32>::from([None, Some(10)]);
    let array = DictionaryArray::<i32>::from_data(keys, Arc::new(values));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])?;

    let table = write(&[batch]);

    let expected = vec![
        "+----+", "| d1 |", "+----+", "| 10 |", "|    |", "|    |", "+----+",
    ];

    let actual: Vec<&str> = table.lines().collect();

    assert_eq!(expected, actual, "Actual result:\n{}", table);

    Ok(())
}

/// Generate an array with type $ARRAYTYPE with a numeric value of
/// $VALUE, and compare $EXPECTED_RESULT to the output of
/// formatting that array with `write`
macro_rules! check_datetime {
    ($ty:ty, $datatype:expr, $value:expr, $EXPECTED_RESULT:expr) => {
        let array = Arc::new(PrimitiveArray::<$ty>::from(&[Some($value), None]).to($datatype));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let table = write(&[batch]);

        let expected = $EXPECTED_RESULT;
        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n\n{:#?}\n\n", actual);
    };
}

#[test]
fn write_timestamp_second() {
    let expected = vec![
        "+---------------------+",
        "| f                   |",
        "+---------------------+",
        "| 1970-05-09 14:25:11 |",
        "|                     |",
        "+---------------------+",
    ];
    check_datetime!(
        i64,
        DataType::Timestamp(TimeUnit::Second, None),
        11111111,
        expected
    );
}

#[test]
fn write_timestamp_second_with_tz() {
    let expected = vec![
        "+----------------------------+",
        "| f                          |",
        "+----------------------------+",
        "| 1970-05-09 14:25:11 +00:00 |",
        "|                            |",
        "+----------------------------+",
    ];
    check_datetime!(
        i64,
        DataType::Timestamp(TimeUnit::Second, Some("UTC".to_string())),
        11111111,
        expected
    );
}

#[test]
fn write_timestamp_millisecond() {
    let expected = vec![
        "+-------------------------+",
        "| f                       |",
        "+-------------------------+",
        "| 1970-01-01 03:05:11.111 |",
        "|                         |",
        "+-------------------------+",
    ];
    check_datetime!(
        i64,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        11111111,
        expected
    );
}

#[test]
fn write_timestamp_microsecond() {
    let expected = vec![
        "+----------------------------+",
        "| f                          |",
        "+----------------------------+",
        "| 1970-01-01 00:00:11.111111 |",
        "|                            |",
        "+----------------------------+",
    ];
    check_datetime!(
        i64,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        11111111,
        expected
    );
}

#[test]
fn write_timestamp_nanosecond() {
    let expected = vec![
        "+-------------------------------+",
        "| f                             |",
        "+-------------------------------+",
        "| 1970-01-01 00:00:00.011111111 |",
        "|                               |",
        "+-------------------------------+",
    ];
    check_datetime!(
        i64,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        11111111,
        expected
    );
}

#[test]
fn write_date_32() {
    let expected = vec![
        "+------------+",
        "| f          |",
        "+------------+",
        "| 1973-05-19 |",
        "|            |",
        "+------------+",
    ];
    check_datetime!(i32, DataType::Date32, 1234, expected);
}

#[test]
fn write_date_64() {
    let expected = vec![
        "+------------+",
        "| f          |",
        "+------------+",
        "| 2005-03-18 |",
        "|            |",
        "+------------+",
    ];
    check_datetime!(i64, DataType::Date64, 1111111100000, expected);
}

#[test]
fn write_time_32_second() {
    let expected = vec![
        "+----------+",
        "| f        |",
        "+----------+",
        "| 00:18:31 |",
        "|          |",
        "+----------+",
    ];
    check_datetime!(i32, DataType::Time32(TimeUnit::Second), 1111, expected);
}

#[test]
fn write_time_32_millisecond() {
    let expected = vec![
        "+--------------+",
        "| f            |",
        "+--------------+",
        "| 03:05:11.111 |",
        "|              |",
        "+--------------+",
    ];
    check_datetime!(
        i32,
        DataType::Time32(TimeUnit::Millisecond),
        11111111,
        expected
    );
}

#[test]
fn write_time_64_microsecond() {
    let expected = vec![
        "+-----------------+",
        "| f               |",
        "+-----------------+",
        "| 00:00:11.111111 |",
        "|                 |",
        "+-----------------+",
    ];
    check_datetime!(
        i64,
        DataType::Time64(TimeUnit::Microsecond),
        11111111,
        expected
    );
}

#[test]
fn write_time_64_nanosecond() {
    let expected = vec![
        "+--------------------+",
        "| f                  |",
        "+--------------------+",
        "| 00:00:00.011111111 |",
        "|                    |",
        "+--------------------+",
    ];
    check_datetime!(
        i64,
        DataType::Time64(TimeUnit::Nanosecond),
        11111111,
        expected
    );
}

#[test]
fn write_struct() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let values = vec![
        Arc::new(Int32Array::from(&[Some(1), None, Some(2)])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("a"), Some("b"), Some("c")])) as Arc<dyn Array>,
    ];

    let validity = Some(Bitmap::from(&[true, false, true]));

    let array = StructArray::from_data(DataType::Struct(fields), values, validity);

    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), true)]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])?;

    let table = write(&[batch]);

    let expected = vec![
        "+--------------+",
        "| a            |",
        "+--------------+",
        "| {a: 1, b: a} |",
        "|              |",
        "| {a: 2, b: c} |",
        "+--------------+",
    ];

    let actual: Vec<&str> = table.lines().collect();

    assert_eq!(expected, actual, "Actual result:\n{}", table);

    Ok(())
}

#[test]
fn write_union() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, UnionMode::Sparse);
    let types = Buffer::from_slice([0, 0, 1]);
    let fields = vec![
        Arc::new(Int32Array::from(&[Some(1), None, Some(2)])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("a"), Some("b"), Some("c")])) as Arc<dyn Array>,
    ];

    let array = UnionArray::from_data(data_type, types, fields, None);

    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), true)]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])?;

    let table = write(&[batch]);

    let expected = vec![
        "+---+", "| a |", "+---+", "| 1 |", "|   |", "| c |", "+---+",
    ];

    let actual: Vec<&str> = table.lines().collect();

    assert_eq!(expected, actual, "Actual result:\n{}", table);

    Ok(())
}
