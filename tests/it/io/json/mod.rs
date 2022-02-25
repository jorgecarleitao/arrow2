mod read;
mod write;

use std::io::Cursor;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::bitmap::Bitmap;
use arrow2::buffer::Buffer;
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::json::read as json_read;
use arrow2::io::json::write as json_write;

fn read_batch(data: String, fields: &[Field]) -> Result<Chunk<Arc<dyn Array>>> {
    let mut reader = Cursor::new(data);

    let mut rows = vec![String::default(); 1024];
    let read = json_read::read_rows(&mut reader, &mut rows)?;
    let rows = &rows[..read];
    json_read::deserialize(rows, fields)
}

fn write_batch<A: AsRef<dyn Array>>(
    batch: Chunk<A>,
    names: Vec<String>,
    format: json_write::Format,
) -> Result<Vec<u8>> {
    let batches = vec![Ok(batch)].into_iter();

    let blocks = json_write::Serializer::new(batches, names, vec![], format);

    let mut buf = Vec::new();
    json_write::write(&mut buf, format, blocks)?;
    Ok(buf)
}

fn round_trip(data: String) -> Result<()> {
    let mut reader = Cursor::new(data);
    let fields = json_read::infer(&mut reader, None)?;
    let data = reader.into_inner();

    let columns = read_batch(data, &fields)?;

    let buf = write_batch(
        columns.clone(),
        fields.iter().map(|x| x.name.clone()).collect(),
        json_write::Format::NewlineDelimitedJson,
    )?;

    let new_chunk = read_batch(String::from_utf8(buf).unwrap(), &fields)?;

    assert_eq!(columns, new_chunk);
    Ok(())
}

#[test]
fn round_trip_basics() -> Result<()> {
    let (data, _, _) = case_basics();
    round_trip(data)
}

#[test]
fn round_trip_list() -> Result<()> {
    let (data, _, _) = case_list();
    round_trip(data)
}

fn case_list() -> (String, Vec<Field>, Vec<Box<dyn Array>>) {
    let data = r#"{"a":1, "b":[2.0, 1.3, -6.1], "c":[false, true], "d":"4"}
            {"a":-10, "b":null, "c":[true, true]}
            {"a":null, "b":[2.1, null, -6.2], "c":[false, null], "d":"text"}
            "#
    .to_string();

    let fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new(
            "b",
            DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            true,
        ),
        Field::new(
            "c",
            DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
            true,
        ),
        Field::new("d", DataType::Utf8, true),
    ];

    let a = Int64Array::from(&[Some(1), Some(-10), None]);
    let mut b = MutableListArray::<i32, MutablePrimitiveArray<f64>>::new();
    b.try_extend(vec![
        Some(vec![Some(2.0), Some(1.3), Some(-6.1)]),
        None,
        Some(vec![Some(2.1), None, Some(-6.2)]),
    ])
    .unwrap();
    let b: ListArray<i32> = b.into();

    let mut c = MutableListArray::<i32, MutableBooleanArray>::new();
    c.try_extend(vec![
        Some(vec![Some(false), Some(true)]),
        Some(vec![Some(true), Some(true)]),
        Some(vec![Some(false), None]),
    ])
    .unwrap();
    let c: ListArray<i32> = c.into();

    let d = Utf8Array::<i32>::from(&[Some("4"), None, Some("text")]);

    let columns = vec![
        Box::new(a) as Box<dyn Array>,
        Box::new(b),
        Box::new(c),
        Box::new(d),
    ];

    (data, fields, columns)
}

fn case_dict() -> (String, Vec<Field>, Vec<Box<dyn Array>>) {
    let data = r#"{"machine": "a", "events": [null, "Elect Leader", "Do Ballot"]}
    {"machine": "b", "events": ["Do Ballot", null, "Send Data", "Elect Leader"]}
    {"machine": "c", "events": ["Send Data"]}
    {"machine": "c"}
    {"machine": "c", "events": null}
    "#
    .to_string();

    let data_type = DataType::List(Box::new(Field::new(
        "item",
        DataType::Dictionary(u64::KEY_TYPE, Box::new(DataType::Utf8), false),
        true,
    )));

    let fields = vec![Field::new("events", data_type, true)];

    type A = MutableDictionaryArray<u64, MutableUtf8Array<i32>>;

    let mut array = MutableListArray::<i32, A>::new();
    array
        .try_extend(vec![
            Some(vec![None, Some("Elect Leader"), Some("Do Ballot")]),
            Some(vec![
                Some("Do Ballot"),
                None,
                Some("Send Data"),
                Some("Elect Leader"),
            ]),
            Some(vec![Some("Send Data")]),
            None,
            None,
        ])
        .unwrap();

    let array: ListArray<i32> = array.into();

    (data, fields, vec![Box::new(array) as Box<dyn Array>])
}

fn case_basics() -> (String, Vec<Field>, Vec<Box<dyn Array>>) {
    let data = r#"{"a":1, "b":2.0, "c":false, "d":"4"}
    {"a":-10, "b":-3.5, "c":true, "d":null}
    {"a":100000000, "b":0.6, "d":"text"}"#
        .to_string();
    let fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Float64, true),
        Field::new("c", DataType::Boolean, true),
        Field::new("d", DataType::Utf8, true),
    ];
    let columns = vec![
        Box::new(Int64Array::from_slice(&[1, -10, 100000000])) as Box<dyn Array>,
        Box::new(Float64Array::from_slice(&[2.0, -3.5, 0.6])),
        Box::new(BooleanArray::from(&[Some(false), Some(true), None])),
        Box::new(Utf8Array::<i32>::from(&[Some("4"), None, Some("text")])),
    ];
    (data, fields, columns)
}

fn case_projection() -> (String, Vec<Field>, Vec<Box<dyn Array>>) {
    let data = r#"{"a":1, "b":2.0, "c":false, "d":"4", "e":"4"}
    {"a":10, "b":-3.5, "c":true, "d":null, "e":"text"}
    {"a":100000000, "b":0.6, "d":"text"}"#
        .to_string();
    let fields = vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::Float32, true),
        Field::new("c", DataType::Boolean, true),
        // note how "d" is not here
        Field::new("e", DataType::Binary, true),
    ];
    let columns = vec![
        Box::new(UInt32Array::from_slice(&[1, 10, 100000000])) as Box<dyn Array>,
        Box::new(Float32Array::from_slice(&[2.0, -3.5, 0.6])),
        Box::new(BooleanArray::from(&[Some(false), Some(true), None])),
        Box::new(BinaryArray::<i32>::from(&[
            Some(b"4".as_ref()),
            Some(b"text".as_ref()),
            None,
        ])),
    ];
    (data, fields, columns)
}

fn case_struct() -> (String, Vec<Field>, Vec<Box<dyn Array>>) {
    let data = r#"{"a": {"b": true, "c": {"d": "text"}}}
    {"a": {"b": false, "c": null}}
    {"a": {"b": true, "c": {"d": "text"}}}
    {"a": 1}"#
        .to_string();

    let d_field = Field::new("d", DataType::Utf8, true);
    let c_field = Field::new("c", DataType::Struct(vec![d_field.clone()]), true);
    let a_field = Field::new(
        "a",
        DataType::Struct(vec![
            Field::new("b", DataType::Boolean, true),
            c_field.clone(),
        ]),
        true,
    );
    let fields = vec![a_field];

    // build expected output
    let d = Utf8Array::<i32>::from(&vec![Some("text"), None, Some("text"), None]);
    let c = StructArray::from_data(DataType::Struct(vec![d_field]), vec![Arc::new(d)], None);

    let b = BooleanArray::from(vec![Some(true), Some(false), Some(true), None]);
    let expected = StructArray::from_data(
        DataType::Struct(vec![Field::new("b", DataType::Boolean, true), c_field]),
        vec![Arc::new(b), Arc::new(c)],
        None,
    );

    (data, fields, vec![Box::new(expected) as Box<dyn Array>])
}

fn case_nested_list() -> (String, Vec<Field>, Vec<Box<dyn Array>>) {
    let d_field = Field::new("d", DataType::Utf8, true);
    let c_field = Field::new("c", DataType::Struct(vec![d_field.clone()]), true);
    let b_field = Field::new("b", DataType::Boolean, true);
    let a_struct_field = Field::new(
        "a",
        DataType::Struct(vec![b_field.clone(), c_field.clone()]),
        true,
    );
    let a_list_data_type = DataType::List(Box::new(a_struct_field));
    let a_field = Field::new("a", a_list_data_type.clone(), true);

    let data = r#"
    {"a": [{"b": true, "c": {"d": "a_text"}}, {"b": false, "c": {"d": "b_text"}}]}
    {"a": [{"b": false, "c": null}]}
    {"a": [{"b": true, "c": {"d": "c_text"}}, {"b": null, "c": {"d": "d_text"}}, {"b": true, "c": {"d": null}}]}
    {"a": null}
    {"a": []}
    "#.to_string();

    // build expected output
    let d = Utf8Array::<i32>::from(&vec![
        Some("a_text"),
        Some("b_text"),
        None,
        Some("c_text"),
        Some("d_text"),
        None,
    ]);

    let c = StructArray::from_data(DataType::Struct(vec![d_field]), vec![Arc::new(d)], None);

    let b = BooleanArray::from(vec![
        Some(true),
        Some(false),
        Some(false),
        Some(true),
        None,
        Some(true),
    ]);
    let a_struct = StructArray::from_data(
        DataType::Struct(vec![b_field, c_field]),
        vec![Arc::new(b) as Arc<dyn Array>, Arc::new(c) as Arc<dyn Array>],
        None,
    );
    let expected = ListArray::from_data(
        a_list_data_type,
        Buffer::from_slice([0i32, 2, 3, 6, 6, 6]),
        Arc::new(a_struct) as Arc<dyn Array>,
        Some(Bitmap::from_u8_slice([0b00010111], 5)),
    );

    (
        data,
        vec![a_field],
        vec![Box::new(expected) as Box<dyn Array>],
    )
}

fn case(case: &str) -> (String, Vec<Field>, Vec<Box<dyn Array>>) {
    match case {
        "basics" => case_basics(),
        "projection" => case_projection(),
        "list" => case_list(),
        "dict" => case_dict(),
        "struct" => case_struct(),
        "nested_list" => case_nested_list(),
        _ => todo!(),
    }
}
