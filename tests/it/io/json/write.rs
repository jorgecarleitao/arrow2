use std::sync::Arc;

use arrow2::{
    array::*,
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field, TimeUnit},
    error::Result,
};

use super::*;

macro_rules! test {
    ($array:expr, $expected:expr) => {{
        let buf = write_batch(Box::new($array))?;
        assert_eq!(String::from_utf8(buf).unwrap(), $expected);
        Ok(())
    }};
}

#[test]
fn int32() -> Result<()> {
    let array = Int32Array::from([Some(1), Some(2), Some(3), None, Some(5)]);
    //let b = Utf8Array::<i32>::from(&vec![Some("a"), Some("b"), Some("c"), Some("d"), None]);

    let expected = r#"[1,2,3,null,5]"#;

    test!(array, expected)
}

#[test]
fn f32() -> Result<()> {
    let array = Float32Array::from([Some(1.5), Some(2.5), Some(f32::NAN), None, Some(5.5)]);

    let expected = r#"[1.5,2.5,null,null,5.5]"#;

    test!(array, expected)
}

#[test]
fn f64() -> Result<()> {
    let array = Float64Array::from([Some(1.5), Some(2.5), Some(f64::NAN), None, Some(5.5)]);

    let expected = r#"[1.5,2.5,null,null,5.5]"#;

    test!(array, expected)
}

#[test]
fn utf8() -> Result<()> {
    let array = Utf8Array::<i32>::from(&vec![Some("a"), Some("b"), Some("c"), Some("d"), None]);

    let expected = r#"["a","b","c","d",null]"#;

    test!(array, expected)
}

#[test]
fn struct_() -> Result<()> {
    let c1 = Int32Array::from([Some(1), Some(2), Some(3), None, Some(5)]);
    let c2 = Utf8Array::<i32>::from(&vec![Some("a"), Some("b"), Some("c"), Some("d"), None]);

    let data_type = DataType::Struct(vec![
        Field::new("c1", c1.data_type().clone(), true),
        Field::new("c2", c2.data_type().clone(), true),
    ]);
    let array = StructArray::from_data(data_type, vec![Arc::new(c1) as _, Arc::new(c2)], None);

    let expected = r#"[{"c1":1,"c2":"a"},{"c1":2,"c2":"b"},{"c1":3,"c2":"c"},{"c1":null,"c2":"d"},{"c1":5,"c2":null}]"#;

    test!(array, expected)
}

#[test]
fn nested_struct_with_validity() -> Result<()> {
    let inner = vec![
        Field::new("c121", DataType::Utf8, false),
        Field::new("c122", DataType::Int32, false),
    ];
    let fields = vec![
        Field::new("c11", DataType::Int32, false),
        Field::new("c12", DataType::Struct(inner.clone()), false),
    ];

    let c1 = StructArray::from_data(
        DataType::Struct(fields),
        vec![
            Arc::new(Int32Array::from(&[Some(1), None, Some(5)])),
            Arc::new(StructArray::from_data(
                DataType::Struct(inner),
                vec![
                    Arc::new(Utf8Array::<i32>::from(&vec![None, Some("f"), Some("g")])),
                    Arc::new(Int32Array::from(&[Some(20), None, Some(43)])),
                ],
                Some(Bitmap::from([false, true, true])),
            )),
        ],
        Some(Bitmap::from([true, true, false])),
    );
    let c2 = Utf8Array::<i32>::from(&vec![Some("a"), Some("b"), Some("c")]);

    let data_type = DataType::Struct(vec![
        Field::new("c1", c1.data_type().clone(), true),
        Field::new("c2", c2.data_type().clone(), true),
    ]);
    let array = StructArray::from_data(data_type, vec![Arc::new(c1) as _, Arc::new(c2)], None);

    let expected = r#"[{"c1":{"c11":1,"c12":null},"c2":"a"},{"c1":{"c11":null,"c12":{"c121":"f","c122":null}},"c2":"b"},{"c1":null,"c2":"c"}]"#;

    test!(array, expected)
}

#[test]
fn nested_struct() -> Result<()> {
    let c121 = Field::new("c121", DataType::Utf8, false);
    let fields = vec![
        Field::new("c11", DataType::Int32, false),
        Field::new("c12", DataType::Struct(vec![c121.clone()]), false),
    ];

    let c1 = StructArray::from_data(
        DataType::Struct(fields),
        vec![
            Arc::new(Int32Array::from(&[Some(1), None, Some(5)])),
            Arc::new(StructArray::from_data(
                DataType::Struct(vec![c121]),
                vec![Arc::new(Utf8Array::<i32>::from(&vec![
                    Some("e"),
                    Some("f"),
                    Some("g"),
                ]))],
                None,
            )),
        ],
        None,
    );

    let c2 = Utf8Array::<i32>::from(&vec![Some("a"), Some("b"), Some("c")]);

    let data_type = DataType::Struct(vec![
        Field::new("c1", c1.data_type().clone(), true),
        Field::new("c2", c2.data_type().clone(), true),
    ]);
    let array = StructArray::from_data(data_type, vec![Arc::new(c1) as _, Arc::new(c2)], None);

    let expected = r#"[{"c1":{"c11":1,"c12":{"c121":"e"}},"c2":"a"},{"c1":{"c11":null,"c12":{"c121":"f"}},"c2":"b"},{"c1":{"c11":5,"c12":{"c121":"g"}},"c2":"c"}]"#;

    test!(array, expected)
}

#[test]
fn struct_with_list_field() -> Result<()> {
    let iter = vec![vec!["a", "a1"], vec!["b"], vec!["c"], vec!["d"], vec!["e"]];

    let iter = iter
        .into_iter()
        .map(|x| x.into_iter().map(Some).collect::<Vec<_>>())
        .map(Some);
    let mut a = MutableListArray::<i32, MutableUtf8Array<i32>>::new_with_field(
        MutableUtf8Array::<i32>::new(),
        "c_list",
        false,
    );
    a.try_extend(iter).unwrap();
    let c1: ListArray<i32> = a.into();

    let c2 = PrimitiveArray::from_slice([1, 2, 3, 4, 5]);

    let data_type = DataType::Struct(vec![
        Field::new("c1", c1.data_type().clone(), true),
        Field::new("c2", c2.data_type().clone(), true),
    ]);
    let array = StructArray::from_data(data_type, vec![Arc::new(c1) as _, Arc::new(c2)], None);

    let expected = r#"[{"c1":["a","a1"],"c2":1},{"c1":["b"],"c2":2},{"c1":["c"],"c2":3},{"c1":["d"],"c2":4},{"c1":["e"],"c2":5}]"#;

    test!(array, expected)
}

#[test]
fn nested_list() -> Result<()> {
    let iter = vec![
        vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3)])],
        vec![],
        vec![Some(vec![Some(4), Some(5), Some(6)])],
    ];

    let iter = iter.into_iter().map(Some);

    let inner = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new_with_field(
        MutablePrimitiveArray::<i32>::new(),
        "b",
        false,
    );
    let mut c1 =
        MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i32>>>::new_with_field(
            inner, "a", false,
        );
    c1.try_extend(iter).unwrap();
    let c1: ListArray<i32> = c1.into();

    let c2 = Utf8Array::<i32>::from(&vec![Some("foo"), Some("bar"), None]);

    let data_type = DataType::Struct(vec![
        Field::new("c1", c1.data_type().clone(), true),
        Field::new("c2", c2.data_type().clone(), true),
    ]);
    let array = StructArray::from_data(data_type, vec![Arc::new(c1) as _, Arc::new(c2)], None);

    let expected =
        r#"[{"c1":[[1,2],[3]],"c2":"foo"},{"c1":[],"c2":"bar"},{"c1":[[4,5,6]],"c2":null}]"#;

    test!(array, expected)
}

#[test]
fn list_of_struct() -> Result<()> {
    let inner = vec![Field::new("c121", DataType::Utf8, false)];
    let fields = vec![
        Field::new("c11", DataType::Int32, false),
        Field::new("c12", DataType::Struct(inner.clone()), false),
    ];
    let c1_datatype = DataType::List(Box::new(Field::new(
        "s",
        DataType::Struct(fields.clone()),
        false,
    )));

    let s = StructArray::from_data(
        DataType::Struct(fields),
        vec![
            Arc::new(Int32Array::from(&[Some(1), None, Some(5)])),
            Arc::new(StructArray::from_data(
                DataType::Struct(inner),
                vec![Arc::new(Utf8Array::<i32>::from(&vec![
                    Some("e"),
                    Some("f"),
                    Some("g"),
                ]))],
                Some(Bitmap::from([false, true, true])),
            )),
        ],
        Some(Bitmap::from([true, true, false])),
    );

    // list column rows (c1):
    // [{"c11": 1, "c12": {"c121": "e"}}, {"c12": {"c121": "f"}}],
    // null,
    // [{"c11": 5, "c12": {"c121": "g"}}]
    let c1 = ListArray::<i32>::from_data(
        c1_datatype,
        Buffer::from_slice([0, 2, 2, 3]),
        Arc::new(s),
        Some(Bitmap::from_u8_slice([0b00000101], 3)),
    );

    let c2 = Int32Array::from_slice(&[1, 2, 3]);

    let data_type = DataType::Struct(vec![
        Field::new("c1", c1.data_type().clone(), true),
        Field::new("c2", c2.data_type().clone(), true),
    ]);
    let array = StructArray::from_data(data_type, vec![Arc::new(c1) as _, Arc::new(c2)], None);

    let expected = r#"[{"c1":[{"c11":1,"c12":null},{"c11":null,"c12":{"c121":"f"}}],"c2":1},{"c1":null,"c2":2},{"c1":[null],"c2":3}]"#;

    test!(array, expected)
}

#[test]
fn escaped_end_of_line_in_utf8() -> Result<()> {
    let array = Utf8Array::<i32>::from(&vec![Some("a\na"), None]);

    let expected = r#"["a\na",null]"#;

    test!(array, expected)
}

#[test]
fn escaped_quotation_marks_in_utf8() -> Result<()> {
    let array = Utf8Array::<i32>::from(&vec![Some("a\"a"), None]);

    let expected = r#"["a\"a",null]"#;

    test!(array, expected)
}

#[test]
fn write_date32() -> Result<()> {
    let array =
        PrimitiveArray::from_data(DataType::Date32, vec![1000i32, 8000, 10000].into(), None);

    let expected = r#"["1972-09-27","1991-11-27","1997-05-19"]"#;

    test!(array, expected)
}

#[test]
fn write_timestamp() -> Result<()> {
    let array = PrimitiveArray::from_data(
        DataType::Timestamp(TimeUnit::Second, None),
        vec![10i64, 1 << 32, 1 << 33].into(),
        None,
    );

    let expected = r#"["1970-01-01 00:00:10","2106-02-07 06:28:16","2242-03-16 12:56:32"]"#;

    test!(array, expected)
}
