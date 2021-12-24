use std::sync::Arc;

use arrow2::{
    array::*,
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field, Schema},
    error::Result,
    record_batch::RecordBatch,
};

use super::*;

#[test]
fn write_simple_rows() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Int32, false),
        Field::new("c2", DataType::Utf8, false),
    ]);

    let a = Int32Array::from([Some(1), Some(2), Some(3), None, Some(5)]);
    let b = Utf8Array::<i32>::from(&vec![Some("a"), Some("b"), Some("c"), Some("d"), None]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

    let buf = write_batch(batch)?;

    assert_eq!(
        String::from_utf8(buf).unwrap(),
        r#"{"c1":1,"c2":"a"}
{"c1":2,"c2":"b"}
{"c1":3,"c2":"c"}
{"c1":null,"c2":"d"}
{"c1":5,"c2":null}
"#
    );
    Ok(())
}

#[test]
fn write_nested_struct_with_validity() -> Result<()> {
    let inner = vec![
        Field::new("c121", DataType::Utf8, false),
        Field::new("c122", DataType::Int32, false),
    ];
    let fields = vec![
        Field::new("c11", DataType::Int32, false),
        Field::new("c12", DataType::Struct(inner.clone()), false),
    ];
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Struct(fields.clone()), false),
        Field::new("c2", DataType::Utf8, false),
    ]);

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

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

    let buf = write_batch(batch)?;

    assert_eq!(
        String::from_utf8(buf).unwrap(),
        r#"{"c1":{"c11":1,"c12":null},"c2":"a"}
{"c1":{"c11":null,"c12":{"c121":"f","c122":null}},"c2":"b"}
{"c1":null,"c2":"c"}
"#
    );
    Ok(())
}

#[test]
fn write_nested_structs() -> Result<()> {
    let c121 = Field::new("c121", DataType::Utf8, false);
    let fields = vec![
        Field::new("c11", DataType::Int32, false),
        Field::new("c12", DataType::Struct(vec![c121.clone()]), false),
    ];
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Struct(fields.clone()), false),
        Field::new("c2", DataType::Utf8, false),
    ]);

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

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

    let buf = write_batch(batch)?;

    assert_eq!(
        String::from_utf8(buf).unwrap(),
        r#"{"c1":{"c11":1,"c12":{"c121":"e"}},"c2":"a"}
{"c1":{"c11":null,"c12":{"c121":"f"}},"c2":"b"}
{"c1":{"c11":5,"c12":{"c121":"g"}},"c2":"c"}
"#
    );
    Ok(())
}

#[test]
fn write_struct_with_list_field() -> Result<()> {
    let list_datatype = DataType::List(Box::new(Field::new("c_list", DataType::Utf8, false)));
    let field_c1 = Field::new("c1", list_datatype, false);
    let field_c2 = Field::new("c2", DataType::Int32, false);
    let schema = Schema::new(vec![field_c1, field_c2]);

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
    let a: ListArray<i32> = a.into();

    let b = PrimitiveArray::from_slice([1, 2, 3, 4, 5]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

    let buf = write_batch(batch)?;

    assert_eq!(
        String::from_utf8(buf).unwrap(),
        r#"{"c1":["a","a1"],"c2":1}
{"c1":["b"],"c2":2}
{"c1":["c"],"c2":3}
{"c1":["d"],"c2":4}
{"c1":["e"],"c2":5}
"#
    );
    Ok(())
}

#[test]
fn write_nested_list() -> Result<()> {
    let list_inner = DataType::List(Box::new(Field::new("b", DataType::Int32, false)));
    let list_datatype = DataType::List(Box::new(Field::new("a", list_inner, false)));
    let field_c1 = Field::new("c1", list_datatype, true);
    let field_c2 = Field::new("c2", DataType::Utf8, true);
    let schema = Schema::new(vec![field_c1, field_c2]);

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

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

    let buf = write_batch(batch)?;

    assert_eq!(
        String::from_utf8(buf).unwrap(),
        r#"{"c1":[[1,2],[3]],"c2":"foo"}
{"c1":[],"c2":"bar"}
{"c1":[[4,5,6]],"c2":null}
"#
    );
    Ok(())
}

#[test]
fn write_list_of_struct() -> Result<()> {
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
    let field_c1 = Field::new("c1", c1_datatype.clone(), true);
    let field_c2 = Field::new("c2", DataType::Int32, false);
    let schema = Schema::new(vec![field_c1, field_c2]);

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

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

    let buf = write_batch(batch)?;

    assert_eq!(
        String::from_utf8(buf).unwrap(),
        r#"{"c1":[{"c11":1,"c12":null},{"c11":null,"c12":{"c121":"f"}}],"c2":1}
{"c1":null,"c2":2}
{"c1":[null],"c2":3}
"#
    );
    Ok(())
}
