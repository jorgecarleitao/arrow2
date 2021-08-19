use std::sync::Arc;

use arrow2::{array::*, buffer::Buffer, datatypes::*, error::Result};

#[test]
fn display() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, true);
    let types = Buffer::from(&[0, 0, 1]);
    let fields = vec![
        Arc::new(Int32Array::from(&[Some(1), None, Some(2)])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("a"), Some("b"), Some("c")])) as Arc<dyn Array>,
    ];

    let array = UnionArray::from_data(data_type, types, fields, None);

    assert_eq!(format!("{}", array), "UnionArray[1, , c]");

    Ok(())
}

#[test]
fn slice() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, true);
    let types = Buffer::from(&[0, 0, 1]);
    let fields = vec![
        Arc::new(Int32Array::from(&[Some(1), None, Some(2)])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("a"), Some("b"), Some("c")])) as Arc<dyn Array>,
    ];

    let array = UnionArray::from_data(data_type.clone(), types, fields.clone(), None);

    let result = array.slice(1, 2);

    let types = Buffer::from(&[0, 1]);
    let expected = UnionArray::from_data(data_type, types, fields, None);

    assert_eq!(expected, result);
    Ok(())
}
