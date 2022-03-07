use std::sync::Arc;

use arrow2::{
    array::*,
    buffer::Buffer,
    datatypes::*,
    error::Result,
    scalar::{PrimitiveScalar, Utf8Scalar},
};

#[test]
fn debug() -> Result<()> {
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

    assert_eq!(format!("{:?}", array), "UnionArray[1, None, c]");

    Ok(())
}

#[test]
fn slice() -> Result<()> {
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

    let array = UnionArray::from_data(data_type.clone(), types, fields.clone(), None);

    let result = array.slice(1, 2);

    let sliced_types = Buffer::from_slice([0, 1]);
    let sliced_fields = vec![
        Arc::new(Int32Array::from(&[None, Some(2)])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("b"), Some("c")])) as Arc<dyn Array>,
    ];
    let expected = UnionArray::from_data(data_type, sliced_types, sliced_fields, None);

    assert_eq!(expected, result);
    Ok(())
}

#[test]
fn iter_sparse() -> Result<()> {
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

    let array = UnionArray::from_data(data_type, types, fields.clone(), None);
    let mut iter = array.iter();

    assert_eq!(
        iter.next()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveScalar<i32>>()
            .unwrap()
            .value(),
        Some(1)
    );
    assert_eq!(
        iter.next()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveScalar<i32>>()
            .unwrap()
            .value(),
        None
    );
    assert_eq!(
        iter.next()
            .unwrap()
            .as_any()
            .downcast_ref::<Utf8Scalar<i32>>()
            .unwrap()
            .value(),
        Some("c")
    );
    assert_eq!(iter.next(), None);

    Ok(())
}

#[test]
fn iter_dense() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, UnionMode::Dense);
    let types = Buffer::from_slice([0, 0, 1]);
    let offsets = Buffer::<i32>::from_slice([0, 1, 0]);
    let fields = vec![
        Arc::new(Int32Array::from(&[Some(1), None])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("c")])) as Arc<dyn Array>,
    ];

    let array = UnionArray::from_data(data_type, types, fields.clone(), Some(offsets));
    let mut iter = array.iter();

    assert_eq!(
        iter.next()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveScalar<i32>>()
            .unwrap()
            .value(),
        Some(1)
    );
    assert_eq!(
        iter.next()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveScalar<i32>>()
            .unwrap()
            .value(),
        None
    );
    assert_eq!(
        iter.next()
            .unwrap()
            .as_any()
            .downcast_ref::<Utf8Scalar<i32>>()
            .unwrap()
            .value(),
        Some("c")
    );
    assert_eq!(iter.next(), None);

    Ok(())
}

#[test]
fn iter_sparse_slice() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, UnionMode::Sparse);
    let types = Buffer::from_slice([0, 0, 1]);
    let fields = vec![
        Arc::new(Int32Array::from(&[Some(1), Some(3), Some(2)])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("a"), Some("b"), Some("c")])) as Arc<dyn Array>,
    ];

    let array = UnionArray::from_data(data_type, types, fields.clone(), None);
    let array_slice = array.slice(1, 1);
    let mut iter = array_slice.iter();

    assert_eq!(
        iter.next()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveScalar<i32>>()
            .unwrap()
            .value(),
        Some(3)
    );
    assert_eq!(iter.next(), None);

    Ok(())
}

#[test]
fn iter_dense_slice() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, UnionMode::Dense);
    let types = Buffer::from_slice([0, 0, 1]);
    let offsets = Buffer::<i32>::from_slice([0, 1, 0]);
    let fields = vec![
        Arc::new(Int32Array::from(&[Some(1), Some(3)])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("c")])) as Arc<dyn Array>,
    ];

    let array = UnionArray::from_data(data_type, types, fields.clone(), Some(offsets));
    let array_slice = array.slice(1, 1);
    let mut iter = array_slice.iter();

    assert_eq!(
        iter.next()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveScalar<i32>>()
            .unwrap()
            .value(),
        Some(3)
    );
    assert_eq!(iter.next(), None);

    Ok(())
}
