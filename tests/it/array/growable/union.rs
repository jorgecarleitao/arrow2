use std::sync::Arc;

use arrow2::{
    array::{
        growable::{Growable, GrowableUnion},
        *,
    },
    datatypes::*,
    error::Result,
};

#[test]
fn sparse() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, UnionMode::Sparse);
    let types = vec![0, 0, 1].into();
    let fields = vec![
        Arc::new(Int32Array::from(&[Some(1), None, Some(2)])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("a"), Some("b"), Some("c")])) as Arc<dyn Array>,
    ];
    let array = UnionArray::from_data(data_type, types, fields, None);

    for length in 1..2 {
        for index in 0..(array.len() - length + 1) {
            let mut a = GrowableUnion::new(vec![&array], 10);

            a.extend(0, index, length);
            let expected = array.slice(index, length);

            let result: UnionArray = a.into();

            assert_eq!(expected, result);
        }
    }

    Ok(())
}

#[test]
fn dense() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, UnionMode::Dense);
    let types = vec![0, 0, 1].into();
    let fields = vec![
        Arc::new(Int32Array::from(&[Some(1), None, Some(2)])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from(&[Some("c")])) as Arc<dyn Array>,
    ];
    let offsets = Some(vec![0, 1, 0].into());

    let array = UnionArray::from_data(data_type, types, fields, offsets);

    for length in 1..2 {
        for index in 0..(array.len() - length + 1) {
            let mut a = GrowableUnion::new(vec![&array], 10);

            a.extend(0, index, length);
            let expected = array.slice(index, length);

            let result: UnionArray = a.into();

            assert_eq!(expected, result);
        }
    }

    Ok(())
}
