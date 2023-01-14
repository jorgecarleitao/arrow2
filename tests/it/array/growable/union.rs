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
        Int32Array::from(&[Some(1), None, Some(2)]).boxed(),
        Utf8Array::<i32>::from([Some("a"), Some("b"), Some("c")]).boxed(),
    ];
    let array = UnionArray::new(data_type, types, fields, None);

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
        Int32Array::from(&[Some(1), None, Some(2)]).boxed(),
        Utf8Array::<i32>::from([Some("c")]).boxed(),
    ];
    let offsets = Some(vec![0, 1, 0].into());

    let array = UnionArray::new(data_type, types, fields, offsets);

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
fn complex_dense() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];

    let data_type = DataType::Union(fields, None, UnionMode::Dense);

    let types = vec![0, 0, 1].into();
    let fields = vec![
        Int32Array::from(&[Some(1), Some(2)]).boxed(),
        Utf8Array::<i32>::from([Some("c")]).boxed(),
    ];
    let offsets = Some(vec![0, 1, 0].into());

    let array1 = UnionArray::new(data_type.clone(), types, fields, offsets);

    let types = vec![1, 1, 0].into();
    let fields = vec![
        Int32Array::from(&[Some(6)]).boxed(),
        Utf8Array::<i32>::from([Some("d"), Some("e")]).boxed(),
    ];
    let offsets = Some(vec![0, 1, 0].into());

    let array2 = UnionArray::new(data_type.clone(), types, fields, offsets);

    let mut a = GrowableUnion::new(vec![&array1, &array2], 10);

    // Effective concat
    a.extend(0, 0, 3);
    a.extend(1, 0, 3);

    let result: UnionArray = a.into();

    let types = vec![0, 0, 1, 1, 1, 0].into();
    let fields = vec![
        Int32Array::from(&[Some(1), Some(2), Some(6)]).boxed(),
        Utf8Array::<i32>::from([Some("c"), Some("d"), Some("e")]).boxed(),
    ];
    let offsets = Some(vec![0, 1, 0, 1, 2, 2].into());

    let expected = UnionArray::new(data_type.clone(), types, fields, offsets);

    assert_eq!(expected, result);

    Ok(())
}
