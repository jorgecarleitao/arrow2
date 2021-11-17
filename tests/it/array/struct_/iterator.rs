use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::scalar::new_scalar;

#[test]
fn test_simple_iter() {
    use std::sync::Arc;
    let boolean = Arc::new(BooleanArray::from_slice(&[false, false, true, true])) as Arc<dyn Array>;
    let int = Arc::new(Int32Array::from_slice(&[42, 28, 19, 31])) as Arc<dyn Array>;

    let fields = vec![
        Field::new("b", DataType::Boolean, false),
        Field::new("c", DataType::Int32, false),
    ];

    let array = StructArray::from_data(
        DataType::Struct(fields),
        vec![boolean.clone(), int.clone()],
        None,
    );

    for (i, item) in array.iter().enumerate() {
        let expected = Some(vec![
            new_scalar(boolean.as_ref(), i),
            new_scalar(int.as_ref(), i),
        ]);
        assert_eq!(expected, item);
    }
}
