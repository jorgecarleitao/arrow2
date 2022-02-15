mod iterator;

use arrow2::array::*;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::*;

#[test]
fn debug() {
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
        Some(Bitmap::from([true, true, false, true])),
    );
    assert_eq!(
        format!("{:?}", array),
        "StructArray[{b: false, c: 42}, {b: false, c: 28}, None, {b: true, c: 31}]"
    );
}
