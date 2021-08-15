use arrow2::array::growable::{Growable, GrowableNull};
use arrow2::array::*;

#[test]
fn null() {
    let mut mutable = GrowableNull::new();

    mutable.extend(0, 1, 2);
    mutable.extend(1, 0, 1);

    let result: NullArray = mutable.into();

    let expected = NullArray::from_data(3);
    assert_eq!(result, expected);
}
