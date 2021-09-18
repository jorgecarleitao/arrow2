use arrow2::array::new_null_array;
use arrow2::compute::comparison::{can_compare, compare, compare_scalar, Operator};
use arrow2::datatypes::DataType::*;
use arrow2::datatypes::TimeUnit;
use arrow2::scalar::new_scalar;

#[test]
fn consistency() {
    let datatypes = vec![
        Null,
        Boolean,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Int8,
        Int16,
        Int32,
        Int64,
        Float32,
        Float64,
        Timestamp(TimeUnit::Second, None),
        Timestamp(TimeUnit::Millisecond, None),
        Timestamp(TimeUnit::Microsecond, None),
        Timestamp(TimeUnit::Nanosecond, None),
        Time64(TimeUnit::Microsecond),
        Time64(TimeUnit::Nanosecond),
        Date32,
        Time32(TimeUnit::Second),
        Time32(TimeUnit::Millisecond),
        Date64,
        Utf8,
        LargeUtf8,
        Binary,
        LargeBinary,
        Duration(TimeUnit::Second),
        Duration(TimeUnit::Millisecond),
        Duration(TimeUnit::Microsecond),
        Duration(TimeUnit::Nanosecond),
    ];

    // array <> array
    datatypes.clone().into_iter().for_each(|d1| {
        let array = new_null_array(d1.clone(), 10);
        let op = Operator::Eq;
        if can_compare(&d1) {
            assert!(compare(array.as_ref(), array.as_ref(), op).is_ok());
        } else {
            assert!(compare(array.as_ref(), array.as_ref(), op).is_err());
        }
    });

    // array <> scalar
    datatypes.into_iter().for_each(|d1| {
        let array = new_null_array(d1.clone(), 10);
        let scalar = new_scalar(array.as_ref(), 0);
        let op = Operator::Eq;
        if can_compare(&d1) {
            assert!(compare_scalar(array.as_ref(), scalar.as_ref(), op).is_ok());
        } else {
            assert!(compare_scalar(array.as_ref(), scalar.as_ref(), op).is_err());
        }
    });
}
