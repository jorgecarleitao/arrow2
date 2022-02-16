use arrow2::array::*;
use arrow2::compute::comparison::{self, boolean::*};
use arrow2::datatypes::{DataType::*, IntervalUnit};
use arrow2::datatypes::{IntegerType, TimeUnit};
use arrow2::scalar::new_scalar;

#[test]
fn consistency() {
    use arrow2::compute::comparison::*;
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
        Interval(IntervalUnit::YearMonth),
        Interval(IntervalUnit::MonthDayNano),
        Interval(IntervalUnit::DayTime),
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
        Dictionary(IntegerType::Int32, Box::new(LargeBinary), false),
    ];

    // array <> array
    datatypes.clone().into_iter().for_each(|d1| {
        let array = new_null_array(d1.clone(), 10);
        if can_eq(&d1) {
            eq(array.as_ref(), array.as_ref());
        }
        if can_lt_eq(&d1) {
            lt_eq(array.as_ref(), array.as_ref());
        }
    });

    // array <> scalar
    datatypes.into_iter().for_each(|d1| {
        let array = new_null_array(d1.clone(), 10);
        let scalar = new_scalar(array.as_ref(), 0);
        if can_eq_scalar(&d1) {
            eq_scalar(array.as_ref(), scalar.as_ref());
        }
        if can_lt_eq_scalar(&d1) {
            lt_eq_scalar(array.as_ref(), scalar.as_ref());
        }
    });
}

// disable wrapping inside literal vectors used for test data and assertions
#[rustfmt::skip::macros(vec)]
#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::bitmap::Bitmap;

    macro_rules! cmp_bool {
        ($KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = BooleanArray::from_slice($A_VEC);
            let b = BooleanArray::from_slice($B_VEC);
            let c = $KERNEL(&a, &b);
            assert_eq!(BooleanArray::from_slice($EXPECTED), c);
        };
    }

    macro_rules! cmp_bool_options {
        ($KERNEL:ident, $A_VEC:expr, $B_VEC:expr, $EXPECTED:expr) => {
            let a = BooleanArray::from($A_VEC);
            let b = BooleanArray::from($B_VEC);
            let c = $KERNEL(&a, &b);
            assert_eq!(BooleanArray::from($EXPECTED), c);
        };
    }

    macro_rules! cmp_bool_scalar {
        ($KERNEL:ident, $A_VEC:expr, $B:literal, $EXPECTED:expr) => {
            let a = BooleanArray::from_slice($A_VEC);
            let c = $KERNEL(&a, $B);
            assert_eq!(BooleanArray::from_slice($EXPECTED), c);
        };
    }

    #[test]
    fn test_eq() {
        cmp_bool!(
            eq,
            &[true, false, true, false],
            &[true, true, false, false],
            &[true, false, false, true]
        );
    }

    #[test]
    fn test_eq_scalar() {
        cmp_bool_scalar!(eq_scalar, &[false, true], true, &[false, true]);
    }

    #[test]
    fn test_eq_with_slice() {
        let a = BooleanArray::from_slice(&[true, true, false]);
        let b = BooleanArray::from_slice(&[true, true, true, true, false]);
        let c = b.slice(2, 3);
        let d = eq(&c, &a);
        assert_eq!(d, BooleanArray::from_slice(&[true, true, true]));
    }

    #[test]
    fn test_neq() {
        cmp_bool!(
            neq,
            &[true, false, true, false],
            &[true, true, false, false],
            &[false, true, true, false]
        );
    }

    #[test]
    fn test_neq_scalar() {
        cmp_bool_scalar!(neq_scalar, &[false, true], true, &[true, false]);
    }

    #[test]
    fn test_lt() {
        cmp_bool!(
            lt,
            &[true, false, true, false],
            &[true, true, false, false],
            &[false, true, false, false]
        );
    }

    #[test]
    fn test_lt_scalar_true() {
        cmp_bool_scalar!(lt_scalar, &[false, true], true, &[true, false]);
    }

    #[test]
    fn test_lt_scalar_false() {
        cmp_bool_scalar!(lt_scalar, &[false, true], false, &[false, false]);
    }

    #[test]
    fn test_lt_eq_scalar_true() {
        cmp_bool_scalar!(lt_eq_scalar, &[false, true], true, &[true, true]);
    }

    #[test]
    fn test_lt_eq_scalar_false() {
        cmp_bool_scalar!(lt_eq_scalar, &[false, true], false, &[true, false]);
    }

    #[test]
    fn test_gt_scalar_true() {
        cmp_bool_scalar!(gt_scalar, &[false, true], true, &[false, false]);
    }

    #[test]
    fn test_gt_scalar_false() {
        cmp_bool_scalar!(gt_scalar, &[false, true], false, &[false, true]);
    }

    #[test]
    fn test_gt_eq_scalar_true() {
        cmp_bool_scalar!(gt_eq_scalar, &[false, true], true, &[false, true]);
    }

    #[test]
    fn test_gt_eq_scalar_false() {
        cmp_bool_scalar!(gt_eq_scalar, &[false, true], false, &[true, true]);
    }

    #[test]
    fn test_lt_eq_scalar_true_1() {
        cmp_bool_scalar!(
            lt_eq_scalar,
            &[false, true, true, true, true, true, true, true, false],
            true,
            &[true, true, true, true, true, true, true, true, true]
        );
    }

    #[test]
    fn eq_nulls() {
        cmp_bool_options!(
            eq,
            &[
                None,
                None,
                None,
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                Some(true)
            ],
            &[
                None,
                Some(false),
                Some(true),
                None,
                Some(false),
                Some(true),
                None,
                Some(false),
                Some(true)
            ],
            &[
                None,
                None,
                None,
                None,
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(true)
            ]
        );
    }

    fn check_mask(mask: &BooleanArray, expected: &[bool]) {
        assert!(mask.validity().is_none());
        let mask = mask.values_iter().collect::<Vec<_>>();
        assert_eq!(mask, expected);
    }

    #[test]
    fn compare_no_propagating_nulls_eq() {
        // single validity
        let a = Utf8Array::<i32>::from_iter([Some("a"), None, Some("c")]);
        let b = Utf8Array::<i32>::from_iter([Some("a"), Some("c"), Some("c")]);

        let out = comparison::utf8::eq_and_validity(&a, &b);
        check_mask(&out, &[true, false, true]);
        let out = comparison::utf8::eq_and_validity(&b, &a);
        check_mask(&out, &[true, false, true]);

        // both have validities
        let b = Utf8Array::<i32>::from_iter([Some("a"), None, None]);
        let out = comparison::utf8::eq_and_validity(&a, &b);
        check_mask(&out, &[true, true, false]);

        // scalar
        let out = comparison::utf8::eq_scalar_and_validity(&a, "a");
        check_mask(&out, &[true, false, false]);

        // now we add a mask while we know that underlying values are equal
        let a = Utf8Array::<i32>::from_iter([Some("a"), Some("b"), Some("c")]);

        // now mask with a null
        let mask = Bitmap::from_iter([false, true, true]);
        let a_masked = a.with_validity(Some(mask));
        let out = comparison::utf8::eq_and_validity(&a, &a_masked);
        check_mask(&out, &[false, true, true]);

        // other types
        let a = Int32Array::from_iter([Some(1), Some(2), Some(3)]);
        let b = Int32Array::from_iter([Some(1), Some(2), None]);
        let out = comparison::primitive::eq_and_validity(&a, &b);
        check_mask(&out, &[true, true, false]);

        let a = BooleanArray::from_iter([Some(true), Some(false), Some(false)]);
        let b = BooleanArray::from_iter([Some(true), Some(true), None]);
        let out = comparison::boolean::eq_and_validity(&a, &b);
        check_mask(&out, &[true, false, false]);
    }

    #[test]
    fn compare_no_propagating_nulls_neq() {
        // single validity
        let a = Utf8Array::<i32>::from_iter([Some("a"), None, Some("c")]);
        let b = Utf8Array::<i32>::from_iter([Some("foo"), Some("c"), Some("c")]);

        let out = comparison::utf8::neq_and_validity(&a, &b);
        check_mask(&out, &[true, true, false]);
        let out = comparison::utf8::neq_and_validity(&b, &a);
        check_mask(&out, &[true, true, false]);

        // both have validities
        let b = Utf8Array::<i32>::from_iter([Some("a"), None, None]);
        let out = comparison::utf8::neq_and_validity(&a, &b);
        check_mask(&out, &[false, false, true]);

        // scalar
        let out = comparison::utf8::neq_scalar_and_validity(&a, "a");
        check_mask(&out, &[false, true, true]);

        // now we add a mask while we know that underlying values are equal
        let a = Utf8Array::<i32>::from_iter([Some("a"), Some("b"), Some("c")]);

        // now mask with a null
        let mask = Bitmap::from_iter([false, true, true]);
        let a_masked = a.with_validity(Some(mask));
        let out = comparison::utf8::neq_and_validity(&a, &a_masked);
        check_mask(&out, &[true, false, false]);

        // other types
        let a = Int32Array::from_iter([Some(1), Some(2), Some(3)]);
        let b = Int32Array::from_iter([Some(1), Some(2), None]);
        let out = comparison::primitive::neq_and_validity(&a, &b);
        check_mask(&out, &[false, false, true]);

        let a = BooleanArray::from_iter([Some(true), Some(false), Some(false)]);
        let b = BooleanArray::from_iter([Some(true), Some(true), None]);
        let out = comparison::boolean::neq_and_validity(&a, &b);
        check_mask(&out, &[false, true, true]);
    }
}
