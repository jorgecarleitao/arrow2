use arrow2::array::*;
use arrow2::compute::comparison::boolean::*;
use arrow2::datatypes::DataType::*;
use arrow2::datatypes::TimeUnit;
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
        if can_eq(&d1) {
            eq(array.as_ref(), array.as_ref());
        }
    });

    // array <> scalar
    datatypes.into_iter().for_each(|d1| {
        let array = new_null_array(d1.clone(), 10);
        let scalar = new_scalar(array.as_ref(), 0);
        if can_eq(&d1) {
            eq_scalar(array.as_ref(), scalar.as_ref());
        }
    });
}

// disable wrapping inside literal vectors used for test data and assertions
#[rustfmt::skip::macros(vec)]
#[cfg(test)]
mod tests {
    use super::*;

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
}
