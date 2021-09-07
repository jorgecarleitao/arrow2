use arrow2::array::BooleanArray;
use arrow2::compute::boolean_kleene::*;

#[test]
fn and_generic() {
    let lhs = BooleanArray::from(&[
        None,
        None,
        None,
        Some(false),
        Some(false),
        Some(false),
        Some(true),
        Some(true),
        Some(true),
    ]);
    let rhs = BooleanArray::from(&[
        None,
        Some(false),
        Some(true),
        None,
        Some(false),
        Some(true),
        None,
        Some(false),
        Some(true),
    ]);
    let c = and(&lhs, &rhs).unwrap();

    let expected = BooleanArray::from(&[
        None,
        Some(false),
        None,
        Some(false),
        Some(false),
        Some(false),
        None,
        Some(false),
        Some(true),
    ]);

    assert_eq!(c, expected);
}

#[test]
fn or_generic() {
    let a = BooleanArray::from(&[
        None,
        None,
        None,
        Some(false),
        Some(false),
        Some(false),
        Some(true),
        Some(true),
        Some(true),
    ]);
    let b = BooleanArray::from(&[
        None,
        Some(false),
        Some(true),
        None,
        Some(false),
        Some(true),
        None,
        Some(false),
        Some(true),
    ]);
    let c = or(&a, &b).unwrap();

    let expected = BooleanArray::from(&[
        None,
        None,
        Some(true),
        None,
        Some(false),
        Some(true),
        Some(true),
        Some(true),
        Some(true),
    ]);

    assert_eq!(c, expected);
}

#[test]
fn or_right_nulls() {
    let a = BooleanArray::from_slice(&[false, false, false, true, true, true]);

    let b = BooleanArray::from(&[Some(true), Some(false), None, Some(true), Some(false), None]);

    let c = or(&a, &b).unwrap();

    let expected = BooleanArray::from(&[
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(true),
        Some(true),
    ]);

    assert_eq!(c, expected);
}

#[test]
fn or_left_nulls() {
    let a = BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
    ]);

    let b = BooleanArray::from_slice(&[false, false, false, true, true, true]);

    let c = or(&a, &b).unwrap();

    let expected = BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(true),
        Some(true),
    ]);

    assert_eq!(c, expected);
}
