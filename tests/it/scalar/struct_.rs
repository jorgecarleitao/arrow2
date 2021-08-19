use std::sync::Arc;

use arrow2::{
    datatypes::{DataType, Field},
    scalar::{BooleanScalar, Scalar, StructScalar},
};

#[allow(clippy::eq_op)]
#[test]
fn equal() {
    let dt = DataType::Struct(vec![Field::new("a", DataType::Boolean, true)]);
    let a = StructScalar::new(
        dt.clone(),
        Some(vec![
            Arc::new(BooleanScalar::from(Some(true))) as Arc<dyn Scalar>
        ]),
    );
    let b = StructScalar::new(dt.clone(), None);
    assert_eq!(a, a);
    assert_eq!(b, b);
    assert!(a != b);
    let b = StructScalar::new(
        dt,
        Some(vec![
            Arc::new(BooleanScalar::from(Some(false))) as Arc<dyn Scalar>
        ]),
    );
    assert!(a != b);
    assert_eq!(b, b);
}

#[test]
fn basics() {
    let dt = DataType::Struct(vec![Field::new("a", DataType::Boolean, true)]);

    let values = vec![Arc::new(BooleanScalar::from(Some(true))) as Arc<dyn Scalar>];

    let a = StructScalar::new(dt.clone(), Some(values.clone()));

    assert_eq!(a.values(), &values);
    assert_eq!(a.data_type(), &dt);
    assert!(a.is_valid());

    let _: &dyn std::any::Any = a.as_any();
}
