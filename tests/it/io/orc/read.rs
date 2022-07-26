use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::error::Error;
use arrow2::io::orc::{format, read};

#[test]
fn infer() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let (_, footer, _) = format::read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&footer)?;

    assert_eq!(schema.fields.len(), 6);
    Ok(())
}

#[test]
fn float32() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let (ps, footer, _) = format::read::read_metadata(&mut reader)?;
    let stripe = read::read_stripe(&mut reader, footer.stripes[0].clone(), ps.compression())?;

    assert_eq!(
        read::deserialize(DataType::Float32, &stripe, 1)?,
        Float32Array::from([Some(1.0), Some(2.0), None, Some(4.0), Some(5.0)]).boxed()
    );

    assert_eq!(
        read::deserialize(DataType::Float32, &stripe, 2)?,
        Float32Array::from([Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]).boxed()
    );
    Ok(())
}

#[test]
fn boolean() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let (ps, footer, _) = format::read::read_metadata(&mut reader)?;
    let stripe = read::read_stripe(&mut reader, footer.stripes[0].clone(), ps.compression())?;

    assert_eq!(
        read::deserialize(DataType::Boolean, &stripe, 3)?,
        BooleanArray::from([Some(true), Some(false), None, Some(true), Some(false)]).boxed()
    );

    assert_eq!(
        read::deserialize(DataType::Boolean, &stripe, 4)?,
        BooleanArray::from([Some(true), Some(false), Some(true), Some(true), Some(false)]).boxed()
    );
    Ok(())
}

#[test]
fn int() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let (ps, footer, _) = format::read::read_metadata(&mut reader)?;
    let stripe = read::read_stripe(&mut reader, footer.stripes[0].clone(), ps.compression())?;

    assert_eq!(
        read::deserialize(DataType::Int32, &stripe, 5)?,
        Int32Array::from([Some(5), Some(-5), None, Some(5), Some(5)]).boxed()
    );

    assert_eq!(
        read::deserialize(DataType::Int32, &stripe, 6)?,
        Int32Array::from([Some(5), Some(-5), Some(1), Some(5), Some(5)]).boxed()
    );
    Ok(())
}
