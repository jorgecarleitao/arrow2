use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::error::Error;
use arrow2::io::orc::{format, read};

#[test]
fn infer() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let metadata = format::read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&metadata.footer)?;

    assert_eq!(schema.fields.len(), 12);
    Ok(())
}

#[test]
fn float32() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let metadata = format::read::read_metadata(&mut reader)?;
    let footer = format::read::read_stripe_footer(&mut reader, &metadata, 0, &mut vec![])?;

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 1, vec![])?;

    assert_eq!(
        read::deserialize(DataType::Float32, &column)?,
        Float32Array::from([Some(1.0), Some(2.0), None, Some(4.0), Some(5.0)]).boxed()
    );

    let (footer, scratch) = column.into_inner();

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 2, scratch)?;

    assert_eq!(
        read::deserialize(DataType::Float32, &column)?,
        Float32Array::from([Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]).boxed()
    );
    Ok(())
}

#[test]
fn float64() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let metadata = format::read::read_metadata(&mut reader)?;
    let footer = format::read::read_stripe_footer(&mut reader, &metadata, 0, &mut vec![])?;

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 7, vec![])?;

    assert_eq!(
        read::deserialize(DataType::Float64, &column)?,
        Float64Array::from([Some(1.0), Some(2.0), None, Some(4.0), Some(5.0)]).boxed()
    );

    let (footer, scratch) = column.into_inner();

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 8, scratch)?;

    assert_eq!(
        read::deserialize(DataType::Float64, &column)?,
        Float64Array::from([Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]).boxed()
    );
    Ok(())
}

#[test]
fn boolean() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let metadata = format::read::read_metadata(&mut reader)?;
    let footer = format::read::read_stripe_footer(&mut reader, &metadata, 0, &mut vec![])?;

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 3, vec![])?;

    assert_eq!(
        read::deserialize(DataType::Boolean, &column)?,
        BooleanArray::from([Some(true), Some(false), None, Some(true), Some(false)]).boxed()
    );

    let (footer, scratch) = column.into_inner();

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 4, scratch)?;

    assert_eq!(
        read::deserialize(DataType::Boolean, &column)?,
        BooleanArray::from([Some(true), Some(false), Some(true), Some(true), Some(false)]).boxed()
    );
    Ok(())
}

#[test]
fn int() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let metadata = format::read::read_metadata(&mut reader)?;
    let footer = format::read::read_stripe_footer(&mut reader, &metadata, 0, &mut vec![])?;

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 6, vec![])?;

    assert_eq!(
        read::deserialize(DataType::Int32, &column)?,
        Int32Array::from([Some(5), Some(-5), Some(1), Some(5), Some(5)]).boxed()
    );

    let (footer, _scratch) = column.into_inner();

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 5, vec![])?;

    assert_eq!(
        read::deserialize(DataType::Int32, &column)?,
        Int32Array::from([Some(5), Some(-5), None, Some(5), Some(5)]).boxed()
    );
    Ok(())
}

#[test]
fn bigint() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let metadata = format::read::read_metadata(&mut reader)?;
    let footer = format::read::read_stripe_footer(&mut reader, &metadata, 0, &mut vec![])?;

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 10, vec![])?;

    assert_eq!(
        read::deserialize(DataType::Int64, &column)?,
        Int64Array::from([Some(5), Some(-5), Some(1), Some(5), Some(5)]).boxed()
    );

    let (footer, scratch) = column.into_inner();

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 9, scratch)?;

    assert_eq!(
        read::deserialize(DataType::Int64, &column)?,
        Int64Array::from([Some(5), Some(-5), None, Some(5), Some(5)]).boxed()
    );
    Ok(())
}

#[test]
fn utf8() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let metadata = format::read::read_metadata(&mut reader)?;
    let footer = format::read::read_stripe_footer(&mut reader, &metadata, 0, &mut vec![])?;

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 11, vec![])?;

    assert_eq!(
        read::deserialize(DataType::Utf8, &column)?,
        Utf8Array::<i32>::from_slice(["a", "bb", "ccc", "dddd", "eeeee"]).boxed()
    );

    let (footer, _scratch) = column.into_inner();

    let column = format::read::read_stripe_column(&mut reader, &metadata, 0, footer, 12, vec![])?;

    assert_eq!(
        read::deserialize(DataType::Utf8, &column)?,
        Utf8Array::<i32>::from([Some("a"), Some("bb"), None, Some("dddd"), Some("eeeee")]).boxed()
    );
    Ok(())
}
