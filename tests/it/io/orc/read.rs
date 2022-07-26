use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::error::Error;
use arrow2::io::orc::{format, read};

#[test]
fn infer() -> Result<(), Error> {
    let mut reader = std::fs::File::open("fixtures/pyorc/test.orc").unwrap();
    let (ps, footer, _) = format::read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&footer)?;

    assert_eq!(schema.fields.len(), 12);

    let stripe = read::read_stripe(&mut reader, footer.stripes[0].clone(), ps.compression())?;

    let array = read::deserialize_f32(DataType::Float32, &stripe, 1)?;
    assert_eq!(
        array,
        Float32Array::from([Some(1.0), Some(2.0), None, Some(4.0), Some(5.0)])
    );

    let array = read::deserialize_bool(DataType::Boolean, &stripe, 2)?;
    assert_eq!(
        array,
        BooleanArray::from([Some(true), Some(false), None, Some(true), Some(false)])
    );
    Ok(())
}
