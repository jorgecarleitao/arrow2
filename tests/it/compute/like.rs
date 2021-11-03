use arrow2::array::*;
use arrow2::compute::like::*;
use arrow2::error::Result;

#[test]
fn test_like_binary() -> Result<()> {
    let strings =
        BinaryArray::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "Arrow", "Ar", "Arrow"]);
    let patterns = BinaryArray::<i32>::from_slice(&["A%", "B%", "%r_ow", "A_", "A_", "A\\%"]);
    let result = like_binary(&strings, &patterns).unwrap();
    assert_eq!(
        result,
        BooleanArray::from_slice(&[true, false, true, false, true, false])
    );
    Ok(())
}

#[test]
fn test_nlike_binary() -> Result<()> {
    let strings =
        BinaryArray::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "Arrow", "Ar", "Arrow"]);
    let patterns = BinaryArray::<i32>::from_slice(&["A%", "B%", "%r_ow", "A_", "A_", "A\\%"]);
    let result = nlike_binary(&strings, &patterns).unwrap();
    assert_eq!(
        result,
        BooleanArray::from_slice(&[false, true, false, true, false, true])
    );
    Ok(())
}

#[test]
fn test_like_binary_scalar() -> Result<()> {
    let array = BinaryArray::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "BA"]);
    let result = like_binary_scalar(&array, b"A%").unwrap();
    assert_eq!(result, BooleanArray::from_slice(&[true, true, true, false]));
    Ok(())
}

#[test]
fn test_nlike_binary_scalar() -> Result<()> {
    let array = BinaryArray::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "BA"]);
    let result = nlike_binary_scalar(&array, "A%".as_bytes()).unwrap();
    assert_eq!(
        result,
        BooleanArray::from_slice(&[false, false, false, true])
    );
    Ok(())
}
