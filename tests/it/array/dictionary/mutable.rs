use arrow2::array::*;
use arrow2::error::Result;

#[test]
fn primitive() -> Result<()> {
    let data = vec![Some(1), Some(2), Some(1)];

    let mut a = MutableDictionaryArray::<i32, MutablePrimitiveArray<i32>>::new();
    a.try_extend(data)?;
    assert_eq!(a.len(), 3);
    assert_eq!(a.values().len(), 2);
    Ok(())
}

#[test]
fn utf8_natural() -> Result<()> {
    let data = vec![Some("a"), Some("b"), Some("a")];

    let mut a = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    a.try_extend(data)?;

    assert_eq!(a.len(), 3);
    assert_eq!(a.values().len(), 2);
    Ok(())
}

#[test]
fn binary_natural() -> Result<()> {
    let data = vec![
        Some("a".as_bytes()),
        Some("b".as_bytes()),
        Some("a".as_bytes()),
    ];

    let mut a = MutableDictionaryArray::<i32, MutableBinaryArray<i32>>::new();
    a.try_extend(data)?;
    assert_eq!(a.len(), 3);
    assert_eq!(a.values().len(), 2);
    Ok(())
}
