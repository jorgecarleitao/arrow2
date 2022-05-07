use arrow2::array::*;
use arrow2::error::Result;
use hash_hasher::HashedMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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

#[test]
fn push_utf8() {
    let mut new: MutableDictionaryArray<i32, MutableUtf8Array<i32>> = MutableDictionaryArray::new();

    for value in [Some("A"), Some("B"), None, Some("C"), Some("A"), Some("B")] {
        new.try_push(value).unwrap();
    }

    assert_eq!(
        new.values().values(),
        MutableUtf8Array::<i32>::from_iter_values(["A", "B", "C"].into_iter()).values()
    );

    let mut expected_keys = MutablePrimitiveArray::<i32>::from_slice(&[0, 1]);
    expected_keys.push(None);
    expected_keys.push(Some(2));
    expected_keys.push(Some(0));
    expected_keys.push(Some(1));
    assert_eq!(*new.keys(), expected_keys);

    let expected_map = ["A", "B", "C"]
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            let hash = hasher.finish();
            (hash, index as i32)
        })
        .collect::<HashedMap<_, _>>();
    assert_eq!(*new.map(), expected_map);
}
