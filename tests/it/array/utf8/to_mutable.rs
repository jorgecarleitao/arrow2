use arrow2::{array::Utf8Array, bitmap::Bitmap, buffer::Buffer, datatypes::DataType};

#[test]
fn not_shared() {
    let array = Utf8Array::<i32>::from(&[Some("hello"), Some(" "), None]);
    assert!(array.into_mut().is_right());
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_validity() {
    let validity = Bitmap::from([true]);
    let array = Utf8Array::<i32>::new(
        DataType::Utf8,
        vec![0, 1].into(),
        b"a".to_vec().into(),
        Some(validity.clone()),
    );
    assert!(array.into_mut().is_left())
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_values() {
    let values: Buffer<u8> = b"a".to_vec().into();
    let array = Utf8Array::<i32>::new(
        DataType::Utf8,
        vec![0, 1].into(),
        values.clone(),
        Some(Bitmap::from([true])),
    );
    assert!(array.into_mut().is_left())
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_offsets_values() {
    let offsets: Buffer<i32> = vec![0, 1].into();
    let values: Buffer<u8> = b"a".to_vec().into();
    let array = Utf8Array::<i32>::new(
        DataType::Utf8,
        offsets.clone(),
        values.clone(),
        Some(Bitmap::from([true])),
    );
    assert!(array.into_mut().is_left())
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_offsets() {
    let offsets: Buffer<i32> = vec![0, 1].into();
    let array = Utf8Array::<i32>::new(
        DataType::Utf8,
        offsets.clone(),
        b"a".to_vec().into(),
        Some(Bitmap::from([true])),
    );
    assert!(array.into_mut().is_left())
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_all() {
    let array = Utf8Array::<i32>::from(&[Some("hello"), Some(" "), None]);
    assert!(array.clone().into_mut().is_left())
}
