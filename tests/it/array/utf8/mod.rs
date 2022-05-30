use arrow2::{array::*, bitmap::Bitmap, buffer::Buffer, datatypes::DataType, error::Result};

mod mutable;
mod to_mutable;

#[test]
fn basics() {
    let data = vec![Some("hello"), None, Some("hello2")];

    let array: Utf8Array<i32> = data.into_iter().collect();

    assert_eq!(array.value(0), "hello");
    assert_eq!(array.value(1), "");
    assert_eq!(array.value(2), "hello2");
    assert_eq!(unsafe { array.value_unchecked(2) }, "hello2");
    assert_eq!(array.values().as_slice(), b"hellohello2");
    assert_eq!(array.offsets().as_slice(), &[0, 5, 5, 11]);
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice(&[0b00000101], 3))
    );
    assert!(array.is_valid(0));
    assert!(!array.is_valid(1));
    assert!(array.is_valid(2));

    let array2 = Utf8Array::<i32>::from_data(
        DataType::Utf8,
        array.offsets().clone(),
        array.values().clone(),
        array.validity().cloned(),
    );
    assert_eq!(array, array2);

    let array = array.slice(1, 2);
    assert_eq!(array.value(0), "");
    assert_eq!(array.value(1), "hello2");
    // note how this keeps everything: the offsets were sliced
    assert_eq!(array.values().as_slice(), b"hellohello2");
    assert_eq!(array.offsets().as_slice(), &[5, 5, 11]);
}

#[test]
fn empty() {
    let array = Utf8Array::<i32>::new_empty(DataType::Utf8);
    assert_eq!(array.values().as_slice(), b"");
    assert_eq!(array.offsets().as_slice(), &[0]);
    assert_eq!(array.validity(), None);
}

#[test]
fn from() {
    let array = Utf8Array::<i32>::from(&[Some("hello"), Some(" "), None]);

    let a = array.validity().unwrap();
    assert_eq!(a, &Bitmap::from([true, true, false]));
}

#[test]
fn from_slice() {
    let b = Utf8Array::<i32>::from_slice(["a", "b", "cc"]);

    let offsets = Buffer::from(vec![0, 1, 2, 4]);
    let values = Buffer::from(b"abcc".to_vec());
    assert_eq!(
        b,
        Utf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None)
    );
}

#[test]
fn from_iter_values() {
    let b = Utf8Array::<i32>::from_iter_values(["a", "b", "cc"].iter());

    let offsets = Buffer::from(vec![0, 1, 2, 4]);
    let values = Buffer::from(b"abcc".to_vec());
    assert_eq!(
        b,
        Utf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None)
    );
}

#[test]
fn from_trusted_len_iter() {
    let b =
        Utf8Array::<i32>::from_trusted_len_iter(vec![Some("a"), Some("b"), Some("cc")].into_iter());

    let offsets = Buffer::from(vec![0, 1, 2, 4]);
    let values = Buffer::from(b"abcc".to_vec());
    assert_eq!(
        b,
        Utf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None)
    );
}

#[test]
fn try_from_trusted_len_iter() {
    let b = Utf8Array::<i32>::try_from_trusted_len_iter(
        vec![Some("a"), Some("b"), Some("cc")]
            .into_iter()
            .map(Result::Ok),
    )
    .unwrap();

    let offsets = Buffer::from(vec![0, 1, 2, 4]);
    let values = Buffer::from(b"abcc".to_vec());
    assert_eq!(
        b,
        Utf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None)
    );
}

#[test]
fn not_utf8() {
    let offsets = Buffer::from(vec![0, 4]);
    let values = Buffer::from(vec![0, 159, 146, 150]); // invalid utf8
    assert!(Utf8Array::<i32>::try_new(DataType::Utf8, offsets, values, None).is_err());
}

#[test]
fn not_utf8_individually() {
    let offsets = Buffer::from(vec![0, 1, 2]);
    let values = Buffer::from(vec![207, 128]); // each is invalid utf8, but together is valid
    assert!(Utf8Array::<i32>::try_new(DataType::Utf8, offsets, values, None).is_err());
}

#[test]
fn wrong_offsets() {
    let offsets = Buffer::from(vec![0, 5, 4]); // invalid offsets
    let values = Buffer::from(b"abbbbb".to_vec());
    assert!(Utf8Array::<i32>::try_new(DataType::Utf8, offsets, values, None).is_err());
}

#[test]
fn wrong_data_type() {
    let offsets = Buffer::from(vec![0, 4]);
    let values = Buffer::from(b"abbb".to_vec());
    assert!(Utf8Array::<i32>::try_new(DataType::Int32, offsets, values, None).is_err());
}

#[test]
fn out_of_bounds_offsets_panics() {
    // the 10 is out of bounds
    let offsets = Buffer::from(vec![0, 10, 11]);
    let values = Buffer::from(b"abbb".to_vec());
    assert!(Utf8Array::<i32>::try_new(DataType::Utf8, offsets, values, None).is_err());
}

#[test]
fn decreasing_offset_and_ascii_panics() {
    let offsets = Buffer::from(vec![0, 2, 1]);
    let values = Buffer::from(b"abbb".to_vec());
    assert!(Utf8Array::<i32>::try_new(DataType::Utf8, offsets, values, None).is_err());
}

#[test]
fn decreasing_offset_and_utf8_panics() {
    let offsets = Buffer::from(vec![0, 2, 4, 2]); // not increasing
    let values = Buffer::from(vec![207, 128, 207, 128, 207, 128]); // valid utf8
    assert!(Utf8Array::<i32>::try_new(DataType::Utf8, offsets, values, None).is_err());
}

#[test]
#[should_panic]
fn index_out_of_bounds_panics() {
    let offsets = Buffer::from(vec![0, 1, 2, 4]);
    let values = Buffer::from(b"abbb".to_vec());
    let array = Utf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None);

    array.value(3);
}

#[test]
fn debug() {
    let array = Utf8Array::<i32>::from(&[Some("aa"), Some(""), None]);

    assert_eq!(format!("{:?}", array), "Utf8Array[aa, , None]");
}

#[test]
fn into_mut_1() {
    let offsets = Buffer::from(vec![0, 1]);
    let values = Buffer::from(b"a".to_vec());
    let a = values.clone(); // cloned values
    assert_eq!(a, values);
    let array = Utf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None);
    assert!(array.into_mut().is_left());
}

#[test]
fn into_mut_2() {
    let offsets = Buffer::from(vec![0, 1]);
    let values = Buffer::from(b"a".to_vec());
    let a = offsets.clone(); // cloned offsets
    assert_eq!(a, offsets);
    let array = Utf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None);
    assert!(array.into_mut().is_left());
}

#[test]
fn into_mut_3() {
    let offsets = Buffer::from(vec![0, 1]);
    let values = Buffer::from(b"a".to_vec());
    let validity = Some([true].into());
    let a = validity.clone(); // cloned validity
    assert_eq!(a, validity);
    let array = Utf8Array::<i32>::new(DataType::Utf8, offsets, values, validity);
    assert!(array.into_mut().is_left());
}

#[test]
fn into_mut_4() {
    let offsets = Buffer::from(vec![0, 1]);
    let values = Buffer::from(b"a".to_vec());
    let validity = Some([true].into());
    let array = Utf8Array::<i32>::new(DataType::Utf8, offsets, values, validity);
    assert!(array.into_mut().is_right());
}
