use arrow2::array::Utf8Array;

#[test]
#[allow(clippy::redundant_clone)]
fn array_to_mutable() {
    let array = Utf8Array::<i32>::from(&[Some("hello"), Some(" "), None]);
    let mutable = array.into_mut().unwrap_right();

    let array: Utf8Array<i32> = mutable.into();
    let array2 = array.clone();
    let maybe_mut = array2.into_mut();
    // the ref count is 2 we should not get a mutable.
    assert!(maybe_mut.is_left())
}
