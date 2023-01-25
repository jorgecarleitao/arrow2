mod data;
mod stream;

#[test]
fn mmap_slice() {
    let slice = &[1, 2, 3];
    let array = unsafe { arrow2::ffi::mmap::slice(slice) };
    assert_eq!(array.values().as_ref(), &[1, 2, 3]);
    // note: when `slice` is dropped, array must be dropped as-well since by construction of `slice` they share their lifetimes.
}
