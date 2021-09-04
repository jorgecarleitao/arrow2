use arrow2::bitmap::Bitmap;

#[test]
fn not_random() {
    let iter = (0..100).map(|x| x % 7 == 0);
    let iter_not = iter.clone().map(|x| !x);

    let bitmap: Bitmap = iter.collect();
    let expected: Bitmap = iter_not.collect();

    assert_eq!(!&bitmap, expected);
}
