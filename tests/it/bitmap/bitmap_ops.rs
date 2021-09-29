use proptest::prelude::*;

use arrow2::bitmap::Bitmap;

use crate::bitmap::bitmap_strategy;

proptest! {
    /// Asserts that !bitmap equals all bits flipped
    #[test]
    #[cfg_attr(miri, ignore)] // miri and proptest do not work well :(
    fn not(bitmap in bitmap_strategy()) {
        let not_bitmap: Bitmap = bitmap.iter().map(|x| !x).collect();

        assert_eq!(!&bitmap, not_bitmap);
    }
}
