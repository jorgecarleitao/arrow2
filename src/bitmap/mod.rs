//! contains [`Bitmap`] and [`MutableBitmap`], containers of `bool`.
mod immutable;
pub use immutable::*;

mod mutable;
pub use mutable::MutableBitmap;

mod bitmap_ops;
pub use bitmap_ops::*;

pub mod utils;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subslicing_gives_correct_null_count() {
        let mut base = MutableBitmap::new();
        base.push(false);
        base.push(true);
        base.push(true);
        base.push(false);
        base.push(false);
        base.push(true);
        base.push(true);
        base.push(true);

        let base = Bitmap::from(base);
        assert_eq!(base.null_count(), 3);

        let view1 = base.clone().slice(0, 1);
        let view2 = base.slice(1, 7);
        assert_eq!(view1.null_count(), 1);
        assert_eq!(view2.null_count(), 2);

        let view3 = view2.slice(0, 1);
        assert_eq!(view3.null_count(), 0);
    }
}
