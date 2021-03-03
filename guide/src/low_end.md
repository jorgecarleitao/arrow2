# Low end API

The most important design decision of this crate is that contiguous regions are shared via an `Arc`. In this context, the operation of slicing a memory region is `O(1)` because it corresponds to changing an offset and length. The tradeoff is that once under and `Arc`, memory regions are imutable.
Because bitmaps' offsets are measured in bits, but other quantities are measured bytes, this crate contains 2 main types of containers of contiguous memory regions:

* `Buffer<T>`: handle contigous memory regions of type T whose offsets are measured in items
* `Bitmap`: handle contigous memory regions of bits whose offsets are measured in bits

These hold _all_ data-related memory in this crate.

Due to their intrinsic imutability, each container has a corresponding mutable (and non-sharable) variant:

* `MutableBuffer<T>`
* `Buffer<T>`
* `MutableBitmap`
* `Bitmap`

Some examples:

Create a new `Buffer<u32>`:

```rust
# use arrow2::buffer::Buffer;
# fn main() {
let x = Buffer::from(&[1u32, 2, 3]);
assert_eq!(x.as_slice(), &[1u32, 2, 3]);

let x = x.slice(1, 2);
assert_eq!(x.as_slice(), &[2, 3]);
# }
```

Using a `MutableBuffer<f64>`:

```rust
# use arrow2::buffer::{Buffer, MutableBuffer};
# fn main() {
let mut x = MutableBuffer::with_capacity(4);
(0..3).for_each(|i| {
    x.push(i as f64)
});
let x: Buffer<f64> = x.into();
assert_eq!(x.as_slice(), &[0.0, 1.0, 2.0]);
# }
```

In this context, `MutableBuffer` is the closest API to rust's `Vec`.

The following demonstrates how to efficiently 
perform an operation from an iterator of [TrustedLen](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html):

```rust
# use std::iter::FromIterator;
# use arrow2::buffer::{Buffer, MutableBuffer};
# fn main() {
let x = Buffer::from_iter((0..1000));
let iter = x.as_slice().iter().map(|x| x * 2);
let y = unsafe { Buffer::from_trusted_len_iter(iter) };
assert_eq!(y.as_slice()[50], 100);
# }
```

Using `from_trusted_len_iter` often causes the compiler to auto-vectorize.
