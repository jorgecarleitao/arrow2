# Array module

This document describes the overall design of this module.

## Notation:

* "array" in this module denotes any struct that implements the trait `Array`.
* "mutable array" in this module denotes any struct that implements the trait `MutableArray`.
* words in `code` denote existing terms on this implementation.

## Arrays:

* Every arrow array with a different physical representation MUST be implemented as a struct or generic struct.

* An array MAY have its own module. E.g. `primitive/mod.rs`

* An array with a null bitmap MUST implement it as `Option<Bitmap>`

* An array MUST be `#[derive(Debug, Clone)]`

* The trait `Array` MUST only be implemented by structs in this module.

* Every child array on the struct MUST be `Arc<dyn Array>`. This enables the struct to be clonable.

* An array MUST implement `from_data(...) -> Self`. This method MUST panic iff:
    * the data does not follow the arrow specification
    * the arguments lead to unsound code (e.g. a Utf8 array MUST verify that its each item is valid `utf8`)

* An array MAY implement `unsafe from_data_unchecked` that skips the soundness validation. `from_data_unchecked` MUST panic if the specification is incorrect.

* An array MUST implement either `new_empty()` or `new_empty(DataType)` that returns a zero-len of `Self`.

* An array MUST implement either `new_null(length: usize)` or `new_null(DataType, length: usize)` that returns a valid array of length `length` whose all elements are null.

* An array MAY implement `value(i: usize)` that returns the value at slot `i` ignoring the validity bitmap.

* functions to create new arrays from native Rust SHOULD be named as follows:
    * `from`: from a slice of optional values (e.g. `AsRef<[Option<bool>]` for `BooleanArray`)
    * `from_slice`: from a slice of values (e.g. `AsRef<[bool]` for `BooleanArray`)
    * `from_trusted_len_iter` from an iterator of trusted len of optional values
    * `from_trusted_len_values_iter` from an iterator of trusted len of values
    * `try_from_trusted_len_iter` from an fallible iterator of trusted len of optional values

### Slot offsets

* An array MUST have a `offset: usize` measuring the number of slots that the array is currently offsetted by if the specification requires.

* An array MUST implement `fn slice(&self, offset: usize, length: usize) -> Self` that returns an offseted and/or truncated clone of the array. This function MUST increase the array's offset if it exists.

* Conversely, `offset` MUST only be changed by `slice`.

The rational of the above is that it enable us to be fully interoperable with the offset logic supported by the C data interface, while at the same time easily perform array slices
within Rust's type safety mechanism.

### Mutable Arrays

* An array MAY have a mutable counterpart. E.g. `MutablePrimitiveArray<T>` is the mutable counterpart of `PrimitiveArray<T>`.

* Arrays with mutable counterparts MUST have its own module, and have the mutable counterpart declared in `{module}/mutable.rs`.

* The trait `MutableArray` MUST only be implemented by mutable arrays in this module.

* A mutable array MUST be `#[derive(Debug)]`

* A mutable array with a null bitmap MUST implement it as `Option<MutableBitmap>`

* Converting a `MutableArray` to its immutable counterpart MUST be `O(1)`. Specifically:
    * it must not allocate
    * it must not cause `O(N)` data transformations

    This is achieved by converting mutable versions to immutable counterparts (e.g. `MutableBitmap -> Bitmap`).

    The rational is that `MutableArray`s can be used to perform in-place operations under 
    the arrow spec.
