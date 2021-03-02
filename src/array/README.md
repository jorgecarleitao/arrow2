# Array module

This document describes the overall design of this module.

## Notation:

* "array" in this module denotes any struct that implements the trait `Array`.
* Coded words denote real items on the implementation.

## Rules:

* Every arrow array with a different physical representation MUST be implemented as a struct or generic struct, here denoted as an "array".

* An array MAY have its own module. E.g. `primitive/mod.rs`

* An array with a null bitmap MUST implement it as `Option<Bitmap>`

* An array MUST be `#[derive(Debug, Clone)]`

* The trait `Array` MUST only be implemented by structs in this module.

* Every child array on the struct MUST be `Arc<dyn Array>`. This enables the struct to be clonable.

* An array MUST implement `from_data` that:
    * panics if the specification is incorrect
    * panics if the arguments lead to unsound code (e.g. a Utf8 array MUST verify that its each item is valid `utf8`)

* An array MAY implement `unsafe from_data_unchecked` that skips the soundness validation. `from_data_unchecked` MUST panic if the specification is incorrect.

* An array MUST implement either `new_empty()` or `new_empty(DataType)` that returns a zero-len of `Self`.

* An array MUST implement either `new_null(length: usize)` or `new_null(DataType, length: usize)` that returns a valid array of length `length`.

* An array MAY implement `value(i: usize)` that returns the value at slot `i` ignoring the validity bitmap.

* An array SHOULD have a `offset: usize` measuring the number of slots that the array is currently offsetted by.

* An array MUST implement `fn slice(&self, offset: usize, length: usize) -> Self` that returns an offseted and/or truncated clone of the array. This function MUST increase the array's offset if it exists.

* Conversely, `offset` MUST only be changed by `slice`.
