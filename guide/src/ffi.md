# Foreign Interfaces

One of the hallmarks of the Arrow format is that its in-memory representation has a specification, which allows languages to share data structures via foreign interfaces
at zero cost (i.e. via pointers). This is known as the [C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html).

Currently, this crate supports importing from and exporting to all non-nested DataTypes.

## Export

The API to export an `Array` is as follows:

```rust
use std::sync::Arc;
use arrow2::array::{Array, Primitive};
use arrow2::datatypes::DataType;
use arrow2::ffi::ArrowArray;

# fn main() {
// Example of an array:
let array = [Some(1), None, Some(123)]
    .iter()
    .collect::<Primitive<i32>>()
    .to(DataType::Int32);
// create a struct from it with the API to export it.
let array = ArrowArray::try_from(Arc::new(array))?;

// export: these are `ArrowArray` and `ArrowSchema` of the C data interface
let (array, schema) = ArrowArray::into_raw(array);
# }
```

Note that this leaks memory iff the consumers of these
pointers do not `release` them according to the [member allocation](https://arrow.apache.org/docs/format/CDataInterface.html#release-callback-semantics-for-consumers).

## Import

The API to import works similarly:

```rust,ignore
use arrow2::array::Array;
use arrow2::ffi::ArrowArray;

// these are `ArrowArray` and `ArrowSchema` of the C data interface
let array = unsafe { ArrowArray::try_from_raw(array, schema) }?;

let array = Box::<dyn Array>::try_from(array)?;
```

Note that this assumes that `array` and `schema` are valid pointers and
that the data on it is also layed out according to the c data interface.
This is impossible to prove at neither compile nor run-time and is thus
intrinsically `unsafe`.

Soundness here relies on the producer creating `array` and `schema` that
follow the c data interface specification.
In particular, if the producer reports an array length larger than what its buffer
was allocated for, it will cause this crate to read out of bounds.
