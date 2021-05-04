# Foreign Interfaces

One of the hallmarks of the Arrow format is that its in-memory representation
has a specification, which allows languages to share data
structures via foreign interfaces at zero cost (i.e. via pointers).
This is known as the [C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html).

This crate supports importing from and exporting to most of `DataType`s.
Types currently not supported:

* `FixedSizeBinary`
* `Union`
* `Dictionary`
* `FixedSizeList`
* `Null`

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

// export the array.
let ffi_array = ffi::export_to_c(Arc::new(array))?;

// these are mutable pointers to `ArrowArray` and `ArrowSchema` of the C data interface
let (array_ptr, schema_ptr) = ffi_array.references();
# }
```

## Import

The API to import works similarly:

```rust
use arrow2::array::Array;
use arrow2::ffi;

let array = Arc::new(ffi::create_empty());

// non-owned mutable pointers.
let (array_ptr, schema_ptr) = array.references();

// write to the pointers using any C data interface exporter

// consume it to a `Box<dyn Array>`
let array = ffi::try_from(array)?;
```

This assumes that the exporter writes to `array_ptr` and `schema_ptr` 
according to the c data interface. This is an intrinsically `unsafe` operation.
Failing to do so results in UB.
