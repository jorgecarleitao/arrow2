# High-level API

The simplest way to think about an arrow `Array` is that it represents
`Vec<Option<T>>` and has a logical type (see [metadata](../metadata.md))) associated with it.

Probably the simplest array in this crate is `PrimitiveArray<T>`. It can be constructed
from a slice as follows:

```rust
# use arrow2::array::{Array, PrimitiveArray};
# fn main() {
let array = PrimitiveArray<i32>::from([Some(1), None, Some(123)]);
assert_eq!(array.len(), 3)
# }
```

from a slice of values,

```rust
# use arrow2::array::{Array, PrimitiveArray};
# fn main() {
let array = PrimitiveArray<i32>::from_slice([1, 0, 123]);
assert_eq!(array.len(), 3)
# }
```

or from an iterator

```rust
# use arrow2::array::{Array, PrimitiveArray};
# fn main() {
let array: PrimitiveArray<i32> = [Some(1), None, Some(123)].iter().collect();
assert_eq!(array.len(), 3)
# }
```

A `PrimitiveArray` has 3 components:

1. A physical type (e.g. `i32`)
2. A logical type (e.g. `DataType::Int32`)
3. Data

The main differences from a `Vec<Option<T>>` are:

* Its data is laid out in memory as a `Buffer<T>` and an `Option<Bitmap>` (see [../low_level.md])
* It has an associated logical type (`DataType`).

The first allows interoperability with Arrow's ecosystem and efficient SIMD operations
(we will re-visit this below); the second is that it gives semantic meaning to the array.
In the example

```rust
# use arrow2::array::PrimitiveArray;
# use arrow2::datatypes::DataType;
# fn main() {
let ints = PrimitiveArray::<i32>::from([Some(1), None]);
let dates = PrimitiveArray::<i32>::from([Some(1), None]).to(DataType::Date32);
# }
```

`ints` and `dates` have the same in-memory representation but different logic
representations (e.g. dates are usually printed to users as "yyyy-mm-dd").

All physical types (e.g. `i32`) have a "natural" logical `DataType` (e.g. `DataType::Int32`)
which is assigned when allocating arrays from iterators, slices, etc.

```rust
# use arrow2::array::{Array, Int32Array, PrimitiveArray};
# use arrow2::datatypes::DataType;
# fn main() {
let array = PrimitiveArray<i32>::from_slice([1, 0, 123]);
assert_eq!(array.data_type(), &DataType::Int32);
# }
```
they can be cheaply converted to via `.to(DataType)`.

The following arrays are supported:

* `NullArray` (just holds nulls)
* `BooleanArray` (booleans)
* `PrimitiveArray<T>` (for ints, floats)
* `Utf8Array<i32>` and `Utf8Array<i64>` (for strings)
* `BinaryArray<i32>` and `BinaryArray<i64>` (for opaque binaries)
* `FixedSizeBinaryArray` (like `BinaryArray`, but fixed size)
* `ListArray<i32>` and `ListArray<i64>` (nested arrays)
* `FixedSizeListArray` (nested arrays of fixed size)
* `StructArray` (every row has multiple logical types)
* `UnionArray` (every row has a different logical type)
* `DictionaryArray<K>` (nested array with encoded values)

## Dynamic Array

There is a more powerful aspect of arrow arrays, and that is that they all
implement the trait `Array` and can be cast to `&dyn Array`, i.e. they can be turned into
a trait object. This enables arrays to have types that are dynamic in nature.

```rust
# use arrow2::array::{Array, PrimitiveArray};
# fn main() {
let a = PrimitiveArray::<i32>::from(&[Some(1), None]);
let a: &dyn Array = &a;
# }
```

### Downcast and `as_any`

Given a trait object `array: &dyn Array`, we know its physical type via
`array.data_type().to_physical_type()`, which we use to downcast the array
to its concrete type:

```rust
# use arrow2::array::{Array, PrimitiveArray};
# use arrow2::datatypes::PhysicalType;
# fn main() {
let a = PrimitiveArray::<i32>::from(&[Some(1), None]);
let array = &array as &dyn Array;

match array.data_type().to_physical_type() {
    PhysicalType::Int32 => {
        let array = array.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
        let values: &[i32] = array.values();
        assert_eq!(values, &[1, 0]);
    }
    _ => todo!()
}
# }
```

There is a many-to-one relationship between `DataType` and an Array (i.e. a physical representation). The relationship is the following:

| `PhysicalType`       | `PhysicalType`            |
|----------------------|---------------------------|
| `UInt8`              | `PrimitiveArray<u8>`      |
| `UInt16`             | `PrimitiveArray<u16>`     |
| `UInt32`             | `PrimitiveArray<u32>`     |
| `UInt64`             | `PrimitiveArray<u64>`     |
| `Int8`               | `PrimitiveArray<i8>`      |
| `Int16`              | `PrimitiveArray<i16>`     |
| `Int32`              | `PrimitiveArray<i32>`     |
| `Int64`              | `PrimitiveArray<i64>`     |
| `Int128`             | `PrimitiveArray<i128>`    |
| `Float32`            | `PrimitiveArray<f32>`     |
| `Float64`            | `PrimitiveArray<f64>`     |
| `DaysMs`             | `PrimitiveArray<days_ms>` |
| `Binary`             | `BinaryArray<i32>`        |
| `LargeBinary`        | `BinaryArray<i64>`        |
| `Utf8`               | `Utf8Array<i32>`          |
| `LargeUtf8`          | `Utf8Array<i64>`          |
| `List`               | `ListArray<i32>`          |
| `LargeList`          | `ListArray<i64>`          |
| `FixedSizeBinary`    | `FixedSizeBinaryArray`    |
| `FixedSizeList`      | `FixedSizeListArray`      |
| `Struct`             | `StructArray`             |
| `Union`              | `UnionArray`              |
| `Dictionary(UInt8)`  | `DictionaryArray<u8>`     |
| `Dictionary(UInt16)` | `DictionaryArray<u16>`    |
| `Dictionary(UInt32)` | `DictionaryArray<u32>`    |
| `Dictionary(UInt64)` | `DictionaryArray<u64>`    |
| `Dictionary(Int8)`   | `DictionaryArray<i8>`     |
| `Dictionary(Int16)`  | `DictionaryArray<i16>`    |
| `Dictionary(Int32)`  | `DictionaryArray<i32>`    |
| `Dictionary(Int64)`  | `DictionaryArray<i64>`    |

## From Iterator

In the examples above, we've introduced how to create an array from an iterator.
These APIs are available for all Arrays, and they are suitable to efficiently
create them. In this section we will go a bit more in detail about these operations,
and how to make them even more efficient.

This crate's APIs are generally split into two patterns: whether an operation leverages
contiguous memory regions or whether it does not.

If yes, then use:

* `Buffer::from_iter`
* `Buffer::from_trusted_len_iter`
* `Buffer::try_from_trusted_len_iter`

If not, then use the builder API, such as `MutablePrimitiveArray<T>`, `MutableUtf8Array<O>` or `MutableListArray`.

We have seen examples where the latter API was used. In the last example of this page
you will be introduced to an example of using the former for SIMD.

## Into Iterator

We've already seen how to create an array from an iterator. Most arrays also implement
`IntoIterator`:

```rust
# use arrow2::array::{Array, Int32Array};
# fn main() {
let array = Int32Array::from(&[Some(1), None, Some(123)]);

for item in array.iter() {
    if let Some(value) = item {
        println!("{}", value);
    } else {
        println!("NULL");
    }
}
# }
```

Like `FromIterator`, this crate contains two sets of APIs to iterate over data. Given
an array `array: &PrimitiveArray<T>`, the following applies:

1. If you need to iterate over `Option<&T>`, use `array.iter()`
2. If you can operate over the values and validity independently, use `array.values() -> &Buffer<T>` and `array.validity() -> &Option<Bitmap>`

Note that case 1 is useful when e.g. you want to perform an operation that depends on both validity and values, while the latter is suitable for SIMD and copies, as they return contiguous memory regions (buffers and bitmaps). We will see below how to leverage these APIs.

This idea holds more generally in this crate's arrays: `values()` returns something that has a contiguous in-memory representation, while `iter()` returns items taking validity into account. To get an iterator over contiguous values, use `array.values().iter()`.

There is one last API that is worth mentioning, and that is `Bitmap::chunks`. When performing
bitwise operations, it is often more performant to operate on chunks of bits instead of single bits. `chunks` offers a `TrustedLen` of `u64` with the bits + an extra `u64` remainder. We expose two functions, `unary(Bitmap, Fn) -> Bitmap` and `binary(Bitmap, Bitmap, Fn) -> Bitmap` that use this API to efficiently perform bitmap operations.

## Vectorized operations

One of the main advantages of the arrow format and its memory layout is that
it often enables SIMD. For example, an unary operation `op` on a `PrimitiveArray` is likely auto-vectorized on the following code:

```rust
# use arrow2::buffer::Buffer;
# use arrow2::{
#     array::{Array, PrimitiveArray},
#     types::NativeType,
#     datatypes::DataType,
# };

pub fn unary<I, F, O>(array: &PrimitiveArray<I>, op: F, data_type: &DataType) -> PrimitiveArray<O>
where
    I: NativeType,
    O: NativeType,
    F: Fn(I) -> O,
{
    let values = array.values().iter().map(|v| op(*v));
    let values = Buffer::from_trusted_len_iter(values);

    PrimitiveArray::<O>::from_data(data_type.clone(), values, array.validity().clone())
}
```

Some notes:

1. We used `array.values()`, as described above: this operation leverages a contiguous memory region.

2. We leveraged normal rust iterators for the operation.

3. We used `op` on the array's values irrespectively of their validity,
and cloned its validity. This approach is suitable for operations whose branching off is more expensive than operating over all values. If the operation is expensive, then using `PrimitiveArray::<O>::from_trusted_len_iter` is likely faster.
