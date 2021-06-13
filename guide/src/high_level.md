# High-level API

The simplest way to think about an arrow `Array` is that it represents
`Vec<Option<T>>` and has a logical type associated with it.

Probably the most simple array in this crate is `PrimitiveArray<T>`. It can be constructed
from an iterator as follows:

```rust
# use arrow2::array::{Array, PrimitiveArray, Primitive};
# use arrow2::datatypes::DataType;
# fn main() {
let array: PrimitiveArray<i32> = [Some(1), None, Some(123)]
    .iter()
    .collect::<Primitive<i32>>()
    .to(DataType::Int32);
assert_eq!(array.len(), 3)
# }
```

A `PrimitiveArray` has 3 components:

1. A physical type (`i32`)
2. A logical type (e.g. `DataType::Int32`)
3. Data

The main differences from a `Vec<Option<T>>` are:

* Its data is laid out in memory as a `Buffer<T>` and an `Option<Bitmap>`.
* It has an associated logical datatype.

The first difference allows interoperability with Arrow's ecosystem and efficient SIMD operations (we will re-visit this below); the second difference is that it gives semantic meaning to the array. In the example

```rust
# use arrow2::array::Primitive;
# use arrow2::datatypes::DataType;
# fn main() {
let ints = Primitive::<i32>::from(&[Some(1), None]).to(DataType::Int32);
let dates = Primitive::<i32>::from(&[Some(1), None]).to(DataType::Date32);
# }
```

`ints` and `dates` have the same in-memory representation but different logic representations (e.g. dates are usually represented as a string).

Some physical types (e.g. `i32`) have a "natural" logical `DataType` (e.g. `DataType::Int32`).
These types support a more compact notation:

```rust
# use arrow2::array::{Array, Int32Array, Primitive};
# use arrow2::datatypes::DataType;
# fn main() {
/// Int32Array = PrimitiveArray<i32>
let array = [Some(1), None, Some(123)].iter().collect::<Int32Array>();
assert_eq!(array.len(), 3);
let array = Int32Array::from(&[Some(1), None, Some(123)]);
assert_eq!(array.len(), 3);
let array = Int32Array::from_slice(&[1, 123]);
assert_eq!(array.len(), 2);
# }
```

The following arrays are supported:

* `NullArray` (just holds nulls)
* `BooleanArray` (booleans)
* `PrimitiveArray<T>` (for ints, floats)
* `Utf8Array<i32>` and `Utf8Array<i64>` (for strings)
* `BinaryArray<i32>` and `BinaryArray<i64>` (for opaque binaries)
* `FixedSizeBinaryArray` (like `BinaryArray`, but fixed size)
* `ListArray<i32>` and `ListArray<i64>` (nested arrays)
* `FixedSizeListArray` (nested arrays of fixed size)
* `StructArray` (when each row has different logical types)
* `DictionaryArray<K>` (nested array with encoded values)

## Dynamic Array

There is a more powerful aspect of arrow arrays, and that is that they all
implement the trait `Array` and can be cast to `&dyn Array`, i.e. they can be turned into
a trait object. This enables arrays to have types that are dynamic in nature.
`ListArray<i32>` is an example of a nested (dynamic) array:

```rust
# use std::sync::Arc;
# use arrow2::array::{Array, ListPrimitive, ListArray, Primitive};
# use arrow2::datatypes::DataType;
# fn main() {
let data = vec![
    Some(vec![Some(1i32), Some(2), Some(3)]),
    None,
    Some(vec![Some(4), None, Some(6)]),
];

let a: ListArray<i32> = data
    .into_iter()
    .collect::<ListPrimitive<i32, Primitive<i32>, i32>>()
    .to(ListArray::<i32>::default_datatype(DataType::Int32));

let inner: &Arc<dyn Array> = a.values();
# }
```

Note how we have not specified the inner type explicitly in the signature `ListArray<i32>`.
Instead, `ListArray` has an inner `Array` representing all its values (available via `.values()`).

### Downcast and `as_any`

Given a trait object `&dyn Array`, we know its logical type via `Array::data_type()` and can use it to downcast the array to its concrete type:

```rust
# use arrow2::array::{Array, PrimitiveArray, Primitive};
# use arrow2::datatypes::DataType;
# fn main() {
let array: PrimitiveArray<i32> = [Some(1), None, Some(123)]
    .iter()
    .collect::<Primitive<i32>>()
    .to(DataType::Int32);
let array = &array as &dyn Array;

let array = array.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
# }
```

There is a many-to-one relationship between `DataType` and an Array (i.e. a physical representation). The relationship is the following:

| `DataType`            | `PhysicalType`            |
|-----------------------|---------------------------|
| `UInt8`               | `PrimitiveArray<u8>`      |
| `UInt16`              | `PrimitiveArray<u16>`     |
| `UInt32`              | `PrimitiveArray<u32>`     |
| `UInt64`              | `PrimitiveArray<u64>`     |
| `Int8`                | `PrimitiveArray<i8>`      |
| `Int16`               | `PrimitiveArray<i16>`     |
| `Int32`               | `PrimitiveArray<i32>`     |
| `Int64`               | `PrimitiveArray<i64>`     |
| `Float32`             | `PrimitiveArray<f32>`     |
| `Float64`             | `PrimitiveArray<f64>`     |
| `Decimal(_,_)`        | `PrimitiveArray<i128>`    |
| `Date32`              | `PrimitiveArray<i32>`     |
| `Date64`              | `PrimitiveArray<i64>`     |
| `Time32(_)`           | `PrimitiveArray<i32>`     |
| `Time64(_)`           | `PrimitiveArray<i64>`     |
| `Timestamp(_,_)`      | `PrimitiveArray<i64>`     |
| `Interval(YearMonth)` | `PrimitiveArray<i32>`     |
| `Interval(DayTime)`   | `PrimitiveArray<days_ms>` |
| `Duration(_)`         | `PrimitiveArray<i64>`     |
| `Binary`              | `BinaryArray<i32>`        |
| `LargeBinary`         | `BinaryArray<i64>`        |
| `Utf8`                | `Utf8Array<i32>`          |
| `LargeUtf8`           | `Utf8Array<i64>`          |
| `List`                | `ListArray<i32>`          |
| `LargeList`           | `ListArray<i64>`          |
| `FixedSizeBinary(_)`  | `FixedSizeBinaryArray`    |
| `FixedSizeList(_,_)`  | `FixedSizeListArray`      |
| `Struct(_)`           | `StructArray`             |
| `Dictionary(UInt8,_)` | `DictionaryArray<u8>`     |
| `Dictionary(UInt16,_)`| `DictionaryArray<u16>`    |
| `Dictionary(UInt32,_)`| `DictionaryArray<u32>`    |
| `Dictionary(UInt64,_)`| `DictionaryArray<u64>`    |
| `Dictionary(Int8,_)`  | `DictionaryArray<i8>`     |
| `Dictionary(Int16,_)` | `DictionaryArray<i16>`    |
| `Dictionary(Int32,_)` | `DictionaryArray<i32>`    |
| `Dictionary(Int64,_)` | `DictionaryArray<i64>`    |

In this context, a common pattern to write operators that receive `&dyn Array` is:

```rust
use arrow2::datatypes::DataType;
use arrow2::array::{Array, PrimitiveArray};

fn float_operator(array: &dyn Array) -> Result<Box<dyn Array>, String> {
    match array.data_type() {
        DataType::Float32 => {
            let array = array.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
            // let array = f32-specific operator
            let array = array.clone();
            Ok(Box::new(array))
        }
        DataType::Float64 => {
            let array = array.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
            // let array = f64-specific operator
            let array = array.clone();
            Ok(Box::new(array))
        }
        _ => Err("This operator is only valid for float point.".to_string()),
    }
}
```

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

If not, then use the builder API, such as `Primitive<T>`, `Utf8Primitive<O>`, `ListPrimitive`, etc.

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
and cloned its validity. This approach is suitable for operations whose branching off is more expensive than operating over all values. If the operation is expensive, then using `Primitive::<O>::from_trusted_len_iter` is likely faster.
