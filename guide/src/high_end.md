# High end API

The simplest way to think about an arrow `Array` is that it represents 
`Vec<Option<T>>` and has a logical type associated with it.

Probably the simplest array in this crate is `PrimitiveArray<T>`. It can be constructed
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
2. A logical type (`DataType::Int32`)
3. Data

Its main difference vs a `Vec<Option<T>>` are:

* Its data is layed out in memory as a `Buffer<T>` and a `Option<Bitmap>`.
* `PrimitivArray<T>` has an associated datatype.

The first difference allows interoperability with Arrow's ecosystem and efficient SIMD operations (we will re-visit this below); the second difference is that it allows semantic meaning to the array: in the example

```rust
# use arrow2::array::Primitive;
# use arrow2::datatypes::DataType;
# fn main() {
let ints = Primitive::<i32>::from(&[Some(1), None]).to(DataType::Int32);
let dates = Primitive::<i32>::from(&[Some(1), None]).to(DataType::Date32);
# }
```

`ints` and `dates` have the same in-memory representation but different logic representations (e.g. dates are usually represented as a string).

The following arrays exist:

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
implement the trait `Array` and can be casted to `&dyn Array`, i.e. they can be turned into
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

Note how we have not specified the the inner type explicitely in the signature `ListArray<i32>`.
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

There is a many to one relationship between `DataType` and a physical type (like `i32`). These are typically encapsulated on `match` patterns.

## From Iterator

In the code above, we've introduced how to create an array from an iterator.
These APIs are avaiable for all Arrays, and they are highly suitable to efficiently
create them. In this section we will go a bit more in detail about these operations,
and how to make them even more efficient.

This crate's APIs are generally split in two parts: whether an operation leverages contiguous memory regions or whether it does not.

If yes, then use:

* `Buffer::from_iter`
* `unsafe Buffer::from_trusted_len_iter`
* `unsafe Buffer::try_from_trusted_len_iter`

If not, then use the builder API, such as `Primitive<T>`, `Utf8Primitive<O>`, `ListPrimitive`, etc.

We have seen examples where the latter API was used. In the last example of this page you will be introduced to an example of using the former for SIMD.

## Into Iterator

We've already seen how to create an array from an iterator. Most arrays also implement
`IntoIterator`:

```rust
# use arrow2::array::{Array, PrimitiveArray, Primitive};
# use arrow2::datatypes::DataType;
# fn main() {
let array: PrimitiveArray<i32> = [Some(1), None, Some(123)]
    .iter()
    .collect::<Primitive<i32>>()
    .to(DataType::Int32);

for item in array.iter() {
    if let Some(value) = item {
        println!("{}", value);
    } else {
        println!("NULL");
    }
}
# }
```

Like `FromIterator`, this crate contains two sets of APIs to iterate over data. Let's say
that you have an array `array: &PrimitiveArray<T>`. The following applies:

1. If you need to iterate over `Option<T>`, use `array.iter()`
2. If you can operate over the values and validity independently, use `array.values() -> &[T]` and `array.validity() -> &Option<Bitmap>`

Note that case 1 is useful when e.g. you want to perform an operation that depends on both validity and values, while the latter is suitable for SIMD and copies, as they return contiguous memory regions (slices and bitmaps). We will see below how to leverage these APIs.

This idea holds more generally in this crate: `values()` always returns something that has a contiguous in-memory representation, while `iter()` returns items taking validity into account. To get an iterator over contiguous values, use `array.values().iter()`.

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
    //  Soundness
    //      `values` is an iterator with a known size because arrays are sized.
    let values = unsafe { Buffer::from_trusted_len_iter(values) };

    PrimitiveArray::<O>::from_data(data_type.clone(), values, array.validity().clone())
}
```

Some notes:

1. We used `array.values()`, as described above: this operation leverages a contiguous memory region.

2. We leveraged normal rust iterators for the operation.

1. We have used `from_trusted_len_iter`, which assumes that the iterator is [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html). This (instead of `.collect`) is necessary because trait specialization is currently unstable.

2. We used `op` on the array's values irrespectively of their validity,
and cloned its validity. This approach is suitable for operations whose branching off is more expensive than operating over all values. If the operation is expensive, then using `Primitive::<O>::from_trusted_len_iter` is likely faster.
