# High end API

The simplest way to think about an arrow `Array` is that it is a `Vec<Option<T>>`
with a logical type associated with it.

Probably the simplest array is `PrimitiveArray<T>`. It can be constructed
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

The first difference allows interoperability with Arrow's ecosystem and efficient operations (we will re-visit this in a second);
the second difference allows semantic meaning to the array: in the example

```rust
# use arrow2::array::Primitive;
# use arrow2::datatypes::DataType;
# fn main() {
let ints = Primitive::<i32>::from(&[Some(1), None]).to(DataType::Int32);
let dates = Primitive::<i32>::from(&[Some(1), None]).to(DataType::Date32);
# }
```

`ints` and `dates` have the same in-memory representation but different logic representations,
(e.g. dates are usually represented as a string).

## Dynamic Array

There is a more powerful aspect of arrow arrays, and that is that they all
implement the trait `Array` and can be casted to `&dyn Array`, i.e. they can be turned into
a trait object. This in particular enables arrays to have types that are dynamic in nature.
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

Note how we have not specified the the inner type explicitely the types of `ListArray<i32>`.
Instead, `ListArray` does have an inner `Array` representing all its values.

## Downcast and `as_any`

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

## Iterators

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

## Vectorized operations

One of the main advantages of the arrow format and its memory layout is that 
it often enables SIMD. For example, an unary operation `op` on a `PrimitiveArray` is likely auto-vectorized on the following code:

```rust
# use arrow2::buffer::Buffer;
# use arrow2::{
#     array::{Array, PrimitiveArray},
#     buffer::NativeType,
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

    PrimitiveArray::<O>::from_data(data_type.clone(), values, array.nulls().clone())
}
```

Two notes:

1. We have used `from_trusted_len_iter`. This is because [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html) and trait specialization are still unstable.

2. Note how we use `op` on the array's values irrespectively of their validity
and cloned the validity `Bitmap`. This approach is suitable for operations whose branching off is more expensive than operating over all values. If the operation is expensive, then using `Primitive::<O>::from_trusted_len_iter` is likely faster.
