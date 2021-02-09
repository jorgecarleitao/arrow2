# Proposal: Safety by design in the Arrow crate

This document and repository proposes a major redesign of the arrow crate to correctly handle
memory safety, offsets and type safety.

TL;DR: this repo reproduces the main parts of the arrow crate with the proposed design in this repo. What it demonstrates:

1. allocations along cache lines, buffers and memory manangement
2. import and export using the FFI / C data interface
3. implementation of nested types (Dict, List, Struct)
4. `dyn Array` and dynamic typing
5. array equality
6. one kernel (`take`) for primitives (1.3x faster than current master).

Not demonstrated (but deemd feasible with the proposed design):

1. SIMD
2. IO (CSV / JSON)
3. `transform/` module (that would need to be migrated)

## Background

The arrow crate uses `Buffer`, a generic struct to store contiguous memory regions (of bytes). This construct is used to store data from all arrays in the rust implementation. The simplest example is a buffer containing `1i32`, that is represented as `&[0u8, 0u8, 0u8, 1u8]` or `&[1u8, 0u8, 0u8, 0u8]` depending on endianness.

When a user wishes to read from a buffer, e.g. to perform a mathematical operation with its values, it needs to interpret the buffer in the target type. Because `Buffer` is a contiguous regions of bytes with no information about its underlying type, users must transmute its data into the respective type.

Arrow currently transmutes buffers on almost all operations, and very often does not verify that there is type alignment nor correct length when we transmute it to a slice of type `&[T]`.

Just as an example, the following code compiles, does not panic, and is unsound and results in UB:s

```rust
let buffer = Buffer::from(&[0i32, 2i32])
let data = ArrayData::new(DataType::Int64, 10, 0, None, 0, vec![buffer], vec![]);
let array = Float64Array::from(Arc::new(data));

println!("{:?}", array.value(1));
```

Note how this initializes a buffer with bytes from `i32`, initializes an `ArrayData` with dynamic type
`Int64`, and then an array `Float64Array` from `Arc<ArrayData>`. `Float64Array`'s internals will essentially consume the pointer from the buffer, re-interpret it as `f64`, and offset it by `1`.

Still within this example, if we were to use `ArrayData`'s datatype, `Int64`, to transmute the buffer, we would be creating `&[i64]` out of a buffer created out of `i32`.

Any Rust developer acknowledges that this behavior goes very much against Rust's core premise that a functions' behvavior must not be undefined depending on whether the arguments are correct. The obvious observation is that transmute is one of the most `unsafe` Rust operations and not allowing the compiler to verify the necessary invariants is a large burden for users and developers to take.

This simple example indicates a broader problem with the current design, that we now explore in detail.

### Root cause analysis

At its core, Arrow's current design is centered around two main `structs`:

1. untyped `Buffer`
2. untyped `ArrayData`

#### 1. untyped `Buffer`

The crate's `Buffer` is untyped, which implies that once created, the type
information used to create it is lost. Consequently, the compiler has no way of verifying that a certain read can be performed. Thus, any read from it requires an alignment and size check at runtime. This is not only detrimental to performance, but also cumbersome.

Over the past 4 months, I have identified and fixed more than 10 instances of unsound code derived from the misuse, within the crate itself, of `Buffer`. This hints that there may be a design problem.

#### 2. untyped `ArrayData`

`ArrayData` is a `struct` containing buffers and child data that does not differentiate which type of array it represents at compile time.

Consequently, all buffer reads from `ArrayData`'s buffers are effectively `unsafe`, as they require certain invariants to hold. These invariants are strictly related to `ArrayData::datatype`: this `enum` differentiates how to transmute the `ArrayData::buffers`. For example, an `ArrayData::datatype` equal to `DataType::UInt32` implies that the buffer should be interpreted / transmuted as `u32`.

The challenge with the above struct is that it is not possible to prove that `ArrayData`'s creation and reads
are sound at compile time. As the sample above shows, there is nothing wrong, during compilation, with passing a buffer with `i32` to an `PrimitiveArray` expecting `i64` (via `ArrayData`). We could of course check it at runtime, and we should, but we are defeating the whole purpose of using a typed system as powerful as Rust offers.

The main consequence of this observation is that the current code has a significant maintenance cost, as we have to be rigorously check the types of the buffers we are working with. The example above shows
that, even with that rigour, we fail to identify obvious problems at runtime.

Overall, there are many instances of our code where we expose public APIs marked as `safe` that are `unsafe` and lead to undefined behavior if used incorrectly. This goes against the core goals of the Rust language, and significantly weakens Arrow Rust's implementation core premise that the compiler and borrow checker proves many of the memory safety concerns that we may have.

Equally important, the inability of the compiler to prove certain invariants is detrimental to performance. As an example, the implementation of the `take` kernel in this repo is semantically equivalent to the current master, but 1.3x faster.

## Proposal

The proposal is to redesign the Arrow crate to address the design limitation described above.
This has a major impact into the whole ecosystem that relies on `Buffer`, `MutableBuffer`, `bytes`,
and has limited impact on high-end `Array` API that rely on iterators and other higher abstractions.

Broadly speaking, this proposes the following changes:

1. Replace `Buffer` by `Buffer<T>`
2. Replace `MutableBuffer` by `MutableBuffer<T>`
3. Replace `Bytes` by `Bytes<T>`
4. Remove `RawPointer`
5. Remove `ArrayData` and place its contents directly on the corresponding arrays.
6. make childs be `Arc<dyn Array>`
7. Remove `Array::data` and `Array::data_ref`
8. redesign `bitmap` to hold offsets
9. replace `Array::slice` by concrete implementations
10. Make `PrimitiveArray<NativeType>` instead of `PrimitiveType`

### 1-4. Replace `Buffer` by `Buffer<T>`

This is one of the core changes and is a major design change: `Buffer`s must be typed. There will be
an `unsafe` trait, `NativeType`, implemented for `u8, u16, u32, u64, i8, i16, i32, i64, f32, f64` corresponding to the only types that can be represented in a buffer.

Create a generic `Buffer<T: NativeType>`, `Bytes<T: NativeType>`, `MutableBuffer<T: NativeType>`, that corresponds to a byte-aligned, cache line-aligned continugous memory regions.

This allow us to only have to deal with `transmute` at ffi boundaries. Effectively, it allow us to not
have to rely on the highly `unsafe` `RawPointer` on array implementations, as well as `as_typed` function that transmutes buffers.

[Here](src/buffer/imutable.rs) you can find the concrete implementation proposed in this repo.

### 5. Remove `ArrayData` and place its contents directly on the corresponding arrays.

For example, for primitive types, such as `Float64` and `Date32`, declare a `PrimitiveArrayData<T>` as follows:

```rust
#[derive(Debug, Clone)]
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Bitmap>,
    offset: usize,
}
```

Note how `T` denotes the _physical_ representation, while `data_type` corresponds to the _logical_ representation. This is so that `Timestamp` with timezones becomes a first class citizen (it currently isn't).

### 6. Child data is stored as `Arc<dyn Array>`

For example, the struct holding a `ListArray` is [defined](src/array/list.rs) as

```rust
#[derive(Debug, Clone)]
pub struct ListArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Arc<dyn Array>,
    validity: Option<Bitmap>,
    offset: usize,
}
```

This greatly simplifies creating nested structures, as there is no longer any `ArrayData`.

Accessing individual (nested) values of this array, e.g. for iterations, work as before:

```rust
impl<O: Offset> ListArray<O> {
    pub fn value(&self, i: usize) -> Box<dyn Array> {
        let offsets = self.offsets.as_slice();
        let offset = offsets[i];
        let offset_1 = offsets[i + 1];
        let length = (offset_1 - offset).to_usize().unwrap();

        self.values.slice(offset.to_usize().unwrap(), length)
    }
}
```

Note the usage of `Array::slice`, an abstract method that each specific implementation must know how to perform. This method has been problematic in the past because its implementation is type-specific, but
the current implementation is type-agnotisc (i.e. a bug).

In the case of a list array:

```rust
impl<O: Offset> ListArray<O> {
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.as_ref().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            offsets: self.offsets.slice(offset, length),
            values: self.values.clone(),
            validity,
            offset,
        }
    }
}

impl<O: Offset> Array for ListArray<O> {
    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}
```

Note how the `offsets` were sliced, but the `values` not. In the current master, both get sliced, which
is semantically incorrect.

Also note that the choice of `Arc` over `Box` is solely for the purposes of enabling a cheap `Clone`.

### 7. Remove `Array::data` and `Array::data_ref`

Without `ArrayData`, these methods are no longer required. Required traits to enable FFI are instead 
provided. This repo supports FFI (import and export), which demonstrates that `ArrayData` is not needed.

### 8. Redesign bitmap

This implementation redesigns `Bitmap` to allow it to hold `Bytes<u8>` and an offset in `bits`.
`Bitmap` is the only struct that holds bitmaps, and has methods to efficiently `get` bits.
Because it has an offset in bits, it contains all information required to correctly offset itself.

This way, users no longer have to use `MutableBuffer<u8>` to handle `bitmaps`, use `unsafe` `get_bit_raw`,
offseting in bits vs bytes, etc.

### 9. replace `Array::slice` by concrete implementations

Slice is an operation whose implementation depends on the particular logical type being implemented.
This proposes that we move `slice` to be an type-specific implementation.

### 10. Make `PrimitiveArray<T: NativeType>` instead of `PrimitiveType`

Currently, `PrimitiveArray` depends on a `ArrowPrimitiveType`, which has an associated `DataType`.
This makes it difficult to distinguish the physical representation from its logical one. I.e. `Int64Type` is both
a physical (`i64`) and logical type (`DataType::Int64`). There are logical types whose physical representation
is the same (e.g. `Timestamp(_, _)`). Hard-coding the logical representation in the type takes away this fundamental
separation.

This proposal separates the two aspects: the generic argumemnt, `T`, is used to declare the physical layout, which, within Rust, is used for type-safety.
The `DataType` is used for a logical representation which, in the context of Rust, is used for dynamic typing, i.e. it enables the trait Object `Array` to implement `as_any()` and use `Array::data_type()` to decide to which concrete 
implementation `&dyn Array` should be `downcast_ref`ed to.

With this design, an incorrect `DataType` only causes `downcast_ref` to fail and cannot cause undefined behavior. The only possible undefined behavior in this new design is at FFI boundaries: a byte buffer that is incorrect for a `DataType` causes the library to interpret bytes of type `x` as type `y`, which is undefined behavior.
