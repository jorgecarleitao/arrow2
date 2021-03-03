# Arrow2: Safe Arrow

This repository contains a Rust Library to work with the Arrow format within Rust.

It is a re-write of the original Arrow crate using `safe` Rust. See FAQ for details.

## How to use

See details in the book.

## Run unit tests

```bash
git clone git@github.com:jorgecarleitao/arrow2.git
cd arrow2
cargo test
```

The test suite is a _superset_ of all integration tests that the original implementation has against golden files from the arrow project. It currently makes no attempt to test the implementation against other implementations in arrow's master; it assumes that arrow's golden files are sufficient to cover the specification. This crate uses both little and big endian golden files, as it supports both endianesses at IPC boundaries.

## Features in this crate and not in the original

* Uses Rust's compiler whenever possible to prove that memory reads are sound
* IPC supports big endian
* More predictable JSON reader
* Generalized parsing of CSV based on logical data types
* conditional compilation based on cargo `features` to reduce dependencies and size
* single repository dedicated to Rust
* faster IPC reader (different design that avoids an extra copy of all data)

## Features in the original not availabe in this crate

* Parquet IO
* some compute kernels
* SIMD (no plans to support: favor auto-vectorization instead)

## Roadmap

1. CI/CD
2. parquet IO
3. bring documentation up to speed
4. compute kernels
5. auto-vectorization of bitmap operations

## How to develop

This is a normal Rust project. Clone and run tests with `cargo test`.

## FAQ

### Why?

The arrow crate uses `Buffer`, a generic struct to store contiguous memory regions (of bytes). This construct is used to store data from all arrays in the rust implementation. The simplest example is a buffer containing `1i32`, that is represented as `&[0u8, 0u8, 0u8, 1u8]` or `&[1u8, 0u8, 0u8, 0u8]` depending on endianness.

When a user wishes to read from a buffer, e.g. to perform a mathematical operation with its values, it needs to interpret the buffer in the target type. Because `Buffer` is a contiguous regions of bytes with no information about its underlying type, users must transmute its data into the respective type.

Arrow currently transmutes buffers on almost all operations, and very often does not verify that there is type alignment nor correct length when we transmute it to a slice of type `&[T]`.

Just as an example, in v3.0.0, the following code compiles, does not panic, is unsound and results in UB:

```rust
let buffer = Buffer::from_slic_ref(&[0i32, 2i32])
let data = ArrayData::new(DataType::Int64, 10, 0, None, 0, vec![buffer], vec![]);
let array = Float64Array::from(Arc::new(data));

println!("{:?}", array.value(1));
```

Note how this initializes a buffer with bytes from `i32`, initializes an `ArrayData` with dynamic type
`Int64`, and then an array `Float64Array` from `Arc<ArrayData>`. `Float64Array`'s internals will essentially consume the pointer from the buffer, re-interpret it as `f64`, and offset it by `1`.

Still within this example, if we were to use `ArrayData`'s datatype, `Int64`, to transmute the buffer, we would be creating `&[i64]` out of a buffer created out of `i32`.

Any Rust developer acknowledges that this behavior goes very much against Rust's core premise that a functions' behvavior must not be undefined depending on whether the arguments are correct. The obvious observation is that transmute is one of the most `unsafe` Rust operations and not allowing the compiler to verify the necessary invariants is a large burden for users and developers to take.

This simple example indicates a broader problem with the current design, that we now explore in detail.

#### Root cause analysis

At its core, Arrow's current design is centered around two main `structs`:

1. untyped `Buffer`
2. untyped `ArrayData`

##### 1. untyped `Buffer`

The crate's buffers are untyped, which implies that once created, the type
information is lost. Consequently, the compiler has no way
of verifying that a certain read can be performed. As such, any read from it requires an alignment and size check at runtime. This is not only detrimental to performance, but also very cumbersome.

For the past 4 months, I have identified and fixed more than 10 instances of unsound code derived from the misuse, within the crate itself, of `Buffer`. This hints that downstream dependencies using this crate and use this API are likely do be even more affected by this.

##### 2. untyped `ArrayData`

`ArrayData` is a `struct` containing buffers and child data that does not differentiate which type of array it represents at compile time.

Consequently, all buffer reads from `ArrayData`'s buffers are effectively `unsafe`, as they require certain invariants to hold. These invariants are strictly related to `ArrayData::datatype`: this `enum` differentiates how to transmute the `ArrayData::buffers`. For example, an `ArrayData::datatype` equal to `DataType::UInt32` implies that the buffer should be transmuted to `u32`.

The challenge with the above struct is that it is not possible to prove that `ArrayData`'s creation
is sound at compile time. As the sample above showed, there was nothing wrong, during compilation, with passing a buffer with `i32` to an `ArrayData` expecting `i64`. We could of course check it at runtime, and we should, but we are defeating the whole purpose of using a typed system as powerful as Rust offers.

The main consequence of this observation is that the current code has a significant maintenance cost, as we have to be rigorously check the types of the buffers we are working with. The example above shows
that, even with that rigour, we fail to identify obvious problems at runtime.

Overall, there are many instances of our code where we expose public APIs marked as `safe` that are `unsafe` and lead to undefined behavior if used incorrectly. This goes against the core goals of the Rust language,  and significantly weakens Arrow Rust's implementation core premise that the compiler and borrow checker proves many of the memory safety concerns that we may have.

Equally important, the inability of the compiler to prove certain invariants is detrimental to performance. As an example, the implementation of the `take` kernel in this repo is semantically equivalent to the current master, but 1.3-2x faster.

### How?

Contrarily to the original implementation, this implementation does not transmutate byte buffers based on runtime types, and instead requires all buffers to be typed (in Rust's sense of a generic).

This removes many existing bugs and enables the compiler to prove all type invariants with the only exception of FFI and IPC boundaries.

This crate also has a different design towards arrays' `offsets` that removes many out of bound reads consequent of using byte and slot offsets interchangibly.

This crate's design of primitive types is also more explicit about its logical and physical representation, enabling support for `Timestamps` with timezones and a safe implementation of the `Interval` type.

Consequently, this crate is easier to use, develop, maintain, and debug.

#### Any plans to merge with the Apache Arrow project?

Yes. The primary reason to have this repo and crate is to be able to propotype and mature using a fundamentally different design based on a transmute-free implementation. This requires breaking backward compatibility and loss of features that is impossible to achieve on the Arrow project.

Furthermore, the arrow project currently has a release mechanism that is unsuitable for this type of work:

* The Apache Arrow project has a single git repository with all 10+ implementations, ranging from C++, Python, C#, Julia, Rust, and execution engines such as Grandiva and DataFusion. A git ref corresponds to all of them, and a commit is about any/all of them.

The implication is that the repository has a CI/CD setup and git history that is difficult to follow. It needs rigorous discipline to understand which commit belongs to which implementation. It also implies that tags of software releases are difficult to manage, as they correspond to all implementations at once (see also next point).

For example, this work would require a proibitive number of Jira issues for each PR to the crate, as well as an inhumane number of PRs, reviews, etc.

Another example is that it is impossible to release a different design of the arrow crate without breaking every dependency within the project which makes it difficult to iterate.

* A release of the Apache consists of a release of all implementations of the arrow format at once, with the same version. It is currently at `3.0.0`.

This implies that the crate version is independent of the changelog or its API stability, which violates SemVer. This procedure makes the crate incompatible with Rusts' (and many others') ecosystem that heavily relies on SemVer to constraint software versions.

Secondly, this implies the arrow crate is versioned as `>0.x`. This places expectations about API stability that are incompatible with this effort.

* Apache Arrow releases a major version every X months

This implies that, irrespectively of how stable or not the API is, or at which pace it was developed during X months, the crate will get a major version bump. This again makes it difficult to depend on the crate.

This also creates a large incentive to break backward compatibility on every release.
