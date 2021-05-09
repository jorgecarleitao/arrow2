# Arrow2: Transmute-free Arrow

![test](https://github.com/jorgecarleitao/arrow2/actions/workflows/test.yml/badge.svg)

This repository contains a Rust library to work with the [Arrow format](https://arrow.apache.org/).
It is a re-write of the [official Arrow crate](https://github.com/apache/arrow) using transmute-free operations. See FAQ for details.

See [the guide](https://jorgecarleitao.github.io/arrow2/).

## Design

This repo and crate's primary goal is to offer a safe Rust implementation to interoperate with the Arrow. As such, it

* MUST NOT implement any logical type other than the ones defined on the arrow specification, [schema.fbs](https://github.com/apache/arrow/blob/master/format/Schema.fbs).
* MUST lay out memory according to the [arrow specification](https://arrow.apache.org/docs/format/Columnar.html)
* MUST support reading from and writing to the [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html) at zero-copy.
* MUST support reading from, and writing to, the [IPC specification](https://arrow.apache.org/docs/python/ipc.html), which it MUST verify against golden files available [here](https://github.com/apache/arrow-testing).

Design documents about each of the parts of this repo are available on their respective READMEs.

## Tests

The test suite is a _superset_ of all tests that the original implementation has against golden files from the arrow project. It includes both little and big endian files.

Furthermore, the CI runs all integration tests against [apache/arrow@master](https://github.com/apache/arrow), demonstrating full interoperability with other implementations.

Integration tests against parquet files created by pyarrow require generating parquet files. These tests run by default.
To not run them, pass `ARROW2_IGNORE_PARQUET` to the tests (the tests will be marked as OK/PASS).

```bash
git clone git@github.com:jorgecarleitao/arrow2.git
cd arrow2
ARROW2_IGNORE_PARQUET= cargo test
```

To generate the necessary parquet files, run

```bash
python3 -m venv venv
venv/bin/pip install pyarrow==3
venv/bin/python parquet_integration/write_parquet.py
```

## Features in this crate and not in the original

* Uses Rust's compiler whenever possible to prove that memory reads are sound
* Reading parquet is 10-20x faster (single core) and deserialization is parallelizable
* Writing parquet is 3-10x faster (single core) and serialization is parallelizable
* MIRI checks on non-IO components (MIRI and file systems are a bit funny atm)
* parquet IO has no `unsafe`
* IPC supports big endian
* More predictable JSON reader
* Generalized parsing of CSV based on logical data types
* conditional compilation based on cargo `features` to reduce dependencies and size
* faster IPC reader (different design that avoids an extra copy of all data)

## Features in the original not available in this crate

* Parquet read of nested types, arrow schema from the metadata, etc.
* Parquet write V2, nested types, arrow schema to the metadata, etc.

## Roadmap

1. parquet read of nested types, arrow schema from the metadata, etc.
2. parquet nested types, arrow schema to the metadata, etc.
3. bring documentation up to speed

## How to develop

This is a normal Rust project. Clone and run tests with `cargo test`.

### Tips for coverage reporting

On a Linux machine, with VS-code:

* install the extension `coverage gutters`
* install `tarpaulin`
* run the command `Coverage Gutters: Watch` and run

```bash
cargo tarpaulin --target-dir target-tarpaulin --lib --out Lcov
```

This will cause tarpaulin to run all tests under coverage and show the coverage on VS-code.
`--target-dir target-tarpaulin` is used to avoid collisions with rust-analyzer / Cargo, as `tarpaulin`
uses different compilation flags.

### How to improve coverage

An overall goal of this repo is to have high coverage over all its code base. To achieve this, we recommend to run coverage against a single module of this project; e.g.

```bash
cargo tarpaulin --target-dir target-tarpaulin --lib --out Lcov -- buffer::immutable
```

and evaluate coverage of that module alone.

## FAQ

### Why?

The arrow crate uses `Buffer`, a generic struct to store contiguous memory regions (of bytes). This construct is used to store data from all arrays in the rust implementation. The simplest example is a buffer containing `1i32`, that is represented as `&[0u8, 0u8, 0u8, 1u8]` or `&[1u8, 0u8, 0u8, 0u8]` depending on endianness.

When a user wishes to read from a buffer, e.g. to perform a mathematical operation with its values, it needs to interpret the buffer in the target type. Because `Buffer` is a contiguous region of bytes with no type information, users must transmute its data into the respective type.

Arrow currently transmutes buffers on almost all operations, and very often does not verify that there is type alignment nor correct length when we transmute it to a slice of type `&[T]`.

Just as an example, in v3.0.0, the following code compiles, does not panic, is unsound and results in UB:

```rust
let buffer = Buffer::from_slic_ref(&[0i32, 2i32])
let data = ArrayData::new(DataType::Int64, 10, 0, None, 0, vec![buffer], vec![]);
let array = Float64Array::from(Arc::new(data));

println!("{:?}", array.value(1));
```

Note how this initializes a buffer with bytes from `i32`, initializes an `ArrayData` with dynamic type `Int64`, and then an array `Float64Array` from `Arc<ArrayData>`. `Float64Array`'s internals will essentially consume the pointer from the buffer, re-interpret it as `f64`, and offset it by `1`.

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

### Any plans to merge with the Apache Arrow project?

Maybe. The primary reason to have this repo and crate is to be able to propotype and mature using a fundamentally different design based on a transmute-free implementation. This requires breaking backward compatibility and loss of features that is impossible to achieve on the Arrow repo.

Furthermore, the arrow project currently has a release mechanism that is unsuitable for this type of work:

* The Apache Arrow project has a single git repository with all 10+ implementations, ranging from C++, Python, C#, Julia, Rust, and execution engines such as Grandiva and DataFusion. A git ref corresponds to all of them, and a commit is about any/all of them.

The implication is this work would require a proibitive number of Jira issues for each PR to the crate, as well as an inhumane number of PRs, reviews, etc.

Another consequence is that it is impossible to release a different design of the arrow crate without breaking every dependency within the project which makes it difficult to iterate.

* A release of the Apache consists of a release of all implementations of the arrow format at once, with the same version. It is currently at `3.0.0`.

This implies that the crate version is independent of the changelog or its API stability, which violates SemVer. This procedure makes the crate incompatible with Rusts' (and many others') ecosystem that heavily relies on SemVer to constraint software versions.

Secondly, this implies the arrow crate is versioned as `>0.x`. This places expectations about API stability that are incompatible with this effort.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
