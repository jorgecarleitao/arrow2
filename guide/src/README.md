# Arrow2

Arrow2 is a Rust library that implements data structures and functionality enabling
interoperability with the arrow format.

The typical use-case for this library is to perform CPU and memory-intensive analytics in a format that supports heterogeneous data structures, null values, and IPC and FFI interfaces across languages.

Arrow2 is divided into three main parts: 

* a [low-level API](./low_level.md) to efficiently operate with contiguous memory regions;
* a [high-level API](./high_level.md) to operate with arrow arrays;
* a [metadata API](./metadata.md) to declare and operate with logical types and metadata.

## Cargo features

This crate has a significant number of cargo features to reduce compilation times and dependencies blowup.
There is also a feature `simd`, that requires the nightly channel, that produces more explicit SIMD instructions via [`packed_simd`](https://github.com/rust-lang/packed_simd).
