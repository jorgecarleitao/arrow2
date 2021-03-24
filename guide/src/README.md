# Arrow2

Arrow2 is a Rust library that implements data structures and functionality enabling
interoperability with the arrow format.

The typical use-case for this library is to perform CPU and memory-intensive analytics in a format that supports heterogeneous data structures, null values, and IPC and FFI interfaces across languages.

Arrow2 is divided into two main parts: a [low-end API](./low_end.md) to efficiently
operate with contiguous memory regions, and a [high-end API](./high_end.md) to operate with
arrow arrays, logical types, schemas, etc.

This repo started as an experiment forked from the Apache arrow project to offer a transmute-free
Rust implementation of that crate. It currently offers most functionality with the notable exception of reading and writing to and from parquet.
