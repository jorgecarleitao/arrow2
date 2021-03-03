# Arrow2

Arrow2 is a `safe` Rust library that implements data structures and functionality enabling
interoperability with the arrow format.

The typical use-case for this library is to perform CPU and memory-intensive analytics in a format that supports heterogeneous data strutures, null values, and IPC and FFI interfaces across languages.

Arrow2 is divided in two main parts: a [low-end API](./low_end.md) to efficiently
operate with contiguous memory regions, and a [high-end API](./high_end.md) to operate with
arrow arrays, logical types, schemas, etc.

Finally, this crate has an anxiliary section devoted to IO (CSV, JSON, parquet).
