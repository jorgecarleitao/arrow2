# Write to Parquet

When compiled with feature `io_parquet`, this crate can be used to write parquet files
from arrow.
It makes minimal assumptions on how you to decompose CPU and IO intensive tasks, as well
as an higher-level API to abstract away some of this work into an easy to use API.

First, some notation:

* `page`: part of a column (e.g. similar of a slice of an `Array`)
* `column chunk`: composed of multiple pages (similar of an `Array`)
* `row group`: a group of columns with the same length (similar of a `RecordBatch` in Arrow)

Here is an example of how to write a single column chunk into a single row group:

```rust
{{#include ../../../examples/parquet_write.rs}}
```

For single-threaded writing, this crate offers an API that encapsulates the above logic. It 
assumes that a `RecordBatch` is mapped to a single row group with a single page per column.

```rust
{{#include ../../../examples/parquet_write_record.rs}}
```
