# Write to Parquet

When compiled with feature `io_parquet`, this crate can be used to write parquet files
from arrow.
It makes minimal assumptions on how you to decompose CPU and IO intensive tasks.

First, some notation:

* `page`: part of a column (e.g. similar of a slice of an `Array`)
* `column chunk`: composed of multiple pages (similar of an `Array`)
* `row group`: a group of columns with the same length (similar of a `RecordBatch` in Arrow)

Here is how to write a single column chunk into a single row group:

```rust
{{#include ../../../examples/parquet_write.rs}}
```
