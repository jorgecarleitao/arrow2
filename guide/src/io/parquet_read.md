# Read parquet

When compiled with feature `io_parquet`, this crate can be used to read parquet files
to arrow.
It makes minimal assumptions on how you to decompose CPU and IO intensive tasks.

First, some notation:

* `page`: part of a column (e.g. similar of a slice of an `Array`)
* `column chunk`: composed of multiple pages (similar of an `Array`)
* `row group`: a group of columns with the same length (similar of a `RecordBatch` in Arrow)

Here is how to read a single column chunk from a single row group:

```rust
{{#include ../../../examples/parquet_read.rs}}
```

The example above minimizes memory usage at the expense of mixing IO and CPU tasks
on the same thread, which may hurt performance if one of them is a bottleneck.

### Parallelism decoupling of CPU from IO

One important aspect of the pages created by the iterator above is that they can cross
thread boundaries. Consequently, the thread reading pages from a file (IO-bounded)
does not have to be the same thread performing CPU-bounded work (decompressing,
decoding, etc.).

The example below assumes that CPU starves the consumption of pages,
and that it is advantageous to have a single thread performing all IO-intensive work,
by delegating all CPU-intensive tasks to separate threads.

```rust
{{#include ../../../examples/parquet_read_parallel.rs}}
```

This can of course be reversed; in configurations where IO is bounded (e.g. when a
network is involved), we can use multiple producers of pages, potentially divided
in file readers, and a single consumer that performs all CPU-intensive work.
