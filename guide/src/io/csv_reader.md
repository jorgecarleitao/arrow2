# CSV reader

When compiled with feature `io_csv`, you can use this crate to read CSV files.
This crate makes minimal assumptions on how you want to read a CSV, and offers a large degree of customization to it, along with a useful default.

## Background

There are two CPU-intensive tasks in reading a CSV file:
* split the CSV file into rows, which includes parsing quotes and delimiters, and is necessary to `seek` to a given row.
* parse a set of CSV rows (bytes) into a `RecordBatch`.

Parsing bytes into values is more expensive than interpreting lines. As such, it is generally advantageous to have multiple readers of a single file that scan different parts of the file (within IO constraints).

This crate relies on [the crate `csv`](https://crates.io/crates/csv) to scan and seek CSV files, and your code also needs such a dependency. With that said, `arrow2` makes no assumptions as to how to efficiently read the CSV: as a single reader per file or multiple readers.

As an example, the following infers the schema and reads a CSV by re-using the same reader:

```rust
use csv::ReaderBuilder;
use arrow2::io::csv::{infer_schema, read_batch, infer, DefaultParser};
use arrow2::error::Result;

fn read_path(path: &str) -> Result<()> {
    let mut reader = ReaderBuilder::new().from_path(path)?;
    let parser = DefaultParser::default();

    let schema = infer_schema(&mut reader, None, true, &infer)?;

    // 0: start from
    // 100: up to (max batch size)
    let batch = read_batch(&mut reader, &parser, 0, 100, schema, None)?;
}
```

## Orchestration and parallelization

Because `csv`'s API is synchronous, the functions above represent the "minimal unit of synchronous work". It is up to you to decide how you want to read the file efficiently:
whether to read batches in sequence or in parallel, or whether IO supports multiple readers per file.

## Customization

In the code above, `parser` and `infer` allow for customization: they declare
how rows of bytes should be inferred (into a logical type), and processed (into a value of said type). They offer good default options, but you can customize the inference and parsing to your own needs. You can also of course decide to parse everything into memory as `Utf8Array` and delay
any data transformation.
