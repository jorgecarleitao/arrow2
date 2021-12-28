Welcome to arrow2's documentation. Thanks for checking it out!

This is a library for efficient in-memory data operations with
[Arrow in-memory format](https://arrow.apache.org/docs/format/Columnar.html).
It is a re-write from the bottom up of the official `arrow` crate with soundness
and type safety in mind.

Check out [the guide](https://jorgecarleitao.github.io/arrow2/) for an introduction.
Below is an example of some of the things you can do with it:

```rust
use std::sync::Arc;

use arrow2::array::*;
use arrow2::datatypes::{Field, DataType, Schema};
use arrow2::compute::arithmetics;
use arrow2::error::Result;
use arrow2::io::parquet::write::*;
use arrow2::chunk::Chunk;

fn main() -> Result<()> {
    // declare arrays
    let a = Int32Array::from(&[Some(1), None, Some(3)]);
    let b = Int32Array::from(&[Some(2), None, Some(6)]);

    // compute (probably the fastest implementation of a nullable op you can find out there)
    let c = arithmetics::basic::mul_scalar(&a, &2);
    assert_eq!(c, b);

    // declare a schema with fields
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]);

    // declare chunk
    let chunk = Chunk::new(vec![
        Arc::new(a) as Arc<dyn Array>,
        Arc::new(b) as Arc<dyn Array>,
    ]);

    // write to parquet (probably the fastest implementation of writing to parquet out there)

    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Snappy,
        version: Version::V1,
    };

    let row_groups = RowGroupIterator::try_new(
        vec![Ok(chunk)].into_iter(),
        &schema,
        options,
        vec![Encoding::Plain, Encoding::Plain],
    )?;

    // anything implementing `std::io::Write` works
    let mut file = vec![];

    let parquet_schema = row_groups.parquet_schema().clone();
    let _ = write_file(
        &mut file,
        row_groups,
        &schema,
        parquet_schema,
        options,
        None,
    )?;

    Ok(())
}
```

## Cargo features

This crate has a significant number of cargo features to reduce compilation
time and number of dependencies. The feature `"full"` activates most
functionality, such as:

* `io_ipc`: to interact with the Arrow IPC format
* `io_ipc_compression`: to read and write compressed Arrow IPC (v2)
* `io_csv` to read and write CSV
* `io_json` to read and write JSON
* `io_flight` to read and write to Arrow's Flight protocol
* `io_parquet` to read and write parquet
* `io_parquet_compression` to read and write compressed parquet
* `io_print` to write batches to formatted ASCII tables
* `compute` to operate on arrays (addition, sum, sort, etc.)

The feature `simd` (not part of `full`) produces more explicit SIMD instructions
via [`packed_simd`](https://github.com/rust-lang/packed_simd), but requires the 
nightly channel.
