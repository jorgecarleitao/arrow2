Welcome to arrow2's documentation. Thanks for checking it out!

This is a library for efficient in-memory data operations using
[Arrow in-memory format](https://arrow.apache.org/docs/format/Columnar.html).
It is a re-write from the bottom up of the official `arrow` crate with soundness
and type safety in mind.

Check out [the guide](https://jorgecarleitao.github.io/arrow2/) for an introduction.
Below is an example of some of the things you can do with it:

```rust
use std::sync::Arc;

use arrow2::array::*;
use arrow2::compute::arithmetics;
use arrow2::error::Result;
use arrow2::io::parquet::write::*;
use arrow2::record_batch::RecordBatch;

fn main() -> Result<()> {
    // declare arrays
    let a = Int32Array::from(&[Some(1), None, Some(3)]);
    let b = Int32Array::from(&[Some(2), None, Some(6)]);

    // compute (probably the fastest implementation of a nullable op you can find out there)
    let c = arithmetics::basic::mul_scalar(&a, &2);
    assert_eq!(c, b);

    // declare records
    let batch = RecordBatch::try_from_iter([
        ("c1", Arc::new(a) as Arc<dyn Array>),
        ("c2", Arc::new(b) as Arc<dyn Array>),
    ])?;
    // with metadata
    println!("{:?}", batch.schema());

    // write to parquet (probably the fastest implementation of writing to parquet out there)
    let schema = batch.schema().clone();

    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Snappy,
        version: Version::V1,
    };

    let row_groups = RowGroupIterator::try_new(
        vec![Ok(batch)].into_iter(),
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
