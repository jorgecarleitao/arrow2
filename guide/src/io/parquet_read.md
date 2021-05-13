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
use crossbeam_channel::unbounded;

use std::fs::File;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use arrow2::compute::cast::cast;
use arrow2::error::Result;
use arrow2::io::parquet::read;

fn parallel_read(path: &str) -> Result<()> {
    // Prepare a single producer multi consumer channel to communicate.
    let (tx, rx) = unbounded();

    // Open the file, read parquet metadata and arrow metadata from it
    // Place the metadata under an Arc so that it can be cheaply shared across threads
    let mut file = File::open(path)?;
    let file_metadata = Arc::new(read::read_metadata(&mut file)?);
    let arrow_schema = Arc::new(read::get_schema(file_metadata.as_ref())?);

    let start = SystemTime::now();

    // spawn a thread to produce `Vec<CompressedPage>` (IO intensive)
    let producer_metadata = file_metadata.clone();
    let child = thread::spawn(move || {
        for column in 0..producer_metadata.schema().num_columns() {
            for row_group in 0..producer_metadata.row_groups.len() {
                let start = SystemTime::now();
                println!("produce start: {} {}", column, row_group);
                let pages =
                    read::get_page_iterator(&producer_metadata, row_group, column, &mut file)
                        .unwrap()
                        .collect::<Vec<_>>();
                println!(
                    "produce end - {:?}: {} {}",
                    start.elapsed().unwrap(),
                    column,
                    row_group
                );
                tx.send((column, row_group, pages)).unwrap();
            }
        }
    });

    // spawn 3 threads that perform `Vec<CompressedPage> -> Box<dyn Array>` (no IO; CPU intensive)
    let mut children = Vec::new();
    for i in 0..3 {
        let rx_consumer = rx.clone();
        let metadata_consumer = file_metadata.clone();
        let arrow_schema_consumer = arrow_schema.clone();
        let child = thread::spawn(move || {
            let (column, row_group, iter) = rx_consumer.recv().unwrap();
            let start = SystemTime::now();
            println!("consumer start - {} {}", column, row_group);
            let descriptor = metadata_consumer.row_groups[row_group]
                .column(column)
                .column_descriptor();
            let array = read::page_iter_to_array(iter.into_iter(), descriptor).unwrap();
            println!(
                "consumer end - {:?}: {} {}",
                start.elapsed().unwrap(),
                column,
                row_group
            );
            cast(
                array.as_ref(),
                arrow_schema_consumer.field(column).data_type(),
            )
        });
        children.push(child);
    }

    // ensure the producer finishes
    child.join().expect("child thread panicked");

    // collect all arrays from consumers.
    let arrays = children
        .into_iter()
        .map(|x| x.join().unwrap())
        .collect::<Result<Vec<_>>>()?;
    println!("Finished - {:?}", start.elapsed().unwrap());

    Ok(())
}

fn main() -> Result<()> {
    let array = parallel_read("fixtures/pyarrow3/v1/basic_nullable_10.parquet")?;
    Ok(())
}
```

This can of course be reversed; in configurations where IO is bounded (e.g. when a
network is involved),
we can use multiple producers of pages, potentially divided in file readers, and a single
consumer that performs all CPU-intensive work.
