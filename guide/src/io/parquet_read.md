# Parquet reader

When compiled with feature `io_parquet`, you can use this crate to read parquet files.
This crate makes minimal assumptions on how you want to read a file, and offers a large
degree of customization to it, particularly into how you want to decompose the
reading between CPU intensive tasks and IO intensive tasks.

Some notation first:

* `page`: a "slice" of a column (e.g. similar of a slice of an `Array`)
* `column chunk`: composed of multiple pages; (similar of an `Array`)
* `row group`: a group of columns with the same length (similar of a `RecordBatch` in Arrow)

Let's start with a simple example: read a single column chunk out of a single row group:

```rust
use std::fs::File;

use arrow2::compute::cast::cast;
use arrow2::io::parquet::read;
use arrow2::{array::Array, error::Result};

fn read_column_chunk(path: &str, row_group: usize, column: usize) -> Result<Box<dyn Array>> {
    // #1 Open a file, a common operation in Rust
    let mut file = File::open(path)?;

    // #2 Read the files' metadata. This has a small IO cost because it requires seeking to the end
    // of the file to read its footer.
    let file_metadata = read::read_metadata(&mut file)?;

    // #3 Convert the files' metadata into an arrow schema. This is CPU-only and amounts to
    // parse thrift if the arrow format is available on a key, or infering the arrow schema from
    // the parquet's physical, converted and logical types.
    let arrow_schema = read::get_schema(&file_metadata)?;

    // #4 Construct an iterator over pages. This binds `file` to this iterator, and each iteration
    // is IO intensive as it will read a compressed page into memory. There is almost no CPU work
    // on this operation
    let iter = read::get_page_iterator(&file_metadata, row_group, column, &mut file)?;

    // #5 This is for now required, but we hope to remove the need for this clone.
    let descriptor = iter.descriptor().clone();

    // #6 This is the actual work. In this case, pages are read (by calling `iter.next()`) and is
    // immediately decompressed, decoded, deserialized to arrow and deallocated.
    // This uses a combination of IO and CPU. At this point, `array` is the arrow corresponding
    // array of the parquets' physical type.
    let array = read::page_iter_to_array(iter, &descriptor)?;

    // #7 Cast the array to the corresponding Arrow's data type. When the physical type
    // is the same, this operation is `O(1)`. Otherwise, it incurs an expected CPU cost.
    cast(array.as_ref(), arrow_schema.field(column).data_type())
}
```

The example above minimizes memory usage, at the expense of mixing IO and CPU, which may hurt performance
if one of them is a bottleneck.

### Parallelism decoupling of CPU from IO

One important aspect of the pages created by the iterator above is that they can cross thread boundaries.
Consequently, the thread reading pages from a file (IO-bounded) does not have to be the same thread
performing CPU-bounded work (decompressing, decoding, etc.).

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

This can of course be reversed; in configurations where IO is bounded (e.g. when a network is involved),
we can use multiple producers of pages, potentially divided in file readers, and a single
consumer that performs all CPU-intensive work.
