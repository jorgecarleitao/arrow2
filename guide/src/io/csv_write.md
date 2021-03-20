# Write CSV

When compiled with feature `io_csv`, you can use this crate to write CSV files.

This crate relies on [the crate csv](https://crates.io/crates/csv) to write well-formed CSV files, which your code should also depend on.

The following example writes a batch as a CSV file with default configuration:

```rust
use csv::WriterBuilder;
use arrow2::io::csv::write;
use arrow2::record_batch::RecordBatch;
use arrow2::error::Result;

fn write(path: &str, batches: &[RecordBatch]) -> Result<()> {
    let writer = &mut WriterBuilder::new().from_path(path)?;

    write::write_header(writer, batches[0].schema())?;

    let options = write::SerializeOptions::default();
    batches.iter().try_for_each(|batch| {
        write::write_batch(writer, batch, &options)
    })
}
```

## Parallelism

This crate exposes functionality to decouple serialization from writing.

In the example above, the serialization and writing to a file is done syncronously.
However, these typically deal with different bounds: serialization is often CPU bounded, while writing is often IO bounded. We can trade-off these through a higher memory usage.

Suppose that we know that we are getting CPU-bounded at serialization, and would like to offload that workload to other threads, at the cost of a higher memory usage. We would achieve this as follows (two batches for simplicity):

```rust
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;

use csv::{WriterBuilder, ByteRecord};
use arrow2::io::csv::write;
use arrow2::record_batch::RecordBatch;
use arrow2::error::Result;

fn parallel_write(path: &str, batches: [RecordBatch; 2]) -> Result<()> {
    let options = write::SerializeOptions::default();

    // write a header
    let writer = &mut WriterBuilder::new().from_path(path)?;
    write::write_header(writer, batches[0].schema())?;

    // prepare a channel to send serialized records from threads
    let (tx, rx): (Sender<_>, Receiver<_>) = mpsc::channel();
    let mut children = Vec::new();

    for id in 0..2 {
        // The sender endpoint can be copied
        let thread_tx = tx.clone();

        let options = options.clone();
        let batch = batches[id].clone();  // note: this is cheap
        let child = thread::spawn(move || {
            let records = write::serialize(&batch, &options).unwrap();
            thread_tx.send(records).unwrap();
        });

        children.push(child);
    }

    for _ in 0..2 {
        // block: assumes that the order of batches matter.
        let records = rx.recv().unwrap();
        records.iter().try_for_each(|record| {
            writer.write_byte_record(record)
        })?
    }

    for child in children {
        child.join().expect("child thread panicked");
    }

    Ok(())
}
```
