use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;

use arrow2::{
    array::{Array, Int32Array},
    datatypes::{Field, Schema},
    error::Result,
    io::csv::write,
    record_batch::RecordBatch,
};

fn parallel_write(path: &str, batches: [RecordBatch; 2]) -> Result<()> {
    let options = write::SerializeOptions::default();

    // write a header
    let writer = &mut write::WriterBuilder::new().from_path(path)?;
    write::write_header(writer, batches[0].schema())?;

    // prepare a channel to send serialized records from threads
    let (tx, rx): (Sender<_>, Receiver<_>) = mpsc::channel();
    let mut children = Vec::new();

    (0..2).for_each(|id| {
        // The sender endpoint can be cloned
        let thread_tx = tx.clone();

        let options = options.clone();
        let batch = batches[id].clone(); // note: this is cheap
        let child = thread::spawn(move || {
            let records = write::serialize(&batch, &options).unwrap();
            thread_tx.send(records).unwrap();
        });

        children.push(child);
    });

    for _ in 0..2 {
        // block: assumes that the order of batches matter.
        let records = rx.recv().unwrap();
        records
            .iter()
            .try_for_each(|record| writer.write_byte_record(record))?
    }

    for child in children {
        child.join().expect("child thread panicked");
    }

    Ok(())
}

fn main() -> Result<()> {
    let array = Int32Array::from(&[
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]);
    let field = Field::new("c1", array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])?;

    parallel_write("example.csv", [batch.clone(), batch])
}
