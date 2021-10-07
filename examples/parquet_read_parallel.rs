use crossbeam_channel::unbounded;

use std::fs::File;
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

use arrow2::{
    array::Array, error::Result, io::parquet::read, io::parquet::read::MutStreamingIterator,
    record_batch::RecordBatch,
};

fn parallel_read(path: &str, row_group: usize) -> Result<RecordBatch> {
    // prepare a channel to send compressed pages across threads.
    let (tx, rx) = unbounded();

    let mut file = File::open(path)?;
    let file_metadata = read::read_metadata(&mut file)?;
    let arrow_schema = Arc::new(read::get_schema(&file_metadata)?);

    let start = SystemTime::now();
    // spawn a thread to produce `Vec<CompressedPage>` (IO bounded)
    let producer = thread::spawn(move || {
        for field in 0..file_metadata.schema().fields().len() {
            let start = SystemTime::now();

            let mut columns = read::get_column_iterator(
                &mut file,
                &file_metadata,
                row_group,
                field,
                None,
                vec![],
            );

            println!("produce start - field: {}", field);

            while let read::State::Some(mut new_iter) = columns.advance().unwrap() {
                if let Some((pages, metadata)) = new_iter.get() {
                    let pages = pages.collect::<Vec<_>>();

                    tx.send((field, metadata.clone(), pages)).unwrap();
                }
                columns = new_iter;
            }
            println!(
                "produce end - {:?}: {} {}",
                start.elapsed().unwrap(),
                field,
                row_group
            );
        }
    });

    // use 2 consumers for CPU-intensive to decompress, decode and deserialize.
    #[allow(clippy::needless_collect)] // we need to collect to parallelize
    let consumers = (0..2)
        .map(|i| {
            let rx_consumer = rx.clone();
            let arrow_schema_consumer = arrow_schema.clone();
            thread::spawn(move || {
                let mut arrays = vec![];
                while let Ok((field, metadata, pages)) = rx_consumer.recv() {
                    let start = SystemTime::now();
                    let data_type = arrow_schema_consumer.fields()[field].data_type().clone();
                    println!("consumer {} start - {}", i, field);

                    let mut pages = read::BasicDecompressor::new(pages.into_iter(), vec![]);

                    let array = read::page_iter_to_array(&mut pages, &metadata, data_type);
                    println!(
                        "consumer {} end - {:?}: {}",
                        i,
                        start.elapsed().unwrap(),
                        field
                    );

                    arrays.push((field, array))
                }
                arrays
            })
        })
        .collect::<Vec<_>>();

    producer.join().expect("producer thread panicked");

    // collect all columns (join threads)
    let mut columns = consumers
        .into_iter()
        .map(|x| x.join().unwrap())
        .flatten()
        .map(|x| Ok((x.0, x.1?)))
        .collect::<Result<Vec<(usize, Box<dyn Array>)>>>()?;
    // order may not be the same
    columns.sort_unstable_by_key(|x| x.0);
    let columns = columns.into_iter().map(|x| x.1.into()).collect();
    println!("Finished - {:?}", start.elapsed().unwrap());

    RecordBatch::try_new(arrow_schema, columns)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];

    let batch = parallel_read(file_path, 0)?;
    for array in batch.columns() {
        println!("{}", array)
    }
    Ok(())
}
