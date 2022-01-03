use std::fs::File;
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

use crossbeam_channel::unbounded;

use arrow2::{
    array::Array, chunk::Chunk, error::Result, io::parquet::read,
    io::parquet::read::MutStreamingIterator,
};

fn parallel_read(path: &str, row_group: usize) -> Result<Chunk<Arc<dyn Array>>> {
    // prepare a channel to send compressed pages across threads.
    let (tx, rx) = unbounded();

    let mut file = File::open(path)?;
    let file_metadata = read::read_metadata(&mut file)?;
    let arrow_schema = Arc::new(read::get_schema(&file_metadata)?);

    let start = SystemTime::now();
    // spawn a thread to produce `Vec<CompressedPage>` (IO bounded)
    let producer = thread::spawn(move || {
        for (field_i, field) in file_metadata.schema().fields().iter().enumerate() {
            let start = SystemTime::now();

            let mut columns = read::get_column_iterator(
                &mut file,
                &file_metadata,
                row_group,
                field_i,
                None,
                vec![],
            );

            println!("produce start - field: {}", field_i);

            let mut column_chunks = vec![];
            while let read::State::Some(mut new_iter) = columns.advance().unwrap() {
                if let Some((pages, metadata)) = new_iter.get() {
                    let pages = pages.collect::<Vec<_>>();

                    column_chunks.push((pages, metadata.clone()));
                }
                columns = new_iter;
            }
            // todo: create API to allow sending each column (and not column chunks) to be processed in parallel
            tx.send((field_i, field.clone(), column_chunks)).unwrap();
            println!(
                "produce end - {:?}: {} {}",
                start.elapsed().unwrap(),
                field_i,
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
                while let Ok((field_i, parquet_field, column_chunks)) = rx_consumer.recv() {
                    let start = SystemTime::now();
                    let field = &arrow_schema_consumer.fields()[field_i];
                    println!("consumer {} start - {}", i, field_i);

                    let columns = read::ReadColumnIterator::new(parquet_field, column_chunks);

                    let array = read::column_iter_to_array(columns, field, vec![]).map(|x| x.0);
                    println!(
                        "consumer {} end - {:?}: {}",
                        i,
                        start.elapsed().unwrap(),
                        field_i
                    );

                    arrays.push((field_i, array))
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

    Chunk::try_new(columns)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];

    let start = SystemTime::now();
    let batch = parallel_read(file_path, 0)?;
    assert!(!batch.is_empty());
    println!("took: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())
}
