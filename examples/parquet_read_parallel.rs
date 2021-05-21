use crossbeam_channel::unbounded;

use std::fs::File;
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

use arrow2::{array::Array, compute::cast::cast, error::Result, io::parquet::read};

fn parallel_read(path: &str) -> Result<Vec<Box<dyn Array>>> {
    // prepare a channel to send serialized records from threads
    let (tx, rx) = unbounded();

    let mut file = File::open(path)?;
    let file_metadata = read::read_metadata(&mut file)?;
    let arrow_schema = Arc::new(read::get_schema(&file_metadata)?);

    let file_metadata = Arc::new(file_metadata);

    let start = SystemTime::now();
    // spawn a thread to produce `Vec<CompressedPage>` (IO bounded)
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

    let mut children = Vec::new();
    // use 3 consumers of to decompress, decode and deserialize.
    for _ in 0..3 {
        let rx_consumer = rx.clone();
        let metadata_consumer = file_metadata.clone();
        let arrow_schema_consumer = arrow_schema.clone();
        let child = thread::spawn(move || {
            let (column, row_group, iter) = rx_consumer.recv().unwrap();
            let start = SystemTime::now();
            println!("consumer start - {} {}", column, row_group);
            let descriptor = metadata_consumer.row_groups[row_group]
                .column(column)
                .descriptor();
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

    child.join().expect("child thread panicked");

    let arrays = children
        .into_iter()
        .map(|x| x.join().unwrap())
        .collect::<Result<Vec<_>>>()?;
    println!("Finished - {:?}", start.elapsed().unwrap());

    Ok(arrays)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];

    let arrays = parallel_read(file_path)?;
    for array in arrays {
        println!("{}", array)
    }
    Ok(())
}
