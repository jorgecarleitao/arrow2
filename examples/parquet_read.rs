use std::fs::File;

use arrow2::io::parquet::read;
use arrow2::{array::Array, error::Result};

fn read_column_chunk(path: &str, row_group: usize, column: usize) -> Result<Box<dyn Array>> {
    // Open a file, a common operation in Rust
    let mut file = File::open(path)?;

    // Read the files' metadata. This has a small IO cost because it requires seeking to the end
    // of the file to read its footer.
    let file_metadata = read::read_metadata(&mut file)?;

    // Convert the files' metadata into an arrow schema. This is CPU-only and amounts to
    // parse thrift if the arrow format is available on a key, or infering the arrow schema from
    // the parquet's physical, converted and logical types.
    let arrow_schema = read::get_schema(&file_metadata)?;

    // Construct an iterator over pages. This binds `file` to this iterator, and each iteration
    // is IO intensive as it will read a compressed page into memory. There is almost no CPU work
    // on this operation
    let pages =
        read::get_page_iterator(&file_metadata, row_group, column, &mut file, None, vec![])?;

    // get the columns' metadata
    let metadata = file_metadata.row_groups[row_group].column(column);

    // get the columns' logical type
    let data_type = arrow_schema.fields()[column].data_type().clone();

    // This is the actual work. In this case, pages are read (by calling `iter.next()`) and are
    // immediately decompressed, decoded, deserialized to arrow and deallocated.
    // This uses a combination of IO and CPU. At this point, `array` is the arrow-corresponding
    // array of the parquets' physical type.
    // `Decompressor` re-uses an internal buffer for de-compression, thereby maximizing memory re-use.
    let mut pages = read::Decompressor::new(pages, vec![]);

    read::page_iter_to_array(&mut pages, metadata, data_type)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];
    let column = args[2].parse::<usize>().unwrap();
    let row_group = args[3].parse::<usize>().unwrap();

    let array = read_column_chunk(file_path, row_group, column)?;
    println!("{}", array);
    Ok(())
}
