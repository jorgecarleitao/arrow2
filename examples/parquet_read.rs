use std::fs::File;
use std::io::BufReader;

use arrow2::io::parquet::read;
use arrow2::{array::Array, error::Result};

fn read_field(path: &str, row_group: usize, field: usize) -> Result<Box<dyn Array>> {
    // Open a file, a common operation in Rust
    let mut file = BufReader::new(File::open(path)?);

    // Read the files' metadata. This has a small IO cost because it requires seeking to the end
    // of the file to read its footer.
    let metadata = read::read_metadata(&mut file)?;

    // Convert the files' metadata into an arrow schema. This is CPU-only and amounts to
    // parse thrift if the arrow format is available on a key, or infering the arrow schema from
    // the parquet's physical, converted and logical types.
    let arrow_schema = read::get_schema(&metadata)?;

    // Created an iterator of column chunks. Each iteration
    // yields an iterator of compressed pages. There is almost no CPU work in iterating.
    let columns = read::get_column_iterator(&mut file, &metadata, row_group, field, None, vec![]);

    // get the columns' logical type
    let data_type = arrow_schema.fields()[field].data_type().clone();

    // This is the actual work. In this case, pages are read and
    // decompressed, decoded and deserialized to arrow.
    // Because `columns` is an iterator, it uses a combination of IO and CPU.
    let (array, _, _) = read::column_iter_to_array(columns, data_type, vec![])?;

    Ok(array)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];
    let field = args[2].parse::<usize>().unwrap();
    let row_group = args[3].parse::<usize>().unwrap();

    let array = read_field(file_path, row_group, field)?;
    println!("{}", array);
    Ok(())
}
