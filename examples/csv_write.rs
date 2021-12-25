use std::sync::Arc;

use arrow2::{
    array::Int32Array,
    datatypes::{Field, Schema},
    error::Result,
    io::csv::write,
    record_batch::RecordBatch,
};

fn write_batch(path: &str, batches: &[RecordBatch]) -> Result<()> {
    let writer = &mut write::WriterBuilder::new().from_path(path)?;

    write::write_header(writer, batches[0].schema())?;

    let options = write::SerializeOptions::default();
    batches
        .iter()
        .try_for_each(|batch| write::write_batch(writer, batch, &options))
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

    write_batch("example.csv", &[batch])
}
