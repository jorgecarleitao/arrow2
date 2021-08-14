use std::sync::Arc;

use arrow2::array::{Float64Array, Utf8Array};
use arrow2::error::Result;
use arrow2::io::csv::read::*;

#[test]
fn read() -> Result<()> {
    let mut reader = ReaderBuilder::new().from_path("test/data/uk_cities_with_headers.csv")?;

    let schema = Arc::new(infer_schema(&mut reader, None, true, &infer)?);

    let mut rows = vec![ByteRecord::default(); 100];
    let rows_read = read_rows(&mut reader, 0, &mut rows)?;

    let batch = deserialize_batch(
        &rows[..rows_read],
        schema.fields(),
        None,
        0,
        deserialize_column,
    )?;

    let batch_schema = batch.schema();

    assert_eq!(&schema, batch_schema);
    assert_eq!(37, batch.num_rows());
    assert_eq!(3, batch.num_columns());

    let lat = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((57.653484 - lat.value(0)).abs() < f64::EPSILON);

    let city = batch
        .column(0)
        .as_any()
        .downcast_ref::<Utf8Array<i32>>()
        .unwrap();

    assert_eq!("Elgin, Scotland, the UK", city.value(0));
    assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
    Ok(())
}
