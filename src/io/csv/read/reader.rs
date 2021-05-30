use std::io::Read;

use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};

use super::{ByteRecord, Reader};

use crate::{
    datatypes::*,
    error::{ArrowError, Result},
};

pub fn projected_schema(schema: &Schema, projection: Option<&[usize]>) -> Schema {
    match &projection {
        Some(projection) => {
            let fields = schema.fields();
            let projected_fields: Vec<Field> =
                projection.iter().map(|i| fields[*i].clone()).collect();
            Schema::new_from(projected_fields, schema.metadata().clone())
        }
        None => schema.clone(),
    }
}

/// Reads `len` rows from the CSV into Bytes, skiping `skip`
/// This operation has minimal CPU work and is thus the fastest way to read through a CSV
/// without deserializing the contents to arrow.
pub fn read_rows<R: Read>(
    reader: &mut Reader<R>,
    skip: usize,
    rows: &mut [ByteRecord],
) -> Result<usize> {
    // skip first `start` rows.
    let mut row = ByteRecord::new();
    for _ in 0..skip {
        let res = reader.read_byte_record(&mut row);
        if !res.unwrap_or(false) {
            break;
        }
    }

    let mut row_number = 0;
    for row in rows.iter_mut() {
        let has_more = reader.read_byte_record(row).map_err(|e| {
            ArrowError::External(format!(" at line {}", skip + row_number), Box::new(e))
        })?;
        if !has_more {
            break;
        }
        row_number += 1;
    }
    Ok(row_number)
}

lazy_static! {
    static ref DECIMAL_RE: Regex = Regex::new(r"^-?(\d+\.\d+)$").unwrap();
    static ref INTEGER_RE: Regex = Regex::new(r"^-?(\d+)$").unwrap();
    static ref BOOLEAN_RE: Regex = RegexBuilder::new(r"^(true)$|^(false)$")
        .case_insensitive(true)
        .build()
        .unwrap();
    static ref DATE_RE: Regex = Regex::new(r"^\d{4}-\d\d-\d\d$").unwrap();
    static ref DATETIME_RE: Regex = Regex::new(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d$").unwrap();
}

/// Infer the data type of a record
pub fn infer(string: &str) -> DataType {
    // when quoting is enabled in the reader, these quotes aren't escaped, we default to
    // Utf8 for them
    if string.starts_with('"') {
        return DataType::Utf8;
    }
    // match regex in a particular order
    if BOOLEAN_RE.is_match(string) {
        DataType::Boolean
    } else if DECIMAL_RE.is_match(string) {
        DataType::Float64
    } else if INTEGER_RE.is_match(string) {
        DataType::Int64
    } else if DATETIME_RE.is_match(string) {
        DataType::Date64
    } else if DATE_RE.is_match(string) {
        DataType::Date32
    } else {
        DataType::Utf8
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::{deserialize_batch, deserialize_column, infer_schema, ReaderBuilder};
    use super::*;

    use crate::array::{Float64Array, Utf8Array};

    #[test]
    fn test_read() -> Result<()> {
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
}
