use std::io::{Read, Seek};
use std::sync::Arc;

use csv::ByteRecord;
use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};

use crate::record_batch::RecordBatch;
use crate::{
    array::Array,
    datatypes::*,
    error::{ArrowError, Result},
};

use super::{new_boolean_array, new_primitive_array, new_utf8_array, parser::GenericParser};

pub fn projected_schema(schema: &Schema, projection: Option<&[usize]>) -> Schema {
    match &projection {
        Some(projection) => {
            let fields = schema.fields();
            let projected_fields: Vec<Field> =
                projection.iter().map(|i| fields[*i].clone()).collect();
            Schema::new_from(
                projected_fields,
                schema.metadata().clone(),
                schema.is_little_endian(),
            )
        }
        None => schema.clone(),
    }
}

pub fn read_batch<R: Read + Seek, P: GenericParser<ArrowError>>(
    reader: &mut csv::Reader<R>,
    parser: &P,
    skip: usize,
    len: usize,
    schema: Schema,
    projection: Option<&[usize]>,
) -> Result<RecordBatch> {
    // skip first `start` rows.
    let mut row = ByteRecord::new();
    for _ in 0..skip {
        let res = reader.read_byte_record(&mut row);
        if !res.unwrap_or(false) {
            break;
        }
    }

    let rows = reader
        .byte_records()
        .enumerate()
        .take(len)
        .map(|(row_number, row)| {
            row.map_err(|e| {
                ArrowError::External(format!(" at line {}", skip + row_number), Box::new(e))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    // parse the batches into a RecordBatch
    parse(&rows, schema.fields(), projection, skip, parser)
}

macro_rules! primitive {
    ($type:ty, $line_number:expr, $rows:expr, $i:expr, $data_type:expr, $parser:expr) => {
        new_primitive_array::<$type, ArrowError, _>($line_number, $rows, $i, $data_type, $parser)
            .map(|x| Arc::new(x) as Arc<dyn Array>)
    };
}

/// parses a slice of [csv_crate::StringRecord] into a [array::record_batch::RecordBatch].
fn parse<P: GenericParser<ArrowError>>(
    rows: &[ByteRecord],
    fields: &[Field],
    projection: Option<&[usize]>,
    line_number: usize,
    parser: &P,
) -> Result<RecordBatch> {
    let projection: Vec<usize> = match projection {
        Some(ref v) => v.to_vec(),
        None => fields.iter().enumerate().map(|(i, _)| i).collect(),
    };

    let columns = projection
        .iter()
        .map(|i| {
            let i = *i;
            let field = &fields[i];
            let data_type = field.data_type();
            match data_type {
                DataType::Boolean => new_boolean_array(line_number, rows, i, parser)
                    .map(|x| Arc::new(x) as Arc<dyn Array>),
                DataType::Int8 => {
                    primitive!(i8, line_number, rows, i, data_type, parser)
                }
                DataType::Int16 => primitive!(i16, line_number, rows, i, data_type, parser),
                DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
                    primitive!(i32, line_number, rows, i, data_type, parser)
                }
                DataType::Int64
                | DataType::Date64
                | DataType::Time64(_)
                | DataType::Timestamp(_, None) => {
                    primitive!(i64, line_number, rows, i, data_type, parser)
                }
                DataType::UInt8 => primitive!(u8, line_number, rows, i, data_type, parser),
                DataType::UInt16 => primitive!(u16, line_number, rows, i, data_type, parser),
                DataType::UInt32 => primitive!(u32, line_number, rows, i, data_type, parser),
                DataType::UInt64 => primitive!(u64, line_number, rows, i, data_type, parser),
                DataType::Float32 => primitive!(f32, line_number, rows, i, data_type, parser),
                DataType::Float64 => primitive!(f64, line_number, rows, i, data_type, parser),
                DataType::Utf8 => {
                    new_utf8_array::<i32, ArrowError, _>(line_number, rows, i, parser)
                        .map(|x| Arc::new(x) as Arc<dyn Array>)
                }
                DataType::LargeUtf8 => {
                    new_utf8_array::<i64, ArrowError, _>(line_number, rows, i, parser)
                        .map(|x| Arc::new(x) as Arc<dyn Array>)
                }
                other => Err(ArrowError::NotYetImplemented(format!(
                    "Unsupported data type {:?}",
                    other
                ))),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let projected_fields: Vec<Field> = projection.iter().map(|i| fields[*i].clone()).collect();

    let projected_schema = Schema::new(projected_fields);

    RecordBatch::try_new(projected_schema, columns)
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
    use csv::ReaderBuilder;

    use crate::array::{Float64Array, Utf8Array};

    use super::super::infer_schema;
    use super::super::DefaultParser;
    use super::*;

    #[test]
    fn test_read() -> Result<()> {
        let builder = ReaderBuilder::new();
        let mut reader = builder.from_path("test/data/uk_cities_with_headers.csv")?;
        let parser = DefaultParser::default();

        let schema = infer_schema(&mut reader, None, true, &infer)?;

        let batch = read_batch(&mut reader, &parser, 0, 100, schema.clone(), None)?;

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
