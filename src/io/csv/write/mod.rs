mod serialize;

use std::io::Write;

// re-export necessary public APIs from csv
pub use csv::{ByteRecord, Writer, WriterBuilder};

pub use serialize::*;

use crate::record_batch::RecordBatch;
use crate::{datatypes::Schema, error::Result};

/// Creates serializers that iterate over each column of `batch` and serialize each item according
/// to `options`.
fn new_serializers<'a>(
    batch: &'a RecordBatch,
    options: &'a SerializeOptions,
) -> Result<Vec<Box<dyn Iterator<Item = Vec<u8>> + 'a>>> {
    batch
        .columns()
        .iter()
        .map(|column| new_serializer(column.as_ref(), options))
        .collect()
}

/// Serializes a [`RecordBatch`] as vector of `ByteRecord`.
/// The vector is guaranteed to have `batch.num_rows()` entries.
/// Each `ByteRecord` is guaranteed to have `batch.num_columns()` fields.
pub fn serialize(batch: &RecordBatch, options: &SerializeOptions) -> Result<Vec<ByteRecord>> {
    let mut serializers = new_serializers(batch, options)?;

    let mut records = vec![ByteRecord::with_capacity(0, batch.num_columns()); batch.num_rows()];
    records.iter_mut().for_each(|record| {
        serializers
            .iter_mut()
            // `unwrap` is infalible because `array.len()` equals `num_rows` on a `RecordBatch`
            .for_each(|iter| record.push_field(&iter.next().unwrap()));
    });
    Ok(records)
}

/// Writes the data in a `RecordBatch` to `writer` according to the serialization options `options`.
pub fn write_batch<W: Write>(
    writer: &mut Writer<W>,
    batch: &RecordBatch,
    options: &SerializeOptions,
) -> Result<()> {
    let mut serializers = new_serializers(batch, options)?;

    let mut record = ByteRecord::with_capacity(0, batch.num_columns());

    // this is where the (expensive) transposition happens: the outer loop is on rows, the inner on columns
    (0..batch.num_rows()).try_for_each(|_| {
        serializers
            .iter_mut()
            // `unwrap` is infalible because `array.len()` equals `num_rows` on a `RecordBatch`
            .for_each(|iter| record.push_field(&iter.next().unwrap()));
        writer.write_byte_record(&record)?;
        record.clear();
        Result::Ok(())
    })?;
    Ok(())
}

/// Writes a header to `writer` according to `schema`
pub fn write_header<W: Write>(writer: &mut Writer<W>, schema: &Schema) -> Result<()> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    writer.write_record(&fields)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::WriterBuilder;

    use super::*;

    use crate::array::*;
    use crate::datatypes::*;

    use std::io::Cursor;
    use std::sync::Arc;

    fn data() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::UInt32, false),
            Field::new("c4", DataType::Boolean, true),
            Field::new("c5", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("c6", DataType::Time32(TimeUnit::Second), false),
        ]);

        let c1 = Utf8Array::<i32>::from_slice([
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
        ]);
        let c2 = Float64Array::from([Some(123.564532), None, Some(-556132.25)]);
        let c3 = UInt32Array::from_slice(&[3, 2, 1]);
        let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
        let c5 = Primitive::<i64>::from([None, Some(1555584887378), Some(1555555555555)])
            .to(DataType::Timestamp(TimeUnit::Millisecond, None));
        let c6 = Primitive::<i32>::from_slice(vec![1234, 24680, 85563])
            .to(DataType::Time32(TimeUnit::Second));

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(c1),
                Arc::new(c2),
                Arc::new(c3),
                Arc::new(c4),
                Arc::new(c5),
                Arc::new(c6),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_write_csv() -> Result<()> {
        let batch = data();

        let write = Cursor::new(Vec::<u8>::new());
        let mut writer = WriterBuilder::new().from_writer(write);

        write_header(&mut writer, batch.schema())?;
        let batches = vec![&batch, &batch];
        let options = SerializeOptions::default();
        batches
            .iter()
            .try_for_each(|batch| write_batch(&mut writer, batch, &options))?;

        // check
        let buffer = writer.into_inner().unwrap().into_inner();
        assert_eq!(
            r#"c1,c2,c3,c4,c5,c6
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03
"#
            .to_string(),
            String::from_utf8(buffer).unwrap(),
        );
        Ok(())
    }

    #[test]
    fn test_write_csv_custom_options() -> Result<()> {
        let batch = data();

        let write = Cursor::new(Vec::<u8>::new());
        let mut writer = WriterBuilder::new().delimiter(b'|').from_writer(write);

        let options = SerializeOptions {
            time_format: "%r".to_string(),
            ..Default::default()
        };
        write_batch(&mut writer, &batch, &options)?;

        // check
        let buffer = writer.into_inner().unwrap().into_inner();
        assert_eq!(
            r#"Lorem ipsum dolor sit amet|123.564532|3|true||12:20:34 AM
consectetur adipiscing elit||2|false|2019-04-18T10:54:47.378000000|06:51:20 AM
sed do eiusmod tempor|-556132.25|1||2019-04-18T02:45:55.555000000|11:46:03 PM
"#
            .to_string(),
            String::from_utf8(buffer).unwrap(),
        );
        Ok(())
    }
}
