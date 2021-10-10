use std::io::Cursor;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::csv::write::*;
use arrow2::record_batch::RecordBatch;

fn data() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Float64, true),
        Field::new("c3", DataType::UInt32, false),
        Field::new("c4", DataType::Boolean, true),
        Field::new("c5", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("c6", DataType::Time32(TimeUnit::Second), false),
        Field::new(
            "c7",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            false,
        ),
    ]);

    let c1 = Utf8Array::<i32>::from_slice([
        "Lorem ipsum dolor sit amet",
        "consectetur adipiscing elit",
        "sed do eiusmod tempor",
    ]);
    let c2 = Float64Array::from([Some(123.564532), None, Some(-556132.25)]);
    let c3 = UInt32Array::from_slice(&[3, 2, 1]);
    let c4 = BooleanArray::from(&[Some(true), Some(false), None]);
    let c5 = PrimitiveArray::<i64>::from([None, Some(1555584887378), Some(1555555555555)])
        .to(DataType::Timestamp(TimeUnit::Millisecond, None));
    let c6 = PrimitiveArray::<i32>::from_slice(&[1234, 24680, 85563])
        .to(DataType::Time32(TimeUnit::Second));
    let keys = UInt32Array::from_slice(&[2, 0, 1]);
    let c7 = DictionaryArray::from_data(keys, Arc::new(c1.clone()));

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(c1),
            Arc::new(c2),
            Arc::new(c3),
            Arc::new(c4),
            Arc::new(c5),
            Arc::new(c6),
            Arc::new(c7),
        ],
    )
    .unwrap()
}

#[test]
fn write_csv() -> Result<()> {
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
        r#"c1,c2,c3,c4,c5,c6,c7
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34,sed do eiusmod tempor
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20,Lorem ipsum dolor sit amet
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03,consectetur adipiscing elit
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34,sed do eiusmod tempor
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20,Lorem ipsum dolor sit amet
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03,consectetur adipiscing elit
"#
        .to_string(),
        String::from_utf8(buffer).unwrap(),
    );
    Ok(())
}

#[test]
fn write_csv_custom_options() -> Result<()> {
    let batch = data();

    let write = Cursor::new(Vec::<u8>::new());
    let mut writer = WriterBuilder::new().delimiter(b'|').from_writer(write);

    let options = SerializeOptions {
        time32_format: "%r".to_string(),
        time64_format: "%r".to_string(),
        ..Default::default()
    };
    write_batch(&mut writer, &batch, &options)?;

    // check
    let buffer = writer.into_inner().unwrap().into_inner();
    assert_eq!(
        r#"Lorem ipsum dolor sit amet|123.564532|3|true||12:20:34 AM|sed do eiusmod tempor
consectetur adipiscing elit||2|false|2019-04-18T10:54:47.378000000|06:51:20 AM|Lorem ipsum dolor sit amet
sed do eiusmod tempor|-556132.25|1||2019-04-18T02:45:55.555000000|11:46:03 PM|consectetur adipiscing elit
"#
        .to_string(),
        String::from_utf8(buffer).unwrap(),
    );
    Ok(())
}
