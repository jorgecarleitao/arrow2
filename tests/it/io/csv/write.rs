use std::io::Cursor;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::csv::write::*;
use arrow2::record_batch::RecordBatch;

fn data() -> RecordBatch {
    let c1 = Utf8Array::<i32>::from_slice(["a b", "c", "d"]);
    let c2 = Float64Array::from([Some(123.564532), None, Some(-556132.25)]);
    let c3 = UInt32Array::from_slice(&[3, 2, 1]);
    let c4 = BooleanArray::from(&[Some(true), Some(false), None]);
    let c5 = PrimitiveArray::<i64>::from([None, Some(1555584887378), Some(1555555555555)])
        .to(DataType::Timestamp(TimeUnit::Millisecond, None));
    let c6 = PrimitiveArray::<i32>::from_slice(&[1234, 24680, 85563])
        .to(DataType::Time32(TimeUnit::Second));
    let keys = UInt32Array::from_slice(&[2, 0, 1]);
    let c7 = DictionaryArray::from_data(keys, Arc::new(c1.clone()));

    RecordBatch::try_from_iter(vec![
        ("c1", Arc::new(c1) as Arc<dyn Array>),
        ("c2", Arc::new(c2) as Arc<dyn Array>),
        ("c3", Arc::new(c3) as Arc<dyn Array>),
        ("c4", Arc::new(c4) as Arc<dyn Array>),
        ("c5", Arc::new(c5) as Arc<dyn Array>),
        ("c6", Arc::new(c6) as Arc<dyn Array>),
        ("c7", Arc::new(c7) as Arc<dyn Array>),
    ])
    .unwrap()
}

#[test]
fn write_csv() -> Result<()> {
    let batch = data();

    let write = Cursor::new(Vec::<u8>::new());
    let mut writer = WriterBuilder::new().from_writer(write);

    write_header(&mut writer, batch.schema())?;
    let options = SerializeOptions::default();
    write_batch(&mut writer, &batch, &options)?;

    // check
    let buffer = writer.into_inner().unwrap().into_inner();
    assert_eq!(
        r#"c1,c2,c3,c4,c5,c6,c7
a b,123.564532,3,true,,00:20:34,d
c,,2,false,2019-04-18 10:54:47.378,06:51:20,a b
d,-556132.25,1,,2019-04-18 02:45:55.555,23:46:03,c
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
        time32_format: Some("%r".to_string()),
        time64_format: Some("%r".to_string()),
        ..Default::default()
    };
    write_batch(&mut writer, &batch, &options)?;

    // check
    let buffer = writer.into_inner().unwrap().into_inner();
    assert_eq!(
        r#"a b|123.564532|3|true||12:20:34 AM|d
c||2|false|2019-04-18 10:54:47.378|06:51:20 AM|a b
d|-556132.25|1||2019-04-18 02:45:55.555|11:46:03 PM|c
"#
        .to_string(),
        String::from_utf8(buffer).unwrap(),
    );
    Ok(())
}

fn data_array(column: usize) -> (RecordBatch, Vec<&'static str>) {
    let (array, expected) = match column {
        0 => (
            Arc::new(Utf8Array::<i64>::from_slice(["a b", "c", "d"])) as Arc<dyn Array>,
            vec!["a b", "c", "d"],
        ),
        1 => (
            Arc::new(BinaryArray::<i32>::from_slice(["a b", "c", "d"])) as Arc<dyn Array>,
            vec!["a b", "c", "d"],
        ),
        2 => (
            Arc::new(BinaryArray::<i64>::from_slice(["a b", "c", "d"])) as Arc<dyn Array>,
            vec!["a b", "c", "d"],
        ),
        3 => (
            Arc::new(Int8Array::from_slice(&[3, 2, 1])) as Arc<dyn Array>,
            vec!["3", "2", "1"],
        ),
        4 => (
            Arc::new(Int16Array::from_slice(&[3, 2, 1])) as Arc<dyn Array>,
            vec!["3", "2", "1"],
        ),
        5 => (
            Arc::new(Int32Array::from_slice(&[3, 2, 1])) as Arc<dyn Array>,
            vec!["3", "2", "1"],
        ),
        6 => (
            Arc::new(Int64Array::from_slice(&[3, 2, 1])) as Arc<dyn Array>,
            vec!["3", "2", "1"],
        ),
        7 => (
            Arc::new(UInt64Array::from_slice(&[3, 2, 1])) as Arc<dyn Array>,
            vec!["3", "2", "1"],
        ),
        8 => (
            Arc::new(UInt64Array::from_slice(&[3, 2, 1])) as Arc<dyn Array>,
            vec!["3", "2", "1"],
        ),
        9 => {
            let array = PrimitiveArray::<i32>::from_slice(&[1_234_001, 24_680_001, 85_563_001])
                .to(DataType::Time32(TimeUnit::Millisecond));
            (
                Arc::new(array) as Arc<dyn Array>,
                vec!["00:20:34.001", "06:51:20.001", "23:46:03.001"],
            )
        }
        10 => {
            let array =
                PrimitiveArray::<i64>::from_slice(&[1_234_000_001, 24_680_000_001, 85_563_000_001])
                    .to(DataType::Time64(TimeUnit::Microsecond));
            (
                Arc::new(array) as Arc<dyn Array>,
                vec!["00:20:34.000001", "06:51:20.000001", "23:46:03.000001"],
            )
        }
        11 => {
            let array = PrimitiveArray::<i64>::from_slice(&[
                1_234_000_000_001,
                24_680_000_000_001,
                85_563_000_000_001,
            ])
            .to(DataType::Time64(TimeUnit::Nanosecond));
            (
                Arc::new(array) as Arc<dyn Array>,
                vec![
                    "00:20:34.000000001",
                    "06:51:20.000000001",
                    "23:46:03.000000001",
                ],
            )
        }
        12 => {
            let array = PrimitiveArray::<i64>::from_slice([
                1_555_584_887_378_000_001,
                1_555_555_555_555_000_001,
            ])
            .to(DataType::Timestamp(TimeUnit::Nanosecond, None));
            (
                Arc::new(array) as Arc<dyn Array>,
                vec![
                    "2019-04-18 10:54:47.378000001",
                    "2019-04-18 02:45:55.555000001",
                ],
            )
        }
        13 => {
            let array = PrimitiveArray::<i64>::from_slice([
                1_555_584_887_378_000_001,
                1_555_555_555_555_000_001,
            ])
            .to(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("+01:00".to_string()),
            ));
            (
                Arc::new(array) as Arc<dyn Array>,
                vec![
                    "2019-04-18 11:54:47.378000001 +01:00",
                    "2019-04-18 03:45:55.555000001 +01:00",
                ],
            )
        }
        14 => {
            let array = PrimitiveArray::<i64>::from_slice([
                1_555_584_887_378_000_001,
                1_555_555_555_555_000_001,
            ])
            .to(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("Europe/Lisbon".to_string()),
            ));
            (
                Arc::new(array) as Arc<dyn Array>,
                vec![
                    "2019-04-18 11:54:47.378000001 WEST",
                    "2019-04-18 03:45:55.555000001 WEST",
                ],
            )
        }
        _ => todo!(),
    };

    (
        RecordBatch::try_from_iter(vec![("c1", array)]).unwrap(),
        expected,
    )
}

fn write_single(column: usize) -> Result<()> {
    let (batch, data) = data_array(column);

    let write = Cursor::new(Vec::<u8>::new());
    let mut writer = WriterBuilder::new().delimiter(b'|').from_writer(write);

    write_header(&mut writer, batch.schema())?;
    let options = SerializeOptions::default();
    write_batch(&mut writer, &batch, &options)?;

    // check
    let buffer = writer.into_inner().unwrap().into_inner();

    let mut expected = "c1\n".to_owned();
    expected.push_str(&data.join("\n"));
    expected.push('\n');
    assert_eq!(expected, String::from_utf8(buffer).unwrap(),);
    Ok(())
}

#[test]
fn write_each() -> Result<()> {
    for i in 0..=13 {
        write_single(i)?;
    }
    Ok(())
}

#[test]
#[cfg(feature = "chrono-tz")]
fn write_tz_timezone() -> Result<()> {
    write_single(14)
}
