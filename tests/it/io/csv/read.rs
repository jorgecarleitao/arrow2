use proptest::prelude::*;

use std::io::Cursor;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::csv::read::*;

#[test]
fn read() -> Result<()> {
    let data = r#"city,lat,lng
"Elgin, Scotland, the UK",57.653484,-3.335724
"Stoke-on-Trent, Staffordshire, the UK",53.002666,-2.179404
"Solihull, Birmingham, UK",52.412811,-1.778197
"Cardiff, Cardiff county, UK",51.481583,-3.179090
"Eastbourne, East Sussex, UK",50.768036,0.290472
"Oxford, Oxfordshire, UK",51.752022,-1.257677
"London, UK",51.509865,-0.118092
"Swindon, Swindon, UK",51.568535,-1.772232
"Gravesend, Kent, UK",51.441883,0.370759
"Northampton, Northamptonshire, UK",52.240479,-0.902656
"Rugby, Warwickshire, UK",52.370876,-1.265032
"Sutton Coldfield, West Midlands, UK",52.570385,-1.824042
"Harlow, Essex, UK",51.772938,0.102310
"Aberdeen, Aberdeen City, UK",57.149651,-2.099075"#;
    let mut reader = ReaderBuilder::new().from_reader(Cursor::new(data));

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
    assert_eq!(14, batch.num_rows());
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

#[test]
fn infer_basics() -> Result<()> {
    let file = Cursor::new("1,2,3\na,b,c\na,,c");
    let mut reader = ReaderBuilder::new().from_reader(file);

    let schema = infer_schema(&mut reader, Some(10), false, &infer)?;

    assert_eq!(
        schema,
        Schema::new(vec![
            Field::new("column_1", DataType::Utf8, true),
            Field::new("column_2", DataType::Utf8, true),
            Field::new("column_3", DataType::Utf8, true),
        ])
    );
    Ok(())
}

#[test]
fn infer_ints() -> Result<()> {
    let file = Cursor::new("1,2,3\n1,a,5\n2,,4");
    let mut reader = ReaderBuilder::new().from_reader(file);

    let schema = infer_schema(&mut reader, Some(10), false, &infer)?;

    assert_eq!(
        schema,
        Schema::new(vec![
            Field::new("column_1", DataType::Int64, true),
            Field::new("column_2", DataType::Utf8, true),
            Field::new("column_3", DataType::Int64, true),
        ])
    );
    Ok(())
}

fn test_deserialize(input: &str, data_type: DataType) -> Result<Arc<dyn Array>> {
    let reader = std::io::Cursor::new(input);
    let mut reader = ReaderBuilder::new().has_headers(false).from_reader(reader);

    let mut rows = vec![ByteRecord::default(); 10];
    let rows_read = read_rows(&mut reader, 0, &mut rows)?;
    deserialize_column(&rows[..rows_read], 0, data_type, 0)
}

#[test]
fn int32() -> Result<()> {
    let result = test_deserialize("1,\n,\n3,", DataType::Int32)?;
    let expected = Int32Array::from(&[Some(1), None, Some(3)]);
    assert_eq!(expected, result.as_ref());
    Ok(())
}

#[test]
fn date32() -> Result<()> {
    let result = test_deserialize("1970-01-01,\n2020-03-15,\n1945-05-08,\n", DataType::Date32)?;
    let expected = Int32Array::from(&[Some(0), Some(18336), Some(-9004)]).to(DataType::Date32);
    assert_eq!(expected, result.as_ref());
    Ok(())
}

#[test]
fn date64() -> Result<()> {
    let input = "1970-01-01T00:00:00,\n \
        2018-11-13T17:11:10,\n \
        2018-11-13T17:11:10.011,\n \
        1900-02-28T12:34:56,\n";

    let result = test_deserialize(input, DataType::Date64)?;
    let expected = Int64Array::from(&[
        Some(0),
        Some(1542129070000),
        Some(1542129070011),
        Some(-2203932304000),
    ])
    .to(DataType::Date64);
    assert_eq!(expected, result.as_ref());
    Ok(())
}

#[test]
fn boolean() -> Result<()> {
    let input = vec!["true", "True", "False", "F", "t"];
    let input = input.join("\n");

    let expected = BooleanArray::from(&[Some(true), Some(true), Some(false), None, None]);

    let result = test_deserialize(&input, DataType::Boolean)?;

    assert_eq!(expected, result.as_ref());
    Ok(())
}

#[test]
fn float32() -> Result<()> {
    let input = vec!["12.34", "12", "0.0", "inf", "-inf", "dd"];
    let input = input.join("\n");

    let expected = Float32Array::from(&[
        Some(12.34),
        Some(12.0),
        Some(0.0),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        None,
    ]);

    let result = test_deserialize(&input, DataType::Float32)?;
    assert_eq!(expected, result.as_ref());
    Ok(())
}

#[test]
fn deserialize_binary() -> Result<()> {
    let input = vec!["aa", "bb"];
    let input = input.join("\n");

    let expected = BinaryArray::<i32>::from([Some(b"aa"), Some(b"bb")]);

    let result = test_deserialize(&input, DataType::Binary)?;
    assert_eq!(expected, result.as_ref());
    Ok(())
}

#[test]
fn deserialize_timestamp() -> Result<()> {
    let input = vec!["1996-12-19T16:34:57-02:00", "1996-12-19T16:34:58-02:00"];
    let input = input.join("\n");

    let data_type = DataType::Timestamp(TimeUnit::Millisecond, Some("-01:00".to_string()));

    let expected = Int64Array::from([Some(851020497000), Some(851020498000)]).to(data_type.clone());

    let result = test_deserialize(&input, data_type)?;
    assert_eq!(expected, result.as_ref());
    Ok(())
}

proptest! {
    #[test]
    #[cfg_attr(miri, ignore)] // miri and proptest do not work well :(
    fn i64(v in any::<i64>()) {
        assert_eq!(infer(v.to_string().as_bytes()), DataType::Int64);
    }
}

proptest! {
    #[test]
    #[cfg_attr(miri, ignore)] // miri and proptest do not work well :(
    fn utf8(v in "a.*") {
        assert_eq!(infer(v.as_bytes()), DataType::Utf8);
    }
}

proptest! {
    #[test]
    #[cfg_attr(miri, ignore)] // miri and proptest do not work well :(
    fn dates(v in "1996-12-19T16:3[0-9]:57-02:00") {
        assert_eq!(infer(v.as_bytes()), DataType::Timestamp(TimeUnit::Millisecond, Some("-02:00".to_string())));
    }
}
