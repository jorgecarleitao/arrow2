use arrow2::array::*;
use arrow2::compute::temporal::*;
use arrow2::datatypes::*;

#[test]
fn temporal_hour() {
    for data_type in TestData::available_time_like_types() {
        let data = TestData::data(&data_type);
        let result = hour(&*data.input).unwrap();

        assert_eq!(
            result,
            data.hour.unwrap(),
            "\"hour\" failed on type: {:?}",
            data_type
        );
    }
}

#[test]
fn temporal_minute() {
    for data_type in TestData::available_time_like_types() {
        let data = TestData::data(&data_type);
        let result = minute(&*data.input).unwrap();

        assert_eq!(
            result,
            data.minute.unwrap(),
            "\"hour\" failed on type: {:?}",
            data_type
        );
    }
}

#[test]
fn temporal_second() {
    for data_type in TestData::available_time_like_types() {
        let data = TestData::data(&data_type);
        let result = second(&*data.input).unwrap();

        assert_eq!(
            result,
            data.second.unwrap(),
            "\"second\" failed on type: {:?}",
            data_type
        );
    }
}

#[test]
fn temporal_nanosecond() {
    for data_type in TestData::available_time_like_types() {
        let data = TestData::data(&data_type);
        let result = nanosecond(&*data.input).unwrap();

        assert_eq!(
            result,
            data.nanosecond.unwrap(),
            "\"nanosecond\" failed on type: {:?}",
            data_type
        );
    }
}

#[test]
fn temporal_year() {
    for data_type in TestData::available_date_like_types() {
        let data = TestData::data(&data_type);
        let result = year(&*data.input).unwrap();

        assert_eq!(
            result,
            data.year.unwrap(),
            "\"year\" failed on type: {:?}",
            data_type
        );
    }
}

#[test]
fn temporal_month() {
    for data_type in TestData::available_date_like_types() {
        let data = TestData::data(&data_type);
        let result = month(&*data.input).unwrap();

        assert_eq!(
            result,
            data.month.unwrap(),
            "\"month\" failed on type: {:?}",
            data_type
        );
    }
}

#[test]
fn temporal_day() {
    for data_type in TestData::available_date_like_types() {
        let data = TestData::data(&data_type);
        let result = day(&*data.input).unwrap();

        assert_eq!(
            result,
            data.day.unwrap(),
            "\"day\" failed on type: {:?}",
            data_type
        );
    }
}

#[test]
fn temporal_weekday() {
    for data_type in TestData::available_date_like_types() {
        let data = TestData::data(&data_type);
        let result = weekday(&*data.input).unwrap();

        assert_eq!(
            result,
            data.weekday.unwrap(),
            "\"weekday\" failed on type: {:?}",
            data_type
        );
    }
}

#[test]
fn temporal_iso_week() {
    for data_type in TestData::available_date_like_types() {
        let data = TestData::data(&data_type);
        let result = iso_week(&*data.input).unwrap();

        assert_eq!(
            result,
            data.iso_week.unwrap(),
            "\"iso_week\" failed on type: {:?}",
            data_type
        );
    }
}

struct TestData {
    input: Box<dyn Array>,
    year: Option<Int32Array>,
    month: Option<UInt32Array>,
    day: Option<UInt32Array>,
    weekday: Option<UInt32Array>,
    iso_week: Option<UInt32Array>,
    hour: Option<UInt32Array>,
    minute: Option<UInt32Array>,
    second: Option<UInt32Array>,
    nanosecond: Option<UInt32Array>,
}

impl TestData {
    fn data(data_type: &DataType) -> TestData {
        match data_type {
            DataType::Date64 => TestData {
                input: Box::new(
                    Int64Array::from(&[Some(1514764800000), None, Some(1550636625000)])
                        .to(data_type.clone()),
                ),
                year: Some(Int32Array::from(&[Some(2018), None, Some(2019)])),
                month: Some(UInt32Array::from(&[Some(1), None, Some(2)])),
                day: Some(UInt32Array::from(&[Some(1), None, Some(20)])),
                weekday: Some(UInt32Array::from(&[Some(1), None, Some(3)])),
                iso_week: Some(UInt32Array::from(&[Some(1), None, Some(8)])),
                hour: Some(UInt32Array::from(&[Some(0), None, Some(4)])),
                minute: Some(UInt32Array::from(&[Some(0), None, Some(23)])),
                second: Some(UInt32Array::from(&[Some(0), None, Some(45)])),
                nanosecond: Some(UInt32Array::from(&[Some(0), None, Some(0)])),
            },
            DataType::Date32 => TestData {
                input: Box::new(Int32Array::from(&[Some(15147), None]).to(data_type.clone())),
                year: Some(Int32Array::from(&[Some(2011), None])),
                month: Some(UInt32Array::from(&[Some(6), None])),
                day: Some(UInt32Array::from(&[Some(22), None])),
                weekday: Some(UInt32Array::from(&[Some(3), None])),
                iso_week: Some(UInt32Array::from(&[Some(25), None])),
                hour: Some(UInt32Array::from(&[Some(0), None])),
                minute: Some(UInt32Array::from(&[Some(0), None])),
                second: Some(UInt32Array::from(&[Some(0), None])),
                nanosecond: Some(UInt32Array::from(&[Some(0), None])),
            },
            DataType::Time32(TimeUnit::Second) => TestData {
                input: Box::new(Int32Array::from(&[Some(37800), None]).to(data_type.clone())),
                year: None,
                month: None,
                day: None,
                weekday: None,
                iso_week: None,
                hour: Some(UInt32Array::from(&[Some(10), None])),
                minute: Some(UInt32Array::from(&[Some(30), None])),
                second: Some(UInt32Array::from(&[Some(0), None])),
                nanosecond: Some(UInt32Array::from(&[Some(0), None])),
            },
            DataType::Time64(TimeUnit::Microsecond) => TestData {
                input: Box::new(Int64Array::from(&[Some(378000000), None]).to(data_type.clone())),
                year: None,
                month: None,
                day: None,
                weekday: None,
                iso_week: None,
                hour: Some(UInt32Array::from(&[Some(0), None])),
                minute: Some(UInt32Array::from(&[Some(6), None])),
                second: Some(UInt32Array::from(&[Some(18), None])),
                nanosecond: Some(UInt32Array::from(&[Some(0), None])),
            },
            DataType::Timestamp(TimeUnit::Microsecond, None) => TestData {
                input: Box::new(
                    Int64Array::from(&[Some(1612025847000000), None]).to(data_type.clone()),
                ),
                year: Some(Int32Array::from(&[Some(2021), None])),
                month: Some(UInt32Array::from(&[Some(1), None])),
                day: Some(UInt32Array::from(&[Some(30), None])),
                weekday: Some(UInt32Array::from(&[Some(6), None])),
                iso_week: Some(UInt32Array::from(&[Some(4), None])),
                hour: Some(UInt32Array::from(&[Some(16), None])),
                minute: Some(UInt32Array::from(&[Some(57), None])),
                second: Some(UInt32Array::from(&[Some(27), None])),
                nanosecond: Some(UInt32Array::from(&[Some(0), None])),
            },
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => TestData {
                // NOTE We hardcode the timezone as an offset, we ignore
                // the zone sent through `data_type`
                input: Box::new(Int64Array::from(&[Some(1621877130000000), None]).to(
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+01:00".to_string())),
                )),
                year: Some(Int32Array::from(&[Some(2021), None])),
                month: Some(UInt32Array::from(&[Some(5), None])),
                day: Some(UInt32Array::from(&[Some(24), None])),
                weekday: Some(UInt32Array::from(&[Some(1), None])),
                iso_week: Some(UInt32Array::from(&[Some(21), None])),
                hour: Some(UInt32Array::from(&[Some(18), None])),
                minute: Some(UInt32Array::from(&[Some(25), None])),
                second: Some(UInt32Array::from(&[Some(30), None])),
                nanosecond: Some(UInt32Array::from(&[Some(0), None])),
            },
            _ => todo!(),
        }
    }

    fn available_time_like_types() -> Vec<DataType> {
        vec![
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            // NOTE The timezone value will be ignored
            DataType::Timestamp(TimeUnit::Microsecond, Some("+01:00".to_string())),
        ]
    }

    fn available_date_like_types() -> Vec<DataType> {
        vec![
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            // NOTE The timezone value will be ignored
            DataType::Timestamp(TimeUnit::Microsecond, Some("+01:00".to_string())),
        ]
    }
}

#[cfg(feature = "chrono-tz")]
#[test]
fn timestamp_micro_hour_tz() {
    let timestamp = 1621877130000000; // Mon May 24 2021 17:25:30 GMT+0000
    let array = Int64Array::from(&[Some(timestamp), None]).to(DataType::Timestamp(
        TimeUnit::Microsecond,
        Some("GMT".to_string()),
    ));

    let result = hour(&array).unwrap();
    let expected = UInt32Array::from(&[Some(17), None]);
    assert_eq!(result, expected);

    // (Western European Summer Time in Lisbon) => +1 hour
    let array = Int64Array::from(&[Some(timestamp), None]).to(DataType::Timestamp(
        TimeUnit::Microsecond,
        Some("Europe/Lisbon".to_string()),
    ));

    let result = hour(&array).unwrap();
    let expected = UInt32Array::from(&[Some(18), None]);
    assert_eq!(result, expected);
}

#[test]
fn consistency_hour() {
    consistency_check(can_hour, hour);
}

#[test]
fn consistency_minute() {
    consistency_check(can_minute, minute);
}

#[test]
fn consistency_second() {
    consistency_check(can_second, second);
}

#[test]
fn consistency_nanosecond() {
    consistency_check(can_nanosecond, nanosecond);
}

#[test]
fn consistency_year() {
    consistency_check(can_year, year);
}

#[test]
fn consistency_month() {
    consistency_check(can_month, month);
}

#[test]
fn consistency_day() {
    consistency_check(can_day, day);
}

#[test]
fn consistency_weekday() {
    consistency_check(can_weekday, weekday);
}

#[test]
fn consistency_iso_week() {
    consistency_check(can_iso_week, iso_week);
}

fn consistency_check<O: arrow2::types::NativeType>(
    can_extract: fn(&DataType) -> bool,
    extract: fn(&dyn Array) -> arrow2::error::Result<PrimitiveArray<O>>,
) {
    use arrow2::datatypes::DataType::*;

    let datatypes = vec![
        Null,
        Boolean,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Int8,
        Int16,
        Int32,
        Int64,
        Float32,
        Float64,
        Timestamp(TimeUnit::Second, None),
        Timestamp(TimeUnit::Millisecond, None),
        Timestamp(TimeUnit::Microsecond, None),
        Timestamp(TimeUnit::Nanosecond, None),
        Timestamp(TimeUnit::Nanosecond, Some("+00:00".to_string())),
        Time64(TimeUnit::Microsecond),
        Time64(TimeUnit::Nanosecond),
        Date32,
        Time32(TimeUnit::Second),
        Time32(TimeUnit::Millisecond),
        Date64,
        Utf8,
        LargeUtf8,
        Binary,
        LargeBinary,
        Duration(TimeUnit::Second),
        Duration(TimeUnit::Millisecond),
        Duration(TimeUnit::Microsecond),
        Duration(TimeUnit::Nanosecond),
    ];

    datatypes.into_iter().for_each(|d1| {
        let array = new_null_array(d1.clone(), 10);
        if can_extract(&d1) {
            assert!(extract(array.as_ref()).is_ok());
        } else {
            assert!(extract(array.as_ref()).is_err());
        }
    });
}
