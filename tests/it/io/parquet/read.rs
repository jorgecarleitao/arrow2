use std::fs::File;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::error::Result;
use arrow2::io::parquet::read::*;

use super::*;

fn test_pyarrow_integration(
    column: usize,
    version: usize,
    type_: &str,
    use_dict: bool,
    required: bool,
) -> Result<()> {
    if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
        return Ok(());
    }
    let use_dict = if use_dict { "dict/" } else { "" };
    let path = if required {
        format!(
            "fixtures/pyarrow3/v{}/{}{}_{}_10.parquet",
            version, use_dict, type_, "required"
        )
    } else {
        format!(
            "fixtures/pyarrow3/v{}/{}{}_{}_10.parquet",
            version, use_dict, type_, "nullable"
        )
    };
    let mut file = File::open(path).unwrap();
    let (array, statistics) = read_column(&mut file, 0, column)?;

    let expected = match (type_, required) {
        ("basic", true) => pyarrow_required(column),
        ("basic", false) => pyarrow_nullable(column),
        ("nested", false) => pyarrow_nested_nullable(column),
        _ => unreachable!(),
    };

    let expected_statistics = match (type_, required) {
        ("basic", true) => pyarrow_required_statistics(column),
        ("basic", false) => pyarrow_nullable_statistics(column),
        ("nested", false) => pyarrow_nested_nullable_statistics(column),
        _ => unreachable!(),
    };

    assert_eq!(expected.as_ref(), array.as_ref());
    assert_eq!(expected_statistics, statistics);

    Ok(())
}

#[test]
fn v1_int64_nullable() -> Result<()> {
    test_pyarrow_integration(0, 1, "basic", false, false)
}

#[test]
fn v1_int64_required() -> Result<()> {
    test_pyarrow_integration(0, 1, "basic", false, true)
}

#[test]
fn v1_float64_nullable() -> Result<()> {
    test_pyarrow_integration(1, 1, "basic", false, false)
}

#[test]
fn v1_utf8_nullable() -> Result<()> {
    test_pyarrow_integration(2, 1, "basic", false, false)
}

#[test]
fn v1_utf8_required() -> Result<()> {
    test_pyarrow_integration(2, 1, "basic", false, true)
}

#[test]
fn v1_boolean_nullable() -> Result<()> {
    test_pyarrow_integration(3, 1, "basic", false, false)
}

#[test]
fn v1_boolean_required() -> Result<()> {
    test_pyarrow_integration(3, 1, "basic", false, true)
}

#[test]
fn v1_timestamp_nullable() -> Result<()> {
    test_pyarrow_integration(4, 1, "basic", false, false)
}

#[test]
#[ignore] // pyarrow issue; see https://issues.apache.org/jira/browse/ARROW-12201
fn v1_u32_nullable() -> Result<()> {
    test_pyarrow_integration(5, 1, "basic", false, false)
}

#[test]
fn v2_int64_nullable() -> Result<()> {
    test_pyarrow_integration(0, 2, "basic", false, false)
}

#[test]
fn v2_int64_nullable_dict() -> Result<()> {
    test_pyarrow_integration(0, 2, "basic", true, false)
}

#[test]
fn v1_int64_nullable_dict() -> Result<()> {
    test_pyarrow_integration(0, 1, "basic", true, false)
}

#[test]
fn v2_utf8_nullable() -> Result<()> {
    test_pyarrow_integration(2, 2, "basic", false, false)
}

#[test]
fn v2_utf8_required() -> Result<()> {
    test_pyarrow_integration(2, 2, "basic", false, true)
}

#[test]
fn v2_utf8_nullable_dict() -> Result<()> {
    test_pyarrow_integration(2, 2, "basic", true, false)
}

#[test]
fn v1_utf8_nullable_dict() -> Result<()> {
    test_pyarrow_integration(2, 1, "basic", true, false)
}

#[test]
fn v2_boolean_nullable() -> Result<()> {
    test_pyarrow_integration(3, 2, "basic", false, false)
}

#[test]
fn v2_boolean_required() -> Result<()> {
    test_pyarrow_integration(3, 2, "basic", false, true)
}

#[test]
fn v2_nested_int64_nullable() -> Result<()> {
    test_pyarrow_integration(0, 2, "nested", false, false)
}

#[test]
fn v1_nested_int64_nullable() -> Result<()> {
    test_pyarrow_integration(0, 1, "nested", false, false)
}

#[test]
fn v2_nested_int64_nullable_required() -> Result<()> {
    test_pyarrow_integration(1, 2, "nested", false, false)
}

#[test]
fn v1_nested_int64_nullable_required() -> Result<()> {
    test_pyarrow_integration(1, 1, "nested", false, false)
}

#[test]
fn v2_nested_int64_required_required() -> Result<()> {
    test_pyarrow_integration(2, 2, "nested", false, false)
}

#[test]
fn v1_nested_int64_required_required() -> Result<()> {
    test_pyarrow_integration(2, 1, "nested", false, false)
}

#[test]
fn v2_nested_i16() -> Result<()> {
    test_pyarrow_integration(3, 2, "nested", false, false)
}

#[test]
fn v1_nested_i16() -> Result<()> {
    test_pyarrow_integration(3, 1, "nested", false, false)
}

#[test]
fn v2_nested_bool() -> Result<()> {
    test_pyarrow_integration(4, 2, "nested", false, false)
}

#[test]
fn v1_nested_bool() -> Result<()> {
    test_pyarrow_integration(4, 1, "nested", false, false)
}

#[test]
fn v2_nested_utf8() -> Result<()> {
    test_pyarrow_integration(5, 2, "nested", false, false)
}

#[test]
fn v1_nested_utf8() -> Result<()> {
    test_pyarrow_integration(5, 1, "nested", false, false)
}

#[test]
fn v2_nested_large_binary() -> Result<()> {
    test_pyarrow_integration(6, 2, "nested", false, false)
}

#[test]
fn v1_nested_large_binary() -> Result<()> {
    test_pyarrow_integration(6, 1, "nested", false, false)
}

/*#[test]
fn v2_nested_nested() {
    let _ = test_pyarrow_integration(7, 1, "nested",false, false);
}*/

#[test]
fn all_types() -> Result<()> {
    let path = "testing/parquet-testing/data/alltypes_plain.parquet";
    let reader = std::fs::File::open(path)?;

    let reader = RecordReader::try_new(reader, None, None, Arc::new(|_, _| true), None)?;

    let batches = reader.collect::<Result<Vec<_>>>()?;
    assert_eq!(batches.len(), 1);

    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(result, &Int32Array::from_slice([4, 5, 6, 7, 2, 3, 0, 1]));

    let result = batches[0]
        .column(6)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(
        result,
        &Float32Array::from_slice([0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1])
    );

    let result = batches[0]
        .column(9)
        .as_any()
        .downcast_ref::<BinaryArray<i32>>()
        .unwrap();
    assert_eq!(
        result,
        &BinaryArray::<i32>::from_slice([[48], [49], [48], [49], [48], [49], [48], [49]])
    );

    Ok(())
}
