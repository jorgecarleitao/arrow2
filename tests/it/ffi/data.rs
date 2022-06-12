use arrow2::array::*;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::{DataType, Field, TimeUnit};
use arrow2::{error::Result, ffi};
use std::collections::BTreeMap;

fn _test_round_trip(array: Box<dyn Array>, expected: Box<dyn Array>) -> Result<()> {
    let field = Field::new("a", array.data_type().clone(), true);

    let array_ptr = Box::new(ffi::ArrowArray::empty());
    let schema_ptr = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = Box::into_raw(array_ptr);
    let schema_ptr = Box::into_raw(schema_ptr);

    unsafe {
        ffi::export_array_to_c(array, array_ptr);
        ffi::export_field_to_c(&field, schema_ptr);
    }

    let array_ptr = unsafe { Box::from_raw(array_ptr) };
    let schema_ptr = unsafe { Box::from_raw(schema_ptr) };

    // import references
    let result_field = unsafe { ffi::import_field_from_c(schema_ptr.as_ref())? };
    let result_array =
        unsafe { ffi::import_array_from_c(array_ptr, result_field.data_type.clone())? };

    assert_eq!(&result_array, &expected);
    assert_eq!(result_field, field);
    Ok(())
}

fn test_round_trip(expected: impl Array + Clone + 'static) -> Result<()> {
    let array: Box<dyn Array> = Box::new(expected.clone());
    let expected = Box::new(expected) as Box<dyn Array>;
    _test_round_trip(array.clone(), clone(expected.as_ref()))?;

    // sliced
    _test_round_trip(array.slice(1, 2), expected.slice(1, 2))
}

fn test_round_trip_schema(field: Field) -> Result<()> {
    // create a `InternalArrowArray` from the data.
    let schema_ptr = Box::new(ffi::ArrowSchema::empty());

    let schema_ptr = Box::into_raw(schema_ptr);

    unsafe { ffi::export_field_to_c(&field, schema_ptr) };

    let schema_ptr = unsafe { Box::from_raw(schema_ptr) };

    let result = unsafe { ffi::import_field_from_c(schema_ptr.as_ref())? };

    assert_eq!(result, field);
    Ok(())
}

#[test]
fn bool_nullable() -> Result<()> {
    let data = BooleanArray::from(&[Some(true), None, Some(false), None]);
    test_round_trip(data)
}

#[test]
fn bool() -> Result<()> {
    let data = BooleanArray::from_slice(&[true, true, false]);
    test_round_trip(data)
}

#[test]
fn bool_nullable_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).slice(1, 3);
    let data = BooleanArray::try_new(DataType::Boolean, [true, true, false].into(), Some(bitmap))?;
    test_round_trip(data)
}

#[test]
fn u32_nullable() -> Result<()> {
    let data = Int32Array::from(&[Some(2), None, Some(1), None]);
    test_round_trip(data)
}

#[test]
fn u32() -> Result<()> {
    let data = Int32Array::from_slice(&[2, 0, 1, 0]);
    test_round_trip(data)
}

#[test]
fn u32_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).slice(1, 3);
    let data = Int32Array::try_new(DataType::Int32, vec![1, 2, 3].into(), Some(bitmap))?;
    test_round_trip(data)
}

#[test]
fn decimal() -> Result<()> {
    let data = Int128Array::from_slice(&[1, 0, 2, 0]);
    test_round_trip(data)
}

#[test]
fn decimal_nullable() -> Result<()> {
    let data = Int128Array::from(&[Some(1), None, Some(2), None]);
    test_round_trip(data)
}

#[test]
fn timestamp_tz() -> Result<()> {
    let data = Int64Array::from(&vec![Some(2), None, None]).to(DataType::Timestamp(
        TimeUnit::Second,
        Some("UTC".to_string()),
    ));
    test_round_trip(data)
}

#[test]
fn utf8_nullable() -> Result<()> {
    let data = Utf8Array::<i32>::from(&vec![Some("a"), None, Some("bb"), None]);
    test_round_trip(data)
}

#[test]
fn utf8() -> Result<()> {
    let data = Utf8Array::<i32>::from_slice(&["a", "", "bb", ""]);
    test_round_trip(data)
}

#[test]
fn utf8_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).slice(1, 3);
    let data = Utf8Array::<i32>::try_new(
        DataType::Utf8,
        vec![0, 1, 1, 2].into(),
        b"ab".to_vec().into(),
        Some(bitmap),
    )?;
    test_round_trip(data)
}

#[test]
fn large_utf8() -> Result<()> {
    let data = Utf8Array::<i64>::from(&vec![Some("a"), None, Some("bb"), None]);
    test_round_trip(data)
}

#[test]
fn binary_nullable() -> Result<()> {
    let data =
        BinaryArray::<i32>::from(&vec![Some(b"a".as_ref()), None, Some(b"bb".as_ref()), None]);
    test_round_trip(data)
}

#[test]
fn binary() -> Result<()> {
    let data = BinaryArray::<i32>::from_slice(&[b"a".as_ref(), b"", b"bb", b""]);
    test_round_trip(data)
}

#[test]
fn binary_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).slice(1, 3);
    let data = BinaryArray::<i32>::try_new(
        DataType::Binary,
        vec![0, 1, 1, 2].into(),
        b"ab".to_vec().into(),
        Some(bitmap),
    )?;
    test_round_trip(data)
}

#[test]
fn large_binary() -> Result<()> {
    let data =
        BinaryArray::<i64>::from(&vec![Some(b"a".as_ref()), None, Some(b"bb".as_ref()), None]);
    test_round_trip(data)
}

#[test]
fn fixed_size_binary() -> Result<()> {
    let data = FixedSizeBinaryArray::new(
        DataType::FixedSizeBinary(2),
        vec![1, 2, 3, 4, 5, 6].into(),
        None,
    );
    test_round_trip(data)
}

#[test]
fn fixed_size_binary_nullable() -> Result<()> {
    let data = FixedSizeBinaryArray::new(
        DataType::FixedSizeBinary(2),
        vec![1, 2, 3, 4, 5, 6].into(),
        Some([true, true, false].into()),
    );
    test_round_trip(data)
}

#[test]
fn fixed_size_binary_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).slice(1, 3);
    let data = FixedSizeBinaryArray::try_new(
        DataType::FixedSizeBinary(2),
        b"ababab".to_vec().into(),
        Some(bitmap),
    )?;
    test_round_trip(data)
}

#[test]
fn list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data)?;

    let array: ListArray<i32> = array.into();

    test_round_trip(array)
}

#[test]
fn list_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).slice(1, 3);

    let array = ListArray::<i32>::try_new(
        DataType::List(Box::new(Field::new("a", DataType::Int32, true))),
        vec![0, 1, 1, 2].into(),
        Box::new(PrimitiveArray::<i32>::from_vec(vec![1, 2])),
        Some(bitmap),
    )?;

    test_round_trip(array)
}

#[test]
fn large_list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i64, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data)?;

    let array: ListArray<i64> = array.into();

    test_round_trip(array)
}

#[test]
fn fixed_size_list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableFixedSizeListArray::new(MutablePrimitiveArray::<i32>::new(), 3);
    array.try_extend(data)?;

    let array: FixedSizeListArray = array.into();

    test_round_trip(array)
}

#[test]
fn fixed_size_list_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).slice(1, 3);

    let array = FixedSizeListArray::try_new(
        DataType::FixedSizeList(Box::new(Field::new("a", DataType::Int32, true)), 2),
        Box::new(PrimitiveArray::<i32>::from_vec(vec![1, 2, 3, 4, 5, 6])),
        Some(bitmap),
    )?;

    test_round_trip(array)
}

#[test]
fn list_list() -> Result<()> {
    let data = vec![
        Some(vec![
            Some(vec![None]),
            Some(vec![Some(2)]),
            Some(vec![Some(3)]),
        ]),
        None,
        Some(vec![Some(vec![Some(4), None, Some(6)])]),
    ];

    let mut array =
        MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i32>>>::new();
    array.try_extend(data)?;

    let array: ListArray<i32> = array.into();

    test_round_trip(array)
}

#[test]
fn struct_() -> Result<()> {
    let data_type = DataType::Struct(vec![Field::new("a", DataType::Int32, true)]);
    let values = vec![Int32Array::from([Some(1), None, Some(3)]).boxed()];
    let validity = Bitmap::from([true, false, true]);

    let array = StructArray::from_data(data_type, values, validity.into());

    test_round_trip(array)
}

#[test]
fn dict() -> Result<()> {
    let data = vec![Some("a"), Some("a"), None, Some("b")];

    let mut array = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    array.try_extend(data)?;

    let array: DictionaryArray<i32> = array.into();

    test_round_trip(array)
}

#[test]
fn schema() -> Result<()> {
    let field = Field::new(
        "a",
        DataType::List(Box::new(Field::new("a", DataType::UInt32, true))),
        true,
    );
    test_round_trip_schema(field)?;

    let field = Field::new(
        "a",
        DataType::Dictionary(u32::KEY_TYPE, Box::new(DataType::Utf8), false),
        true,
    );
    test_round_trip_schema(field)?;

    let field = Field::new("a", DataType::Int32, true);
    let mut metadata = BTreeMap::new();
    metadata.insert("some".to_string(), "stuff".to_string());
    let field = field.with_metadata(metadata);
    test_round_trip_schema(field)
}

#[test]
fn extension() -> Result<()> {
    let field = Field::new(
        "a",
        DataType::Extension(
            "a".to_string(),
            Box::new(DataType::Int32),
            Some("bla".to_string()),
        ),
        true,
    );
    test_round_trip_schema(field)
}
