use arrow2::array::*;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::{DataType, Field, IntegerType, TimeUnit};
use arrow2::offset::Offsets;
use arrow_array::ArrayRef;
use proptest::num::i32;

fn test_arrow2_roundtrip(array: &dyn arrow_array::Array) {
    let arrow2 = Box::<dyn Array>::from(array);
    assert_eq!(arrow2.len(), array.len());

    let back = ArrayRef::from(arrow2);
    assert_eq!(back.len(), array.len());

    assert_eq!(array, back.as_ref());
    assert_eq!(array.data_type(), back.data_type());
}

fn test_arrow_roundtrip(array: &dyn Array) {
    let arrow = ArrayRef::from(array);
    assert_eq!(arrow.len(), array.len());

    let back = Box::<dyn Array>::from(arrow);
    assert_eq!(back.len(), array.len());

    assert_eq!(array, back.as_ref());
    assert_eq!(array.data_type(), back.data_type());
}

fn test_conversion(array: &dyn Array) {
    test_arrow_roundtrip(array);
    let to_arrow = ArrayRef::from(array);
    test_arrow2_roundtrip(to_arrow.as_ref());

    if !array.is_empty() {
        let sliced = array.sliced(1, array.len() - 1);
        test_arrow_roundtrip(sliced.as_ref());

        let sliced = to_arrow.slice(1, array.len() - 1);
        test_arrow2_roundtrip(sliced.as_ref());
    }

    if array.len() > 2 {
        let sliced = array.sliced(1, array.len() - 2);
        test_arrow_roundtrip(sliced.as_ref());

        let sliced = to_arrow.slice(1, array.len() - 2);
        test_arrow2_roundtrip(sliced.as_ref());
    }
}

#[test]
fn test_primitive() {
    let data_type = DataType::Int32;
    let array = PrimitiveArray::new(data_type, vec![1, 2, 3].into(), None);
    test_conversion(&array);

    let data_type = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
    let nulls = Bitmap::from_iter([true, true, false]);
    let array = PrimitiveArray::new(data_type, vec![1_i64, 24, 0].into(), Some(nulls));
    test_conversion(&array);
}

#[test]
fn test_utf8() {
    let array = Utf8Array::<i32>::from_iter([Some("asd\0"), None, Some("45\0848"), Some("")]);
    test_conversion(&array);

    let array = Utf8Array::<i64>::from_iter([Some("asd"), None, Some("45\n848"), Some("")]);
    test_conversion(&array);

    let array = Utf8Array::<i32>::new_empty(DataType::Utf8);
    test_conversion(&array);
}

#[test]
fn test_binary() {
    let array = BinaryArray::<i32>::from_iter([Some("s".as_bytes()), Some(b"sd\xFFfk\x23"), None]);
    test_conversion(&array);

    let array = BinaryArray::<i64>::from_iter([Some("45848".as_bytes()), Some(b"\x03\xFF"), None]);
    test_conversion(&array);

    let array = BinaryArray::<i32>::new_empty(DataType::Binary);
    test_conversion(&array);
}

/// Returns a 3 element struct array
fn make_struct() -> StructArray {
    let a1 = BinaryArray::<i32>::from_iter([Some("s".as_bytes()), Some(b"sd\xFFfk\x23"), None]);
    let a2 = BinaryArray::<i64>::from_iter([Some("45848".as_bytes()), Some(b"\x03\xFF"), None]);

    let data_type = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
    let nulls = Bitmap::from_iter([true, true, false]);
    let a3 = PrimitiveArray::new(data_type, vec![1_i64, 24, 0].into(), Some(nulls));

    let nulls = [true, true, false].into_iter().collect();
    StructArray::new(
        DataType::Struct(vec![
            Field::new("a1", a1.data_type().clone(), true),
            Field::new("a2", a2.data_type().clone(), true),
            Field::new("a3", a3.data_type().clone(), true),
        ]),
        vec![Box::new(a1), Box::new(a2), Box::new(a3)],
        Some(nulls),
    )
}

#[test]
fn test_struct() {
    let array = make_struct();
    test_conversion(&array);
}

#[test]
fn test_list() {
    let values = Utf8Array::<i32>::from_iter([
        Some("asd\0"),
        None,
        Some("45\0848"),
        Some(""),
        Some("335"),
        Some("test"),
    ]);

    let validity = [true, true, false, false, true].into_iter().collect();
    let offsets = Offsets::try_from_iter(vec![0, 2, 2, 2, 0]).unwrap();
    let list = ListArray::<i32>::new(
        DataType::List(Box::new(Field::new("element", DataType::Utf8, true))),
        offsets.into(),
        Box::new(values),
        Some(validity),
    );

    test_conversion(&list);
}

#[test]
fn test_list_struct() {
    let values = make_struct();
    let validity = [true, true, false, true].into_iter().collect();
    let offsets = Offsets::try_from_iter(vec![0, 1, 0, 2]).unwrap();
    let list = ListArray::<i32>::new(
        DataType::List(Box::new(Field::new(
            "element",
            values.data_type().clone(),
            true,
        ))),
        offsets.into(),
        Box::new(values),
        Some(validity),
    );

    test_conversion(&list);
}

#[test]
fn test_dictionary() {
    let nulls = [true, false, true, true, true].into_iter().collect();
    let keys = PrimitiveArray::new(DataType::Int16, vec![1_i16, 1, 0, 2, 2].into(), Some(nulls));
    let values = make_struct();
    let dictionary = DictionaryArray::try_new(
        DataType::Dictionary(
            IntegerType::Int16,
            Box::new(values.data_type().clone()),
            false,
        ),
        keys,
        Box::new(values),
    )
    .unwrap();

    test_conversion(&dictionary);
}

#[test]
fn test_fixed_size_binary() {
    let data = (0_u8..16).collect::<Vec<_>>();
    let nulls = [false, false, true, true, true, false, false, true]
        .into_iter()
        .collect();

    let array = FixedSizeBinaryArray::new(DataType::FixedSizeBinary(2), data.into(), Some(nulls));
    test_conversion(&array);
}

#[test]
fn test_fixed_size_list() {
    let values = vec![1_i64, 2, 3, 4, 5, 6, 7, 8];
    let nulls = [false, false, true, true, true, true, false, false]
        .into_iter()
        .collect();
    let values = PrimitiveArray::new(DataType::Int64, values.into(), Some(nulls));

    let nulls = [true, true, false, true].into_iter().collect();
    let array = FixedSizeListArray::new(
        DataType::FixedSizeList(Box::new(Field::new("element", DataType::Int64, true)), 2),
        Box::new(values),
        Some(nulls),
    );

    test_conversion(&array);
}
