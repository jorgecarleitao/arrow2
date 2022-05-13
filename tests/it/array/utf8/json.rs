use arrow2::array::{Array, Utf8Array, StructArray};
use arrow2::bitmap::Bitmap;

#[test]
fn json_deserialize() {
    let array = Utf8Array::<i64>::from([
        Some(r#"{"a": 1, "b": [{"c": 0}, {"c": 1}]}"#),
        None,
        Some(r#"{"a": 2, "b": [{"c": 2}, {"c": 5}]}"#),
        None,
    ]);
    let data_type = array.json_infer(None).unwrap();
    let new_array = array.json_deserialize(data_type).unwrap();

    // Explicitly cast as StructArray
    let new_array = new_array.as_any().downcast_ref::<StructArray>().unwrap();

    assert_eq!(array.len(), new_array.len());
    assert_eq!(array.null_count(), new_array.null_count());
    assert_eq!(array.validity().unwrap(), new_array.validity().unwrap());

    let field_names: Vec<String> = new_array.fields().iter().map(|f| f.name.clone()).collect();
    assert_eq!(field_names, vec!["a".to_string(), "b".to_string()]);
}
