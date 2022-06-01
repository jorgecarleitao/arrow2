use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::json::read;

#[test]
fn read_json() -> Result<()> {
    let data = br#"[
        {
            "a": 1
        },
        {
            "a": 2
        },
        {
            "a": 3
        }
    ]"#;

    let json = json_deserializer::parse(data)?;

    let data_type = read::infer(&json)?;

    let result = read::deserialize(&json, data_type)?;

    let expected = StructArray::from_data(
        DataType::Struct(vec![Field::new("a", DataType::Int64, true)]),
        vec![Box::new(Int64Array::from_slice([1, 2, 3])) as _],
        None,
    );

    assert_eq!(expected, result.as_ref());

    Ok(())
}
