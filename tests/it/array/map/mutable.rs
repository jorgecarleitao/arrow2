use arrow2::{
    array::{MapArray, MutableArray, MutableMapArray, MutableUtf8Array},
    datatypes::{DataType, Field},
};

#[test]
fn basics() {
    let dt = DataType::Struct(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let data_type = DataType::Map(Box::new(Field::new("a", dt.clone(), true)), false);

    let values = vec![
        Box::new(MutableUtf8Array::<i32>::new()) as Box<dyn MutableArray>,
        Box::new(MutableUtf8Array::<i32>::new()) as Box<dyn MutableArray>,
    ];

    let mut array = MutableMapArray::try_new(data_type, values).unwrap();
    assert_eq!(array.len(), 0);

    let field = array.mut_field();
    field
        .value::<MutableUtf8Array<i32>>(0)
        .unwrap()
        .extend([Some("a"), Some("aa"), Some("aaa")]);
    field
        .value::<MutableUtf8Array<i32>>(1)
        .unwrap()
        .extend([Some("b"), Some("bb"), Some("bbb")]);
    array.try_push_valid().unwrap();
    assert_eq!(array.len(), 1);

    array.keys::<MutableUtf8Array<i32>>().push(Some("foo"));
    array.values::<MutableUtf8Array<i32>>().push(Some("bar"));
    array.try_push_valid().unwrap();
    assert_eq!(array.len(), 2);

    let map: MapArray = array.into();
    dbg!(map);
}

#[test]
fn failure() {
    let dt = DataType::Struct(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("extra", DataType::Utf8, true),
    ]);
    let data_type = DataType::Map(Box::new(Field::new("item", dt.clone(), true)), false);

    let values = vec![
        Box::new(MutableUtf8Array::<i32>::new()) as Box<dyn MutableArray>,
        Box::new(MutableUtf8Array::<i32>::new()) as Box<dyn MutableArray>,
    ];

    assert!(matches!(
        MutableMapArray::try_new(data_type, values),
        Err(arrow2::error::Error::InvalidArgumentError(_))
    ));
}
