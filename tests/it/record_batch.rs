use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::record_batch::RecordBatch;

#[test]
fn basic() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]);

    let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
    let b = Utf8Array::<i32>::from_slice(&["a", "b", "c", "d", "e"]);

    let record_batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();
    check_batch(record_batch)
}

fn check_batch(record_batch: RecordBatch) {
    assert_eq!(5, record_batch.num_rows());
    assert_eq!(2, record_batch.num_columns());
    assert_eq!(&DataType::Int32, record_batch.schema().field(0).data_type());
    assert_eq!(&DataType::Utf8, record_batch.schema().field(1).data_type());
    assert_eq!(5, record_batch.column(0).len());
    assert_eq!(5, record_batch.column(1).len());
}

#[test]
fn try_from_iter() {
    let a: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        None,
        Some(4),
        Some(5),
    ]));
    let b: ArrayRef = Arc::new(Utf8Array::<i32>::from_slice(&["a", "b", "c", "d", "e"]));

    let record_batch =
        RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).expect("valid conversion");

    let expected_schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, false),
    ]);
    assert_eq!(record_batch.schema().as_ref(), &expected_schema);
    check_batch(record_batch);
}

#[test]
fn try_from_iter_with_nullable() {
    let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 3, 4, 5]));
    let b: ArrayRef = Arc::new(Utf8Array::<i32>::from_slice(&["a", "b", "c", "d", "e"]));

    // Note there are no nulls in a or b, but we specify that b is nullable
    let record_batch =
        RecordBatch::try_from_iter_with_nullable(vec![("a", a, false), ("b", b, true)])
            .expect("valid conversion");

    let expected_schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    assert_eq!(record_batch.schema().as_ref(), &expected_schema);
    check_batch(record_batch);
}

#[test]
fn type_mismatch() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let a = Int64Array::from_slice(&[1, 2, 3, 4, 5]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]);
    assert!(batch.is_err());
}

#[test]
fn number_of_fields_mismatch() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
    let b = Int32Array::from_slice(&[1, 2, 3, 4, 5]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]);
    assert!(batch.is_err());
}

#[test]
fn from_struct_array() {
    let boolean = Arc::new(BooleanArray::from_slice(&[false, false, true, true])) as ArrayRef;
    let int = Arc::new(Int32Array::from_slice(&[42, 28, 19, 31])) as ArrayRef;
    let struct_array = StructArray::from_data(
        vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ],
        vec![boolean.clone(), int.clone()],
        None,
    );

    let batch = RecordBatch::from(&struct_array);
    assert_eq!(2, batch.num_columns());
    assert_eq!(4, batch.num_rows());
    assert_eq!(
        struct_array.data_type(),
        &DataType::Struct(batch.schema().fields().to_vec())
    );
    assert_eq!(boolean.as_ref(), batch.column(0).as_ref());
    assert_eq!(int.as_ref(), batch.column(1).as_ref());
}
