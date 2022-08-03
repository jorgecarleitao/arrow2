use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};
use arrow2::error::Result;
use arrow2::io::ipc::read::read_file_metadata;

use super::write::file::write;

fn round_trip(array: Box<dyn Array>) -> Result<()> {
    let schema = Schema::from(vec![Field::new("a", array.data_type().clone(), true)]);
    let columns = Chunk::try_new(vec![array.clone()])?;

    let data = write(&[columns], &schema, None, None)?;

    let metadata = read_file_metadata(&mut std::io::Cursor::new(&data))?;

    let new_array = unsafe { arrow2::mmap::mmap_unchecked(&metadata, data, 0)? };
    assert_eq!(new_array.into_arrays()[0], array);
    Ok(())
}

#[test]
fn utf8() -> Result<()> {
    let array = Utf8Array::<i32>::from([None, None, Some("bb")])
        .slice(1, 2)
        .boxed();
    round_trip(array)
}

#[test]
fn primitive() -> Result<()> {
    let array = PrimitiveArray::<i32>::from([None, None, Some(3)])
        .slice(1, 2)
        .boxed();
    round_trip(array)
}

#[test]
fn boolean() -> Result<()> {
    let array = BooleanArray::from([None, None, Some(true)])
        .slice(1, 2)
        .boxed();
    round_trip(array)
}

#[test]
fn list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data).unwrap();
    let array = array.into_box().slice(1, 2);
    round_trip(array)
}
