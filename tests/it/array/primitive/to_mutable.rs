use arrow2::array::PrimitiveArray;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType;
use arrow2::to_mutable::MaybeMut;

#[test]
fn array_to_mutable() {
    let data = vec![1, 2, 3];
    let arr = PrimitiveArray::from_data(DataType::Int32, data.into(), None);

    // to mutable push and freeze again
    let mut mut_arr = arr.into_mutable().unwrap_mut();
    mut_arr.push(Some(5));
    let immut: PrimitiveArray<i32> = mut_arr.into();
    assert_eq!(immut.values().as_slice(), [1, 2, 3, 5]);

    // let's cause a realloc and see if miri is ok
    let mut mut_arr = immut.into_mutable().unwrap_mut();
    mut_arr.extend_constant(256, Some(9));
    let immut: PrimitiveArray<i32> = mut_arr.into();
    assert_eq!(immut.values().len(), 256 + 4);
}

#[test]
fn array_to_mutable_not_owned() {
    let data = vec![1, 2, 3];
    let arr = PrimitiveArray::from_data(DataType::Int32, data.into(), None);
    let arr2 = arr.clone();

    // to the `to_mutable` should fail and we should get back the original array
    match arr2.into_mutable() {
        MaybeMut::Immutable(arr2) => {
            assert_eq!(arr, arr2);
        }
        _ => panic!(),
    }
}

#[test]
fn array_to_mutable_validity() {
    let data = vec![1, 2, 3];

    // both have a single reference should be ok
    let bitmap = Bitmap::from_iter([true, false, true]);
    let arr = PrimitiveArray::from_data(DataType::Int32, data.clone().into(), Some(bitmap));
    assert!(matches!(arr.into_mutable(), MaybeMut::Mutable(_)));

    // now we clone the bitmap increasing the ref count
    let bitmap = Bitmap::from_iter([true, false, true]);
    let arr = PrimitiveArray::from_data(DataType::Int32, data.into(), Some(bitmap.clone()));
    assert!(matches!(arr.into_mutable(), MaybeMut::Immutable(_)));
}
