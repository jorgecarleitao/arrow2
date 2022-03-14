mod binary;
mod boolean;
mod dictionary;
mod fixed_binary;
mod fixed_size_list;
mod list;
mod null;
mod primitive;
mod struct_;
mod union;
mod utf8;

use arrow2::array::growable::make_growable;
use arrow2::array::*;
use arrow2::datatypes::DataType;

#[test]
fn test_make_growable() {
    let array = Int32Array::from_slice([1, 2]);
    make_growable(&[&array], false, 2);

    let array = Utf8Array::<i32>::from_slice(["a", "aa"]);
    make_growable(&[&array], false, 2);

    let array = Utf8Array::<i64>::from_slice(["a", "aa"]);
    make_growable(&[&array], false, 2);

    let array = BinaryArray::<i32>::from_slice([b"a".as_ref(), b"aa".as_ref()]);
    make_growable(&[&array], false, 2);

    let array = BinaryArray::<i64>::from_slice([b"a".as_ref(), b"aa".as_ref()]);
    make_growable(&[&array], false, 2);

    let array = BinaryArray::<i64>::from_slice([b"a".as_ref(), b"aa".as_ref()]);
    make_growable(&[&array], false, 2);

    let array =
        FixedSizeBinaryArray::new(DataType::FixedSizeBinary(2), b"abcd".to_vec().into(), None);
    make_growable(&[&array], false, 2);

    let array = DictionaryArray::<i32>::from_data(
        Int32Array::from_slice([1, 2]),
        std::sync::Arc::new(Int32Array::from_slice([1, 2])),
    );
    make_growable(&[&array], false, 2);
}
