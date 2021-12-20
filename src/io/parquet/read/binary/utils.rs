use crate::{
    array::{Array, BinaryArray, Offset, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::DataType,
};

pub(super) fn finish_array<O: Offset>(
    data_type: DataType,
    offsets: Vec<O>,
    values: Vec<u8>,
    validity: MutableBitmap,
) -> Box<dyn Array> {
    match data_type {
        DataType::LargeBinary | DataType::Binary => Box::new(BinaryArray::from_data(
            data_type,
            offsets.into(),
            values.into(),
            validity.into(),
        )),
        DataType::LargeUtf8 | DataType::Utf8 => Box::new(Utf8Array::from_data(
            data_type,
            offsets.into(),
            values.into(),
            validity.into(),
        )),
        _ => unreachable!(),
    }
}
