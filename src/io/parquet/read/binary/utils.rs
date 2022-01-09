use crate::{
    array::{Array, BinaryArray, Offset, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::ArrowError,
};

pub(super) fn finish_array<O: Offset>(
    data_type: DataType,
    offsets: Vec<O>,
    values: Vec<u8>,
    validity: Option<MutableBitmap>,
) -> Result<Box<dyn Array>, ArrowError> {
    match data_type {
        DataType::LargeBinary | DataType::Binary => BinaryArray::try_new(
            data_type,
            offsets.into(),
            values.into(),
            validity.map(|x| x.into()),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        DataType::LargeUtf8 | DataType::Utf8 => Utf8Array::try_new(
            data_type,
            offsets.into(),
            values.into(),
            validity.map(|x| x.into()),
        )
        .map(|x| Box::new(x) as Box<dyn Array>),
        _ => unreachable!(),
    }
}
