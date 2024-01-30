use crate::array::*;
use crate::bitmap::Bitmap;
use crate::datatypes::DataType;
use crate::error::{Error, Result};
use crate::offset::Offset;
use crate::types::NativeType;

use odbc_api as api;
use odbc_api::buffers::{AnySliceMut, BinColumnSliceMut, NullableSliceMut, TextColumnSliceMut};

/// Serializes an [`Array`] to [`api::buffers::AnyColumnViewMut`]
/// This operation is CPU-bounded
pub fn serialize(array: &dyn Array, column: &mut AnySliceMut) -> Result<()> {
    match array.data_type() {
        DataType::Boolean => {
            if let AnySliceMut::Bit(values) = column {
                bool(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else if let AnySliceMut::NullableBit(values) = column {
                bool_optional(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize bool to non-bool ODBC"))
            }
        }
        DataType::Int16 => {
            if let AnySliceMut::I16(values) = column {
                primitive(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else if let AnySliceMut::NullableI16(values) = column {
                primitive_optional(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize i16 to non-i16 ODBC"))
            }
        }
        DataType::Int32 => {
            if let AnySliceMut::I32(values) = column {
                primitive(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else if let AnySliceMut::NullableI32(values) = column {
                primitive_optional(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize i32 to non-i32 ODBC"))
            }
        }
        DataType::Float32 => {
            if let AnySliceMut::F32(values) = column {
                primitive(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else if let AnySliceMut::NullableF32(values) = column {
                primitive_optional(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize f32 to non-f32 ODBC"))
            }
        }
        DataType::Float64 => {
            if let AnySliceMut::F64(values) = column {
                primitive(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else if let AnySliceMut::NullableF64(values) = column {
                primitive_optional(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize f64 to non-f64 ODBC"))
            }
        }
        DataType::Utf8 => {
            if let AnySliceMut::Text(values) = column {
                utf8::<i32>(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize utf8 to non-text ODBC"))
            }
        }
        DataType::LargeUtf8 => {
            if let AnySliceMut::Text(values) = column {
                utf8::<i64>(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize utf8 to non-text ODBC"))
            }
        }
        DataType::Binary => {
            if let AnySliceMut::Binary(values) = column {
                binary::<i32>(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize utf8 to non-binary ODBC"))
            }
        }
        DataType::LargeBinary => {
            if let AnySliceMut::Binary(values) = column {
                binary::<i64>(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize utf8 to non-text ODBC"))
            }
        }
        DataType::FixedSizeBinary(_) => {
            if let AnySliceMut::Binary(values) = column {
                fixed_binary(array.as_any().downcast_ref().unwrap(), values);
                Ok(())
            } else {
                Err(Error::nyi("serialize fixed to non-binary ODBC"))
            }
        }
        other => Err(Error::nyi(format!("{other:?} to ODBC"))),
    }
}

fn bool(array: &BooleanArray, values: &mut [api::Bit]) {
    array
        .values()
        .iter()
        .zip(values.iter_mut())
        .for_each(|(from, to)| *to = api::Bit(from as u8));
}

fn bool_optional(array: &BooleanArray, values: &mut NullableSliceMut<api::Bit>) {
    let (values, indicators) = values.raw_values();
    array
        .values()
        .iter()
        .zip(values.iter_mut())
        .for_each(|(from, to)| *to = api::Bit(from as u8));
    write_validity(array.validity(), indicators);
}

fn primitive<T: NativeType>(array: &PrimitiveArray<T>, values: &mut [T]) {
    values.copy_from_slice(array.values())
}

fn write_validity(validity: Option<&Bitmap>, indicators: &mut [isize]) {
    if let Some(validity) = validity {
        indicators
            .iter_mut()
            .zip(validity.iter())
            .for_each(|(indicator, is_valid)| *indicator = if is_valid { 0 } else { -1 })
    } else {
        indicators.iter_mut().for_each(|x| *x = 0)
    }
}

fn primitive_optional<T: NativeType>(array: &PrimitiveArray<T>, values: &mut NullableSliceMut<T>) {
    let (values, indicators) = values.raw_values();
    values.copy_from_slice(array.values());
    write_validity(array.validity(), indicators);
}

fn fixed_binary(array: &FixedSizeBinaryArray, writer: &mut BinColumnSliceMut) {
    // Since the length of each elment is identical and fixed as `array.size`,
    // we only need to reallocate and rebind the buffer once.
    writer.ensure_max_element_length(array.size(), 0).unwrap();

    for (row_index, value) in array
        .values()
        .chunks(array.size())
        .collect::<Vec<_>>()
        .iter()
        .enumerate()
    {
        writer.set_cell(row_index, Some(value));
    }
}

fn binary<O: Offset>(array: &BinaryArray<O>, writer: &mut BinColumnSliceMut) {
    // Get the largest length from all the elements

    let max_len = array
        .offsets()
        .buffer()
        .windows(2)
        .map(|x| (x[1] - x[0]).to_usize())
        .max()
        .unwrap_or(0);

    writer.ensure_max_element_length(max_len, 0).unwrap();

    (0..array.offsets().len_proxy()) // loop index of each elements
        .for_each(|row_idx| writer.set_cell(row_idx, array.get(row_idx)));
}

fn utf8<O: Offset>(array: &Utf8Array<O>, writer: &mut TextColumnSliceMut<u8>) {
    let max_len = array
        .offsets()
        .buffer()
        .windows(2)
        .map(|x| (x[1] - x[0]).to_usize())
        .max()
        .unwrap_or(0);
    writer.ensure_max_element_length(max_len, 0).ok();

    (0..array.offsets().len_proxy()) // loop index of each elements
        .for_each(|row_idx| writer.set_cell(row_idx, array.get(row_idx).map(|s| s.as_bytes())));
}
