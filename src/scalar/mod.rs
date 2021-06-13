use std::any::Any;

use crate::{array::*, datatypes::*, types::days_ms};

mod primitive;
pub use primitive::*;
mod utf8;
pub use utf8::*;
mod binary;
pub use binary::*;
mod boolean;
pub use boolean::*;
mod list;
pub use list::*;
mod null;
pub use null::*;

pub trait Scalar: std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn is_valid(&self) -> bool;

    fn data_type(&self) -> &DataType;

    fn to_boxed_array(&self, length: usize) -> Box<dyn Array>;
}

macro_rules! dyn_new {
    ($array:expr, $index:expr, $type:ty) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$type>>()
            .unwrap();
        let value = if array.is_valid($index) {
            Some(array.value($index))
        } else {
            None
        };
        Box::new(PrimitiveScalar::new(array.data_type().clone(), value))
    }};
}

macro_rules! dyn_new_utf8 {
    ($array:expr, $index:expr, $type:ty) => {{
        let array = $array.as_any().downcast_ref::<Utf8Array<$type>>().unwrap();
        let value = if array.is_valid($index) {
            Some(array.value($index))
        } else {
            None
        };
        Box::new(Utf8Scalar::<$type>::new(value))
    }};
}

macro_rules! dyn_new_binary {
    ($array:expr, $index:expr, $type:ty) => {{
        let array = $array
            .as_any()
            .downcast_ref::<BinaryArray<$type>>()
            .unwrap();
        let value = if array.is_valid($index) {
            Some(array.value($index))
        } else {
            None
        };
        Box::new(BinaryScalar::<$type>::new(value))
    }};
}

macro_rules! dyn_new_list {
    ($array:expr, $index:expr, $type:ty) => {{
        let array = $array.as_any().downcast_ref::<ListArray<$type>>().unwrap();
        let value = if array.is_valid($index) {
            ListScalarNew::Array(array.value($index).into())
        } else {
            ListScalarNew::DataType(array.data_type().clone())
        };
        Box::new(ListScalar::<$type>::new(value))
    }};
}

/// creates a new [`Scalar`] from an [`Array`].
pub fn new_scalar(array: &dyn Array, index: usize) -> Box<dyn Scalar> {
    use DataType::*;
    match array.data_type() {
        Null => Box::new(NullScalar::new()),
        Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let value = if array.is_valid(index) {
                Some(array.value(index))
            } else {
                None
            };
            Box::new(BooleanScalar::new(value))
        }
        Int8 => dyn_new!(array, index, i8),
        Int16 => dyn_new!(array, index, i16),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            dyn_new!(array, index, i32)
        }
        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => dyn_new!(array, index, i64),
        Interval(IntervalUnit::DayTime) => dyn_new!(array, index, days_ms),
        UInt8 => dyn_new!(array, index, u8),
        UInt16 => dyn_new!(array, index, u16),
        UInt32 => dyn_new!(array, index, u32),
        UInt64 => dyn_new!(array, index, u64),
        Decimal(_, _) => dyn_new!(array, index, i128),
        Float16 => unreachable!(),
        Float32 => dyn_new!(array, index, f32),
        Float64 => dyn_new!(array, index, f64),
        Utf8 => dyn_new_utf8!(array, index, i32),
        LargeUtf8 => dyn_new_utf8!(array, index, i64),
        Binary => dyn_new_binary!(array, index, i32),
        LargeBinary => dyn_new_binary!(array, index, i64),
        List(_) => dyn_new_list!(array, index, i32),
        LargeList(_) => dyn_new_list!(array, index, i64),
        /*
        FixedSizeBinary(_) => {}
        FixedSizeList(_, _) => {}
        Struct(_) => {}
        Union(_) => {}
        Dictionary(_, _) => {}
         */
        _ => todo!(),
    }
}
