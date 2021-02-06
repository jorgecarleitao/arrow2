use crate::buffers::types::NativeType;

use super::{DataType, IntervalUnit, TimeUnit};

/// Trait bridging the dynamic-typed nature of Arrow (via [`DataType`]) with the
/// static-typed nature of rust types ([`NativeType`]) for all types that implement [`NativeType`].
pub trait PrimitiveType: std::fmt::Debug + 'static {
    /// Corresponding Rust native type for the primitive type.
    type Native: NativeType;

    /// the corresponding Arrow data type of this primitive type.
    const DATA_TYPE: DataType;
}

// BooleanType is special: its bit-width is not the size of the primitive type, and its `index`
// operation assumes bit-packing.
#[derive(Debug)]
pub struct BooleanType {}

impl BooleanType {
    pub const DATA_TYPE: DataType = DataType::Boolean;
}

macro_rules! make_type {
    ($name:ident, $native_ty:ty, $data_ty:expr) => {
        #[derive(Debug)]
        pub struct $name {}

        impl PrimitiveType for $name {
            type Native = $native_ty;
            const DATA_TYPE: DataType = $data_ty;
        }
    };
}

make_type!(Int8Type, i8, DataType::Int8);
make_type!(Int16Type, i16, DataType::Int16);
make_type!(Int32Type, i32, DataType::Int32);
make_type!(Int64Type, i64, DataType::Int64);
make_type!(UInt8Type, u8, DataType::UInt8);
make_type!(UInt16Type, u16, DataType::UInt16);
make_type!(UInt32Type, u32, DataType::UInt32);
make_type!(UInt64Type, u64, DataType::UInt64);
make_type!(Float32Type, f32, DataType::Float32);
make_type!(Float64Type, f64, DataType::Float64);
make_type!(
    TimestampSecondType,
    i64,
    DataType::Timestamp(TimeUnit::Second, None)
);
make_type!(
    TimestampMillisecondType,
    i64,
    DataType::Timestamp(TimeUnit::Millisecond, None)
);
make_type!(
    TimestampMicrosecondType,
    i64,
    DataType::Timestamp(TimeUnit::Microsecond, None)
);
make_type!(
    TimestampNanosecondType,
    i64,
    DataType::Timestamp(TimeUnit::Nanosecond, None)
);
make_type!(Date32Type, i32, DataType::Date32);
make_type!(Date64Type, i64, DataType::Date64);
make_type!(Time32SecondType, i32, DataType::Time32(TimeUnit::Second));
make_type!(
    Time32MillisecondType,
    i32,
    DataType::Time32(TimeUnit::Millisecond)
);
make_type!(
    Time64MicrosecondType,
    i64,
    DataType::Time64(TimeUnit::Microsecond)
);
make_type!(
    Time64NanosecondType,
    i64,
    DataType::Time64(TimeUnit::Nanosecond)
);
make_type!(
    IntervalYearMonthType,
    i32,
    DataType::Interval(IntervalUnit::YearMonth)
);
make_type!(
    IntervalDayTimeType,
    i64,
    DataType::Interval(IntervalUnit::DayTime)
);
make_type!(
    DurationSecondType,
    i64,
    DataType::Duration(TimeUnit::Second)
);
make_type!(
    DurationMillisecondType,
    i64,
    DataType::Duration(TimeUnit::Millisecond)
);
make_type!(
    DurationMicrosecondType,
    i64,
    DataType::Duration(TimeUnit::Microsecond)
);
make_type!(
    DurationNanosecondType,
    i64,
    DataType::Duration(TimeUnit::Nanosecond)
);
