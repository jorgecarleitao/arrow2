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

make_type!(Int8, i8, DataType::Int8);
make_type!(Int16, i16, DataType::Int16);
make_type!(Int32, i32, DataType::Int32);
make_type!(Int64, i64, DataType::Int64);
make_type!(UInt8, u8, DataType::UInt8);
make_type!(UInt16, u16, DataType::UInt16);
make_type!(UInt32, u32, DataType::UInt32);
make_type!(UInt64, u64, DataType::UInt64);
make_type!(Float32, f32, DataType::Float32);
make_type!(Float64, f64, DataType::Float64);
make_type!(
    TimestampSecond,
    i64,
    DataType::Timestamp(TimeUnit::Second, None)
);
make_type!(
    TimestampMillisecond,
    i64,
    DataType::Timestamp(TimeUnit::Millisecond, None)
);
make_type!(
    TimestampMicrosecond,
    i64,
    DataType::Timestamp(TimeUnit::Microsecond, None)
);
make_type!(
    TimestampNanosecond,
    i64,
    DataType::Timestamp(TimeUnit::Nanosecond, None)
);
make_type!(Date32, i32, DataType::Date32);
make_type!(Date64, i64, DataType::Date64);
make_type!(Time32Second, i32, DataType::Time32(TimeUnit::Second));
make_type!(
    Time32Millisecond,
    i32,
    DataType::Time32(TimeUnit::Millisecond)
);
make_type!(
    Time64Microsecond,
    i64,
    DataType::Time64(TimeUnit::Microsecond)
);
make_type!(
    Time64Nanosecond,
    i64,
    DataType::Time64(TimeUnit::Nanosecond)
);
make_type!(
    IntervalYearMonth,
    i32,
    DataType::Interval(IntervalUnit::YearMonth)
);
make_type!(
    IntervalDayTime,
    i64,
    DataType::Interval(IntervalUnit::DayTime)
);
make_type!(DurationSecond, i64, DataType::Duration(TimeUnit::Second));
make_type!(
    DurationMillisecond,
    i64,
    DataType::Duration(TimeUnit::Millisecond)
);
make_type!(
    DurationMicrosecond,
    i64,
    DataType::Duration(TimeUnit::Microsecond)
);
make_type!(
    DurationNanosecond,
    i64,
    DataType::Duration(TimeUnit::Nanosecond)
);
