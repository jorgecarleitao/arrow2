//! Metadata declarations such as [`DataType`], [`Field`] and [`Schema`].
mod field;
mod physical_type;
mod schema;

pub use field::Field;
pub use physical_type::*;
pub use schema::Schema;

pub(crate) use field::{get_extension, Extension, Metadata};

/// The set of supported logical types.
/// Each variant uniquely identifies a logical type, which define specific semantics to the data (e.g. how it should be represented).
/// A [`DataType`] has an unique corresponding [`PhysicalType`], obtained via [`DataType::to_physical_type`],
/// which uniquely identifies an in-memory representation of data.
/// The [`DataType::Extension`] is special in that it augments a [`DataType`] with metadata to support custom types.
/// Use `to_logical_type` to desugar such type and return its correspoding logical type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Null type
    Null,
    /// `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// A timestamp with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a 64-bit integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    Timestamp(TimeUnit, Option<String>),
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits).
    Date32,
    /// A 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds (64 bits). Values are evenly divisible by 86400000.
    Date64,
    /// A 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    Time32(TimeUnit),
    /// A 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    Time64(TimeUnit),
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    /// A "calendar" interval which models types that don't necessarily
    /// have a precise duration without the context of a base timestamp (e.g.
    /// days can differ in length during day light savings time transitions).
    Interval(IntervalUnit),
    /// Opaque binary data of variable length.
    Binary,
    /// Opaque binary data of fixed size.
    /// Enum parameter specifies the number of bytes per value.
    FixedSizeBinary(i32),
    /// Opaque binary data of variable length and 64-bit offsets.
    LargeBinary,
    /// A variable-length string in Unicode with UTF-8 encoding.
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    LargeUtf8,
    /// A list of some logical data type with variable length.
    List(Box<Field>),
    /// A list of some logical data type with fixed length.
    FixedSizeList(Box<Field>, i32),
    /// A list of some logical data type with variable length and 64-bit offsets.
    LargeList(Box<Field>),
    /// A nested datatype that contains a number of sub-fields.
    Struct(Vec<Field>),
    /// A nested datatype that can represent slots of differing types.
    /// Third argument represents sparsness
    Union(Vec<Field>, Option<Vec<i32>>, bool),
    /// A dictionary encoded array (`key_type`, `value_type`), where
    /// each array element is an index of `key_type` into an
    /// associated dictionary of `value_type`.
    ///
    /// Dictionary arrays are used to store columns of `value_type`
    /// that contain many repeated values using less memory, but with
    /// a higher CPU overhead for some operations.
    ///
    /// This type mostly used to represent low cardinality string
    /// arrays or a limited set of primitive types as integers.
    Dictionary(Box<DataType>, Box<DataType>),
    /// Decimal value with precision and scale
    /// precision is the number of digits in the number and
    /// scale is the number of decimal places.
    /// The number 999.99 has a precision of 5 and scale of 2.
    Decimal(usize, usize),
    /// Extension type.
    Extension(String, Box<DataType>, Option<String>),
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Time units defined in Arrow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    /// Time in seconds.
    Second,
    /// Time in milliseconds.
    Millisecond,
    /// Time in microseconds.
    Microsecond,
    /// Time in nanoseconds.
    Nanosecond,
}

/// Interval units defined in Arrow
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntervalUnit {
    /// Indicates the number of elapsed whole months, stored as 4-byte integers.
    YearMonth,
    /// Indicates the number of elapsed days and milliseconds,
    /// stored as 2 contiguous 32-bit integers (8-bytes in total).
    DayTime,
    /// The values are stored contiguously in 16 byte blocks. Months and
    /// days are encoded as 32 bit integers and nanoseconds is encoded as a
    /// 64 bit integer. All integers are signed. Each field is independent
    /// (e.g. there is no constraint that nanoseconds have the same sign
    /// as days or that the quantitiy of nanoseconds represents less
    /// then a day's worth of time).
    MonthDayNano,
}

impl DataType {
    /// Compares the datatype with another, ignoring nested field names
    /// and metadata.
    pub(crate) fn equals_datatype(&self, other: &DataType) -> bool {
        match (&self, other) {
            (DataType::List(a), DataType::List(b))
            | (DataType::LargeList(a), DataType::LargeList(b)) => {
                a.is_nullable() == b.is_nullable() && a.data_type().equals_datatype(b.data_type())
            }
            (DataType::FixedSizeList(a, a_size), DataType::FixedSizeList(b, b_size)) => {
                a_size == b_size
                    && a.is_nullable() == b.is_nullable()
                    && a.data_type().equals_datatype(b.data_type())
            }
            (DataType::Struct(a), DataType::Struct(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|(a, b)| {
                        a.is_nullable() == b.is_nullable()
                            && a.data_type().equals_datatype(b.data_type())
                    })
            }
            _ => self == other,
        }
    }

    /// the [`PhysicalType`] of this [`DataType`].
    pub fn to_physical_type(&self) -> PhysicalType {
        use DataType::*;
        match self {
            Null => PhysicalType::Null,
            Boolean => PhysicalType::Boolean,
            Int8 => PhysicalType::Primitive(PrimitiveType::Int8),
            Int16 => PhysicalType::Primitive(PrimitiveType::Int16),
            Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
                PhysicalType::Primitive(PrimitiveType::Int32)
            }
            Int64 | Date64 | Timestamp(_, _) | Time64(_) | Duration(_) => {
                PhysicalType::Primitive(PrimitiveType::Int64)
            }
            Decimal(_, _) => PhysicalType::Primitive(PrimitiveType::Int128),
            UInt8 => PhysicalType::Primitive(PrimitiveType::UInt8),
            UInt16 => PhysicalType::Primitive(PrimitiveType::UInt16),
            UInt32 => PhysicalType::Primitive(PrimitiveType::UInt32),
            UInt64 => PhysicalType::Primitive(PrimitiveType::UInt64),
            Float16 => unreachable!(),
            Float32 => PhysicalType::Primitive(PrimitiveType::Float32),
            Float64 => PhysicalType::Primitive(PrimitiveType::Float64),
            Interval(IntervalUnit::DayTime) => PhysicalType::Primitive(PrimitiveType::DaysMs),
            Interval(IntervalUnit::MonthDayNano) => {
                PhysicalType::Primitive(PrimitiveType::MonthDayNano)
            }
            Binary => PhysicalType::Binary,
            FixedSizeBinary(_) => PhysicalType::FixedSizeBinary,
            LargeBinary => PhysicalType::LargeBinary,
            Utf8 => PhysicalType::Utf8,
            LargeUtf8 => PhysicalType::LargeUtf8,
            List(_) => PhysicalType::List,
            FixedSizeList(_, _) => PhysicalType::FixedSizeList,
            LargeList(_) => PhysicalType::LargeList,
            Struct(_) => PhysicalType::Struct,
            Union(_, _, _) => PhysicalType::Union,
            Dictionary(key, _) => PhysicalType::Dictionary(to_dictionary_index_type(key.as_ref())),
            Extension(_, key, _) => key.to_physical_type(),
        }
    }

    /// Returns `&self` for all but [`DataType::Extension`]. For [`DataType::Extension`],
    /// (recursively) returns the inner [`DataType`].
    /// Never returns the variant [`DataType::Extension`].
    pub fn to_logical_type(&self) -> &DataType {
        use DataType::*;
        match self {
            Extension(_, key, _) => key.to_logical_type(),
            _ => self,
        }
    }
}

fn to_dictionary_index_type(data_type: &DataType) -> DictionaryIndexType {
    match data_type {
        DataType::Int8 => DictionaryIndexType::Int8,
        DataType::Int16 => DictionaryIndexType::Int16,
        DataType::Int32 => DictionaryIndexType::Int32,
        DataType::Int64 => DictionaryIndexType::Int64,
        DataType::UInt8 => DictionaryIndexType::UInt8,
        DataType::UInt16 => DictionaryIndexType::UInt16,
        DataType::UInt32 => DictionaryIndexType::UInt32,
        DataType::UInt64 => DictionaryIndexType::UInt64,
        _ => ::core::unreachable!("A dictionary key type can only be of integer types"),
    }
}

// backward compatibility
use std::sync::Arc;
/// typedef for [`Arc<Schema>`].
pub type SchemaRef = Arc<Schema>;
