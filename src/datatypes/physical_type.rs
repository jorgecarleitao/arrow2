/// the set of valid indices used to index a dictionary-encoded Array.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DictionaryIndexType {
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
}

/// The set of physical types: unique in-memory representations of an Arrow array.
/// A physical type has a one-to-many relationship with a [`crate::datatypes::DataType`] and
/// a one-to-one mapping with each struct in this crate that implements [`crate::array::Array`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PhysicalType {
    /// A Null with no allocation.
    Null,
    /// A boolean represented as a single bit.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// A signed 128-bit integer.
    Int128,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// Two i32 representing days and ms
    DaysMs,
    /// Opaque binary data of variable length.
    Binary,
    /// Opaque binary data of fixed size.
    FixedSizeBinary,
    /// Opaque binary data of variable length and 64-bit offsets.
    LargeBinary,
    /// A variable-length string in Unicode with UTF-8 encoding.
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    LargeUtf8,
    /// A list of some data type with variable length.
    List,
    /// A list of some data type with fixed length.
    FixedSizeList,
    /// A list of some data type with variable length and 64-bit offsets.
    LargeList,
    /// A nested type that contains an arbitrary number of fields.
    Struct,
    /// A nested type that represents slots of differing types.
    Union,
    /// A dictionary encoded array by `DictionaryIndexType`.
    Dictionary(DictionaryIndexType),
}
