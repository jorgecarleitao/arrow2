// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A comparable row-oriented representation of a collection of [`Array`].
//!
//! **This module is an arrow2 version of [arrow::row]:[https://docs.rs/arrow/latest/arrow/row/index.html]**
//!
//! As [`Row`] are [normalized for sorting], they can be very efficiently [compared](PartialOrd),
//! using [`memcmp`] under the hood, or used in [non-comparison sorts] such as [radix sort]. This
//! makes the row format ideal for implementing efficient multi-column sorting,
//! grouping, aggregation, windowing and more.
//!
//! _Comparing [`Rows`] generated by different [`RowConverter`] is not guaranteed to
//! yield a meaningful ordering_
//!
//! [non-comparison sorts]:[https://en.wikipedia.org/wiki/Sorting_algorithm#Non-comparison_sorts]
//! [radix sort]:[https://en.wikipedia.org/wiki/Radix_sort]
//! [normalized for sorting]:[https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.83.1080&rep=rep1&type=pdf]
//! [`memcmp`]:[https://www.man7.org/linux/man-pages/man3/memcmp.3.html]
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::{
    array::{Array, BinaryArray, BooleanArray, DictionaryArray, PrimitiveArray, Utf8Array},
    datatypes::PhysicalType,
    error::*,
};
use crate::{compute::sort::SortOptions, datatypes::DataType};

use self::{
    dictionary::{compute_dictionary_mapping, encode_dictionary},
    interner::OrderPreservingInterner,
};

mod dictionary;
mod fixed;
mod interner;
mod variable;

/// Converts `Box<dyn Array>` columns into a row-oriented format.
///
/// # Format
///
/// The encoding of the row format should not be considered stable, but is documented here
/// for reference.
///
/// ## Unsigned Integer Encoding
///
/// A null integer is encoded as a `0_u8`, followed by a zero-ed number of bytes corresponding
/// to the integer's length
///
/// A valid integer is encoded as `1_u8`, followed by the big-endian representation of the
/// integer
///
/// ## Signed Integer Encoding
///
/// Signed integers have their most significant sign bit flipped, and are then encoded in the
/// same manner as an unsigned integer
///
/// ## Float Encoding
///
/// Floats are converted from IEEE 754 representation to a signed integer representation
/// by flipping all bar the sign bit if they are negative.
///
/// They are then encoded in the same manner as a signed integer
///
/// ## Variable Length Bytes Encoding
///
/// A null is encoded as a `0_u8`
///
/// An empty byte array is encoded as `1_u8`
///
/// A non-null, non-empty byte array is encoded as `2_u8` followed by the byte array
/// encoded using a block based scheme described below.
///
/// The byte array is broken up into 32-byte blocks, each block is written in turn
/// to the output, followed by `0xFF_u8`. The final block is padded to 32-bytes
/// with `0_u8` and written to the output, followed by the un-padded length in bytes
/// of this final block as a `u8`
///
/// This is loosely inspired by [COBS] encoding, and chosen over more traditional
/// [byte stuffing] as it is more amenable to vectorisation, in particular AVX-256.
///
/// ## Dictionary Encoding
///
/// [`RowConverter`] needs to support converting dictionary encoded arrays with unsorted, and
/// potentially distinct dictionaries. One simple mechanism to avoid this would be to reverse
/// the dictionary encoding, and encode the array values directly, however, this would lose
/// the benefits of dictionary encoding to reduce memory and CPU consumption.
///
/// As such the [`RowConverter`] maintains an order-preserving dictionary encoding for each
/// dictionary encoded column. As this is a variable-length encoding, new dictionary values
/// can be added whilst preserving the sort order.
///
/// A null dictionary value is encoded as `0_u8`.
///
/// A non-null dictionary value is encoded as `1_u8` followed by a null-terminated byte array
/// key determined by the order-preserving dictionary encoding
///
/// # Ordering
///
/// ## Float Ordering
///
/// Floats are totally ordered in accordance to the `totalOrder` predicate as defined
/// in the IEEE 754 (2008 revision) floating point standard.
///
/// The ordering established by this does not always agree with the
/// [`PartialOrd`] and [`PartialEq`] implementations of `f32`. For example,
/// they consider negative and positive zero equal, while this does not
///
/// ## Null Ordering
///
/// The encoding described above will order nulls first, this can be inverted by representing
/// nulls as `0xFF_u8` instead of `0_u8`
///
/// ## Reverse Column Ordering
///
/// The order of a given column can be reversed by negating the encoded bytes of non-null values
///
/// [COBS]:[https://en.wikipedia.org/wiki/Consistent_Overhead_Byte_Stuffing]
/// [byte stuffing]:[https://en.wikipedia.org/wiki/High-Level_Data_Link_Control#Asynchronous_framing]
#[derive(Debug)]
pub struct RowConverter {
    /// Sort fields
    fields: Arc<[SortField]>,
    /// interning state for column `i`, if column`i` is a dictionary
    interners: Vec<Option<Box<OrderPreservingInterner>>>,
}

/// Configure the data type and sort order for a given column
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortField {
    /// Sort options
    options: SortOptions,
    /// Data type
    data_type: DataType,
}

impl SortField {
    /// Create a new column with the given data type
    pub fn new(data_type: DataType) -> Self {
        Self::new_with_options(data_type, SortOptions::default())
    }

    /// Create a new column with the given data type and [`SortOptions`]
    pub fn new_with_options(data_type: DataType, options: SortOptions) -> Self {
        Self { options, data_type }
    }
}

impl RowConverter {
    /// Create a new [`RowConverter`] with the provided schema
    pub fn new(fields: Vec<SortField>) -> Self {
        let interners = vec![None; fields.len()];
        Self {
            fields: fields.into(),
            interners,
        }
    }

    /// Convert a slice of [`Box<dyn Array>`] columns into [`Rows`]
    ///
    /// See [`Row`] for information on when [`Row`] can be compared
    ///
    /// # Panics
    ///
    /// Panics if the schema of `columns` does not match that provided to [`RowConverter::new`]
    pub fn convert_columns(&mut self, columns: &[Box<dyn Array>]) -> Result<Rows> {
        if columns.len() != self.fields.len() {
            return Err(Error::InvalidArgumentError(format!(
                "Incorrect number of arrays provided to RowConverter, expected {} got {}",
                self.fields.len(),
                columns.len()
            )));
        }

        let dictionaries = columns
            .iter()
            .zip(&mut self.interners)
            .zip(self.fields.iter())
            .map(|((column, interner), field)| {
                if column.data_type() != &field.data_type {
                    return Err(Error::InvalidArgumentError(format!(
                        "RowConverter column schema mismatch, expected {:?} got {:?}",
                        field.data_type,
                        column.data_type()
                    )));
                }

                let values = match column.data_type().to_logical_type() {
                    DataType::Dictionary(k, _, _) => match_integer_type!(k, |$T| {
                        let column = column
                            .as_any()
                            .downcast_ref::<DictionaryArray<$T>>()
                            .unwrap();
                        column.values()
                    }),
                    _ => return Ok(None),
                };

                let interner = interner.get_or_insert_with(Default::default);

                let mapping = compute_dictionary_mapping(interner, values)?
                    .into_iter()
                    .map(|maybe_interned| {
                        maybe_interned.map(|interned| interner.normalized_key(interned))
                    })
                    .collect::<Vec<_>>();

                Ok(Some(mapping))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut rows = new_empty_rows(columns, &dictionaries)?;

        // jorgecarleitao's comments in PR#1287:
        // This seems to be embarassibly parallel.
        // Given that this is a transpose of O(N x C) where N is length and C number of columns, I wonder if we could split this so users can parallelize.
        // This is almost parallelizable - it is changing rows.
        // However, there is still an optimization since modifying rows is O(1) but encoding is O(C).
        // Will continue to think about this.
        for ((column, field), dictionary) in
            columns.iter().zip(self.fields.iter()).zip(dictionaries)
        {
            // We encode a column at a time to minimise dispatch overheads
            encode_column(&mut rows, column, field.options, dictionary.as_deref())
        }

        Ok(rows)
    }
}

/// A row-oriented representation of arrow data, that is normalized for comparison
///
/// See [`RowConverter`]
#[derive(Debug)]
pub struct Rows {
    /// Underlying row bytes
    buffer: Box<[u8]>,
    /// Row `i` has data `&buffer[offsets[i]..offsets[i+1]]`
    offsets: Box<[usize]>,
}

impl Rows {
    /// Get a reference to a certain row.
    pub fn row(&self, row: usize) -> Row<'_> {
        let end = self.offsets[row + 1];
        let start = self.offsets[row];
        Row {
            data: unsafe { self.buffer.get_unchecked(start..end) },
        }
    }

    /// Get a reference to a certain row but not check the bounds.
    pub fn row_unchecked(&self, row: usize) -> Row<'_> {
        let data = unsafe {
            let end = *self.offsets.get_unchecked(row + 1);
            let start = *self.offsets.get_unchecked(row);
            self.buffer.get_unchecked(start..end)
        };
        Row { data }
    }

    /// Returns the number of rows
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    /// Returns the iterator
    pub fn iter(&self) -> RowsIter<'_> {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a Rows {
    type Item = Row<'a>;
    type IntoIter = RowsIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        RowsIter {
            rows: self,
            start: 0,
            end: self.len(),
        }
    }
}

/// An iterator over [`Rows`]
#[derive(Debug)]
pub struct RowsIter<'a> {
    rows: &'a Rows,
    start: usize,
    end: usize,
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = Row<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let row = self.rows.row_unchecked(self.start);
            self.start += 1;
            Some(row)
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'a> ExactSizeIterator for RowsIter<'a> {
    #[inline]
    fn len(&self) -> usize {
        self.end - self.start
    }
}

impl<'a> DoubleEndedIterator for RowsIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.end == self.start {
            return None;
        }
        let row = self.rows.row(self.end);
        self.end -= 1;
        Some(row)
    }
}

unsafe impl<'a> crate::trusted_len::TrustedLen for RowsIter<'a> {}

/// A comparable representation of a row
///
/// Two [`Row`] can be compared if they both belong to [`Rows`] returned by calls to
/// [`RowConverter::convert_columns`] on the same [`RowConverter`]
///
/// Otherwise any ordering established by comparing the [`Row`] is arbitrary
#[derive(Debug, Copy, Clone)]
pub struct Row<'a> {
    data: &'a [u8],
}

// Manually derive these as don't wish to include `fields`

impl<'a> PartialEq for Row<'a> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(other.data)
    }
}

impl<'a> Eq for Row<'a> {}

impl<'a> PartialOrd for Row<'a> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.data.partial_cmp(other.data)
    }
}

impl<'a> Ord for Row<'a> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(other.data)
    }
}

impl<'a> Hash for Row<'a> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state)
    }
}

impl<'a> AsRef<[u8]> for Row<'a> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

/// Returns the null sentinel, negated if `invert` is true
#[inline]
fn null_sentinel(options: SortOptions) -> u8 {
    match options.nulls_first {
        true => 0,
        false => 0xFF,
    }
}

/// Match `PrimitiveType` to standard Rust types
#[macro_export]
macro_rules! with_match_primitive_without_interval_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::PrimitiveType::*;
    use $crate::types::{f16, i256};
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        Int128 => __with_ty__! { i128 },
        Int256 => __with_ty__! { i256 },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float16 => __with_ty__! { f16 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },
        _ => unimplemented!("Unsupported type: {:?}", $key_type),
    }
})}

/// Computes the length of each encoded [`Rows`] and returns an empty [`Rows`]
fn new_empty_rows(
    cols: &[Box<dyn Array>],
    dictionaries: &[Option<Vec<Option<&[u8]>>>],
) -> Result<Rows> {
    use fixed::FixedLengthEncoding;

    let num_rows = cols.first().map(|x| x.len()).unwrap_or(0);
    let mut lengths = vec![0; num_rows];

    for (array, dict) in cols.iter().zip(dictionaries) {
        match array.data_type().to_physical_type() {
            PhysicalType::Primitive(primitive) => {
                with_match_primitive_without_interval_type!(primitive, |$T| {
                    let array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<$T>>()
                        .unwrap();
                    lengths.iter_mut().for_each(|x| *x += fixed::encoded_len(array))
                })
            }
            PhysicalType::Null => {}
            PhysicalType::Boolean => lengths.iter_mut().for_each(|x| *x += bool::ENCODED_LEN),
            PhysicalType::Binary => array
                .as_any()
                .downcast_ref::<BinaryArray<i32>>()
                .unwrap()
                .iter()
                .zip(lengths.iter_mut())
                .for_each(|(slice, length)| *length += variable::encoded_len(slice)),
            PhysicalType::LargeBinary => array
                .as_any()
                .downcast_ref::<BinaryArray<i64>>()
                .unwrap()
                .iter()
                .zip(lengths.iter_mut())
                .for_each(|(slice, length)| *length += variable::encoded_len(slice)),
            PhysicalType::Utf8 => array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .unwrap()
                .iter()
                .zip(lengths.iter_mut())
                .for_each(|(slice, length)| {
                    *length += variable::encoded_len(slice.map(|x| x.as_bytes()))
                }),
            PhysicalType::LargeUtf8 => array
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .unwrap()
                .iter()
                .zip(lengths.iter_mut())
                .for_each(|(slice, length)| {
                    *length += variable::encoded_len(slice.map(|x| x.as_bytes()))
                }),
            PhysicalType::Dictionary(k) => match_integer_type!(k, |$T| {
                let array = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<$T>>()
                    .unwrap();
                let dict = dict.as_ref().unwrap();
                for (v, length) in array.keys().iter().zip(lengths.iter_mut()) {
                    match v.and_then(|v| dict[*v as usize]) {
                        Some(k) => *length += k.len() + 1,
                        None => *length += 1,
                    }
                }
            }),
            t => {
                return Err(Error::NotYetImplemented(format!(
                    "not yet implemented: {t:?}"
                )))
            }
        }
    }

    let mut offsets = Vec::with_capacity(num_rows + 1);
    offsets.push(0);

    // We initialize the offsets shifted down by one row index.
    //
    // As the rows are appended to the offsets will be incremented to match
    //
    // For example, consider the case of 3 rows of length 3, 4, and 6 respectively.
    // The offsets would be initialized to `0, 0, 3, 7`
    //
    // Writing the first row entirely would yield `0, 3, 3, 7`
    // The second, `0, 3, 7, 7`
    // The third, `0, 3, 7, 13`
    //
    // This would be the final offsets for reading
    //
    // In this way offsets tracks the position during writing whilst eventually serving
    // as identifying the offsets of the written rows
    let mut cur_offset = 0_usize;
    for l in lengths {
        offsets.push(cur_offset);
        cur_offset = cur_offset.checked_add(l).expect("overflow");
    }

    let buffer = vec![0_u8; cur_offset];

    Ok(Rows {
        buffer: buffer.into(),
        offsets: offsets.into(),
    })
}

/// Encodes a column to the provided [`Rows`] incrementing the offsets as it progresses
fn encode_column(
    out: &mut Rows,
    column: &Box<dyn Array>,
    opts: SortOptions,
    dictionary: Option<&[Option<&[u8]>]>,
) {
    match column.data_type().to_physical_type() {
        PhysicalType::Primitive(primitive) => {
            with_match_primitive_without_interval_type!(primitive, |$T| {
                let column = column
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$T>>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|v| *v));
                fixed::encode(out, column, opts);
            })
        }
        PhysicalType::Null => {}
        PhysicalType::Boolean => fixed::encode(
            out,
            column.as_any().downcast_ref::<BooleanArray>().unwrap(),
            opts,
        ),
        PhysicalType::Binary => {
            variable::encode(
                out,
                column
                    .as_any()
                    .downcast_ref::<BinaryArray<i32>>()
                    .unwrap()
                    .iter(),
                opts,
            );
        }
        PhysicalType::LargeBinary => {
            variable::encode(
                out,
                column
                    .as_any()
                    .downcast_ref::<BinaryArray<i64>>()
                    .unwrap()
                    .iter(),
                opts,
            );
        }
        PhysicalType::Utf8 => variable::encode(
            out,
            column
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .unwrap()
                .iter()
                .map(|x| x.map(|x| x.as_bytes())),
            opts,
        ),
        PhysicalType::LargeUtf8 => variable::encode(
            out,
            column
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .unwrap()
                .iter()
                .map(|x| x.map(|x| x.as_bytes())),
            opts,
        ),
        PhysicalType::Dictionary(k) => match_integer_type!(k, |$T| {
            let column = column
                .as_any()
                .downcast_ref::<DictionaryArray<$T>>()
                .unwrap();
            encode_dictionary(out, column, dictionary.unwrap(), opts);
        }),
        t => unimplemented!("not yet implemented: {:?}", t),
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use rand::{
        distributions::{uniform::SampleUniform, Distribution, Standard},
        thread_rng, Rng,
    };

    use super::*;
    use crate::{
        array::{Array, DictionaryKey, Float32Array, Int16Array, NullArray},
        compute::sort::build_compare,
        datatypes::DataType,
        offset::Offset,
        types::NativeType,
    };

    #[test]
    fn test_fixed_width() {
        let cols = [
            Int16Array::from([Some(1), Some(2), None, Some(-5), Some(2), Some(2), Some(0)])
                .to_boxed(),
            Float32Array::from([
                Some(1.3),
                Some(2.5),
                None,
                Some(4.),
                Some(0.1),
                Some(-4.),
                Some(-0.),
            ])
            .to_boxed(),
        ];

        let mut converter = RowConverter::new(vec![
            SortField::new(DataType::Int16),
            SortField::new(DataType::Float32),
        ]);
        let rows = converter.convert_columns(&cols).unwrap();

        assert_eq!(rows.offsets.as_ref(), &[0, 8, 16, 24, 32, 40, 48, 56]);
        assert_eq!(
            rows.buffer.as_ref(),
            &[
                1, 128, 1, //
                1, 191, 166, 102, 102, //
                1, 128, 2, //
                1, 192, 32, 0, 0, //
                0, 0, 0, //
                0, 0, 0, 0, 0, //
                1, 127, 251, //
                1, 192, 128, 0, 0, //
                1, 128, 2, //
                1, 189, 204, 204, 205, //
                1, 128, 2, //
                1, 63, 127, 255, 255, //
                1, 128, 0, //
                1, 127, 255, 255, 255 //
            ]
        );

        assert!(rows.row(3) < rows.row(6));
        assert!(rows.row(0) < rows.row(1));
        assert!(rows.row(3) < rows.row(0));
        assert!(rows.row(4) < rows.row(1));
        assert!(rows.row(5) < rows.row(4));
    }

    #[test]
    fn test_null_encoding() {
        let col = NullArray::new(DataType::Null, 10).to_boxed();
        let mut converter = RowConverter::new(vec![SortField::new(DataType::Null)]);
        let rows = converter.convert_columns(&[col]).unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(rows.row(1).data.len(), 0);
    }

    fn generate_primitive_array<K>(len: usize, valid_percent: f64) -> PrimitiveArray<K>
    where
        K: NativeType,
        Standard: Distribution<K>,
    {
        let mut rng = thread_rng();
        (0..len)
            .map(|_| rng.gen_bool(valid_percent).then(|| rng.gen()))
            .collect()
    }

    fn generate_strings<O: Offset>(len: usize, valid_percent: f64) -> Utf8Array<O> {
        let mut rng = thread_rng();
        (0..len)
            .map(|_| {
                rng.gen_bool(valid_percent).then(|| {
                    let len = rng.gen_range(0..100);
                    let bytes = (0..len).map(|_| rng.gen_range(0..128)).collect();
                    String::from_utf8(bytes).unwrap()
                })
            })
            .collect()
    }

    fn generate_dictionary<K>(
        values: Box<dyn Array>,
        len: usize,
        valid_percent: f64,
    ) -> DictionaryArray<K>
    where
        K: DictionaryKey + Ord + SampleUniform,
        <K as TryFrom<usize>>::Error: Debug,
    {
        let mut rng = thread_rng();
        let min_key = 0_usize.try_into().unwrap();
        let max_key = values.len().try_into().unwrap();
        let keys: PrimitiveArray<K> = (0..len)
            .map(|_| {
                rng.gen_bool(valid_percent)
                    .then(|| rng.gen_range(min_key..max_key))
            })
            .collect();

        DictionaryArray::try_from_keys(keys, values).unwrap()
    }

    fn generate_column(len: usize) -> Box<dyn Array> {
        let mut rng = thread_rng();
        match rng.gen_range(0..9) {
            0 => Box::new(generate_primitive_array::<i32>(len, 0.8)),
            1 => Box::new(generate_primitive_array::<u32>(len, 0.8)),
            2 => Box::new(generate_primitive_array::<i64>(len, 0.8)),
            3 => Box::new(generate_primitive_array::<u64>(len, 0.8)),
            4 => Box::new(generate_primitive_array::<f32>(len, 0.8)),
            5 => Box::new(generate_primitive_array::<f64>(len, 0.8)),
            6 => Box::new(generate_strings::<i32>(len, 0.8)),
            7 => Box::new(generate_dictionary::<i64>(
                // Cannot test dictionaries containing null values because of #2687
                Box::new(generate_strings::<i32>(rng.gen_range(1..len), 1.0)),
                len,
                0.8,
            )),
            8 => Box::new(generate_dictionary::<i64>(
                // Cannot test dictionaries containing null values because of #2687
                Box::new(generate_primitive_array::<i64>(rng.gen_range(1..len), 1.0)),
                len,
                0.8,
            )),
            _ => unreachable!(),
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn fuzz_test() {
        for _ in 0..100 {
            let mut rng = thread_rng();
            let num_columns = rng.gen_range(1..5);
            let len = rng.gen_range(5..100);
            let arrays: Vec<_> = (0..num_columns).map(|_| generate_column(len)).collect();

            let options: Vec<_> = (0..num_columns)
                .map(|_| SortOptions {
                    descending: rng.gen_bool(0.5),
                    nulls_first: rng.gen_bool(0.5),
                })
                .collect();

            let comparators = arrays
                .iter()
                .zip(options.iter())
                .map(|(a, o)| build_compare(&**a, *o).unwrap())
                .collect::<Vec<_>>();

            let columns = options
                .into_iter()
                .zip(&arrays)
                .map(|(o, a)| SortField::new_with_options(a.data_type().clone(), o))
                .collect();

            let mut converter = RowConverter::new(columns);
            let rows = converter.convert_columns(&arrays).unwrap();
            let cmp = |i, j| {
                for cmp in comparators.iter() {
                    let cmp = cmp(i, j);
                    if cmp != Ordering::Equal {
                        return cmp;
                    }
                }
                Ordering::Equal
            };

            for i in 0..len {
                for j in 0..len {
                    let row_i = rows.row(i);
                    let row_j = rows.row(j);
                    let row_cmp = row_i.cmp(&row_j);
                    let lex_cmp = cmp(i, j);
                    assert_eq!(row_cmp, lex_cmp);
                }
            }
        }
    }
}
