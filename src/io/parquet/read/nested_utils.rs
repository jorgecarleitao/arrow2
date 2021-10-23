use std::sync::Arc;

use parquet2::schema::{types::ParquetType, Repetition};

use crate::{
    array::{Array, ListArray},
    bitmap::{Bitmap, MutableBitmap},
    buffer::{Buffer, MutableBuffer},
    datatypes::DataType,
    error::{ArrowError, Result},
};

/// trait describing deserialized repetition and definition levels
pub trait Nested: std::fmt::Debug {
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>);

    fn last_offset(&self) -> i64;

    fn push(&mut self, length: i64, is_valid: bool);

    fn offsets(&mut self) -> &[i64];

    fn close(&mut self, length: i64);
}

#[derive(Debug, Default)]
pub struct NestedOptional {
    pub validity: MutableBitmap,
    pub offsets: MutableBuffer<i64>,
}

impl Nested for NestedOptional {
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>) {
        let offsets = std::mem::take(&mut self.offsets);
        let validity = std::mem::take(&mut self.validity);
        (offsets.into(), validity.into())
    }

    #[inline]
    fn last_offset(&self) -> i64 {
        *self.offsets.last().unwrap()
    }

    fn push(&mut self, value: i64, is_valid: bool) {
        self.offsets.push(value);
        self.validity.push(is_valid);
    }

    fn offsets(&mut self) -> &[i64] {
        &self.offsets
    }

    fn close(&mut self, length: i64) {
        self.offsets.push(length)
    }
}

impl NestedOptional {
    pub fn with_capacity(capacity: usize) -> Self {
        let offsets = MutableBuffer::<i64>::with_capacity(capacity + 1);
        let validity = MutableBitmap::with_capacity(capacity);
        Self { validity, offsets }
    }
}

#[derive(Debug, Default)]
pub struct NestedValid {
    pub offsets: MutableBuffer<i64>,
}

impl Nested for NestedValid {
    fn inner(&mut self) -> (Buffer<i64>, Option<Bitmap>) {
        let offsets = std::mem::take(&mut self.offsets);
        (offsets.into(), None)
    }

    #[inline]
    fn last_offset(&self) -> i64 {
        *self.offsets.last().unwrap()
    }

    fn push(&mut self, value: i64, _is_valid: bool) {
        self.offsets.push(value);
    }

    fn offsets(&mut self) -> &[i64] {
        &self.offsets
    }

    fn close(&mut self, length: i64) {
        self.offsets.push(length)
    }
}

impl NestedValid {
    pub fn with_capacity(capacity: usize) -> Self {
        let offsets = MutableBuffer::<i64>::with_capacity(capacity + 1);
        Self { offsets }
    }
}

pub fn extend_offsets<R, D>(
    rep_levels: R,
    def_levels: D,
    is_nullable: bool,
    max_rep: u32,
    max_def: u32,
    nested: &mut Vec<Box<dyn Nested>>,
) where
    R: Iterator<Item = u32>,
    D: Iterator<Item = u32>,
{
    assert_eq!(max_rep, 1);
    let mut values_count = 0;
    rep_levels.zip(def_levels).for_each(|(rep, def)| {
        if rep == 0 {
            nested[0].push(values_count, def != 0);
        }
        if def == max_def || (is_nullable && def == max_def - 1) {
            values_count += 1;
        }
    });
    nested[0].close(values_count);
}

pub fn is_nullable(type_: &ParquetType, container: &mut Vec<bool>) {
    match type_ {
        ParquetType::PrimitiveType { basic_info, .. } => {
            container.push(super::schema::is_nullable(basic_info));
        }
        ParquetType::GroupType {
            basic_info, fields, ..
        } => {
            if basic_info.repetition() != &Repetition::Repeated {
                container.push(super::schema::is_nullable(basic_info));
            }
            for field in fields {
                is_nullable(field, container)
            }
        }
    }
}

pub fn init_nested(base_type: &ParquetType, capacity: usize) -> (Vec<Box<dyn Nested>>, bool) {
    let mut nullable = Vec::new();
    is_nullable(base_type, &mut nullable);
    // the primitive's nullability is the last on the list
    let is_nullable = nullable.pop().unwrap();

    (
        nullable
            .iter()
            .map(|is_nullable| {
                if *is_nullable {
                    Box::new(NestedOptional::with_capacity(capacity)) as Box<dyn Nested>
                } else {
                    Box::new(NestedValid::with_capacity(capacity)) as Box<dyn Nested>
                }
            })
            .collect(),
        is_nullable,
    )
}

pub fn create_list(
    data_type: DataType,
    nested: &mut [Box<dyn Nested>],
    values: Arc<dyn Array>,
) -> Result<Box<dyn Array>> {
    Ok(match data_type {
        DataType::List(_) => {
            let (offsets, validity) = nested[0].inner();

            let offsets = Buffer::<i32>::from_trusted_len_iter(offsets.iter().map(|x| *x as i32));
            Box::new(ListArray::<i32>::from_data(
                data_type, offsets, values, validity,
            ))
        }
        DataType::LargeList(_) => {
            let (offsets, validity) = nested[0].inner();

            Box::new(ListArray::<i64>::from_data(
                data_type, offsets, values, validity,
            ))
        }
        _ => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Read nested datatype {:?}",
                data_type
            )))
        }
    })
}
