//! APIs to read from [ORC format](https://orc.apache.org).
use std::io::{Read, Seek, SeekFrom};

use crate::array::{Array, BooleanArray, Float32Array, Int32Array, Int64Array};
use crate::bitmap::{Bitmap, MutableBitmap};
use crate::datatypes::{DataType, Field, Schema};
use crate::error::Error;

use orc_format::fallible_streaming_iterator::FallibleStreamingIterator;
use orc_format::proto::stream::Kind;
use orc_format::proto::{CompressionKind, Footer, StripeInformation, Type};
use orc_format::read::decode;
use orc_format::read::Stripe;

/// Infers a [`Schema`] from the files' [`Footer`].
/// # Errors
/// This function errors if the type is not yet supported.
pub fn infer_schema(footer: &Footer) -> Result<Schema, Error> {
    let types = &footer.types;

    let dt = infer_dt(&footer.types[0], types)?;
    if let DataType::Struct(fields) = dt {
        Ok(fields.into())
    } else {
        Err(Error::ExternalFormat(
            "ORC root type must be a struct".to_string(),
        ))
    }
}

fn infer_dt(type_: &Type, types: &[Type]) -> Result<DataType, Error> {
    use orc_format::proto::r#type::Kind::*;
    let dt = match type_.kind() {
        Boolean => DataType::Boolean,
        Byte => DataType::Int8,
        Short => DataType::Int16,
        Int => DataType::Int32,
        Long => DataType::Int64,
        Float => DataType::Float32,
        Double => DataType::Float64,
        String => DataType::Utf8,
        Binary => DataType::Binary,
        Struct => {
            let sub_types = type_
                .subtypes
                .iter()
                .cloned()
                .zip(type_.field_names.iter())
                .map(|(i, name)| {
                    infer_dt(
                        types.get(i as usize).ok_or_else(|| {
                            Error::ExternalFormat(format!("ORC field {i} not found"))
                        })?,
                        types,
                    )
                    .map(|dt| Field::new(name, dt, true))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            DataType::Struct(sub_types)
        }
        kind => return Err(Error::nyi(format!("Reading {kind:?} from ORC"))),
    };
    Ok(dt)
}

/// Reads the stripe [`StripeInformation`] into memory.
pub fn read_stripe<R: Read + Seek>(
    reader: &mut R,
    stripe_info: StripeInformation,
    compression: CompressionKind,
) -> Result<Stripe, Error> {
    let offset = stripe_info.offset();
    reader.seek(SeekFrom::Start(offset)).unwrap();

    let len = stripe_info.index_length() + stripe_info.data_length() + stripe_info.footer_length();
    let mut stripe = vec![0; len as usize];
    reader.read_exact(&mut stripe).unwrap();

    Ok(Stripe::try_new(stripe, stripe_info, compression)?)
}

fn deserialize_validity(
    stripe: &Stripe,
    column: usize,
    scratch: &mut Vec<u8>,
) -> Result<Option<Bitmap>, Error> {
    let mut chunks = stripe.get_bytes(column, Kind::Present, std::mem::take(scratch))?;

    let mut validity = MutableBitmap::with_capacity(stripe.number_of_rows());
    let mut remaining = stripe.number_of_rows();
    while let Some(chunk) = chunks.next()? {
        // todo: this can be faster by iterating in bytes instead of single bits via `BooleanRun`
        let iter = decode::BooleanIter::new(chunk, remaining);
        for item in iter {
            remaining -= 1;
            validity.push(item?)
        }
    }
    *scratch = std::mem::take(&mut chunks.into_inner());

    Ok(validity.into())
}

/// Deserializes column `column` from `stripe`, assumed to represent a f32
fn deserialize_f32(
    data_type: DataType,
    stripe: &Stripe,
    column: usize,
) -> Result<Float32Array, Error> {
    let mut scratch = vec![];
    let num_rows = stripe.number_of_rows();

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let mut chunks = stripe.get_bytes(column, Kind::Data, scratch)?;

    let mut values = Vec::with_capacity(num_rows);
    if let Some(validity) = &validity {
        let mut validity_iter = validity.iter();
        while let Some(chunk) = chunks.next()? {
            let mut valid_iter = decode::deserialize_f32(chunk);
            let iter = validity_iter.by_ref().map(|is_valid| {
                if is_valid {
                    valid_iter.next().unwrap()
                } else {
                    0.0f32
                }
            });
            values.extend(iter);
        }
    } else {
        while let Some(chunk) = chunks.next()? {
            values.extend(decode::deserialize_f32(chunk));
        }
    }

    Float32Array::try_new(data_type, values.into(), validity)
}

/// Deserializes column `column` from `stripe`, assumed to represent a boolean array
fn deserialize_bool(
    data_type: DataType,
    stripe: &Stripe,
    column: usize,
) -> Result<BooleanArray, Error> {
    let num_rows = stripe.number_of_rows();
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let mut chunks = stripe.get_bytes(column, Kind::Data, std::mem::take(&mut scratch))?;

    let mut values = MutableBitmap::with_capacity(num_rows);
    if let Some(validity) = &validity {
        let mut validity_iter = validity.iter();

        while let Some(chunk) = chunks.next()? {
            let mut valid_iter = decode::BooleanIter::new(chunk, chunk.len() * 8);
            validity_iter.by_ref().try_for_each(|is_valid| {
                values.push(if is_valid {
                    valid_iter.next().unwrap()?
                } else {
                    false
                });
                Result::<(), Error>::Ok(())
            })?;
        }
    } else {
        while let Some(chunk) = chunks.next()? {
            let valid_iter = decode::BooleanIter::new(chunk, num_rows);
            for v in valid_iter {
                values.push(v?)
            }
        }
    }

    BooleanArray::try_new(data_type, values.into(), validity)
}

struct IntIter<'a> {
    current: Option<decode::SignedRleV2Run<'a>>,
    runs: decode::SignedRleV2Iter<'a>,
}

impl<'a> IntIter<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            runs: decode::SignedRleV2Iter::new(data),
            current: None,
        }
    }
}

impl<'a> Iterator for IntIter<'a> {
    type Item = Result<i64, Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let next = if let Some(run) = &mut self.current {
            match run {
                decode::SignedRleV2Run::Direct(values_iter) => values_iter.next(),
                decode::SignedRleV2Run::Delta(values_iter) => values_iter.next(),
                decode::SignedRleV2Run::ShortRepeat(values_iter) => values_iter.next(),
            }
        } else {
            None
        };

        if next.is_none() {
            match self.runs.next()? {
                Ok(run) => self.current = Some(run),
                Err(e) => return Some(Err(Error::ExternalFormat(format!("{:?}", e)))),
            }
            self.next()
        } else {
            next.map(Ok)
        }
    }
}

/// Deserializes column `column` from `stripe`, assumed to represent a boolean array
fn deserialize_i64(
    data_type: DataType,
    stripe: &Stripe,
    column: usize,
) -> Result<Int64Array, Error> {
    let num_rows = stripe.number_of_rows();
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let mut chunks = stripe.get_bytes(column, Kind::Data, std::mem::take(&mut scratch))?;

    let mut values = Vec::with_capacity(num_rows);
    if let Some(validity) = &validity {
        let validity_iter = validity.iter();

        let mut iter = IntIter::new(chunks.next()?.unwrap());
        for is_valid in validity_iter {
            if is_valid {
                let item = iter.next().transpose()?;
                let item = if let Some(item) = item {
                    item
                } else {
                    iter = IntIter::new(chunks.next()?.unwrap());
                    iter.next().transpose()?.unwrap()
                };
                values.push(item);
            } else {
                values.push(0);
            }
        }
    } else {
        while let Some(chunk) = chunks.next()? {
            decode::SignedRleV2Iter::new(chunk).try_for_each(|run| {
                run.map(|run| match run {
                    decode::SignedRleV2Run::Direct(values_iter) => values.extend(values_iter),
                    decode::SignedRleV2Run::Delta(values_iter) => values.extend(values_iter),
                    decode::SignedRleV2Run::ShortRepeat(values_iter) => values.extend(values_iter),
                })
            })?;
        }
    }

    Int64Array::try_new(data_type, values.into(), validity)
}

/// Deserializes column `column` from `stripe`, assumed to represent a boolean array
fn deserialize_i32(
    data_type: DataType,
    stripe: &Stripe,
    column: usize,
) -> Result<Int32Array, Error> {
    let num_rows = stripe.number_of_rows();
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let mut chunks = stripe.get_bytes(column, Kind::Data, std::mem::take(&mut scratch))?;

    let mut values = Vec::with_capacity(num_rows);
    if let Some(validity) = &validity {
        let validity_iter = validity.iter();

        let mut iter = IntIter::new(chunks.next()?.unwrap());
        for is_valid in validity_iter {
            if is_valid {
                let item = iter.next().transpose()?;
                let item = if let Some(item) = item {
                    item
                } else {
                    iter = IntIter::new(chunks.next()?.unwrap());
                    iter.next().transpose()?.unwrap()
                };
                values.push(item as i32);
            } else {
                values.push(0);
            }
        }
    } else {
        while let Some(chunk) = chunks.next()? {
            decode::SignedRleV2Iter::new(chunk).try_for_each(|run| {
                run.map(|run| match run {
                    decode::SignedRleV2Run::Direct(values_iter) => {
                        values.extend(values_iter.map(|x| x as i32))
                    }
                    decode::SignedRleV2Run::Delta(values_iter) => {
                        values.extend(values_iter.map(|x| x as i32))
                    }
                    decode::SignedRleV2Run::ShortRepeat(values_iter) => {
                        values.extend(values_iter.map(|x| x as i32))
                    }
                })
            })?;
        }
    }

    Int32Array::try_new(data_type, values.into(), validity)
}

/// Deserializes column `column` from `stripe`, assumed
/// to represent an array of `data_type`.
pub fn deserialize(
    data_type: DataType,
    stripe: &Stripe,
    column: usize,
) -> Result<Box<dyn Array>, Error> {
    match data_type {
        DataType::Boolean => deserialize_bool(data_type, stripe, column).map(|x| x.boxed()),
        DataType::Int32 => deserialize_i32(data_type, stripe, column).map(|x| x.boxed()),
        DataType::Int64 => deserialize_i64(data_type, stripe, column).map(|x| x.boxed()),
        DataType::Float32 => deserialize_f32(data_type, stripe, column).map(|x| x.boxed()),
        dt => return Err(Error::nyi(format!("Reading {dt:?} from ORC"))),
    }
}
