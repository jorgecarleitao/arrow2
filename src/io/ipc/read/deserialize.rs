//! Arrow IPC File and Stream Readers
//!
//! The `FileReader` and `StreamReader` have similar interfaces,
//! however the `FileReader` expects a reader that supports `Seek`ing

use std::collections::VecDeque;
use std::{
    io::{Read, Seek},
    sync::Arc,
};

use gen::Schema::MetadataVersion;

use crate::datatypes::{DataType, IntervalUnit};
use crate::error::Result;
use crate::io::ipc::gen::Message::BodyCompression;
use crate::{array::*, types::days_ms};

use super::super::gen;
use super::array::*;

pub type Node<'a> = (&'a gen::Message::FieldNode, &'a Option<Arc<dyn Array>>);

pub fn read<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<BodyCompression>,
    version: MetadataVersion,
) -> Result<Arc<dyn Array>> {
    match data_type {
        DataType::Null => {
            let array = read_null(field_nodes);
            Ok(Arc::new(array))
        }
        DataType::Boolean => {
            read_boolean(field_nodes, buffers, reader, block_offset, is_little_endian)
                .map(|x| Arc::new(x) as Arc<dyn Array>)
        }
        DataType::Int8 => read_primitive::<i8, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Int16 => read_primitive::<i16, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => read_primitive::<i32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => read_primitive::<i64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Decimal(_, _) => read_primitive::<i128, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Interval(IntervalUnit::DayTime) => read_primitive::<days_ms, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt8 => read_primitive::<u8, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt16 => read_primitive::<u16, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt32 => read_primitive::<u32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt64 => read_primitive::<u64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => read_primitive::<f32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Float64 => read_primitive::<f64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Binary => {
            let array = read_binary::<i32, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
                compression,
            )?;
            Ok(Arc::new(array))
        }
        DataType::LargeBinary => {
            let array = read_binary::<i64, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
                compression,
            )?;
            Ok(Arc::new(array))
        }
        DataType::FixedSizeBinary(_) => {
            let array = read_fixed_size_binary(
                field_nodes,
                data_type,
                buffers,
                reader,
                block_offset,
                is_little_endian,
                compression,
            )?;
            Ok(Arc::new(array))
        }
        DataType::Utf8 => {
            let array = read_utf8::<i32, _>(
                field_nodes,
                data_type,
                buffers,
                reader,
                block_offset,
                is_little_endian,
                compression,
            )?;
            Ok(Arc::new(array))
        }
        DataType::LargeUtf8 => {
            let array = read_utf8::<i64, _>(
                field_nodes,
                data_type,
                buffers,
                reader,
                block_offset,
                is_little_endian,
                compression,
            )?;
            Ok(Arc::new(array))
        }
        DataType::List(_) => read_list::<i32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            version,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::LargeList(_) => read_list::<i64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            version,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::FixedSizeList(_, _) => read_fixed_size_list(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            version,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Struct(_) => read_struct(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            version,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Dictionary(ref key_type, _) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                read_dictionary::<$T, _>(
                    field_nodes,
                    buffers,
                    reader,
                    block_offset,
                    is_little_endian,
                )
                .map(|x| Arc::new(x) as Arc<dyn Array>)
            })
        }
        DataType::Union(_, _, _) => read_union(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            version,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
    }
}

pub fn skip(
    field_nodes: &mut VecDeque<Node>,
    data_type: &DataType,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
) {
    match data_type {
        DataType::Null => skip_null(field_nodes),
        DataType::Boolean => skip_boolean(field_nodes, buffers),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(_)
        | DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Decimal(_, _)
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float16
        | DataType::Float64 => skip_primitive(field_nodes, buffers),
        DataType::LargeBinary | DataType::Binary => skip_binary(field_nodes, buffers),
        DataType::LargeUtf8 | DataType::Utf8 => skip_utf8(field_nodes, buffers),
        DataType::FixedSizeBinary(_) => skip_fixed_size_binary(field_nodes, buffers),
        DataType::List(_) => skip_list::<i32>(field_nodes, data_type, buffers),
        DataType::LargeList(_) => skip_list::<i64>(field_nodes, data_type, buffers),
        DataType::FixedSizeList(_, _) => skip_fixed_size_list(field_nodes, data_type, buffers),
        DataType::Struct(_) => skip_struct(field_nodes, data_type, buffers),
        DataType::Dictionary(_, _) => skip_dictionary(field_nodes, buffers),
        DataType::Union(_, _, _) => skip_union(field_nodes, data_type, buffers),
    }
}
