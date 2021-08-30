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

use crate::datatypes::{DataType, PhysicalType};
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
    use PhysicalType::*;
    match data_type.to_physical_type() {
        Null => {
            let array = read_null(field_nodes, data_type);
            Ok(Arc::new(array))
        }
        Boolean => read_boolean(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        Int8 => read_primitive::<i8, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        Int16 => read_primitive::<i16, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        Int32 => read_primitive::<i32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        Int64 => read_primitive::<i64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        Int128 => read_primitive::<i128, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DaysMs => read_primitive::<days_ms, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        UInt8 => read_primitive::<u8, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        UInt16 => read_primitive::<u16, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        UInt32 => read_primitive::<u32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        UInt64 => read_primitive::<u64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        Float32 => read_primitive::<f32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        Float64 => read_primitive::<f64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        Binary => {
            let array = read_binary::<i32, _>(
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
        LargeBinary => {
            let array = read_binary::<i64, _>(
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
        FixedSizeBinary => {
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
        Utf8 => {
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
        LargeUtf8 => {
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
        List => read_list::<i32, _>(
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
        LargeList => read_list::<i64, _>(
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
        FixedSizeList => read_fixed_size_list(
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
        Struct => read_struct(
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
        Dictionary(key_type) => {
            with_match_physical_dictionary_key_type!(key_type, |$T| {
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
        Union => read_union(
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
    use PhysicalType::*;
    match data_type.to_physical_type() {
        Null => skip_null(field_nodes),
        Boolean => skip_boolean(field_nodes, buffers),
        Int8 | Int16 | Int32 | Int64 | Int128 | UInt8 | UInt16 | UInt32 | UInt64 | Float32
        | Float64 | DaysMs => skip_primitive(field_nodes, buffers),
        LargeBinary | Binary => skip_binary(field_nodes, buffers),
        LargeUtf8 | Utf8 => skip_utf8(field_nodes, buffers),
        FixedSizeBinary => skip_fixed_size_binary(field_nodes, buffers),
        List => skip_list::<i32>(field_nodes, data_type, buffers),
        LargeList => skip_list::<i64>(field_nodes, data_type, buffers),
        FixedSizeList => skip_fixed_size_list(field_nodes, data_type, buffers),
        Struct => skip_struct(field_nodes, data_type, buffers),
        Dictionary(_) => skip_dictionary(field_nodes, buffers),
        Union => skip_union(field_nodes, data_type, buffers),
    }
}
