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

//! Utilities for converting between IPC types and native Arrow types

use crate::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use crate::endianess::is_native_little_endian;
use crate::io::ipc::convert::ipc::UnionMode;

mod ipc {
    pub use super::super::gen::File::*;
    pub use super::super::gen::Message::*;
    pub use super::super::gen::Schema::*;
}

use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, UnionWIPOffset, Vector, WIPOffset};
use std::collections::{BTreeMap, HashMap};

use DataType::*;

type Metadata = Option<BTreeMap<String, String>>;
type Extension = Option<(String, Option<String>)>;

pub fn schema_to_fb_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    schema: &Schema,
) -> WIPOffset<ipc::Schema<'a>> {
    let mut fields = vec![];
    for field in schema.fields() {
        let fb_field = build_field(fbb, field);
        fields.push(fb_field);
    }

    let mut custom_metadata = vec![];
    for (k, v) in schema.metadata() {
        let fb_key_name = fbb.create_string(k.as_str());
        let fb_val_name = fbb.create_string(v.as_str());

        let mut kv_builder = ipc::KeyValueBuilder::new(fbb);
        kv_builder.add_key(fb_key_name);
        kv_builder.add_value(fb_val_name);
        custom_metadata.push(kv_builder.finish());
    }

    let fb_field_list = fbb.create_vector(&fields);
    let fb_metadata_list = fbb.create_vector(&custom_metadata);

    let mut builder = ipc::SchemaBuilder::new(fbb);
    builder.add_fields(fb_field_list);
    builder.add_custom_metadata(fb_metadata_list);
    builder.add_endianness(if is_native_little_endian() {
        ipc::Endianness::Little
    } else {
        ipc::Endianness::Big
    });
    builder.finish()
}

fn read_metadata(field: &ipc::Field) -> Metadata {
    if let Some(list) = field.custom_metadata() {
        let mut metadata_map = BTreeMap::default();
        for kv in list {
            if let (Some(k), Some(v)) = (kv.key(), kv.value()) {
                metadata_map.insert(k.to_string(), v.to_string());
            }
        }
        Some(metadata_map)
    } else {
        None
    }
}

pub(crate) fn get_extension(metadata: &Metadata) -> Extension {
    if let Some(metadata) = metadata {
        if let Some(name) = metadata.get("ARROW:extension:name") {
            let metadata = metadata.get("ARROW:extension:metadata").cloned();
            Some((name.clone(), metadata))
        } else {
            None
        }
    } else {
        None
    }
}

/// Convert an IPC Field to Arrow Field
impl<'a> From<ipc::Field<'a>> for Field {
    fn from(field: ipc::Field) -> Field {
        let metadata = read_metadata(&field);

        let extension = get_extension(&metadata);

        let data_type = get_data_type(field, extension, true);

        let mut arrow_field = if let Some(dictionary) = field.dictionary() {
            Field::new_dict(
                field.name().unwrap(),
                data_type,
                field.nullable(),
                dictionary.id(),
                dictionary.isOrdered(),
            )
        } else {
            Field::new(field.name().unwrap(), data_type, field.nullable())
        };

        arrow_field.set_metadata(metadata);
        arrow_field
    }
}

/// Deserialize a Schema table from IPC format to Schema data type
pub fn fb_to_schema(fb: ipc::Schema) -> (Schema, bool) {
    let mut fields: Vec<Field> = vec![];
    let c_fields = fb.fields().unwrap();
    let len = c_fields.len();
    for i in 0..len {
        let c_field: ipc::Field = c_fields.get(i);
        fields.push(c_field.into());
    }

    let is_little_endian = fb.endianness().variant_name().unwrap_or("Little") == "Little";

    let mut metadata: HashMap<String, String> = HashMap::default();
    if let Some(md_fields) = fb.custom_metadata() {
        let len = md_fields.len();
        for i in 0..len {
            let kv = md_fields.get(i);
            let k_str = kv.key();
            let v_str = kv.value();
            if let Some(k) = k_str {
                if let Some(v) = v_str {
                    metadata.insert(k.to_string(), v.to_string());
                }
            }
        }
    }
    (Schema::new_from(fields, metadata), is_little_endian)
}

/// Get the Arrow data type from the flatbuffer Field table
fn get_data_type(field: ipc::Field, extension: Extension, may_be_dictionary: bool) -> DataType {
    if let Some(dictionary) = field.dictionary() {
        if may_be_dictionary {
            let int = dictionary.indexType().unwrap();
            let index_type = match (int.bitWidth(), int.is_signed()) {
                (8, true) => DataType::Int8,
                (8, false) => DataType::UInt8,
                (16, true) => DataType::Int16,
                (16, false) => DataType::UInt16,
                (32, true) => DataType::Int32,
                (32, false) => DataType::UInt32,
                (64, true) => DataType::Int64,
                (64, false) => DataType::UInt64,
                _ => panic!("Unexpected bitwidth and signed"),
            };
            return DataType::Dictionary(
                Box::new(index_type),
                Box::new(get_data_type(field, extension, false)),
            );
        }
    }

    if let Some(extension) = extension {
        let (name, metadata) = extension;
        let data_type = get_data_type(field, None, false);
        return DataType::Extension(name, Box::new(data_type), metadata);
    }

    match field.type_type() {
        ipc::Type::Null => DataType::Null,
        ipc::Type::Bool => DataType::Boolean,
        ipc::Type::Int => {
            let int = field.type_as_int().unwrap();
            match (int.bitWidth(), int.is_signed()) {
                (8, true) => DataType::Int8,
                (8, false) => DataType::UInt8,
                (16, true) => DataType::Int16,
                (16, false) => DataType::UInt16,
                (32, true) => DataType::Int32,
                (32, false) => DataType::UInt32,
                (64, true) => DataType::Int64,
                (64, false) => DataType::UInt64,
                z => panic!(
                    "Int type with bit width of {} and signed of {} not supported",
                    z.0, z.1
                ),
            }
        }
        ipc::Type::Binary => DataType::Binary,
        ipc::Type::LargeBinary => DataType::LargeBinary,
        ipc::Type::Utf8 => DataType::Utf8,
        ipc::Type::LargeUtf8 => DataType::LargeUtf8,
        ipc::Type::FixedSizeBinary => {
            let fsb = field.type_as_fixed_size_binary().unwrap();
            DataType::FixedSizeBinary(fsb.byteWidth())
        }
        ipc::Type::FloatingPoint => {
            let float = field.type_as_floating_point().unwrap();
            match float.precision() {
                ipc::Precision::HALF => DataType::Float16,
                ipc::Precision::SINGLE => DataType::Float32,
                ipc::Precision::DOUBLE => DataType::Float64,
                z => panic!("FloatingPoint type with precision of {:?} not supported", z),
            }
        }
        ipc::Type::Date => {
            let date = field.type_as_date().unwrap();
            match date.unit() {
                ipc::DateUnit::DAY => DataType::Date32,
                ipc::DateUnit::MILLISECOND => DataType::Date64,
                z => panic!("Date type with unit of {:?} not supported", z),
            }
        }
        ipc::Type::Time => {
            let time = field.type_as_time().unwrap();
            match (time.bitWidth(), time.unit()) {
                (32, ipc::TimeUnit::SECOND) => DataType::Time32(TimeUnit::Second),
                (32, ipc::TimeUnit::MILLISECOND) => DataType::Time32(TimeUnit::Millisecond),
                (64, ipc::TimeUnit::MICROSECOND) => DataType::Time64(TimeUnit::Microsecond),
                (64, ipc::TimeUnit::NANOSECOND) => DataType::Time64(TimeUnit::Nanosecond),
                z => panic!(
                    "Time type with bit width of {} and unit of {:?} not supported",
                    z.0, z.1
                ),
            }
        }
        ipc::Type::Timestamp => {
            let timestamp = field.type_as_timestamp().unwrap();
            let timezone: Option<String> = timestamp.timezone().map(|tz| tz.to_string());
            match timestamp.unit() {
                ipc::TimeUnit::SECOND => DataType::Timestamp(TimeUnit::Second, timezone),
                ipc::TimeUnit::MILLISECOND => DataType::Timestamp(TimeUnit::Millisecond, timezone),
                ipc::TimeUnit::MICROSECOND => DataType::Timestamp(TimeUnit::Microsecond, timezone),
                ipc::TimeUnit::NANOSECOND => DataType::Timestamp(TimeUnit::Nanosecond, timezone),
                z => panic!("Timestamp type with unit of {:?} not supported", z),
            }
        }
        ipc::Type::Interval => {
            let interval = field.type_as_interval().unwrap();
            match interval.unit() {
                ipc::IntervalUnit::YEAR_MONTH => DataType::Interval(IntervalUnit::YearMonth),
                ipc::IntervalUnit::DAY_TIME => DataType::Interval(IntervalUnit::DayTime),
                z => panic!("Interval type with unit of {:?} unsupported", z),
            }
        }
        ipc::Type::Duration => {
            let duration = field.type_as_duration().unwrap();
            match duration.unit() {
                ipc::TimeUnit::SECOND => DataType::Duration(TimeUnit::Second),
                ipc::TimeUnit::MILLISECOND => DataType::Duration(TimeUnit::Millisecond),
                ipc::TimeUnit::MICROSECOND => DataType::Duration(TimeUnit::Microsecond),
                ipc::TimeUnit::NANOSECOND => DataType::Duration(TimeUnit::Nanosecond),
                z => panic!("Duration type with unit of {:?} unsupported", z),
            }
        }
        ipc::Type::List => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a list to have one child")
            }
            DataType::List(Box::new(children.get(0).into()))
        }
        ipc::Type::LargeList => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a large list to have one child")
            }
            DataType::LargeList(Box::new(children.get(0).into()))
        }
        ipc::Type::FixedSizeList => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a list to have one child")
            }
            let fsl = field.type_as_fixed_size_list().unwrap();
            DataType::FixedSizeList(Box::new(children.get(0).into()), fsl.listSize())
        }
        ipc::Type::Struct_ => {
            let mut fields = vec![];
            if let Some(children) = field.children() {
                for i in 0..children.len() {
                    fields.push(children.get(i).into());
                }
            };

            DataType::Struct(fields)
        }
        ipc::Type::Decimal => {
            let fsb = field.type_as_decimal().unwrap();
            DataType::Decimal(fsb.precision() as usize, fsb.scale() as usize)
        }
        ipc::Type::Union => {
            let type_ = field.type_as_union().unwrap();

            let is_sparse = type_.mode() == UnionMode::Sparse;

            let ids = type_.typeIds().map(|x| x.iter().collect());

            let fields = if let Some(children) = field.children() {
                (0..children.len())
                    .map(|i| children.get(i).into())
                    .collect()
            } else {
                vec![]
            };
            DataType::Union(fields, ids, is_sparse)
        }
        t => unimplemented!("Type {:?} not supported", t),
    }
}

pub(crate) struct FbFieldType<'b> {
    pub(crate) type_type: ipc::Type,
    pub(crate) type_: WIPOffset<UnionWIPOffset>,
    pub(crate) children: Option<WIPOffset<Vector<'b, ForwardsUOffset<ipc::Field<'b>>>>>,
}

fn write_metadata<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    metadata: &BTreeMap<String, String>,
    kv_vec: &mut Vec<WIPOffset<ipc::KeyValue<'a>>>,
) {
    for (k, v) in metadata {
        if k != "ARROW:extension:name" && k != "ARROW:extension:metadata" {
            let kv_args = ipc::KeyValueArgs {
                key: Some(fbb.create_string(k.as_str())),
                value: Some(fbb.create_string(v.as_str())),
            };
            kv_vec.push(ipc::KeyValue::create(fbb, &kv_args));
        }
    }
}

/// Create an IPC Field from an Arrow Field
pub(crate) fn build_field<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    field: &Field,
) -> WIPOffset<ipc::Field<'a>> {
    // custom metadata.
    let mut kv_vec = vec![];
    if let DataType::Extension(name, _, metadata) = field.data_type() {
        // append extension information.

        // metadata
        if let Some(metadata) = metadata {
            let kv_args = ipc::KeyValueArgs {
                key: Some(fbb.create_string("ARROW:extension:metadata")),
                value: Some(fbb.create_string(metadata.as_str())),
            };
            kv_vec.push(ipc::KeyValue::create(fbb, &kv_args));
        }

        // name
        let kv_args = ipc::KeyValueArgs {
            key: Some(fbb.create_string("ARROW:extension:name")),
            value: Some(fbb.create_string(name.as_str())),
        };
        kv_vec.push(ipc::KeyValue::create(fbb, &kv_args));
    }
    if let Some(metadata) = field.metadata() {
        if !metadata.is_empty() {
            write_metadata(fbb, metadata, &mut kv_vec);
        }
    };
    let fb_metadata = if !kv_vec.is_empty() {
        Some(fbb.create_vector(&kv_vec))
    } else {
        None
    };

    let fb_field_name = fbb.create_string(field.name().as_str());
    let field_type = get_fb_field_type(field.data_type(), field.is_nullable(), fbb);

    let fb_dictionary = if let Dictionary(index_type, _) = field.data_type() {
        Some(get_fb_dictionary(
            index_type,
            field
                .dict_id()
                .expect("All Dictionary types have `dict_id`"),
            field
                .dict_is_ordered()
                .expect("All Dictionary types have `dict_is_ordered`"),
            fbb,
        ))
    } else {
        None
    };

    let mut field_builder = ipc::FieldBuilder::new(fbb);
    field_builder.add_name(fb_field_name);
    if let Some(dictionary) = fb_dictionary {
        field_builder.add_dictionary(dictionary)
    }
    field_builder.add_type_type(field_type.type_type);
    field_builder.add_nullable(field.is_nullable());
    match field_type.children {
        None => {}
        Some(children) => field_builder.add_children(children),
    };
    field_builder.add_type_(field_type.type_);

    if let Some(fb_metadata) = fb_metadata {
        field_builder.add_custom_metadata(fb_metadata);
    }

    field_builder.finish()
}

fn type_to_field_type(data_type: &DataType) -> ipc::Type {
    match data_type {
        Null => ipc::Type::Null,
        Boolean => ipc::Type::Bool,
        UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 => ipc::Type::Int,
        Float16 | Float32 | Float64 => ipc::Type::FloatingPoint,
        Decimal(_, _) => ipc::Type::Decimal,
        Binary => ipc::Type::Binary,
        LargeBinary => ipc::Type::LargeBinary,
        Utf8 => ipc::Type::Utf8,
        LargeUtf8 => ipc::Type::LargeUtf8,
        FixedSizeBinary(_) => ipc::Type::FixedSizeBinary,
        Date32 | Date64 => ipc::Type::Date,
        Duration(_) => ipc::Type::Duration,
        Time32(_) | Time64(_) => ipc::Type::Time,
        Timestamp(_, _) => ipc::Type::Timestamp,
        Interval(_) => ipc::Type::Interval,
        List(_) => ipc::Type::List,
        LargeList(_) => ipc::Type::LargeList,
        FixedSizeList(_, _) => ipc::Type::FixedSizeList,
        Union(_, _, _) => ipc::Type::Union,
        Struct(_) => ipc::Type::Struct_,
        Dictionary(_, v) => type_to_field_type(v),
        Extension(_, v, _) => type_to_field_type(v),
    }
}

/// Get the IPC type of a data type
pub(crate) fn get_fb_field_type<'a>(
    data_type: &DataType,
    is_nullable: bool,
    fbb: &mut FlatBufferBuilder<'a>,
) -> FbFieldType<'a> {
    let type_type = type_to_field_type(data_type);

    // some IPC implementations expect an empty list for child data, instead of a null value.
    // An empty field list is thus returned for primitive types
    let empty_fields: Vec<WIPOffset<ipc::Field>> = vec![];
    match data_type {
        Null => FbFieldType {
            type_type,
            type_: ipc::NullBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        Boolean => FbFieldType {
            type_type,
            type_: ipc::BoolBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => {
            let children = fbb.create_vector(&empty_fields[..]);
            let mut builder = ipc::IntBuilder::new(fbb);
            if matches!(data_type, UInt8 | UInt16 | UInt32 | UInt64) {
                builder.add_is_signed(false);
            } else {
                builder.add_is_signed(true);
            }
            match data_type {
                Int8 | UInt8 => builder.add_bitWidth(8),
                Int16 | UInt16 => builder.add_bitWidth(16),
                Int32 | UInt32 => builder.add_bitWidth(32),
                Int64 | UInt64 => builder.add_bitWidth(64),
                _ => {}
            };
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(children),
            }
        }
        Float16 | Float32 | Float64 => {
            let children = fbb.create_vector(&empty_fields[..]);
            let mut builder = ipc::FloatingPointBuilder::new(fbb);
            match data_type {
                Float16 => builder.add_precision(ipc::Precision::HALF),
                Float32 => builder.add_precision(ipc::Precision::SINGLE),
                Float64 => builder.add_precision(ipc::Precision::DOUBLE),
                _ => {}
            };
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(children),
            }
        }
        Binary => FbFieldType {
            type_type,
            type_: ipc::BinaryBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        LargeBinary => FbFieldType {
            type_type,
            type_: ipc::LargeBinaryBuilder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        Utf8 => FbFieldType {
            type_type,
            type_: ipc::Utf8Builder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        LargeUtf8 => FbFieldType {
            type_type,
            type_: ipc::LargeUtf8Builder::new(fbb).finish().as_union_value(),
            children: Some(fbb.create_vector(&empty_fields[..])),
        },
        FixedSizeBinary(len) => {
            let mut builder = ipc::FixedSizeBinaryBuilder::new(fbb);
            builder.add_byteWidth(*len as i32);
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Date32 => {
            let mut builder = ipc::DateBuilder::new(fbb);
            builder.add_unit(ipc::DateUnit::DAY);
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Date64 => {
            let mut builder = ipc::DateBuilder::new(fbb);
            builder.add_unit(ipc::DateUnit::MILLISECOND);
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Time32(unit) | Time64(unit) => {
            let mut builder = ipc::TimeBuilder::new(fbb);
            match unit {
                TimeUnit::Second => {
                    builder.add_bitWidth(32);
                    builder.add_unit(ipc::TimeUnit::SECOND);
                }
                TimeUnit::Millisecond => {
                    builder.add_bitWidth(32);
                    builder.add_unit(ipc::TimeUnit::MILLISECOND);
                }
                TimeUnit::Microsecond => {
                    builder.add_bitWidth(64);
                    builder.add_unit(ipc::TimeUnit::MICROSECOND);
                }
                TimeUnit::Nanosecond => {
                    builder.add_bitWidth(64);
                    builder.add_unit(ipc::TimeUnit::NANOSECOND);
                }
            }
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Timestamp(unit, tz) => {
            let tz = tz.clone().unwrap_or_else(String::new);
            let tz_str = fbb.create_string(tz.as_str());
            let mut builder = ipc::TimestampBuilder::new(fbb);
            let time_unit = match unit {
                TimeUnit::Second => ipc::TimeUnit::SECOND,
                TimeUnit::Millisecond => ipc::TimeUnit::MILLISECOND,
                TimeUnit::Microsecond => ipc::TimeUnit::MICROSECOND,
                TimeUnit::Nanosecond => ipc::TimeUnit::NANOSECOND,
            };
            builder.add_unit(time_unit);
            if !tz.is_empty() {
                builder.add_timezone(tz_str);
            }
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Interval(unit) => {
            let mut builder = ipc::IntervalBuilder::new(fbb);
            let interval_unit = match unit {
                IntervalUnit::YearMonth => ipc::IntervalUnit::YEAR_MONTH,
                IntervalUnit::DayTime => ipc::IntervalUnit::DAY_TIME,
            };
            builder.add_unit(interval_unit);
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Duration(unit) => {
            let mut builder = ipc::DurationBuilder::new(fbb);
            let time_unit = match unit {
                TimeUnit::Second => ipc::TimeUnit::SECOND,
                TimeUnit::Millisecond => ipc::TimeUnit::MILLISECOND,
                TimeUnit::Microsecond => ipc::TimeUnit::MICROSECOND,
                TimeUnit::Nanosecond => ipc::TimeUnit::NANOSECOND,
            };
            builder.add_unit(time_unit);
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        List(ref list_type) => {
            let child = build_field(fbb, list_type);
            FbFieldType {
                type_type,
                type_: ipc::ListBuilder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        LargeList(ref list_type) => {
            let child = build_field(fbb, list_type);
            FbFieldType {
                type_type,
                type_: ipc::LargeListBuilder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        FixedSizeList(ref list_type, len) => {
            let child = build_field(fbb, list_type);
            let mut builder = ipc::FixedSizeListBuilder::new(fbb);
            builder.add_listSize(*len as i32);
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        Struct(fields) => {
            // struct's fields are children
            let mut children = vec![];
            for field in fields {
                let inner_types = get_fb_field_type(field.data_type(), field.is_nullable(), fbb);
                let field_name = fbb.create_string(field.name());
                children.push(ipc::Field::create(
                    fbb,
                    &ipc::FieldArgs {
                        name: Some(field_name),
                        nullable: field.is_nullable(),
                        type_type: inner_types.type_type,
                        type_: Some(inner_types.type_),
                        dictionary: None,
                        children: inner_types.children,
                        custom_metadata: None,
                    },
                ));
            }
            FbFieldType {
                type_type,
                type_: ipc::Struct_Builder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&children[..])),
            }
        }
        Dictionary(_, value_type) => {
            // In this library, the dictionary "type" is a logical construct. Here we
            // pass through to the value type, as we've already captured the index
            // type in the DictionaryEncoding metadata in the parent field
            get_fb_field_type(value_type, is_nullable, fbb)
        }
        Extension(_, value_type, _) => get_fb_field_type(value_type, is_nullable, fbb),
        Decimal(precision, scale) => {
            let mut builder = ipc::DecimalBuilder::new(fbb);
            builder.add_precision(*precision as i32);
            builder.add_scale(*scale as i32);
            builder.add_bitWidth(128);
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&empty_fields[..])),
            }
        }
        Union(fields, ids, is_sparse) => {
            let children: Vec<_> = fields.iter().map(|field| build_field(fbb, field)).collect();

            let ids = ids.as_ref().map(|ids| fbb.create_vector(ids));

            let mut builder = ipc::UnionBuilder::new(fbb);
            builder.add_mode(if *is_sparse {
                UnionMode::Sparse
            } else {
                UnionMode::Dense
            });

            if let Some(ids) = ids {
                builder.add_typeIds(ids);
            }
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&children)),
            }
        }
    }
}

/// Create an IPC dictionary encoding
pub(crate) fn get_fb_dictionary<'a>(
    index_type: &DataType,
    dict_id: i64,
    dict_is_ordered: bool,
    fbb: &mut FlatBufferBuilder<'a>,
) -> WIPOffset<ipc::DictionaryEncoding<'a>> {
    // We assume that the dictionary index type (as an integer) has already been
    // validated elsewhere, and can safely assume we are dealing with integers
    let mut index_builder = ipc::IntBuilder::new(fbb);

    match *index_type {
        Int8 | Int16 | Int32 | Int64 => index_builder.add_is_signed(true),
        UInt8 | UInt16 | UInt32 | UInt64 => index_builder.add_is_signed(false),
        _ => {}
    }

    match *index_type {
        Int8 | UInt8 => index_builder.add_bitWidth(8),
        Int16 | UInt16 => index_builder.add_bitWidth(16),
        Int32 | UInt32 => index_builder.add_bitWidth(32),
        Int64 | UInt64 => index_builder.add_bitWidth(64),
        _ => {}
    }

    let index_builder = index_builder.finish();

    let mut builder = ipc::DictionaryEncodingBuilder::new(fbb);
    builder.add_id(dict_id);
    builder.add_indexType(index_builder);
    builder.add_isOrdered(dict_is_ordered);

    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::{DataType, Field, Schema};

    /// Serialize a schema in IPC format
    fn schema_to_fb(schema: &Schema) -> FlatBufferBuilder {
        let mut fbb = FlatBufferBuilder::new();

        let root = schema_to_fb_offset(&mut fbb, schema);

        fbb.finish(root, None);

        fbb
    }

    #[test]
    fn convert_schema_round_trip() {
        let md: HashMap<String, String> = [("Key".to_string(), "value".to_string())]
            .iter()
            .cloned()
            .collect();
        let field_md: BTreeMap<String, String> = [("k".to_string(), "v".to_string())]
            .iter()
            .cloned()
            .collect();
        let schema = Schema::new_from(
            vec![
                {
                    let mut f = Field::new("uint8", DataType::UInt8, false);
                    f.set_metadata(Some(field_md));
                    f
                },
                Field::new("uint16", DataType::UInt16, true),
                Field::new("uint32", DataType::UInt32, false),
                Field::new("uint64", DataType::UInt64, true),
                Field::new("int8", DataType::Int8, true),
                Field::new("int16", DataType::Int16, false),
                Field::new("int32", DataType::Int32, true),
                Field::new("int64", DataType::Int64, false),
                Field::new("float16", DataType::Float16, true),
                Field::new("float32", DataType::Float32, false),
                Field::new("float64", DataType::Float64, true),
                Field::new("null", DataType::Null, false),
                Field::new("bool", DataType::Boolean, false),
                Field::new("date32", DataType::Date32, false),
                Field::new("date64", DataType::Date64, true),
                Field::new("time32[s]", DataType::Time32(TimeUnit::Second), true),
                Field::new("time32[ms]", DataType::Time32(TimeUnit::Millisecond), false),
                Field::new("time64[us]", DataType::Time64(TimeUnit::Microsecond), false),
                Field::new("time64[ns]", DataType::Time64(TimeUnit::Nanosecond), true),
                Field::new(
                    "timestamp[s]",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
                Field::new(
                    "timestamp[ms]",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new(
                    "timestamp[us]",
                    DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("Africa/Johannesburg".to_string()),
                    ),
                    false,
                ),
                Field::new(
                    "timestamp[ns]",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    "interval[ym]",
                    DataType::Interval(IntervalUnit::YearMonth),
                    true,
                ),
                Field::new(
                    "interval[dt]",
                    DataType::Interval(IntervalUnit::DayTime),
                    true,
                ),
                Field::new("utf8", DataType::Utf8, false),
                Field::new("binary", DataType::Binary, false),
                Field::new(
                    "list[u8]",
                    DataType::List(Box::new(Field::new("item", DataType::UInt8, false))),
                    true,
                ),
                Field::new(
                    "list[struct<float32, int32, bool>]",
                    DataType::List(Box::new(Field::new(
                        "struct",
                        DataType::Struct(vec![
                            Field::new("float32", DataType::UInt8, false),
                            Field::new("int32", DataType::Int32, true),
                            Field::new("bool", DataType::Boolean, true),
                        ]),
                        true,
                    ))),
                    false,
                ),
                Field::new(
                    "struct<int64, list[struct<date32, list[struct<>]>]>",
                    DataType::Struct(vec![
                        Field::new("int64", DataType::Int64, true),
                        Field::new(
                            "list[struct<date32, list[struct<>]>]",
                            DataType::List(Box::new(Field::new(
                                "struct",
                                DataType::Struct(vec![
                                    Field::new("date32", DataType::Date32, true),
                                    Field::new(
                                        "list[struct<>]",
                                        DataType::List(Box::new(Field::new(
                                            "struct",
                                            DataType::Struct(vec![]),
                                            false,
                                        ))),
                                        false,
                                    ),
                                ]),
                                false,
                            ))),
                            false,
                        ),
                    ]),
                    false,
                ),
                Field::new("struct<>", DataType::Struct(vec![]), true),
                Field::new_dict(
                    "dictionary<int32, utf8>",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                    123,
                    true,
                ),
                Field::new_dict(
                    "dictionary<uint8, uint32>",
                    DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt32)),
                    true,
                    123,
                    true,
                ),
                Field::new("decimal<usize, usize>", DataType::Decimal(10, 6), false),
            ],
            md,
        );

        let fb = schema_to_fb(&schema);

        // read back fields
        let ipc = ipc::root_as_schema(fb.finished_data()).unwrap();
        let (schema2, _) = fb_to_schema(ipc);
        assert_eq!(schema, schema2);
    }
}
