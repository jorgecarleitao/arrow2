use arrow_format::ipc::flatbuffers::{
    FlatBufferBuilder, ForwardsUOffset, UnionWIPOffset, Vector, WIPOffset,
};
mod ipc {
    pub use arrow_format::ipc::File::*;
    pub use arrow_format::ipc::Message::*;
    pub use arrow_format::ipc::Schema::*;
}

use crate::datatypes::{DataType, Field, IntegerType, IntervalUnit, Metadata, Schema, TimeUnit};
use crate::io::ipc::endianess::is_native_little_endian;

use super::super::IpcField;

/// Converts
pub fn schema_to_bytes(schema: &Schema, ipc_fields: &[IpcField]) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let schema = {
        let fb = schema_to_fb_offset(&mut fbb, schema, ipc_fields);
        fb.as_union_value()
    };

    let mut message = ipc::MessageBuilder::new(&mut fbb);
    message.add_version(ipc::MetadataVersion::V5);
    message.add_header_type(ipc::MessageHeader::Schema);
    message.add_bodyLength(0);
    message.add_header(schema);
    // TODO: custom metadata
    let data = message.finish();
    fbb.finish(data, None);

    fbb.finished_data().to_vec()
}

pub fn schema_to_fb_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    schema: &Schema,
    ipc_fields: &[IpcField],
) -> WIPOffset<ipc::Schema<'a>> {
    let fields = schema
        .fields
        .iter()
        .zip(ipc_fields.iter())
        .map(|(field, ipc_field)| build_field(fbb, field, ipc_field))
        .collect::<Vec<_>>();

    let mut custom_metadata = vec![];
    for (k, v) in &schema.metadata {
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

pub(crate) struct FbFieldType<'b> {
    pub(crate) type_type: ipc::Type,
    pub(crate) type_: WIPOffset<UnionWIPOffset>,
    pub(crate) children: Option<WIPOffset<Vector<'b, ForwardsUOffset<ipc::Field<'b>>>>>,
}

fn write_metadata<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    metadata: &Metadata,
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

fn write_extension<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    name: &str,
    metadata: &Option<String>,
    kv_vec: &mut Vec<WIPOffset<ipc::KeyValue<'a>>>,
) {
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
        value: Some(fbb.create_string(name)),
    };
    kv_vec.push(ipc::KeyValue::create(fbb, &kv_args));
}

/// Create an IPC Field from an Arrow Field
pub(crate) fn build_field<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    field: &Field,
    ipc_field: &IpcField,
) -> WIPOffset<ipc::Field<'a>> {
    // custom metadata.
    let mut kv_vec = vec![];
    if let DataType::Extension(name, _, metadata) = field.data_type() {
        write_extension(fbb, name, metadata, &mut kv_vec);
    }

    let fb_field_name = fbb.create_string(field.name.as_str());
    let field_type = get_fb_field_type(field.data_type(), ipc_field, field.is_nullable, fbb);

    let fb_dictionary =
        if let DataType::Dictionary(index_type, inner, is_ordered) = field.data_type() {
            if let DataType::Extension(name, _, metadata) = inner.as_ref() {
                write_extension(fbb, name, metadata, &mut kv_vec);
            }
            Some(get_fb_dictionary(
                index_type,
                ipc_field
                    .dictionary_id
                    .expect("All Dictionary types have `dict_id`"),
                *is_ordered,
                fbb,
            ))
        } else {
            None
        };

    write_metadata(fbb, &field.metadata, &mut kv_vec);

    let fb_metadata = if !kv_vec.is_empty() {
        Some(fbb.create_vector(&kv_vec))
    } else {
        None
    };

    let mut field_builder = ipc::FieldBuilder::new(fbb);
    field_builder.add_name(fb_field_name);
    if let Some(dictionary) = fb_dictionary {
        field_builder.add_dictionary(dictionary)
    }
    field_builder.add_type_type(field_type.type_type);
    field_builder.add_nullable(field.is_nullable);
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
    use DataType::*;
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
        Map(_, _) => ipc::Type::Map,
        Struct(_) => ipc::Type::Struct_,
        Dictionary(_, v, _) => type_to_field_type(v),
        Extension(_, v, _) => type_to_field_type(v),
    }
}

/// Get the IPC type of a data type
pub(crate) fn get_fb_field_type<'a>(
    data_type: &DataType,
    ipc_field: &IpcField,
    is_nullable: bool,
    fbb: &mut FlatBufferBuilder<'a>,
) -> FbFieldType<'a> {
    use DataType::*;
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
                IntervalUnit::MonthDayNano => ipc::IntervalUnit::MONTH_DAY_NANO,
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
            let child = build_field(fbb, list_type, &ipc_field.fields[0]);
            FbFieldType {
                type_type,
                type_: ipc::ListBuilder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        LargeList(ref list_type) => {
            let child = build_field(fbb, list_type, &ipc_field.fields[0]);
            FbFieldType {
                type_type,
                type_: ipc::LargeListBuilder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        FixedSizeList(ref list_type, len) => {
            let child = build_field(fbb, list_type, &ipc_field.fields[0]);
            let mut builder = ipc::FixedSizeListBuilder::new(fbb);
            builder.add_listSize(*len as i32);
            FbFieldType {
                type_type,
                type_: builder.finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
        Struct(fields) => {
            let children: Vec<_> = fields
                .iter()
                .zip(ipc_field.fields.iter())
                .map(|(field, ipc_field)| build_field(fbb, field, ipc_field))
                .collect();

            FbFieldType {
                type_type,
                type_: ipc::Struct_Builder::new(fbb).finish().as_union_value(),
                children: Some(fbb.create_vector(&children[..])),
            }
        }
        Dictionary(_, value_type, _) => {
            // In this library, the dictionary "type" is a logical construct. Here we
            // pass through to the value type, as we've already captured the index
            // type in the DictionaryEncoding metadata in the parent field
            get_fb_field_type(value_type, ipc_field, is_nullable, fbb)
        }
        Extension(_, value_type, _) => get_fb_field_type(value_type, ipc_field, is_nullable, fbb),
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
        Union(fields, ids, mode) => {
            let children: Vec<_> = fields
                .iter()
                .zip(ipc_field.fields.iter())
                .map(|(field, ipc_field)| build_field(fbb, field, ipc_field))
                .collect();

            let ids = ids.as_ref().map(|ids| fbb.create_vector(ids));

            let mut builder = ipc::UnionBuilder::new(fbb);
            builder.add_mode(if mode.is_sparse() {
                ipc::UnionMode::Sparse
            } else {
                ipc::UnionMode::Dense
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
        Map(field, keys_sorted) => {
            let child = build_field(fbb, field, &ipc_field.fields[0]);
            let mut field_type = ipc::MapBuilder::new(fbb);
            field_type.add_keysSorted(*keys_sorted);
            FbFieldType {
                type_type: ipc::Type::Map,
                type_: field_type.finish().as_union_value(),
                children: Some(fbb.create_vector(&[child])),
            }
        }
    }
}

/// Create an IPC dictionary encoding
pub(crate) fn get_fb_dictionary<'a>(
    index_type: &IntegerType,
    dict_id: i64,
    dict_is_ordered: bool,
    fbb: &mut FlatBufferBuilder<'a>,
) -> WIPOffset<ipc::DictionaryEncoding<'a>> {
    use IntegerType::*;
    // We assume that the dictionary index type (as an integer) has already been
    // validated elsewhere, and can safely assume we are dealing with integers
    let mut index_builder = ipc::IntBuilder::new(fbb);

    match index_type {
        Int8 | Int16 | Int32 | Int64 => index_builder.add_is_signed(true),
        UInt8 | UInt16 | UInt32 | UInt64 => index_builder.add_is_signed(false),
    }

    match index_type {
        Int8 | UInt8 => index_builder.add_bitWidth(8),
        Int16 | UInt16 => index_builder.add_bitWidth(16),
        Int32 | UInt32 => index_builder.add_bitWidth(32),
        Int64 | UInt64 => index_builder.add_bitWidth(64),
    }

    let index_builder = index_builder.finish();

    let mut builder = ipc::DictionaryEncodingBuilder::new(fbb);
    builder.add_id(dict_id);
    builder.add_indexType(index_builder);
    builder.add_isOrdered(dict_is_ordered);

    builder.finish()
}

/*
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
                    DataType::Dictionary(IntegerType::Int32, Box::new(DataType::Utf8), true),
                    true,
                    123,
                ),
                Field::new_dict(
                    "dictionary<uint8, uint32>",
                    DataType::Dictionary(IntegerType::UInt8, Box::new(DataType::UInt32), true),
                    true,
                    123,
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
 */
