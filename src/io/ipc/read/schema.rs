use std::collections::{BTreeMap, HashMap};

mod ipc {
    pub use arrow_format::ipc::File::*;
    pub use arrow_format::ipc::Message::*;
    pub use arrow_format::ipc::Schema::*;
}

use crate::datatypes::{
    get_extension, DataType, Extension, Field, IntegerType, IntervalUnit, Metadata, Schema,
    TimeUnit, UnionMode,
};

use super::super::{IpcField, IpcSchema};

fn deserialize_field(ipc_field: ipc::Field) -> (Field, IpcField) {
    let metadata = read_metadata(&ipc_field);

    let extension = get_extension(&metadata);

    let (data_type, ipc_field_) = get_data_type(ipc_field, extension, true);

    let field = Field {
        name: ipc_field.name().unwrap().to_string(),
        data_type,
        nullable: ipc_field.nullable(),
        metadata,
    };

    (field, ipc_field_)
}

fn read_metadata(field: &ipc::Field) -> Metadata {
    if let Some(list) = field.custom_metadata() {
        if !list.is_empty() {
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
    } else {
        None
    }
}

/// Get the Arrow data type from the flatbuffer Field table
fn get_data_type(
    field: ipc::Field,
    extension: Extension,
    may_be_dictionary: bool,
) -> (DataType, IpcField) {
    if let Some(dictionary) = field.dictionary() {
        if may_be_dictionary {
            let int = dictionary.indexType().unwrap();
            let index_type = match (int.bitWidth(), int.is_signed()) {
                (8, true) => IntegerType::Int8,
                (8, false) => IntegerType::UInt8,
                (16, true) => IntegerType::Int16,
                (16, false) => IntegerType::UInt16,
                (32, true) => IntegerType::Int32,
                (32, false) => IntegerType::UInt32,
                (64, true) => IntegerType::Int64,
                (64, false) => IntegerType::UInt64,
                _ => panic!("Unexpected bitwidth and signed"),
            };
            let (inner, mut ipc_field) = get_data_type(field, extension, false);
            ipc_field.dictionary_id = Some(dictionary.id());
            return (
                DataType::Dictionary(index_type, Box::new(inner), dictionary.isOrdered()),
                ipc_field,
            );
        }
    }

    if let Some(extension) = extension {
        let (name, metadata) = extension;
        let (data_type, fields) = get_data_type(field, None, false);
        return (
            DataType::Extension(name, Box::new(data_type), metadata),
            fields,
        );
    }

    match field.type_type() {
        ipc::Type::Null => (DataType::Null, IpcField::default()),
        ipc::Type::Bool => (DataType::Boolean, IpcField::default()),
        ipc::Type::Int => {
            let int = field.type_as_int().unwrap();
            let data_type = match (int.bitWidth(), int.is_signed()) {
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
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Binary => (DataType::Binary, IpcField::default()),
        ipc::Type::LargeBinary => (DataType::LargeBinary, IpcField::default()),
        ipc::Type::Utf8 => (DataType::Utf8, IpcField::default()),
        ipc::Type::LargeUtf8 => (DataType::LargeUtf8, IpcField::default()),
        ipc::Type::FixedSizeBinary => {
            let fsb = field.type_as_fixed_size_binary().unwrap();
            (
                DataType::FixedSizeBinary(fsb.byteWidth() as usize),
                IpcField::default(),
            )
        }
        ipc::Type::FloatingPoint => {
            let float = field.type_as_floating_point().unwrap();
            let data_type = match float.precision() {
                ipc::Precision::HALF => DataType::Float16,
                ipc::Precision::SINGLE => DataType::Float32,
                ipc::Precision::DOUBLE => DataType::Float64,
                z => panic!("FloatingPoint type with precision of {:?} not supported", z),
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Date => {
            let date = field.type_as_date().unwrap();
            let data_type = match date.unit() {
                ipc::DateUnit::DAY => DataType::Date32,
                ipc::DateUnit::MILLISECOND => DataType::Date64,
                z => panic!("Date type with unit of {:?} not supported", z),
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Time => {
            let time = field.type_as_time().unwrap();
            let data_type = match (time.bitWidth(), time.unit()) {
                (32, ipc::TimeUnit::SECOND) => DataType::Time32(TimeUnit::Second),
                (32, ipc::TimeUnit::MILLISECOND) => DataType::Time32(TimeUnit::Millisecond),
                (64, ipc::TimeUnit::MICROSECOND) => DataType::Time64(TimeUnit::Microsecond),
                (64, ipc::TimeUnit::NANOSECOND) => DataType::Time64(TimeUnit::Nanosecond),
                z => panic!(
                    "Time type with bit width of {} and unit of {:?} not supported",
                    z.0, z.1
                ),
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Timestamp => {
            let timestamp = field.type_as_timestamp().unwrap();
            let timezone: Option<String> = timestamp.timezone().map(|tz| tz.to_string());
            let data_type = match timestamp.unit() {
                ipc::TimeUnit::SECOND => DataType::Timestamp(TimeUnit::Second, timezone),
                ipc::TimeUnit::MILLISECOND => DataType::Timestamp(TimeUnit::Millisecond, timezone),
                ipc::TimeUnit::MICROSECOND => DataType::Timestamp(TimeUnit::Microsecond, timezone),
                ipc::TimeUnit::NANOSECOND => DataType::Timestamp(TimeUnit::Nanosecond, timezone),
                z => panic!("Timestamp type with unit of {:?} not supported", z),
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Interval => {
            let interval = field.type_as_interval().unwrap();
            let data_type = match interval.unit() {
                ipc::IntervalUnit::YEAR_MONTH => DataType::Interval(IntervalUnit::YearMonth),
                ipc::IntervalUnit::DAY_TIME => DataType::Interval(IntervalUnit::DayTime),
                ipc::IntervalUnit::MONTH_DAY_NANO => DataType::Interval(IntervalUnit::MonthDayNano),
                z => panic!("Interval type with unit of {:?} unsupported", z),
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Duration => {
            let duration = field.type_as_duration().unwrap();
            let data_type = match duration.unit() {
                ipc::TimeUnit::SECOND => DataType::Duration(TimeUnit::Second),
                ipc::TimeUnit::MILLISECOND => DataType::Duration(TimeUnit::Millisecond),
                ipc::TimeUnit::MICROSECOND => DataType::Duration(TimeUnit::Microsecond),
                ipc::TimeUnit::NANOSECOND => DataType::Duration(TimeUnit::Nanosecond),
                z => panic!("Duration type with unit of {:?} unsupported", z),
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Decimal => {
            let fsb = field.type_as_decimal().unwrap();
            let data_type = DataType::Decimal(fsb.precision() as usize, fsb.scale() as usize);
            (data_type, IpcField::default())
        }
        ipc::Type::List => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a list to have one child")
            }
            let (field, ipc_field) = deserialize_field(children.get(0));

            (
                DataType::List(Box::new(field)),
                IpcField {
                    fields: vec![ipc_field],
                    dictionary_id: None,
                },
            )
        }
        ipc::Type::LargeList => {
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a large list to have one child")
            }
            let (field, ipc_field) = deserialize_field(children.get(0));

            (
                DataType::LargeList(Box::new(field)),
                IpcField {
                    fields: vec![ipc_field],
                    dictionary_id: None,
                },
            )
        }
        ipc::Type::FixedSizeList => {
            let fsl = field.type_as_fixed_size_list().unwrap();
            let size = fsl.listSize() as usize;
            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a list to have one child")
            }
            let (field, ipc_field) = deserialize_field(children.get(0));

            (
                DataType::FixedSizeList(Box::new(field), size),
                IpcField {
                    fields: vec![ipc_field],
                    dictionary_id: None,
                },
            )
        }
        ipc::Type::Struct_ => {
            let fields = field.children().unwrap();
            if fields.is_empty() {
                panic!("expect a struct to have at least one child")
            }
            let (fields, ipc_fields): (Vec<_>, Vec<_>) = (0..fields.len())
                .map(|field| {
                    let field = fields.get(field);
                    let (field, fields) = deserialize_field(field);
                    (field, fields)
                })
                .unzip();
            let ipc_field = IpcField {
                fields: ipc_fields,
                dictionary_id: None,
            };
            (DataType::Struct(fields), ipc_field)
        }
        ipc::Type::Union => {
            let type_ = field.type_as_union().unwrap();
            let mode = UnionMode::sparse(type_.mode() == ipc::UnionMode::Sparse);
            let ids = type_.typeIds().map(|x| x.iter().collect());

            let fields = field.children().unwrap();
            if fields.is_empty() {
                panic!("expect a struct to have at least one child")
            }

            let (fields, ipc_fields): (Vec<_>, Vec<_>) = (0..fields.len())
                .map(|field| {
                    let field = fields.get(field);
                    let (field, fields) = deserialize_field(field);
                    (field, fields)
                })
                .unzip();
            let ipc_field = IpcField {
                fields: ipc_fields,
                dictionary_id: None,
            };
            (DataType::Union(fields, ids, mode), ipc_field)
        }
        ipc::Type::Map => {
            let map = field.type_as_map().unwrap();
            let is_sorted = map.keysSorted();

            let children = field.children().unwrap();
            if children.len() != 1 {
                panic!("expect a list to have one child")
            }
            let (field, ipc_field) = deserialize_field(children.get(0));

            let data_type = DataType::Map(Box::new(field), is_sorted);
            (
                data_type,
                IpcField {
                    fields: vec![ipc_field],
                    dictionary_id: None,
                },
            )
        }
        t => unimplemented!("Type {:?} not supported", t),
    }
}

/// Deserialize the raw Schema table from IPC format to Schema data type
pub fn fb_to_schema(fb: ipc::Schema) -> (Schema, IpcSchema) {
    let fields = fb.fields().unwrap();
    let (fields, ipc_fields): (Vec<_>, Vec<_>) = (0..fields.len())
        .map(|field| {
            let field = fields.get(field);
            let (field, fields) = deserialize_field(field);
            (field, fields)
        })
        .unzip();

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

    (
        Schema { fields, metadata },
        IpcSchema {
            fields: ipc_fields,
            is_little_endian,
        },
    )
}
