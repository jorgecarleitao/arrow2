mod ipc {
    pub use arrow_format::ipc::File::*;
    pub use arrow_format::ipc::Message::*;
    pub use arrow_format::ipc::Schema::*;
}

use crate::{
    datatypes::{
        get_extension, DataType, Extension, Field, IntegerType, IntervalUnit, Metadata, Schema,
        TimeUnit, UnionMode,
    },
    error::{ArrowError, Result},
};

use super::super::{IpcField, IpcSchema};

fn try_unzip_vec<A, B, I: Iterator<Item = Result<(A, B)>>>(iter: I) -> Result<(Vec<A>, Vec<B>)> {
    let mut a = vec![];
    let mut b = vec![];
    for maybe_item in iter {
        let (a_i, b_i) = maybe_item?;
        a.push(a_i);
        b.push(b_i);
    }

    Ok((a, b))
}

fn deserialize_field(ipc_field: ipc::Field) -> Result<(Field, IpcField)> {
    let metadata = read_metadata(&ipc_field);

    let extension = get_extension(&metadata);

    let (data_type, ipc_field_) = get_data_type(ipc_field, extension, true)?;

    let field = Field {
        name: ipc_field
            .name()
            .ok_or_else(|| ArrowError::oos("Every field in IPC must have a name"))?
            .to_string(),
        data_type,
        is_nullable: ipc_field.nullable(),
        metadata,
    };

    Ok((field, ipc_field_))
}

fn read_metadata(field: &ipc::Field) -> Metadata {
    if let Some(list) = field.custom_metadata() {
        let mut metadata_map = Metadata::new();
        for kv in list {
            if let (Some(k), Some(v)) = (kv.key(), kv.value()) {
                metadata_map.insert(k.to_string(), v.to_string());
            }
        }
        metadata_map
    } else {
        Metadata::default()
    }
}

fn deserialize_integer(int: ipc::Int) -> Result<IntegerType> {
    Ok(match (int.bitWidth(), int.is_signed()) {
        (8, true) => IntegerType::Int8,
        (8, false) => IntegerType::UInt8,
        (16, true) => IntegerType::Int16,
        (16, false) => IntegerType::UInt16,
        (32, true) => IntegerType::Int32,
        (32, false) => IntegerType::UInt32,
        (64, true) => IntegerType::Int64,
        (64, false) => IntegerType::UInt64,
        _ => {
            return Err(ArrowError::oos(
                "IPC: indexType can only be 8, 16, 32 or 64.",
            ))
        }
    })
}

/// Get the Arrow data type from the flatbuffer Field table
fn get_data_type(
    field: ipc::Field,
    extension: Extension,
    may_be_dictionary: bool,
) -> Result<(DataType, IpcField)> {
    if let Some(dictionary) = field.dictionary() {
        if may_be_dictionary {
            let int = dictionary
                .indexType()
                .ok_or_else(|| ArrowError::oos("indexType is mandatory in Dictionary."))?;
            let index_type = deserialize_integer(int)?;
            let (inner, mut ipc_field) = get_data_type(field, extension, false)?;
            ipc_field.dictionary_id = Some(dictionary.id());
            return Ok((
                DataType::Dictionary(index_type, Box::new(inner), dictionary.isOrdered()),
                ipc_field,
            ));
        }
    }

    if let Some(extension) = extension {
        let (name, metadata) = extension;
        let (data_type, fields) = get_data_type(field, None, false)?;
        return Ok((
            DataType::Extension(name, Box::new(data_type), metadata),
            fields,
        ));
    }

    Ok(match field.type_type() {
        ipc::Type::Null => (DataType::Null, IpcField::default()),
        ipc::Type::Bool => (DataType::Boolean, IpcField::default()),
        ipc::Type::Int => {
            let int = field
                .type_as_int()
                .ok_or_else(|| ArrowError::oos("IPC: Integer type must be an integer"))?;
            let data_type = deserialize_integer(int)?.into();
            (data_type, IpcField::default())
        }
        ipc::Type::Binary => (DataType::Binary, IpcField::default()),
        ipc::Type::LargeBinary => (DataType::LargeBinary, IpcField::default()),
        ipc::Type::Utf8 => (DataType::Utf8, IpcField::default()),
        ipc::Type::LargeUtf8 => (DataType::LargeUtf8, IpcField::default()),
        ipc::Type::FixedSizeBinary => {
            let fsb = field.type_as_fixed_size_binary().ok_or_else(|| {
                ArrowError::oos("IPC: FixedSizeBinary type must be a FixedSizeBinary")
            })?;
            (
                DataType::FixedSizeBinary(fsb.byteWidth() as usize),
                IpcField::default(),
            )
        }
        ipc::Type::FloatingPoint => {
            let float = field.type_as_floating_point().ok_or_else(|| {
                ArrowError::oos("IPC: FloatingPoint type must be a FloatingPoint")
            })?;
            let data_type = match float.precision() {
                ipc::Precision::HALF => DataType::Float16,
                ipc::Precision::SINGLE => DataType::Float32,
                ipc::Precision::DOUBLE => DataType::Float64,
                z => return Err(ArrowError::nyi(format!("IPC: float of precision {:?}", z))),
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Date => {
            let date = field
                .type_as_date()
                .ok_or_else(|| ArrowError::oos("IPC: Date type must be a Date"))?;
            let data_type = match date.unit() {
                ipc::DateUnit::DAY => DataType::Date32,
                ipc::DateUnit::MILLISECOND => DataType::Date64,
                z => {
                    return Err(ArrowError::nyi(format!(
                        "IPC: date unit of precision {:?}",
                        z
                    )))
                }
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Time => {
            let time = field
                .type_as_time()
                .ok_or_else(|| ArrowError::oos("IPC: Time type must be a Time"))?;
            let data_type = match (time.bitWidth(), time.unit()) {
                (32, ipc::TimeUnit::SECOND) => DataType::Time32(TimeUnit::Second),
                (32, ipc::TimeUnit::MILLISECOND) => DataType::Time32(TimeUnit::Millisecond),
                (64, ipc::TimeUnit::MICROSECOND) => DataType::Time64(TimeUnit::Microsecond),
                (64, ipc::TimeUnit::NANOSECOND) => DataType::Time64(TimeUnit::Nanosecond),
                (bits, precision) => {
                    return Err(ArrowError::nyi(format!(
                        "Time type with bit width of {} and unit of {:?}",
                        bits, precision
                    )))
                }
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Timestamp => {
            let timestamp = field
                .type_as_timestamp()
                .ok_or_else(|| ArrowError::oos("IPC: Timestamp type must be a Timestamp"))?;
            let timezone: Option<String> = timestamp.timezone().map(|tz| tz.to_string());
            let data_type = match timestamp.unit() {
                ipc::TimeUnit::SECOND => DataType::Timestamp(TimeUnit::Second, timezone),
                ipc::TimeUnit::MILLISECOND => DataType::Timestamp(TimeUnit::Millisecond, timezone),
                ipc::TimeUnit::MICROSECOND => DataType::Timestamp(TimeUnit::Microsecond, timezone),
                ipc::TimeUnit::NANOSECOND => DataType::Timestamp(TimeUnit::Nanosecond, timezone),
                z => {
                    return Err(ArrowError::nyi(format!(
                        "Timestamp type with unit of {:?}",
                        z
                    )))
                }
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Interval => {
            let interval = field
                .type_as_interval()
                .ok_or_else(|| ArrowError::oos("IPC: Interval type must be a Interval"))?;
            let data_type = match interval.unit() {
                ipc::IntervalUnit::YEAR_MONTH => DataType::Interval(IntervalUnit::YearMonth),
                ipc::IntervalUnit::DAY_TIME => DataType::Interval(IntervalUnit::DayTime),
                ipc::IntervalUnit::MONTH_DAY_NANO => DataType::Interval(IntervalUnit::MonthDayNano),
                z => {
                    return Err(ArrowError::nyi(format!(
                        "Interval type with unit of {:?}",
                        z
                    )))
                }
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Duration => {
            let duration = field
                .type_as_duration()
                .ok_or_else(|| ArrowError::oos("IPC: Duration type must be a Duration"))?;
            let data_type = match duration.unit() {
                ipc::TimeUnit::SECOND => DataType::Duration(TimeUnit::Second),
                ipc::TimeUnit::MILLISECOND => DataType::Duration(TimeUnit::Millisecond),
                ipc::TimeUnit::MICROSECOND => DataType::Duration(TimeUnit::Microsecond),
                ipc::TimeUnit::NANOSECOND => DataType::Duration(TimeUnit::Nanosecond),
                z => {
                    return Err(ArrowError::nyi(format!(
                        "Duration type with unit of {:?}",
                        z
                    )))
                }
            };
            (data_type, IpcField::default())
        }
        ipc::Type::Decimal => {
            let fsb = field
                .type_as_decimal()
                .ok_or_else(|| ArrowError::oos("IPC: Decimal type must be a Decimal"))?;
            let data_type = DataType::Decimal(fsb.precision() as usize, fsb.scale() as usize);
            (data_type, IpcField::default())
        }
        ipc::Type::List => {
            let children = field
                .children()
                .ok_or_else(|| ArrowError::oos("IPC: List must contain children"))?;
            if children.len() != 1 {
                return Err(ArrowError::oos("IPC: List must contain one child"));
            }
            let (field, ipc_field) = deserialize_field(children.get(0))?;

            (
                DataType::List(Box::new(field)),
                IpcField {
                    fields: vec![ipc_field],
                    dictionary_id: None,
                },
            )
        }
        ipc::Type::LargeList => {
            let children = field
                .children()
                .ok_or_else(|| ArrowError::oos("IPC: LargeList must contain children"))?;
            if children.len() != 1 {
                return Err(ArrowError::oos("IPC: LargeList must contain one child"));
            }
            let (field, ipc_field) = deserialize_field(children.get(0))?;

            (
                DataType::LargeList(Box::new(field)),
                IpcField {
                    fields: vec![ipc_field],
                    dictionary_id: None,
                },
            )
        }
        ipc::Type::FixedSizeList => {
            let fsl = field.type_as_fixed_size_list().ok_or_else(|| {
                ArrowError::oos("IPC: FixedSizeList type must be a FixedSizeList")
            })?;
            let size = fsl.listSize() as usize;
            let children = field
                .children()
                .ok_or_else(|| ArrowError::oos("IPC: FixedSizeList must contain children"))?;
            if children.len() != 1 {
                return Err(ArrowError::oos("IPC: FixedSizeList must contain one child"));
            }
            let (field, ipc_field) = deserialize_field(children.get(0))?;

            (
                DataType::FixedSizeList(Box::new(field), size),
                IpcField {
                    fields: vec![ipc_field],
                    dictionary_id: None,
                },
            )
        }
        ipc::Type::Struct_ => {
            let fields = field
                .children()
                .ok_or_else(|| ArrowError::oos("IPC: Struct must contain children"))?;
            if fields.is_empty() {
                return Err(ArrowError::oos(
                    "IPC: Struct must contain at least one child",
                ));
            }
            let (fields, ipc_fields) = try_unzip_vec(fields.iter().map(|field| {
                let (field, fields) = deserialize_field(field)?;
                Ok((field, fields))
            }))?;
            let ipc_field = IpcField {
                fields: ipc_fields,
                dictionary_id: None,
            };
            (DataType::Struct(fields), ipc_field)
        }
        ipc::Type::Union => {
            let type_ = field
                .type_as_union()
                .ok_or_else(|| ArrowError::oos("IPC: Union type must be a Union"))?;
            let mode = UnionMode::sparse(type_.mode() == ipc::UnionMode::Sparse);
            let ids = type_.typeIds().map(|x| x.iter().collect());

            let fields = field
                .children()
                .ok_or_else(|| ArrowError::oos("IPC: Union must contain children"))?;
            if fields.is_empty() {
                return Err(ArrowError::oos(
                    "IPC: Union must contain at least one child",
                ));
            }

            let (fields, ipc_fields) = try_unzip_vec(fields.iter().map(|field| {
                let (field, fields) = deserialize_field(field)?;
                Ok((field, fields))
            }))?;
            let ipc_field = IpcField {
                fields: ipc_fields,
                dictionary_id: None,
            };
            (DataType::Union(fields, ids, mode), ipc_field)
        }
        ipc::Type::Map => {
            let map = field
                .type_as_map()
                .ok_or_else(|| ArrowError::oos("IPC: Map type must be a Map"))?;
            let is_sorted = map.keysSorted();

            let children = field
                .children()
                .ok_or_else(|| ArrowError::oos("IPC: Map must contain children"))?;
            if children.len() != 1 {
                return Err(ArrowError::oos("IPC: Map must contain one child"));
            }
            let (field, ipc_field) = deserialize_field(children.get(0))?;

            let data_type = DataType::Map(Box::new(field), is_sorted);
            (
                data_type,
                IpcField {
                    fields: vec![ipc_field],
                    dictionary_id: None,
                },
            )
        }
        t => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Reading {:?} from IPC",
                t
            )))
        }
    })
}

/// Deserialize the raw Schema table from IPC format to Schema data type
pub fn fb_to_schema(fb: ipc::Schema) -> Result<(Schema, IpcSchema)> {
    let fields = fb
        .fields()
        .ok_or_else(|| ArrowError::oos("IPC: Schema must contain fields"))?;
    let (fields, ipc_fields) = try_unzip_vec(fields.iter().map(|field| {
        let (field, fields) = deserialize_field(field)?;
        Ok((field, fields))
    }))?;

    let is_little_endian = fb.endianness().variant_name().unwrap_or("Little") == "Little";

    let metadata = if let Some(md_fields) = fb.custom_metadata() {
        md_fields
            .iter()
            .filter_map(|f| {
                let k = f.key();
                let v = f.value();
                k.and_then(|k| v.map(|v| (k.to_string(), v.to_string())))
            })
            .collect()
    } else {
        Default::default()
    };

    Ok((
        Schema { fields, metadata },
        IpcSchema {
            fields: ipc_fields,
            is_little_endian,
        },
    ))
}
