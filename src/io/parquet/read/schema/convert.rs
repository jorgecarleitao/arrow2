use std::collections::HashMap;

// Convert a parquet schema into Arrow schema
use parquet2::{
    metadata::{KeyValue, SchemaDescriptor},
    schema::{
        types::{
            BasicTypeInfo, GroupConvertedType, LogicalType, ParquetType, PhysicalType,
            PrimitiveConvertedType, TimeUnit as ParquetTimeUnit,
        },
        Repetition, TimestampType,
    },
};

use crate::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use crate::error::{ArrowError, Result};

fn parse_key_value_metadata(
    key_value_metadata: &Option<Vec<KeyValue>>,
) -> Option<HashMap<String, String>> {
    match key_value_metadata {
        Some(key_values) => {
            let map: HashMap<String, String> = key_values
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .map(|value| (kv.key.clone(), value.clone()))
                })
                .collect();

            if map.is_empty() {
                None
            } else {
                Some(map)
            }
        }
        None => None,
    }
}

/// Convert parquet schema to arrow schema
pub fn parquet_to_arrow_schema(
    schema: &SchemaDescriptor,
    key_value_metadata: &Option<Vec<KeyValue>>,
) -> Result<Schema> {
    let metadata = parse_key_value_metadata(key_value_metadata).unwrap_or_default();

    schema
        .fields()
        .iter()
        .map(|t| to_field(t))
        .filter_map(|x| x.transpose())
        .collect::<Result<Vec<_>>>()
        .map(|fields| Schema::new_from(fields, metadata))
}

pub fn from_int32(
    logical_type: &Option<LogicalType>,
    converted_type: &Option<PrimitiveConvertedType>,
) -> Result<DataType> {
    match (logical_type, converted_type) {
        (None, None) => Ok(DataType::Int32),
        (Some(LogicalType::INTEGER(t)), _) => match (t.bit_width, t.is_signed) {
            (8, true) => Ok(DataType::Int8),
            (16, true) => Ok(DataType::Int16),
            (32, true) => Ok(DataType::Int32),
            (8, false) => Ok(DataType::UInt8),
            (16, false) => Ok(DataType::UInt16),
            (32, false) => Ok(DataType::UInt32),
            _ => Err(ArrowError::ExternalFormat(format!(
                "Cannot create INT32 physical type from {:?}",
                t
            ))),
        },
        (Some(LogicalType::DECIMAL(t)), _) => {
            Ok(DataType::Decimal(t.precision as usize, t.scale as usize))
        }
        (Some(LogicalType::DATE(_)), _) => Ok(DataType::Date32),
        (Some(LogicalType::TIME(t)), _) => match t.unit {
            ParquetTimeUnit::MILLIS(_) => Ok(DataType::Time32(TimeUnit::Millisecond)),
            _ => Err(ArrowError::ExternalFormat(format!(
                "Cannot create INT32 physical type from {:?}",
                t.unit
            ))),
        },
        (None, Some(PrimitiveConvertedType::Uint8)) => Ok(DataType::UInt8),
        (None, Some(PrimitiveConvertedType::Uint16)) => Ok(DataType::UInt16),
        (None, Some(PrimitiveConvertedType::Uint32)) => Ok(DataType::UInt32),
        (None, Some(PrimitiveConvertedType::Int8)) => Ok(DataType::Int8),
        (None, Some(PrimitiveConvertedType::Int16)) => Ok(DataType::Int16),
        (None, Some(PrimitiveConvertedType::Int32)) => Ok(DataType::Int32),
        (None, Some(PrimitiveConvertedType::Date)) => Ok(DataType::Date32),
        (None, Some(PrimitiveConvertedType::TimeMillis)) => {
            Ok(DataType::Time32(TimeUnit::Millisecond))
        }
        (None, Some(PrimitiveConvertedType::Decimal(precision, scale))) => {
            Ok(DataType::Decimal(*precision as usize, *scale as usize))
        }
        (logical, converted) => Err(ArrowError::ExternalFormat(format!(
            "Unable to convert parquet INT32 logical type {:?} or converted type {:?}",
            logical, converted
        ))),
    }
}

pub fn from_int64(
    logical_type: &Option<LogicalType>,
    converted_type: &Option<PrimitiveConvertedType>,
) -> Result<DataType> {
    Ok(match (converted_type, logical_type) {
        (None, None) => DataType::Int64,
        (_, Some(LogicalType::INTEGER(t))) if t.bit_width == 64 => match t.is_signed {
            true => DataType::Int64,
            false => DataType::UInt64,
        },
        (Some(PrimitiveConvertedType::Int64), None) => DataType::Int64,
        (Some(PrimitiveConvertedType::Uint64), None) => DataType::UInt64,
        (
            _,
            Some(LogicalType::TIMESTAMP(TimestampType {
                is_adjusted_to_u_t_c,
                unit,
            })),
        ) => {
            let timezone = if *is_adjusted_to_u_t_c {
                // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
                // A TIMESTAMP with isAdjustedToUTC=true is defined as [...] elapsed since the Unix epoch
                Some("+00:00".to_string())
            } else {
                // PARQUET:
                // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
                // A TIMESTAMP with isAdjustedToUTC=false represents [...] such
                // timestamps should always be displayed the same way, regardless of the local time zone in effect
                // ARROW:
                // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
                // If the time zone is null or equal to an empty string, the data is "time
                // zone naive" and shall be displayed *as is* to the user, not localized
                // to the locale of the user.
                None
            };

            match unit {
                ParquetTimeUnit::MILLIS(_) => DataType::Timestamp(TimeUnit::Millisecond, timezone),
                ParquetTimeUnit::MICROS(_) => DataType::Timestamp(TimeUnit::Microsecond, timezone),
                ParquetTimeUnit::NANOS(_) => DataType::Timestamp(TimeUnit::Nanosecond, timezone),
            }
        }
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
        // *Backward compatibility:*
        // TIME_MILLIS | TimeType (isAdjustedToUTC = true, unit = MILLIS)
        // TIME_MICROS | TimeType (isAdjustedToUTC = true, unit = MICROS)
        (Some(PrimitiveConvertedType::TimeMillis), None) => DataType::Time32(TimeUnit::Millisecond),
        (Some(PrimitiveConvertedType::TimeMicros), None) => DataType::Time64(TimeUnit::Microsecond),
        (Some(PrimitiveConvertedType::TimestampMillis), None) => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
        (Some(PrimitiveConvertedType::TimestampMicros), None) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        (_, Some(LogicalType::TIME(t))) => match t.unit {
            ParquetTimeUnit::MILLIS(_) => {
                return Err(ArrowError::ExternalFormat(
                    "Cannot create INT64 from MILLIS time unit".to_string(),
                ))
            }
            ParquetTimeUnit::MICROS(_) => DataType::Time64(TimeUnit::Microsecond),
            ParquetTimeUnit::NANOS(_) => DataType::Time64(TimeUnit::Nanosecond),
        },
        (c, l) => {
            return Err(ArrowError::NotYetImplemented(format!(
                "The conversion of (Int64, {:?}, {:?}) to arrow still not implemented",
                c, l
            )))
        }
    })
}

pub fn from_byte_array(
    logical_type: &Option<LogicalType>,
    converted_type: &Option<PrimitiveConvertedType>,
) -> Result<DataType> {
    match (logical_type, converted_type) {
        (Some(LogicalType::STRING(_)), _) => Ok(DataType::Utf8),
        (Some(LogicalType::JSON(_)), _) => Ok(DataType::Binary),
        (Some(LogicalType::BSON(_)), _) => Ok(DataType::Binary),
        (Some(LogicalType::ENUM(_)), _) => Ok(DataType::Binary),
        (None, None) => Ok(DataType::Binary),
        (None, Some(PrimitiveConvertedType::Json)) => Ok(DataType::Binary),
        (None, Some(PrimitiveConvertedType::Bson)) => Ok(DataType::Binary),
        (None, Some(PrimitiveConvertedType::Enum)) => Ok(DataType::Binary),
        (None, Some(PrimitiveConvertedType::Utf8)) => Ok(DataType::Utf8),
        (logical, converted) => Err(ArrowError::ExternalFormat(format!(
            "Unable to convert parquet BYTE_ARRAY logical type {:?} or converted type {:?}",
            logical, converted
        ))),
    }
}

pub fn from_fixed_len_byte_array(
    length: &i32,
    logical_type: &Option<LogicalType>,
    converted_type: &Option<PrimitiveConvertedType>,
) -> DataType {
    match (logical_type, converted_type) {
        (Some(LogicalType::DECIMAL(t)), _) => {
            DataType::Decimal(t.precision as usize, t.scale as usize)
        }
        (None, Some(PrimitiveConvertedType::Decimal(precision, scale))) => {
            DataType::Decimal(*precision as usize, *scale as usize)
        }
        (None, Some(PrimitiveConvertedType::Interval)) => {
            // There is currently no reliable way of determining which IntervalUnit
            // to return. Thus without the original Arrow schema, the results
            // would be incorrect if all 12 bytes of the interval are populated
            DataType::Interval(IntervalUnit::DayTime)
        }
        _ => DataType::FixedSizeBinary(*length),
    }
}

/// Converting parquet primitive type to arrow data type.
fn to_primitive_type_inner(
    physical_type: &PhysicalType,
    logical_type: &Option<LogicalType>,
    converted_type: &Option<PrimitiveConvertedType>,
) -> Result<DataType> {
    match physical_type {
        PhysicalType::Boolean => Ok(DataType::Boolean),
        PhysicalType::Int32 => from_int32(logical_type, converted_type),
        PhysicalType::Int64 => from_int64(logical_type, converted_type),
        PhysicalType::Int96 => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        PhysicalType::Float => Ok(DataType::Float32),
        PhysicalType::Double => Ok(DataType::Float64),
        PhysicalType::ByteArray => from_byte_array(logical_type, converted_type),
        PhysicalType::FixedLenByteArray(length) => Ok(from_fixed_len_byte_array(
            length,
            logical_type,
            converted_type,
        )),
    }
}

/// Entry point for converting parquet primitive type to arrow type.
///
/// This function takes care of repetition.
fn to_primitive_type(
    basic_info: &BasicTypeInfo,
    physical_type: &PhysicalType,
    logical_type: &Option<LogicalType>,
    converted_type: &Option<PrimitiveConvertedType>,
) -> Result<Option<DataType>> {
    to_primitive_type_inner(physical_type, logical_type, converted_type).map(|dt| {
        Some(if basic_info.repetition() == &Repetition::Repeated {
            DataType::List(Box::new(Field::new(
                basic_info.name(),
                dt,
                is_nullable(basic_info),
            )))
        } else {
            dt
        })
    })
}

/// Converting parquet primitive type to arrow data type.
fn to_group_type_inner(
    logical_type: &Option<LogicalType>,
    converted_type: &Option<GroupConvertedType>,
    fields: &[ParquetType],
    parent_name: &str,
) -> Result<Option<DataType>> {
    match (logical_type, converted_type) {
        (Some(LogicalType::LIST(_)), _) => to_list(fields, parent_name),
        (None, Some(GroupConvertedType::List)) => to_list(fields, parent_name),
        _ => to_struct(fields),
    }
}

/// Converts a parquet group type to arrow struct.
fn to_struct(fields: &[ParquetType]) -> Result<Option<DataType>> {
    fields
        .iter()
        .map(|field| to_field(field))
        .collect::<Result<Vec<Option<Field>>>>()
        .map(|result| result.into_iter().flatten().collect::<Vec<Field>>())
        .map(|fields| {
            if fields.is_empty() {
                None
            } else {
                Some(DataType::Struct(fields))
            }
        })
}

/// Entry point for converting parquet group type.
///
/// This function takes care of logical type and repetition.
fn to_group_type(
    basic_info: &BasicTypeInfo,
    logical_type: &Option<LogicalType>,
    converted_type: &Option<GroupConvertedType>,
    fields: &[ParquetType],
    parent_name: &str,
) -> Result<Option<DataType>> {
    if basic_info.repetition() == &Repetition::Repeated {
        to_struct(fields).map(|opt| {
            opt.map(|dt| {
                DataType::List(Box::new(Field::new(
                    basic_info.name(),
                    dt,
                    is_nullable(basic_info),
                )))
            })
        })
    } else {
        to_group_type_inner(logical_type, converted_type, fields, parent_name)
    }
}

/// Checks whether this schema is nullable.
pub(super) fn is_nullable(basic_info: &BasicTypeInfo) -> bool {
    match basic_info.repetition() {
        Repetition::Optional => true,
        Repetition::Repeated => true,
        Repetition::Required => false,
    }
}

/// Converts parquet schema to arrow field.
fn to_field(type_: &ParquetType) -> Result<Option<Field>> {
    to_data_type(type_).map(|opt| {
        opt.map(|dt| {
            Field::new(
                type_.get_basic_info().name(),
                dt,
                is_nullable(type_.get_basic_info()),
            )
        })
    })
}

/// Converts a parquet list to arrow list.
///
/// To fully understand this algorithm, please refer to
/// [parquet doc](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md).
fn to_list(fields: &[ParquetType], parent_name: &str) -> Result<Option<DataType>> {
    let list_item = fields.first().unwrap();

    let item_type = match list_item {
        ParquetType::PrimitiveType {
            basic_info,
            physical_type,
            logical_type,
            converted_type,
            ..
        } => {
            if basic_info.repetition() == &Repetition::Repeated {
                to_primitive_type_inner(physical_type, logical_type, converted_type).map(Some)
            } else {
                Err(ArrowError::ExternalFormat(
                    "Primitive element type of list must be repeated.".to_string(),
                ))
            }
        }
        ParquetType::GroupType { fields, .. } => {
            if fields.len() == 1
                && list_item.name() != "array"
                && list_item.name() != format!("{}_tuple", parent_name)
            {
                let nested_item = fields.first().unwrap();
                to_data_type(nested_item)
            } else {
                to_struct(fields)
            }
        }
    };

    // Check that the name of the list child is "list", in which case we
    // get the child nullability and name (normally "element") from the nested
    // group type.
    // Without this step, the child incorrectly inherits the parent's optionality
    let (list_item_name, item_is_optional) = match list_item {
        ParquetType::GroupType {
            basic_info, fields, ..
        } if basic_info.name() == "list" && fields.len() == 1 => {
            let field = fields.first().unwrap();
            (
                field.name(),
                field.get_basic_info().repetition() != &Repetition::Required,
            )
        }
        _ => (
            list_item.name(),
            list_item.get_basic_info().repetition() != &Repetition::Required,
        ),
    };

    item_type.map(|opt| {
        opt.map(|dt| DataType::List(Box::new(Field::new(list_item_name, dt, item_is_optional))))
    })
}

/// Converts parquet schema to arrow data type.
///
/// This function discards schema name.
///
/// If this schema is a primitive type and not included in the leaves, the result is
/// Ok(None).
///
/// If this schema is a group type and none of its children is reserved in the
/// conversion, the result is Ok(None).
fn to_data_type(type_: &ParquetType) -> Result<Option<DataType>> {
    match type_ {
        ParquetType::PrimitiveType {
            basic_info,
            physical_type,
            logical_type,
            converted_type,
        } => to_primitive_type(basic_info, physical_type, logical_type, converted_type),
        ParquetType::GroupType {
            basic_info,
            logical_type,
            converted_type,
            fields,
        } => to_group_type(
            basic_info,
            logical_type,
            converted_type,
            fields,
            basic_info.name(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datatypes::{DataType, Field, TimeUnit};
    use crate::error::Result;

    #[test]
    fn test_flat_primitives() -> Result<()> {
        let message = "
        message test_schema {
            REQUIRED BOOLEAN boolean;
            REQUIRED INT32   int8  (INT_8);
            REQUIRED INT32   int16 (INT_16);
            REQUIRED INT32   uint8 (INTEGER(8,false));
            REQUIRED INT32   uint16 (INTEGER(16,false));
            REQUIRED INT32   int32;
            REQUIRED INT64   int64 ;
            OPTIONAL DOUBLE  double;
            OPTIONAL FLOAT   float;
            OPTIONAL BINARY  string (UTF8);
            OPTIONAL BINARY  string_2 (STRING);
        }
        ";
        let expected = &[
            Field::new("boolean", DataType::Boolean, false),
            Field::new("int8", DataType::Int8, false),
            Field::new("int16", DataType::Int16, false),
            Field::new("uint8", DataType::UInt8, false),
            Field::new("uint16", DataType::UInt16, false),
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("double", DataType::Float64, true),
            Field::new("float", DataType::Float32, true),
            Field::new("string", DataType::Utf8, true),
            Field::new("string_2", DataType::Utf8, true),
        ];

        let parquet_schema = SchemaDescriptor::try_from_message(message)?;
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, &None)?;

        assert_eq!(converted_arrow_schema.fields(), &expected);
        Ok(())
    }

    #[test]
    fn test_byte_array_fields() -> Result<()> {
        let message = "
        message test_schema {
            REQUIRED BYTE_ARRAY binary;
            REQUIRED FIXED_LEN_BYTE_ARRAY (20) fixed_binary;
        }
        ";
        let expected = vec![
            Field::new("binary", DataType::Binary, false),
            Field::new("fixed_binary", DataType::FixedSizeBinary(20), false),
        ];

        let parquet_schema = SchemaDescriptor::try_from_message(message)?;
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, &None)?;

        assert_eq!(converted_arrow_schema.fields(), &expected);
        Ok(())
    }

    #[test]
    fn test_duplicate_fields() -> Result<()> {
        let message = "
        message test_schema {
            REQUIRED BOOLEAN boolean;
            REQUIRED INT32 int8 (INT_8);
        }
        ";
        let expected = &[
            Field::new("boolean", DataType::Boolean, false),
            Field::new("int8", DataType::Int8, false),
        ];

        let parquet_schema = SchemaDescriptor::try_from_message(message)?;
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, &None)?;

        assert_eq!(converted_arrow_schema.fields(), &expected);
        Ok(())
    }

    #[test]
    fn test_parquet_lists() -> Result<()> {
        let mut arrow_fields = Vec::new();

        // LIST encoding example taken from parquet-format/LogicalTypes.md
        let message_type = "
        message test_schema {
          REQUIRED GROUP my_list (LIST) {
            REPEATED GROUP list {
              OPTIONAL BINARY element (UTF8);
            }
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP list {
              REQUIRED BINARY element (UTF8);
            }
          }
          OPTIONAL GROUP array_of_arrays (LIST) {
            REPEATED GROUP list {
              REQUIRED GROUP element (LIST) {
                REPEATED GROUP list {
                  REQUIRED INT32 element;
                }
              }
            }
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP element {
              REQUIRED BINARY str (UTF8);
            }
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED INT32 element;
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP element {
              REQUIRED BINARY str (UTF8);
              REQUIRED INT32 num;
            }
          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP array {
              REQUIRED BINARY str (UTF8);
            }

          }
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP my_list_tuple {
              REQUIRED BINARY str (UTF8);
            }
          }
          REPEATED INT32 name;
        }
        ";

        // // List<String> (list non-null, elements nullable)
        // required group my_list (LIST) {
        //   repeated group list {
        //     optional binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new(
                "my_list",
                DataType::List(Box::new(Field::new("element", DataType::Utf8, true))),
                false,
            ));
        }

        // // List<String> (list nullable, elements non-null)
        // optional group my_list (LIST) {
        //   repeated group list {
        //     required binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new(
                "my_list",
                DataType::List(Box::new(Field::new("element", DataType::Utf8, false))),
                true,
            ));
        }

        // Element types can be nested structures. For example, a list of lists:
        //
        // // List<List<Integer>>
        // optional group array_of_arrays (LIST) {
        //   repeated group list {
        //     required group element (LIST) {
        //       repeated group list {
        //         required int32 element;
        //       }
        //     }
        //   }
        // }
        {
            let arrow_inner_list =
                DataType::List(Box::new(Field::new("element", DataType::Int32, false)));
            arrow_fields.push(Field::new(
                "array_of_arrays",
                DataType::List(Box::new(Field::new("element", arrow_inner_list, false))),
                true,
            ));
        }

        // // List<String> (list nullable, elements non-null)
        // optional group my_list (LIST) {
        //   repeated group element {
        //     required binary str (UTF8);
        //   };
        // }
        {
            arrow_fields.push(Field::new(
                "my_list",
                DataType::List(Box::new(Field::new("element", DataType::Utf8, true))),
                true,
            ));
        }

        // // List<Integer> (nullable list, non-null elements)
        // optional group my_list (LIST) {
        //   repeated int32 element;
        // }
        {
            arrow_fields.push(Field::new(
                "my_list",
                DataType::List(Box::new(Field::new("element", DataType::Int32, true))),
                true,
            ));
        }

        // // List<Tuple<String, Integer>> (nullable list, non-null elements)
        // optional group my_list (LIST) {
        //   repeated group element {
        //     required binary str (UTF8);
        //     required int32 num;
        //   };
        // }
        {
            let arrow_struct = DataType::Struct(vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("num", DataType::Int32, false),
            ]);
            arrow_fields.push(Field::new(
                "my_list",
                DataType::List(Box::new(Field::new("element", arrow_struct, true))),
                true,
            ));
        }

        // // List<OneTuple<String>> (nullable list, non-null elements)
        // optional group my_list (LIST) {
        //   repeated group array {
        //     required binary str (UTF8);
        //   };
        // }
        // Special case: group is named array
        {
            let arrow_struct = DataType::Struct(vec![Field::new("str", DataType::Utf8, false)]);
            arrow_fields.push(Field::new(
                "my_list",
                DataType::List(Box::new(Field::new("array", arrow_struct, true))),
                true,
            ));
        }

        // // List<OneTuple<String>> (nullable list, non-null elements)
        // optional group my_list (LIST) {
        //   repeated group my_list_tuple {
        //     required binary str (UTF8);
        //   };
        // }
        // Special case: group named ends in _tuple
        {
            let arrow_struct = DataType::Struct(vec![Field::new("str", DataType::Utf8, false)]);
            arrow_fields.push(Field::new(
                "my_list",
                DataType::List(Box::new(Field::new("my_list_tuple", arrow_struct, true))),
                true,
            ));
        }

        // One-level encoding: Only allows required lists with required cells
        //   repeated value_type name
        {
            arrow_fields.push(Field::new(
                "name",
                DataType::List(Box::new(Field::new("name", DataType::Int32, true))),
                true,
            ));
        }

        let parquet_schema = SchemaDescriptor::try_from_message(message_type)?;
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, &None)?;

        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(arrow_fields[i], converted_fields[i]);
        }
        Ok(())
    }

    #[test]
    fn test_parquet_list_nullable() -> Result<()> {
        let mut arrow_fields = Vec::new();

        let message_type = "
        message test_schema {
          REQUIRED GROUP my_list1 (LIST) {
            REPEATED GROUP list {
              OPTIONAL BINARY element (UTF8);
            }
          }
          OPTIONAL GROUP my_list2 (LIST) {
            REPEATED GROUP list {
              REQUIRED BINARY element (UTF8);
            }
          }
          REQUIRED GROUP my_list3 (LIST) {
            REPEATED GROUP list {
              REQUIRED BINARY element (UTF8);
            }
          }
        }
        ";

        // // List<String> (list non-null, elements nullable)
        // required group my_list1 (LIST) {
        //   repeated group list {
        //     optional binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new(
                "my_list1",
                DataType::List(Box::new(Field::new("element", DataType::Utf8, true))),
                false,
            ));
        }

        // // List<String> (list nullable, elements non-null)
        // optional group my_list2 (LIST) {
        //   repeated group list {
        //     required binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new(
                "my_list2",
                DataType::List(Box::new(Field::new("element", DataType::Utf8, false))),
                true,
            ));
        }

        // // List<String> (list non-null, elements non-null)
        // repeated group my_list3 (LIST) {
        //   repeated group list {
        //     required binary element (UTF8);
        //   }
        // }
        {
            arrow_fields.push(Field::new(
                "my_list3",
                DataType::List(Box::new(Field::new("element", DataType::Utf8, false))),
                false,
            ));
        }

        let parquet_schema = SchemaDescriptor::try_from_message(message_type)?;
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, &None)?;

        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(arrow_fields[i], converted_fields[i]);
        }
        Ok(())
    }

    #[test]
    fn test_nested_schema() -> Result<()> {
        let mut arrow_fields = Vec::new();
        {
            let group1_fields = vec![
                Field::new("leaf1", DataType::Boolean, false),
                Field::new("leaf2", DataType::Int32, false),
            ];
            let group1_struct = Field::new("group1", DataType::Struct(group1_fields), false);
            arrow_fields.push(group1_struct);

            let leaf3_field = Field::new("leaf3", DataType::Int64, false);
            arrow_fields.push(leaf3_field);
        }

        let message_type = "
        message test_schema {
          REQUIRED GROUP group1 {
            REQUIRED BOOLEAN leaf1;
            REQUIRED INT32 leaf2;
          }
          REQUIRED INT64 leaf3;
        }
        ";

        let parquet_schema = SchemaDescriptor::try_from_message(message_type)?;
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, &None)?;

        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(arrow_fields[i], converted_fields[i]);
        }
        Ok(())
    }

    #[test]
    fn test_repeated_nested_schema() -> Result<()> {
        let mut arrow_fields = Vec::new();
        {
            arrow_fields.push(Field::new("leaf1", DataType::Int32, true));

            let inner_group_list = Field::new(
                "innerGroup",
                DataType::List(Box::new(Field::new(
                    "innerGroup",
                    DataType::Struct(vec![Field::new("leaf3", DataType::Int32, true)]),
                    true,
                ))),
                true,
            );

            let outer_group_list = Field::new(
                "outerGroup",
                DataType::List(Box::new(Field::new(
                    "outerGroup",
                    DataType::Struct(vec![
                        Field::new("leaf2", DataType::Int32, true),
                        inner_group_list,
                    ]),
                    true,
                ))),
                true,
            );
            arrow_fields.push(outer_group_list);
        }

        let message_type = "
        message test_schema {
          OPTIONAL INT32 leaf1;
          REPEATED GROUP outerGroup {
            OPTIONAL INT32 leaf2;
            REPEATED GROUP innerGroup {
              OPTIONAL INT32 leaf3;
            }
          }
        }
        ";

        let parquet_schema = SchemaDescriptor::try_from_message(message_type)?;
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, &None)?;

        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(arrow_fields[i], converted_fields[i]);
        }
        Ok(())
    }

    #[test]
    fn test_column_desc_to_field() -> Result<()> {
        let message_type = "
        message test_schema {
            REQUIRED BOOLEAN boolean;
            REQUIRED INT32   int8  (INT_8);
            REQUIRED INT32   uint8 (INTEGER(8,false));
            REQUIRED INT32   int16 (INT_16);
            REQUIRED INT32   uint16 (INTEGER(16,false));
            REQUIRED INT32   int32;
            REQUIRED INT64   int64;
            OPTIONAL DOUBLE  double;
            OPTIONAL FLOAT   float;
            OPTIONAL BINARY  string (UTF8);
            REPEATED BOOLEAN bools;
            OPTIONAL INT32   date       (DATE);
            OPTIONAL INT32   time_milli (TIME_MILLIS);
            OPTIONAL INT64   time_micro (TIME_MICROS);
            OPTIONAL INT64   time_nano (TIME(NANOS,false));
            OPTIONAL INT64   ts_milli (TIMESTAMP_MILLIS);
            REQUIRED INT64   ts_micro (TIMESTAMP_MICROS);
            REQUIRED INT64   ts_nano (TIMESTAMP(NANOS,true));
        }
        ";
        let arrow_fields = vec![
            Field::new("boolean", DataType::Boolean, false),
            Field::new("int8", DataType::Int8, false),
            Field::new("uint8", DataType::UInt8, false),
            Field::new("int16", DataType::Int16, false),
            Field::new("uint16", DataType::UInt16, false),
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("double", DataType::Float64, true),
            Field::new("float", DataType::Float32, true),
            Field::new("string", DataType::Utf8, true),
            Field::new(
                "bools",
                DataType::List(Box::new(Field::new("bools", DataType::Boolean, true))),
                true,
            ),
            Field::new("date", DataType::Date32, true),
            Field::new("time_milli", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new("time_micro", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("time_nano", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new(
                "ts_milli",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_micro",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "ts_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".to_string())),
                false,
            ),
        ];

        let parquet_schema = SchemaDescriptor::try_from_message(message_type)?;
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, &None)?;

        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(arrow_fields[i], converted_fields[i]);
        }
        Ok(())
    }

    #[test]
    fn test_field_to_column_desc() -> Result<()> {
        let message_type = "
        message arrow_schema {
            REQUIRED BOOLEAN boolean;
            REQUIRED INT32   int8  (INT_8);
            REQUIRED INT32   int16 (INTEGER(16,true));
            REQUIRED INT32   int32;
            REQUIRED INT64   int64;
            OPTIONAL DOUBLE  double;
            OPTIONAL FLOAT   float;
            OPTIONAL BINARY  string (STRING);
            OPTIONAL GROUP   bools (LIST) {
                REPEATED GROUP list {
                    OPTIONAL BOOLEAN element;
                }
            }
            REQUIRED GROUP   bools_non_null (LIST) {
                REPEATED GROUP list {
                    REQUIRED BOOLEAN element;
                }
            }
            OPTIONAL INT32   date       (DATE);
            OPTIONAL INT32   time_milli (TIME(MILLIS,false));
            OPTIONAL INT64   time_micro (TIME_MICROS);
            OPTIONAL INT64   ts_milli (TIMESTAMP_MILLIS);
            REQUIRED INT64   ts_micro (TIMESTAMP(MICROS,false));
            REQUIRED GROUP struct {
                REQUIRED BOOLEAN bools;
                REQUIRED INT32 uint32 (INTEGER(32,false));
                REQUIRED GROUP   int32 (LIST) {
                    REPEATED GROUP list {
                        OPTIONAL INT32 element;
                    }
                }
            }
            REQUIRED BINARY  dictionary_strings (STRING);
        }
        ";

        let arrow_fields = vec![
            Field::new("boolean", DataType::Boolean, false),
            Field::new("int8", DataType::Int8, false),
            Field::new("int16", DataType::Int16, false),
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("double", DataType::Float64, true),
            Field::new("float", DataType::Float32, true),
            Field::new("string", DataType::Utf8, true),
            Field::new(
                "bools",
                DataType::List(Box::new(Field::new("element", DataType::Boolean, true))),
                true,
            ),
            Field::new(
                "bools_non_null",
                DataType::List(Box::new(Field::new("element", DataType::Boolean, false))),
                false,
            ),
            Field::new("date", DataType::Date32, true),
            Field::new("time_milli", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new("time_micro", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new(
                "ts_milli",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_micro",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "struct",
                DataType::Struct(vec![
                    Field::new("bools", DataType::Boolean, false),
                    Field::new("uint32", DataType::UInt32, false),
                    Field::new(
                        "int32",
                        DataType::List(Box::new(Field::new("element", DataType::Int32, true))),
                        false,
                    ),
                ]),
                false,
            ),
            Field::new("dictionary_strings", DataType::Utf8, false),
        ];

        let parquet_schema = SchemaDescriptor::try_from_message(message_type)?;
        let converted_arrow_schema = parquet_to_arrow_schema(&parquet_schema, &None)?;

        let converted_fields = converted_arrow_schema.fields();

        assert_eq!(arrow_fields.len(), converted_fields.len());
        for i in 0..arrow_fields.len() {
            assert_eq!(arrow_fields[i], converted_fields[i]);
        }
        Ok(())
    }

    #[test]
    fn test_metadata() -> Result<()> {
        let message_type = "
        message test_schema {
            OPTIONAL BINARY  string (STRING);
        }
        ";
        let parquet_schema = SchemaDescriptor::try_from_message(message_type)?;

        let key_value_metadata = vec![
            KeyValue::new("foo".to_owned(), Some("bar".to_owned())),
            KeyValue::new("baz".to_owned(), None),
        ];

        let converted_arrow_schema =
            parquet_to_arrow_schema(&parquet_schema, &Some(key_value_metadata))?;

        let mut expected_metadata: HashMap<String, String> = HashMap::new();
        expected_metadata.insert("foo".to_owned(), "bar".to_owned());

        assert_eq!(converted_arrow_schema.metadata(), &expected_metadata);
        Ok(())
    }
}
