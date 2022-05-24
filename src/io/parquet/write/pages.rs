use parquet2::schema::types::ParquetType;
use parquet2::{page::EncodedPage, write::DynIter};

use crate::array::{ListArray, StructArray};
use crate::bitmap::Bitmap;
use crate::datatypes::PhysicalType;
use crate::io::parquet::read::schema::is_nullable;
use crate::{
    array::Array,
    error::{ArrowError, Result},
};

use super::levels::NestedInfo;
use super::{array_to_pages, Encoding, WriteOptions};

#[derive(Debug, Clone, PartialEq)]
pub enum Nested<'a> {
    Primitive(Option<&'a Bitmap>, bool),
    List(NestedInfo<'a, i32>),
    LargeList(NestedInfo<'a, i64>),
    Struct(Option<&'a Bitmap>, bool),
}

/// Constructs the necessary `Vec<Vec<Nested>>` to write the rep and def levels of `array` to parquet
pub fn to_nested(array: &dyn Array, type_: ParquetType) -> Result<Vec<Vec<Nested>>> {
    let mut nested = vec![];

    to_nested_recursive(array, type_, &mut nested, vec![])?;
    Ok(nested)
}

fn to_nested_recursive<'a>(
    array: &'a dyn Array,
    type_: ParquetType,
    nested: &mut Vec<Vec<Nested<'a>>>,
    mut parents: Vec<Nested<'a>>,
) -> Result<()> {
    let is_optional = is_nullable(type_.get_field_info());

    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Struct => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let fields = if let ParquetType::GroupType { fields, .. } = type_ {
                fields
            } else {
                return Err(ArrowError::InvalidArgumentError(
                    "Parquet type must be a group for a struct array".to_string(),
                ));
            };

            parents.push(Nested::Struct(array.validity(), is_optional));

            for (type_, array) in fields.into_iter().zip(array.values()) {
                to_nested_recursive(array.as_ref(), type_, nested, parents.clone())?;
            }
        }
        List => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            let type_ = if let ParquetType::GroupType { mut fields, .. } = type_ {
                if let ParquetType::GroupType { mut fields, .. } = fields.pop().unwrap() {
                    fields.pop().unwrap()
                } else {
                    return Err(ArrowError::InvalidArgumentError(
                        "Parquet type must be a group for a list array".to_string(),
                    ));
                }
            } else {
                return Err(ArrowError::InvalidArgumentError(
                    "Parquet type must be a group for a list array".to_string(),
                ));
            };

            parents.push(Nested::List(NestedInfo::new(
                array.offsets(),
                array.validity(),
                is_optional,
            )));
            to_nested_recursive(array.values().as_ref(), type_, nested, parents)?;
        }
        LargeList => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            let type_ = if let ParquetType::GroupType { mut fields, .. } = type_ {
                if let ParquetType::GroupType { mut fields, .. } = fields.pop().unwrap() {
                    fields.pop().unwrap()
                } else {
                    return Err(ArrowError::InvalidArgumentError(
                        "Parquet type must be a group for a list array".to_string(),
                    ));
                }
            } else {
                return Err(ArrowError::InvalidArgumentError(
                    "Parquet type must be a group for a list array".to_string(),
                ));
            };

            parents.push(Nested::LargeList(NestedInfo::new(
                array.offsets(),
                array.validity(),
                is_optional,
            )));
            to_nested_recursive(array.values().as_ref(), type_, nested, parents)?;
        }
        _ => {
            parents.push(Nested::Primitive(array.validity(), is_optional));
            nested.push(parents)
        }
    }
    Ok(())
}

fn array_to_columns(
    array: &dyn Array,
    type_: ParquetType,
    options: WriteOptions,
    mut encoding: Vec<Encoding>,
    columns: &mut Vec<DynIter<'static, Result<EncodedPage>>>,
) -> Result<()> {
    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | LargeUtf8 | Dictionary(_) => columns.push(array_to_pages(
            array,
            type_,
            options,
            encoding.pop().unwrap(),
        )?),
        _ => {

            // it is nested
            // we need to prepare the nested info to be used on individual pages
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use parquet2::schema::Repetition;

    use super::*;

    use crate::array::*;
    use crate::bitmap::Bitmap;
    use crate::datatypes::*;

    use super::super::{FieldInfo, ParquetPhysicalType, ParquetPrimitiveType};

    #[test]
    fn test_struct() {
        use std::sync::Arc;
        let boolean =
            Arc::new(BooleanArray::from_slice(&[false, false, true, true])) as Arc<dyn Array>;
        let int = Arc::new(Int32Array::from_slice(&[42, 28, 19, 31])) as Arc<dyn Array>;

        let fields = vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ];

        let array = StructArray::from_data(
            DataType::Struct(fields),
            vec![boolean.clone(), int.clone()],
            Some(Bitmap::from([true, true, false, true])),
        );

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "b".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Boolean,
                }),
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "c".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Int32,
                }),
            ],
        };
        let a = to_nested(&array, type_).unwrap();

        assert_eq!(
            a,
            vec![
                vec![
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true),
                    Nested::Primitive(None, false),
                ],
                vec![
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true),
                    Nested::Primitive(None, false),
                ],
            ]
        );
    }

    #[test]
    fn test_struct_struct() {
        use std::sync::Arc;
        let boolean =
            Arc::new(BooleanArray::from_slice(&[false, false, true, true])) as Arc<dyn Array>;
        let int = Arc::new(Int32Array::from_slice(&[42, 28, 19, 31])) as Arc<dyn Array>;

        let fields = vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ];

        let array = StructArray::from_data(
            DataType::Struct(fields),
            vec![boolean.clone(), int.clone()],
            Some(Bitmap::from([true, true, false, true])),
        );

        let fields = vec![
            Field::new("b", array.data_type().clone(), true),
            Field::new("c", array.data_type().clone(), true),
        ];

        let array = StructArray::from_data(
            DataType::Struct(fields),
            vec![Arc::new(array.clone()), Arc::new(array)],
            None,
        );

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "b".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Boolean,
                }),
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "c".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Int32,
                }),
            ],
        };

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![type_.clone(), type_],
        };

        let a = to_nested(&array, type_).unwrap();

        assert_eq!(
            a,
            vec![
                // a.b.b
                vec![
                    Nested::Struct(None, false),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true),
                    Nested::Primitive(None, false),
                ],
                // a.b.c
                vec![
                    Nested::Struct(None, false),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true),
                    Nested::Primitive(None, false),
                ],
                // a.c.b
                vec![
                    Nested::Struct(None, false),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true),
                    Nested::Primitive(None, false),
                ],
                // a.c.c
                vec![
                    Nested::Struct(None, false),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true),
                    Nested::Primitive(None, false),
                ],
            ]
        );
    }

    #[test]
    fn test_list_struct() {
        use std::sync::Arc;
        let boolean =
            Arc::new(BooleanArray::from_slice(&[false, false, true, true])) as Arc<dyn Array>;
        let int = Arc::new(Int32Array::from_slice(&[42, 28, 19, 31])) as Arc<dyn Array>;

        let fields = vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ];

        let array = StructArray::from_data(
            DataType::Struct(fields),
            vec![boolean.clone(), int.clone()],
            Some(Bitmap::from([true, true, false, true])),
        );

        let array = ListArray::new(
            DataType::List(Box::new(Field::new("l", array.data_type().clone(), true))),
            vec![0i32, 2, 4].into(),
            Arc::new(array),
            None,
        );

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "b".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Boolean,
                }),
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "c".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Int32,
                }),
            ],
        };

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "l".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![ParquetType::GroupType {
                field_info: FieldInfo {
                    name: "list".to_string(),
                    repetition: Repetition::Repeated,
                    id: None,
                },
                logical_type: None,
                converted_type: None,
                fields: vec![type_],
            }],
        };

        let a = to_nested(&array, type_).unwrap();

        assert_eq!(
            a,
            vec![
                vec![
                    Nested::List(NestedInfo::<i32> {
                        is_optional: false,
                        offsets: &[0, 2, 4],
                        validity: None,
                    }),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true),
                    Nested::Primitive(None, false),
                ],
                vec![
                    Nested::List(NestedInfo::<i32> {
                        is_optional: false,
                        offsets: &[0, 2, 4],
                        validity: None,
                    }),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true),
                    Nested::Primitive(None, false),
                ],
            ]
        );
    }
}
