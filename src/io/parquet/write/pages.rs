use parquet2::schema::types::{ParquetType, PrimitiveType as ParquetPrimitiveType};
use parquet2::{page::Page, write::DynIter};
use std::fmt::Debug;

use crate::array::{FixedSizeListArray, ListArray, PrimitiveArray, StructArray};
use crate::bitmap::Bitmap;
use crate::datatypes::{DataType, PhysicalType};
use crate::io::parquet::read::schema::is_nullable;
use crate::offset::Offset;
use crate::{
    array::Array,
    error::{Error, Result},
};

use super::{array_to_pages, Encoding, WriteOptions};

#[derive(Debug, Clone, PartialEq)]
pub struct ListNested<'a, O: Offset> {
    pub is_optional: bool,
    pub offsets: &'a [O],
    pub validity: Option<&'a Bitmap>,
}

impl<'a, O: Offset> ListNested<'a, O> {
    pub fn new(offsets: &'a [O], validity: Option<&'a Bitmap>, is_optional: bool) -> Self {
        Self {
            is_optional,
            offsets,
            validity,
        }
    }
}

/// Descriptor of nested information of a field
#[derive(Debug, Clone, PartialEq)]
pub enum Nested<'a> {
    /// a primitive (leaf or parquet column)
    /// bitmap, _, length
    Primitive(Option<&'a Bitmap>, bool, usize),
    /// a list
    List(ListNested<'a, i32>),
    /// a list
    LargeList(ListNested<'a, i64>),
    /// a struct
    Struct(Option<&'a Bitmap>, bool, usize),
}

impl Nested<'_> {
    /// Returns the length (number of rows) of the element
    pub fn len(&self) -> usize {
        match self {
            Nested::Primitive(_, _, length) => *length,
            Nested::List(nested) => nested.offsets.len() - 1,
            Nested::LargeList(nested) => nested.offsets.len() - 1,
            Nested::Struct(_, _, len) => *len,
        }
    }
}

/// Constructs the necessary `Vec<Vec<Nested>>` to write the rep and def levels of `array` to parquet
pub fn to_nested<'a>(array: &'a dyn Array, type_: &ParquetType) -> Result<Vec<Vec<Nested<'a>>>> {
    let mut nested = vec![];

    to_nested_recursive(array, type_, &mut nested, vec![])?;
    Ok(nested)
}

fn to_nested_recursive<'a>(
    array: &'a dyn Array,
    type_: &ParquetType,
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
                return Err(Error::InvalidArgumentError(
                    "Parquet type must be a group for a struct array".to_string(),
                ));
            };

            parents.push(Nested::Struct(array.validity(), is_optional, array.len()));

            for (type_, array) in fields.iter().zip(array.values()) {
                to_nested_recursive(array.as_ref(), type_, nested, parents.clone())?;
            }
        }
        List => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            let type_ = if let ParquetType::GroupType { fields, .. } = type_ {
                if let ParquetType::GroupType { fields, .. } = &fields[0] {
                    &fields[0]
                } else {
                    return Err(Error::InvalidArgumentError(
                        "Parquet type must be a group for a list array".to_string(),
                    ));
                }
            } else {
                return Err(Error::InvalidArgumentError(
                    "Parquet type must be a group for a list array".to_string(),
                ));
            };

            parents.push(Nested::List(ListNested::new(
                array.offsets().buffer(),
                array.validity(),
                is_optional,
            )));
            to_nested_recursive(array.values().as_ref(), type_, nested, parents)?;
        }
        LargeList => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            let type_ = if let ParquetType::GroupType { fields, .. } = type_ {
                if let ParquetType::GroupType { fields, .. } = &fields[0] {
                    &fields[0]
                } else {
                    return Err(Error::InvalidArgumentError(
                        "Parquet type must be a group for a list array".to_string(),
                    ));
                }
            } else {
                return Err(Error::InvalidArgumentError(
                    "Parquet type must be a group for a list array".to_string(),
                ));
            };

            parents.push(Nested::LargeList(ListNested::new(
                array.offsets().buffer(),
                array.validity(),
                is_optional,
            )));
            to_nested_recursive(array.values().as_ref(), type_, nested, parents)?;
        }
        _ => {
            parents.push(Nested::Primitive(
                array.validity(),
                is_optional,
                array.len(),
            ));
            nested.push(parents)
        }
    }
    Ok(())
}

/// Convert [`Array`] to `Vec<&dyn Array>` leaves in DFS order.
pub fn to_leaves(array: &dyn Array) -> Vec<&dyn Array> {
    let mut leaves = vec![];
    to_leaves_recursive(array, &mut leaves);
    leaves
}

fn to_leaves_recursive<'a>(array: &'a dyn Array, leaves: &mut Vec<&'a dyn Array>) {
    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Struct => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array
                .values()
                .iter()
                .for_each(|a| to_leaves_recursive(a.as_ref(), leaves));
        }
        List => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            to_leaves_recursive(array.values().as_ref(), leaves);
        }
        LargeList => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            to_leaves_recursive(array.values().as_ref(), leaves);
        }
        FixedSizeList => {
            let indices: Option<Vec<u32>> = array.validity().map(|validity| {
                validity
                    .into_iter()
                    .enumerate()
                    .map(|(idx, val)| if val { Some(idx as u32) } else { None })
                    .flatten()
                    .collect()
            });

            if let Some(indices) = indices {
                let new_array = crate::compute::take::take(
                    array,
                    &PrimitiveArray::new(DataType::UInt32, indices.into(), None),
                )
                .unwrap();
                let new_array = new_array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap();
                to_leaves_recursive(new_array.values().as_ref(), leaves);
            } else {
                let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                to_leaves_recursive(array.values().as_ref(), leaves);
            }
        }
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | LargeUtf8 | Dictionary(_) => leaves.push(array),
        other => todo!("Writing {:?} to parquet not yet implemented", other),
    }
}

/// Convert `ParquetType` to `Vec<ParquetPrimitiveType>` leaves in DFS order.
pub fn to_parquet_leaves(type_: ParquetType) -> Vec<ParquetPrimitiveType> {
    let mut leaves = vec![];
    to_parquet_leaves_recursive(type_, &mut leaves);
    leaves
}

fn to_parquet_leaves_recursive(type_: ParquetType, leaves: &mut Vec<ParquetPrimitiveType>) {
    match type_ {
        ParquetType::PrimitiveType(primitive) => leaves.push(primitive),
        ParquetType::GroupType { fields, .. } => {
            fields
                .into_iter()
                .for_each(|type_| to_parquet_leaves_recursive(type_, leaves));
        }
    }
}

/// Returns a vector of iterators of [`Page`], one per leaf column in the array
pub fn array_to_columns<A: AsRef<dyn Array> + Send + Sync>(
    array: A,
    type_: ParquetType,
    options: WriteOptions,
    encoding: &[Encoding],
) -> Result<Vec<DynIter<'static, Result<Page>>>> {
    let array = array.as_ref();
    let nested = to_nested(array, &type_)?;

    let types = to_parquet_leaves(type_);

    let values = to_leaves(array);

    assert_eq!(encoding.len(), types.len());

    values
        .iter()
        .zip(nested.into_iter())
        .zip(types.into_iter())
        .zip(encoding.iter())
        .map(|(((values, nested), type_), encoding)| {
            array_to_pages(*values, type_, &nested, options, *encoding)
        })
        .collect()
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
        let boolean = BooleanArray::from_slice([false, false, true, true]).boxed();
        let int = Int32Array::from_slice([42, 28, 19, 31]).boxed();

        let fields = vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ];

        let array = StructArray::new(
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
        let a = to_nested(&array, &type_).unwrap();

        assert_eq!(
            a,
            vec![
                vec![
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                vec![
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
            ]
        );
    }

    #[test]
    fn test_struct_struct() {
        let boolean = BooleanArray::from_slice([false, false, true, true]).boxed();
        let int = Int32Array::from_slice([42, 28, 19, 31]).boxed();

        let fields = vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ];

        let array = StructArray::new(
            DataType::Struct(fields),
            vec![boolean.clone(), int.clone()],
            Some(Bitmap::from([true, true, false, true])),
        );

        let fields = vec![
            Field::new("b", array.data_type().clone(), true),
            Field::new("c", array.data_type().clone(), true),
        ];

        let array = StructArray::new(
            DataType::Struct(fields),
            vec![Box::new(array.clone()), Box::new(array)],
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

        let a = to_nested(&array, &type_).unwrap();

        assert_eq!(
            a,
            vec![
                // a.b.b
                vec![
                    Nested::Struct(None, false, 4),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                // a.b.c
                vec![
                    Nested::Struct(None, false, 4),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                // a.c.b
                vec![
                    Nested::Struct(None, false, 4),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                // a.c.c
                vec![
                    Nested::Struct(None, false, 4),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
            ]
        );
    }

    #[test]
    fn test_list_struct() {
        let boolean = BooleanArray::from_slice([false, false, true, true]).boxed();
        let int = Int32Array::from_slice([42, 28, 19, 31]).boxed();

        let fields = vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ];

        let array = StructArray::new(
            DataType::Struct(fields),
            vec![boolean.clone(), int.clone()],
            Some(Bitmap::from([true, true, false, true])),
        );

        let array = ListArray::new(
            DataType::List(Box::new(Field::new("l", array.data_type().clone(), true))),
            vec![0i32, 2, 4].try_into().unwrap(),
            Box::new(array),
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

        let a = to_nested(&array, &type_).unwrap();

        assert_eq!(
            a,
            vec![
                vec![
                    Nested::List(ListNested::<i32> {
                        is_optional: false,
                        offsets: &[0, 2, 4],
                        validity: None,
                    }),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                vec![
                    Nested::List(ListNested::<i32> {
                        is_optional: false,
                        offsets: &[0, 2, 4],
                        validity: None,
                    }),
                    Nested::Struct(Some(&Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
            ]
        );
    }
}
