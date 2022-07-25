//! APIs to read from Parquet format.
mod binary;
mod boolean;
mod dictionary;
mod fixed_size_binary;
mod nested_utils;
mod null;
mod primitive;
mod simple;
mod struct_;
mod utils;

use parquet2::read::get_page_iterator as _get_page_iterator;
use parquet2::schema::types::PrimitiveType;

use crate::{
    array::{
        Array, BinaryArray, DictionaryKey, FixedSizeListArray, ListArray, MapArray, Utf8Array,
    },
    datatypes::{DataType, Field, IntervalUnit},
    error::{Error, Result},
};

use self::nested_utils::{InitNested, NestedArrayIter, NestedState};
use simple::page_iter_to_arrays;

use super::*;

/// Creates a new iterator of compressed pages.
pub fn get_page_iterator<R: Read + Seek>(
    column_metadata: &ColumnChunkMetaData,
    reader: R,
    pages_filter: Option<PageFilter>,
    buffer: Vec<u8>,
) -> Result<PageReader<R>> {
    Ok(_get_page_iterator(
        column_metadata,
        reader,
        pages_filter,
        buffer,
    )?)
}

fn create_list(
    data_type: DataType,
    nested: &mut NestedState,
    values: Box<dyn Array>,
) -> Box<dyn Array> {
    let (mut offsets, validity) = nested.nested.pop().unwrap().inner();
    match data_type.to_logical_type() {
        DataType::List(_) => {
            offsets.push(values.len() as i64);

            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();
            Box::new(ListArray::<i32>::new(
                data_type,
                offsets.into(),
                values,
                validity.and_then(|x| x.into()),
            ))
        }
        DataType::LargeList(_) => {
            offsets.push(values.len() as i64);

            Box::new(ListArray::<i64>::new(
                data_type,
                offsets.into(),
                values,
                validity.and_then(|x| x.into()),
            ))
        }
        DataType::FixedSizeList(_, _) => Box::new(FixedSizeListArray::new(
            data_type,
            values,
            validity.and_then(|x| x.into()),
        )),
        _ => unreachable!(),
    }
}

fn is_primitive(data_type: &DataType) -> bool {
    matches!(
        data_type.to_physical_type(),
        crate::datatypes::PhysicalType::Primitive(_)
            | crate::datatypes::PhysicalType::Null
            | crate::datatypes::PhysicalType::Boolean
            | crate::datatypes::PhysicalType::Utf8
            | crate::datatypes::PhysicalType::LargeUtf8
            | crate::datatypes::PhysicalType::Binary
            | crate::datatypes::PhysicalType::LargeBinary
            | crate::datatypes::PhysicalType::FixedSizeBinary
            | crate::datatypes::PhysicalType::Dictionary(_)
    )
}

fn columns_to_iter_recursive<'a, I: 'a>(
    mut columns: Vec<I>,
    mut types: Vec<&PrimitiveType>,
    field: Field,
    mut init: Vec<InitNested>,
    num_rows: usize,
    chunk_size: Option<usize>,
) -> Result<NestedArrayIter<'a>>
where
    I: DataPages,
{
    use crate::datatypes::PhysicalType::*;
    use crate::datatypes::PrimitiveType::*;

    if init.is_empty() && is_primitive(&field.data_type) {
        return Ok(Box::new(
            page_iter_to_arrays(
                columns.pop().unwrap(),
                types.pop().unwrap(),
                field.data_type,
                chunk_size,
                num_rows,
            )?
            .map(|x| Ok((NestedState::new(vec![]), x?))),
        ));
    }

    Ok(match field.data_type().to_physical_type() {
        Boolean => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            boolean::iter_to_arrays_nested(columns.pop().unwrap(), init, num_rows, chunk_size)
        }
        Primitive(Int8) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x as i8,
            )
        }
        Primitive(Int16) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x as i16,
            )
        }
        Primitive(Int32) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x,
            )
        }
        Primitive(Int64) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i64| x,
            )
        }
        Primitive(UInt8) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x as u8,
            )
        }
        Primitive(UInt16) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x as u16,
            )
        }
        Primitive(UInt32) => {
            init.push(InitNested::Primitive(field.is_nullable));
            let type_ = types.pop().unwrap();
            match type_.physical_type {
                PhysicalType::Int32 => primitive::iter_to_arrays_nested(
                    columns.pop().unwrap(),
                    init,
                    field.data_type().clone(),
                    num_rows,
                    chunk_size,
                    |x: i32| x as u32,
                ),
                // some implementations of parquet write arrow's u32 into i64.
                PhysicalType::Int64 => primitive::iter_to_arrays_nested(
                    columns.pop().unwrap(),
                    init,
                    field.data_type().clone(),
                    num_rows,
                    chunk_size,
                    |x: i64| x as u32,
                ),
                other => {
                    return Err(Error::nyi(format!(
                        "Deserializing UInt32 from {other:?}'s parquet"
                    )))
                }
            }
        }
        Primitive(UInt64) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i64| x as u64,
            )
        }
        Primitive(Float32) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: f32| x,
            )
        }
        Primitive(Float64) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: f64| x,
            )
        }
        Utf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            binary::iter_to_arrays_nested::<i32, Utf8Array<i32>, _>(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
            )
        }
        LargeUtf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            binary::iter_to_arrays_nested::<i64, Utf8Array<i64>, _>(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
            )
        }
        Binary => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            binary::iter_to_arrays_nested::<i32, BinaryArray<i32>, _>(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
            )
        }
        LargeBinary => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            binary::iter_to_arrays_nested::<i64, BinaryArray<i64>, _>(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
            )
        }
        _ => match field.data_type().to_logical_type() {
            DataType::Dictionary(key_type, _, _) => {
                init.push(InitNested::Primitive(field.is_nullable));
                let type_ = types.pop().unwrap();
                let iter = columns.pop().unwrap();
                let data_type = field.data_type().clone();
                match_integer_type!(key_type, |$K| {
                    dict_read::<$K, _>(iter, init, type_, data_type, num_rows, chunk_size)
                })?
            }
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _) => {
                init.push(InitNested::List(field.is_nullable));
                let iter = columns_to_iter_recursive(
                    columns,
                    types,
                    inner.as_ref().clone(),
                    init,
                    num_rows,
                    chunk_size,
                )?;
                let iter = iter.map(move |x| {
                    let (mut nested, array) = x?;
                    let array = create_list(field.data_type().clone(), &mut nested, array);
                    Ok((nested, array))
                });
                Box::new(iter) as _
            }
            DataType::Struct(fields) => {
                let columns = fields
                    .iter()
                    .rev()
                    .map(|f| {
                        let mut init = init.clone();
                        init.push(InitNested::Struct(field.is_nullable));
                        let n = n_columns(&f.data_type);
                        let columns = columns.drain(columns.len() - n..).collect();
                        let types = types.drain(types.len() - n..).collect();
                        columns_to_iter_recursive(
                            columns,
                            types,
                            f.clone(),
                            init,
                            num_rows,
                            chunk_size,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let columns = columns.into_iter().rev().collect();
                Box::new(struct_::StructIterator::new(columns, fields.clone()))
            }
            DataType::Map(inner, _) => {
                init.push(InitNested::List(field.is_nullable));
                let iter = columns_to_iter_recursive(
                    columns,
                    types,
                    inner.as_ref().clone(),
                    init,
                    num_rows,
                    chunk_size,
                )?;
                Box::new(iter.map(move |x| {
                    let (nested, inner) = x?;
                    let array = MapArray::new(
                        field.data_type().clone(),
                        vec![0, inner.len() as i32].into(),
                        inner,
                        None,
                    );
                    Ok((nested, array.boxed()))
                }))
            }
            other => {
                return Err(Error::nyi(format!(
                    "Deserializing type {other:?} from parquet"
                )))
            }
        },
    })
}

/// Returns the number of (parquet) columns that a [`DataType`] contains.
fn n_columns(data_type: &DataType) -> usize {
    use crate::datatypes::PhysicalType::*;
    match data_type.to_physical_type() {
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | Dictionary(_) | LargeUtf8 => 1,
        List | FixedSizeList | LargeList => {
            let a = data_type.to_logical_type();
            if let DataType::List(inner) = a {
                n_columns(&inner.data_type)
            } else if let DataType::LargeList(inner) = a {
                n_columns(&inner.data_type)
            } else if let DataType::FixedSizeList(inner, _) = a {
                n_columns(&inner.data_type)
            } else {
                unreachable!()
            }
        }
        Struct => {
            if let DataType::Struct(fields) = data_type.to_logical_type() {
                fields.iter().map(|inner| n_columns(&inner.data_type)).sum()
            } else {
                unreachable!()
            }
        }
        _ => todo!(),
    }
}

/// An iterator adapter that maps multiple iterators of [`DataPages`] into an iterator of [`Array`]s.
///
/// For a non-nested datatypes such as [`DataType::Int32`], this function requires a single element in `columns` and `types`.
/// For nested types, `columns` must be composed by all parquet columns with associated types `types`.
///
/// The arrays are guaranteed to be at most of size `chunk_size` and data type `field.data_type`.
pub fn column_iter_to_arrays<'a, I: 'a>(
    columns: Vec<I>,
    types: Vec<&PrimitiveType>,
    field: Field,
    chunk_size: Option<usize>,
    num_rows: usize,
) -> Result<ArrayIter<'a>>
where
    I: DataPages,
{
    Ok(Box::new(
        columns_to_iter_recursive(columns, types, field, vec![], num_rows, chunk_size)?
            .map(|x| x.map(|x| x.1)),
    ))
}

fn dict_read<'a, K: DictionaryKey, I: 'a + DataPages>(
    iter: I,
    init: Vec<InitNested>,
    _type_: &PrimitiveType,
    data_type: DataType,
    num_rows: usize,
    chunk_size: Option<usize>,
) -> Result<NestedArrayIter<'a>> {
    use DataType::*;
    let values_data_type = if let Dictionary(_, v, _) = &data_type {
        v.as_ref()
    } else {
        panic!()
    };

    Ok(match values_data_type.to_logical_type() {
        UInt8 => primitive::iter_to_dict_arrays_nested::<K, _, _, _, _>(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as u8,
        ),
        UInt16 => primitive::iter_to_dict_arrays_nested::<K, _, _, _, _>(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as u16,
        ),
        UInt32 => primitive::iter_to_dict_arrays_nested::<K, _, _, _, _>(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as u32,
        ),
        Int8 => primitive::iter_to_dict_arrays_nested::<K, _, _, _, _>(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as i8,
        ),
        Int16 => primitive::iter_to_dict_arrays_nested::<K, _, _, _, _>(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as i16,
        ),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive::iter_to_dict_arrays_nested::<K, _, _, _, _>(
                iter,
                init,
                data_type,
                num_rows,
                chunk_size,
                |x: i32| x,
            )
        }
        Int64 | Date64 | Time64(_) | Duration(_) => {
            primitive::iter_to_dict_arrays_nested::<K, _, _, _, _>(
                iter,
                init,
                data_type,
                num_rows,
                chunk_size,
                |x: i64| x as i32,
            )
        }
        Float32 => primitive::iter_to_dict_arrays_nested::<K, _, _, _, _>(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: f32| x,
        ),
        Float64 => primitive::iter_to_dict_arrays_nested::<K, _, _, _, _>(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: f64| x,
        ),
        Utf8 | Binary => binary::iter_to_dict_arrays_nested::<K, i32, _>(
            iter, init, data_type, num_rows, chunk_size,
        ),
        LargeUtf8 | LargeBinary => binary::iter_to_dict_arrays_nested::<K, i64, _>(
            iter, init, data_type, num_rows, chunk_size,
        ),
        FixedSizeBinary(_) => fixed_size_binary::iter_to_dict_arrays_nested::<K, _>(
            iter, init, data_type, num_rows, chunk_size,
        ),
        /*

        Timestamp(time_unit, _) => {
            let time_unit = *time_unit;
            return timestamp_dict::<K, _>(
                iter,
                physical_type,
                logical_type,
                data_type,
                chunk_size,
                time_unit,
            );
        }
         */
        other => {
            return Err(Error::nyi(format!(
                "Reading nested dictionaries of type {:?}",
                other
            )))
        }
    })
}
