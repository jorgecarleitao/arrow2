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

use crate::{
    array::{Array, BinaryArray, FixedSizeListArray, ListArray, MapArray, Utf8Array},
    datatypes::{DataType, Field},
    error::{Error, Result},
};

use self::nested_utils::{InitNested, NestedArrayIter, NestedState};
use parquet2::schema::types::PrimitiveType;
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
    values: Arc<dyn Array>,
) -> Arc<dyn Array> {
    let (mut offsets, validity) = nested.nested.pop().unwrap().inner();
    match data_type.to_logical_type() {
        DataType::List(_) => {
            offsets.push(values.len() as i64);

            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();
            Arc::new(ListArray::<i32>::new(
                data_type,
                offsets.into(),
                values,
                validity.and_then(|x| x.into()),
            ))
        }
        DataType::LargeList(_) => {
            offsets.push(values.len() as i64);

            Arc::new(ListArray::<i64>::new(
                data_type,
                offsets.into(),
                values,
                validity.and_then(|x| x.into()),
            ))
        }
        DataType::FixedSizeList(_, _) => Arc::new(FixedSizeListArray::new(
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
    chunk_size: usize,
) -> Result<NestedArrayIter<'a>>
where
    I: DataPages,
{
    use DataType::*;
    if init.is_empty() && is_primitive(&field.data_type) {
        return Ok(Box::new(
            page_iter_to_arrays(
                columns.pop().unwrap(),
                types.pop().unwrap(),
                field.data_type,
                chunk_size,
            )?
            .map(|x| Ok((NestedState::new(vec![]), x?))),
        ));
    }

    Ok(match field.data_type().to_logical_type() {
        Boolean => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            boolean::iter_to_arrays_nested(columns.pop().unwrap(), init, chunk_size)
        }
        Int8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                chunk_size,
                |x: i32| x as i8,
            )
        }
        Int16 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                chunk_size,
                |x: i32| x as i16,
            )
        }
        Int32 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                chunk_size,
                |x: i32| x,
            )
        }
        Int64 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                chunk_size,
                |x: i64| x,
            )
        }
        UInt8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                chunk_size,
                |x: i32| x as u8,
            )
        }
        UInt16 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                chunk_size,
                |x: i32| x as u16,
            )
        }
        UInt32 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                chunk_size,
                |x: i32| x as u32,
            )
        }
        UInt64 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                chunk_size,
                |x: i64| x as u64,
            )
        }
        Float32 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                chunk_size,
                |x: f32| x,
            )
        }
        Float64 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
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
                chunk_size,
            )
        }
        List(inner) | LargeList(inner) | FixedSizeList(inner, _) => {
            init.push(InitNested::List(field.is_nullable));
            let iter = columns_to_iter_recursive(
                columns,
                types,
                inner.as_ref().clone(),
                init,
                chunk_size,
            )?;
            let iter = iter.map(move |x| {
                let (mut nested, array) = x?;
                let array = create_list(field.data_type().clone(), &mut nested, array);
                Ok((nested, array))
            });
            Box::new(iter) as _
        }
        Struct(fields) => {
            let columns = fields
                .iter()
                .rev()
                .map(|f| {
                    let mut init = init.clone();
                    init.push(InitNested::Struct(field.is_nullable));
                    let n = n_columns(&f.data_type);
                    let columns = columns.drain(columns.len() - n..).collect();
                    let types = types.drain(types.len() - n..).collect();
                    columns_to_iter_recursive(columns, types, f.clone(), init, chunk_size)
                })
                .collect::<Result<Vec<_>>>()?;
            let columns = columns.into_iter().rev().collect();
            Box::new(struct_::StructIterator::new(columns, fields.clone()))
        }
        Map(inner, _) => {
            init.push(InitNested::List(field.is_nullable));
            let iter = columns_to_iter_recursive(
                columns,
                types,
                inner.as_ref().clone(),
                init,
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
                Ok((nested, array.arced()))
            }))
        }
        other => {
            return Err(Error::nyi(format!(
                "Deserializing type {other:?} from parquet"
            )))
        }
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
/// The arrays are guaranteed to be at most of size `chunk_size` and data type `field.data_type`.
pub fn column_iter_to_arrays<'a, I: 'a>(
    columns: Vec<I>,
    types: Vec<&PrimitiveType>,
    field: Field,
    chunk_size: usize,
) -> Result<ArrayIter<'a>>
where
    I: DataPages,
{
    Ok(Box::new(
        columns_to_iter_recursive(columns, types, field, vec![], chunk_size)?
            .map(|x| x.map(|x| x.1)),
    ))
}
