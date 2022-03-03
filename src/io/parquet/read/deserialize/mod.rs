//! APIs to read from Parquet format.
mod binary;
mod boolean;
mod dictionary;
mod fixed_size_binary;
mod nested_utils;
mod null;
mod primitive;
mod simple;
mod utils;

use crate::{
    array::{Array, BinaryArray, ListArray, StructArray, Utf8Array},
    datatypes::{DataType, Field},
    error::{ArrowError, Result},
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
) -> Result<PageIterator<R>> {
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
) -> Result<Arc<dyn Array>> {
    Ok(match data_type {
        DataType::List(_) => {
            let (offsets, validity) = nested.nested.pop().unwrap().inner();

            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();
            Arc::new(ListArray::<i32>::from_data(
                data_type,
                offsets.into(),
                values,
                validity,
            ))
        }
        DataType::LargeList(_) => {
            let (offsets, validity) = nested.nested.pop().unwrap().inner();

            Arc::new(ListArray::<i64>::from_data(
                data_type, offsets, values, validity,
            ))
        }
        _ => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Read nested datatype {:?}",
                data_type
            )))
        }
    })
}

struct StructIterator<'a> {
    iters: Vec<NestedArrayIter<'a>>,
    fields: Vec<Field>,
}

impl<'a> StructIterator<'a> {
    pub fn new(iters: Vec<NestedArrayIter<'a>>, fields: Vec<Field>) -> Self {
        assert_eq!(iters.len(), fields.len());
        Self { iters, fields }
    }
}

impl<'a> Iterator for StructIterator<'a> {
    type Item = Result<(NestedState, Arc<dyn Array>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Vec<_>>();

        if values.iter().any(|x| x.is_none()) {
            return None;
        }
        let values = values
            .into_iter()
            .map(|x| x.unwrap().map(|x| x.1))
            .collect::<Result<Vec<_>>>();

        match values {
            Ok(values) => Some(Ok((
                NestedState::new(vec![]), // todo
                Arc::new(StructArray::from_data(
                    DataType::Struct(self.fields.clone()),
                    values,
                    None,
                )),
            ))),
            Err(e) => Some(Err(e)),
        }
    }
}

fn columns_to_iter_recursive<'a, I: 'a>(
    mut columns: Vec<I>,
    mut types: Vec<&ParquetType>,
    field: Field,
    mut init: Vec<InitNested>,
    chunk_size: usize,
) -> Result<NestedArrayIter<'a>>
where
    I: DataPages,
{
    use DataType::*;
    if init.len() == 1 && init[0].is_primitive() {
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
            types.pop();
            boolean::iter_to_arrays_nested(columns.pop().unwrap(), init.pop().unwrap(), chunk_size)
        }
        Int16 => {
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
                |x: i32| x as i16,
            )
        }
        Int64 => {
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
                |x: i64| x,
            )
        }
        Float32 => {
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
                |x: f32| x,
            )
        }
        Float64 => {
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
                |x: f64| x,
            )
        }
        Utf8 => {
            types.pop();
            binary::iter_to_arrays_nested::<i32, Utf8Array<i32>, _>(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
            )
        }
        LargeBinary => {
            types.pop();
            binary::iter_to_arrays_nested::<i64, BinaryArray<i64>, _>(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
            )
        }
        List(inner) => {
            let iter = columns_to_iter_recursive(
                vec![columns.pop().unwrap()],
                types,
                inner.as_ref().clone(),
                init,
                chunk_size,
            )?;
            let iter = iter.map(move |x| {
                let (mut nested, array) = x?;
                let array = create_list(field.data_type().clone(), &mut nested, array)?;
                Ok((nested, array))
            });
            Box::new(iter) as _
        }
        Struct(fields) => {
            let columns = fields
                .iter()
                .rev()
                .map(|f| {
                    columns_to_iter_recursive(
                        vec![columns.pop().unwrap()],
                        vec![types.pop().unwrap()],
                        f.clone(),
                        vec![init.pop().unwrap()],
                        chunk_size,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let columns = columns.into_iter().rev().collect();
            Box::new(StructIterator::new(columns, fields.clone()))
        }
        _ => todo!(),
    })
}

fn field_to_init(field: &Field) -> Vec<InitNested> {
    use crate::datatypes::PhysicalType::*;
    match field.data_type.to_physical_type() {
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | Dictionary(_) | LargeUtf8 => vec![InitNested::Primitive(field.is_nullable)],
        List | FixedSizeList | LargeList => {
            let a = field.data_type().to_logical_type();
            let inner = if let DataType::List(inner) = a {
                field_to_init(inner)
            } else if let DataType::LargeList(inner) = a {
                field_to_init(inner)
            } else if let DataType::FixedSizeList(inner, _) = a {
                field_to_init(inner)
            } else {
                unreachable!()
            };
            inner
                .into_iter()
                .map(|x| InitNested::List(Box::new(x), field.is_nullable))
                .collect()
        }
        Struct => {
            let inner = if let DataType::Struct(fields) = field.data_type.to_logical_type() {
                fields.iter().rev().map(field_to_init).collect::<Vec<_>>()
            } else {
                unreachable!()
            };
            inner
                .into_iter()
                .flatten()
                .map(|x| InitNested::Struct(Box::new(x), field.is_nullable))
                .collect()
        }
        _ => todo!(),
    }
}

/// An iterator adapter that maps multiple iterators of [`DataPages`] into an iterator of [`Array`]s.
///
/// The arrays are guaranteed to be at most of size `chunk_size` and data type `field.data_type`.
pub fn column_iter_to_arrays<'a, I: 'a>(
    columns: Vec<I>,
    types: Vec<&ParquetType>,
    field: Field,
    chunk_size: usize,
) -> Result<ArrayIter<'a>>
where
    I: DataPages,
{
    let init = field_to_init(&field);

    Ok(Box::new(
        columns_to_iter_recursive(columns, types, field, init, chunk_size)?.map(|x| x.map(|x| x.1)),
    ))
}
