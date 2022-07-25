use parquet2::schema::types::PrimitiveType;

use crate::{
    array::{BinaryArray, MapArray, Utf8Array},
    datatypes::{DataType, Field},
    error::{Error, Result},
};

use super::nested_utils::{InitNested, NestedArrayIter};
use super::*;

/// Converts an iterator of arrays to a trait object returning trait objects
#[inline]
fn primitive<'a, A, I>(iter: I) -> NestedArrayIter<'a>
where
    A: Array,
    I: Iterator<Item = Result<(NestedState, A)>> + Send + Sync + 'a,
{
    Box::new(iter.map(|x| {
        x.map(|(mut nested, array)| {
            let _ = nested.nested.pop().unwrap(); // the primitive
            (nested, Box::new(array) as _)
        })
    }))
}

pub fn columns_to_iter_recursive<'a, I: 'a>(
    mut columns: Vec<I>,
    mut types: Vec<&PrimitiveType>,
    field: Field,
    mut init: Vec<InitNested>,
    num_rows: usize,
    chunk_size: Option<usize>,
) -> Result<NestedArrayIter<'a>>
where
    I: Pages,
{
    use crate::datatypes::PhysicalType::*;
    use crate::datatypes::PrimitiveType::*;

    Ok(match field.data_type().to_physical_type() {
        Boolean => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(boolean::NestedIter::new(
                columns.pop().unwrap(),
                init,
                num_rows,
                chunk_size,
            ))
        }
        Primitive(Int8) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(primitive::NestedIter::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x as i8,
            ))
        }
        Primitive(Int16) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(primitive::NestedIter::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x as i16,
            ))
        }
        Primitive(Int32) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(primitive::NestedIter::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x,
            ))
        }
        Primitive(Int64) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(primitive::NestedIter::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i64| x,
            ))
        }
        Primitive(UInt8) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(primitive::NestedIter::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x as u8,
            ))
        }
        Primitive(UInt16) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(primitive::NestedIter::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i32| x as u16,
            ))
        }
        Primitive(UInt32) => {
            init.push(InitNested::Primitive(field.is_nullable));
            let type_ = types.pop().unwrap();
            match type_.physical_type {
                PhysicalType::Int32 => primitive(primitive::NestedIter::new(
                    columns.pop().unwrap(),
                    init,
                    field.data_type().clone(),
                    num_rows,
                    chunk_size,
                    |x: i32| x as u32,
                )),
                // some implementations of parquet write arrow's u32 into i64.
                PhysicalType::Int64 => primitive(primitive::NestedIter::new(
                    columns.pop().unwrap(),
                    init,
                    field.data_type().clone(),
                    num_rows,
                    chunk_size,
                    |x: i64| x as u32,
                )),
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
            primitive(primitive::NestedIter::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: i64| x as u64,
            ))
        }
        Primitive(Float32) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(primitive::NestedIter::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: f32| x,
            ))
        }
        Primitive(Float64) => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(primitive::NestedIter::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
                |x: f64| x,
            ))
        }
        Utf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(binary::NestedIter::<i32, Utf8Array<i32>, _>::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
            ))
        }
        LargeUtf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(binary::NestedIter::<i64, Utf8Array<i64>, _>::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
            ))
        }
        Binary => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(binary::NestedIter::<i32, BinaryArray<i32>, _>::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
            ))
        }
        LargeBinary => {
            init.push(InitNested::Primitive(field.is_nullable));
            types.pop();
            primitive(binary::NestedIter::<i64, BinaryArray<i64>, _>::new(
                columns.pop().unwrap(),
                init,
                field.data_type().clone(),
                num_rows,
                chunk_size,
            ))
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

fn dict_read<'a, K: DictionaryKey, I: 'a + Pages>(
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
        UInt8 => primitive(primitive::NestedDictIter::<K, _, _, _, _>::new(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as u8,
        )),
        UInt16 => primitive(primitive::NestedDictIter::<K, _, _, _, _>::new(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as u16,
        )),
        UInt32 => primitive(primitive::NestedDictIter::<K, _, _, _, _>::new(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as u32,
        )),
        Int8 => primitive(primitive::NestedDictIter::<K, _, _, _, _>::new(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as i8,
        )),
        Int16 => primitive(primitive::NestedDictIter::<K, _, _, _, _>::new(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: i32| x as i16,
        )),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive(primitive::NestedDictIter::<K, _, _, _, _>::new(
                iter,
                init,
                data_type,
                num_rows,
                chunk_size,
                |x: i32| x,
            ))
        }
        Int64 | Date64 | Time64(_) | Duration(_) => {
            primitive(primitive::NestedDictIter::<K, _, _, _, _>::new(
                iter,
                init,
                data_type,
                num_rows,
                chunk_size,
                |x: i64| x as i32,
            ))
        }
        Float32 => primitive(primitive::NestedDictIter::<K, _, _, _, _>::new(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: f32| x,
        )),
        Float64 => primitive(primitive::NestedDictIter::<K, _, _, _, _>::new(
            iter,
            init,
            data_type,
            num_rows,
            chunk_size,
            |x: f64| x,
        )),
        Utf8 | Binary => primitive(binary::NestedDictIter::<K, i32, _>::new(
            iter, init, data_type, num_rows, chunk_size,
        )),
        LargeUtf8 | LargeBinary => primitive(binary::NestedDictIter::<K, i64, _>::new(
            iter, init, data_type, num_rows, chunk_size,
        )),
        FixedSizeBinary(_) => primitive(fixed_size_binary::NestedDictIter::<K, _>::new(
            iter, init, data_type, num_rows, chunk_size,
        )),
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
