//! Contains operators to filter arrays such as [`filter`].
use crate::array::growable::{make_growable, Growable};
use crate::bitmap::{utils::SlicesIterator, Bitmap, MutableBitmap};
use crate::columns::Columns;
use crate::datatypes::DataType;
use crate::error::Result;
use crate::{array::*, types::NativeType};

/// Function that can filter arbitrary arrays
pub type Filter<'a> = Box<dyn Fn(&dyn Array) -> Box<dyn Array> + 'a + Send + Sync>;

fn filter_nonnull_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    mask: &Bitmap,
) -> PrimitiveArray<T> {
    assert_eq!(array.len(), mask.len());
    let filter_count = mask.len() - mask.null_count();

    let mut buffer = Vec::<T>::with_capacity(filter_count);
    if let Some(validity) = array.validity() {
        let mut new_validity = MutableBitmap::with_capacity(filter_count);

        array
            .values()
            .iter()
            .zip(validity.iter())
            .zip(mask.iter())
            .filter(|x| x.1)
            .map(|x| x.0)
            .for_each(|(item, is_valid)| unsafe {
                buffer.push(*item);
                new_validity.push_unchecked(is_valid);
            });

        PrimitiveArray::<T>::from_data(
            array.data_type().clone(),
            buffer.into(),
            new_validity.into(),
        )
    } else {
        array
            .values()
            .iter()
            .zip(mask.iter())
            .filter(|x| x.1)
            .map(|x| x.0)
            .for_each(|item| buffer.push(*item));

        PrimitiveArray::<T>::from_data(array.data_type().clone(), buffer.into(), None)
    }
}

fn filter_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    mask: &BooleanArray,
) -> PrimitiveArray<T> {
    // todo: branch on mask.validity()
    filter_nonnull_primitive(array, mask.values())
}

fn filter_growable<'a>(growable: &mut impl Growable<'a>, chunks: &[(usize, usize)]) {
    chunks
        .iter()
        .for_each(|(start, len)| growable.extend(0, *start, *len));
}

/// Returns a prepared function optimized to filter multiple arrays.
/// Creating this function requires time, but using it is faster than [filter] when the
/// same filter needs to be applied to multiple arrays (e.g. a multiple columns).
pub fn build_filter(filter: &BooleanArray) -> Result<Filter> {
    let iter = SlicesIterator::new(filter.values());
    let filter_count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    use crate::datatypes::PhysicalType::*;
    Ok(Box::new(move |array: &dyn Array| {
        match array.data_type().to_physical_type() {
            Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
                let array = array.as_any().downcast_ref().unwrap();
                let mut growable =
                    growable::GrowablePrimitive::<$T>::new(vec![array], false, filter_count);
                filter_growable(&mut growable, &chunks);
                let array: PrimitiveArray<$T> = growable.into();
                Box::new(array)
            }),
            Utf8 => {
                let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                let mut growable = growable::GrowableUtf8::new(vec![array], false, filter_count);
                filter_growable(&mut growable, &chunks);
                let array: Utf8Array<i32> = growable.into();
                Box::new(array)
            }
            LargeUtf8 => {
                let array = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
                let mut growable = growable::GrowableUtf8::new(vec![array], false, filter_count);
                filter_growable(&mut growable, &chunks);
                let array: Utf8Array<i64> = growable.into();
                Box::new(array)
            }
            _ => {
                let mut mutable = make_growable(&[array], false, filter_count);
                chunks
                    .iter()
                    .for_each(|(start, len)| mutable.extend(0, *start, *len));
                mutable.as_box()
            }
        }
    }))
}

/// Filters an [Array], returning elements matching the filter (i.e. where the values are true).
///
/// Note that the nulls of `filter` are interpreted as `false` will lead to these elements being
/// masked out.
///
/// # Example
/// ```rust
/// # use arrow2::array::{Int32Array, PrimitiveArray, BooleanArray};
/// # use arrow2::error::Result;
/// # use arrow2::compute::filter::filter;
/// # fn main() -> Result<()> {
/// let array = PrimitiveArray::from_slice([5, 6, 7, 8, 9]);
/// let filter_array = BooleanArray::from_slice(&vec![true, false, false, true, false]);
/// let c = filter(&array, &filter_array)?;
/// let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(c, &PrimitiveArray::from_slice(vec![5, 8]));
/// # Ok(())
/// # }
/// ```
pub fn filter(array: &dyn Array, filter: &BooleanArray) -> Result<Box<dyn Array>> {
    // The validities may be masking out `true` bits, making the filter operation
    // based on the values incorrect
    if let Some(validities) = filter.validity() {
        let values = filter.values();
        let new_values = values & validities;
        let filter = BooleanArray::from_data(DataType::Boolean, new_values, None);
        return crate::compute::filter::filter(array, &filter);
    }

    use crate::datatypes::PhysicalType::*;
    match array.data_type().to_physical_type() {
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<$T>(array, filter)))
        }),
        _ => {
            let iter = SlicesIterator::new(filter.values());
            let mut mutable = make_growable(&[array], false, iter.slots());
            iter.for_each(|(start, len)| mutable.extend(0, start, len));
            Ok(mutable.as_box())
        }
    }
}

/// Returns a new [Columns] with arrays containing only values matching the filter.
/// This is a convenience function: filter multiple columns is embarassingly parallel.
pub fn filter_columns<A: AsRef<dyn Array>>(
    columns: &Columns<A>,
    filter_values: &BooleanArray,
) -> Result<Columns<Box<dyn Array>>> {
    let arrays = columns.arrays();

    let num_colums = arrays.len();

    let filtered_arrays = match num_colums {
        1 => {
            vec![filter(columns.arrays()[0].as_ref(), filter_values)?]
        }
        _ => {
            let filter = build_filter(filter_values)?;
            arrays.iter().map(|a| filter(a.as_ref())).collect()
        }
    };
    Columns::try_new(filtered_arrays)
}
