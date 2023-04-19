use crate::array::*;
use crate::error::Result;

pub(super) fn push(min: &mut dyn MutableArray, max: &mut dyn MutableArray) -> Result<()> {
    let min = min.as_mut_any().downcast_mut::<NullArray>().unwrap();
    let max = max.as_mut_any().downcast_mut::<NullArray>().unwrap();
    min.push_null();
    max.push_null();

    Ok(())
}
