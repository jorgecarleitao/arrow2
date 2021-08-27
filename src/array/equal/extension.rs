use crate::array::ExtensionArray;

use super::equal as main_equal;

pub(super) fn equal(lhs: &ExtensionArray, rhs: &ExtensionArray) -> bool {
    main_equal(lhs.inner(), rhs.inner())
}
