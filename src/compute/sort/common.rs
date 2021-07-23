/// # Safety
/// `indices[i] < values.len()` for all i
/// `limit < values.len()`
#[inline]
unsafe fn k_element_sort_inner<T, G, F>(
    indices: &mut [i32],
    get: G,
    descending: bool,
    limit: usize,
    mut cmp: F,
) where
    G: Fn(usize) -> T,
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    if descending {
        let compare = |lhs: &i32, rhs: &i32| {
            let lhs = get(*lhs as usize);
            let rhs = get(*rhs as usize);
            cmp(&lhs, &rhs).reverse()
        };
        let (before, _, _) = indices.select_nth_unstable_by(limit, compare);
        let compare = |lhs: &i32, rhs: &i32| {
            let lhs = get(*lhs as usize);
            let rhs = get(*rhs as usize);
            cmp(&lhs, &rhs).reverse()
        };
        before.sort_unstable_by(compare);
    } else {
        let compare = |lhs: &i32, rhs: &i32| {
            let lhs = get(*lhs as usize);
            let rhs = get(*rhs as usize);
            cmp(&lhs, &rhs)
        };
        let (before, _, _) = indices.select_nth_unstable_by(limit, compare);
        let compare = |lhs: &i32, rhs: &i32| {
            let lhs = get(*lhs as usize);
            let rhs = get(*rhs as usize);
            cmp(&lhs, &rhs)
        };
        before.sort_unstable_by(compare);
    }
}

/// # Safety
/// Safe iff
/// * `indices[i] < values.len()` for all i
/// * `limit < values.len()`
#[inline]
pub(super) unsafe fn sort_unstable_by<T, G, F>(
    indices: &mut [i32],
    get: G,
    mut cmp: F,
    descending: bool,
    limit: usize,
) where
    G: Fn(usize) -> T,
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    if limit != indices.len() {
        return k_element_sort_inner(indices, get, descending, limit, cmp);
    }

    if descending {
        indices.sort_unstable_by(|lhs, rhs| {
            let lhs = get(*lhs as usize);
            let rhs = get(*rhs as usize);
            cmp(&lhs, &rhs).reverse()
        })
    } else {
        indices.sort_unstable_by(|lhs, rhs| {
            let lhs = get(*lhs as usize);
            let rhs = get(*rhs as usize);
            cmp(&lhs, &rhs)
        })
    }
}
