use arrow2::array::*;

use super::{binary_cases, test_equal};

fn test_generic_string_equal<O: Offset>() {
    let cases = binary_cases();

    for (lhs, rhs, expected) in cases {
        let lhs = lhs.iter().map(|x| x.as_deref()).collect::<Vec<_>>();
        let rhs = rhs.iter().map(|x| x.as_deref()).collect::<Vec<_>>();
        let lhs = Utf8Array::<O>::from(&lhs);
        let rhs = Utf8Array::<O>::from(&rhs);
        test_equal(&lhs, &rhs, expected);
    }
}

#[test]
fn utf8_equal() {
    test_generic_string_equal::<i32>()
}

#[test]
fn large_utf8_equal() {
    test_generic_string_equal::<i64>()
}
