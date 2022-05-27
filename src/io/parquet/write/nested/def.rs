use crate::{array::Offset, bitmap::Bitmap};

use super::super::pages::{ListNested, Nested};
use super::rep::num_values;
use super::to_length;

trait DebugIter: Iterator<Item = (u32, usize)> + std::fmt::Debug {}

impl<A: Iterator<Item = (u32, usize)> + std::fmt::Debug> DebugIter for A {}

fn single_iter<'a>(
    validity: Option<&'a Bitmap>,
    is_optional: bool,
    length: usize,
) -> Box<dyn DebugIter + 'a> {
    match (is_optional, validity) {
        (false, _) => {
            Box::new(std::iter::repeat((0u32, 1usize)).take(length)) as Box<dyn DebugIter>
        }
        (true, None) => {
            Box::new(std::iter::repeat((1u32, 1usize)).take(length)) as Box<dyn DebugIter>
        }
        (true, Some(validity)) => {
            Box::new(validity.iter().map(|v| (v as u32, 1usize)).take(length)) as Box<dyn DebugIter>
        }
    }
}

fn single_list_iter<'a, O: Offset>(nested: &ListNested<'a, O>) -> Box<dyn DebugIter + 'a> {
    match (nested.is_optional, nested.validity) {
        (false, _) => {
            Box::new(std::iter::repeat(1u32).zip(to_length(nested.offsets))) as Box<dyn DebugIter>
        }
        (true, None) => {
            Box::new(std::iter::repeat(2u32).zip(to_length(nested.offsets))) as Box<dyn DebugIter>
        }
        (true, Some(validity)) => Box::new(
            validity
                .iter()
                // lists have 2 groups, so
                // True => 2
                // False => 1
                .map(|x| (x as u32) + 1)
                .zip(to_length(nested.offsets)),
        ) as Box<dyn DebugIter>,
    }
}

fn iter<'a>(nested: &'a [Nested]) -> Vec<Box<dyn DebugIter + 'a>> {
    nested
        .iter()
        .map(|nested| match nested {
            Nested::Primitive(validity, is_optional, length) => {
                single_iter(*validity, *is_optional, *length)
            }
            Nested::List(nested) => single_list_iter(nested),
            Nested::LargeList(nested) => single_list_iter(nested),
            Nested::Struct(validity, is_optional, length) => {
                single_iter(*validity, *is_optional, *length)
            }
        })
        .collect()
}

/// Iterator adapter of parquet / dremel definition levels
#[derive(Debug)]
pub struct DefLevelsIter<'a> {
    // iterators of validities and lengths. E.g. [[[None,b,c], None], None] -> [[(true, 2), (false, 0)], [(true, 3), (false, 0)], [(false, 1), (true, 1), (true, 1)]]
    iter: Vec<Box<dyn DebugIter + 'a>>,
    primitive_validity: Box<dyn DebugIter + 'a>,
    // vector containing the remaining number of values of each iterator.
    // e.g. the iters [[2, 2], [3, 4, 1, 2]] after the first iteration will return [2, 3],
    // and remaining will be [2, 3].
    // on the second iteration, it will be `[2, 2]` (since iterations consume the last items)
    remaining: Vec<usize>, /* < remaining.len() == iter.len() */
    validity: Vec<u32>,
    // cache of the first `remaining` that is non-zero. Examples:
    // * `remaining = [2, 2] => current_level = 2`
    // * `remaining = [2, 0] => current_level = 1`
    // * `remaining = [0, 0] => current_level = 0`
    current_level: usize, /* < iter.len() */
    // the total definition level at any given point during the iteration
    total: u32, /* < iter.len() */
    // the total number of items that this iterator will return
    remaining_values: usize,
}

impl<'a> DefLevelsIter<'a> {
    pub fn new(nested: &'a [Nested]) -> Self {
        let remaining_values = num_values(nested);

        let primitive_validity = iter(&nested[nested.len() - 1..]).pop().unwrap();

        let iter = iter(&nested[..nested.len() - 1]);
        let remaining = std::iter::repeat(0).take(iter.len()).collect();
        let validity = std::iter::repeat(0).take(iter.len()).collect();

        Self {
            iter,
            primitive_validity,
            remaining,
            validity,
            total: 0,
            current_level: 0,
            remaining_values,
        }
    }
}

impl<'a> Iterator for DefLevelsIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if *self.remaining.last().unwrap() > 0 {
            *self.remaining.last_mut().unwrap() -= 1;

            let primitive = self.primitive_validity.next()?.0 as u32;
            let r = Some(self.total + primitive);

            for level in 0..self.current_level - 1 {
                let level = self.remaining.len() - level - 1;
                if self.remaining[level] == 0 {
                    self.current_level -= 1;
                    self.total -= self.validity[level];
                    self.remaining[level.saturating_sub(1)] -= 1;
                }
            }
            if self.remaining[0] == 0 {
                self.current_level -= 1;
                self.total -= self.validity[0] as u32;
            }
            self.remaining_values -= 1;
            return r;
        }

        for ((iter, remaining), validity) in self
            .iter
            .iter_mut()
            .zip(self.remaining.iter_mut())
            .zip(self.validity.iter_mut())
            .skip(self.current_level)
        {
            let (is_valid, length): (u32, usize) = iter.next()?;
            *validity = is_valid;
            if length == 0 {
                self.remaining_values -= 1;
                return Some(self.total + is_valid / 2);
            }
            *remaining = length;
            self.current_level += 1;
            self.total += is_valid;
        }
        self.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let length = self.remaining_values;
        (length, Some(length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test(nested: Vec<Nested>, expected: Vec<u32>) {
        let mut iter = DefLevelsIter::new(&nested);
        assert_eq!(iter.size_hint().0, expected.len());
        let result = iter.by_ref().collect::<Vec<_>>();
        assert_eq!(result, expected);
        assert_eq!(iter.size_hint().0, 0);
    }

    #[test]
    fn struct_optional() {
        let b = Bitmap::from([
            true, false, true, true, false, true, false, false, true, true,
        ]);
        let nested = vec![
            Nested::Struct(None, true, 10),
            Nested::Primitive(Some(&b), true, 10),
        ];
        let expected = vec![2, 1, 2, 2, 1, 2, 1, 1, 2, 2];

        test(nested, expected)
    }

    #[test]
    fn struct_optional_1() {
        let b = Bitmap::from([
            true, false, true, true, false, true, false, false, true, true,
        ]);
        let nested = vec![
            Nested::Struct(None, true, 10),
            Nested::Primitive(Some(&b), true, 10),
        ];
        let expected = vec![2, 1, 2, 2, 1, 2, 1, 1, 2, 2];

        test(nested, expected)
    }

    #[test]
    fn struct_optional_optional() {
        let nested = vec![
            Nested::Struct(None, true, 10),
            Nested::Primitive(None, true, 10),
        ];
        let expected = vec![2, 2, 2, 2, 2, 2, 2, 2, 2, 2];

        test(nested, expected)
    }

    #[test]
    fn l1_required_required() {
        let nested = vec![
            // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
            Nested::List(ListNested::<i32> {
                is_optional: false,
                offsets: &[0, 2, 2, 5, 8, 8, 11, 11, 12],
                validity: None,
            }),
            Nested::Primitive(None, false, 12),
        ];
        let expected = vec![1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1];

        test(nested, expected)
    }

    #[test]
    fn l1_optional_optional() {
        // [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]

        let v0 = Bitmap::from([true, false, true, true, true, true, false, true]);
        let v1 = Bitmap::from([
            true, true, //[0, 1]
            true, false, true, //[2, None, 3]
            true, true, true, //[4, 5, 6]
            true, true, true, //[7, 8, 9]
            true, //[10]
        ]);
        let nested = vec![
            Nested::List(ListNested::<i32> {
                is_optional: true,
                offsets: &[0, 2, 2, 5, 8, 8, 11, 11, 12],
                validity: Some(&v0),
            }),
            Nested::Primitive(Some(&v1), true, 12),
        ];
        let expected = vec![3u32, 3, 0, 3, 2, 3, 3, 3, 3, 1, 3, 3, 3, 0, 3];

        test(nested, expected)
    }

    #[test]
    fn l2_required_required_required() {
        let nested = vec![
            Nested::List(ListNested::<i32> {
                is_optional: false,
                offsets: &[0, 2, 4],
                validity: None,
            }),
            Nested::List(ListNested::<i32> {
                is_optional: false,
                offsets: &[0, 3, 7, 8, 10],
                validity: None,
            }),
            Nested::Primitive(None, false, 12),
        ];
        let expected = vec![2, 2, 2, 2, 2, 2, 2, 2, 2, 2];

        test(nested, expected)
    }

    #[test]
    fn l2_optional_required_required() {
        let a = Bitmap::from([true, false, true, true]);
        // e.g. [[[1,2,3], [4,5,6,7]], None, [[8], [], [9, 10]]]
        let nested = vec![
            Nested::List(ListNested::<i32> {
                is_optional: true,
                offsets: &[0, 2, 2, 2, 5],
                validity: Some(&a),
            }),
            Nested::List(ListNested::<i32> {
                is_optional: false,
                offsets: &[0, 3, 7, 8, 8, 10],
                validity: None,
            }),
            Nested::Primitive(None, false, 12),
        ];
        let expected = vec![3, 3, 3, 3, 3, 3, 3, 0, 1, 3, 2, 3, 3];

        test(nested, expected)
    }

    #[test]
    fn l2_optional_optional_required() {
        let a = Bitmap::from([true, false, true]);
        let b = Bitmap::from([true, true, true, true, false]);
        // e.g. [[[1,2,3], [4,5,6,7]], None, [[8], [], None]]
        let nested = vec![
            Nested::List(ListNested::<i32> {
                is_optional: true,
                offsets: &[0, 2, 2, 5],
                validity: Some(&a),
            }),
            Nested::List(ListNested::<i32> {
                is_optional: true,
                offsets: &[0, 3, 7, 8, 8, 8],
                validity: Some(&b),
            }),
            Nested::Primitive(None, false, 12),
        ];
        let expected = vec![4, 4, 4, 4, 4, 4, 4, 0, 4, 3, 2];

        test(nested, expected)
    }

    #[test]
    fn l2_optional_optional_optional() {
        let a = Bitmap::from([true, false, true]);
        let b = Bitmap::from([true, true, true, false]);
        let c = Bitmap::from([true, true, true, true, false, true, true, true]);
        // e.g. [[[1,2,3], [4,None,6,7]], None, [[8], None]]
        let nested = vec![
            Nested::List(ListNested::<i32> {
                is_optional: true,
                offsets: &[0, 2, 2, 4],
                validity: Some(&a),
            }),
            Nested::List(ListNested::<i32> {
                is_optional: true,
                offsets: &[0, 3, 7, 8, 8],
                validity: Some(&b),
            }),
            Nested::Primitive(Some(&c), true, 12),
        ];
        let expected = vec![5, 5, 5, 5, 4, 5, 5, 0, 5, 2];

        test(nested, expected)
    }
}
