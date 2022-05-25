use super::super::pages::Nested;
use super::to_length;

trait DebugIter: Iterator<Item = usize> + std::fmt::Debug {}

impl<A: Iterator<Item = usize> + std::fmt::Debug> DebugIter for A {}

fn iter<'a>(nested: &'a [Nested]) -> Vec<Box<dyn DebugIter + 'a>> {
    nested
        .iter()
        .filter_map(|nested| match nested {
            Nested::Primitive(_, _, _) => None,
            Nested::List(nested) => Some(Box::new(to_length(nested.offsets)) as Box<dyn DebugIter>),
            Nested::LargeList(nested) => {
                Some(Box::new(to_length(nested.offsets)) as Box<dyn DebugIter>)
            }
            Nested::Struct(_, _, length) => {
                Some(Box::new(std::iter::repeat(0usize).take(*length)) as Box<dyn DebugIter>)
            }
        })
        .collect()
}

pub fn num_values(nested: &[Nested]) -> usize {
    let iterators = iter(nested);
    let depth = iterators.len();

    iterators
        .into_iter()
        .enumerate()
        .map(|(index, lengths)| {
            if index == depth - 1 {
                lengths
                    .map(|length| if length == 0 { 1 } else { length })
                    .sum::<usize>()
            } else {
                lengths
                    .map(|length| if length == 0 { 1 } else { 0 })
                    .sum::<usize>()
            }
        })
        .sum()
}

/// Iterator adapter of parquet / dremel repetition levels
#[derive(Debug)]
pub struct RepLevelsIter<'a> {
    // iterators of lengths. E.g. [[[a,b,c], [d,e,f,g]], [[h], [i,j]]] -> [[2, 2], [3, 4, 1, 2]]
    iter: Vec<Box<dyn DebugIter + 'a>>,
    // vector containing the remaining number of values of each iterator.
    // e.g. the iters [[2, 2], [3, 4, 1, 2]] after the first iteration will return [2, 3],
    // and remaining will be [2, 3].
    // on the second iteration, it will be `[2, 2]` (since iterations consume the last items)
    remaining: Vec<usize>, /* < remaining.len() == iter.len() */
    // cache of the first `remaining` that is non-zero. Examples:
    // * `remaining = [2, 2] => current_level = 2`
    // * `remaining = [2, 0] => current_level = 1`
    // * `remaining = [0, 0] => current_level = 0`
    current_level: usize, /* < iter.len() */
    // the number to discount due to being the first element of the iterators.
    total: usize, /* < iter.len() */

    // the total number of items that this iterator will return
    remaining_values: usize,
}

impl<'a> RepLevelsIter<'a> {
    pub fn new(nested: &'a [Nested]) -> Self {
        let remaining_values = num_values(nested);

        let iter = iter(nested);
        let remaining = std::iter::repeat(0).take(iter.len()).collect();

        Self {
            iter,
            remaining,
            total: 0,
            current_level: 0,
            remaining_values,
        }
    }
}

impl<'a> Iterator for RepLevelsIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if *self.remaining.last().unwrap() > 0 {
            *self.remaining.last_mut().unwrap() -= 1;

            let total = self.total;
            self.total = 0;
            let r = Some((self.current_level - total) as u32);

            for level in 0..self.current_level - 1 {
                let level = self.remaining.len() - level - 1;
                if self.remaining[level] == 0 {
                    self.current_level -= 1;
                    self.remaining[level.saturating_sub(1)] -= 1;
                }
            }
            if self.remaining[0] == 0 {
                self.current_level -= 1;
            }
            self.remaining_values -= 1;
            return r;
        }

        self.total = 0;
        for (iter, remaining) in self
            .iter
            .iter_mut()
            .zip(self.remaining.iter_mut())
            .skip(self.current_level)
        {
            let length: usize = iter.next()?;
            if length == 0 {
                self.remaining_values -= 1;
                return Some(self.current_level as u32);
            }
            *remaining = length;
            self.current_level += 1;
            self.total += 1;
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
    use super::super::super::pages::ListNested;

    use super::*;

    fn test(nested: Vec<Nested>, expected: Vec<u32>) {
        let mut iter = RepLevelsIter::new(&nested);
        assert_eq!(iter.size_hint().0, expected.len());
        let result = iter.by_ref().collect::<Vec<_>>();
        assert_eq!(result, expected);
        assert_eq!(iter.size_hint().0, 0);
    }

    #[test]
    fn struct_required() {
        let nested = vec![
            Nested::Struct(None, true, 10),
            Nested::Primitive(None, true, 10),
        ];
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        test(nested, expected)
    }

    #[test]
    fn l1() {
        let nested = vec![
            Nested::List(ListNested::<i32> {
                is_optional: false,
                offsets: &[0, 2, 2, 5, 8, 8, 11, 11, 12],
                validity: None,
            }),
            Nested::Primitive(None, false, 12),
        ];

        let expected = vec![0u32, 1, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0];

        test(nested, expected)
    }

    #[test]
    fn l2() {
        let nested = vec![
            Nested::List(ListNested::<i32> {
                is_optional: false,
                offsets: &[0, 2, 2, 4],
                validity: None,
            }),
            Nested::List(ListNested::<i32> {
                is_optional: false,
                offsets: &[0, 3, 7, 8, 10],
                validity: None,
            }),
            Nested::Primitive(None, false, 10),
        ];
        let expected = vec![0, 2, 2, 1, 2, 2, 2, 0, 0, 1, 2];

        test(nested, expected)
    }
}
