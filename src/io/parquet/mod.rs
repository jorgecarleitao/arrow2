use crate::error::ArrowError;

pub mod read;
pub mod write;

impl From<parquet2::error::ParquetError> for ArrowError {
    fn from(error: parquet2::error::ParquetError) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use crate::array::*;
    use crate::datatypes::*;

    pub fn pyarrow_integration(column: usize) -> Box<dyn Array> {
        let i64_values = &[
            Some(0),
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
            Some(6),
            Some(7),
            None,
            Some(9),
        ];

        match column {
            0 => Box::new(Primitive::<i64>::from(i64_values).to(DataType::Int64)),
            1 => Box::new(
                Primitive::<f64>::from(&[
                    Some(0.0),
                    Some(1.0),
                    None,
                    Some(3.0),
                    None,
                    Some(5.0),
                    Some(6.0),
                    Some(7.0),
                    None,
                    Some(9.0),
                ])
                .to(DataType::Float64),
            ),
            2 => Box::new(Utf8Array::<i32>::from(&vec![
                Some("Hello".to_string()),
                None,
                Some("aa".to_string()),
                Some("".to_string()),
                None,
                Some("abc".to_string()),
                None,
                None,
                Some("def".to_string()),
                Some("aaa".to_string()),
            ])),
            3 => Box::new(BooleanArray::from(&[
                Some(true),
                None,
                Some(false),
                Some(false),
                None,
                Some(true),
                None,
                None,
                Some(true),
                Some(true),
            ])),
            4 => Box::new(
                Primitive::<i64>::from(i64_values)
                    .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
            ),
            5 => {
                let values = i64_values
                    .iter()
                    .map(|x| x.map(|x| x as u32))
                    .collect::<Vec<_>>();
                Box::new(Primitive::<u32>::from(values).to(DataType::UInt32))
            }
            _ => unreachable!(),
        }
    }
}
