// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{array::*, error::Result, record_batch::RecordBatch};

use prettytable::format;
use prettytable::{Cell, Row, Table};

/// Returns a visual representation of multiple [`RecordBatch`]es.
pub fn write(batches: &[RecordBatch]) -> Result<String> {
    Ok(create_table(batches)?.to_string())
}

/// Prints a visual representation of record batches to stdout
pub fn print(results: &[RecordBatch]) -> Result<()> {
    create_table(results)?.printstd();
    Ok(())
}

/// Convert a series of record batches into a table
fn create_table(results: &[RecordBatch]) -> Result<Table> {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(&field.name()));
    }
    table.set_titles(Row::new(header));

    for batch in results {
        let displayes = batch
            .columns()
            .iter()
            .map(|array| get_display(array.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            (0..batch.num_columns()).for_each(|col| {
                let string = displayes[col](row);
                cells.push(Cell::new(&string));
            });
            table.add_row(Row::new(cells));
        }
    }

    Ok(table)
}

#[cfg(test)]
mod tests {
    use crate::{array::*, datatypes::*};

    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_write() -> Result<()> {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Utf8Array::<i32>::from(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some("d"),
                ])),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(10), Some(100)])),
            ],
        )?;

        let table = write(&[batch])?;

        let expected = vec![
            "+---+-----+",
            "| a | b   |",
            "+---+-----+",
            "| a | 1   |",
            "| b |     |",
            "|   | 10  |",
            "| d | 100 |",
            "+---+-----+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_write_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Null, true),
        ]));

        let num_rows = 4;
        let arrays = schema
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type().clone(), num_rows).into())
            .collect();

        // define data (null)
        let batch = RecordBatch::try_new(schema, arrays).unwrap();

        let table = write(&[batch]).unwrap();

        let expected = vec![
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "|   |   |   |",
            "|   |   |   |",
            "|   |   |   |",
            "|   |   |   |",
            "+---+---+---+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{:#?}", table);
    }

    #[test]
    fn test_write_dictionary() -> Result<()> {
        // define a schema.
        let field_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![Field::new("d1", field_type, true)]));

        let array = DictionaryPrimitive::<i32, Utf8Primitive<i32>, _>::try_from_iter(
            vec![Ok(Some("one")), Ok(None), Ok(Some("three"))].into_iter(),
        )
        .unwrap()
        .into_arc();

        let batch = RecordBatch::try_new(schema, vec![array])?;

        let table = write(&[batch])?;

        let expected = vec![
            "+-------+",
            "| d1    |",
            "+-------+",
            "| one   |",
            "|       |",
            "| three |",
            "+-------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    /// Generate an array with type $ARRAYTYPE with a numeric value of
    /// $VALUE, and compare $EXPECTED_RESULT to the output of
    /// formatting that array with `write`
    macro_rules! check_datetime {
        ($ty:ty, $datatype:expr, $value:expr, $EXPECTED_RESULT:expr) => {
            let array = Primitive::<$ty>::from(&[Some($value), None]).to_arc(&$datatype);

            let schema = Arc::new(Schema::new(vec![Field::new(
                "f",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

            let table = write(&[batch]).expect("formatting batches");

            let expected = $EXPECTED_RESULT;
            let actual: Vec<&str> = table.lines().collect();

            assert_eq!(expected, actual, "Actual result:\n\n{:#?}\n\n", actual);
        };
    }

    #[test]
    fn test_write_timestamp_second() {
        let expected = vec![
            "+---------------------+",
            "| f                   |",
            "+---------------------+",
            "| 1970-05-09 14:25:11 |",
            "|                     |",
            "+---------------------+",
        ];
        check_datetime!(
            i64,
            DataType::Timestamp(TimeUnit::Second, None),
            11111111,
            expected
        );
    }

    #[test]
    fn test_write_timestamp_millisecond() {
        let expected = vec![
            "+-------------------------+",
            "| f                       |",
            "+-------------------------+",
            "| 1970-01-01 03:05:11.111 |",
            "|                         |",
            "+-------------------------+",
        ];
        check_datetime!(
            i64,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            11111111,
            expected
        );
    }

    #[test]
    fn test_write_timestamp_microsecond() {
        let expected = vec![
            "+----------------------------+",
            "| f                          |",
            "+----------------------------+",
            "| 1970-01-01 00:00:11.111111 |",
            "|                            |",
            "+----------------------------+",
        ];
        check_datetime!(
            i64,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            11111111,
            expected
        );
    }

    #[test]
    fn test_write_timestamp_nanosecond() {
        let expected = vec![
            "+-------------------------------+",
            "| f                             |",
            "+-------------------------------+",
            "| 1970-01-01 00:00:00.011111111 |",
            "|                               |",
            "+-------------------------------+",
        ];
        check_datetime!(
            i64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            11111111,
            expected
        );
    }

    #[test]
    fn test_write_date_32() {
        let expected = vec![
            "+------------+",
            "| f          |",
            "+------------+",
            "| 1973-05-19 |",
            "|            |",
            "+------------+",
        ];
        check_datetime!(i32, DataType::Date32, 1234, expected);
    }

    #[test]
    fn test_write_date_64() {
        let expected = vec![
            "+------------+",
            "| f          |",
            "+------------+",
            "| 2005-03-18 |",
            "|            |",
            "+------------+",
        ];
        check_datetime!(i64, DataType::Date64, 1111111100000, expected);
    }

    #[test]
    fn test_write_time_32_second() {
        let expected = vec![
            "+----------+",
            "| f        |",
            "+----------+",
            "| 00:18:31 |",
            "|          |",
            "+----------+",
        ];
        check_datetime!(i32, DataType::Time32(TimeUnit::Second), 1111, expected);
    }

    #[test]
    fn test_write_time_32_millisecond() {
        let expected = vec![
            "+--------------+",
            "| f            |",
            "+--------------+",
            "| 03:05:11.111 |",
            "|              |",
            "+--------------+",
        ];
        check_datetime!(
            i32,
            DataType::Time32(TimeUnit::Millisecond),
            11111111,
            expected
        );
    }

    #[test]
    fn test_write_time_64_microsecond() {
        let expected = vec![
            "+-----------------+",
            "| f               |",
            "+-----------------+",
            "| 00:00:11.111111 |",
            "|                 |",
            "+-----------------+",
        ];
        check_datetime!(
            i64,
            DataType::Time64(TimeUnit::Microsecond),
            11111111,
            expected
        );
    }

    #[test]
    fn test_write_time_64_nanosecond() {
        let expected = vec![
            "+--------------------+",
            "| f                  |",
            "+--------------------+",
            "| 00:00:00.011111111 |",
            "|                    |",
            "+--------------------+",
        ];
        check_datetime!(
            i64,
            DataType::Time64(TimeUnit::Nanosecond),
            11111111,
            expected
        );
    }
}
