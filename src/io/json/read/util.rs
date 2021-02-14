use std::io::{BufRead, BufReader, Read};

use serde_json::Value;

use crate::error::{ArrowError, Result};

/// JSON file reader that produces a serde_json::Value iterator from a Read trait
///
/// # Example
///
/// ```
/// use std::fs::File;
/// use std::io::BufReader;
/// use arrow::json::reader::ValueIter;
///
/// let mut reader =
///     BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
/// let mut value_reader = ValueIter::new(&mut reader, None);
/// for value in value_reader {
///     println!("JSON value: {}", value.unwrap());
/// }
/// ```
#[derive(Debug)]
pub(super) struct ValueIter<'a, R: Read> {
    reader: &'a mut BufReader<R>,
    max_read_records: Option<usize>,
    record_count: usize,
    // reuse line buffer to avoid allocation on each record
    line_buf: String,
}

impl<'a, R: Read> ValueIter<'a, R> {
    pub fn new(reader: &'a mut BufReader<R>, max_read_records: Option<usize>) -> Self {
        Self {
            reader,
            max_read_records,
            record_count: 0,
            line_buf: String::new(),
        }
    }
}

impl<'a, R: Read> Iterator for ValueIter<'a, R> {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(max) = self.max_read_records {
            if self.record_count >= max {
                return None;
            }
        }

        loop {
            self.line_buf.truncate(0);
            match self.reader.read_line(&mut self.line_buf) {
                Ok(0) => {
                    // read_line returns 0 when stream reached EOF
                    return None;
                }
                Err(e) => {
                    return Some(Err(ArrowError::JsonError(format!(
                        "Failed to read JSON record: {}",
                        e
                    ))));
                }
                _ => {
                    let trimmed_s = self.line_buf.trim();
                    if trimmed_s.is_empty() {
                        // ignore empty lines
                        continue;
                    }

                    self.record_count += 1;
                    return Some(
                        serde_json::from_str(trimmed_s)
                            .map_err(|e| ArrowError::JsonError(format!("Not valid JSON: {}", e))),
                    );
                }
            }
        }
    }
}
