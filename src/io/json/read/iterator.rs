use std::io::BufRead;

use serde_json::Value;

use crate::error::{ArrowError, Result};

#[derive(Debug)]
pub struct ValueIter<'a, R: BufRead> {
    reader: &'a mut R,
    remaining: usize,
    // reuse line buffer to avoid allocation on each record
    line_buf: String,
}

impl<'a, R: BufRead> ValueIter<'a, R> {
    pub fn new(reader: &'a mut R, number_of_rows: Option<usize>) -> Self {
        Self {
            reader,
            remaining: number_of_rows.unwrap_or(usize::MAX),
            line_buf: String::new(),
        }
    }
}

impl<'a, R: BufRead> Iterator for ValueIter<'a, R> {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        loop {
            self.line_buf.truncate(0);
            match self.reader.read_line(&mut self.line_buf) {
                Ok(0) => {
                    // read_line returns 0 when stream reached EOF
                    return None;
                }
                Err(e) => {
                    return Some(Err(ArrowError::from(e)));
                }
                _ => {
                    let trimmed_s = self.line_buf.trim();
                    if trimmed_s.is_empty() {
                        // ignore empty lines
                        continue;
                    }

                    self.remaining -= 1;
                    return Some(serde_json::from_str(trimmed_s).map_err(ArrowError::from));
                }
            }
        }
    }
}
