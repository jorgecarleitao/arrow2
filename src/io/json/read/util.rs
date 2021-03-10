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

use std::io::{BufRead, BufReader, Read};

use serde_json::Value;

use crate::error::{ArrowError, Result};

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
                    return Some(Err(ArrowError::from(e)));
                }
                _ => {
                    let trimmed_s = self.line_buf.trim();
                    if trimmed_s.is_empty() {
                        // ignore empty lines
                        continue;
                    }

                    self.record_count += 1;
                    return Some(serde_json::from_str(trimmed_s).map_err(ArrowError::from));
                }
            }
        }
    }
}
