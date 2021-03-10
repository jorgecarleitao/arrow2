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

//! Transfer data between the Arrow memory format and CSV (comma-separated values).

use crate::error::ArrowError;

impl From<csv::Error> for ArrowError {
    fn from(error: csv::Error) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

impl From<chrono::ParseError> for ArrowError {
    fn from(error: chrono::ParseError) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

mod parser;
mod reader;
mod writer;

mod infer_schema;
mod read_boolean;
mod read_primitive;
pub use infer_schema::{infer_file_schema, infer_schema_from_files};
pub use read_boolean::{new_boolean_array, BooleanParser};
pub use read_primitive::{new_primitive_array, PrimitiveParser};

pub use reader::*;
pub use writer::{Writer, WriterBuilder};
