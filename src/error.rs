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

//! Defines `ArrowError` for representing failures in various Arrow operations.
use std::fmt::{Debug, Display, Formatter};

use std::error::Error;

/// Many different operations in the `arrow` crate return this error type.
#[derive(Debug)]
pub enum ArrowError {
    /// Returned when functionality is not yet available.
    NotYetImplemented(String),
    /// Triggered by an external error, such as CSV, serde, chrono.
    External(String, Box<dyn Error + Send + Sync>),
    Schema(String),
    Io(std::io::Error),
    InvalidArgumentError(String),
    /// Error during import or export to/from C Data Interface
    FFI(String),
    /// Error during import or export to/from IPC
    IPC(String),
    /// Error during import or export to/from a format
    ExternalFormat(String),
    DictionaryKeyOverflowError,
    Other(String),
}

impl ArrowError {
    /// Wraps an external error in an `ArrowError`.
    pub fn from_external_error(error: Box<dyn ::std::error::Error + Send + Sync>) -> Self {
        Self::External("".to_string(), error)
    }
}

impl From<::std::io::Error> for ArrowError {
    fn from(error: std::io::Error) -> Self {
        ArrowError::Io(error)
    }
}

impl Display for ArrowError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowError::NotYetImplemented(source) => {
                write!(f, "Not yet implemented: {}", &source)
            }
            ArrowError::External(message, source) => {
                write!(f, "External error{}: {}", message, &source)
            }
            ArrowError::Schema(desc) => write!(f, "Schema error: {}", desc),
            ArrowError::Io(desc) => write!(f, "Io error: {}", desc),
            ArrowError::InvalidArgumentError(desc) => {
                write!(f, "Invalid argument error: {}", desc)
            }
            ArrowError::FFI(desc) => {
                write!(f, "FFI error: {}", desc)
            }
            ArrowError::IPC(desc) => {
                write!(f, "IPC error: {}", desc)
            }
            ArrowError::ExternalFormat(desc) => {
                write!(f, "External format error: {}", desc)
            }
            ArrowError::DictionaryKeyOverflowError => {
                write!(f, "Dictionary key bigger than the key type")
            }
            ArrowError::Other(message) => {
                write!(f, "{}", message)
            }
        }
    }
}

impl Error for ArrowError {}

pub type Result<T> = std::result::Result<T, ArrowError>;
