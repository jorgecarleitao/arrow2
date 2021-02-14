//! JSON Reader
//!
//! This JSON reader allows JSON line-delimited files to be read into the Arrow memory
//! model. Records are loaded in batches and are then converted from row-based data to
//! columnar data.
//!
//! Example:
//!
//! ```
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::json;
//! use std::fs::File;
//! use std::io::BufReader;
//! use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("a", DataType::Float64, false),
//!     Field::new("b", DataType::Float64, false),
//!     Field::new("c", DataType::Float64, false),
//! ]);
//!
//! let file = File::open("test/data/basic.json").unwrap();
//!
//! let mut json = json::Reader::new(BufReader::new(file), Arc::new(schema), 1024, None);
//! let batch = json.next().unwrap().unwrap();
//! ```

mod deserialize;
mod infer_schema;
mod reader;
mod util;

pub use infer_schema::*;
pub use reader::*;
